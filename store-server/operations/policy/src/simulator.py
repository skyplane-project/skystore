import csv
from datetime import datetime
from typing import Dict, List
import typer
from src.placement_policy import LocalWrite, SingleRegionWrite, PullOnRead, ReplicateAll, SPANStore
from src.transfer_policy import DirectTransfer, ClosestTransfer
from src.utils.helpers import load_config, get_full_path, make_nx_graph
from src.utils.definitions import aws_instance_throughput_limit, gcp_instance_throughput_limit, azure_instance_throughput_limit
from src.model.region_mgmt import RegionManager
from src.model.object import LogicalObject, PhysicalObject, Status
from src.model.tracker import Tracker
from src.model.request import Request
import networkx as nx
import matplotlib.pyplot as plt
import logging
from prettytable import PrettyTable
import coloredlogs
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn
from src.utils.definitions import GB
import textwrap
from datetime import datetime, timedelta
import heapq

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

coloredlogs.install(level="INFO", logger=logger)


class Event:
    def __init__(self, timestamp: datetime, action, *args):
        self.timestamp = timestamp
        self.action = action
        self.args = args

    def __lt__(self, other):
        return self.timestamp < other.timestamp


class Simulator:
    def __init__(self, config_path: str, trace_path: str, num_vms=1):
        self.config = load_config(config_path)
        self.trace_path = trace_path
        self._print_config_details()
        self.total_graph = make_nx_graph()
        self.num_vms = num_vms
        self.tracker = Tracker()

        self.logical_objects: Dict[str, LogicalObject] = {}  # key to logical object
        self.region_manager = RegionManager(self.total_graph, logger)  # region to physical object
        self.actions = []  # list of actions taken by the simulator

        self.placement_policy = self._select_placement_policy(self.config.placement_policy)
        if self.config.placement_policy not in ["oracle", "spanstore"]:
            self.transfer_policy = self._select_transfer_policy(self.config.transfer_policy)
        else:
            # Oracle, SPANStore: decide both placement and transfer
            self.transfer_policy = None

        # Event queue
        self.events = []

        # SPANStore: request aggregation
        self.request_buffer: List[Request] = []
        self.last_processed_time = None
        self.update_time_interval = timedelta(hours=1)
        self.place_decisions: Dict[str, List[str]] = {}
        self.is_updating_placement = False  # State variable to indicate whether we are updating placement decisions

    ############################ EVENT FUNCTIONS ############################
    def schedule_event(self, base_timestamp: datetime, delay: timedelta, action, *args):
        print(f"Scheduling event: {action.__name__} at {base_timestamp + delay}")
        event_time = base_timestamp + delay
        heapq.heappush(self.events, Event(event_time, action, *args))

    def process_events(self, until: datetime):
        while self.events and self.events[0].timestamp <= until:
            event = heapq.heappop(self.events)
            event.action(*event.args)

    def initiate_data_transfer(self, current_timestamp: datetime, request: Request, transfer_graphs: List[nx.DiGraph], place_regions: List[str]):
        """
        Initiate data transfer for the given request.

        Args:
            current_timestamp (datetime): the current timestamp
            request (Request): the request
            transfer_graphs (List[nx.DiGraph]): list of path to transfer data (i.e. push-based: write to multi-regions)
            place_regions (List[str]): list of regions to write to
        """
        transfer_times = []
        for i in range(len(transfer_graphs)):
            transfer_graph = transfer_graphs[i]
            num_partitions = list(transfer_graph.edges.data())[0][-1]["num_partitions"]
            # Transfer time is the min of the transfer time (i.e. push-based, write local succeeds then write succeeds)
            transfer_time = min(
                [
                    8 * request.size * len(edge[-1]["partitions"]) / (GB * num_partitions) * (1 / edge[-1]["throughput"])
                    for edge in transfer_graph.edges.data()
                ]
            )
            transfer_time_millis = transfer_time * 1000
            typer.echo(
                f"\n({typer.style(request.op, fg='yellow')}) Transfer start: key ({typer.style(str(request.obj_key), fg='yellow')}) - start time({typer.style(str(current_timestamp), fg='yellow')} - placed region {[] if len(place_regions) == 0 else typer.style(place_regions[i], fg='yellow')} - transfer time ({typer.style(str(round(transfer_time, 3)), fg='yellow')})"
            )
            schedule_place_region = place_regions[i] if len(place_regions) > 0 else []

            # Schedule the completion of the transfer after the transfer time
            self.schedule_event(
                current_timestamp,
                timedelta(milliseconds=transfer_time_millis),
                self.complete_data_transfer,
                request,
                schedule_place_region,
                transfer_time_millis,
            )
            transfer_times.append(transfer_time_millis)
        return transfer_times

    def complete_data_transfer(self, request: Request, place_region: str, transfer_time: float):
        """
        Complete data transfer for the given request. Update the metadata and set the status to 'ready'.

        Args:
            request (Request): the request
            place_region (str): the region to write to
            transfer_time (float): the transfer time
        """
        obj_key = request.obj_key

        # Calculate the end timestamp of the transfer using the request timestamp
        end_timestamp = request.timestamp + timedelta(milliseconds=transfer_time)
        typer.echo(
            f"\n({typer.style(request.op, fg='yellow')}) Transfer end ({typer.style(place_region, fg='yellow')}): key ({typer.style(str(obj_key), fg='yellow')}) - end time ({typer.style(str(end_timestamp), fg='yellow')}) - transfer time ({typer.style(str(round(transfer_time, 3)), fg='yellow')} - request {request})"
        )

        if place_region == []:
            print("No place region, so don't write")
            return

        # If write, update metadata (mappings) and set the status to 'ready'.
        dst = place_region
        physical_obj = PhysicalObject(location_tag=dst, key=obj_key, size=request.size)
        self.logical_objects[obj_key].add_physical_object(dst, physical_obj)
        self.region_manager.add_object_to_region(dst, physical_obj)
        self.logical_objects[obj_key].physical_objects[dst].set_status(Status.ready)
        # Transfer completion time is when we start storing the data
        self.logical_objects[obj_key].physical_objects[dst].set_storage_start_time(end_timestamp)

        assert self.region_manager.has_object_in_region(dst, obj_key)

    ############################ TODO: SPANSTORE HELPER FUNCTIONS ############################
    def _update_placement_decisions(self, current_timestamp: datetime) -> Dict[str, List[str]]:
        """Update placement policy decisions."""
        # Check if there's enough data (duration) for a decision
        aggregated_requests = self._aggregate_requests(self.last_processed_time + self.update_time_interval)

        print(f"Update placement decisions: {aggregated_requests}")

        # Use ILP to get placement and transfer decisions
        if self.config.placement_policy in ["oracle", "spanstore"]:
            # only pass in actions for the last hour (same length as aggregated requests)
            actions = self.actions[-len(aggregated_requests) :]
            self.placement_policy.generate_decisions(aggregated_requests, actions)

        place_regions = {key: self.placement_policy.place(key) for key in self.logical_objects.keys()}
        # Set the next update time
        self.last_processed_time = current_timestamp
        print(f"Last processed time: {self.last_processed_time}")
        self.is_updating_placement = True
        return place_regions

    def _aggregate_requests(self, end_time: datetime) -> List[Request]:
        """Aggregate the requests in the past duration relative to end_time."""
        cutoff_time = end_time - self.update_time_interval
        agg_reqs = [req for req in self.request_buffer if cutoff_time <= req.timestamp <= end_time]
        print(f"Aggregating requests from {cutoff_time} to {end_time}")
        print(f"Agg results: {agg_reqs}")
        return agg_reqs

    def _fallback_policy(self):
        """Default policy when SPANStore hasn't computed data yet."""
        return {key: self.config.storage_region for key in self.logical_objects.keys()}

    def _should_update_policy(self, current_timestamp: datetime) -> bool:
        """Checks whether to update the placement policy."""
        if current_timestamp - self.last_processed_time >= self.update_time_interval:
            # Check if there's enough data (duration) to make a placement decision
            if (current_timestamp - self.request_buffer[0].timestamp) < self.update_time_interval:
                return False  # Not enough data, so don't update
            return True  # Otherwise, update the policy

    ############################ SPANSTORE FUNCTIONS ############################

    ############################ MAIN FUNCTIONS ############################
    def run(self):
        logger.info("Running simulations...")

        # Graphs for each read and write transfer
        read_graphs, write_graphs = [], []
        start_timestamp, end_timestamp = None, None
        num_lines = sum(1 for _ in open(get_full_path(self.trace_path), "r"))

        with open(get_full_path(self.trace_path), "r") as f:
            reader = csv.DictReader(f)
            with Progress(
                TextColumn("{task.fields[filename]}"),
                SpinnerColumn(),
                TextColumn("{task.percentage:>3.0f}%"),
                BarColumn(),
                transient=True,
            ) as progress:
                task = progress.add_task("[cyan]Processing...", total=num_lines - 1, filename="Processing requests")

                for row in reader:
                    # request_data = {k: v if k != "timestamp" else datetime.fromisoformat(v) for k, v in row.items()}
                    timestamp_str = row["timestamp"]
                    if timestamp_str.replace("-", "").replace(":", "").replace(" ", "").isdigit():
                        # Assuming the timestamp is in seconds since epoch
                        request_timestamp = datetime.fromtimestamp(int(timestamp_str))
                    else:
                        # ISO format parsing
                        request_timestamp = datetime.fromisoformat(timestamp_str)

                    request_data = {k: v for k, v in row.items()}
                    request_data["timestamp"] = request_timestamp

                    if request_data["op"] == "GET":
                        request_data["op"] = "read"
                    elif request_data["op"] == "PUT":
                        request_data["op"] = "write"

                    request = Request(**request_data)
                    obj_key = request.obj_key

                    # Process any events that have occurred since the last request
                    self.process_events(request.timestamp)

                    if start_timestamp is None:
                        start_timestamp = request.timestamp
                        self.last_processed_time = start_timestamp
                        print(f"Last processed time: {self.last_processed_time}")

                    self.request_buffer.append(request)  # This is for SPANStore
                    self.tracker.add_request_size(request.size)

                    # Add default storage location if not present
                    if obj_key not in self.logical_objects:
                        logical_obj = LogicalObject(key=obj_key, size=request.size, last_modified=request.timestamp)
                        self.logical_objects[obj_key] = logical_obj

                    read_transfer_graph, write_transfer_graph = None, None
                    runtime, cost = 0, 0

                    # Simulate policy decision
                    if self.config.placement_policy in ["oracle", "spanstore"]:
                        if self._should_update_policy(request.timestamp):
                            print("Update placement decisions")
                            place_regions = self._update_placement_decisions(request.timestamp)[obj_key]
                        else:
                            if self.is_updating_placement is False:
                                # Never updated placement policy, so use fallback
                                place_regions = [self._fallback_policy()[obj_key]]
                            else:
                                # Computed placement policy, but not enough data to update
                                place_regions = self.placement_policy.place(obj_key)

                        # TODO: move this logic outside the loop?
                        removed_regions = []
                        for region in place_regions:
                            if self.region_manager.has_object_in_region(region, obj_key):
                                print(f"Region {region} already contains object {obj_key} at timestamp {request.timestamp}, so don't transfer")
                                removed_regions.append(region)
                            else:
                                print(f"Region {region} does not contain object {obj_key} at timestamp {request.timestamp}, so transfer")
                        place_regions = [region for region in place_regions if region not in removed_regions]

                    else:
                        place_regions = self.placement_policy.place(request, self.config)

                    print("Place regions: ", place_regions)

                    policy = self.placement_policy if self.config.placement_policy in ["oracle", "spanstore"] else self.transfer_policy
                    issue_timestamp = request.timestamp
                    if request.op == "read":
                        read_region, read_transfer_graph = policy.read_transfer_path(request)
                        read_graphs.append(read_transfer_graph)

                        # Schedule placed region at the same time
                        transfer_time = self.initiate_data_transfer(
                            issue_timestamp, request, [read_transfer_graph], [request.issue_region] if request.issue_region in place_regions else []
                        )
                        assert len(transfer_time) == 1

                        if read_region == request.issue_region and read_region in place_regions:
                            place_regions.remove(read_region)

                        if read_region in place_regions:
                            # Shouldn't write again if read from it??
                            print(f"Read region {read_region} is in place regions {place_regions}, so don't write")
                            place_regions.remove(read_region)

                        print(f"Issue region: {request.issue_region}, read region: {read_region}, place regions: {place_regions}")

                        if len(place_regions) > 0:
                            for region in place_regions:
                                write_transfer_graph = policy.write_transfer_path(request, dst=region)
                                write_graphs.append(write_transfer_graph)

                            # Schedule writes after read transfer
                            self.initiate_data_transfer(
                                issue_timestamp + timedelta(milliseconds=transfer_time[0]),
                                request,
                                write_graphs[-len(place_regions) :],
                                place_regions,
                            )

                    elif request.op == "write":
                        for region in place_regions:
                            write_transfer_graph = policy.write_transfer_path(request, dst=region)
                            write_graphs.append(write_transfer_graph)
                        read_region = []
                        print("Write graphs: ", write_graphs[-len(place_regions) :])
                        self.initiate_data_transfer(issue_timestamp, request, write_graphs[-len(place_regions) :], place_regions)
                    else:
                        raise ValueError("Invalid operation type")

                    # Update metrics
                    if read_transfer_graph is not None:
                        r, c = self._update_transfer_metric(read_transfer_graph, request)

                        read_latency = max(r, 0.04)  # object size smaller than don't use tput
                        self.tracker.add_latency("read", read_latency)
                        self.tracker.add_transfer_cost(c)

                        runtime += r
                        cost += c

                    if write_transfer_graph is not None:
                        overall_latency = []
                        for write_graph in write_graphs[-len(place_regions) :]:
                            r, c = self._update_transfer_metric(write_graph, request)
                            print("Write latency: ", r)

                            self.tracker.add_transfer_cost(c)
                            overall_latency.append(r)
                            cost += c
                        runtime += min(overall_latency)  # take the min latency of write graphs (assume eventual)

                        if request.op == "write":
                            print("Add write latency")
                            write_latency = max(min(overall_latency), 0.04)  # object size smaller than don't use tput
                            self.tracker.add_latency("write", write_latency)

                    for region in place_regions:
                        put_cost = self.total_graph.nodes[region]["pricePut"]
                        self.tracker.add_request_cost(put_cost)
                        cost += put_cost

                    get_cost = self.total_graph.nodes[region]["priceGet"]
                    self.tracker.add_request_cost(get_cost)
                    cost += get_cost

                    self.actions.append(
                        {
                            "timestamp": request.timestamp,
                            "R/W": request.op,
                            "obj_key": obj_key,
                            "size(GB)": request.size / GB,
                            "issue_region": request.issue_region,
                            "read_region": read_region,
                            "write_region": place_regions,
                            "latency(ms)": runtime * 1000,
                            "cost($)": cost,
                        }
                    )

                    progress.update(task, advance=1)
                    end_timestamp = request.timestamp

                if end_timestamp and start_timestamp:
                    # Hard code the end timestamp to be 50 seconds after the last request
                    self.tracker.set_duration(end_timestamp - start_timestamp)
                    self.process_events(end_timestamp + timedelta(seconds=50))
                    self.region_manager.calculate_remaining_storage_costs(end_timestamp + timedelta(seconds=50))

        self._print_action_table()
        self._print_region_manager()

    ############################ REPORT FUNCTIONS ############################
    def report_metrics(self):
        table = PrettyTable()
        table.field_names = ["Metric", "Value"]

        self.tracker.add_storage_cost(self.region_manager.aggregate_storage_cost())
        metrics = self.tracker.get_metrics()

        for key, value in metrics.items():
            table.add_row([key, value])

        logger.info("\n" + str(table))

    def plot_graphs(self, graphs: List):
        for graph in graphs:
            plt.figure()
            pos = nx.spring_layout(graph)
            nx.draw(
                graph,
                pos,
                with_labels=True,
                node_size=2000,
                node_color="skyblue",
                font_size=15,
            )
            edge_labels = nx.get_edge_attributes(graph, "obj_key")
            nx.draw_networkx_edge_labels(graph, pos, edge_labels=edge_labels)
            plt.title("Transfer Graph")
            plt.show()

    def _print_config_details(self):
        config_str = textwrap.dedent(
            """
            ------ Config Details ------
            storage_region: {}
            
            placement_policy: {}
            
            transfer_policy: {}
            
            latency_slo:
                read: {} ms
                write: {} ms
                
            consistency: {}
            ---------------------------
        """
        ).format(
            self.config.storage_region,
            self.config.placement_policy,
            self.config.transfer_policy,
            self.config.latency_slo.read,
            self.config.latency_slo.write,
            self.config.consistency,
        )
        logger.info(config_str)

    def _select_placement_policy(self, policy_type: str):
        if policy_type == "local":
            return LocalWrite()
        elif policy_type == "single_region":
            return SingleRegionWrite()
        elif policy_type == "pull_on_read":
            return PullOnRead(region_manager=self.region_manager)
        elif policy_type == "replicate_all":
            return ReplicateAll(total_graph=self.total_graph)
        elif policy_type == "spanstore":
            return SPANStore(
                policy="spanstore",
                config=self.config,
                total_graph=self.total_graph,
                objects=self.logical_objects,
                region_mgmt=self.region_manager,
                trace_path=self.trace_path,
            )
        else:
            raise ValueError("Invalid placement policy type")

    def _select_transfer_policy(self, policy_type: str):
        if policy_type == "direct":
            return DirectTransfer(config=self.config, total_graph=self.total_graph)
        elif policy_type == "closest":
            return ClosestTransfer(config=self.config, total_graph=self.total_graph, object_dict=self.logical_objects)
        else:
            raise ValueError("Invalid transfer policy type")

    def _print_action_table(self):
        columns = ["timestamp", "R/W", "obj_key", "size(GB)", "issue_region", "read_region", "write_region", "latency(ms)", "cost($)"]

        table = PrettyTable()
        table.field_names = columns

        for action in self.actions:
            table.add_row([round(float(action[col]), 6) if col in ["latency(ms)", "size(GB)", "cost($)"] else action[col] for col in columns])

        logger.info("\n" + str(table))

    def _print_region_manager(self):
        table2 = PrettyTable()
        table2.field_names = ["Region", "Objects", "Cost ($)"]

        for region, objects in self.region_manager.regions.items():
            # Convert to strings to join
            objects_list = [str(obj) for obj in objects]
            cost = self.region_manager.storage_costs.get(region, 0.0)
            table2.add_row([region, ", ".join(objects_list), cost])

        self.region_manager.print_stat()
        # logger.info("\n" + str(table2))

    def _update_transfer_metric(self, transfer_graph: nx.DiGraph, request: Request):
        num_partitions = list(transfer_graph.edges.data())[0][-1]["num_partitions"]
        # NOTE: eventual consistency (?)
        runtime = min(
            [
                8 * request.size * len(edge[-1]["partitions"]) / (GB * num_partitions) * (1 / edge[-1]["throughput"])
                for edge in transfer_graph.edges.data()
            ]
        )
        # cannot exceeds the ingress limit
        if request.op == "read":
            ingress_limit = (
                aws_instance_throughput_limit[1]
                if request.issue_region.startswith("aws")
                else gcp_instance_throughput_limit[1]
                if request.issue_region.startswith("gcp")
                else azure_instance_throughput_limit[1]
            )
            ingress_limit = ingress_limit * self.num_vms

            runtime = max(runtime, request.size * 8 / (GB * ingress_limit))
        elif request.op == "write":
            egress_limit = (
                aws_instance_throughput_limit[0]
                if request.issue_region.startswith("aws")
                else gcp_instance_throughput_limit[0]
                if request.issue_region.startswith("gcp")
                else azure_instance_throughput_limit[0]
            )
            print("Egress limit: ", egress_limit)
            egress_limit = egress_limit * self.num_vms
            print("Egress limit: ", egress_limit)
            print("Size: ", request.size)
            runtime = max(runtime, request.size * 8 / (GB * egress_limit))

        print("Transfer graph: ", transfer_graph.edges.data())
        each_edge_cost = [
            edge[-1]["cost"] * request.size * len(edge[-1]["partitions"]) / (GB * num_partitions) for edge in transfer_graph.edges.data()
        ]
        cost = sum(each_edge_cost)
        print(
            f"Size of data: {request.size / GB}, transfer runtime: {runtime}, transfer cost: {cost}, transfer graph: {transfer_graph.edges}, each edge cost = {each_edge_cost}"
        )

        return runtime, cost

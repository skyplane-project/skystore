import csv
from src.placement_policy.policy import PlacementPolicy
from src.model.config import Config
from src.model.request import Request
from src.model.object import LogicalObject
from typing import Dict, Tuple, List, Set
import networkx as nx
from skypie.api import create_oracle, OracleType, PACKAGE_RESOURCES
from collections import defaultdict
from src.utils.definitions import GB
from datetime import timedelta
from src.utils.helpers import refine_string, convert_hyphen_to_colon
from src.model.region_mgmt import RegionManager


class SPANStore(PlacementPolicy):
    def __init__(
        self,
        policy: str,
        config: Config,
        total_graph: nx.DiGraph,
        objects: Dict[str, LogicalObject],
        region_mgmt: RegionManager,
        trace_path: str,
        verbose: int = 1,
    ) -> None:
        self.config = config
        self.total_graph = total_graph
        self.objects = objects
        self.object_sizes: Dict[str, float] = {}

        self.access_sets: Dict[str, Tuple] = {}  # access sets for each object
        self.access_sets_objects: Dict[Tuple, List[str]] = {}  # for each access sets, what are the objects
        self._update_access_sets(trace_path)

        oracle_directory = "/Users/shuL/Desktop/sky-pie-oracle/examples/example_oracle_aws-replication-factor-2"

        if policy == "oracle":
            self.oracle = create_oracle(oracle_directory, OracleType.SKYPIE, verbose=verbose)
        elif policy == "spanstore":
            # Replace this with the data path
            prefix = "/Users/shuL/Desktop/sky-pie-oracle/src/skypie/data/"
            latency_slo = 2.0
            latency_file_object_size = 41943040

            self.oracle = create_oracle(
                oracle_directory=oracle_directory,
                oracle_type=OracleType.ILP,
                oracle_impl_args={
                    # "networkPriceFile": prefix + "network_cost_v2.csv",
                    # "storagePriceFile": prefix + "storage_pricing.csv",
                    # "noStrictReplication": False,
                    "latency_slo": latency_slo,
                    "network_latency_file": PACKAGE_RESOURCES.network_latency_files[latency_file_object_size],
                },
            )
        else:
            raise NotImplementedError("Policy {} not supported yet".format(policy))

        self.region_mgmt = region_mgmt
        self.policy = policy
        self.workload = None
        self.placement_decisions: Dict[str, List[str]] = {}
        self.get_decisions: Dict[str, str] = {}
        self.past_get_decisions: Dict[str, str] = {}
        super().__init__()

    def _update_access_sets(self, trace_path: str):
        """
        Calculate access sets given a particular trace

        Args:
            trace_path (str): path to the trace file
        """
        region_to_objects = {}

        with open(trace_path, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                region = row["issue_region"]
                obj_key = row["obj_key"]
                size = float(row["size"])

                self.object_sizes[obj_key] = size
                if region not in region_to_objects:
                    region_to_objects[region] = set()
                region_to_objects[region].add(obj_key)

        access_sets = {}
        for region, objects in region_to_objects.items():
            for obj in objects:
                # Update the access_sets
                if obj not in access_sets:
                    access_sets[obj] = set()
                access_sets[obj].add(region)

        # refactor access_sets so that the value is tuple as well
        self.access_sets = {k: tuple(sorted(v)) for k, v in access_sets.items()}
        # Invert keys and values of access_sets
        self.access_sets_objects = {}
        for obj_key, regions in self.access_sets.items():
            if regions not in self.access_sets_objects:
                self.access_sets_objects[regions] = []
            self.access_sets_objects[regions].append(obj_key)

        print(f"Access sets: {self.access_sets}")
        print(f"Access set objects: {self.access_sets_objects}")

    def spanstore_aggregate(
        self, requests: List[Request], actions: List[Dict[str, str]], access_set: Set[str], duration: timedelta = timedelta(hours=1)
    ) -> Tuple[int, Dict[str, int], Dict[str, int], Dict[str, float], Dict[str, float]]:
        """
        Aggregate requests and return inputs to SPANStore (i.e. size, put, get, ingress, egress)

        Args:
            requests (List[Request]): list of requests to aggregate
            actions (List[str]): actions taken to satisfy this request (i.e. recording where to read and write)
            duration (timedelta): the duration from the most recent request to aggregate

        Returns:
            size: total amount of data of all objects in this access set
            put: number of put request for each region in access set to the objects in that access sets
            get: number of get request for each region in access set to the objects in that access sets
            egress: for each object in the acccess sets, product of object size * # of puts (sum them up for each region)
            ingress: for each object in the acccess sets, product of object size * # of puts (sum them up for each region)
        """
        put_counts = defaultdict(int)
        get_counts = defaultdict(int)
        ingress_counts = defaultdict(float)
        egress_counts = defaultdict(float)

        objects_in_access_set = self.access_sets_objects[tuple(sorted(access_set))]
        print(f"Objects in access set: {objects_in_access_set}")
        print(f"Self access set objects: {self.access_sets_objects}")

        cutoff_time = requests[-1].timestamp - duration
        print(f"Last action: {actions[-1]}, and request: {requests[-1]}")
        print(f"Length of actions: {len(actions)}, and requests: {len(requests)}")

        # Start from the end of the requests list and iterate backwards until a request older than the cutoff_time is encountered
        for i in range(len(requests) - 1, -1, -1):
            request = requests[i]
            if request.timestamp < cutoff_time:
                break

            if request.obj_key in objects_in_access_set:
                if request.op == "write":
                    put_counts[request.issue_region] += 1
                elif request.op == "read":
                    get_counts[request.issue_region] += 1

            # Corresponding action for the request
            action = actions[i]
            if action["obj_key"] in objects_in_access_set:
                if action["R/W"] == "write":
                    for region in action["write_region"]:
                        if region in access_set:
                            ingress_counts[region] += action["size(GB)"] * GB
                elif action["R/W"] == "read":
                    if action["read_region"] != action["issue_region"]:
                        egress_counts[action["read_region"]] += action["size(GB)"] * GB

        # Compute size as the sum of sizes of objects in the access set for the filtered requests?
        # Or is it in the access sets?
        # size = sum([request.size for request in requests[i + 1 :] if request.obj_key in objects_in_access_set])

        # TODO: fix bug, here obj_key might not exist?
        size = sum([self.object_sizes[obj_key] for obj_key in objects_in_access_set])

        # For region in the access sets but didn't issue the request
        for region in access_set:
            if region not in put_counts:
                put_counts[region] = 0
            if region not in get_counts:
                get_counts[region] = 0
            if region not in ingress_counts:
                ingress_counts[region] = 0
            if region not in egress_counts:
                egress_counts[region] = 0

        # replace the key with first '-' with ':'
        put_counts = {convert_hyphen_to_colon(k): v for k, v in put_counts.items()}
        get_counts = {convert_hyphen_to_colon(k): v for k, v in get_counts.items()}
        ingress_counts = {convert_hyphen_to_colon(k): v for k, v in ingress_counts.items()}
        egress_counts = {convert_hyphen_to_colon(k): v for k, v in egress_counts.items()}

        return size, dict(put_counts), dict(get_counts), dict(ingress_counts), dict(egress_counts)

    def generate_decisions(self, requests: List[Request], actions: List[str], duration: timedelta = timedelta(hours=1)):
        # Reset the decisions periodically
        self.placement_decisions = {}
        if len(self.get_decisions) > 0:
            self.past_get_decisions = self.get_decisions

        self.get_decisions = {}
        # Iterate over each access set
        print(f"Requests passing in: {requests}")
        costs = 0  # total cost
        for access_set in self.access_sets_objects.keys():
            size, put, get, ingress, egress = self.spanstore_aggregate(requests, actions, access_set)
            print(f"Generate decisions for access set: {access_set}, Size: {size}, put: {put}, get: {get}, ingress: {ingress}, egress: {egress}")

            self.workload = self.oracle.create_workload_by_region_name(size=size, put=put, get=get, ingress=ingress, egress=egress)
            decisions = self.oracle.query(w=self.workload, translateOptSchemes=True)
            cost, decision = decisions[0]
            print(f"Decision: {decision}")
            assert len(decision.objectStores) == 1
            assert len(decision.assignments) == 1
            # NOTE: why would v be a set?
            place_regions = list(set(refine_string(r) for r in decision.objectStores[0]))
            app_assignments = {refine_string(k): refine_string(list(v)[0]) for k, v in decision.assignments[0].items()}

            # All objects in this access sets are placed in the same regions
            self.placement_decisions[access_set] = place_regions
            self.get_decisions = app_assignments
            costs += cost

        print("Placement decisions: ", self.placement_decisions)
        print("Get decisions: ", self.get_decisions)
        print("Cost: ", costs)

    def place(self, key: str) -> List[str]:
        # Find the access set this object belongs to
        access_set = self.access_sets[key]

        print(f"Access set for {key}: {access_set}")
        # Get the placement decisions for this access set
        place_regions = self.placement_decisions[access_set]
        print(f"Placement decisions for {key}: {place_regions}")
        return place_regions

    def read_transfer_path(self, req: Request) -> Tuple[str, nx.DiGraph]:
        # Get read location from optimizer decisions
        if len(self.get_decisions) == 0 or req.issue_region not in self.get_decisions:
            src = self.config.storage_region
        else:
            new_policy_decision = self.get_decisions[req.issue_region]
            if self.region_mgmt.has_object_in_region(new_policy_decision, req.obj_key):
                print(f"New policy decision, object has been transferred")
                src = new_policy_decision
            else:
                if len(self.past_get_decisions) == 0:
                    # If object not yet being written with the new policy
                    print(f"Use storage location, object not yet transferred")
                    src = self.config.storage_region
                else:
                    print(f"Use past policy decision, object not yet transferred")
                    src = self.past_get_decisions[req.issue_region]

            if self.region_mgmt.has_object_in_region(req.issue_region, req.obj_key):
                src = req.issue_region

        dst = req.issue_region

        print(f"Read transfer path for {req.obj_key}: {src} -> {dst}")
        assert req.obj_key in self.objects

        G = nx.DiGraph()
        G.add_edge(
            src,
            dst,
            obj_key=req.obj_key,
            size=req.size,
            num_partitions=1,
            partitions=[0],
            throughput=self.total_graph[src][dst]["throughput"],
            cost=self.total_graph[src][dst]["cost"],
        )
        return src, G

    def write_transfer_path(self, req: Request, dst: str) -> nx.DiGraph:
        G = nx.DiGraph()
        src = req.issue_region

        G.add_edge(
            src,
            dst,
            obj_key=req.obj_key,
            size=req.size,
            num_partitions=1,
            partitions=[0],
            throughput=self.total_graph[src][dst]["throughput"],
            cost=self.total_graph[src][dst]["cost"],
        )

        return G

from src.placement_policy.policy import PlacementPolicy
from src.model.config import Config
from src.model.request import Request
from src.model.object import LogicalObject, Status
from typing import Dict, Tuple, List, Set
import networkx as nx
from src.utils.helpers import refine_string
from skypie.api import create_oracle, OracleType
from collections import defaultdict
from src.utils.definitions import GB
from datetime import timedelta
import os


class Oracle(PlacementPolicy):
    def __init__(self, config: Config, total_graph: nx.DiGraph, objects: Dict[str, LogicalObject], trace_path: str, verbose: int = 1) -> None:
        self.config = config
        self.total_graph = total_graph
        self.objects = objects
        oracle_directory = os.path.join(os.path.dirname(os.path.realpath(__file__)), "example_oracle_aws-replication-factor-2")
        self.oracle = create_oracle(oracle_directory, OracleType.SKYPIE, verbose=verbose)

        self.workload = None
        self.placement_decisions = Dict[str, List[str]]
        self.get_decisions = Dict[str, str]
        super().__init__()

    def generate_decisions(self, request: Request, action: Dict[str, str]):
        # Generate decision per requests
        self.placement_decisions = {}
        self.get_decisions = {}

        put_counts = defaultdict(int)
        get_counts = defaultdict(int)
        ingress_counts = defaultdict(float)
        egress_counts = defaultdict(float)

        if request.op == "write":
            put_counts[request.issue_region] += 1
        elif request.op == "read":
            get_counts[request.issue_region] += 1

        if action["R/W"] == "write":
            ingress_counts[action["write_region"]] += action["size(GB)"] * GB
        elif action["R/W"] == "read":
            egress_counts[action["read_region"]] += action["size(GB)"] * GB

        for region in list(set(self.total_graph.nodes)):
            if region not in put_counts:
                put_counts[region] = 0
            if region not in get_counts:
                get_counts[region] = 0
            if region not in ingress_counts:
                ingress_counts[region] = 0
            if region not in egress_counts:
                egress_counts[region] = 0

        size, put, get, ingress, egress = self.aggregate_requests(requests, actions, access_set=set(self.total_graph.nodes))
        self.workload = self.oracle.create_workload_by_region_name(
            size=size, put=put_counts, get=get_counts, ingress=ingress_counts, egress=egress_counts
        )
        decisions = self.oracle.query(w=self.workload, translateOptSchemes=True)
        cost, decision = decisions[0]

        place_regions = decision.replication_scheme.object_stores
        app_assignments = {a.app: a.object_store for a in decision.replication_scheme.app_assignments}

        place_decision = {region: [refine_string(r) for r in place_regions] for region in set(self.total_graph.nodes)}
        get_decision = {refine_string(k): refine_string(v) for k, v in app_assignments.items()}
        print("Placement decisions: ", place_decision)
        print("Get decisions: ", get_decision)
        # Append to placement decisions
        self.placement_decisions.update(place_decision)
        self.get_decisions.update(get_decision)

    def place(self, key: str) -> List[str]:
        """
        Given an object key, return the placement decisions for this object

        Args:
            key (str): key of the object

        Returns:
            List[str]: list of regions to place this object
        """
        place_regions = self.placement_decisions[key]
        return place_regions

    def read_transfer_path(self, req: Request) -> Tuple[str, nx.DiGraph]:
        """
        Get the transfer path for a read request

        Args:
            req (Request): request to satisfy
            dst (str): destination region

        Returns:
            Tuple[str, nx.DiGraph]: source region and transfer path
        """
        # Get read location from optimizer decisions
        src = self.get_decisions[req.obj_key]
        dst = req.issue_region

        assert req.obj_key in self.objects
        assert self.objects[req.obj_key].physical_objects[src].status == Status.ready

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

    def write_transfer_path(self, req: Request, dsts: List[str]) -> nx.DiGraph:
        """
        Get the transfer path for a write request

        Args:
            req (Request): request to satisfy
            dsts (List[str]): list of destination regions

        Returns:
            nx.DiGraph: transfer path
        """
        src = req.issue_region

        G = nx.DiGraph()
        for dst in dsts:
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

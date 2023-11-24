import networkx as nx
from src.model.config import Config
from src.model.request import Request
from src.transfer_policy.policy import TransferPolicy
import networkx as nx
from typing import Dict, List, Tuple
from src.model.object import LogicalObject, Status
from src.utils.definitions import aws_instance_throughput_limit, azure_instance_throughput_limit, gcp_instance_throughput_limit


class ClosestTransfer(TransferPolicy):
    def __init__(self, config: Config, total_graph: nx.DiGraph, object_dict: Dict[str, LogicalObject]):
        super().__init__(config, total_graph, object_dict)

    def read_transfer_path(self, req: Request) -> Tuple[str, nx.DiGraph]:
        # Direct transfer: given a destination, select the source and transfer path
        dst = req.issue_region

        if req.obj_key in self.objects:
            region_list = [obj.location_tag for obj in self.objects[req.obj_key].physical_objects.values() if obj.status == Status.ready]
            src = max(region_list, key=lambda x: self.total_graph[x][dst]["throughput"])
        else:
            assert self.config.storage_region != ""
            src = self.config.storage_region

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

    def write_transfer_path(self, req: Request, dst: str) -> nx.DiGraph:
        # Direct transfer
        # TODO: scale down egress throughput
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

import networkx as nx
from src.model.config import Config
from src.model.object import LogicalObject, Status
from src.model.request import Request
from src.transfer_policy.policy import TransferPolicy
import networkx as nx
from typing import Dict, List, Tuple
from src.utils.definitions import aws_instance_throughput_limit, azure_instance_throughput_limit, gcp_instance_throughput_limit


class DirectTransfer(TransferPolicy):
    def __init__(self, config: Config, total_graph: nx.DiGraph, object_dict: Dict[str, LogicalObject]):
        super().__init__(config, total_graph, object_dict)

    def read_transfer_path(self, req: Request) -> Tuple[str, nx.DiGraph]:
        # Direct transfer: given a destination, select the source and transfer path
        G = nx.DiGraph()
        src = self.config.storage_region
        dst = req.issue_region

        # check to see of object from src region is ready
        assert req.obj_key in self.objects
        assert self.objects[req.obj_key].physical_objects[src].status == Status.ready

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

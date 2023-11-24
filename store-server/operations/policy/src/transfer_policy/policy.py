from src.model.config import Config
from src.model.object import LogicalObject
from src.model.request import Request
import networkx as nx

from typing import Dict, List, Tuple


class TransferPolicy:
    def __init__(self, config: Config, total_graph: nx.DiGraph, object_dict: Dict[str, LogicalObject], num_vms: int = 2):
        self.config = config
        self.total_graph = total_graph
        self.objects = object_dict
        self.num_vms = num_vms

    def read_transfer_path(self, req: Request) -> Tuple[str, nx.DiGraph]:
        """_summary_

        Args:
            req (Request): data request
            config (Config): config for system (e.g. SLOs and consistency requirements)

        Returns:
            str: source region to read from
            nx.DiGraph: transfer path
        """
        pass

    def write_transfer_path(self, req: Request, dst: str) -> nx.DiGraph:
        """_summary_

        Args:
            req (Request): data request
            config (Config): config for system (e.g. SLOs and consistency requirements)
            src (str): region issueing the write
            dst (str): single region to write to

        Returns:
            nx.DiGraph: transfer path
        """
        pass

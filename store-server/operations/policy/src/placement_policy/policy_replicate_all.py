from typing import List
from src.placement_policy.policy import PlacementPolicy
from src.model.config import Config
from src.model.request import Request
import networkx as nx


class ReplicateAll(PlacementPolicy):
    """
    Replicate all objects to all regions
    """

    def __init__(self, total_graph: nx.DiGraph) -> None:
        self.total_graph = total_graph
        super().__init__()

    def place(self, req: Request, config: Config) -> List[str]:
        if req.op == "write":
            return list(self.total_graph.nodes())
        else:
            return []

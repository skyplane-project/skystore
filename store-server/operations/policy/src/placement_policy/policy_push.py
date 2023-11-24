from typing import List
from src.model.region_mgmt import RegionManager
from src.placement_policy.policy import PlacementPolicy
from src.model.config import Config
from src.model.request import Request


class PushonWrite(PlacementPolicy):
    """
    Write local and push asynchronously to a set of pushed regions
    """

    def __init__(self, push_regions: List[str] = []) -> None:
        self.push_regions = push_regions
        super().__init__()

    def place(self, req: Request, config: Config = None) -> List[str]:
        return list(set(req.issue_region + self.push_regions))

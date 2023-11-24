from typing import List
from src.model.region_mgmt import RegionManager
from src.placement_policy.policy import PlacementPolicy
from src.model.config import Config
from src.model.request import Request


class PullOnRead(PlacementPolicy):
    """
    Write local, and pull on read if data is not available locally
    """

    def __init__(self, region_manager: RegionManager) -> None:
        self.region_manager = region_manager
        super().__init__()

    def place(self, req: Request, config: Config = None) -> List[str]:
        return [req.issue_region]

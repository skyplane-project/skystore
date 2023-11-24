from typing import List
from src.placement_policy.policy import PlacementPolicy
from src.model.config import Config
from src.model.request import Request


class SingleRegionWrite(PlacementPolicy):
    """
    Write to the same region as the original storage region defined in the config
    """

    def place(self, req: Request, config: Config) -> List[str]:
        if req.op == "write":
            return [config.storage_region]
        else:
            return []

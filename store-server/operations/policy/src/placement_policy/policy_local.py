from src.placement_policy.policy import PlacementPolicy
from src.model.config import Config
from src.model.request import Request
from typing import List


class LocalWrite(PlacementPolicy):
    """
    Write to local region
    """

    def place(self, req: Request, config: Config = None) -> List[str]:
        if req.op == "write":
            return [req.issue_region]
        else:
            return []

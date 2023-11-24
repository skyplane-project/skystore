from typing import List, Union

from pydantic import BaseModel
from src.model.config import Config
from src.model.request import Request


class PlacementPolicy:
    def place(self, req: Request, config: Config) -> List[str]:
        pass

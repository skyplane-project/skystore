from pydantic import BaseModel
from datetime import datetime
from typing import Literal


class Request(BaseModel):
    timestamp: datetime
    op: Literal["read", "write"]
    issue_region: str
    obj_key: str
    size: float

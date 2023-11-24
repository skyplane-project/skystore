from pydantic import BaseModel, validator
from typing import List, Dict


class LatencySLO(BaseModel):
    read: int  # ms
    write: int  # ms


class Config(BaseModel):
    storage_region: str
    placement_policy: str
    transfer_policy: str
    latency_slo: LatencySLO
    consistency: str

    # TODO: Add other validations?

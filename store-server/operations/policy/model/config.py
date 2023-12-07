from pydantic import BaseModel


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

from pydantic import BaseModel, Field
from typing import Optional


class PhysicalLocation(BaseModel):
    name: str

    cloud: str
    region: str
    bucket: str
    prefix: str = ""

    broadcast_to: list[str] = Field(default_factory=list)


class Configuration(BaseModel):
    physical_locations: list[PhysicalLocation] = Field(default_factory=list)

    def lookup(self, location_name: str) -> PhysicalLocation:
        for location in self.physical_locations:
            if location.name == location_name:
                return location
        raise ValueError(f"Unknown location: {location_name}")


TEST_CONFIGURATION = Configuration(
    physical_locations=[
        PhysicalLocation(
            name="aws:us-west-1",
            cloud="aws",
            region="us-west-1",
            bucket="my-bucket-1",
            prefix="my-prefix-1/",
            broadcast_to=["aws:us-east-2"],
        ),
        PhysicalLocation(
            name="aws:us-east-2",
            cloud="aws",
            region="us-east-2",
            bucket="my-bucket-2",
            prefix="my-prefix-2/",
        ),
        PhysicalLocation(
            name="gcp:us-central-3",
            cloud="gcp",
            region="us-central-3",
            bucket="my-bucket-3",
            prefix="my-prefix-3/",
        ),
    ]
)


DEMO_CONFIGURATION = Configuration(
    physical_locations=[
        PhysicalLocation(
            name="azure:westus3",
            cloud="azure",
            region="westus3",
            bucket="sky-s3-backend",
            prefix="demo-dry-run/",
            broadcast_to=["gcp:us-west1", "aws:us-west-2"],
        ),
        PhysicalLocation(
            name="gcp:us-west1",
            cloud="gcp",
            region="us-west1",
            bucket="sky-s3-backend",
            prefix="demo-dry-run/",
        ),
        PhysicalLocation(
            name="aws:us-west-2",
            cloud="aws",
            region="us-west-2",
            bucket="sky-s3-backend",
            prefix="demo-dry-run/",
        ),
    ]
)

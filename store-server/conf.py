from pydantic import BaseModel, Field


class PhysicalLocation(BaseModel):
    name: str

    cloud: str
    region: str
    bucket: str
    prefix: str = ""

    is_primary: bool = False
    need_warmup: bool = (
        False  # secondary region needs to be warmed up from primary region
    )


class Configuration(BaseModel):
    physical_locations: list[PhysicalLocation] = Field(default_factory=list)

    def lookup(self, location_name: str) -> PhysicalLocation:
        for location in self.physical_locations:
            if location.name == location_name:
                return location
        raise ValueError(f"Unknown location: {location_name}")


DEFAULT_INIT_REGIONS = ["aws:eu-central-1", "aws:us-west-1"]

TEST_CONFIGURATION = Configuration(
    physical_locations=[
        PhysicalLocation(
            name="aws:us-west-1",
            cloud="aws",
            region="us-west-1",
            bucket="my-bucket-1",
            prefix="my-prefix-1/",
            is_primary=True
            # broadcast_to=["aws:us-east-2"],
        ),
        PhysicalLocation(
            name="aws:us-east-2",
            cloud="aws",
            region="us-east-2",
            bucket="my-bucket-2",
            need_warmup=True,
            # prefix="my-prefix-2/",
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

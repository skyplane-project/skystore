from pydantic import BaseModel, Field


class PhysicalLocation(BaseModel):
    name: str

    cloud: str
    region: str
    bucket: str
    prefix: str = ""

    is_primary: bool = False
    need_warmup: bool = False  # warmup on write


class Configuration(BaseModel):
    bucket_name: str = "default-skybucket"
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
            bucket="my-sky-bucket-1",
            is_primary=True,
        ),
        PhysicalLocation(
            name="aws:us-east-2",
            cloud="aws",
            region="us-east-2",
            bucket="my-sky-bucket-2",
            need_warmup=True,
        ),
        PhysicalLocation(
            name="gcp:us-west1",
            cloud="gcp",
            region="us-west1",
            bucket="my-sky-bucket-3",
            need_warmup=True,
        ),
        PhysicalLocation(
            name="azure:westus3",
            cloud="azure",
            region="westus3",
            bucket="sky-s3-backend",
            prefix="demo-dry-run/",
            # broadcast_to=["gcp:us-west1", "aws:us-west-2"],
        ),
    ]
)

DEFAULT_INIT_REGIONS = [
    "aws:us-west-1",
    "aws:us-east-2",
    # "aws:us-west-2",
    # "gcp:us-west1",
    # "azure:westus3",
    # "aws:eu-central-1",
]

DEFAULT_SKYSTORE_BUCKET_PREFIX = "skystore"

DEMO_CONFIGURATION = Configuration(
    physical_locations=[
        PhysicalLocation(
            name="azure:westus3",
            cloud="azure",
            region="westus3",
            bucket="sky-s3-backend",
            prefix="demo-dry-run/",
            # broadcast_to=["gcp:us-west1", "aws:us-west-2"],
        ),
        PhysicalLocation(
            name="gcp:us-west1",
            cloud="gcp",
            region="us-west1",
            bucket="sky-s3-backend",
            prefix="demo-dry-run/",
            need_warmup=True,
        ),
        PhysicalLocation(
            name="aws:us-west-2",
            cloud="aws",
            region="us-west-2",
            bucket="sky-s3-backend",
            prefix="demo-dry-run/",
            need_warmup=True,
        ),
    ]
)

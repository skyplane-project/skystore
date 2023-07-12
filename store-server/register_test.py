from conf import TEST_CONFIGURATION
from starlette.testclient import TestClient
from app import app


client = TestClient(app)
resp = client.post(
    "/register_buckets",
    json={
        "bucket": TEST_CONFIGURATION.bucket_name,
        "config": {
            "physical_locations": [
                TEST_CONFIGURATION.physical_locations[i].dict()
                for i in range(len(TEST_CONFIGURATION.physical_locations))
            ]
        },
    },
)
resp.raise_for_status()

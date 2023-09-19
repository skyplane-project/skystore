import pytest
from starlette.testclient import TestClient
from app import app


@pytest.fixture
def client():
    with TestClient(app) as client:
        yield client


def test_delete_object(client):
    """Test that the `delete_object` endpoint functions correctly."""

    resp = client.post(
        "/start_create_bucket",
        json={
            "bucket": "my-delete-object-bucket",
            "client_from_region": "aws:us-west-1",
            "warmup_regions": ["gcp:us-west1"],
        },
    )
    resp.raise_for_status()

    # patch
    for physical_bucket in resp.json()["locators"]:
        resp = client.patch(
            "/complete_create_bucket",
            json={
                "id": physical_bucket["id"],
                "creation_date": "2020-01-01T00:00:00",
            },
        )
        resp.raise_for_status()

    resp = client.post(
        "/start_upload",
        json={
            "bucket": "my-delete-object-bucket",
            "key": "my-key",
            "client_from_region": "aws:us-west-1",
            "is_multipart": False,
        },
    )
    resp.raise_for_status()

    for physical_object in resp.json()["locators"]:
        client.patch(
            "/complete_upload",
            json={
                "id": physical_object["id"],
                "size": 100,
                "etag": "123",
                "last_modified": "2020-01-01T00:00:00",
            },
        ).raise_for_status()

    resp = client.post(
        "/list_objects",
        json={
            "bucket": "my-delete-object-bucket",
        },
    )
    assert resp.json() != []

    # delete object
    resp = client.post(
        "/start_delete_objects",
        json={
            "bucket": "my-delete-object-bucket",
            "keys": ["my-key"],
        },
    )

    for key, physical_objects in resp.json()["locators"].items():
        assert key == "my-key"

        for physical_object in physical_objects:
            resp = client.patch(
                "/complete_delete_objects",
                json={"ids": [physical_object["id"]]},
            )
            resp.raise_for_status()

    resp = client.post(
        "/list_objects",
        json={
            "bucket": "my-delete-object-bucket",
        },
    )
    assert resp.json() == []


def test_create_bucket(client):
    """Test that the `create_bucket` endpoint works."""
    resp = client.post(
        "/start_create_bucket",
        json={
            "bucket": "test-bucket-op",
            "client_from_region": "aws:us-west-1",
            "warmup_regions": ["gcp:us-west1"],
        },
    )
    resp.raise_for_status()

    # Double create: 409
    assert (
        client.post(
            "/start_create_bucket",
            json={
                "bucket": "test-bucket-op",
                "client_from_region": "aws:us-west-1",
                "warmup_regions": ["gcp:us-west1"],
            },
        ).status_code
        == 409
    )

    # patch
    for physical_bucket in resp.json()["locators"]:
        resp = client.patch(
            "/complete_create_bucket",
            json={
                "id": physical_bucket["id"],
                "creation_date": "2020-01-01T00:00:00",
            },
        )
        resp.raise_for_status()

    # list buckets
    resp = client.post("/list_buckets")
    target_bucket = {
        "bucket": "test-bucket-op",
        "creation_date": "2020-01-01T00:00:00",
    }
    assert target_bucket in resp.json()

    # delete bucket
    resp = client.post(
        "/start_delete_bucket",
        json={
            "bucket": "test-bucket-op",
        },
    )

    for physical_bucket in resp.json()["locators"]:
        resp = client.patch(
            "/complete_delete_bucket",
            json={"id": physical_bucket["id"]},
        )
        resp.raise_for_status()

    resp = client.post("/list_buckets")
    assert target_bucket not in resp.json()


def test_register_bucket(client):
    resp = client.post(
        "/register_buckets",
        json={
            "bucket": "test-bucket-register",
            "config": {
                "physical_locations": [
                    {
                        "name": "aws:us-west-1",
                        "cloud": "aws",
                        "region": "us-west-1",
                        "bucket": "my-bucket-1",
                        "prefix": "my-prefix-1/",
                        "is_primary": True,
                        "need_warmup": False,
                    },
                    {
                        "name": "aws:us-east-2",
                        "cloud": "aws",
                        "region": "us-east-2",
                        "bucket": "my-bucket-2",
                        "prefix": "my-prefix-2/",
                        "is_primary": False,
                        "need_warmup": False,
                    },
                    {
                        "name": "gcp:us-west1",
                        "cloud": "gcp",
                        "region": "us-west1",
                        "bucket": "my-bucket-3",
                        "prefix": "my-prefix-3/",
                        "is_primary": False,
                        "need_warmup": True,
                    },
                ]
            },
        },
    )
    resp.raise_for_status()

    # Upload to a primary location
    resp = client.post(
        "/start_upload",
        json={
            "bucket": "test-bucket-register",
            "key": "my-key",
            "client_from_region": "aws:us-west-1",
            "is_multipart": False,
        },
    )
    resp.raise_for_status()

    for physical_object in resp.json()["locators"]:
        client.patch(
            "/complete_upload",
            json={
                "id": physical_object["id"],
                "size": 100,
                "etag": "123",
                "last_modified": "2023-07-01T00:00:00",
            },
        ).raise_for_status()

    # Check if object can be located from primary region
    resp = client.post(
        "/locate_object",
        json={
            "bucket": "test-bucket-register",
            "key": "my-key",
            "client_from_region": "aws:us-west-1",
        },
    )
    resp.raise_for_status()
    resp_data = resp.json()
    assert resp_data["tag"] == "aws:us-west-1"
    assert resp_data["region"] == "us-west-1"

    # Check if object can be located from warmup region
    resp = client.post(
        "/locate_object",
        json={
            "bucket": "test-bucket-register",
            "key": "my-key",
            "client_from_region": "gcp:us-west1",
        },
    )
    resp.raise_for_status()
    resp_data = resp.json()
    assert resp_data["tag"] == "gcp:us-west1"
    assert resp_data["region"] == "us-west1"

    # Check if object can be located from a non-warmup region
    resp = client.post(
        "/locate_object",
        json={
            "bucket": "test-bucket-register",
            "key": "my-key",
            "client_from_region": "aws:us-east-2",
        },
    )
    resp.raise_for_status()
    resp_data = resp.json()
    assert resp_data["tag"] == "aws:us-west-1"
    assert resp_data["region"] == "us-west-1"

    resp = client.post(
        "/locate_object",
        json={
            "bucket": "test-bucket-register",
            "key": "my-key",
            "client_from_region": "aws:eu-central-1",
        },
    )
    resp.raise_for_status()
    resp_data = resp.json()
    assert resp_data["tag"] == "aws:us-west-1"
    assert resp_data["region"] == "us-west-1"


def test_delete_bucket(client):
    resp = client.post(
        "/start_create_bucket",
        json={"bucket": "my-bucket", "client_from_region": "aws:us-west-1"},
    )
    resp.raise_for_status()

    for physical_bucket in resp.json()["locators"]:
        resp = client.patch(
            "/complete_create_bucket",
            json={
                "id": physical_bucket["id"],
                "creation_date": "2020-01-01T00:00:00",
            },
        )
        resp.raise_for_status()

    resp = client.post(
        "/start_upload",
        json={
            "bucket": "my-bucket",
            "key": "my-key",
            "client_from_region": "aws:us-west-1",
            "is_multipart": False,
        },
    )
    resp.raise_for_status()

    for physical_object in resp.json()["locators"]:
        client.patch(
            "/complete_upload",
            json={
                "id": physical_object["id"],
                "size": 100,
                "etag": "123",
                "last_modified": "2020-01-01T00:00:00",
            },
        ).raise_for_status()

    assert (
        client.post(
            "/start_delete_bucket",
            json={
                "bucket": "my-bucket",
            },
        ).status_code
        == 409
    )


def test_get_object(client):
    """Test that the `get_object` endpoint returns the correct object."""

    resp = client.post(
        "/start_create_bucket",
        json={
            "bucket": "my-get-bucket",
            "client_from_region": "aws:us-west-1",
            "warmup_regions": ["gcp:us-west1"],
        },
    )
    resp.raise_for_status()

    # patch
    for physical_bucket in resp.json()["locators"]:
        resp = client.patch(
            "/complete_create_bucket",
            json={
                "id": physical_bucket["id"],
                "creation_date": "2020-01-01T00:00:00",
            },
        )
        resp.raise_for_status()

    resp = client.post(
        "/start_upload",
        json={
            "bucket": "my-get-bucket",
            "key": "my-key",
            "client_from_region": "aws:us-west-1",
            "is_multipart": False,
        },
    )
    resp.raise_for_status()

    for physical_object in resp.json()["locators"]:
        client.patch(
            "/complete_upload",
            json={
                "id": physical_object["id"],
                "size": 100,
                "etag": "123",
                "last_modified": "2020-01-01T00:00:00",
            },
        ).raise_for_status()

    resp = client.post(
        "/locate_object",
        json={
            "bucket": "my-get-bucket",
            "key": "my-key",
            "client_from_region": "aws:us-west-1",
        },
    )
    resp.raise_for_status()
    resp_data = resp.json()
    resp_data.pop("id")
    assert resp_data == {
        "tag": "aws:us-west-1",
        "bucket": "skystore-us-west-1",  # Bucket is prefixed with "skystore-"
        "key": "my-key",  # "my-get-bucket/my-key",  # Key is prefixed with logical bucket name
        "region": "us-west-1",
        "cloud": "aws",
        "size": 100,
        "etag": "123",
        "last_modified": "2020-01-01T00:00:00",
        "multipart_upload_id": None,
    }

    # 404
    assert (
        client.post(
            "/locate_object",
            json={
                "bucket": "my-get-bucket",
                "key": "non-existent-my-key",
                "client_from_region": "aws:us-west-2",
            },
        ).status_code
        == 404
    )

    # Read from a broadcasted location
    location = client.post(
        "/locate_object",
        json={
            "bucket": "my-get-bucket",
            "key": "my-key",
            "client_from_region": "gcp:us-west1",
        },
    ).json()["region"]
    assert location == "us-west1"

    # Remote Read
    location = client.post(
        "/locate_object",
        json={
            "bucket": "my-get-bucket",
            "key": "my-key",
            "client_from_region": "aws:eu-west-1",
        },
    ).json()["region"]
    assert location in {"us-west-1", "us-west1"}


def test_warmup(client):
    # init region in aws:us-west-1 and aws:us-east-2
    resp = client.post(
        "/start_create_bucket",
        json={"bucket": "my-warmup-bucket", "client_from_region": "aws:us-east-2"},
    )
    resp.raise_for_status()

    # patch
    for physical_bucket in resp.json()["locators"]:
        resp = client.patch(
            "/complete_create_bucket",
            json={
                "id": physical_bucket["id"],
                "creation_date": "2020-01-01T00:00:00",
            },
        )
        resp.raise_for_status()

    resp = client.post(
        "/start_upload",
        json={
            "bucket": "my-warmup-bucket",
            "key": "my-key-warmup",
            "client_from_region": "aws:us-east-2",
            "is_multipart": False,
        },
    )
    for locator in resp.json()["locators"]:
        client.patch(
            "/complete_upload",
            json={
                "id": locator["id"],
                "size": 100,
                "etag": "123",
                "last_modified": "2020-01-01T00:00:00.000Z",
            },
        ).raise_for_status()

    # warmup
    resp = client.post(
        "/start_warmup",
        json={
            "bucket": "my-warmup-bucket",
            "key": "my-key-warmup",
            "client_from_region": "aws:us-east-2",
            "warmup_regions": ["aws:us-west-1"],
        },
    )
    resp.raise_for_status()
    for locator in resp.json()["dst_locators"]:
        client.patch(
            "/complete_upload",
            json={
                "id": locator["id"],
                "size": 100,
                "etag": "123",
                "last_modified": "2020-01-01T00:00:00.000Z",
            },
        ).raise_for_status()

    # try locate object from warmup region
    resp = client.post(
        "/locate_object",
        json={
            "bucket": "my-warmup-bucket",
            "key": "my-key-warmup",
            "client_from_region": "aws:us-west-1",
        },
    )
    assert resp.json()["region"] == "us-west-1"


def test_write_back(client):
    resp = client.post(
        "/start_create_bucket",
        json={"bucket": "my-writeback-bucket", "client_from_region": "aws:us-east-2"},
    )
    resp.raise_for_status()

    # patch
    for physical_bucket in resp.json()["locators"]:
        resp = client.patch(
            "/complete_create_bucket",
            json={
                "id": physical_bucket["id"],
                "creation_date": "2020-01-01T00:00:00",
            },
        )
        resp.raise_for_status()

    resp = client.post(
        "/start_upload",
        json={
            "bucket": "my-writeback-bucket",
            "key": "my-key-write-back",
            "client_from_region": "aws:us-east-2",
            "is_multipart": False,
        },
    )
    for locator in resp.json()["locators"]:
        client.patch(
            "/complete_upload",
            json={
                "id": locator["id"],
                "size": 100,
                "etag": "123",
                "last_modified": "2020-01-01T00:00:00.000Z",
            },
        ).raise_for_status()

    # we should be able to get it from us-east-2 (Pull-based Policy)
    resp = client.post(
        "/locate_object",
        json={
            "bucket": "my-writeback-bucket",
            "key": "my-key-write-back",
            "client_from_region": "aws:us-west-1",
        },
    )
    assert resp.json()["region"] == "us-east-2"

    # Now write it to local store
    resp = client.post(
        "/start_upload",
        json={
            "bucket": "my-writeback-bucket",
            "key": "my-key-write-back",
            "client_from_region": "aws:us-west-1",
            "is_multipart": False,
        },
    )
    resp.raise_for_status()
    for locator in resp.json()["locators"]:
        client.patch(
            "/complete_upload",
            json={
                "id": locator["id"],
                "size": 100,
                "etag": "123",
                "last_modified": "2020-01-01T00:00:00.000Z",
            },
        ).raise_for_status()

    # we should get able to get it from us-west-1, now.
    resp = client.post(
        "/locate_object",
        json={
            "bucket": "my-writeback-bucket",
            "key": "my-key-write-back",
            "client_from_region": "aws:us-west-1",
        },
    )
    assert resp.json()["region"] == "us-west-1"


def test_list_objects(client):
    resp = client.post(
        "/start_create_bucket",
        json={
            "bucket": "my-list-bucket",
            "client_from_region": "aws:us-west-1",
        },
    )
    resp.raise_for_status()

    # patch
    for physical_bucket in resp.json()["locators"]:
        resp = client.patch(
            "/complete_create_bucket",
            json={
                "id": physical_bucket["id"],
                "creation_date": "2020-01-01T00:00:00",
            },
        )
        resp.raise_for_status()

    resp = client.post(
        "/start_upload",
        json={
            "bucket": "my-list-bucket",
            "key": "my-key-1",
            "client_from_region": "aws:us-west-1",
            "is_multipart": False,
        },
    )
    for locator in resp.json()["locators"]:
        client.patch(
            "/complete_upload",
            json={
                "id": locator["id"],
                "size": 100,
                "etag": "123",
                "last_modified": "2020-01-01T00:00:00.000Z",
            },
        ).raise_for_status()

    resp = client.post(
        "/list_objects",
        json={
            "bucket": "my-list-bucket",
            "prefix": "my-prefix-1/",
        },
    )
    assert resp.json() == []

    resp = client.post(
        "/list_objects",
        json={
            "bucket": "my-list-bucket",
            "prefix": "my-key",
        },
    )
    assert resp.json() == [
        {
            "bucket": "my-list-bucket",
            "key": "my-key-1",
            "size": 100,
            "etag": "123",
            "last_modified": "2020-01-01T00:00:00",
        }
    ]


def test_multipart_flow(client):
    """Test the a workflow for multipart upload works."""

    # Simulate CreateMultipartUpload. We create an multipart, get a logical id, return it to the client.
    # Also crated the actual multipart id and stuck them in database.
    resp = client.post(
        "/start_create_bucket",
        json={
            "bucket": "my-multipart-bucket",
            "client_from_region": "aws:us-west-1",
            "warmup_regions": ["gcp:us-west1"],
        },
    )
    resp.raise_for_status()

    # patch
    for physical_bucket in resp.json()["locators"]:
        resp = client.patch(
            "/complete_create_bucket",
            json={
                "id": physical_bucket["id"],
                "creation_date": "2020-01-01T00:00:00",
            },
        )
        resp.raise_for_status()

    resp = client.post(
        "/start_upload",
        json={
            "bucket": "my-multipart-bucket",
            "key": "my-key-multipart",
            "client_from_region": "aws:us-west-1",
            "is_multipart": True,
        },
    )
    resp.raise_for_status()
    resp_data = resp.json()
    multipart_upload_id = resp_data["multipart_upload_id"]
    assert multipart_upload_id is not None

    # generate the id
    for locator in resp_data["locators"]:
        client.patch(
            "/set_multipart_id",
            json={
                "id": locator["id"],
                "multipart_upload_id": f"{locator['tag']}-{multipart_upload_id}",
            },
        ).raise_for_status()
    ###

    # Simulate ListMultipartUploads
    resp = client.post(
        "/list_multipart_uploads",
        json={
            "bucket": "my-multipart-bucket",
            "prefix": "my-key-multi",
        },
    )
    resp.raise_for_status()
    resp_data = resp.json()
    assert resp_data == [
        {
            "bucket": "my-multipart-bucket",
            "key": "my-key-multipart",
            "upload_id": multipart_upload_id,
        }
    ]

    # Simulate UploadPart. We get the logical id, and upload the part to the actual location.
    resp = client.post(
        "/continue_upload",
        json={
            "bucket": "my-multipart-bucket",
            "key": "my-key-multipart",
            "client_from_region": "aws:us-west-1",
            "multipart_upload_id": multipart_upload_id,
        },
    )
    resp.raise_for_status()
    resp_data = resp.json()
    for locator in resp_data:
        assert (
            locator["multipart_upload_id"] == locator["tag"] + "-" + multipart_upload_id
        )

        client.patch(
            "/append_part",
            json={
                "id": locator["id"],
                "part_number": 1,
                "etag": "123",
                "size": 100,
            },
        ).raise_for_status()

    # Simulate ListParts
    resp = client.post(
        "/list_parts",
        json={
            "bucket": "my-multipart-bucket",
            "key": "my-key-multipart",
            "upload_id": multipart_upload_id,
        },
    )
    resp.raise_for_status()
    assert resp.json() == [
        {
            "part_number": 1,
            "etag": "123",
            "size": 100,
        }
    ]

    # Simulate CompleteMultipartUpload. We want to "sealed" it.
    for locator in resp_data:
        client.patch(
            "/complete_upload",
            json={
                "id": locator["id"],
                "size": 100,
                "etag": "123",
                "last_modified": "2020-01-01T00:00:00.000Z",
            },
        ).raise_for_status()

    # Now we should be able to locate it.
    resp = client.post(
        "/locate_object",
        json={
            "bucket": "my-multipart-bucket",
            "key": "my-key-multipart",
            "client_from_region": "aws:us-west-1",
        },
    )
    resp.raise_for_status()
    resp_data = resp.json()
    assert resp_data["region"] == "us-west-1"

import pytest
from starlette.testclient import TestClient
from app import app


@pytest.fixture
def client():
    with TestClient(app) as client:
        yield client


def test_get_object(client):
    """Test that the `get_object` endpoint returns the correct object."""

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

    resp = client.post(
        "/locate_object",
        json={
            "bucket": "my-bucket",
            "key": "my-key",
            "client_from_region": "aws:us-west-1",
        },
    )
    resp.raise_for_status()
    resp_data = resp.json()
    resp_data.pop("id")
    assert resp_data == {
        "tag": "aws:us-west-1",
        "bucket": "my-bucket-1",
        "key": "my-prefix-1/my-key",
        "region": "us-west-1",
        "cloud": "aws",
        "size": 100,
        "etag": "123",
        "last_modified": "2020-01-01T00:00:00",
    }

    # 404
    assert (
        client.post(
            "/locate_object",
            json={
                "bucket": "my-bucket",
                "key": "non-existent-my-key",
                "client_from_region": "us-west-2",
            },
        ).status_code
        == 404
    )

    # Read from a broadcasted location
    location = client.post(
        "/locate_object",
        json={
            "bucket": "my-bucket",
            "key": "my-key",
            "client_from_region": "aws:us-east-2",
        },
    ).json()["region"]
    assert location == "us-east-2"

    # Remote read
    location = client.post(
        "/locate_object",
        json={
            "bucket": "my-bucket",
            "key": "my-key",
            "client_from_region": "gcp:us-central-3",
        },
    ).json()["region"]
    assert location in {"us-west-1", "us-east-2"}


def test_write_back(client):
    resp = client.post(
        "/start_upload",
        json={
            "bucket": "my-bucket",
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

    # we should get able to get it from us-east-2
    resp = client.post(
        "/locate_object",
        json={
            "bucket": "my-bucket",
            "key": "my-key-write-back",
            "client_from_region": "aws:us-west-1",
        },
    )
    assert resp.json()["region"] == "us-east-2"

    # Now write it to local store
    resp = client.post(
        "/start_upload",
        json={
            "bucket": "my-bucket",
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
            "bucket": "my-bucket",
            "key": "my-key-write-back",
            "client_from_region": "aws:us-west-1",
        },
    )
    assert resp.json()["region"] == "us-west-1"


def test_list_objects(client):
    resp = client.post(
        "/start_upload",
        json={
            "bucket": "my-bucket-2",
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
            "bucket": "my-bucket-2",
            "prefix": "my-prefix-1",
        },
    )
    assert resp.json() == []

    resp = client.post(
        "/list_objects",
        json={
            "bucket": "my-bucket-2",
            "prefix": "my-key",
        },
    )
    assert resp.json() == [
        {
            "bucket": "my-bucket-2",
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
        "/start_upload",
        json={
            "bucket": "my-bucket",
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
            "bucket": "my-bucket",
            "prefix": "my-key-multi",
        },
    )
    resp.raise_for_status()
    resp_data = resp.json()
    assert resp_data == [
        {
            "bucket": "my-bucket",
            "key": "my-key-multipart",
            "upload_id": multipart_upload_id,
        }
    ]

    # Simulate UploadPart. We get the logical id, and upload the part to the actual location.
    resp = client.post(
        "/continue_upload",
        json={
            "bucket": "my-bucket",
            "key": "my-key-multipart",
            "client_from_region": "aws:us-west-1",
            "multipart_upload_id": multipart_upload_id,
        },
    )
    resp.raise_for_status()
    resp_data = resp.json()
    for locator in resp_data:
        assert locator["multipart_upload_id"] == locator["tag"] + "-" + multipart_upload_id

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
            "bucket": "my-bucket",
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
            "bucket": "my-bucket",
            "key": "my-key-multipart",
            "client_from_region": "aws:us-west-1",
        },
    )
    resp.raise_for_status()
    resp_data = resp.json()
    assert resp_data["region"] == "us-west-1"

def test_copy_flow():
    # TODO write the test for this.
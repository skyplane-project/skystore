import pytest
from starlette.testclient import TestClient
from app import app, rm_lock_on_timeout
import uuid 
import subprocess as sp

@pytest.fixture
def client():
    with TestClient(app) as client:
        yield client

# NOTE: Do not change the position of this test, it should be the first test
def test_remove_db(client):
    sp.run("rm skystore.db", shell=True)

def test_delete_objects(client):
    """Test that the `delete_object` endpoint functions correctly."""

    resp = client.post(
        "/start_create_bucket",
        json={
            "bucket": "my-delete-object-version-bucket",
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

    # enable bucket versioning
    resp = client.post(
        "/put_bucket_versioning",
        json={
            "bucket": "my-delete-object-version-bucket",
            "versioning": True,
        },
    )
    #resp.raise_for_status()

    # start uploading many objects with the same key
    resp1 = client.post(
        "/start_upload",
        json={
            "bucket": "my-delete-object-version-bucket",
            "key": "my-key",
            "client_from_region": "aws:us-west-1",
            "is_multipart": False,
            "policy": "push",
        },
    )
    resp1.raise_for_status()

    #print("resp.json() = ", resp1.json())

    # second object
    resp2 = client.post(
        "/start_upload",
        json={
            "bucket": "my-delete-object-version-bucket",
            "key": "my-key",
            "client_from_region": "aws:us-west-1",
            "is_multipart": False,
            "policy": "push",
        },
    )
    resp2.raise_for_status()

    # third object
    resp3 = client.post(
        "/start_upload",
        json={
            "bucket": "my-delete-object-version-bucket",
            "key": "my-key",
            "client_from_region": "aws:us-west-1",
            "is_multipart": False,
            "policy": "push",
        },
    )
    resp3.raise_for_status()

    #print("resp.json() = ", resp2.json())

    for i, physical_object in enumerate(resp1.json()["locators"]):
        client.patch(
            "/complete_upload",
            json={
                "id": physical_object["id"],
                "size": 100,
                "etag": "{}".format(uuid.uuid4()),
                "last_modified": f"2020-01-01T00:00:0{i}",
                "version_id": f"version-{i}",   # physical version id
            },
        ).raise_for_status()

    for i, physical_object in enumerate(resp2.json()["locators"]):
        client.patch(
            "/complete_upload",
            json={
                "id": physical_object["id"],
                "size": 100,
                "etag": "{}".format(uuid.uuid4()),
                "last_modified": f"2020-01-01T00:00:0{i + 1}",
                "version_id": f"version-{i + 10}",  # make sure version id is different
            },
        ).raise_for_status()

    for i, physical_object in enumerate(resp3.json()["locators"]):
        client.patch(
            "/complete_upload",
            json={
                "id": physical_object["id"],
                "size": 100,
                "etag": "{}".format(uuid.uuid4()),
                "last_modified": f"2020-01-01T00:00:0{i + 1}",
                "version_id": f"version-{i + 20}",  # make sure version id is different
            },
        ).raise_for_status()

    resp = client.post(
        "/list_objects_versioning",
        json={
            "bucket": "my-delete-object-version-bucket",
        },
    )
    print("resp.json() = ", resp.json())
    assert len(resp.json()) == 3
    # print("resp.json() = ", resp.json())

    # delete object with specific logical version set
    # should only delete the object with logical version 1
    resp1 = client.post(
        "/start_delete_objects",
        json={
            "bucket": "my-delete-object-version-bucket",
            "object_identifiers": {"my-key": list({"1"})},   # type 'set' is not json serializable, use List instead
        },
    )

    for key, physical_objects in resp1.json()["locators"].items():
        assert key == "my-key"

        for physical_object in physical_objects:
            resp = client.patch(
                "/complete_delete_objects",
                json={"ids": [physical_object["id"]]},
            )
            resp1.raise_for_status()

    resp = client.post(
        "/list_objects_versioning",
        json={
            "bucket": "my-delete-object-version-bucket",
        },
    )  

    assert len(resp.json()) == 2

    # delete objects without setting version id explicitly
    # this op should cause all objects with the same key to be deleted
    resp2 = client.post(
        "/start_delete_objects",
        json={
            "bucket": "my-delete-object-version-bucket",
            "object_identifiers": {"my-key": list()},   # type set is not json serializable
        },
    )

    # print("resp.json() = ", resp2.json())

    for key, physical_objects in resp2.json()["locators"].items():
        assert key == "my-key"

        for physical_object in physical_objects:
            resp = client.patch(
                "/complete_delete_objects",
                json={"ids": [physical_object["id"]]},
            )
            resp2.raise_for_status()

    # print("resp.json() = ", resp2.json())

    resp = client.post(
        "/list_objects",
        json={
            "bucket": "my-delete-object-version-bucket",
        },
    )

    # test that all objects with the same key are deleted
    assert resp.json() == []


# def test_put_bucket_versioning(client):
#     resp = client.post(
#         "/register_buckets",
#         json={
#             "bucket": "test-version-bucket-register",
#             "config": {
#                 "physical_locations": [
#                     {
#                         "name": "aws:us-west-1",
#                         "cloud": "aws",
#                         "region": "us-west-1",
#                         "bucket": "my-version-bucket-1",
#                         "prefix": "my-prefix-1/",
#                         "is_primary": True,
#                         "need_warmup": False,
#                     },
#                     {
#                         "name": "aws:us-east-2",
#                         "cloud": "aws",
#                         "region": "us-east-2",
#                         "bucket": "my-version-bucket-2",
#                         "prefix": "my-prefix-2/",
#                         "is_primary": False,
#                         "need_warmup": False,
#                     },
#                     {
#                         "name": "gcp:us-west1",
#                         "cloud": "gcp",
#                         "region": "us-west1",
#                         "bucket": "my-version-bucket-3",
#                         "prefix": "my-prefix-3/",
#                         "is_primary": False,
#                         "need_warmup": True,
#                     },
#                 ]
#             },
#         },
#     )
#     resp.raise_for_status()

#     # put bucket versioning test
#     resp = client.post(
#         "/put_bucket_versioning",
#         json={
#             "bucket": "test-version-bucket-register",
#             "versioning": True,
#         },
#     )

#     print("resp.json() = ", resp.json())

def test_get_objects(client):
    """Test that the `get_object` endpoint returns the correct object."""

    resp = client.post(
        "/start_create_bucket",
        json={
            "bucket": "my-get-version-bucket",
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

    # enable bucket versioning
    resp = client.post(
        "/put_bucket_versioning",
        json={
            "bucket": "my-get-version-bucket",
            "versioning": True,
        },
    )

    resp = client.post(
        "/start_upload",
        json={
            "bucket": "my-get-version-bucket",
            "key": "my-key",
            "client_from_region": "aws:us-west-1",
            "is_multipart": False,
            "policy": "push",
        },
    )
    resp.raise_for_status()

    for i, physical_object in enumerate(resp.json()["locators"]):
        client.patch(
            "/complete_upload",
            json={
                "id": physical_object["id"],
                "size": 100,
                "etag": "123",
                "last_modified": "2020-01-01T00:00:00",
                "version_id": f"version-{i}",
            },
        ).raise_for_status()

    # multi version test
    resp = client.post(
        "/start_upload",
        json={
            "bucket": "my-get-version-bucket",
            "key": "my-key",
            "client_from_region": "aws:us-west-1",
            "is_multipart": False,
            "policy": "push",
        },
    )
    resp.raise_for_status()

    print("resp.json() = ", resp.json())

    for i, physical_object in enumerate(resp.json()["locators"]):
        print("physical_object = ", physical_object)
        client.patch(
            "/complete_upload",
            json={
                "id": physical_object["id"],
                "size": 100,
                "etag": "124",
                "last_modified": "2020-01-01T00:00:00",
                "version_id": f"version-{i + 10}",  # make sure version id is different
            },
        ).raise_for_status()

    # # this will locate the newest version of the object
    resp = client.post(
        "/locate_object",
        json={
            "bucket": "my-get-version-bucket",
            "key": "my-key",
            "client_from_region": "aws:us-west-1",
        },
    )
    resp.raise_for_status()
    resp_data = resp.json()

    assert resp_data["etag"] == "124"
    assert resp_data["version"] == 2    # should be the newest version
    assert resp_data["version_id"] == "version-11" or resp_data["version_id"] == "version-10"  

    # this should fetch the newest version of the object
    assert resp_data["tag"] == "aws:us-west-1" and resp_data["region"] == "us-west-1" \
            and resp_data["etag"] == "124" \
            and resp_data["last_modified"] == "2020-01-01T00:00:00" \
            and resp_data["size"] == 100 and resp_data["key"] == "my-key" \
            and resp_data["bucket"] == "skystore-us-west-1" and resp_data["cloud"] == "aws" \
            and resp_data["multipart_upload_id"] == None 

    # 404
    assert (
        client.post(
            "/locate_object",
            json={
                "bucket": "my-get-version-bucket",
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
            "bucket": "my-get-version-bucket",
            "key": "my-key",
            "client_from_region": "gcp:us-west1",
        },
    ).json()["region"]
    assert location == "us-west1"

    # Remote Read
    location = client.post(
        "/locate_object",
        json={
            "bucket": "my-get-version-bucket",
            "key": "my-key",
            "client_from_region": "aws:eu-west-1",
        },
    ).json()["region"]
    assert location in {"us-west-1", "us-west1"}    # use push policy, depend on which one is the first primary write region

    # Read a specific version
    location = client.post(
        "/locate_object",
        json={
            "bucket": "my-get-version-bucket",
            "key": "my-key",
            "client_from_region": "gcp:us-west1",
            "version_id": 1,
        },
    ).json()
    assert location["version"] == 1


def test_get_object_write_local_and_pull(client):
    """Test that the `get_object` endpoint works using write_local logic with pull-on-read."""

    resp = client.post(
        "/start_create_bucket",
        json={
            "bucket": "my-get-version-bucket-write_local",
            "client_from_region": "aws:us-west-1",
        },
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

    # enable bucket versioning
    resp = client.post(
        "/put_bucket_versioning",
        json={
            "bucket": "my-get-version-bucket-write_local",
            "versioning": True,
        },
    )

    # Start upload from another region, expected write local
    resp = client.post(
        "/start_upload",
        json={
            "bucket": "my-get-version-bucket-write_local",
            "key": "my-key-write_local",
            "client_from_region": "aws:us-east-1",
            "is_multipart": False,
            "policy": "write_local",  # write local policy
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
                "policy": "write_local",  # write local policy
            },
        ).raise_for_status()

    # Try reading from the client's region
    resp = client.post(
        "/locate_object",
        json={
            "bucket": "my-get-version-bucket-write_local",
            "key": "my-key-write_local",
            "client_from_region": "aws:us-east-1",
        },
    )
    resp.raise_for_status()
    resp_data = resp.json()
    resp_data.pop("id")
    resp_data.pop("version_id")
    assert resp_data == {
        "tag": "aws:us-east-1",
        "bucket": "skystore-us-east-1",
        "key": "my-key-write_local",
        "region": "us-east-1",
        "cloud": "aws",
        "size": 100,
        "etag": "123",
        "last_modified": "2020-01-01T00:00:00",
        "multipart_upload_id": None,
        "version": 3,   # NOTE: If you run this test separately, this version number will be different
    }

    # Try copy_on_read policy, first write to the primary region
    resp = client.post(
        "/start_upload",
        json={
            "bucket": "my-get-version-bucket-write_local",
            "key": "my-key-write_local",
            "client_from_region": "aws:us-west-1",
            "is_multipart": False,
            "policy": "write_local",  # write local policy
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
                "policy": "write_local",  # write local policy
            },
        ).raise_for_status()

    # upload again, now pull-on-read
    resp = client.post(
        "/start_upload",
        json={
            "bucket": "my-get-version-bucket-write_local",
            "key": "my-key-write_local",
            "client_from_region": "aws:us-east-1",
            "is_multipart": False,
            "policy": "copy_on_read",  # copy_on_read policy
            "version_id": 4,
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
                "policy": "copy_on_read",  # copy_on_read policy
            },
        ).raise_for_status()

    # there should still only be two logical objects version
    resp = client.post(
        "/list_objects_versioning",
        json={
            "bucket": "my-get-version-bucket-write_local",
        },
    )
    #print("resp.json() = ", resp.json())
    assert len(resp.json()) == 2


def test_warmup(client):
    # init region in aws:us-west-1 and aws:us-east-2
    resp = client.post(
        "/start_create_bucket",
        json={"bucket": "my-warmup-version-bucket", "client_from_region": "aws:us-east-2"},
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

    # enable bucket versioning
    resp = client.post(
        "/put_bucket_versioning",
        json={
            "bucket": "my-warmup-version-bucket",
            "versioning": True,
        },
    )

    # 1st version
    resp1 = client.post(
        "/start_upload",
        json={
            "bucket": "my-warmup-version-bucket",
            "key": "my-key-warmup",
            "client_from_region": "aws:us-east-2",
            "is_multipart": False,
            "policy": "push",
        },
    )
    # 2nd version
    resp2 = client.post(
        "/start_upload",
        json={
            "bucket": "my-warmup-version-bucket",
            "key": "my-key-warmup",
            "client_from_region": "aws:us-east-2",
            "is_multipart": False,
            "policy": "push",
        },
    )
    for locator in resp2.json()["locators"]:
        client.patch(
            "/complete_upload",
            json={
                "id": locator["id"],
                "size": 100,
                "etag": "123",
                "last_modified": "2020-01-01T00:00:00.000Z",
                #"version_id": "version-1"
            },
        ).raise_for_status()

    for locator in resp1.json()["locators"]:
        client.patch(
            "/complete_upload",
            json={
                "id": locator["id"],
                "size": 100,
                "etag": "123",
                "last_modified": "2020-01-01T00:00:00.000Z",
                #"version_id": "version-1"
            },
        ).raise_for_status()

    # warmup
    resp = client.post(
        "/start_warmup",
        json={
            "bucket": "my-warmup-version-bucket",
            "key": "my-key-warmup",
            "client_from_region": "aws:us-east-2",
            "warmup_regions": ["aws:us-west-1"],
            "version_id": 5,    #NOTE: If you run this test separately, this version number will be different
        },
    )
    resp.raise_for_status()
    #print("resp.json() = ", resp.json())
    for i, locator in enumerate(resp.json()["dst_locators"]):
        client.patch(
            "/complete_upload",
            json={
                "id": locator["id"],
                "size": 100,
                "etag": "123",
                "last_modified": "2020-01-01T00:00:00.000Z",
                "version_id": f"version-{i}",
            },
        ).raise_for_status()

    # try locate object from warmup region
    resp = client.post(
        "/locate_object",
        json={
            "bucket": "my-warmup-version-bucket",
            "key": "my-key-warmup",
            "client_from_region": "aws:us-west-1",
            "version_id": 5,    # should be able to locate this version from warmup region
        },
    )
    assert resp.json()["region"] == "us-west-1"
    # check we have fetched the specific version
    assert resp.json()["version"] == 5

    # now it should have two logical objects version still (warmup should not create new versions)
    resp = client.post(
        "/list_objects_versioning",
        json={
            "bucket": "my-warmup-version-bucket",
        }
    )

    assert len(resp.json()) == 2


def test_write_back(client):
    resp = client.post(
        "/start_create_bucket",
        json={"bucket": "my-writeback-version-bucket", "client_from_region": "aws:us-east-2"},
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

    # enable bucket versioning
    resp = client.post(
        "/put_bucket_versioning",
        json={
            "bucket": "my-writeback-version-bucket",
            "versioning": True,
        },
    )

    resp = client.post(
        "/start_upload",
        json={
            "bucket": "my-writeback-version-bucket",
            "key": "my-key-write-back",
            "client_from_region": "aws:us-east-2",
            "is_multipart": False,
            "policy": "push",
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
            "bucket": "my-writeback-version-bucket",
            "key": "my-key-write-back",
            "client_from_region": "aws:us-west-1",
        },
    )
    assert resp.json()["region"] == "us-east-2"

    # Now write it to local store
    resp = client.post(
        "/start_upload",
        json={
            "bucket": "my-writeback-version-bucket",
            "key": "my-key-write-back",
            "client_from_region": "aws:us-west-1",
            "is_multipart": False,
            "policy": "copy_on_read",   # since we enable versioning, we should make sure we use copy_on_read policy now
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
            "bucket": "my-writeback-version-bucket",
            "key": "my-key-write-back",
            "client_from_region": "aws:us-west-1",
        },
    )
    assert resp.json()["region"] == "us-west-1"

# when we have multiple versions of the same object, we should be able to locate the newest logical version
def test_list_objects(client):
    resp = client.post(
        "/start_create_bucket",
        json={
            "bucket": "my-list-version-bucket",
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

    # enable bucket versioning
    resp = client.post(
        "/put_bucket_versioning",
        json={
            "bucket": "my-list-version-bucket",
            "versioning": True,
        },
    )

    resp = client.post(
        "/start_upload",
        json={
            "bucket": "my-list-version-bucket",
            "key": "my-key-1",
            "client_from_region": "aws:us-west-1",
            "is_multipart": False,
            "policy": "push",
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
        "/start_upload",
        json={
            "bucket": "my-list-version-bucket",
            "key": "my-key-1",
            "client_from_region": "aws:us-west-1",
            "is_multipart": False,
            "policy": "push",
        },
    )
    for locator in resp.json()["locators"]:
        client.patch(
            "/complete_upload",
            json={
                "id": locator["id"],
                "size": 100,
                "etag": "124",  # different from the previous version
                "last_modified": "2020-01-01T00:00:00.000Z",
            },
        ).raise_for_status()

    resp = client.post(
        "/list_objects",
        json={
            "bucket": "my-list-version-bucket",
            "prefix": "my-prefix-1/",
        },
    )
    assert resp.json() == []

    resp = client.post(
        "/list_objects",
        json={
            "bucket": "my-list-version-bucket",
            "prefix": "my-key",
        },
    )

    assert resp.json() == [
        {
            "bucket": "my-list-version-bucket",
            "key": "my-key-1",
            "size": 100,
            "etag": "124",  # should not be 123
            "last_modified": "2020-01-01T00:00:00",
            'version_id': None,
        }
    ]


def test_multipart_flow(client):
    """Test the a workflow for multipart upload works."""

    # Simulate CreateMultipartUpload. We create an multipart, get a logical id, return it to the client.
    # Also crated the actual multipart id and stuck them in database.
    resp = client.post(
        "/start_create_bucket",
        json={
            "bucket": "my-multipart-version-bucket",
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

    # enable bucket versioning
    resp = client.post(
        "/put_bucket_versioning",
        json={
            "bucket": "my-multipart-version-bucket",
            "versioning": True,
        },
    )

    resp = client.post(
        "/start_upload",
        json={
            "bucket": "my-multipart-version-bucket",
            "key": "my-key-multipart",
            "client_from_region": "aws:us-west-1",
            "is_multipart": True,
            "policy": "push",
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
            "bucket": "my-multipart-version-bucket",
            "prefix": "my-key-multi",
        },
    )
    resp.raise_for_status()
    resp_data = resp.json()
    assert resp_data == [
        {
            "bucket": "my-multipart-version-bucket",
            "key": "my-key-multipart",
            "upload_id": multipart_upload_id,
        }
    ]

    # Simulate UploadPart. We get the logical id, and upload the part to the actual location.
    resp = client.post(
        "/continue_upload",
        json={
            "bucket": "my-multipart-version-bucket",
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
            "bucket": "my-multipart-version-bucket",
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

    # we should be able to preform multiple complete-upload operations
    for locator in resp_data:
        client.patch(
            "/complete_upload",
            json={
                "id": locator["id"],
                "size": 100,
                "etag": "124",  # different from the previous version
                "last_modified": "2020-01-01T00:00:00.000Z",
            },
        ).raise_for_status()

    # Now we should be able to locate it.
    resp = client.post(
        "/locate_object",
        json={
            "bucket": "my-multipart-version-bucket",
            "key": "my-key-multipart",
            "client_from_region": "aws:us-west-1",
        },
    )
    resp.raise_for_status()
    resp_data = resp.json()
    assert resp_data["region"] == "us-west-1"
    # should be the newset version


@pytest.mark.asyncio
async def test_metadata_clean_up(client):
    """Test that the background process in `complete_create_bucket` endpoint functions correctly."""
    resp = client.post(
        "/start_create_bucket",
        json={
            "bucket": "temp-object-version-bucket",
            "client_from_region": "aws:us-west-1",
            "warmup_regions": ["gcp:us-west1"],
        },
    )
    resp.raise_for_status()

    # set minutes to 0 just to prevent stalling and set testing to True. Will bypass initial wait
    await rm_lock_on_timeout(0, test=True)

    resp = client.post(
        "/locate_bucket_status",
        json={
            "bucket": "temp-object-version-bucket",
            "client_from_region": "aws:us-west-1",
        },
    )

    assert resp.json()["status"] == "ready"

    # enable bucket versioning
    resp = client.post(
        "/put_bucket_versioning",
        json={
            "bucket": "temp-object-version-bucket",
            "versioning": True,
        },
    )

    # 1st version
    resp = client.post(
        "/start_upload",
        json={
            "bucket": "temp-object-version-bucket",
            "key": "my-key",
            "client_from_region": "aws:us-west-1",
            "is_multipart": False,
        },
    )
    resp.raise_for_status()

    # 2nd version
    resp = client.post(
        "/start_upload",
        json={
            "bucket": "temp-object-version-bucket",
            "key": "my-key",
            "client_from_region": "aws:us-west-1",
            "is_multipart": False,
        },
    )
    resp.raise_for_status()

    # 3rd version    
    resp = client.post(
        "/start_upload",
        json={
            "bucket": "temp-object-version-bucket",
            "key": "my-key",
            "client_from_region": "aws:us-west-1",
            "is_multipart": False,
        },
    )
    resp.raise_for_status()

    # set minutes to 0 just to prevent stalling and set testing to True. Will bypass initial wait
    await rm_lock_on_timeout(0, test=True)

    resp = client.post(
        "/locate_object_status",
        json={
            "bucket": "temp-object-version-bucket",
            "key": "my-key",
            "client_from_region": "aws:us-west-1",
        },
    )
    # rm_lock_on_timeout should have reset all locks. So search should return 'ready'
    for obj in resp.json():
        assert obj["status"] == "ready"

def test_disable_bucket_versioning(client):
    """without bucket versioning, we should only have one logical object version
        and reject multiple upload requests
    """
    resp = client.post(
        "/start_create_bucket",
        json={
            "bucket": "my-version-bucket",
            "client_from_region": "aws:us-west-1",
            "warmup_regions": ["gcp:us-west1"],
        },
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
    
    # 1st upload
    resp = client.post(
        "/start_upload",
        json={
            "bucket": "my-version-bucket",
            "key": "my-key",
            "client_from_region": "aws:us-west-1",
            "is_multipart": False,
            "policy": "push",
        }
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
            }
        ).raise_for_status()

    # 2nd upload
    resp = client.post(
        "/start_upload",
        json={
            "bucket": "my-version-bucket",
            "key": "my-key",
            "client_from_region": "aws:us-west-1",
            "is_multipart": False,
            "policy": "push",
        }
    )
    # resp.raise_for_status()

    # check result of 2nd upload, shoule be error
    assert resp.status_code == 409

import json
import os
from typing import Dict, Union
import pytest
from skyplane.obj_store.object_store_interface import ObjectStoreInterface
from skyplane.skystore.api.skystore_client import SkyStoreClient
import uuid


@pytest.mark.skip(reason="Shared function")
def test_region(client_region):
    gateway_api_url = "http://localhost:8080"

    client = SkyStoreClient().sky_object_store(client_region, gateway_api_url)

    # create src and dst file names
    key = str(uuid.uuid4()).replace("-", "")
    src_filename = f"src_{key}"
    dst_filename = f"dst_{key}"

    provider = client_region.split(":")[0]
    if provider == "azure":
        # need both storage account and container
        bucket_name = str(uuid.uuid4()).replace("-", "")[:24] + "/" + str(uuid.uuid4()).replace("-", "")
    else:
        bucket_name = str(uuid.uuid4()).replace("-", "")
    file_size = 1024

    # create bucket
    client.create_bucket(bucket_name)
    assert client.bucket_exists(bucket_name), f"Bucket {bucket_name} does not exist"

    # # upload object
    with open(src_filename, "wb") as fout:
        fout.write(os.urandom(file_size))
    client.upload_object(src_filename, bucket_name, key)
    assert client.exists(bucket_name, key), f"Object {key} does not exist in bucket {bucket_name}"

    # list objects
    for object in client.list_objects(bucket_name):
        print("Listing Object: ", object)

    # download object
    client.download_object(bucket_name, key, dst_filename)
    assert (
        open(src_filename, "rb").read() == open(dst_filename, "rb").read()
    ), f"Downloaded file {dst_filename} does not match uploaded filez {src_filename}"

    # delete object
    assert client.bucket_exists(bucket_name), f"Bucket {bucket_name} does not exist"
    client.delete_object(bucket_name, key)

    # delete bucket
    client.delete_bucket(bucket_name)
    assert not client.bucket_exists(bucket_name), f"Bucket {bucket_name} still exists"

    # cleanup
    os.remove(src_filename)
    os.remove(dst_filename)


def test_aws_interface_client():
    test_region("aws:us-east-1")
    return True


test_aws_interface_client()

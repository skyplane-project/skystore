import pytest
from skyplane.skystore.api.skystore_client import SkyStoreClient
from skyplane.skystore.api.skystore_server_client import StorageServerClient
from concurrent.futures import ThreadPoolExecutor
import uuid
import os


@pytest.mark.skip(reason="Shared function")
def create_skystore_server(server_region):
    client = StorageServerClient()
    server_dp = client.dataplane(server_region, debug=True)
    server_dp.provision(spinner=True)

    instance = None
    assert len(server_dp.bound_nodes) == 1, "Server should have 1 node"
    for _, i in server_dp.bound_nodes.items():
        instance = i

    gateway_api_url = instance.gateway_api_url
    print("Gateway API URL: ", gateway_api_url)
    # TODO: still need to make sure that the dynamodb tables are deleted upon server termination
    return server_dp, gateway_api_url


def test_client(server_dp, client):
    # create src and dst file names
    key = str(uuid.uuid4()).replace("-", "")
    src_filename = f"src_{key}"
    dst_filename = f"dst_{key}"

    provider = client.region.split(":")[0]
    if provider == "azure":
        # need both storage account and container
        bucket_name = str(uuid.uuid4()).replace("-", "")[:24] + "/" + str(uuid.uuid4()).replace("-", "")
    else:
        bucket_name = str(uuid.uuid4()).replace("-", "")
    file_size = 1024

    try:
        # create bucket
        client.create_bucket(bucket_name)
        assert client.bucket_exists(bucket_name), f"Bucket {bucket_name} does not exist"

        # upload object
        with open(src_filename, "wb") as fout:
            fout.write(os.urandom(file_size))
        client.upload_object(src_filename, bucket_name, key, client.region)
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
        client.delete_object(bucket_name, key)

        # delete bucket
        client.delete_bucket(bucket_name)
        assert not client.bucket_exists(bucket_name), f"Bucket {bucket_name} still exists"
    except Exception as e:
        print(f"Exception: {e} occurred. Copying gateway logs...")
        server_dp.copy_gateway_logs()
    finally:
        # cleanup
        os.remove(os.path.join(os.getcwd(), src_filename))
        os.remove(os.path.join(os.getcwd(), dst_filename))


@pytest.mark.skip(reason="Shared function")
def test_region(server_region, client_regions):
    server_dp, gateway_api_url = create_skystore_server(server_region)

    with ThreadPoolExecutor(max_workers=2) as executor:
        for client_region in client_regions:
            # create client object
            client = SkyStoreClient().sky_object_store(client_region, gateway_api_url)
            executor.submit(test_client, server_dp=server_dp, client=client)

    server_dp.deprovision(spinner=True)


def test_aws_interface_client():
    server_region = "aws:us-east-1"
    client_regions = ["aws:us-east-1", "aws:us-west-2"]
    test_region(server_region, client_regions)
    return True


test_aws_interface_client()

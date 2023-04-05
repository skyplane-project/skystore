import pytest
from skyplane.skystore.api.skystore_server_client import StorageServerClient


@pytest.mark.skip(reason="Shared function")
def create_skystore_server(server_region):
    client = StorageServerClient()
    dp = client.dataplane(server_region)
    dp.provision(spinner=True)
    # TODO: still need to make sure that the dynamodb tables are deleted upon server termination
    dp.deprovision(spinner=True)


def test_aws_interface_server():
    create_skystore_server("aws:us-east-1")
    return True


test_aws_interface_server()

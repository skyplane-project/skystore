from skyplane.api.usage import get_clientid
from typing import TYPE_CHECKING, Optional


from skyplane.skystore.api.skystore_server_config import SkyStoreConfig
from skyplane.skystore.api.skystore_obj_store import SkyObjectStore

if TYPE_CHECKING:
    from skyplane.api.config import AWSConfig, AzureConfig, GCPConfig


class SkyStoreClient:
    """Client for initializing cloud provider configurations"""

    def __init__(
        self,
        aws_config: Optional["AWSConfig"] = None,
        azure_config: Optional["AzureConfig"] = None,
        gcp_config: Optional["GCPConfig"] = None,
        skystore_config: Optional[SkyStoreConfig] = None,
    ):
        self.clientid = get_clientid()
        self.aws_auth = aws_config.make_auth_provider() if aws_config else None
        self.azure_auth = azure_config.make_auth_provider() if azure_config else None
        self.gcp_auth = gcp_config.make_auth_provider() if gcp_config else None
        self.skystore_config = skystore_config if skystore_config else SkyStoreConfig()

    def sky_object_store(self, region: str, server_api_url: str):
        return SkyObjectStore(region, server_api_url)

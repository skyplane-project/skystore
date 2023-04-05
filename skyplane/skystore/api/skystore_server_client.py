from datetime import datetime
from pathlib import Path
import uuid
from typing import TYPE_CHECKING, Optional
from skyplane.api.provisioner import Provisioner

from skyplane.api.usage import get_clientid
from skyplane.skystore.api.skystore_server_dataplane import SkyStoreDataplane
from skyplane.skystore.utils.definitions import tmp_log_dir
from skyplane.skystore.api.skystore_server_config import SkyStoreConfig
from skyplane.utils import logger


if TYPE_CHECKING:
    from skyplane.api.config import AWSConfig, AzureConfig, GCPConfig


class StorageServerClient:
    """Creating a storage server client to initialize a storage server."""

    def __init__(
        self,
        aws_config: Optional["AWSConfig"] = None,
        azure_config: Optional["AzureConfig"] = None,
        gcp_config: Optional["GCPConfig"] = None,
        skystore_config: Optional[SkyStoreConfig] = None,
        log_dir: Optional[str] = None,
    ):
        self.clientid = get_clientid()
        self.aws_auth = aws_config.make_auth_provider() if aws_config else None
        self.azure_auth = azure_config.make_auth_provider() if azure_config else None
        self.gcp_auth = gcp_config.make_auth_provider() if gcp_config else None
        self.skystore_config = skystore_config if skystore_config else SkyStoreConfig()

        self.log_dir = (
            tmp_log_dir / "server_logs" / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}-{uuid.uuid4().hex[:8]}"
            if log_dir is None
            else Path(log_dir)
        )
        print("Server log dir: ", self.log_dir / "server.log")

        # set up logging
        self.log_dir.mkdir(parents=True, exist_ok=True)
        logger.open_log_file(self.log_dir / "server.log")

        self.provisioner = Provisioner(
            host_uuid=self.clientid,
            aws_auth=self.aws_auth,
            azure_auth=self.azure_auth,
            gcp_auth=self.gcp_auth,
        )

    # methods to create dataplane
    def dataplane(
        self,
        region: str,
        debug: bool = False,
    ) -> SkyStoreDataplane:
        """
        Create a dataplane to provision the storage server.
        """
        return SkyStoreDataplane(
            clientid=self.clientid, region=region, provisioner=self.provisioner, server_config=SkyStoreConfig, debug=debug
        )

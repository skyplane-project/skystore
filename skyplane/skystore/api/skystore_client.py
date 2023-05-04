from datetime import datetime
from pathlib import Path
import uuid
from typing import TYPE_CHECKING, Optional
import typer
from skyplane.api.provisioner import Provisioner

from skyplane.api.usage import get_clientid
from skyplane.skystore.api.skystore_dataplane import SkyStoreDataplane
from skyplane.skystore.utils.definitions import tmp_log_dir
from skyplane.skystore.api.skystore_gateway_config import SkyStoreGatewayConfig
from skyplane.skystore.api.skystore_obj_store import SkyObjectStore
from skyplane.utils import logger


if TYPE_CHECKING:
    from skyplane.api.config import AWSConfig, AzureConfig, GCPConfig


class SkyStoreClient:
    """Creating a storage server client to initialize a storage server in the cloud."""

    def __init__(
        self,
        region: str,
        aws_config: Optional["AWSConfig"] = None,
        azure_config: Optional["AzureConfig"] = None,
        gcp_config: Optional["GCPConfig"] = None,
        server_config: Optional[SkyStoreGatewayConfig] = None,
        log_dir: Optional[str] = None,
        debug: bool = True,
        gateway_url: Optional[str] = None,
    ):
        typer.secho(f"Creating a SkyStore client in {region}", fg="green")
        self.region = region
        self.client_id = get_clientid()
        self.aws_auth = aws_config.make_auth_provider() if aws_config else None
        self.azure_auth = azure_config.make_auth_provider() if azure_config else None
        self.gcp_auth = gcp_config.make_auth_provider() if gcp_config else None
        self.server_config = server_config if server_config else SkyStoreGatewayConfig()

        self.log_dir = (
            tmp_log_dir / "client_server_logs" / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}-{uuid.uuid4().hex[:8]}"
            if log_dir is None
            else Path(log_dir)
        )

        # set up logging
        self.log_dir.mkdir(parents=True, exist_ok=True)
        logger.open_log_file(self.log_dir / "client_server.log")
        logger.debug("Client server log dir: " + str(self.log_dir / "client_server.log"))

        # provision skystore server
        if gateway_url == "local":
            self.gateway_url = "http://localhost:8080"
        else:
            self.provisioner = Provisioner(
                aws_auth=self.aws_auth,
                azure_auth=self.azure_auth,
                gcp_auth=self.gcp_auth,
                host_uuid=self.client_id,
            )
            self.dp = SkyStoreDataplane(
                clientid=self.client_id, region=self.region, provisioner=self.provisioner, gateway_config=self.server_config, debug=debug
            )
            self.dp.provision(spinner=True)
            instance = None
            assert len(self.dp.bound_nodes) == 1, "Server should have 1 node"
            for _, i in self.dp.bound_nodes.items():
                instance = i
            self.gateway_url = instance.gateway_api_url

    def sky_obj_store(self) -> SkyObjectStore:
        # NOTE: just for testing
        return SkyObjectStore(self.region, self.gateway_url)

    def close(self):
        # self.dp.copy_gateway_logs()
        self.dp.deprovision(spinner=True)

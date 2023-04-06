import json
import os
import threading
from collections import defaultdict
from datetime import datetime
from functools import partial
from datetime import datetime
from skyplane.api.dataplane import DataplaneAutoDeprovision
from skyplane.planner.topology import ReplicationTopologyGateway
import typer
import urllib3
from typing import TYPE_CHECKING, Dict, List, Optional

from skyplane import compute
from skyplane.utils import logger
from skyplane.utils.definitions import gateway_docker_image
from skyplane.utils.fn import PathLike, do_parallel

from skyplane.skystore.api.skystore_gateway_config import SkyStoreConfig
from skyplane.skystore.utils.definitions import tmp_log_dir

if TYPE_CHECKING:
    from skyplane.api.provisioner import Provisioner


class SkyStoreDataplane:
    """A Dataplane for SkyStore used for launching the storage server VM."""

    def __init__(self, clientid: str, region: str, provisioner: "Provisioner", gateway_config: SkyStoreConfig, debug: bool = False):
        """
        :param clientid: the uuid of the local host to create the dataplane
        :type clientid: str
        :param region: the region to launch the server VM
        :type region: str
        :param provisioner: the provisioner to launch the VMs
        :type provisioner: Provisioner
        :param gateway_config: the configuration of the server to be launched
        :type gateway_config: SkyStoreConfig
        :param debug: whether to enable debug mode, defaults to False
        :type debug: bool, optional
        """
        self.clientid = clientid
        self.region = region
        self.provisioner = provisioner
        self.gateway_config = gateway_config 

        self.http_pool = urllib3.PoolManager(retries=urllib3.Retry(total=3))
        self.provisioning_lock = threading.Lock()
        self.provisioned = False
        
        # transfer logs
        self.skystore_server_dir = tmp_log_dir / "server_logs" / datetime.now().strftime("%Y%m%d_%H%M%S")
        self.skystore_server_dir.mkdir(exist_ok=True, parents=True)
        self.bound_nodes: Dict[ReplicationTopologyGateway, compute.Server] = {}
        self.node: ReplicationTopologyGateway = None

        self.debug = debug

    def _start_gateway(
        self,
        gateway_docker_image: str,
        gateway_server: compute.Server,
        gateway_log_dir: Optional[PathLike] = None,
        authorize_ssh_pub_key: Optional[str] = None,
    ):
        # start gateway
        if gateway_log_dir:
            gateway_server.init_log_files(gateway_log_dir)
        if authorize_ssh_pub_key:
            gateway_server.copy_public_key(authorize_ssh_pub_key)
        gateway_server.start_skystore_gateway(region=self.region, gateway_docker_image=gateway_docker_image)

    def provision(
        self,
        allow_firewall: bool = True,
        gateway_docker_image: str = os.environ.get("SKYPLANE_DOCKER_IMAGE", gateway_docker_image()),
        gateway_log_dir: Optional[PathLike] = None,
        authorize_ssh_pub_key: Optional[str] = None,
        max_jobs: int = 16,
        spinner: bool = False,
    ):
        """
        Provision the transfer gateways.

        :param allow_firewall: whether to apply firewall rules in the gatweway network (default: True)
        :type allow_firewall: bool
        :param gateway_docker_image: Docker image token in github
        :type gateway_docker_image: str
        :param gateway_log_dir: path to the log directory in the remote gatweways
        :type gateway_log_dir: PathLike
        :param authorize_ssh_pub_key: authorization ssh key to the remote gateways
        :type authorize_ssh_pub_key: str
        :param max_jobs: maximum number of provision jobs to launch concurrently (default: 16)
        :type max_jobs: int
        :param spinner: whether to show the spinner during the job (default: False)
        :type spinner: bool
        """
        with self.provisioning_lock:
            if self.provisioned:
                logger.error("Cannot provision dataplane, already provisioned!")
                return

            is_aws_used, is_azure_used, is_gcp_used = False, False, False
            if self.region.startswith("aws"):
                is_aws_used = True
            elif self.region.startswith("azure"):
                is_azure_used = True
            elif self.region.startswith("gcp"):
                is_gcp_used = True
            else:
                raise ValueError(f"Invalid region: {self.region}")

            # create VMs from the topology
            cloud_provider, region = self.region.split(":")
            self.provisioner.add_task(
                cloud_provider=cloud_provider,
                region=region,
                vm_type=getattr(self.gateway_config, f"{cloud_provider}_instance_class"),
                spot=getattr(self.gateway_config, f"{cloud_provider}_use_spot_instances"),
                autoterminate_minutes=self.gateway_config.autoterminate_minutes,
            )

            # initialize clouds
            self.provisioner.init_global(aws=is_aws_used, azure=is_azure_used, gcp=is_gcp_used)

            # provision VMs
            uuids = self.provisioner.provision(
                authorize_firewall=allow_firewall,  # TODO: might need to change this later for multiple cloud providers
                max_jobs=max_jobs,
                spinner=spinner,
            )

            # create a ReplicationTopologyGateway for this VM
            self.node = ReplicationTopologyGateway(self.region, instance=0)

            # bind VMs to nodes
            servers = [self.provisioner.get_node(u) for u in uuids]
            servers_by_region = defaultdict(list)
            for s in servers:
                servers_by_region[s.region_tag].append(s)
            instance = servers_by_region[self.region].pop()
            self.bound_nodes[self.node] = instance
            logger.fs.debug(f"[Dataplane.provision] bound_nodes = {self.bound_nodes}")
            gateway_bound_nodes = self.bound_nodes.copy()

            # start gateways
            self.provisioned = True

        # todo: move server.py:start_gateway here
        logger.fs.info(f"Using docker image {gateway_docker_image}")

        jobs = []
        for _, server in gateway_bound_nodes.items():
            jobs.append(partial(self._start_gateway, gateway_docker_image, server, gateway_log_dir, authorize_ssh_pub_key))
        logger.fs.debug(f"[Dataplane.provision] Starting gateways on {len(jobs)} servers")
        do_parallel(lambda fn: fn(), jobs, n=-1, spinner=spinner, spinner_persist=spinner, desc="Starting gateway container on VMs")

    def deprovision(self, max_jobs: int = 64, spinner: bool = False):
        """
        Deprovision the remote gateways

        :param max_jobs: maximum number of jobs to deprovision the remote gateways (default: 64)
        :type max_jobs: int
        :param spinner: Whether to show the spinner during the job (default: False)
        :type spinner: bool
        """
        with self.provisioning_lock:
            if self.debug:
                logger.fs.info(f"Copying gateway logs to {self.skystore_server_dir}")
                self.copy_gateway_logs()

            if not self.provisioned:
                logger.fs.warning("Attempting to deprovision dataplane that is not provisioned, this may be from auto_deprovision.")

            self.provisioner.deprovision(
                max_jobs=max_jobs,
                spinner=spinner,
            )
            self.provisioned = False

    def copy_gateway_logs(self):
        # copy logs from all gateways in parallel
        def copy_log(instance):
            typer.secho(f"Downloading log: {self.skystore_server_dir}/gateway_{instance.uuid()}.stdout", fg="bright_black")
            typer.secho(f"Downloading log: {self.skystore_server_dir}/gateway_{instance.uuid()}.stderr", fg="bright_black")

            instance.run_command("sudo docker logs -t skyplane_gateway 2> /tmp/gateway.stderr > /tmp/gateway.stdout")
            instance.download_file("/tmp/gateway.stdout", self.skystore_server_dir / f"gateway_{instance.uuid()}.stdout")
            instance.download_file("/tmp/gateway.stderr", self.skystore_server_dir / f"gateway_{instance.uuid()}.stderr")

        do_parallel(copy_log, self.bound_nodes.values(), n=-1)

    def check_error_logs(self) -> Dict[str, List[str]]:
        """Get the error log from remote gateways if there is any error."""

        def get_error_logs(args):
            _, instance = args
            reply = self.http_pool.request("GET", f"{instance.gateway_api_url}/api/v1/errors")
            if reply.status != 200:
                raise Exception(f"Failed to get error logs from gateway instance {instance.instance_name()}: {reply.data.decode('utf-8')}")
            return json.loads(reply.data.decode("utf-8"))["errors"]

        errors: Dict[str, List[str]] = {}
        for (_, instance), result in do_parallel(get_error_logs, self.bound_nodes.items(), n=-1):
            errors[instance] = result
        return errors

    def auto_deprovision(self) -> DataplaneAutoDeprovision:
        """Returns a context manager that will automatically call deprovision upon exit."""
        return DataplaneAutoDeprovision(self)

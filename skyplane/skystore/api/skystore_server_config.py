from dataclasses import dataclass


@dataclass(frozen=True)
class SkyStoreConfig:
    autoterminate_minutes: int = 15
    requester_pays: bool = False

    # gateway settings
    use_socket_tls: bool = False

    # provisioning config
    aws_use_spot_instances: bool = False
    azure_use_spot_instances: bool = False
    gcp_use_spot_instances: bool = False
    aws_instance_class: str = "m5.xlarge"
    azure_instance_class: str = "Standard_D2_v5"  # TODO: check if this is the right one
    gcp_instance_class: str = "n2-standard-16"  # TODO: check if this is the right one
    gcp_use_premium_network: bool = True

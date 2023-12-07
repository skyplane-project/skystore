from typing import Tuple

KB = 1024
MB = 1024 * 1024
GB = 1024 * 1024 * 1024


# provider bandwidth limits (egress, ingress)
aws_instance_throughput_limit: Tuple[float, float] = (5, 10)
gcp_instance_throughput_limit: Tuple[float, float] = (7, 16)
azure_instance_throughput_limit: Tuple[float, float] = (16, 16)

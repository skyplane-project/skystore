import json
import typer
from typing import Dict, List
from skyplane import compute
from skyplane.cli.experiments.provision import provision
from skyplane.compute.const_cmds import make_sysctl_tcp_tuning_command
from skyplane.utils import logger
from skyplane.utils.fn import do_parallel

from skyplane.compute.aws.aws_auth import AWSAuthentication
import csv
import time
from datetime import datetime

all_aws_regions = compute.AWSCloudProvider.region_list()
all_azure_regions = compute.AzureCloudProvider.region_list()
all_gcp_regions = compute.GCPCloudProvider.region_list()
all_gcp_regions_standard = compute.GCPCloudProvider.region_list_standard()
all_ibmcloud_regions = compute.IBMCloudProvider.region_list()


def aws_credentials():
    auth = AWSAuthentication()
    access_key, secret_key = auth.get_credentials()
    return access_key, secret_key


def create_instance(
    aws_region_list=None,
    azure_region_list=None,
    gcp_region_list=None,
    gcp_standard_region_list=None,
    ibmcloud_region_list=None,
    enable_aws=True,
    enable_azure=False,
    enable_gcp=False,
    enable_gcp_standard=False,
    enable_ibmcloud=False,
    aws_instance_class="m5.8xlarge",
    azure_instance_class="Standard_D32_v5",
    gcp_instance_class="n2-standard-32",
    ibmcloud_instance_class="bx2-2x8",
):
    def check_stderr(tup):
        assert tup[1].strip() == "", f"Command failed, err: {tup[1]}"

    # validate AWS regions
    aws_region_list = aws_region_list if enable_aws else []
    azure_region_list = azure_region_list if enable_azure else []
    gcp_region_list = gcp_region_list if enable_gcp else []
    ibmcloud_region_list = ibmcloud_region_list if enable_ibmcloud else []
    if not enable_aws and not enable_azure and not enable_gcp and not enable_ibmcloud:
        logger.error("At least one of -aws, -azure, -gcp, -ibmcloud must be enabled.")
        raise typer.Abort()

    # validate AWS regions
    if not enable_aws:
        aws_region_list = []
    elif not all(r in all_aws_regions for r in aws_region_list):
        logger.error(f"Invalid AWS region list: {aws_region_list}")
        raise typer.Abort()

    # validate Azure regions
    if not enable_azure:
        azure_region_list = []
    elif not all(r in all_azure_regions for r in azure_region_list):
        logger.error(f"Invalid Azure region list: {azure_region_list}")
        raise typer.Abort()

    # validate GCP regions
    assert (
        not enable_gcp_standard or enable_gcp
    ), "GCP is disabled but GCP standard is enabled"
    if not enable_gcp:
        gcp_region_list = []
    elif not all(r in all_gcp_regions for r in gcp_region_list):
        logger.error(f"Invalid GCP region list: {gcp_region_list}")
        raise typer.Abort()

    # validate GCP standard instances
    if not enable_gcp_standard:
        gcp_standard_region_list = []
    if not all(r in all_gcp_regions_standard for r in gcp_standard_region_list):
        logger.error(f"Invalid GCP standard region list: {gcp_standard_region_list}")
        raise typer.Abort()

    # validate IBM Cloud regions
    if not enable_ibmcloud:
        ibmcloud_region_list = []
    elif not all(r in all_ibmcloud_regions for r in ibmcloud_region_list):
        logger.error(f"Invalid IBM Cloud region list: {ibmcloud_region_list}")
        raise typer.Abort()

    # provision servers
    aws = compute.AWSCloudProvider()
    azure = compute.AzureCloudProvider()
    gcp = compute.GCPCloudProvider()
    ibmcloud = compute.IBMCloudProvider()

    aws_instances, azure_instances, gcp_instances, ibmcloud_instances = provision(
        aws=aws,
        azure=azure,
        gcp=gcp,
        ibmcloud=ibmcloud,
        aws_regions_to_provision=aws_region_list,
        azure_regions_to_provision=azure_region_list,
        gcp_regions_to_provision=gcp_region_list,
        ibmcloud_regions_to_provision=ibmcloud_region_list,
        aws_instance_class=aws_instance_class,
        azure_instance_class=azure_instance_class,
        gcp_instance_class=gcp_instance_class,
        ibmcloud_instance_class=ibmcloud_instance_class,
        aws_instance_os="ubuntu",
        gcp_instance_os="ubuntu",
        gcp_use_premium_network=True,
    )
    # instance_list: List[compute.Server] = [i for ilist in aws_instances.values() for i in ilist]
    instances_dict: Dict[str, compute.Server] = {}

    # AWS instances
    for region, ilist in aws_instances.items():
        for instance in ilist:
            instances_dict[f"aws:{region}"] = instance

    # Azure instances
    for region, ilist in azure_instances.items():
        for instance in ilist:
            instances_dict[f"azure:{region}"] = instance

    # GCP instances
    for region, ilist in gcp_instances.items():
        for instance in ilist:
            instances_dict[f"gcp:{region}"] = instance

    print("instances_dict: ", instances_dict)
    with open("ssh_cmd.txt", "a") as f:
        for key, instance in instances_dict.items():
            print("instance: ", key)
            ssh_cmd = instance.get_ssh_cmd()
            print(ssh_cmd)
            ssh_parts = ssh_cmd.split(" ", 1)
            modified_ssh_cmd = (
                f"{ssh_parts[0]} -o StrictHostKeyChecking=accept-new {ssh_parts[1]}"
            )
            f.write(modified_ssh_cmd + "\n")

        f.close()

    # setup instances
    def setup(server: compute.Server):
        print("Setting up instance: ", server.region_tag)
        config_content = {
            "init_regions": [f"aws:{region}" for region in aws_region_list]
            + [f"azure:{region}" for region in azure_region_list]
            + [f"gcp:{region}" for region in gcp_region_list]
            + [f"ibmcloud:{region}" for region in ibmcloud_region_list],
            "client_from_region": server.region_tag,
            "skystore_bucket_prefix": "skystore",
            "put_policy": "replicate_all",
            "get_policy": "closest",
        }
        config_file_path = f"/tmp/init_config_{server.region_tag}.json"
        check_stderr(
            server.run_command(
                f"echo '{json.dumps(config_content)}' > {config_file_path}"
            )
        )

        check_stderr(
            server.run_command(
                "echo 'debconf debconf/frontend select Noninteractive' | sudo debconf-set-selections"
            )
        )
        server.run_command(
            "sudo apt remove python3-apt -y; sudo apt autoremove -y; \
            sudo apt autoclean; sudo apt install python3-apt -y; \
            (sudo apt-get update && sudo apt-get install python3-pip -y && sudo pip3 install awscli);\
            sudo apt install python3.9 python3-apt pkg-config libssl-dev -y\
            sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.9 1;\
            sudo update-alternatives --config python3"
        )

        server.run_command(make_sysctl_tcp_tuning_command(cc="cubic"))
        server.run_command(
            f"aws configure set aws_access_key_id {aws_credentials()[0]};\
            aws configure set aws_secret_access_key {aws_credentials()[1]}"
        )

        # Set up other stuff
        url = "https://github.com/shaopu1225/skystore.git"
        clone_cmd = f"git clone {url}; cd skystore; git switch experiment; "
        cmd1 = f"sudo apt remove python3-apt -y; sudo apt autoremove -y; \
                sudo apt autoclean; sudo apt install python3-apt -y; sudo apt-get update; \
                sudo apt install python3.9 -y; sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.9 1; \
                sudo update-alternatives --config python3; \
                sudo pip3 install awscli; \
                curl https://sh.rustup.rs -sSf | sh -s -- -y; source $HOME/.cargo/env;\
                {clone_cmd} "

        cmd2 = '/home/ubuntu/.cargo/bin/cargo install just --force; \
                sudo apt install pkg-config libssl-dev; \
                cd /home/ubuntu/skystore; \
                curl -sSL https://install.python-poetry.org | python3 -; \
                /home/ubuntu/.local/bin/poetry install; python3 -m pip install pip==23.2.1; \
                export PATH="/home/ubuntu/.local/bin:$PATH"; pip3 install -e .; cd store-server; \
                sudo apt-get install libpq-dev -y; sudo apt-get install python3.9-dev -y; \
                pip3 install -r requirements.txt; \
                cd ../s3-proxy; \
                  /home/ubuntu/.cargo/bin/cargo install --force \
                --git https://github.com/Nugine/s3s \
                --rev 0cc49cf24c05eeb6a809882d1a7b76e953822c0d \
                --bin s3s-fs \
                --features="binary" \
                s3s-fs; \
                cd ..; \
                skystore exit; '
        cmd3 = f"cd /home/ubuntu/skystore; \
                export AWS_ACCESS_KEY_ID={aws_credentials()[0]}; \
                export AWS_SECRET_ACCESS_KEY={aws_credentials()[1]}; \
                /home/ubuntu/.cargo/bin/cargo build --release; \
                nohup python3 send.py > send_output 2>&1 & \
                nohup /home/ubuntu/.local/bin/skystore init --config {config_file_path} > data_plane_output 2>&1 &"
        server.run_command(cmd1)
        server.run_command(cmd2)
        server.run_command(cmd3)

        # server.run_command(f"rm {config_file_path}")

    do_parallel(setup, list(instances_dict.values()), spinner=True, n=-1, desc="Setup")
    return instances_dict


def generate_file_on_server(server, size, filename):
    cmd = f"dd if=/dev/urandom of={filename} bs={size} count=1"
    server.run_command(cmd)


def extract_regions_from_trace(trace_file_path: str) -> Dict[str, List[str]]:
    regions = {
        "aws": set(),
        "azure": set(),
        "gcp": set(),
    }

    with open(trace_file_path, "r") as f:
        csv_reader = csv.reader(f)
        next(csv_reader)  # Skip the header
        for row in csv_reader:
            provider, region = row[2].split(":")
            regions[provider].add(region)

    # Convert sets to lists
    for provider in regions:
        regions[provider] = list(regions[provider])

    return regions


def issue_requests(trace_file_path: str):
    # Extract distinct cloud providers from the trace
    regions_dict = extract_regions_from_trace(trace_file_path)
    print("Extracted regions: ", regions_dict)

    enable_aws = len(regions_dict["aws"]) > 0
    enable_gcp = len(regions_dict["gcp"]) > 0
    enable_azure = len(regions_dict["azure"]) > 0
    print(
        f"enable_aws: {enable_aws}, enable_gcp: {enable_gcp}, enable_azure: {enable_azure}"
    )
    instances_dict = create_instance(
        aws_region_list=regions_dict.get("aws", []),
        azure_region_list=regions_dict.get("azure", []),
        gcp_region_list=regions_dict.get("gcp", []),
        enable_aws=enable_aws,
        enable_azure=enable_azure,
        enable_gcp=enable_gcp,
        enable_gcp_standard=enable_gcp,
        enable_ibmcloud=False,
        aws_instance_class="m5.8xlarge",
        azure_instance_class="Standard_D32_v5",
        gcp_instance_class="n2-standard-32",
    )

    print("Create instance finished.")

    # wait for the init background task to finish
    time.sleep(20)

    previous_timestamp = None
    s3_args = "--endpoint-url http://127.0.0.1:8002 --no-verify-ssl"  # get/put object requires signature

    with open(trace_file_path, "r") as f:
        csv_reader = csv.reader(f)
        next(csv_reader)  # Skip the header
        for row in csv_reader:
            timestamp_str, op, issue_region, data_id, size = row
            timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")

            if previous_timestamp:
                wait_time = (timestamp - previous_timestamp).total_seconds()
                time.sleep(wait_time)

            # Construct server key
            server_key = issue_region
            server = instances_dict.get(server_key)
            print("server: ", server)

            print("start time: ", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

            if server:
                if op == "write":
                    filename = f"{data_id}.data"
                    generate_file_on_server(server, size, filename)
                    print(
                        "generate file finish: ",
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    )
                    cmd = f"time aws s3api {s3_args} put-object --bucket default-skybucket --key {data_id} --body {filename}"
                elif op == "read":
                    cmd = f"time aws s3api {s3_args} get-object --bucket default-skybucket --key {data_id} {data_id}"

                print(
                    "execution finish: ", datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                )

                print(f"Executing command: {cmd}")
                stdout, stderr = server.run_command(cmd)
                print(f"stdout: {stdout}")
                print(f"stderr: {stderr}")
            else:
                print(f"No server found for region: {issue_region}")

            previous_timestamp = timestamp


if __name__ == "__main__":
    typer.run(issue_requests)

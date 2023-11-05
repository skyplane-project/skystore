import csv
import time
from datetime import datetime
from typing import Dict, List
from client import create_instance
import typer


def generate_file_on_server(server, size, filename):
    cmd = f"dd if=/dev/urandom of={filename} bs=1 count={size}"
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
    print(f"enable_aws: {enable_aws}, enable_gcp: {enable_gcp}, enable_azure: {enable_azure}")
    instances_dict = create_instance(
        aws_region_list=regions_dict.get("aws", []),
        azure_region_list=regions_dict.get("azure", []),
        gcp_region_list=regions_dict.get("gcp", []),
        enable_aws=enable_aws,
        enable_azure=enable_azure,
        enable_gcp=enable_gcp,
        enable_gcp_standard=enable_gcp, 
        enable_ibmcloud=False,
    )

    previous_timestamp = None
    s3_args = "--endpoint-url http://127.0.0.1:8002 --no-verify-ssl --no-sign-request"

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
            server_key = issue_region.replace(":", "")
            server = instances_dict.get(server_key)
            print("server: ", server)

            if True and server:
                if op == "write":
                    filename = f"{data_id}.data"
                    generate_file_on_server(server, size, filename)
                    cmd = f"aws s3api {s3_args} put-object --bucket default-skybucket --key {data_id} --body {filename}"
                elif op == "read":
                    cmd = f"aws s3api {s3_args} get-object --bucket default-skybucket --key {data_id} {data_id}"

                print(f"Executing command: {cmd}")
                server.run_command(cmd)
            else:
                print(f"No server found for region: {issue_region}")

            previous_timestamp = timestamp


if __name__ == "__main__":
    typer.run(issue_requests)

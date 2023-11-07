from typing import List
import typer
import json
import subprocess
import os
import time
import requests
from enum import Enum

app = typer.Typer(name="skystore")
env = os.environ.copy()

DEFAULT_SKY_S3_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "target/debug/sky-s3"
)

DEFAULT_STORE_SERVER_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "store-server"
)


class Policy(str, Enum):
    copy_on_read = "copy_on_read"
    read = "read"
    write_local = "write_local"
    push = "push"


@app.command()
def init(
    config_file: str = typer.Option(
        ..., "--config", help="Path to the init config file"
    ),
    start_server: bool = typer.Option(
        False, "--start-server", help="Whether to start the server on localhost or not"
    ),
    local_test: bool = typer.Option(
        False, "--local", help="Whether it is a local test or not"
    ),
    sky_s3_binary_path: str = typer.Option(
        DEFAULT_SKY_S3_PATH, "--sky-s3-path", help="Path to the sky-s3 binary"
    ),
    policy: Policy = typer.Option(
        Policy.write_local, "--policy", help="Policy to use for data placement"
    ),
):
    with open(config_file, "r") as f:
        config = json.load(f)

    init_regions_str = ",".join(config["init_regions"])
    skystore_bucket_prefix = (
        config["skystore_bucket_prefix"]
        if "skystore_bucket_prefix" in config
        else "skystore"
    )
    env = {
        **os.environ,
        "INIT_REGIONS": init_regions_str,
        "CLIENT_FROM_REGION": config["client_from_region"],
        "RUST_LOG": "INFO",
        "RUST_BACKTRACE": "full",
        "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID"),
        "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY"),
        "LOCAL": str(local_test).lower(),
        "LOCAL_SERVER": str(start_server).lower(),
        "POLICY": policy,
        "SKYSTORE_BUCKET_PREFIX": skystore_bucket_prefix,
    }
    env = {k: v for k, v in env.items() if v is not None}

    # Local test: start local s3
    if local_test:
        subprocess.check_call(["mkdir", "-p", "/tmp/s3-local-cache"], env=env)
        s3s_fs_command = (
            "RUST_LOG=s3s_fs=DEBUG s3s-fs --host localhost --port 8014"
            f" --access-key {env['AWS_ACCESS_KEY_ID']} "
            f"--secret-key {env['AWS_SECRET_ACCESS_KEY']} "
            "--domain-name localhost:8014 /tmp/s3-local-cache"
        )
        subprocess.Popen(
            [s3s_fs_command],
            shell=True,
            env=env,
        )

    # Start the skystore server
    if start_server:
        subprocess.Popen(
            f"cd {DEFAULT_STORE_SERVER_PATH}; "
            "rm skystore.db; python3 -m uvicorn app:app --reload --port 3000",
            shell=True,
            env=env,
        )

    time.sleep(2)

    # Start the s3-proxy
    if os.path.exists(sky_s3_binary_path):
        subprocess.Popen(
            sky_s3_binary_path,
            env=env,
        )
    else:
        subprocess.Popen(
            ["cargo", "run"],
            env=env,
        )
    typer.secho(f"SkyStore initialized at: {'http://127.0.0.1:8002'}", fg="green")


@app.command()
def register(
    register_config: str = typer.Option(
        ..., "--config", help="Path to the register config file"
    ),
    local_test: bool = typer.Option(
        False, "--local", help="Whether it is a local test or not"
    ),
):
    # read from LOCAL_SERVER environmental variable instead
    if local_test:
        server_addr = "localhost"
    else:
        # NOTE: ip address set to be the remote store-server addr
        server_addr = "54.183.123.82"

    try:
        with open(register_config, "r") as f:
            config = json.load(f)

        resp = requests.post(
            f"http://{server_addr}:3000/register_buckets",
            json={"bucket": config["bucket"], "config": config["config"]},
        )
        if resp.status_code == 200:
            typer.secho("Successfully registered.", fg="green")
        else:
            typer.secho(f"Registration failed: {resp.text}", fg="red")

    except requests.RequestException as e:
        typer.secho(f"Request error: {e}.", fg="red")


@app.command()
def exit():
    try:
        for port in [3000, 8002, 8014]:
            result = subprocess.run(
                [f"lsof -t -i:{port}"], shell=True, stdout=subprocess.PIPE
            )
            pids = result.stdout.decode("utf-8").strip().split("\n")

            for pid in pids:
                if pid:
                    subprocess.run([f"kill -15 {pid}"], shell=True)

            typer.secho(f"Stopped services running on port {port}.", fg="red")
    except FileNotFoundError:
        typer.secho("PID file not found. Cleaned up processes by port.", fg="yellow")
    except Exception as e:
        typer.secho(f"An error occurred during cleanup: {e}", fg="red")


@app.command()
def warmup(
    bucket: str = typer.Option(
        ..., "--bucket", help="Bucket name which contains the object to warmup"
    ),
    key: str = typer.Option(..., "--key", help="Key of object to warmup"),
    regions: List[str] = typer.Option(
        ..., "--regions", help="Region to warmup objects in"
    ),
):
    try:
        resp = requests.post(
            "http://127.0.0.1:8002/_/warmup_object",
            json={
                "bucket": bucket,
                "key": key,
                "warmup_regions": regions,
            },
        )
        if resp.status_code == 200:
            typer.secho(
                f"Warmup for bucket {bucket} and key {key} was successful.",
                fg="green",
            )
        else:
            typer.secho(f"Error during warmup: {resp.text}.", fg="red")
    except requests.RequestException as e:
        typer.secho(f"Request error: {e}.", fg="red")


def main():
    app()


if __name__ == "__main__":
    app()

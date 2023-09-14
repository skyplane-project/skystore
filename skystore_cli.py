from typing import List
from time import sleep 
import typer
import json
import subprocess
import os
import time
import requests

app = typer.Typer(name="skystore")
env = os.environ.copy()

@app.command()
def init(
    config_file: str = typer.Option(
        ..., "--config", help="Path to the init config file"
    ), 
    local_test: bool = typer.Option(
        False, "--local", help="Whether it is a local test or not"
    )
):
    with open(config_file, "r") as f:
        config = json.load(f)

    init_regions_str = ",".join(config["init_regions"])

    # Local test: start local s3 
    if local_test: 
        subprocess.Popen(
            ["just", "--justfile", "s3-proxy/justfile", "run-local-s3"],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    
    # Start the skystore server
    env["INIT_REGIONS"] = init_regions_str
    subprocess.Popen(
        ["just", "--justfile", "s3-proxy/justfile", "run-skystore-server"],
        env=env,
        # stdout=subprocess.DEVNULL,
        # stderr=subprocess.DEVNULL,
    )

    time.sleep(2)

    # Start the s3-proxy
    env["CLIENT_FROM_REGION"] = config["client_from_region"]
    env["RUST_LOG"] = "INFO"
    env["RUST_BACKTRACE"] = "full"
    subprocess.Popen(
        ["cargo", "run"],
        cwd="s3-proxy",
        env=env,
        # stdout=subprocess.DEVNULL,
        # stderr=subprocess.DEVNULL,
    )
    typer.secho(f"SkyStore initialized at: {'http://127.0.0.1:8002'}", fg="green")


@app.command()
def register(
    register_config: str = typer.Option(
        ..., "--config", help="Path to the register config file"
    )
):
    try:
        with open(register_config, "r") as f:
            config = json.load(f)

        resp = requests.post(
            "http://localhost:3000/register_buckets",
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
            "http://127.0.0.1:8002/warmup_object",
            json={
                "bucket": bucket,
                "key": key,
                "warmup_regions": regions,
            },
        )
        if resp.status_code == 200:  # Assuming 200 status code means success
            typer.secho(
                f"Warmup for bucket: {bucket} and key: {key} was successful.",
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

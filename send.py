from fastapi import FastAPI
import httpx
import json
import asyncio
import argparse

app = FastAPI()

"""
    This file will be run as a separate process and will read from the metrics.json file,
    and send the metrics to the store server periodically. When you try to establish the clients,
    you can include this script into your client script.
"""


@app.post("/send_message/")
async def send_message(message, host):
    async with httpx.AsyncClient() as client:
        response = await client.post(f"http://{host}:3000/update_metrics", json=message)
        return response.json()


# read from metrics.json line by line and pass to the store server using the above function
def read_metrics():
    metrics = []
    with open("metrics.json", "r") as f:
        # read the file line by line
        for line in f:
            metrics.append(json.loads(line))
    return metrics


async def main(server_addr):
    last_file_line = 0
    sleep_time = 0
    while True:
        await asyncio.sleep(30)
        metrics = read_metrics()
        if len(metrics) == last_file_line:
            sleep_time += 1
            continue
        print("new metrics", metrics[last_file_line:])
        for metric in metrics[last_file_line:]:
            await send_message(
                {
                    "timestamp": metric["timestamp"],
                    "latency": metric["latency"],
                    "request_region": metric["request_region"],
                    "destination_region": metric["destination_region"],
                    "key": metric["key"],
                    "size": metric["size"],
                    "op": metric["op"],
                },
                server_addr,
            )
        last_file_line = len(metrics)

# Initialize parser
parser = argparse.ArgumentParser()
 
# Adding optional argument
parser.add_argument("--server_addr", help = "ip address of the server")

args = parser.parse_args()

asyncio.run(main(args.server_addr))

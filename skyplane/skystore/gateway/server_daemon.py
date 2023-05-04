import argparse
from asyncio import Queue
import atexit
from multiprocessing import Event
import signal
import sys
import boto3
from skyplane.utils import logger
from skyplane.skystore.gateway.server_api import SkyStoreServerAPI
from skyplane.api.obj_store import ObjectStore


class SkyStoreServerDaemon:
    def __init__(
        self,
        region: str,
        object_table_name: str = "ObjectReplicaMappings",
        bucket_table_name: str = "BucketMappings",
        table_region: str = "us-east-1",
    ) -> None:
        self.region = region
        self.error_event = Event()
        self.error_queue = Queue()

        self.dynamodb = boto3.client("dynamodb", region_name=table_region)
        self.dynamodb_resource = boto3.resource("dynamodb", region_name=table_region)
        self.object_table = self.dynamodb_resource.Table(object_table_name)
        self.bucket_table = self.dynamodb_resource.Table(bucket_table_name)

        # API server
        self.api_server = SkyStoreServerAPI(
            self.region, self.dynamodb, self.object_table, self.bucket_table, ObjectStore(), self.error_event, self.error_queue
        )
        self.api_server.start()
        atexit.register(self.api_server.shutdown)
        logger.info(f"[skystore_daemon] API started at {self.api_server.url}")

    def run(self):
        exit_flag = Event()

        def exit_handler(signum, frame):
            logger.warning("[skystore_daemon] Received signal {}. Exiting...".format(signum))
            exit_flag.set()
            obj_response = self.dynamodb.delete_table(TableName=self.object_table_name)
            bucket_response = self.dynamodb.delete_table(TableName=self.bucket_table_name)
            print(f"Table {obj_response['TableDescription']['TableName']} and {bucket_response['TableDescription']['TableName']} deleted.")
            del self.dynamodb
            sys.exit(0)

        signal.signal(signal.SIGINT, exit_handler)
        signal.signal(signal.SIGTERM, exit_handler)

        logger.info("[skystore_daemon] Starting daemon loop")
        try:
            while not exit_flag.is_set() and not self.error_event.is_set():
                print("running skystore api server...")
                self.api_server.run()

        except Exception as e:
            self.error_queue.put(e)
            self.error_event.set()
            logger.error(f"[skystore_daemon] Exception in daemon: {e}")
            logger.exception(e)

        # shut down workers except for API to report status
        logger.info("[skystore_daemon] Done")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SkyStore Client Gateway Daemon")
    parser.add_argument("--region", type=str, required=True, help="Region tag (provider:region)")
    args = parser.parse_args()

    server = SkyStoreServerDaemon(region=args.region)
    server.run()

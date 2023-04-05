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
    def __init__(self, region: str) -> None:
        self.region = region
        self.dynamodb = boto3.resource("dynamodb", region_name=region.split(":")[1])

        self.read_capacity = 5
        self.write_capacity = 5
        self.object_table_name = "ObjectReplicaMappings"
        self.bucket_table_name = "BucketMappings"

        def delete_all_items(table):
            with table.batch_writer() as batch:
                items = table.scan()["Items"]
                key_attribute_names = [key_schema["AttributeName"] for key_schema in table.key_schema]
                for item in items:
                    key = {attr_name: item[attr_name] for attr_name in key_attribute_names}
                    batch.delete_item(Key=key)

        if self.object_table_name in [t.name for t in self.dynamodb.tables.all()]:
            print("Object table exists, deleting all items...")
            self.object_table = self.dynamodb.Table(self.object_table_name)
            delete_all_items(self.object_table)
        else:
            self.object_table = self.dynamodb.create_table(
                TableName=self.object_table_name,
                KeySchema=[{"AttributeName": "bucket_name", "KeyType": "HASH"}, {"AttributeName": "key", "KeyType": "RANGE"}],
                AttributeDefinitions=[
                    {"AttributeName": "bucket_name", "AttributeType": "S"},
                    {"AttributeName": "key", "AttributeType": "S"},
                ],
                ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
            )

        if self.bucket_table_name in [t.name for t in self.dynamodb.tables.all()]:
            print("Bucket table exists, deleting all items...")
            self.bucket_table = self.dynamodb.Table(self.bucket_table_name)
            delete_all_items(self.bucket_table)
        else:
            self.bucket_table = self.dynamodb.create_table(
                TableName=self.bucket_table_name,
                KeySchema=[{"AttributeName": "bucket_name", "KeyType": "HASH"}, {"AttributeName": "region_name", "KeyType": "RANGE"}],
                AttributeDefinitions=[
                    {"AttributeName": "bucket_name", "AttributeType": "S"},
                    {"AttributeName": "region_name", "AttributeType": "S"},
                ],
                ProvisionedThroughput={"ReadCapacityUnits": self.read_capacity, "WriteCapacityUnits": self.write_capacity},
            )

        self.object_table.wait_until_exists()
        self.bucket_table.wait_until_exists()
        self.error_event = Event()
        self.error_queue = Queue()

        # API server
        self.api_server = SkyStoreServerAPI(self.object_table, self.bucket_table, ObjectStore(), self.error_event, self.error_queue)
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
    parser = argparse.ArgumentParser(description="SkyStore Server Gateway Daemon")
    parser.add_argument("--region", type=str, required=True, help="Region tag (provider:region)")
    args = parser.parse_args()

    server = SkyStoreServerDaemon(region=args.region)
    server.run()

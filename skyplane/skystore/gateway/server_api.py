from datetime import datetime, timedelta
from functools import partial
import json
import logging
import logging.handlers
import os
from pathlib import Path
import random
import threading
from multiprocessing import Queue, Event
from queue import Empty
import time
from traceback import TracebackException
from botocore.exceptions import ClientError
import uuid
import boto3
import uvicorn

from typing import List, Optional
from skyplane.utils.definitions import MB

from fastapi import FastAPI, Response
from pydantic import BaseModel, Field
from skyplane.utils.path import parse_path
from skyplane.utils import logger
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Attr
from skyplane.utils.retry import retry_backoff


class PhysicalObjectLocator(BaseModel):
    """This is the physical location of an object. It is decomposed from AWS's virtual host style url: https://bucket-name.s3.region-code.amazonaws.com/key-name"""

    bucket: str = Field(..., description="The bucket name of the object.")
    region: str = Field(..., description="The region of the object.")
    key: str = Field(..., description="The key of the object.")


class SkyStoreServerAPI(threading.Thread):
    def __init__(
        self,
        region,
        dynamodb,
        object_table,
        bucket_table,
        objstore_client,
        error_event: Event,
        error_queue: Queue,
        host="0.0.0.0",
        port=8081,
    ):
        super().__init__()
        self.app = FastAPI()
        self.region = region

        self.dynamodb = dynamodb
        self.object_table = object_table
        self.bucket_table = bucket_table

        self.objstore_client = objstore_client
        self.error_event = error_event
        self.error_queue = error_queue
        self.error_list: List[TracebackException] = []
        self.error_list_lock = threading.Lock()

        # write directory
        self.file_dir = Path("/tmp/skyplane/chunks")
        self.file_dir.mkdir(parents=True, exist_ok=True)
        # delete existing chunks
        for chunk_file in self.file_dir.glob("*"):
            chunk_file.unlink()

        # part size for multipart upload / download
        self.part_size = 320 * MB

        # load routes
        self.register_global_routes(self.app)
        self.register_request_routes(self.app)
        self.register_error_routes(self.app)

        # make server
        self.host = host
        self.port = port
        self.url = "http://{}:{}".format(host, port)

        logging.getLogger("werkzeug").setLevel(logging.WARNING)
        self.config = uvicorn.Config(self.app, host=self.host, port=self.port)
        self.server = uvicorn.Server(self.config)

        self.cleanup_thread = threading.Thread(target=self.cleanup_expired_prepared_objects)
        self.cleanup_thread.start()

    def scan_table(self):
        # TEST: scanning table
        print("Scanning table....")
        response = self.object_table.scan()
        items = response["Items"]
        for item in items:
            print(item)

    def cleanup_expired_prepared_objects(self, expiration_time: Optional[float] = 60):
        EXPIRATION_PERIOD = timedelta(minutes=expiration_time / 60)

        while not self.server.should_exit:
            # Filter the objects that have a "PREPARE" status in the "urls" list
            all_objects = self.object_table.scan()["Items"]
            prepared_objects = [obj for obj in all_objects if any(url.get("obj_status") == "PREPARE" for url in obj.get("urls", []))]

            # Check if the object has been in the "PREPARE" status for too long
            for obj_item in prepared_objects:
                print("checking prepare object ", obj_item["object_key"], " in bucket ", obj_item["bucket_name"], " ...")
                for idx, url_item in enumerate(obj_item.get("urls", [])):
                    if url_item["obj_status"] == "PREPARE":
                        prepare_timestamp = url_item.get("timestamp")
                        if prepare_timestamp:
                            prepare_timestamp = datetime.strptime(prepare_timestamp, "%Y-%m-%dT%H:%M:%S.%f")
                            if datetime.now() - prepare_timestamp > EXPIRATION_PERIOD:
                                # Remove the URL entry if it has been in "PREPARE" status for too long
                                try:
                                    self.object_table.update_item(
                                        Key={"bucket_name": obj_item["bucket_name"], "object_key": obj_item["object_key"]},
                                        UpdateExpression=f"REMOVE urls[{idx}]",
                                        ConditionExpression=f"attribute_exists(urls[{idx}]) AND urls[{idx}].obj_status = :prepare",
                                        ExpressionAttributeValues={":prepare": "PREPARE"},
                                    )
                                except ClientError as e:
                                    error_code = e.response["Error"]["Code"]
                                    if error_code != "ConditionalCheckFailedException":
                                        print(f"Error removing the expired item {obj_item['object_key']} in {obj_item['bucket_name']}: {e}")
                                    raise e

                # Remove the whole object entry if the 'urls' field is an empty list
                obj_response = self.object_table.get_item(
                    Key={"bucket_name": obj_item["bucket_name"], "object_key": obj_item["object_key"]}
                )
                if "Item" in obj_response and not obj_response["Item"].get("urls", []):
                    try:
                        print("Clean up objects: ", obj_item)
                        self.object_table.delete_item(Key={"bucket_name": obj_item["bucket_name"], "object_key": obj_item["object_key"]})
                    except ClientError as e:
                        print(f"Error deleting the empty object entry {obj_item['object_key']} in {obj_item['bucket_name']}: {e}")
                        raise e

            # Sleep for a certain period before running the cleanup again
            time.sleep(60 * 5)

    def run(self):
        self.server.run()

    def shutdown(self):
        self.server.should_exit = True
        self.cleanup_thread.join()
        self.server.shutdown()

    def register_global_routes(self, app):
        # index route returns API version
        @app.get("/")
        async def get_index():
            return Response(status_code=200, content=json.dumps({"version": "v1"}))

        # index for v1 api routes, return all available routes as HTML page with links
        @app.get("/api/v1")
        async def get_v1_index():
            output = ""
            for rule in sorted(self.app.url_map.iter_rules(), key=lambda r: r.rule):
                if rule.endpoint != "static":
                    methods = set(m for m in rule.methods if m not in ["HEAD", "OPTIONS"])
                    output += f"<a href='{rule.rule}'>{rule.rule}</a>: {methods}<br>"
            return output

        # status route returns if API is up
        @app.get("/api/v1/status")
        async def get_status():
            return Response(status_code=200, content=json.dumps({"obj_status": "ok"}))

        # shutdown route
        @app.post("/api/v1/shutdown")
        async def shutdown():
            self.shutdown()
            logger.error("Shutdown complete. Hard exit.")
            os._exit(1)

    def register_request_routes(self, app):
        def get_provider_and_bucket(path: str):
            provider, bucket, path = parse_path(path)
            return provider, bucket

        def get_region_bucket(bucket_name: str, region: str):
            # Check DynamoDB if the bucket exists
            response = self.bucket_table.query(KeyConditionExpression=Key("bucket_name").eq(bucket_name))
            if "Items" not in response or len(response["Items"]) == 0:
                raise Exception(f"Bucket {bucket_name} does not exist.")

            # get the bucket info for the region
            bucket = next((item for item in response["Items"] if item["region_name"] == region), None)
            return bucket

        def create_bucket_helper(global_bucket_name: str, regions: List[str]):
            # create bucket in each region
            region_suffixes = {region: str(uuid.uuid4())[:8] for region in regions}
            bucket_names = {region: f"{global_bucket_name}-{region.split(':')[1]}-{region_suffixes[region]}" for region in regions}
            created_buckets, metadata_items = [], []

            def rollback(max_retries=4):
                for bucket_info, item in zip(created_buckets, metadata_items):
                    try:
                        retry_backoff(
                            partial(self.objstore_client.delete_bucket, bucket_info["bucket_name"], bucket_info["provider"]),
                            max_retries=max_retries,
                            exception_class=Exception,
                        )

                        retry_backoff(
                            partial(
                                self.bucket_table.delete_item, Key={"bucket_name": item["bucket_name"], "region_name": item["region_name"]}
                            ),
                            max_retries=max_retries,
                            exception_class=Exception,
                        )

                    except Exception as delete_error:
                        logger.error(f"Error during rollback: {str(delete_error)}")

            # Prepare transaction items
            transact_items = []
            try:
                for region, local_bucket_name in bucket_names.items():
                    new_item = {"bucket_name": global_bucket_name, "region_name": region, "bucket_url": None}
                    metadata_items.append(new_item)

                    # Update the metadata table with the new bucket, ensuring first-write-wins
                    self.bucket_table.put_item(
                        Item=new_item, ConditionExpression="attribute_not_exists(bucket_name) AND attribute_not_exists(region_name)"
                    )

                    # Create bucket in the region
                    try:
                        bucket_url = self.objstore_client.create_bucket(region, local_bucket_name)
                        created_buckets.append({"region": region, "bucket_name": local_bucket_name, "provider": region.split(":")[0]})
                    except Exception as e:
                        rollback()
                        raise e

                    # Add update_item transaction to the list
                    transact_items.append(
                        {
                            "Update": {
                                "TableName": self.bucket_table.name,
                                "Key": {"bucket_name": {"S": global_bucket_name}, "region_name": {"S": region}},
                                "UpdateExpression": "SET bucket_url = :url",
                                "ExpressionAttributeValues": {":url": {"S": bucket_url}},
                            }
                        }
                    )
            except ClientError as e:
                rollback()
                print("ClientError: ", e)
                if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                    return Response(status_code=409, content=f"Bucket {global_bucket_name} already exists")
                else:
                    return Response(status_code=500, content=f"Error creating {global_bucket_name}: {e}")

            # Execute the transaction to update all bucket_urls atomically
            self.dynamodb.transact_write_items(TransactItems=transact_items)
            return Response(status_code=200, content=f"Bucket {global_bucket_name} created successfully")

        @app.get("/api/v1/{bucket_name}")
        async def bucket_exists(bucket_name: str):
            try:
                response = self.bucket_table.query(KeyConditionExpression=boto3.dynamodb.conditions.Key("bucket_name").eq(bucket_name))
                if response["Count"] != 0:
                    return Response(status_code=200, content=f"Bucket {bucket_name} exists.")
                else:
                    return Response(status_code=404, content=f"Bucket {bucket_name} does not exist.")
            except ClientError as e:
                return Response(status_code=500, content=json.dumps({"error": str(e)}))

        @app.post("/api/v1/{bucket_name}")
        async def create_bucket(bucket_name: str, regions: List[str]):
            return create_bucket_helper(bucket_name, regions)

        @app.delete("/api/v1/{bucket_name}")
        async def delete_bucket(bucket_name: str):
            deleted_buckets, metadata_items = [], []

            def rollback(max_retries=4):
                for bucket_info, item in zip(deleted_buckets, metadata_items):
                    try:
                        retry_backoff(
                            partial(
                                self.objstore_client.create_bucket,
                                bucket_info["region"],
                                bucket_info["bucket_name"],
                            ),
                            max_retries=max_retries,
                        )
                        retry_backoff(
                            partial(
                                self.bucket_table.put_item,
                                item,
                            ),
                            max_retries=max_retries,
                        )
                    except Exception as delete_error:
                        logger.error(f"Error during rollback: {str(delete_error)}")

            try:
                # Prepare transaction items
                transact_items = []

                # Get all the related items from the metadata table
                response = self.bucket_table.query(KeyConditionExpression=Key("bucket_name").eq(bucket_name))
                for item in response["Items"]:
                    region_name = item["region_name"]

                    # Check if there are any objects in the bucket
                    obj_response = self.object_table.scan(FilterExpression=Attr("bucket_name").eq(bucket_name))
                    if obj_response["Count"] != 0:
                        return Response(status_code=400, content=f"Objects remained in bucket {bucket_name}. Remove the objects first.")

                    # Add delete_item transaction to the list
                    transact_items.append(
                        {
                            "Delete": {
                                "TableName": self.bucket_table.name,
                                "Key": {"bucket_name": {"S": bucket_name}, "region_name": {"S": region_name}},
                                "ConditionExpression": "attribute_exists(bucket_name) AND attribute_exists(region_name)",
                            }
                        }
                    )
                    # Store metadata for rollback
                    metadata_items.append(item)

                # Execute the transaction to delete all metadata atomically
                self.dynamodb.transact_write_items(TransactItems=transact_items)

                for item in metadata_items:
                    region_name = item["region_name"]
                    provider, actual_bucket_name = get_provider_and_bucket(item["bucket_url"])

                    try:
                        self.objstore_client.delete_bucket(actual_bucket_name, provider)
                        deleted_buckets.append({"region": region_name, "bucket_name": actual_bucket_name, "provider": provider})
                    except Exception as e:
                        rollback()
                        raise e

            except ClientError as e:
                rollback()
                if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                    return Response(status_code=404, content=f"Bucket {bucket_name} does not exist.")
                else:
                    return Response(status_code=500, content=f"Fail to delete bucket: {str(e)}")

            return Response(status_code=200, content=f"Bucket {bucket_name} deleted.")

        @app.get("/api/v1/list_objects/{bucket_name}/{prefix:path}")
        async def list_objects(bucket_name: str, prefix: Optional[str] = "") -> List[str]:
            try:
                response = self.bucket_table.query(KeyConditionExpression=boto3.dynamodb.conditions.Key("bucket_name").eq(bucket_name))
                if response["Count"] == 0:
                    return Response(status_code=404, content=f"Bucket {bucket_name} does not exist.")

                #  TODO: random choice for URL for now
                urls = [item["bucket_url"] for item in response["Items"] if item["bucket_url"] is not None]
                return [obj.key for obj in self.objstore_client.list_objects(random.choice(urls), prefix=prefix)]

            except ClientError as e:
                return Response(status_code=500, content=json.dumps({"error": str(e)}))

        @app.get("/api/v1/{bucket_name}/{key}")
        async def download_object(bucket_name: str, key: str, client_from_region: str) -> PhysicalObjectLocator:
            try:
                # Find a URL in the same region and with "COMMIT" status
                response = self.object_table.scan(FilterExpression=Key("bucket_name").eq(bucket_name) & Key("object_key").eq(key))
                matching_items = [
                    item
                    for item in response["Items"]
                    if any(url["region"] == client_from_region and url["obj_status"] == "COMMIT" for url in item["urls"])
                ]

                if matching_items:
                    # Find a matching URL in the same region and with "COMMIT" status
                    url_item = next(
                        url for url in matching_items[0]["urls"] if url["region"] == client_from_region and url["obj_status"] == "COMMIT"
                    )
                else:
                    # If no matching URL is found, use the primary URL with "COMMIT" status
                    primary_items = [
                        item
                        for item in response["Items"]
                        if any(url["type"] == "primary" and url["obj_status"] == "COMMIT" for url in item["urls"])
                    ]
                    url_item = (
                        next(url for url in primary_items[0]["urls"] if url["type"] == "primary" and url["obj_status"] == "COMMIT")
                        if primary_items
                        else None
                    )

                if url_item:
                    download_url = url_item["url"]
                    download_region = self.objstore_client.bucket_region(download_url)
                    return PhysicalObjectLocator(bucket=get_provider_and_bucket(download_url)[1], region=download_region, key=key)
                else:
                    return Response(status_code=404, content=f"No 'COMMIT' stage URL found for object {key} in Bucket {bucket_name}.")

            except ClientError as e:
                return Response(status_code=500, content=f"Error querying object {key} in {bucket_name}: {e}")

        @app.post("/api/v1/{bucket_name}/{key}")
        async def start_object_upload(bucket_name: str, key: str, client_from_region: str) -> List[PhysicalObjectLocator]:
            # Policy: upload locally
            try:
                region_bucket = get_region_bucket(bucket_name, client_from_region)
                region_url = region_bucket["bucket_url"]
            except Exception as e:
                return Response(status_code=404, content=f"Bucket {bucket_name} does not exist.")

            # Check if the object exists and its status
            obj_response = self.object_table.get_item(Key={"bucket_name": bucket_name, "object_key": key})
            obj_item = obj_response.get("Item")

            if obj_item:
                # If exists and in "COMMIT" status or "PREPARE" status, return 409
                for url_item in obj_item.get("urls", []):
                    if url_item["region"] == client_from_region:
                        if url_item["obj_status"] == "COMMIT":
                            return Response(status_code=409, content=f"Replication: Object {key} already exists in bucket {bucket_name}")

                        elif url_item["obj_status"] == "PREPARE":
                            return Response(
                                status_code=409, content=f"Replication: Object {key} in bucket {bucket_name} is in 'PREPARE' status."
                            )

                # If exists but not in the same region -> Replication
                obj_item["urls"].append(
                    {
                        "region": client_from_region,
                        "url": region_url,
                        "type": "secondary",
                        "obj_status": "PREPARE",
                        "physical_multipart_upload_id": "",
                        "timestamp": datetime.now().isoformat(),
                    }
                )

                try:
                    # TODO: conditional check correct here?
                    self.object_table.put_item(
                        Item=obj_item, ConditionExpression="attribute_exists(bucket_name) AND attribute_exists(object_key)"
                    )
                except ClientError as e:
                    if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                        return Response(status_code=404, content=f"Replication: Object {key} not found in bucket {bucket_name}.")
                    else:
                        return Response(status_code=500, content=f"Replication: Error putting the item {key} in {bucket_name}: {e}")
            else:
                # If not exists --> Primary Write
                object_item = {
                    "bucket_name": bucket_name,
                    "object_key": key,
                    "urls": [
                        {
                            "region": region_bucket["region_name"],
                            "url": region_url,
                            "type": "primary",
                            "obj_status": "PREPARE",
                            "physical_multipart_upload_id": "",
                            "timestamp": datetime.now().isoformat(),
                        }
                    ],
                }
                try:
                    # TODO: conditional check correct here? should consider status or not?
                    self.object_table.put_item(
                        Item=object_item, ConditionExpression="attribute_not_exists(bucket_name) AND attribute_not_exists(object_key)"
                    )
                except ClientError as e:
                    if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                        return Response(status_code=409, content=f"Primary Write: Object {key} already exists in bucket {bucket_name}.")
                    else:
                        return Response(status_code=500, content=f"Primary Write: Error putting the item {key} in {bucket_name}: {e}")

            return [PhysicalObjectLocator(bucket=get_provider_and_bucket(region_url)[1], region=region_bucket["region_name"], key=key)]

        @app.put("/api/v1/{bucket}/{key}")
        async def continue_multipart_upload(bucket: str, key: str, multipart_id: str):
            response = self.object_table.query(
                KeyConditionExpression=Key("bucket_name").eq(bucket) & Key("object_key").eq(key),
                FilterExpression="urls[0].physical_multipart_upload_id = :multipart_id AND urls[0].obj_status = :status",
                ExpressionAttributeValues={
                    ":multipart_id": multipart_id,
                    ":status": "PREPARE",
                },
            )
            if response["Count"] > 0:
                url_item = response["Items"][0]["urls"][0]
                return PhysicalObjectLocator(
                    bucket=get_provider_and_bucket(url_item["url"])[1],
                    region=url_item["region"],
                    key=key,
                )

            return Response(status_code=404, content=f"Object {key} not found in bucket {bucket} with multipart id {multipart_id}.")

        @app.patch("/api/v1/{bucket_name}/{key}")
        async def complete_object_upload(bucket_name: str, key: str, client_from_region: str, multipart_id: Optional[str] = None):
            obj_response = self.object_table.get_item(Key={"bucket_name": bucket_name, "object_key": key})
            if "Item" not in obj_response:
                return Response(status_code=404, content=f"Object {key} not found in bucket {bucket_name}.")
            obj_item = obj_response["Item"]

            # Get region url and url index
            region_url, url_index = None, -1
            for idx, url_item in enumerate(obj_item.get("urls", [])):
                if url_item["region"] == client_from_region:
                    region_url = url_item["url"]
                    url_index = idx
                    if url_item["obj_status"] != "PREPARE":
                        return Response(
                            status_code=409,
                            content=f"Object {key} in bucket {bucket_name} is not in 'PREPARE' status in the current region {client_from_region}.",
                        )
            if region_url is None:
                return Response(
                    status_code=404, content=f"Object {key} not found in bucket {bucket_name} in the current region {client_from_region}."
                )

            # Check multipart_id if exists
            if multipart_id is not None:
                # TODO: how to handle concurrent update?
                try:
                    self.object_table.update_item(
                        Key={"bucket_name": bucket_name, "object_key": key},
                        UpdateExpression=f"SET urls[{url_index}].physical_multipart_upload_id = :multipart_id",
                        ConditionExpression=f"urls[{url_index}].physical_multipart_upload_id = :empty",
                        ExpressionAttributeValues={":multipart_id": multipart_id, ":empty": ""},
                    )
                except ClientError as e:
                    return Response(
                        status_code=500, content=f"Error updating the item {key} in {bucket_name} with multipart id {multipart_id}: {e}"
                    )

                return Response(status_code=200, content=f"Update object {key} in bucket {bucket_name} with multipart id {multipart_id}.")

            # TODO: who should check whether the object exists in the bucket?
            if not self.objstore_client.obj_exists(region_url, key):
                return Response(
                    status_code=404,
                    content=f"Object {key} physically not found in bucket {bucket_name} in the current region {client_from_region}.",
                )

            # Update the object's status in the metadata table
            try:
                self.object_table.update_item(
                    Key={"bucket_name": bucket_name, "object_key": key},
                    UpdateExpression=f"SET urls[{url_index}].#status = :commit",
                    ConditionExpression=(f"attribute_exists(urls[{url_index}]) AND urls[{url_index}].#status = :prepare"),
                    ExpressionAttributeValues={":commit": "COMMIT", ":prepare": "PREPARE"},
                    ExpressionAttributeNames={"#status": "obj_status"},
                )

            except ClientError as e:
                error_code = e.response["Error"]["Code"]
                if error_code == "ConditionalCheckFailedException":
                    return Response(status_code=404, content=f"Object {key} not found in bucket {bucket_name} or not in 'PREPARE' status.")
                else:
                    return Response(
                        status_code=500, content=f"Error updating upload status to COMMIT for object {key} in {bucket_name}: {e}"
                    )

            return Response(status_code=200, content=f"Update upload status to COMMIT for object {key} in bucket {bucket_name})")

    def register_error_routes(self, app):
        @app.get("/api/v1/errors")
        async def get_errors():
            with self.error_list_lock:
                while True:
                    try:
                        elem = self.error_queue.get_nowait()
                        self.error_list.append(elem)
                    except Empty:
                        break
                # convert TracebackException to list
                error_list_str = [str(e) for e in self.error_list]
                return Response(status_code=200, content=json.dumps(error_list_str))

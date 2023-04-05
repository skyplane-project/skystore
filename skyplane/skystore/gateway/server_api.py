from concurrent.futures import ThreadPoolExecutor
import json
import logging
import logging.handlers
import os
import threading
from multiprocessing import Queue, Event
from queue import Empty
from traceback import TracebackException
import uuid
import boto3

from flask import Flask, jsonify, request
from typing import Dict, List, Tuple
from werkzeug.serving import make_server

from skyplane.utils.path import parse_path
from skyplane.utils import logger
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Attr


class SkyStoreServerAPI(threading.Thread):
    # """
    # API documentation:
    # * GET /api/v1/status - returns status of API
    # * GET /api/v1/buckets - returns list of buckets

    # * GET /api/v1/buckets/<bucket_name> - returns list of regions for bucket
    # * POST /api/v1/buckets/<bucket_name> - creates bucket in specified regions
    # * DELETE /api/v1/buckets/<bucket_name> - deletes bucket

    # * GET /api/v1/<bucket_name>/<key> - returns metadata for object
    # * POST /api/v1/<bucket_name>/<key> - uploads object
    # * DELETE /api/v1/<bucket_name>/<key> - deletes object
    # """

    def __init__(self, object_table, bucket_table, objstore_client, error_event: Event, error_queue: Queue, host="0.0.0.0", port=8081):
        super().__init__()
        self.app = Flask("gateway_metadata_server")
        self.object_table = object_table
        self.bucket_table = bucket_table
        self.objstore_client = objstore_client
        self.error_event = error_event
        self.error_queue = error_queue
        self.error_list: List[TracebackException] = []
        self.error_list_lock = threading.Lock()

        # load routes
        self.register_global_routes(self.app)
        self.register_request_routes(self.app)
        self.register_error_routes(self.app)

        # make server
        self.host = host
        self.port = port
        self.url = "http://{}:{}".format(host, port)

        logging.getLogger("werkzeug").setLevel(logging.WARNING)
        self.server = make_server(host, port, self.app, threaded=True)

    def run(self):
        self.server.serve_forever()

    def shutdown(self):
        self.server.shutdown()

    def register_global_routes(self, app):
        # index route returns API version
        @app.route("/", methods=["GET"])
        def get_index():
            return jsonify({"version": "v1"})

        # index for v1 api routes, return all available routes as HTML page with links
        @app.route("/api/v1", methods=["GET"])
        def get_v1_index():
            output = ""
            for rule in sorted(self.app.url_map.iter_rules(), key=lambda r: r.rule):
                if rule.endpoint != "static":
                    methods = set(m for m in rule.methods if m not in ["HEAD", "OPTIONS"])
                    output += f"<a href='{rule.rule}'>{rule.rule}</a>: {methods}<br>"
            return output

        # status route returns if API is up
        @app.route("/api/v1/status", methods=["GET"])
        def get_status():
            return jsonify({"status": "ok"})

        # shutdown route
        @app.route("/api/v1/shutdown", methods=["POST"])
        def shutdown():
            self.shutdown()
            logger.error("Shutdown complete. Hard exit.")
            os._exit(1)

    def register_request_routes(self, app):
        def get_provider_and_bucket(path: str):
            provider, bucket, path = parse_path(path)
            return provider, bucket

        def placement_policy(bucket_name: str, key: str, primary_region: str) -> Tuple[str, List[str]]:
            # object is placed in the region where PUT is issued first
            response = self.bucket_table.query(KeyConditionExpression=Key("bucket_name").eq(bucket_name))

            secondary_regions = [item["region_name"] for item in response["Items"] if item["region_name"] != primary_region]

            primary_item = self.bucket_table.scan(
                FilterExpression=Attr("bucket_name").eq(bucket_name) & Attr("region_name").eq(primary_region)
            )
            primary_urls = primary_item["Items"][0]["bucket_url"]

            secondary_urls = []
            # TESTING: client gateways that run a bunch of queries

            for secondary_region in secondary_regions:
                secondary_item = self.bucket_table.scan(
                    FilterExpression=Attr("bucket_name").eq(bucket_name) & Attr("region_name").eq(secondary_region)
                )
                secondary_urls.append(secondary_item["Items"][0]["bucket_url"])

            return primary_urls, secondary_urls

        def upload_object_helper(path: str, key: str, data):
            provider, bucket_name = get_provider_and_bucket(path)
            try:
                self.objstore_client.upload_object(data, bucket_name, provider, key)
            except Exception as e:
                error_message = f"Error uploading object to bucket {bucket_name} at URL {path}: {str(e)}"
                app.logger.error(error_message)

        def delete_object_helper(path: str, key: str):
            provider, bucket_name = get_provider_and_bucket(path)
            try:
                self.objstore_client.delete_objects(bucket_name, provider, [key])
            except Exception as e:
                error_message = f"Error deleting object inbucket {bucket_name} at URL {path}: {str(e)}"
                app.logger.error(error_message)

        def create_bucket_helper(global_bucket_name: str, bucket_names: Dict[str, str]):
            for region, bucket_name in bucket_names.items():
                # create the bucket
                bucket_url = self.objstore_client.create_bucket(region, bucket_name)
                new_item = {"bucket_name": global_bucket_name, "region_name": region, "bucket_url": bucket_url}

                # update the metadata table with the new bucket
                self.bucket_table.put_item(Item=new_item)

        @app.route("/api/v1/<bucket_name>", methods=["GET"])
        def bucket_exists(bucket_name: str):
            try:
                response = self.bucket_table.query(
                    KeyConditionExpression=boto3.dynamodb.conditions.Key("bucket_name").eq(bucket_name), Select="ALL_ATTRIBUTES"
                )
                if response["Count"] != 0:
                    bucket_urls = [item["bucket_url"] for item in response["Items"]]
                    return jsonify({"message": f"Bucket {bucket_name} exists.", "URLs": bucket_urls}), 200
                else:
                    return jsonify({"message": f"Bucket {bucket_name} does not exist."}), 404
            except ClientError as e:
                # Handle exception
                return jsonify({"error": f"An error occurred: {str(e)}"}), 500

        @app.route("/api/v1/<bucket_name>", methods=["DELETE"])
        def delete_bucket(bucket_name: str):
            # Make sure that the bucket exists
            response = self.bucket_table.query(KeyConditionExpression=boto3.dynamodb.conditions.Key("bucket_name").eq(bucket_name), Limit=1)
            if response["Count"] == 0:
                error_message = f"Bucket {bucket_name} not found."
                return jsonify({"error": error_message}), 404

            # Make sure that all objects are deleted
            obj_response = self.object_table.query(KeyConditionExpression=Key("bucket_name").eq(bucket_name))
            if obj_response["Count"] != 0:
                return jsonify({"error": f"Objects remained in bucket {bucket_name}. Remove the objects first."}), 400

            response = self.bucket_table.query(KeyConditionExpression=Key("bucket_name").eq(bucket_name))
            for item in response["Items"]:
                region_name = item["region_name"]
                provider, actual_bucket_name = get_provider_and_bucket(item["bucket_url"])

                # delete bucket from object store and metadata table
                with self.bucket_table.batch_writer() as batch:
                    try:
                        self.objstore_client.delete_bucket(actual_bucket_name, provider)
                        batch.delete_item(Key={"bucket_name": bucket_name, "region_name": region_name})
                    except ClientError as e:
                        # Handle exception
                        batch.rollback()
                        return jsonify({"error": f"Fail to delete bucket: {str(e)}"}), 500

            assert self.bucket_table.query(KeyConditionExpression=Key("bucket_name").eq(bucket_name))["Count"] == 0
            return jsonify({"message": f"Bucket {bucket_name} deleted."}), 200

        @app.route("/api/v1/<bucket_name>", methods=["POST"])
        def create_bucket(bucket_name: str):
            # Check if the bucket already exists
            response = self.bucket_table.query(KeyConditionExpression=boto3.dynamodb.conditions.Key("bucket_name").eq(bucket_name), Limit=1)
            if response["Count"] != 0:
                return jsonify({"error": f"Bucket {bucket_name} already exists"}), 409

            # Extract the regions from the request body and decode them from JSON
            try:
                data = json.loads(request.data)
                regions = json.loads(data["regions"])
            except Exception as e:
                return jsonify({"error": f"Error parsing request body: {str(e)}"}), 400

            # Name the bucket in different regions
            region_suffixes = {region: str(uuid.uuid4())[:8] for region in regions}
            bucket_names = {region: f"{bucket_name}-{region.split(':')[1]}-{region_suffixes[region]}" for region in regions}

            # Create the bucket
            create_bucket_helper(global_bucket_name=bucket_name, bucket_names=bucket_names)
            return jsonify({"message": f"Bucket {bucket_name} created successfully"}), 200

        @app.route("/api/v1/<bucket_name>/<key>", methods=["GET"])
        def get_object_replica_info(bucket_name: str, key: str):
            response = self.object_table.get_item(Key={"bucket_name": bucket_name, "key": key})
            if "Item" in response:
                return jsonify({"Item": response["Item"]})
            else:
                return jsonify({"error": f"Object {key} in Bucket {bucket_name} not found"}), 404

        @app.route("/api/v1/<bucket_name>/<key>", methods=["POST"])
        def upload_object(bucket_name, key):
            # Get the request data and region from the client
            data = request.get_data()
            headers = request.headers
            region = headers.get("region")

            # Check dynamoDB if the bucket exists and the requested region is within domain (?)
            # TODO: bucket must be created in advanced
            response = self.bucket_table.query(KeyConditionExpression=Key("bucket_name").eq(bucket_name))

            if "Items" not in response or len(response["Items"]) == 0:
                error_message = f"Bucket {bucket_name} not found."
                return jsonify({"error": error_message}), 404

            regions = [item["region_name"] for item in response["Items"]]

            if region not in regions:
                error_message = f"Region {region} not associated with bucket {bucket_name}; bucket has {regions}"
                return jsonify({"error": error_message}), 400

            # Get primary and secondary URLs to add to
            primary_url, secondary_urls = placement_policy(bucket_name, key, region)

            # Write to the object mapping table
            self.object_table.put_item(
                Item={
                    "bucket_name": bucket_name,
                    "key": key,
                    "primary_url": primary_url,
                    "secondary_urls": json.dumps(secondary_urls),
                }
            )

            upload_object_helper(primary_url, key, data)
            success_message = f"Object {key} uploaded to bucket {bucket_name} successfully."
            response_data = {"message": success_message}

            # Start an asynchronous task to upload the object to other object store URLs
            with ThreadPoolExecutor() as executor:
                for url in secondary_urls:
                    executor.submit(upload_object_helper, url, key, data)
            return jsonify(response_data), 200

        @app.route("/api/v1/<bucket_name>/<key>", methods=["DELETE"])
        def delete_object(bucket_name, key):
            # Check if the bucket
            response = self.bucket_table.query(KeyConditionExpression=boto3.dynamodb.conditions.Key("bucket_name").eq(bucket_name), Limit=1)

            if response["Count"] == 0:
                error_message = f"Bucket {bucket_name} not found."
                return jsonify({"error": error_message}), 404

            # Check if the object exists
            object_key = {"bucket_name": bucket_name, "key": key}
            response = self.object_table.get_item(Key=object_key)

            if "Item" not in response:
                error_message = f"Object {key} not found in bucket {bucket_name}."
                return jsonify({"error": error_message}), 404

            # Delete metadata in object and bucket table
            try:
                primary_url = response["Item"]["primary_url"]
                secondary_urls = json.loads(response["Item"]["secondary_urls"])

                delete_object_helper(primary_url, key)

                with ThreadPoolExecutor() as executor:
                    for url in secondary_urls:
                        executor.submit(delete_object_helper, url, key)

                # TODO: when should I update the metadata table? (after or before the deletion?)???
                self.object_table.delete_item(Key=object_key)

                success_message = f"Object {key} deleted from bucket {bucket_name} successfully."
                response_data = {"message": success_message}

                # TODO: add check for the dynaoDB table
                return jsonify(response_data), 200
            except Exception as e:
                error_message = f"Error deleting the item {key} in {bucket_name}: {e}"
                return jsonify({"error": error_message}), 500

    def register_error_routes(self, app):
        @app.route("/api/v1/errors", methods=["GET"])
        def get_errors():
            with self.error_list_lock:
                while True:
                    try:
                        elem = self.error_queue.get_nowait()
                        self.error_list.append(elem)
                    except Empty:
                        break
                # convert TracebackException to list
                error_list_str = [str(e) for e in self.error_list]
                return jsonify({"errors": error_list_str}), 200

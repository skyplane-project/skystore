from concurrent.futures import ThreadPoolExecutor
import json
from typing import Dict, List, Tuple
import uuid
import boto3
from flask import Flask, jsonify, request
from skyplane.api.obj_store import ObjectStore
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
from skyplane.utils.path import parse_path
from boto3.dynamodb.conditions import Attr


class SkyStoreServerDaemon:
    def __init__(self, region: str) -> None:
        self.region = region
        self.objstore_client = ObjectStore()

        self.dynamodb = boto3.resource("dynamodb", region_name=region.split(":")[1])
        self.read_capacity = 5
        self.write_capacity = 5
        self.object_table_name = "ObjectReplicaMappings"
        self.bucket_table_name = "BucketMappings"

        self.app = Flask("gateway_metadata_server")

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

    def placement_policy(self, bucket_name: str, key: str, region: str) -> Tuple[str, List[str]]:
        primary_region = region

        response = self.bucket_table.query(KeyConditionExpression=Key("bucket_name").eq(bucket_name))

        secondary_regions = [item["region_name"] for item in response["Items"] if item["region_name"] != primary_region]

        primary_item = self.bucket_table.scan(FilterExpression=Attr("bucket_name").eq(bucket_name) & Attr("region_name").eq(primary_region))
        primary_urls = primary_item["Items"][0]["bucket_url"]

        secondary_urls = []
        for secondary_region in secondary_regions:
            secondary_item = self.bucket_table.scan(
                FilterExpression=Attr("bucket_name").eq(bucket_name) & Attr("region_name").eq(secondary_region)
            )
            secondary_urls.append(secondary_item["Items"][0]["bucket_url"])

        return primary_urls, secondary_urls

    def delete_object_async(self, path: str, key: str):
        provider, bucket_name = self.get_provider_and_bucket(path)
        try:
            self.objstore_client.delete_objects(bucket_name, provider, [key])
        except Exception as e:
            error_message = f"Error deleting object inbucket {bucket_name} at URL {path}: {str(e)}"
            app.logger.error(error_message)

    def upload_object_async(self, path: str, key: str, data):
        provider, bucket_name = self.get_provider_and_bucket(path)
        try:
            self.objstore_client.upload_object(data, bucket_name, provider, key)
        except Exception as e:
            error_message = f"Error uploading object to bucket {bucket_name} at URL {path}: {str(e)}"
            app.logger.error(error_message)

    def create_bucket_helper(self, global_bucket_name: str, bucket_names: Dict[str, str]):
        # create the bucket and return the URL
        for region, bucket_name in bucket_names.items():
            bucket_url = self.objstore_client.create_bucket(region, bucket_name)
            new_item = {"bucket_name": global_bucket_name, "region_name": region, "bucket_url": bucket_url}
            # Add the items to the table
            self.bucket_table.put_item(Item=new_item)

            response2 = skystore.bucket_table.scan()
            items = response2["Items"]
            print(f"Create bucket helper: My table items are {items}")

    def get_provider_and_bucket(self, path: str):
        provider, bucket, path = parse_path(path)
        return provider, bucket


skystore = SkyStoreServerDaemon("aws:us-east-1")
app = skystore.app


@app.route("/api/v1/<bucket_name>/<key>", methods=["DELETE"])
def delete_object(bucket_name, key):
    # Check if the bucket
    response = skystore.bucket_table.query(KeyConditionExpression=boto3.dynamodb.conditions.Key("bucket_name").eq(bucket_name), Limit=1)

    if response["Count"] == 0:
        error_message = f"Bucket {bucket_name} not found."
        return jsonify({"error": error_message}), 404

    # Check if the object exists
    object_key = {"bucket_name": bucket_name, "key": key}
    response = skystore.object_table.get_item(Key=object_key)

    if "Item" not in response:
        error_message = f"Object {key} not found in bucket {bucket_name}."
        return jsonify({"error": error_message}), 404

    # Delete metadata in object and bucket table
    try:
        secondary_urls = json.loads(response["Item"]["secondary_urls"])
        with ThreadPoolExecutor() as executor:
            for url in secondary_urls:
                executor.submit(skystore.delete_object_async, url, key)

        skystore.object_table.delete_item(Key=object_key)
        success_message = f"Object {key} deleted from bucket {bucket_name} successfully."
        response_data = {"message": success_message}

        # TODO: add check for the dynaoDB table
        return jsonify(response_data), 200
    except Exception as e:
        error_message = f"Error deleting the item {key} in {bucket_name}: {e}"
        return jsonify({"error": error_message}), 500


@app.route("/api/v1/<bucket_name>/<key>", methods=["GET"])
def get_object_replica_info(bucket_name: str, key: str):
    response = skystore.object_table.get_item(Key={"bucket_name": bucket_name, "key": key})
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
    response = skystore.bucket_table.query(KeyConditionExpression=Key("bucket_name").eq(bucket_name))

    if "Items" not in response or len(response["Items"]) == 0:
        error_message = f"Bucket {bucket_name} not found."
        return jsonify({"error": error_message}), 404

    regions = [item["region_name"] for item in response["Items"]]

    if region not in regions:
        error_message = f"Region {region} not associated with bucket {bucket_name}; bucket has {regions}"
        return jsonify({"error": error_message}), 400

    # Get primary and secondary URLs to add to
    primary_url, secondary_urls = skystore.placement_policy(bucket_name, key, region)

    # TODO: put items before / after the upload? (in case of failure)
    # Update the object mapping table
    skystore.object_table.put_item(
        Item={
            "bucket_name": bucket_name,
            "key": key,
            "primary_url": primary_url,
            "secondary_urls": json.dumps(secondary_urls),
        }
    )

    skystore.upload_object_async(primary_url, key, data)
    success_message = f"Object {key} uploaded to bucket {bucket_name} successfully."
    response_data = {"message": success_message}

    # Start an asynchronous task to upload the object to other object store URLs
    with ThreadPoolExecutor() as executor:
        for url in secondary_urls:
            executor.submit(skystore.upload_object_async, url, key, data)
    return jsonify(response_data), 200


# response = skystore.bucket_table.get_item(Key={"bucket_name": bucket_name})


@app.route("/api/v1/<bucket_name>", methods=["GET"])
def bucket_exists(bucket_name: str):
    try:
        response = skystore.bucket_table.query(
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
    # Get the bucket from the bucket table
    response = skystore.bucket_table.query(KeyConditionExpression=boto3.dynamodb.conditions.Key("bucket_name").eq(bucket_name), Limit=1)

    if response["Count"] == 0:
        error_message = f"Bucket {bucket_name} not found."
        return jsonify({"error": error_message}), 404

    # Make sure that all objects are deleted
    obj_response = skystore.object_table.query(KeyConditionExpression=Key("bucket_name").eq(bucket_name))

    if obj_response["Count"] != 0:
        return jsonify({"error": f"Objects remained in bucket {bucket_name}. Remove the objects first."}), 400

    response = skystore.bucket_table.query(KeyConditionExpression=Key("bucket_name").eq(bucket_name))
    for item in response["Items"]:
        print("item: ", item)
        region_name = item["region_name"]
        provider, actual_bucket_name = skystore.get_provider_and_bucket(item["bucket_url"])
        skystore.objstore_client.delete_bucket(actual_bucket_name, provider)
        skystore.bucket_table.delete_item(Key={"bucket_name": bucket_name, "region_name": region_name})

    assert skystore.bucket_table.query(KeyConditionExpression=Key("bucket_name").eq(bucket_name))["Count"] == 0
    return jsonify({"message": f"Bucket {bucket_name} deleted."}), 200


@app.route("/api/v1/<bucket_name>", methods=["POST"])
def create_bucket(bucket_name: str):
    # Check if the bucket already exists
    response = skystore.bucket_table.query(KeyConditionExpression=boto3.dynamodb.conditions.Key("bucket_name").eq(bucket_name), Limit=1)
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
    skystore.create_bucket_helper(global_bucket_name=bucket_name, bucket_names=bucket_names)
    return jsonify({"message": f"Bucket {bucket_name} created successfully"}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)

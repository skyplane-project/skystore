import os
import random
import time
import uuid
from datetime import datetime
from pathlib import Path
import functools
import json
from typing import Dict, Iterator, List, Optional, Union

import urllib3
from skyplane.obj_store.object_store_interface import ObjectStoreInterface, ObjectStoreObject
from skyplane.skystore.utils.definitions import tmp_log_dir
from skyplane.utils.path import parse_path
from skyplane import compute
from skyplane.utils import logger


class SkyObjectStore:
    """
    Clinet for interacting with object stores (S3, GCS, Azure)
    """

    def __init__(self, region: str, server_api_url: str, log_dir: Optional[str] = None) -> None:
        self.region = region
        self.server_api_url = server_api_url
        self.http_pool = urllib3.PoolManager()

        # set up logging
        self.log_dir = (
            tmp_log_dir / "client_request_logs" / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}-{uuid.uuid4().hex[:8]}"
            if log_dir is None
            else Path(log_dir)
        )
        self.log_dir.mkdir(parents=True, exist_ok=True)
        logger.open_log_file(self.log_dir / "client.log")
        print("Client log loc: ", self.log_dir / "client.log")
        print("Client trace loc: ", self.log_dir / "trace.log")

    # helper functions
    def get_obj_store_interface(self, path: str) -> ObjectStoreInterface:
        provider, bucket, path = parse_path(path)
        region_tag = f"{provider}:infer"
        return ObjectStoreInterface.create(region_tag, bucket)

    def log_event(self, op: str, obj: str, obj_size_MB: Optional[int] = None, requested_region: Optional[str] = "") -> None:
        req_time = int(time.time())
        req_region = self.region
        req_obj_size_MB = obj_size_MB or 0

        trace_line = f"{req_time}, {req_region}, {requested_region}, {op}, {obj}, {req_obj_size_MB}\n"
        with open(self.log_dir / "trace.log", "a") as trace_file:
            trace_file.write(trace_line)

    def get_file_size_MB(self, filename: str) -> int:
        file_size = os.path.getsize(filename)
        return int(file_size / (1024 * 1024))

    @functools.lru_cache(maxsize=1)
    def get_replica_info(self, bucket_name: str, key: str) -> Dict[str, Union[ObjectStoreInterface, List[ObjectStoreInterface]]]:
        response = self.http_pool.request("GET", f"{self.server_api_url}/api/v1/{bucket_name}/{key}")
        if response.status == 200:
            data = json.loads(response.data.decode("utf-8"))
            for k, v in data.items():
                if k == "primary_url":
                    data[k] = self.get_obj_store_interface(v)
                if k == "secondary_urls":
                    data[k] = [self.get_obj_store_interface(url) for url in v]
            return data

        elif response.status == 404:
            return {}
        else:
            raise Exception(f"Error getting replica info for bucket {bucket_name}: {response.data.decode('utf-8')}")

    def exists(self, bucket_name: str, key: str) -> bool:
        if self.get_replica_info(bucket_name, key):
            return True
        return False

    def bucket_exists(self, bucket_name: str) -> bool:
        response = self.http_pool.request("GET", f"{self.server_api_url}/api/v1/{bucket_name}")
        if response.status == 200:
            return True
        elif response.status == 404:
            return False
        else:
            raise Exception(f"Error checking if bucket {bucket_name} exists: {response.data.decode('utf-8')}")

    def download_object(self, bucket_name: str, key: str, filename: str):
        # NOTE: V1 - strong consistency (download from primary replica)
            # also read-after-write consistency as write is directed to primary replica in local region 
        # TODO: V2 - eventual consistency (download from any replica + retries)

        # V1 - strong consistency
        replica_info = self.get_replica_info(bucket_name, key)["Item"]
        primary_url = replica_info["primary_url"]
        
        obj_store = self.get_obj_store_interface(primary_url)
        obj_store.download_object(key, filename)
        
        # V2 - eventual consistency
        # secondary_urls = replica_info["secondary_urls"]
        # self.get_obj_store_interface(random.choice(secondary_urls)).download_object(key, filename)
        
        # log event
        requested_region = obj_store.region_tag()
        self.log_event("DOWNLOAD_OBJECT", f"{bucket_name}/{key}", self.get_file_size_MB(filename), requested_region)

    def list_objects(self, bucket_name: str, prefix="") -> Iterator[ObjectStoreObject]:
        response = self.http_pool.request("GET", f"{self.server_api_url}/api/v1/{bucket_name}")
        if response.status != 200:
            raise Exception(f"Error listing objects in bucket {bucket_name}: {response.data.decode('utf-8')}")

        # return a URL to list the objects (TODO: which URL should we return? -> maybe the one within the same region, otherwise random)
        print("Response in listing object: ", response.data.decode("utf-8"))
        urls = json.loads(response.data.decode("utf-8"))["URLs"]

        # Find a URL with a matching region tag [strong read-after-write consistency]
        matching_url = None
        for url in urls:
            obj_store = self.get_obj_store_interface(url)
            if obj_store.region_tag() == self.region:
                matching_url = url
                break

        # If no matching URL is found, randomly select one
        if not matching_url:
            matching_url = random.choice(urls)

        obj_store = self.get_obj_store_interface(matching_url)
        return obj_store.list_objects(prefix)

    def upload_object(self, filename: str, bucket_name: str, key: str, client_region: str = "aws:us-east-1"):
        with open(filename, "rb") as f:
            data = f.read()

        headers = {"region": client_region}
        response = self.http_pool.request("POST", f"{self.server_api_url}/api/v1/{bucket_name}/{key}", body=data, headers=headers)
        print("Response in uploading object: ", response.data.decode("utf-8"))
        if response.status != 200:
            raise Exception(f"Error uploading object to bucket {bucket_name}: {response.data.decode('utf-8')}")
        self.log_event("UPLOAD_OBJECT", f"{bucket_name}/{key}", self.get_file_size_MB(filename))

    def delete_object(self, bucket_name: str, key: str):
        request_url = f"{self.server_api_url}/api/v1/{bucket_name}/{key}"
        response = self.http_pool.request("DELETE", request_url)
        print("Response in deleting object: ", response.data.decode("utf-8"))
        if response.status != 200:
            raise Exception(f"Error deleting object {key} from bucket {bucket_name}: {response.data.decode('utf-8')}")
        self.log_event("DELETE_OBJECT", f"{bucket_name}/{key}")

    def delete_objects(self, bucket_name: str, keys: List[str]):
        # TODO: implement this
        # Send the request to the server to delete the objects within a single bucket
        payload = {"keys": keys}
        headers = {"Content-Type": "application/json"}
        response = self.http_pool.request(
            "DELETE", f"{self.server_api_url}/api/v1/{bucket_name}/delete_objects", headers=headers, json=payload
        )

        # Check the status code of the response to see if the request was successful
        if response.status != 200:
            raise Exception(f"Error deleting objects with keys={keys} from bucket {bucket_name}: {response.data.decode('utf-8')}")

    def create_bucket(self, bucket_name: str, regions: Optional[List[str]] = None):
        if regions is None:
            regions = compute.AWSCloudProvider.region_list()
            regions = ["aws:" + region for region in regions][:1]
            if self.region not in regions:
                regions.append(self.region)

        body = {"regions": json.dumps(regions)}
        headers = {"Content-Type": "application/json"}
        # Encode the body as JSON
        json_body = json.dumps(body).encode("utf-8")
        response = self.http_pool.request("POST", f"{self.server_api_url}/api/v1/{bucket_name}", body=json_body, headers=headers)
        print("Response in creating bucket: ", response.data.decode("utf-8"))
        if response.status != 200:
            raise Exception(f"Error creating bucket {bucket_name}: {response.data.decode('utf-8')}")
        self.log_event("CREATE_BUCKET", bucket_name)

    def delete_bucket(self, bucket_name: str):
        response = self.http_pool.request("DELETE", f"{self.server_api_url}/api/v1/{bucket_name}")
        print("Response in deleting bucket: ", response.data.decode("utf-8"))

        # Check if request was successful
        if response.status != 200:
            raise Exception(f"Failed to delete bucket {bucket_name}. Server response: {response.data}")
        self.log_event("DELETE_BUCKET", bucket_name)

import os
import json
from typing import List, Optional
import urllib3
from skyplane import compute
import requests
from skyplane.api.obj_store import ObjectStore


class SkyObjectStore:
    """
    Client for interacting with SkyStore server.
    # TODO: test delete bucket and delete object
    """

    def __init__(self, region: str, server_api_url: str, log_dir: Optional[str] = None) -> None:
        self.region = region
        self.objstore_client = ObjectStore()
        self.server_api_url = server_api_url
        self.req_client = requests.Session()

    def create_bucket(self, bucket_name: str, warmup_regions: Optional[List[str]] = None):
        # Create bucket in all warmup regions
        if warmup_regions is None:
            warmup_regions = compute.AWSCloudProvider.region_list()
            warmup_regions = ["aws:" + region for region in warmup_regions[:1]]  # just pick the first region for now
        if self.region not in warmup_regions:
            warmup_regions.append(self.region)
        print("Warmup regions: ", warmup_regions)
        self.req_client.post(f"{self.server_api_url}/api/v1/{bucket_name}", json=warmup_regions).raise_for_status()

    def bucket_exists(self, bucket_name: str) -> bool:
        response = self.req_client.get(f"{self.server_api_url}/api/v1/{bucket_name}")
        if response.status_code == 200:
            return True
        elif response.status_code == 404:
            return False
        else:
            raise Exception(f"Error checking if bucket {bucket_name} exists: {response.text}")

    def list_objects(self, bucket_name: str, prefix: str = ""):
        response = self.req_client.get(f"{self.server_api_url}/api/v1/list_objects/{bucket_name}/{prefix}")
        assert response.status_code == 200
        return response.json()

    def download_object(self, bucket_name: str, key: str, download_region: Optional[str] = "aws:us-east-1"):
        self.req_client.get(f"{self.server_api_url}/api/v1/{bucket_name}/{key}?client_from_region={download_region}").raise_for_status()

    def upload_object_part(
        self, bucket_name: str, key: str, upload_region: Optional[str] = "aws:us-east-1", file_size: Optional[int] = 1024
    ):
        src_filename = f"src_{key}"
        with open(src_filename, "wb") as fout:
            fout.write(os.urandom(file_size))

        # Prepare upload
        prepare_response = self.req_client.post(f"{self.server_api_url}/api/v1/{bucket_name}/{key}?client_from_region={upload_region}")
        assert prepare_response.status_code == 200

        # Give multipart id
        self.req_client.patch(
            f"{self.server_api_url}/api/v1/{bucket_name}/{key}?client_from_region={upload_region}&multipart_id=123"
        ).raise_for_status()

        # Locate the upload with multipart id
        self.req_client.put(
            f"{self.server_api_url}/api/v1/{bucket_name}/{key}?client_from_region={upload_region}&multipart_id=123"
        ).raise_for_status()

        assert (
            self.req_client.put(
                f"{self.server_api_url}/api/v1/{bucket_name}/{key}?client_from_region={upload_region}&multipart_id=1"
            ).status_code
            == 404
        )

        for item in prepare_response.json():
            # Perform upload
            local_bucket_name = item["bucket"]
            key = item["key"]
            region_tag = item["region"]
            provider, _ = region_tag.split(":")
            self.objstore_client.upload_object(src_filename, local_bucket_name, provider, key)

            # Finish upload: ? what should be client region
            self.req_client.patch(f"{self.server_api_url}/api/v1/{bucket_name}/{key}?client_from_region={region_tag}").raise_for_status()

            # Should be able to get
            response = self.req_client.get(f"{self.server_api_url}/api/v1/{bucket_name}/{key}?client_from_region={region_tag}")
            assert response.status_code == 200

        # Cleanup
        os.remove(src_filename)

    def upload_object(self, bucket_name: str, key: str, upload_region: Optional[str] = "aws:us-east-1", file_size: Optional[int] = 1024):
        src_filename = f"src_{key}"
        with open(src_filename, "wb") as fout:
            fout.write(os.urandom(file_size))

        # prepare upload
        prepare_response = self.req_client.post(f"{self.server_api_url}/api/v1/{bucket_name}/{key}?client_from_region={upload_region}")
        assert prepare_response.status_code == 200

        # TEST: prepare upload during 'PREPARE' should fail
        assert (
            self.req_client.post(f"{self.server_api_url}/api/v1/{bucket_name}/{key}?client_from_region={upload_region}").status_code == 409
        )

        # TEST: get during 'PREPARE' should fail
        assert (
            self.req_client.get(f"{self.server_api_url}/api/v1/{bucket_name}/{key}?client_from_region={upload_region}").status_code == 404
        )

        # TEST: patch when object is not physically uploaded should fail
        assert (
            self.req_client.patch(f"{self.server_api_url}/api/v1/{bucket_name}/{key}?client_from_region={upload_region}").status_code == 404
        )

        # perform upload
        for item in prepare_response.json():
            local_bucket_name = item["bucket"]
            key = item["key"]
            region_tag = item["region"]
            provider, _ = region_tag.split(":")
            self.objstore_client.upload_object(src_filename, local_bucket_name, provider, key)

            # finish upload # TODO: this might be wrong, client_from_region should be region [returned from prepare]
            assert (
                self.req_client.patch(f"{self.server_api_url}/api/v1/{bucket_name}/{key}?client_from_region={region_tag}").status_code
                == 200
            )

        # TEST: random region commit should fail
        assert self.req_client.patch(f"{self.server_api_url}/api/v1/{bucket_name}/{key}?client_from_region=random").status_code == 404
        os.remove(src_filename)

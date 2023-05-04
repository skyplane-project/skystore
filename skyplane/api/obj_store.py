from typing import List
from skyplane.obj_store.object_store_interface import ObjectStoreInterface
from skyplane.utils.path import parse_path


class ObjectStore:
    def __init__(self) -> None:
        pass

    # NOTE: add this for skystore
    def delete_objects(self, bucket_name: str, provider: str, keys: List[str]):
        obj_store = ObjectStoreInterface.create(f"{provider}:infer", bucket_name)
        obj_store.delete_objects(keys)

    def download_object(self, bucket_name: str, provider: str, key: str, filename: str, **kwargs):
        obj_store = ObjectStoreInterface.create(f"{provider}:infer", bucket_name)
        return obj_store.download_object(key, filename, **kwargs)

    def upload_object(self, filename: str, bucket_name: str, provider: str, key: str):
        obj_store = ObjectStoreInterface.create(f"{provider}:infer", bucket_name)
        try:
            obj_store.upload_object(filename, key)
        except Exception as e:
            print(f"Failed to upload {filename} to {bucket_name}/{key}. exception: {e}")

    def exists(self, bucket_name: str, provider: str, key: str) -> bool:
        obj_store = ObjectStoreInterface.create(f"{provider}:infer", bucket_name)
        return obj_store.exists(key)

    def bucket_exists(self, bucket_name: str, provider: str) -> bool:
        # azure not implemented
        if provider == "azure":
            raise NotImplementedError(f"Provider {provider} not implemented")

        obj_store = ObjectStoreInterface.create(f"{provider}:infer", bucket_name)
        return obj_store.bucket_exists()

    def create_bucket(self, region: str, bucket_name: str):
        provider = region.split(":")[0]
        # azure not implemented
        if provider == "azure":
            raise NotImplementedError(f"Provider {provider} not implemented")

        print(f"Creating bucket {bucket_name} in region {region}")
        obj_store = ObjectStoreInterface.create(region, bucket_name)
        obj_store.create_bucket(region.split(":")[1])

        # TODO: create util function for this
        if provider == "aws":
            return f"s3://{bucket_name}"
        elif provider == "gcp":
            return f"gs://{bucket_name}"
        else:
            raise NotImplementedError(f"Provider {provider} not implemented")

    def delete_bucket(self, bucket_name: str, provider: str):
        # azure not implemented
        if provider == "azure":
            raise NotImplementedError(f"Provider {provider} not implemented")

        obj_store = ObjectStoreInterface.create(f"{provider}:infer", bucket_name)
        obj_store.delete_bucket()

    def list_objects(self, bucket_url: str, prefix: str = None) -> List[str]:
        provider, bucket_name, _ = parse_path(bucket_url)
        obj_store = ObjectStoreInterface.create(f"{provider}:infer", bucket_name)
        return obj_store.list_objects(prefix)

    def bucket_region(self, bucket_url: str) -> str:
        provider, bucket_name, _ = parse_path(bucket_url)
        # azure not implemented
        if provider == "azure":
            raise NotImplementedError(f"Provider {provider} not implemented")
        obj_store = ObjectStoreInterface.create(f"{provider}:infer", bucket_name)
        return obj_store.region_tag()

    def obj_exists(self, bucket_url: str, key: str) -> bool:
        provider, bucket_name, _ = parse_path(bucket_url)
        return self.exists(bucket_name, provider, key)

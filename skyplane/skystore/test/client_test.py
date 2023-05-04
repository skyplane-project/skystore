from skyplane.skystore.api.skystore_client import SkyStoreClient
import uuid


def test_skystore_server(region, warmup_regions=["aws:us-west-1"]):
    client = SkyStoreClient(region)
    objstore = client.sky_obj_store()

    # create src and dst file names
    key = str(uuid.uuid4()).replace("-", "")

    provider = client.region.split(":")[0]
    if provider == "azure":
        # need both storage account and container
        bucket_name = str(uuid.uuid4()).replace("-", "")[:24] + "/" + str(uuid.uuid4()).replace("-", "")
    else:
        bucket_name = str(uuid.uuid4()).replace("-", "")

    try:
        # create bucket in region + warmup_regions
        objstore.create_bucket(bucket_name, warmup_regions=warmup_regions)
        assert objstore.bucket_exists(bucket_name), f"Bucket {bucket_name} does not exist"

        # upload object [upload region needs to be in warmup regions or client region]
        objstore.upload_object(bucket_name, key, upload_region=warmup_regions[0])

        # list objects
        for object in objstore.list_objects(bucket_name):
            print("Listing Object: ", object)

        # multipart upload object [upload region needs to be in warmup regions or client region]
        objstore.upload_object_part(bucket_name, f"new_{key}", upload_region=warmup_regions[0])
        for object in objstore.list_objects(bucket_name):
            print("Listing Object: ", object)

        # download object
        objstore.download_object(bucket_name, key, download_region=region)
        objstore.download_object(bucket_name, key, download_region=warmup_regions[0])

    except Exception as e:
        print(f"Exception: {e} occurred. Copying gateway logs...")
    finally:
        client.close()


test_skystore_server("aws:us-east-1")

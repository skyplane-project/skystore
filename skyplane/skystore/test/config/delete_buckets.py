import boto3


def get_bucket_region(bucket_name):
    s3 = boto3.client("s3")
    try:
        region = s3.get_bucket_location(Bucket=bucket_name)["LocationConstraint"]
    except Exception as e:
        print(f"Failed to get bucket location for bucket: {bucket_name}")
        return None

    if region is None:
        region = "us-east-1"  # LocationConstraint is None for the 'us-east-1' region
    return region


def delete_all_objects(bucket_name, region):
    s3 = boto3.resource("s3", region_name=region)
    bucket = s3.Bucket(bucket_name)
    bucket.objects.all().delete()


def delete_all_buckets_except_broadcast():
    s3 = boto3.client("s3")
    for bucket in s3.list_buckets()["Buckets"]:
        bucket_name = bucket["Name"]
        if not bucket_name.startswith("broadcast") and "-read-" not in bucket_name and "-write-" not in bucket_name:
            region = get_bucket_region(bucket_name)
            if region is None:
                continue

            delete_all_objects(bucket_name, region)
            s3 = boto3.client("s3", region_name=region)
            s3.delete_bucket(Bucket=bucket_name)
            print(f"Deleted bucket: {bucket_name}")


delete_all_buckets_except_broadcast()

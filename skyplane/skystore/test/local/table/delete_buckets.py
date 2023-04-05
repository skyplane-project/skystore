import boto3

# Create an S3 client
s3 = boto3.client("s3")

# List all S3 buckets
response = s3.list_buckets()
buckets = response["Buckets"]


# Function to check if a bucket is empty
def is_bucket_empty(bucket_name):
    response = s3.list_objects_v2(Bucket=bucket_name)
    return response["KeyCount"] == 0


# Iterate through the buckets and delete empty ones
for bucket in buckets:
    bucket_name = bucket["Name"]

    if is_bucket_empty(bucket_name):
        print(f"Deleting empty bucket: {bucket_name}")
        s3.delete_bucket(Bucket=bucket_name)
    else:
        if "-us-east-1-" in bucket_name or "-ap-south-1-" in bucket_name:
            # List all objects in the bucket
            response = s3.list_objects_v2(Bucket=bucket_name)
            objects = response.get("Contents", [])

            # Delete all objects in the bucket
            for obj in objects:
                object_key = obj["Key"]
                print(f"Deleting object: {object_key}")
                s3.delete_object(Bucket=bucket_name, Key=object_key)

            # Delete the bucket
            print(f"Deleting bucket: {bucket_name}")
            s3.delete_bucket(Bucket=bucket_name)
        else:
            print(f"Skipping non-empty bucket: {bucket_name}")

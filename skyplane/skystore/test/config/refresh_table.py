import boto3


def delete_all_items(table):
    with table.batch_writer() as batch:
        items = table.scan()["Items"]
        key_attribute_names = [key_schema["AttributeName"] for key_schema in table.key_schema]
        for item in items:
            key = {attr_name: item[attr_name] for attr_name in key_attribute_names}
            batch.delete_item(Key=key)


object_table_name = "ObjectReplicaMappings"
bucket_table_name = "BucketMappings"
table_region = "us-east-1"

dynamodb = boto3.client("dynamodb", region_name=table_region)
object_table = boto3.resource("dynamodb").Table(object_table_name)
bucket_table = boto3.resource("dynamodb").Table(bucket_table_name)

delete_all_items(object_table)
delete_all_items(bucket_table)

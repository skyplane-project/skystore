import boto3


def create_dynamic_dynamodb_tables(
    region: str = "aws:us-east-1",
    object_table_name: str = "ObjectReplicaMappings",
    bucket_table_name: str = "BucketMappings",
    read_capacity: int = 5,
    write_capacity: int = 5,
):
    dynamodb = boto3.resource("dynamodb", region_name=region.split(":")[1])

    def delete_all_items(table):
        with table.batch_writer() as batch:
            items = table.scan()["Items"]
            key_attribute_names = [key_schema["AttributeName"] for key_schema in table.key_schema]
            for item in items:
                key = {attr_name: item[attr_name] for attr_name in key_attribute_names}
                batch.delete_item(Key=key)

    if object_table_name in [t.name for t in dynamodb.tables.all()]:
        print("Object table exists, deleting all items...")
        object_table = dynamodb.Table(object_table_name)
        delete_all_items(object_table)
    else:
        object_table = dynamodb.create_table(
            TableName=object_table_name,
            KeySchema=[{"AttributeName": "bucket_name", "KeyType": "HASH"}, {"AttributeName": "object_key", "KeyType": "RANGE"}],
            AttributeDefinitions=[
                {"AttributeName": "bucket_name", "AttributeType": "S"},
                {"AttributeName": "object_key", "AttributeType": "S"},
            ],
            ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
        )

    if bucket_table_name in [t.name for t in dynamodb.tables.all()]:
        print("Bucket table exists, deleting all items...")
        bucket_table = dynamodb.Table(bucket_table_name)
        delete_all_items(bucket_table)
    else:
        # TODO: revisit this, shall I just use bucket_name only as the key schema?
        bucket_table = dynamodb.create_table(
            TableName=bucket_table_name,
            KeySchema=[{"AttributeName": "bucket_name", "KeyType": "HASH"}, {"AttributeName": "region_name", "KeyType": "RANGE"}],
            AttributeDefinitions=[
                {"AttributeName": "bucket_name", "AttributeType": "S"},
                {"AttributeName": "region_name", "AttributeType": "S"},
            ],
            ProvisionedThroughput={"ReadCapacityUnits": read_capacity, "WriteCapacityUnits": write_capacity},
        )

    object_table.wait_until_exists()
    bucket_table.wait_until_exists()

    return region, object_table_name, bucket_table_name


create_dynamic_dynamodb_tables()

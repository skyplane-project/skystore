# SkyStore Server 
## DynamoDB build 
```
# create DynamoDB table 
python skyplane/skystore/test/config/create_dynamodb.py 

# refresh DynamoDB table 
python skyplane/skystore/test/config/refresh_table.py 
```

## Usage 
```
export SKYPLANE_DOCKER_IMAGE=$(bash scripts/pack_docker_skystore.sh <YOUR_GITHUB_USERNAME_HERE>); pip install -e ".[aws,azure,gcp]"
```

## Test 
Script for provisioning a SkyStore server and interact with it 
```
python skyplane/skystore/test/skystore_server_test.py 
```

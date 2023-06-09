This is a starting point for implementing a S3 proxy service and get it ready for the container registry workload.

Environment:
- Install rust and docker
- Install just command runner through rust: `cargo install just`

Run:
- Compile and run the project: `just run`
- Run a registry proxy to the local S3 proxy: `just run-registry`
- Run a push: `just run-sample-push`.

Currently nothing is implemented. You should see the S3 proxy fail and logs the failure.

We use the following emulators:
- https://github.com/fsouza/fake-gcs-server
- https://github.com/azure/azurite
- https://github.com/Nugine/s3s/tree/main/crates/s3s-fs (TODO: maybe use moto instead http://docs.getmoto.org/en/latest/docs/services/s3.html? or localstack https://github.com/localstack/localstack)

We use the following SDKs:
- https://github.com/Azure/azure-sdk-for-rust
- https://crates.io/crates/aws-sdk-s3 (through s3s)
- (maybe) https://github.com/yoshidan/google-cloud-rust

Notes on logging in:

Azure
az login # use device code doesn't work
az account set --subscription 0d9fbbd5-4ec5-4933-8411-be33baacae69
az storage container create --name sky-s3-backend --account-name skycontainer
az storage account blob-service-properties update -n skycontainer --default-service-version 2022-11-02 # otherwise it doesn't support uploading 100MB+ for UploadPart



GCP
sky-s3-backend bucket in SkyContainerDemo account

AWS
sky-s3-backend bucket  # in us-west-2
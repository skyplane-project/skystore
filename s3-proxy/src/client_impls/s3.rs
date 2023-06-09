use crate::objstore_client::ObjectStoreClient;
use s3s::dto::*;
use s3s::S3;
use s3s::{S3Request, S3Result};
use s3s_aws::Proxy;

pub struct S3ObjectStoreClient {
    s3_proxy: Proxy,
}

impl S3ObjectStoreClient {
    pub async fn new(endpoint_url: String) -> Self {
        let config: aws_config::SdkConfig = aws_config::from_env()
            .endpoint_url(endpoint_url)
            .load()
            .await;
        let s3_config = aws_sdk_s3::config::Builder::from(&config)
            .force_path_style(true)
            .build();
        let sdk_client = aws_sdk_s3::Client::from_conf(s3_config);
        let s3_proxy = Proxy::from(sdk_client);
        Self { s3_proxy }
    }
}

#[async_trait::async_trait]
impl ObjectStoreClient for S3ObjectStoreClient {
    async fn head_object(&self, req: S3Request<HeadObjectInput>) -> S3Result<HeadObjectOutput> {
        return self.s3_proxy.head_object(req).await;
    }

    // async fn list_objects_v2(
    //     &self,
    //     req: S3Request<ListObjectsV2Input>,
    // ) -> S3Result<ListObjectsV2Output> {
    //     return self.s3_proxy.list_objects_v2(req).await;
    // }

    async fn get_object(&self, req: S3Request<GetObjectInput>) -> S3Result<GetObjectOutput> {
        return self.s3_proxy.get_object(req).await;
    }

    async fn put_object(&self, req: S3Request<PutObjectInput>) -> S3Result<PutObjectOutput> {
        return self.s3_proxy.put_object(req).await;
    }

    async fn copy_object(&self, req: S3Request<CopyObjectInput>) -> S3Result<CopyObjectOutput> {
        return self.s3_proxy.copy_object(req).await;
    }

    // async fn delete_object(
    //     &self,
    //     req: S3Request<DeleteObjectInput>,
    // ) -> S3Result<DeleteObjectOutput> {
    //     return self.s3_proxy.delete_object(req).await;
    // }

    // async fn delete_objects(
    //     &self,
    //     req: S3Request<DeleteObjectsInput>,
    // ) -> S3Result<DeleteObjectsOutput> {
    //     return self.s3_proxy.delete_objects(req).await;
    // }

    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<CreateMultipartUploadOutput> {
        return self.s3_proxy.create_multipart_upload(req).await;
    }

    // async fn list_multipart_uploads(
    //     &self,
    //     req: S3Request<ListMultipartUploadsInput>,
    // ) -> S3Result<ListMultipartUploadsOutput> {
    //     return self.s3_proxy.list_multipart_uploads(req).await;
    // }

    // async fn list_parts(&self, req: S3Request<ListPartsInput>) -> S3Result<ListPartsOutput> {
    //     return self.s3_proxy.list_parts(req).await;
    // }

    async fn upload_part(&self, req: S3Request<UploadPartInput>) -> S3Result<UploadPartOutput> {
        return self.s3_proxy.upload_part(req).await;
    }

    async fn upload_part_copy(
        &self,
        req: S3Request<UploadPartCopyInput>,
    ) -> S3Result<UploadPartCopyOutput> {
        return self.s3_proxy.upload_part_copy(req).await;
    }

    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<CompleteMultipartUploadOutput> {
        return self.s3_proxy.complete_multipart_upload(req).await;
    }

    // async fn abort_multipart_upload(
    //     &self,
    //     req: S3Request<AbortMultipartUploadInput>,
    // ) -> S3Result<AbortMultipartUploadOutput> {
    //     return self.s3_proxy.abort_multipart_upload(req).await;
    // }
}

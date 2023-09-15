use crate::objstore_client::ObjectStoreClient;
use aws_sdk_s3::config::Region;
use reqwest::Url;
use s3s::dto::*;
use s3s::S3;
use s3s::{S3Request, S3Response, S3Result};
use s3s_aws::Proxy;

pub struct S3ObjectStoreClient {
    s3_proxy: Proxy,
}

impl S3ObjectStoreClient {
    #[allow(dead_code)]
    pub async fn new(endpoint_url: String) -> Self {
        let config: aws_config::SdkConfig = if endpoint_url.starts_with("http://localhost:") {
            aws_config::from_env()
                .endpoint_url(endpoint_url)
                .load()
                .await
        } else {
            let url = Url::parse(&endpoint_url).unwrap();
            let host = url.host_str().unwrap().to_string();
            let region = host.split('.').nth(1).unwrap().to_string();
            aws_config::from_env()
                .region(Region::new(region))
                .endpoint_url(endpoint_url)
                .load()
                .await
        };

        let s3_config = aws_sdk_s3::config::Builder::from(&config)
            .force_path_style(true)
            .build();
        let sdk_client = aws_sdk_s3::client::Client::from_conf(s3_config);
        let s3_proxy = Proxy::from(sdk_client);
        Self { s3_proxy }
    }
}

#[async_trait::async_trait]
impl ObjectStoreClient for S3ObjectStoreClient {
    async fn create_bucket(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        return self.s3_proxy.create_bucket(req).await;
    }

    async fn delete_bucket(
        &self,
        req: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        return self.s3_proxy.delete_bucket(req).await;
    }

    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        return self.s3_proxy.head_object(req).await;
    }

    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        return self.s3_proxy.get_object(req).await;
    }

    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        return self.s3_proxy.put_object(req).await;
    }

    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        return self.s3_proxy.delete_object(req).await;
    }

    async fn copy_object(
        &self,
        req: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        return self.s3_proxy.copy_object(req).await;
    }

    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        return self.s3_proxy.create_multipart_upload(req).await;
    }

    async fn upload_part(
        &self,
        req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        return self.s3_proxy.upload_part(req).await;
    }

    async fn upload_part_copy(
        &self,
        req: S3Request<UploadPartCopyInput>,
    ) -> S3Result<S3Response<UploadPartCopyOutput>> {
        return self.s3_proxy.upload_part_copy(req).await;
    }

    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        return self.s3_proxy.complete_multipart_upload(req).await;
    }
}

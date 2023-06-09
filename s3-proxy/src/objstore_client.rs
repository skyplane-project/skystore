use s3s::dto::*;
use s3s::{S3Request, S3Result};

#[async_trait::async_trait]
pub trait ObjectStoreClient: Send + Sync + 'static {
    async fn head_object(&self, req: S3Request<HeadObjectInput>) -> S3Result<HeadObjectOutput>;

    // async fn list_objects_v2(
    //     &self,
    //     req: S3Request<ListObjectsV2Input>,
    // ) -> S3Result<ListObjectsV2Output>;

    async fn get_object(&self, req: S3Request<GetObjectInput>) -> S3Result<GetObjectOutput>;

    async fn put_object(&self, req: S3Request<PutObjectInput>) -> S3Result<PutObjectOutput>;

    async fn copy_object(&self, req: S3Request<CopyObjectInput>) -> S3Result<CopyObjectOutput>;

    // async fn delete_object(
    //     &self,
    //     req: S3Request<DeleteObjectInput>,
    // ) -> S3Result<DeleteObjectOutput>;

    // async fn delete_objects(
    //     &self,
    //     req: S3Request<DeleteObjectsInput>,
    // ) -> S3Result<DeleteObjectsOutput>;

    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<CreateMultipartUploadOutput>;

    // async fn list_multipart_uploads(
    //     &self,
    //     req: S3Request<ListMultipartUploadsInput>,
    // ) -> S3Result<ListMultipartUploadsOutput>;

    // async fn list_parts(&self, req: S3Request<ListPartsInput>) -> S3Result<ListPartsOutput>;

    async fn upload_part(&self, req: S3Request<UploadPartInput>) -> S3Result<UploadPartOutput>;

    async fn upload_part_copy(
        &self,
        req: S3Request<UploadPartCopyInput>,
    ) -> S3Result<UploadPartCopyOutput>;

    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<CompleteMultipartUploadOutput>;

    // async fn abort_multipart_upload(
    //     &self,
    //     req: S3Request<AbortMultipartUploadInput>,
    // ) -> S3Result<AbortMultipartUploadOutput>;
}

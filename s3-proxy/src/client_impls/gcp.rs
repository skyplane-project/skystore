use crate::objstore_client::ObjectStoreClient;
use google_cloud_default::WithAuthExt;
use google_cloud_storage::client::{Client, ClientConfig};
use google_cloud_storage::http::objects::compose::{ComposeObjectRequest, ComposingTargets};
use google_cloud_storage::http::objects::copy::CopyObjectRequest;
use google_cloud_storage::http::objects::download::Range;
use google_cloud_storage::http::objects::get::GetObjectRequest;
use google_cloud_storage::http::objects::upload::{Media, UploadObjectRequest, UploadType};
use google_cloud_storage::http::objects::SourceObjects;

use s3s::dto::{Range as S3Range, *};
use s3s::{S3Request, S3Result};

pub struct GCPObjectStoreClient {
    client: Client,
}

impl GCPObjectStoreClient {
    pub async fn new() -> Self {
        let config = ClientConfig::default().with_auth().await.unwrap();
        Self {
            client: Client::new(config),
        }
    }
}

#[async_trait::async_trait]
impl ObjectStoreClient for GCPObjectStoreClient {
    async fn head_object(&self, req: S3Request<HeadObjectInput>) -> S3Result<HeadObjectOutput> {
        let req = req.input;
        let bucket = req.bucket;
        let object = req.key;

        let res = self
            .client
            .get_object(&GetObjectRequest {
                bucket,
                object,
                ..Default::default()
            })
            .await
            .unwrap();

        Ok(HeadObjectOutput {
            e_tag: Some(res.etag),
            content_length: res.size,
            last_modified: res.updated.map(|t| Timestamp::from(t)),
            ..Default::default()
        })
    }

    async fn get_object(&self, req: S3Request<GetObjectInput>) -> S3Result<GetObjectOutput> {
        let req = req.input;
        let bucket = req.bucket;
        let object = req.key;

        let metadata = self
            .client
            .get_object(&GetObjectRequest {
                bucket: bucket.clone(),
                object: object.clone(),
                ..Default::default()
            })
            .await
            .unwrap();

        let mut range = Range(None, None);
        if let Some(S3Range::Int { first, last }) = req.range {
            range = Range(Some(first), last);
        }
        let res = self
            .client
            .download_streamed_object(
                &GetObjectRequest {
                    bucket,
                    object,
                    ..Default::default()
                },
                &range,
            )
            .await
            .unwrap();

        Ok(GetObjectOutput {
            body: Some(StreamingBlob::wrap(res)),
            content_length: metadata.size,
            last_modified: metadata.updated.map(|t| Timestamp::from(t)),
            ..Default::default()
        })
    }

    async fn put_object(&self, req: S3Request<PutObjectInput>) -> S3Result<PutObjectOutput> {
        let req = req.input;
        let bucket = req.bucket;
        let object = req.key;

        let res = self
            .client
            .upload_streamed_object(
                &UploadObjectRequest {
                    bucket,
                    ..Default::default()
                },
                req.body.unwrap().inner,
                &UploadType::Simple(Media::new(object)),
            )
            .await
            .unwrap();

        Ok(PutObjectOutput {
            e_tag: Some(res.etag),
            ..Default::default()
        })
    }

    async fn copy_object(&self, req: S3Request<CopyObjectInput>) -> S3Result<CopyObjectOutput> {
        let req = req.input;
        let destination_bucket = req.bucket;
        let destination_object = req.key;

        let CopySource::Bucket {
            bucket: source_bucket,
            key: source_object,
            version_id: _,
        } = req.copy_source else {
            panic!("Only bucket copy is supported");
        };

        let res = self
            .client
            .copy_object(&CopyObjectRequest {
                destination_bucket,
                destination_object,
                source_bucket: source_bucket.to_string(),
                source_object: source_object.to_string(),
                ..Default::default()
            })
            .await
            .unwrap();

        Ok(CopyObjectOutput {
            copy_object_result: Some(CopyObjectResult {
                e_tag: Some(res.etag),
                ..Default::default()
            }),
            ..Default::default()
        })
    }

    async fn create_multipart_upload(
        &self,
        _req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<CreateMultipartUploadOutput> {
        Ok(CreateMultipartUploadOutput {
            upload_id: Some(uuid::Uuid::new_v4().to_string()),
            ..Default::default()
        })
    }

    async fn upload_part(&self, req: S3Request<UploadPartInput>) -> S3Result<UploadPartOutput> {
        let req = req.input;
        let bucket = req.bucket;
        let object = req.key;
        let upload_id = req.upload_id;
        let part_number = req.part_number;

        let res = self
            .client
            .upload_streamed_object(
                &UploadObjectRequest {
                    bucket,
                    ..Default::default()
                },
                req.body.unwrap().inner,
                &UploadType::Simple(Media::new(format!(
                    "{}.sky-upload-{}.sky-multipart-{}",
                    object, upload_id, part_number
                ))),
            )
            .await
            .unwrap();

        Ok(UploadPartOutput {
            e_tag: Some(res.etag),
            ..Default::default()
        })
    }

    async fn upload_part_copy(
        &self,
        req: S3Request<UploadPartCopyInput>,
    ) -> S3Result<UploadPartCopyOutput> {
        let req = req.input;
        let destination_bucket = req.bucket;
        let destination_object = req.key;
        let upload_id = req.upload_id;
        let part_number = req.part_number;

        let CopySource::Bucket {
            bucket: source_bucket,
            key: source_object,
            version_id: _,
        } = req.copy_source else {
            panic!("Only bucket copy is supported");
        };

        let res = self
            .client
            .copy_object(&CopyObjectRequest {
                destination_bucket,
                destination_object: format!(
                    "{}.sky-upload-{}.sky-multipart-{}",
                    destination_object, upload_id, part_number
                ),
                source_bucket: source_bucket.to_string(),
                source_object: source_object.to_string(),
                ..Default::default()
            })
            .await
            .unwrap();

        Ok(UploadPartCopyOutput {
            copy_part_result: Some(CopyPartResult {
                e_tag: Some(res.etag),
                ..Default::default()
            }),
            ..Default::default()
        })
    }

    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<CompleteMultipartUploadOutput> {
        let req = req.input;
        let bucket = req.bucket;
        let object = req.key;
        let upload_id = req.upload_id;

        let mut parts: Vec<SourceObjects> = req
            .multipart_upload
            .unwrap()
            .parts
            .unwrap()
            .iter()
            .map(|part| {
                format!(
                    "{}.sky-upload-{}.sky-multipart-{}",
                    object, upload_id, part.part_number,
                )
            })
            .map(|s| SourceObjects {
                name: s.to_string(),
                ..Default::default()
            })
            .collect();

        if parts.len() > 32 {
            // GCS only supports compsing 1-32 objects. In this case,
            // we need to compose multiple times.
            let mut composed_parts = Vec::new();
            let mut compose_batch_id: usize = 0;
            while parts.len() > 32 {
                let composed_object = format!(
                    "{}.sky-upload-{}.sky-multipart-compose-batch-{}",
                    object, upload_id, compose_batch_id,
                );
                let res = self
                    .client
                    .compose_object(&ComposeObjectRequest {
                        bucket: bucket.clone(),
                        destination_object: composed_object,
                        composing_targets: ComposingTargets {
                            source_objects: parts.drain(..32).collect(),
                            ..Default::default()
                        },
                        ..Default::default()
                    })
                    .await
                    .unwrap();

                composed_parts.push(SourceObjects {
                    name: res.name,
                    ..Default::default()
                });

                compose_batch_id += 1;
            }
        }

        let res = self
            .client
            .compose_object(&ComposeObjectRequest {
                bucket,
                destination_object: object,
                composing_targets: ComposingTargets {
                    source_objects: parts,
                    ..Default::default()
                },
                ..Default::default()
            })
            .await
            .unwrap();

        // TODO: delete parts, high priority

        Ok(CompleteMultipartUploadOutput {
            e_tag: Some(res.etag),
            ..Default::default()
        })
    }
}

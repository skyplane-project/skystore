use crate::objstore_client::ObjectStoreClient;
use crate::utils::type_utils::parse_range;
use google_cloud_default::WithAuthExt;
use google_cloud_storage::client::{Client, ClientConfig};
use google_cloud_storage::http::buckets::delete::DeleteBucketRequest;
use google_cloud_storage::http::buckets::get::GetBucketRequest;
use google_cloud_storage::http::buckets::insert::{
    BucketCreationConfig, InsertBucketParam, InsertBucketRequest,
};
use google_cloud_storage::http::buckets::patch::BucketPatchConfig;
use google_cloud_storage::http::buckets::patch::PatchBucketRequest;
use google_cloud_storage::http::buckets::Versioning;
use google_cloud_storage::http::objects::compose::{ComposeObjectRequest, ComposingTargets};
use google_cloud_storage::http::objects::copy::CopyObjectRequest;
use google_cloud_storage::http::objects::delete::DeleteObjectRequest;
use google_cloud_storage::http::objects::download::Range;
use google_cloud_storage::http::objects::get::GetObjectRequest;
use google_cloud_storage::http::objects::list::ListObjectsRequest;
use google_cloud_storage::http::objects::upload::{Media, UploadObjectRequest, UploadType};
use google_cloud_storage::http::objects::SourceObjects;

use s3s::dto::{Range as S3Range, *};
use s3s::{S3Request, S3Response, S3Result};

pub struct GCPObjectStoreClient {
    client: Client,
}

impl GCPObjectStoreClient {
    #[allow(dead_code)]
    pub async fn new() -> Self {
        let config = ClientConfig::default().with_auth().await.unwrap();
        Self {
            client: Client::new(config),
        }
    }
}

#[async_trait::async_trait]
impl ObjectStoreClient for GCPObjectStoreClient {
    async fn create_bucket(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        let req = req.input;
        let bucket_name = req.bucket;
        let location = req
            .create_bucket_configuration
            .unwrap()
            .location_constraint
            .unwrap();

        let bucket_config = BucketCreationConfig {
            location: location.as_str().to_string(),
            ..Default::default()
        };

        // must obtain project id from the config first
        let config = ClientConfig::default().with_auth().await.unwrap();

        let insert_param = InsertBucketParam {
            project: config.project_id.unwrap(),
            ..Default::default()
        };

        let res = self
            .client
            .insert_bucket(&InsertBucketRequest {
                name: bucket_name,
                bucket: bucket_config,
                param: insert_param,
            })
            .await;

        match res {
            Ok(res) => Ok(S3Response::new(CreateBucketOutput {
                location: Some(res.location),
            })),
            Err(err) => Err(s3s::S3Error::with_message(
                s3s::S3ErrorCode::InternalError,
                format!("Failed to create bucket: {}", err),
            )),
        }
    }

    async fn delete_bucket(
        &self,
        req: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        let req = req.input;
        let bucket = req.bucket;

        self.client
            .delete_bucket(&DeleteBucketRequest {
                bucket,
                ..Default::default()
            })
            .await
            .unwrap();

        Ok(S3Response::new(DeleteBucketOutput::default()))
    }

    async fn head_bucket(
        &self,
        req: S3Request<HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        let req = req.input;
        let bucket = req.bucket;

        let _res = self
            .client
            .get_bucket(&GetBucketRequest {
                bucket,
                ..Default::default()
            })
            .await;

        Ok(S3Response::new(HeadBucketOutput {}))
    }

    async fn put_bucket_versioning(
        &self,
        req: S3Request<PutBucketVersioningInput>,
    ) -> S3Result<S3Response<PutBucketVersioningOutput>> {
        let req = req.input;
        let bucket_name = req.bucket;
        let versioning = match req.versioning_configuration.status.unwrap().as_str() {
            "Enabled" => true,
            "Suspended" => false,
            _ => {
                return Err(s3s::S3Error::with_message(
                    s3s::S3ErrorCode::InternalError,
                    "Failed to create bucket".to_string(),
                ))
            }
        };

        let _res = self
            .client
            .patch_bucket(&PatchBucketRequest {
                bucket: bucket_name,
                metadata: Some(BucketPatchConfig {
                    versioning: Some(Versioning {
                        enabled: versioning,
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            })
            .await;

        Ok(S3Response::new(PutBucketVersioningOutput {}))
    }

    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        let req = req.input;
        let bucket = req.bucket;
        let object = req.key;
        let version_id = req.version_id.map(|id| id.parse::<i64>().unwrap());

        let res = self
            .client
            .get_object(&GetObjectRequest {
                bucket,
                object,
                generation: version_id,
                ..Default::default()
            })
            .await
            .unwrap();

        // retrieve the version id from the generation
        let vid = res.generation.to_string();

        Ok(S3Response::new(HeadObjectOutput {
            e_tag: Some(res.etag),
            content_length: res.size,
            last_modified: res.updated.map(Timestamp::from),
            version_id: Some(vid),
            ..Default::default()
        }))
    }

    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let req = req.input;
        let bucket = req.bucket;
        let object = req.key;
        let version_id = req.version_id.map(|id| id.parse::<i64>().unwrap());

        let metadata = self
            .client
            .get_object(&GetObjectRequest {
                bucket: bucket.clone(),
                object: object.clone(),
                generation: version_id,
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
                    generation: version_id,
                    ..Default::default()
                },
                &range,
            )
            .await
            .unwrap();

        Ok(S3Response::new(GetObjectOutput {
            body: Some(StreamingBlob::wrap(res)),
            content_length: metadata.size,
            last_modified: metadata.updated.map(Timestamp::from),
            version_id: Some(metadata.generation.to_string()),
            ..Default::default()
        }))
    }

    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
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
                req.body.unwrap(),
                &UploadType::Simple(Media::new(object)),
            )
            .await
            .unwrap();

        Ok(S3Response::new(PutObjectOutput {
            e_tag: Some(res.etag),
            version_id: Some(res.generation.to_string()),
            ..Default::default()
        }))
    }

    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        let req = req.input;
        let bucket = req.bucket;
        let object = req.key;
        let version_id = req.version_id.map(|id| id.parse::<i64>().unwrap());

        self.client
            .delete_object(&DeleteObjectRequest {
                bucket,
                object,
                generation: version_id,
                ..Default::default()
            })
            .await
            .unwrap();

        Ok(S3Response::new(DeleteObjectOutput {
            delete_marker: false, // TODO: add versioning support
            version_id: Some(version_id.unwrap().to_string()),
            ..Default::default()
        }))
    }

    async fn copy_object(
        &self,
        req: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        let req = req.input;
        let destination_bucket = req.bucket;
        let destination_object = req.key;

        let CopySource::Bucket {
            bucket: source_bucket,
            key: source_object,
            version_id: _,
        } = req.copy_source
        else {
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

        Ok(S3Response::new(CopyObjectOutput {
            copy_object_result: Some(CopyObjectResult {
                e_tag: Some(res.etag),
                ..Default::default()
            }),
            version_id: Some(res.generation.to_string()),
            ..Default::default()
        }))
    }

    async fn create_multipart_upload(
        &self,
        _req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        Ok(S3Response::new(CreateMultipartUploadOutput {
            upload_id: Some(uuid::Uuid::new_v4().to_string()),
            ..Default::default()
        }))
    }

    async fn upload_part(
        &self,
        req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
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
                req.body.unwrap(),
                &UploadType::Simple(Media::new(format!(
                    "{object}.sky-upload-{upload_id}.sky-multipart-{part_number}"
                ))),
            )
            .await
            .unwrap();

        Ok(S3Response::new(UploadPartOutput {
            e_tag: Some(res.etag),
            ..Default::default()
        }))
    }

    async fn upload_part_copy(
        &self,
        req: S3Request<UploadPartCopyInput>,
    ) -> S3Result<S3Response<UploadPartCopyOutput>> {
        let req = req.input;
        let destination_bucket = req.bucket;
        let destination_object = req.key;
        let upload_id = req.upload_id;
        let part_number = req.part_number;

        let CopySource::Bucket {
            bucket: source_bucket,
            key: source_object,
            version_id: _,
        } = req.copy_source
        else {
            panic!("Only bucket copy is supported");
        };

        if let Some(range) = req.copy_source_range {
            // use the metadata together with the range given to decide whether
            // we should use the upload_from_file or upload_streamed_object
            let metadata = self
                .client
                .get_object(&GetObjectRequest {
                    bucket: source_bucket.to_string(),
                    object: source_object.to_string(),
                    ..Default::default()
                })
                .await
                .unwrap();

            let (start, end) = parse_range(&range);
            let max = end.unwrap_or(u64::MAX);
            // NOTE: threshold to 2GB, alternatively writing to a temporary file
            let cutoff = 2 * 1024 * 1024 * 1024;
            if metadata.size as u64 - start > cutoff && max - start > cutoff {
                panic!("The range is too large, try to reduce the range to be less than 2GB!");
            }
            let range = Range(Some(start), end);
            // first download the object within the range selected
            let res = self
                .client
                .download_streamed_object(
                    &GetObjectRequest {
                        bucket: source_bucket.to_string(),
                        object: source_object.to_string(),
                        ..Default::default()
                    },
                    &range,
                )
                .await
                .unwrap();
            let body = StreamingBlob::wrap(res);
            // now upload the selected range to the destination bucket
            let res = self
                .client
                .upload_streamed_object(
                    &UploadObjectRequest {
                        bucket: destination_bucket.clone(),
                        ..Default::default()
                    },
                    body,
                    &UploadType::Simple(Media::new(format!(
                        "{destination_object}.sky-upload-{upload_id}.sky-multipart-{part_number}"
                    ))),
                )
                .await
                .unwrap();

            Ok(S3Response::new(UploadPartCopyOutput {
                copy_part_result: Some(CopyPartResult {
                    e_tag: Some(res.etag),
                    ..Default::default()
                }),
                ..Default::default()
            }))
        } else {
            let res = self
                .client
                .copy_object(&CopyObjectRequest {
                    destination_bucket,
                    destination_object: format!(
                        "{destination_object}.sky-upload-{upload_id}.sky-multipart-{part_number}"
                    ),
                    source_bucket: source_bucket.to_string(),
                    source_object: source_object.to_string(),
                    ..Default::default()
                })
                .await
                .unwrap();

            return Ok(S3Response::new(UploadPartCopyOutput {
                copy_part_result: Some(CopyPartResult {
                    e_tag: Some(res.etag),
                    ..Default::default()
                }),
                ..Default::default()
            }));
        }
    }

    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        let req = req.input;
        let bucket = req.bucket;
        let bucket_clone = bucket.clone();
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
                name: s,
                ..Default::default()
            })
            .collect();

        let mut to_delete: Vec<SourceObjects> = parts.clone();

        if parts.len() > 32 {
            // GCS only supports compsing 1-32 objects. In this case,
            // we need to compose multiple times.
            let mut next_composed_parts = Vec::new();
            let mut compose_batch_id: usize = 0;
            let mut curr_composed_parts = parts;
            let mut level = 0;
            while curr_composed_parts.len() > 32 {
                let composed_object = format!(
                    "{object}.sky-upload-{upload_id}.sky-multipart-compose-batch-{level}-{compose_batch_id}",
                );
                let res = self
                    .client
                    .compose_object(&ComposeObjectRequest {
                        bucket: bucket.clone(),
                        destination_object: composed_object.clone(),
                        composing_targets: ComposingTargets {
                            source_objects: curr_composed_parts.drain(..32).collect(),
                            ..Default::default()
                        },
                        ..Default::default()
                    })
                    .await
                    .unwrap();

                next_composed_parts.push(SourceObjects {
                    name: res.name,
                    ..Default::default()
                });

                to_delete.push(SourceObjects {
                    name: composed_object,
                    ..Default::default()
                });

                compose_batch_id += 1;

                // when do we need to start next round merging?
                if curr_composed_parts.len() < 32
                    && curr_composed_parts.len() + next_composed_parts.len() >= 32
                {
                    level += 1;
                    compose_batch_id = 0;
                    // add this level's remaining composed parts to the next round composed parts vector
                    next_composed_parts.append(&mut curr_composed_parts.clone());
                    std::mem::swap(&mut curr_composed_parts, &mut next_composed_parts);
                    next_composed_parts.clear();
                } else if curr_composed_parts.len() + next_composed_parts.len() < 32 {
                    // loop end forever
                    next_composed_parts.append(&mut curr_composed_parts.clone());
                }
            }
            // move back to the parts vector
            parts = next_composed_parts;
        }

        // finally, merging the remaining parts, which is less than 32 definitely

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

        // Delete the intermediate parts and objects
        // TODO: consider sending a batch request or parallelize it.
        for obj in to_delete {
            self.client
                .delete_object(&DeleteObjectRequest {
                    bucket: bucket_clone.clone(),
                    object: obj.name,
                    ..Default::default()
                })
                .await
                .unwrap();
        }

        Ok(S3Response::new(CompleteMultipartUploadOutput {
            e_tag: Some(res.etag),
            version_id: Some(res.generation.to_string()),
            ..Default::default()
        }))
    }

    async fn abort_multipart_upload(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        let req = req.input;
        let bucket = req.bucket;
        let object = req.key;
        let upload_id = req.upload_id;

        // List all objects with the prefix
        let prefix = format!("{}.sky-upload-{}.", object, upload_id);
        let objects_to_delete = self
            .client
            .list_objects(&ListObjectsRequest {
                bucket: bucket.clone(),
                prefix: Some(prefix.clone()),
                ..Default::default()
            })
            .await
            .unwrap()
            .items
            .unwrap_or_default();

        // Delete all the objects that were listed as parts of this upload
        for obj in objects_to_delete {
            self.client
                .delete_object(&DeleteObjectRequest {
                    bucket: bucket.clone(),
                    object: obj.name,
                    ..Default::default()
                })
                .await
                .unwrap(); // TODO: error handlings
        }

        Ok(S3Response::new(AbortMultipartUploadOutput::default()))
    }
}

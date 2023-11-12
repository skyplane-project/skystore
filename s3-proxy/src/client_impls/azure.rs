use crate::{objstore_client::ObjectStoreClient, utils::type_utils::parse_range};
use azure_core::{Body, SeekableStream};
use azure_storage::StorageCredentials;
use azure_storage_blobs::prelude::*;
use futures::io::AsyncRead;
use futures_lite::stream::StreamExt;
use s3s::{dto::*, stream::ByteStream, S3Response};
use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use azure_storage::shared_access_signature::service_sas::BlobSasPermissions;
use s3s::{S3Request, S3Result};

// use azure_svc_blobstorage::*;
// use azure_identity::DefaultAzureCredential;

pub struct AzureObjectStoreClient {
    client: BlobServiceClient,
    // version_enable: bool,
}

#[derive(Debug)]
struct SeekableBlobWrapper {
    blob: Arc<Mutex<StreamingBlob>>,
    initial_size: usize,
    buf: Vec<u8>,
}

impl SeekableBlobWrapper {
    fn new(blob: StreamingBlob) -> Self {
        let initial_size = blob.remaining_length().exact().unwrap();
        Self {
            blob: Arc::new(Mutex::new(blob)),
            initial_size,
            buf: Vec::new(),
        }
    }
}

impl From<SeekableBlobWrapper> for Body {
    fn from(val: SeekableBlobWrapper) -> Self {
        Body::SeekableStream(Box::new(val))
    }
}

impl Clone for SeekableBlobWrapper {
    fn clone(&self) -> Self {
        Self {
            blob: self.blob.clone(),
            initial_size: self.initial_size,
            buf: Vec::new(),
        }
    }
}

impl AsyncRead for SeekableBlobWrapper {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let this = self.get_mut();

        // if we have some data left over from the last read, use it first.
        if !this.buf.is_empty() {
            let len = std::cmp::min(buf.len(), this.buf.len());
            buf[..len].copy_from_slice(&this.buf[..len]);

            if this.buf.len() > len {
                // we have some left over data, keep it for next time.
                this.buf = this.buf[len..].to_vec();
            } else {
                this.buf.clear();
            }

            return Poll::Ready(Ok(len));
        }

        let item = Pin::new(&mut this.blob.lock().unwrap()).poll_next(cx);
        match item {
            Poll::Ready(Some(Ok(bytes))) => {
                let len = std::cmp::min(buf.len(), bytes.len());
                buf[..len].copy_from_slice(&bytes[..len]);

                if bytes.len() > len {
                    // we have some left over data, keep it for next time.
                    this.buf = bytes[len..].to_vec();
                }

                Poll::Ready(Ok(len))
            }
            Poll::Ready(Some(Err(err))) => Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("read error: {err}"),
            ))),
            Poll::Ready(None) => Poll::Ready(Ok(0)),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[async_trait::async_trait]
impl SeekableStream for SeekableBlobWrapper {
    async fn reset(&mut self) -> azure_core::error::Result<()> {
        Ok(())
    }
    fn len(&self) -> usize {
        self.initial_size
    }
}

impl AzureObjectStoreClient {
    #[allow(dead_code)]
    pub async fn new(/* version_enable: bool */) -> Self {
        let account = std::env::var("STORAGE_ACCOUNT").expect("missing STORAGE_ACCOUNT");
        let access_key = std::env::var("STORAGE_ACCESS_KEY").expect("missing STORAGE_ACCOUNT_KEY");
        let storage_credentials = StorageCredentials::access_key(account.clone(), access_key);

        let client = ClientBuilder::new(account, storage_credentials)
            // .retry(RetryOptions::none())
            .blob_service_client();
        // let client = ClientBuilder::emulator().blob_service_client();
        // let service_client = azure_svc_blobstorage::Client::builder(storage_credentials);
        // act as healthcheck
        client.list_containers().into_stream().next().await;

        Self {
            client,
            // version_enable,
        }
    }

    fn blob_client(&self, container_name: &String, blob_name: &String) -> BlobClient {
        self.client
            .clone()
            .container_client(container_name)
            .blob_client(blob_name)
    }

    fn container_client(&self, container_name: &String) -> ContainerClient {
        self.client.clone().container_client(container_name)
    }
}

#[async_trait::async_trait]
impl ObjectStoreClient for AzureObjectStoreClient {
    async fn create_bucket(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        let req = req.input;
        let container_name = req.bucket;

        let container_client = self.container_client(&container_name);
        let resp = container_client.create().await;

        match resp {
            Ok(_) => Ok(S3Response::new(CreateBucketOutput {
                location: Some(container_name.clone()),
            })),
            Err(err) => Err(s3s::S3Error::with_message(
                s3s::S3ErrorCode::InternalError,
                format!("Request failed: {err}"),
            )),
        }
    }

    async fn put_bucket_versioning(
        &self,
        req: S3Request<PutBucketVersioningInput>,
    ) -> S3Result<S3Response<PutBucketVersioningOutput>> {
        println!("put_bucket_versioning: {:?}", req);
        println!("Azure currently does not support bucket level versioning");
        // since Azure does not support container level version control natively,
        // but support blob version control.
        // this op will enable version control for all the objects in the current container
        // let req = req.input;
        // let container_name = req.bucket;
        // let container_client = self.container_client(&container_name);
        // let blobs = container_client
        //     .list_blobs()
        //     .into_stream()
        //     .next()
        //     .await
        //     .expect("list blobs next() should return value")
        //     .expect("list blobs should succeed");
        // let blob_list = blobs.blobs.blobs().collect::<Vec<_>>();
        // if blob_list.len() > 0 {
        // for blob in blob_list {
        // let blob_client = self.blob_client(&container_name, &blob.name);
        // let cur_properties = blob_client.get_properties().await.unwrap();
        // let properties = blob_client.set_properties().await.unwrap();
        // properties.
        // let resp = blob_client
        // .set_properties()
        // .set_blob_versioning(BlobVersioning::Enabled)
        // .await;
        // match resp {
        //     Ok(_) => (),
        //     Err(err) => Err(s3s::S3Error::with_message(
        //         s3s::S3ErrorCode::InternalError,
        //         format!("Request failed: {err}"),
        //     )),
        // }
        // }
        // }
        return Ok(S3Response::new(PutBucketVersioningOutput::default()));
    }

    async fn delete_bucket(
        &self,
        req: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        let req = req.input;
        let container_name = req.bucket;

        let container_client = self.container_client(&container_name);
        let resp = container_client.delete().await;

        match resp {
            Ok(_) => Ok(S3Response::new(DeleteBucketOutput::default())),
            Err(err) => Err(s3s::S3Error::with_message(
                s3s::S3ErrorCode::InternalError,
                format!("Request failed: {}", err),
            )),
        }
    }

    // NOTE: in the current impl, we are just going to map bucket to
    // container and key to blob. Theoretically, bucket should be
    // mapped to storage account and key is mapped to container+blob.
    // A future follow up work.
    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        let req = req.input;
        let container_name = req.bucket;
        let blob_name = req.key;

        let blob_client = self.blob_client(&container_name, &blob_name);
        let resp = blob_client.get_properties().await;
        match resp {
            Ok(resp) => Ok(S3Response::new(HeadObjectOutput {
                content_length: resp.blob.properties.content_length as i64,
                e_tag: Some(resp.blob.properties.etag.to_string()),
                last_modified: Some(resp.blob.properties.last_modified.into()),
                ..Default::default()
            })),
            // TODO: handle 404 better here.
            Err(err) => Err(s3s::S3Error::with_message(
                s3s::S3ErrorCode::InternalError,
                format!("Request failed: {err}"),
            )),
        }
    }

    async fn head_bucket(
        &self,
        req: S3Request<HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        let req = req.input;
        let container_name = req.bucket;

        let container_client = self.container_client(&container_name);
        let _resp = container_client.exists().await;
        match _resp {
            Ok(_) => Ok(S3Response::new(HeadBucketOutput {})),
            Err(err) => Err(s3s::S3Error::with_message(
                s3s::S3ErrorCode::InternalError,
                format!("Request failed: {err}"),
            )),
        }
    }

    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let req = req.input;
        let container_name = req.bucket;
        let blob_name = req.key;

        let blob_client = self.blob_client(&container_name, &blob_name);
        // The chunksize is used by SDK to return iterator, we actually don't want this behavior.
        let mut request_builder = blob_client.get().chunk_size(u64::MAX);
        if let Some(Range::Int {
            first: start,
            last: end,
        }) = req.range
        {
            let end = end.unwrap_or(u64::MAX);
            request_builder = request_builder.range(start..end);
        }
        let resp = request_builder.into_stream();
        let mut resp: Vec<_> = resp.collect().await;
        assert_eq!(resp.len(), 1);
        let resp = resp.pop().unwrap();

        match resp {
            Ok(resp) => Ok(S3Response::new(GetObjectOutput {
                body: Some(StreamingBlob::wrap(resp.data)),
                content_length: resp.blob.properties.content_length as i64,
                e_tag: Some(resp.blob.properties.etag.to_string()),
                last_modified: Some(resp.blob.properties.last_modified.into()),
                ..Default::default()
            })),
            Err(err) => Err(s3s::S3Error::with_message(
                s3s::S3ErrorCode::InternalError,
                format!("Request failed: {err}"),
            )),
        }
    }

    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        let req = req.input;
        let container_name = req.bucket;
        let blob_name = req.key;

        // https://github.com/Azure/azure-sdk-for-rust/issues/1121
        // :angry:
        // TODO: write this up for future me :(

        let blob_client = self.blob_client(&container_name, &blob_name);
        let input_stream = SeekableBlobWrapper::new(req.body.unwrap());
        let resp = blob_client.put_block_blob(input_stream).await;

        match resp {
            Ok(resp) => Ok(S3Response::new(PutObjectOutput {
                e_tag: Some(resp.etag),
                ..Default::default()
            })),
            Err(err) => Err(s3s::S3Error::with_message(
                s3s::S3ErrorCode::InternalError,
                format!("Request failed: {err}"),
            )),
        }
    }

    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        let req = req.input;
        let container_name = req.bucket;
        let blob_name = req.key;

        let blob_client = self.blob_client(&container_name, &blob_name);
        let resp = blob_client.delete().await;

        match resp {
            Ok(_resp) => Ok(S3Response::new(DeleteObjectOutput {
                delete_marker: false, // TODO: add versioning support
                version_id: None,
                ..Default::default()
            })),
            Err(err) => Err(s3s::S3Error::with_message(
                s3s::S3ErrorCode::InternalError,
                format!("Request failed: {err}"),
            )),
        }
    }

    async fn copy_object(
        &self,
        req: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        let req = req.input;

        let container_name = req.bucket;
        let blob_name = req.key;

        let CopySource::Bucket {
            bucket: src_container,
            key: src_blob,
            version_id: _,
        } = req.copy_source
        else {
            panic!("AccessPoint copy not supported");
        };
        let src_blob_url = self
            .blob_client(&src_container.to_string(), &src_blob.to_string())
            .url()
            .unwrap();

        let blob_client = self.blob_client(&container_name, &blob_name);
        let resp = blob_client.copy(src_blob_url).await.unwrap();

        Ok(S3Response::new(CopyObjectOutput {
            copy_object_result: Some(CopyObjectResult {
                e_tag: Some(resp.etag.to_string()),
                last_modified: Some(resp.last_modified.into()),
                ..Default::default()
            }),
            ..Default::default()
        }))
    }

    async fn create_multipart_upload(
        &self,
        _req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        // Azure doesn't need you to create mutlipart ahead of time.
        Ok(S3Response::new(CreateMultipartUploadOutput {
            upload_id: Some(uuid::Uuid::new_v4().to_string()),
            ..Default::default()
        }))
    }

    async fn upload_part(
        &self,
        req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        // Azure requires the block id to be the same length so we pad the part number to full four bytes
        // The e_tag is no needed for complete upload. We just stuff it with md5 but it's not necessary.
        let req = req.input;
        let container_name = req.bucket;
        let blob_name = req.key;
        let upload_id = req.upload_id;
        let part_number = req.part_number;
        assert!(part_number > 0 && part_number < 10000);

        let blob_client = self.blob_client(&container_name, &blob_name);
        let input_stream = SeekableBlobWrapper::new(req.body.unwrap());
        let block_id = format!("{upload_id}-{part_number:04}");
        let resp = blob_client.put_block(block_id, input_stream).await;

        match resp {
            Ok(resp) => Ok(S3Response::new(UploadPartOutput {
                // Azure doesn't return checksum unless you also provide the checksum.
                e_tag: Some(resp.request_id.to_string()),
                ..Default::default()
            })),
            Err(err) => Err(s3s::S3Error::with_message(
                s3s::S3ErrorCode::InternalError,
                format!("Request failed: {err}"),
            )),
        }
    }

    async fn upload_part_copy(
        &self,
        req: S3Request<UploadPartCopyInput>,
    ) -> S3Result<S3Response<UploadPartCopyOutput>> {
        let req = req.input;
        let container_name = req.bucket;
        let blob_name = req.key;
        let upload_id = req.upload_id;
        let part_number = req.part_number;

        let CopySource::Bucket {
            bucket: src_container,
            key: src_blob,
            version_id: _,
        } = req.copy_source
        else {
            panic!("AccessPoint copy not supported");
        };

        let src_blob_client = self.blob_client(&src_container.to_string(), &src_blob.to_string());
        let token = src_blob_client
            .shared_access_signature(
                BlobSasPermissions {
                    read: true,
                    ..Default::default()
                },
                time::OffsetDateTime::now_utc().saturating_add(time::Duration::days(2)),
            )
            .await
            .unwrap();
        let src_block_url = src_blob_client.generate_signed_blob_url(&token).unwrap();

        let blob_client = self.blob_client(&container_name, &blob_name);
        let block_id = format!("{upload_id}-{part_number:04}");
        let (mut start, mut end) = (0, Some(u64::MAX));
        if let Some(range) = req.copy_source_range {
            (start, end) = parse_range(&range);
        }

        let mut end = end.unwrap_or(u64::MAX);
        if end != u64::MAX {
            end = end.saturating_add(1);
        }
        let resp = blob_client
            .put_block_url(block_id, src_block_url)
            .range(start..end)
            .await;
        match resp {
            Ok(resp) => Ok(S3Response::new(UploadPartCopyOutput {
                copy_part_result: Some(CopyPartResult {
                    e_tag: Some(resp.request_id.to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            })),
            Err(err) => Err(s3s::S3Error::with_message(
                s3s::S3ErrorCode::InternalError,
                format!("Upload_part_copy failed: {err}"),
            )),
        }
    }

    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        let req = req.input;
        let container_name = req.bucket;
        let blob_name = req.key;
        let upload_id = req.upload_id;

        let blob_client = self.blob_client(&container_name, &blob_name);

        let block_list = BlockList {
            blocks: req
                .multipart_upload
                .unwrap()
                .parts
                .unwrap()
                .into_iter()
                .map(|part| {
                    BlobBlockType::new_latest(format!("{}-{:04}", upload_id, part.part_number))
                })
                .collect(),
        };

        let resp = blob_client.put_block_list(block_list).await.unwrap();

        Ok(S3Response::new(CompleteMultipartUploadOutput {
            bucket: Some(container_name),
            key: Some(blob_name),
            e_tag: Some(resp.etag),
            ..Default::default()
        }))
    }

    async fn abort_multipart_upload(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        let req = req.input;
        let container_name = req.bucket;
        let blob_name = req.key;
        let upload_id = req.upload_id;
        let blob_client = self.blob_client(&container_name, &blob_name);

        // List all the uncommitted blocks associated with the upload ID
        let block_list_result = blob_client
            .get_block_list()
            .block_list_type(BlockListType::Uncommitted)
            .await;

        match block_list_result {
            Ok(block_list) => {
                let blocks_to_retain: Vec<_> = block_list
                    .block_with_size_list
                    .blocks
                    .iter()
                    .filter(|block_with_size| {
                        // Extract block ID and check if it starts with the upload ID
                        if let BlobBlockType::Uncommitted(block_id) =
                            &block_with_size.block_list_type
                        {
                            let block_id_str =
                                String::from_utf8(block_id.bytes().to_vec()).unwrap_or_default();
                            !block_id_str.starts_with(&format!("{}-", upload_id))
                        } else {
                            true
                        }
                    })
                    .map(|block_with_size| block_with_size.block_list_type.clone())
                    .collect();

                let block_list = BlockList {
                    blocks: blocks_to_retain,
                };

                // Not include the blocks with the matching prefix. Azure doesn't have a delete block API.
                let put_block_list_result = blob_client.put_block_list(block_list).await;

                match put_block_list_result {
                    Ok(_) => Ok(S3Response::new(AbortMultipartUploadOutput {
                        ..Default::default()
                    })),
                    Err(err) => Err(s3s::S3Error::with_message(
                        s3s::S3ErrorCode::InternalError,
                        format!("Failed to update block list: {}", err),
                    )),
                }
            }
            Err(err) => Err(s3s::S3Error::with_message(
                s3s::S3ErrorCode::InternalError,
                format!("Failed to get block list: {}", err),
            )),
        }
    }
}

use crate::objstore_client::ObjectStoreClient;
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

pub struct AzureObjectStoreClient {
    client: BlobServiceClient,
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
    pub async fn new() -> Self {
        let account = std::env::var("STORAGE_ACCOUNT").expect("missing STORAGE_ACCOUNT");
        let access_key = std::env::var("STORAGE_ACCESS_KEY").expect("missing STORAGE_ACCOUNT_KEY");
        let storage_credentials = StorageCredentials::Key(account.clone(), access_key);

        let client = ClientBuilder::new(account, storage_credentials)
            // .retry(RetryOptions::none())
            .blob_service_client();
        // let client = ClientBuilder::emulator().blob_service_client();

        // act as healthcheck
        client.list_containers().into_stream().next().await;

        Self { client }
    }

    fn blob_client(&self, container_name: &String, blob_name: &String) -> BlobClient {
        self.client
            .clone()
            .container_client(container_name)
            .blob_client(blob_name)
    }
}

#[async_trait::async_trait]
impl ObjectStoreClient for AzureObjectStoreClient {
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
        } = req.copy_source else {
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
        } = req.copy_source else {
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
            .unwrap();
        let src_block_url = src_blob_client.generate_signed_blob_url(&token).unwrap();

        let blob_client = self.blob_client(&container_name, &blob_name);
        let block_id = format!("{upload_id}-{part_number:04}");
        let resp = blob_client
            .put_block_url(block_id, src_block_url)
            .await
            .unwrap();

        Ok(S3Response::new(UploadPartCopyOutput {
            copy_part_result: Some(CopyPartResult {
                // e_tag: Some(resp.content_md5.unwrap().bytes().encode_hex::<String>()),
                e_tag: Some(resp.request_id.to_string()),
                ..Default::default()
            }),
            ..Default::default()
        }))
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
}

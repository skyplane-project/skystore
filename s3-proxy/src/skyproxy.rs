use crate::objstore_client::ObjectStoreClient;
use crate::utils::stream_utils::split_streaming_blob;
use crate::utils::type_utils::*;

use bytes::{Bytes, BytesMut};
use chrono::Utc;
use core::panic;
use futures::stream::{Stream, StreamExt};
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use s3s::stream::ByteStream;
use s3s::{dto::*, Body};
use s3s::{S3Request, S3Response, S3Result, S3};
use skystore_rust_client::apis::configuration::Configuration;
use skystore_rust_client::apis::default_api as apis;
use skystore_rust_client::models::{self};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Instant, SystemTime};
use tokio_util::io::StreamReader;
use tracing::error;

pub struct SkyProxy {
    pub store_clients: HashMap<String, Arc<Box<dyn ObjectStoreClient>>>,
    pub dir_conf: Configuration,
    pub client_from_region: String,
    pub get_policy: String,
    pub put_policy: String,
    pub skystore_bucket_prefix: String,
    pub version_enable: String,
}

impl SkyProxy {
    pub async fn new(
        regions: Vec<String>,
        client_from_region: String,
        local: bool,
        local_server: bool,
        policy: (String, String),
        skystore_bucket_prefix: String,
        version_enable: String,
    ) -> Self {
        let mut store_clients = HashMap::new();

        if local {
            // Local test configuration
            for r in regions {
                let split: Vec<&str> = r.splitn(2, ':').collect();
                let (_, region) = (split[0], split[1]);

                let client = Arc::new(Box::new(
                    crate::client_impls::s3::S3ObjectStoreClient::new(
                        "http://localhost:8014".to_string(),
                    )
                    .await,
                ) as Box<dyn ObjectStoreClient>);

                store_clients.insert(r.to_string(), client.clone());

                // if bucket not exists, create one
                let skystore_bucket_name = format!("{}-{}", skystore_bucket_prefix, region);
                match client
                    .create_bucket(S3Request::new(new_create_bucket_request(
                        skystore_bucket_name.clone(),
                        Some(r),
                    )))
                    .await
                {
                    Ok(_) => {}
                    Err(e) => {
                        if e.to_string().contains("BucketAlreadyExists") {
                        } else {
                            panic!("Failed to create bucket: {}", e);
                        }
                    }
                };
                // if creating bucket succeed or the bucket already exists, we can continue to check whether versioning
                // status should change this time
                if version_enable != *"NULL" {
                    match client
                        .put_bucket_versioning(S3Request::new(new_put_bucket_versioning_request(
                            skystore_bucket_name.clone(),
                            VersioningConfiguration {
                                status: Some(BucketVersioningStatus::from(version_enable.clone())),
                                ..Default::default()
                            },
                        )))
                        .await
                    {
                        Ok(_) => {}
                        Err(e) => {
                            panic!("Failed to change bucket versioning status: {}", e)
                        }
                    }
                }
            }
        } else {
            // Real object store configuration (regions: init regions)
            for r in regions {
                let split: Vec<&str> = r.splitn(2, ':').collect();
                let (provider, region) = (split[0], split[1]);

                let client: Box<dyn ObjectStoreClient> = match provider {
                    "azure" => {
                        Box::new(crate::client_impls::azure::AzureObjectStoreClient::new().await)
                    }
                    "gcp" => Box::new(crate::client_impls::gcp::GCPObjectStoreClient::new().await),
                    "aws" => Box::new(
                        crate::client_impls::s3::S3ObjectStoreClient::new(format!(
                            "https://s3.{}.amazonaws.com",
                            region
                        ))
                        .await,
                    ),
                    _ => panic!("Unknown provider: {}", provider),
                };

                let client_arc = Arc::new(client);
                store_clients.insert(r.to_string(), client_arc.clone());
                // if bucket not exists, create one
                let skystore_bucket_name = format!("{}-{}", skystore_bucket_prefix, region);
                let bucket_region = if provider == "aws" || provider == "gcp" {
                    Some(region.to_string())
                } else {
                    None
                };
                let mut bucket_exists = true;
                match client_arc
                    .head_bucket(S3Request::new(new_head_bucket_request(
                        skystore_bucket_name.clone(),
                        bucket_region.clone(),
                    )))
                    .await
                {
                    Ok(_) => {}
                    Err(e) => {
                        if e.to_string().contains("BucketAlreadyOwnedByYou")
                            || http::StatusCode::INTERNAL_SERVER_ERROR == e.status_code().unwrap()
                        {
                            // Bucket already exists, no action needed
                        } else {
                            //panic!("Bbucket: {} not exists", e);
                            bucket_exists = false;
                        }
                    }
                };
                if !bucket_exists {
                    match client_arc
                        .create_bucket(S3Request::new(new_create_bucket_request(
                            skystore_bucket_name.clone(),
                            bucket_region.clone(),
                        )))
                        .await
                    {
                        Ok(_) => {}
                        Err(e) => {
                            if e.to_string().contains("BucketAlreadyOwnedByYou")
                                || http::StatusCode::INTERNAL_SERVER_ERROR
                                    == e.status_code().unwrap()
                            {
                                // Bucket already exists, no action needed
                            } else {
                                panic!("Failed to create bucket: {}", e);
                            }
                        }
                    }
                }
                // if creating bucket succeed or the bucket already exists, we can continue to check whether versioning
                // status should change this time
                if version_enable != *"NULL" {
                    match client_arc
                        .put_bucket_versioning(S3Request::new(new_put_bucket_versioning_request(
                            skystore_bucket_name.clone(),
                            VersioningConfiguration {
                                status: Some(BucketVersioningStatus::from(version_enable.clone())),
                                ..Default::default()
                            },
                        )))
                        .await
                    {
                        Ok(_) => {}
                        Err(e) => {
                            panic!("Failed to change bucket versioning status: {}", e)
                        }
                    }
                }
            }
        }

        let dir_conf = Configuration {
            base_path: if local_server {
                "http://127.0.0.1:3000".to_string()
            } else {
                // NOTE: ip address set to be the remote store-server addr
                "http://54.183.123.82:3000".to_string()
            },
            ..Default::default()
        };

        apis::healthz(&dir_conf)
            .await
            .expect("directory service not healthy");

        // set policy
        apis::update_policy(
            &dir_conf,
            models::SetPolicyRequest {
                get_policy: Some(policy.0.clone()),
                put_policy: Some(policy.1.clone()),
            },
        )
        .await
        .expect("update policy failed");

        Self {
            store_clients,
            dir_conf,
            client_from_region,
            get_policy: policy.0,
            put_policy: policy.1,
            skystore_bucket_prefix,
            version_enable,
        }
    }
}

impl Clone for SkyProxy {
    fn clone(&self) -> Self {
        Self {
            store_clients: self.store_clients.clone(),
            dir_conf: self.dir_conf.clone(),
            client_from_region: self.client_from_region.clone(),
            skystore_bucket_prefix: self.skystore_bucket_prefix.clone(),
            get_policy: self.get_policy.clone(),
            put_policy: self.put_policy.clone(),
            version_enable: self.version_enable.clone(),
        }
    }
}

// Needed for S3 trait
impl std::fmt::Debug for SkyProxy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let client_keys: Vec<&String> = self.store_clients.keys().collect();

        // Add fields to the formatter.
        f.debug_struct("SkyProxy")
            .field("store_clients", &client_keys)
            .field("dir_conf", &self.dir_conf)
            .field("client_from_region", &self.client_from_region)
            .field("policy", &self.get_policy)
            .field("policy", &self.put_policy)
            .finish()
    }
}

impl SkyProxy {
    async fn locate_object(
        &self,
        bucket: String,
        key: String,
        version_id: Option<i32>,
    ) -> Result<Option<models::LocateObjectResponse>, s3s::S3Error> {
        let locate_resp = apis::locate_object(
            &self.dir_conf,
            models::LocateObjectRequest {
                bucket,
                key,
                client_from_region: self.client_from_region.clone(),
                version_id,
            },
        )
        .await;

        match locate_resp {
            Ok(locator) => Ok(Some(locator)),
            Err(error) => {
                let is_404 = locate_response_is_404(&error);

                if is_404 {
                    Ok(None)
                } else {
                    Err(s3s::S3Error::with_message(
                        s3s::S3ErrorCode::InternalError,
                        "locate_object failed",
                    ))
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl S3 for SkyProxy {
    ///////////////////////////////////////////////////////////////////////////
    /// We start with the "blanket impl" that don't need actual operation
    /// from the physical store clients.
    #[tracing::instrument(level = "info")]
    async fn list_objects(
        &self,
        req: S3Request<ListObjectsInput>,
    ) -> S3Result<S3Response<ListObjectsOutput>> {
        // let start_time = Instant::now();

        let v2 = self
            .list_objects_v2(req.map_input(Into::into))
            .await?
            .output;

        let output = ListObjectsOutput {
            contents: v2.contents,
            delimiter: v2.delimiter,
            encoding_type: v2.encoding_type,
            name: v2.name,
            prefix: v2.prefix,
            max_keys: v2.max_keys,
            ..Default::default()
        };

        // let end_time = Instant::now();

        // write_time_duration_to_file(
        //     "LIST OBJECTS".to_string(),
        //     (end_time - start_time).as_millis(),
        //     "measure.txt",
        // );

        Ok(S3Response::new(output))
    }
    ///////////////////////////////////////////////////////////////////////////

    #[tracing::instrument(level = "info")]
    async fn create_bucket(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        // Send start create bucket request
        let create_bucket_resp = apis::start_create_bucket(
            &self.dir_conf,
            models::CreateBucketRequest {
                bucket: req.input.bucket.clone(),
                client_from_region: self.client_from_region.clone(),
                warmup_regions: None, // TODO
            },
        )
        .await
        .unwrap();

        // Create bucket in actual storages
        let mut tasks = tokio::task::JoinSet::new();
        let locators = create_bucket_resp.locators;

        for locator in locators {
            let client: Arc<Box<dyn ObjectStoreClient>> =
                self.store_clients.get(&locator.tag).unwrap().clone();

            let bucket_name = locator.bucket.clone();
            let dir_conf = self.dir_conf.clone();

            // TODO: fix
            // if bucket name starts with $skystore_bucket_prefix, then it's a skybucket already created during initialization
            // so we don't need to create it again
            if bucket_name.starts_with(&self.skystore_bucket_prefix) {
                let system_time: SystemTime = Utc::now().into();
                let timestamp: s3s::dto::Timestamp = system_time.into();

                apis::complete_create_bucket(
                    &dir_conf,
                    models::CreateBucketIsCompleted {
                        id: locator.id,
                        creation_date: timestamp_to_string(timestamp), // TODO: fix this, get metadata from actual storage
                    },
                )
                .await
                .unwrap();
                continue;
            }

            let version_enable = self.version_enable.clone();

            tasks.spawn(async move {
                let create_bucket_req = new_create_bucket_request(bucket_name.clone(), None);
                client
                    .create_bucket(S3Request::new(create_bucket_req))
                    .await
                    .unwrap();

                // change bucket versioning setting from NULL to Enabled/Suspended
                if version_enable != *"NULL" {
                    client
                        .put_bucket_versioning(S3Request::new(new_put_bucket_versioning_request(
                            bucket_name.clone(),
                            VersioningConfiguration {
                                status: Some(BucketVersioningStatus::from(version_enable.clone())),
                                ..Default::default()
                            },
                        )))
                        .await
                        .unwrap();
                }
                let system_time: SystemTime = Utc::now().into();
                let timestamp: s3s::dto::Timestamp = system_time.into();

                apis::complete_create_bucket(
                    &dir_conf,
                    models::CreateBucketIsCompleted {
                        id: locator.id,
                        creation_date: timestamp_to_string(timestamp), // TODO: fix this, get metadata from actual storage
                    },
                )
                .await
                .unwrap();
            });
        }

        while let Some(result) = tasks.join_next().await {
            if let Err(err) = result {
                return Err(s3s::S3Error::with_message(
                    s3s::S3ErrorCode::InternalError,
                    format!("Error creating bucket: {}", err),
                ));
            }
        }

        Ok(S3Response::new(CreateBucketOutput::default()))
    }

    #[tracing::instrument(level = "info")]
    async fn delete_bucket(
        &self,
        req: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        // Send start delete bucket request
        let delete_bucket_resp = apis::start_delete_bucket(
            &self.dir_conf,
            models::DeleteBucketRequest {
                bucket: req.input.bucket.clone(),
            },
        )
        .await
        .unwrap();

        // Delete bucket in actual storages
        let mut tasks = tokio::task::JoinSet::new();
        let locators = delete_bucket_resp.locators;

        for locator in locators {
            let client: Arc<Box<dyn ObjectStoreClient>> =
                self.store_clients.get(&locator.tag).unwrap().clone();

            let bucket_name = req.input.bucket.clone();
            let dir_conf = self.dir_conf.clone();

            tasks.spawn(async move {
                let delete_bucket_req = new_delete_bucket_request(bucket_name.clone());
                client
                    .delete_bucket(S3Request::new(delete_bucket_req))
                    .await
                    .unwrap();

                apis::complete_delete_bucket(
                    &dir_conf,
                    models::DeleteBucketIsCompleted { id: locator.id },
                )
                .await
                .unwrap();
            });
        }

        while let Some(result) = tasks.join_next().await {
            if let Err(err) = result {
                return Err(s3s::S3Error::with_message(
                    s3s::S3ErrorCode::InternalError,
                    format!("Error deleting bucket: {}", err),
                ));
            }
        }

        Ok(S3Response::new(DeleteBucketOutput::default()))
    }

    #[tracing::instrument(level = "info")]
    async fn list_buckets(
        &self,
        _req: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        let resp = apis::list_buckets(&self.dir_conf).await;

        match resp {
            Ok(resp) => {
                let mut buckets: Vec<Bucket> = Vec::new();

                for bucket in resp {
                    buckets.push(Bucket {
                        creation_date: Some(string_to_timestamp(&bucket.creation_date)),
                        name: Some(bucket.bucket),
                    });
                }

                Ok(S3Response::new(ListBucketsOutput {
                    buckets: Some(buckets),
                    owner: None,
                }))
            }
            Err(err) => {
                error!("list_buckets failed: {:?}", err);
                Err(s3s::S3Error::with_message(
                    s3s::S3ErrorCode::InternalError,
                    "list_buckets failed",
                ))
            }
        }
    }

    #[tracing::instrument(level = "info")]
    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        let resp = apis::list_objects(
            &self.dir_conf,
            models::ListObjectRequest {
                bucket: req.input.bucket.clone(),
                prefix: req.input.prefix.clone(),
                start_after: req.input.start_after.clone(),
                max_keys: req.input.max_keys,
            },
        )
        .await;

        match resp {
            Ok(resp) => {
                let mut objects: Vec<Object> = Vec::new();

                // To deal with special key: encode key name in response if encoding type is url
                // NOTE: not related to actual encoding type of the object
                for obj in resp {
                    let key = match &req.input.encoding_type {
                        Some(encoding) if encoding.as_str() == EncodingType::URL => {
                            utf8_percent_encode(&obj.key, NON_ALPHANUMERIC).to_string()
                        }
                        _ => obj.key.clone(),
                    };

                    objects.push(Object {
                        key: Some(key),
                        size: obj.size as i64,
                        last_modified: Some(string_to_timestamp(
                            &obj.last_modified.clone().unwrap(),
                        )),
                        ..Default::default()
                    })
                }

                let output = ListObjectsV2Output {
                    contents: Some(objects),
                    ..Default::default()
                };
                Ok(S3Response::new(output))
            }
            Err(err) => {
                error!("list_objects_v2 failed: {:?}", err);
                Err(s3s::S3Error::with_message(
                    s3s::S3ErrorCode::InternalError,
                    "list_objects_v2 failed",
                ))
            }
        }
    }

    #[tracing::instrument(level = "info")]
    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        // let start_time = Instant::now();

        let vid = req.input.version_id.map(|id| id.parse::<i32>().unwrap());

        // since we will detect whether we have enabled versioning the the server side,
        // we don't need to check versioning status here

        // Directly call the control plane instead of going through the object store
        let head_resp = apis::head_object(
            &self.dir_conf,
            models::HeadObjectRequest {
                bucket: req.input.bucket.clone(),
                key: req.input.key.clone(),
                version_id: vid, // logical version
            },
        )
        .await;

        let output = match head_resp {
            Ok(resp) => HeadObjectOutput {
                content_length: resp.size as i64,
                e_tag: Some(resp.etag),
                last_modified: Some(string_to_timestamp(&resp.last_modified)),
                version_id: resp.version_id.map(|id| id.to_string()), // logical version
                ..Default::default()
            },
            Err(_) => {
                return Err(s3s::S3Error::with_message(
                    s3s::S3ErrorCode::NoSuchKey,
                    "No such key",
                ))
            }
        };
        let mut head_object_response = S3Response::new(output);

        // warmup logic
        if let Some(warmup_header) = req.headers.get("X-SKYSTORE-WARMUP") {
            let warmup_regions: Vec<String> = warmup_header
                .to_str()
                .unwrap()
                .split(',')
                .map(|s| s.to_string())
                .collect();

            let warmup_resp = apis::start_warmup(
                &self.dir_conf,
                models::StartWarmupRequest {
                    bucket: req.input.bucket.clone(),
                    key: req.input.key.clone(),
                    client_from_region: self.client_from_region.clone(),
                    version_id: vid, // logical version
                    warmup_regions,
                },
            )
            .await
            .unwrap();

            let src_locator = warmup_resp.src_locator;
            let mut tasks = tokio::task::JoinSet::new();

            for locator in warmup_resp.dst_locators {
                let conf = self.dir_conf.clone();
                let client: Arc<Box<dyn ObjectStoreClient>> =
                    self.store_clients.get(&locator.tag).unwrap().clone();

                let src_bucket = src_locator.bucket.clone();
                let src_key = src_locator.key.clone();
                let version_id = src_locator.version_id.clone(); // physical version

                tasks.spawn(async move {
                    let copy_resp = client
                        .copy_object(S3Request::new(new_copy_object_request(
                            src_bucket,
                            src_key,
                            locator.bucket.clone(),
                            locator.key.clone(),
                            version_id, // physical version
                        )))
                        .await
                        .unwrap();

                    let etag = copy_resp.output.copy_object_result.unwrap().e_tag.unwrap();

                    let head_output = client
                        .head_object(S3Request::new(new_head_object_request(
                            locator.bucket.clone(),
                            locator.key.clone(),
                            copy_resp.output.version_id.clone(), // physical version
                        )))
                        .await
                        .unwrap();

                    apis::complete_upload(
                        &conf,
                        models::PatchUploadIsCompleted {
                            id: locator.id,
                            size: head_output.output.content_length as u64,
                            etag: etag.clone(),
                            last_modified: timestamp_to_string(
                                head_output.output.last_modified.unwrap(),
                            ),
                            version_id: head_output.output.version_id, // physical version
                        },
                    )
                    .await
                    .unwrap();

                    etag
                });
            }

            let mut e_tags = Vec::new();
            while let Some(Ok(e_tag)) = tasks.join_next().await {
                e_tags.push(e_tag);
            }
            let e_tags_str = e_tags.join(",");

            // Add custom headers
            head_object_response
                .headers
                .insert("X-SKYSTORE-WARMUP-ETAGS", e_tags_str.parse().unwrap());
        }

        // let end_time = Instant::now();
        // write_time_duration_to_file(
        //     "HEAD OBJECT".to_string(),
        //     (end_time - start_time).as_millis(),
        //     "measure.txt",
        // );

        Ok(head_object_response)
    }

    // #[tracing::instrument(level = "info")]
    // async fn get_object(
    //     &self,
    //     req: S3Request<GetObjectInput>,
    // ) -> S3Result<S3Response<GetObjectOutput>> {
    //     let locator = self
    //         .locate_object(req.input.bucket.clone(), req.input.key.clone())
    //         .await?;

    //     match locator {
    //         Some(location) => {
    //             self.store_clients
    //                 .get(&location.tag)
    //                 .unwrap()
    //                 .get_object(req.map_input(|mut input: GetObjectInput| {
    //                     input.bucket = location.bucket;
    //                     input.key = location.key;
    //                     input
    //                 }))
    //                 .await
    //         }
    //         None => Err(s3s::S3Error::with_message(
    //             s3s::S3ErrorCode::NoSuchKey,
    //             "Object not found",
    //         )),
    //     }
    // }

    #[tracing::instrument(level = "info")]
    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        // let start_time = Instant::now();

        let bucket = req.input.bucket.clone();
        let key = req.input.key.clone();
        let vid = req
            .input
            .version_id
            .clone()
            .map(|id| id.parse::<i32>().unwrap());

        let locator = self.locate_object(bucket.clone(), key.clone(), vid).await?;

        match locator {
            Some(location) => {
                if req.headers.get("X-SKYSTORE-PULL").is_some() {
                    if location.tag != self.client_from_region {
                        let get_resp = self
                            .store_clients
                            .get(&location.tag)
                            .unwrap()
                            .get_object(req.map_input(|mut input: GetObjectInput| {
                                input.bucket = location.bucket;
                                input.key = location.key;
                                input.version_id = location.version_id; // physical version
                                input
                            }))
                            .await?;
                        let data = get_resp.output.body.unwrap();

                        let dir_conf_clone = self.dir_conf.clone();
                        let client_from_region_clone = self.client_from_region.clone();
                        let store_clients_clone = self.store_clients.clone();

                        let (mut input_blobs, _) = split_streaming_blob(data, 2); // locators.len() + 1
                        let response_blob = input_blobs.pop();

                        // Spawn a background task to store the object in the local object store
                        tokio::spawn(async move {
                            let start_upload_resp_result = apis::start_upload(
                                &dir_conf_clone,
                                models::StartUploadRequest {
                                    bucket: bucket.clone(),
                                    key: key.clone(),
                                    client_from_region: client_from_region_clone,
                                    version_id: vid, // logical version
                                    is_multipart: false,
                                    copy_src_bucket: None,
                                    copy_src_key: None,
                                },
                            )
                            .await;

                            // When version setting is NULL:
                            // In case of multi-concurrent GET request with copy_on_read policy,
                            // only upload if start_upload returns successful, this indicates that the object is not in the local object store
                            // status neither pending nor ready
                            if let Ok(start_upload_resp) = start_upload_resp_result {
                                let locators = start_upload_resp.locators;
                                let request_template = clone_put_object_request(
                                    &new_put_object_request(bucket, key),
                                    None,
                                );

                                for (locator, input_blob) in
                                    locators.into_iter().zip(input_blobs.into_iter())
                                {
                                    let client: Arc<Box<dyn ObjectStoreClient>> =
                                        store_clients_clone.get(&locator.tag).unwrap().clone();
                                    let req = S3Request::new(clone_put_object_request(
                                        &request_template,
                                        Some(input_blob),
                                    ))
                                    .map_input(|mut input| {
                                        input.bucket = locator.bucket.clone();
                                        input.key = locator.key.clone();
                                        input
                                    });

                                    let put_resp = client.put_object(req).await.unwrap();
                                    let e_tag = put_resp.output.e_tag.unwrap();
                                    let head_resp = client
                                        .head_object(S3Request::new(new_head_object_request(
                                            locator.bucket.clone(),
                                            locator.key.clone(),
                                            put_resp.output.version_id.clone(), // physical version
                                        )))
                                        .await
                                        .unwrap();
                                    apis::complete_upload(
                                        &dir_conf_clone,
                                        models::PatchUploadIsCompleted {
                                            id: locator.id,
                                            size: head_resp.output.content_length as u64,
                                            etag: e_tag,
                                            last_modified: timestamp_to_string(
                                                head_resp.output.last_modified.unwrap(),
                                            ),
                                            version_id: head_resp.output.version_id, // physical version
                                        },
                                    )
                                    .await
                                    .unwrap();
                                }
                            }
                        });

                        let response = S3Response::new(GetObjectOutput {
                            body: Some(response_blob.unwrap()),
                            bucket_key_enabled: get_resp.output.bucket_key_enabled,
                            content_length: get_resp.output.content_length,
                            delete_marker: get_resp.output.delete_marker,
                            missing_meta: get_resp.output.missing_meta,
                            parts_count: get_resp.output.parts_count,
                            tag_count: get_resp.output.tag_count,
                            version_id: vid.map(|id| id.to_string()), // logical version
                            ..Default::default()
                        });

                        // let end_time = Instant::now();

                        // write_time_duration_to_file("GET OBJECT".to_string(), (end_time - start_time).as_millis(), "measure.txt");

                        return Ok(response);
                    } else {
                        return self
                            .store_clients
                            .get(&self.client_from_region)
                            .unwrap()
                            .get_object(req.map_input(|mut input: GetObjectInput| {
                                input.bucket = location.bucket;
                                input.key = location.key;
                                input.version_id = location.version_id; // physical version
                                input
                            }))
                            .await;
                        // let end_time = Instant::now();
                        // write_time_duration_to_file(
                        //     "GET OBJECT".to_string(),
                        //     (end_time - start_time).as_millis(),
                        //     "measure.txt",
                        // );
                        // return res;
                    }
                } else {
                    let mut res = self
                        .store_clients
                        .get(&location.tag)
                        .unwrap()
                        .get_object(req.map_input(|mut input: GetObjectInput| {
                            input.bucket = location.bucket;
                            input.key = location.key;
                            input.version_id = location.version_id; // physical version
                            input
                        }))
                        .await?;
                    let mut blob = res.output.body.unwrap();
                    // blob impl Stream
                    // read all bytes from blob
                    let mut bytes = BytesMut::new();
                    // let mut stream_reader = StreamReader::new(blob);
                    while let Some(item) = blob.next().await {
                        let item = item.unwrap();
                        bytes.extend_from_slice(&item);
                    }
                    let new_body = Some(StreamingBlob::from(Body::from(bytes.freeze())));
                    res.output.body = new_body;
                    // let res = S3Response::new(GetObjectOutput {
                    //     body: Some(StreamingBlob::from(Body::from(bytes.freeze()))),
                    //     ..Default::default()
                    // });

                    // let end_time = Instant::now();
                    // write_time_duration_to_file("GET OBJECT".to_string(), (end_time - start_time).as_millis(), "measure.txt");
                    return Ok(res);
                }
            }
            None => {
                // let end_time = Instant::now();
                // write_time_duration_to_file(
                //     "GET OBJECT NOT FOUND".to_string(),
                //     (end_time - start_time).as_millis(),
                //     "measure.txt",
                // );
                Err(s3s::S3Error::with_message(
                    s3s::S3ErrorCode::NoSuchKey,
                    "Object not found",
                ))
            }
        }
    }

    #[tracing::instrument(level = "info")]
    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        // Idempotent PUT

        // let start_time = Instant::now();

        if self.version_enable == *"NULL" {
            let locator = self
                .locate_object(req.input.bucket.clone(), req.input.key.clone(), None)
                .await?;
            if let Some(locator) = locator {
                // let end_time = Instant::now();
                // write_time_duration_to_file("PUT OBJECT".to_string(), (end_time - start_time).as_millis(), "measure.txt");

                return Ok(S3Response::new(PutObjectOutput {
                    e_tag: locator.etag,
                    ..Default::default()
                }));
            }
        }

        // let mid_time = Instant::now();

        // println!("PUT OBJECT: check version setting time: {} ms", (mid_time - start_time).as_millis());

        let start_upload_resp = apis::start_upload(
            &self.dir_conf,
            models::StartUploadRequest {
                bucket: req.input.bucket.clone(),
                key: req.input.key.clone(),
                client_from_region: self.client_from_region.clone(),
                version_id: None,
                is_multipart: false,
                copy_src_bucket: None,
                copy_src_key: None,
            },
        )
        .await
        .unwrap();

        // let end_time = Instant::now();

        // println!("PUT OBJECT: start upload time (in control plane) : {} ms", (end_time - mid_time).as_millis());

        let mut tasks = tokio::task::JoinSet::new();
        let locators = start_upload_resp.locators;
        let request_template = clone_put_object_request(&req.input, None);
        let (input_blobs, sizes) = split_streaming_blob(req.input.body.unwrap(), locators.len());

        let vid = locators[0].version.map(|id| id.to_string()); // logical version

        locators
            .into_iter()
            .zip(input_blobs.into_iter())
            .zip(sizes.into_iter())
            .for_each(|((locator, input_blob), size)| {
                let conf = self.dir_conf.clone();
                let client: Arc<Box<dyn ObjectStoreClient>> =
                    self.store_clients.get(&locator.tag).unwrap().clone();
                let req = S3Request::new(clone_put_object_request(
                    &request_template,
                    Some(input_blob),
                ))
                .map_input(|mut input| {
                    input.bucket = locator.bucket.clone();
                    input.key = locator.key.clone();
                    input
                });

                let content_length = req.input.content_length;
                let size_to_get = if content_length.is_none() {
                    size
                } else {
                    content_length.unwrap()
                };

                tasks.spawn(async move {
                    let put_resp = client.put_object(req).await.unwrap();
                    let e_tag = put_resp.output.e_tag.unwrap();
                    let last_modified = current_timestamp_string();

                    // let start_time = Instant::now();

                    apis::complete_upload(
                        &conf,
                        models::PatchUploadIsCompleted {
                            id: locator.id,
                            size: size_to_get as u64,
                            etag: e_tag.clone(),
                            last_modified,
                            version_id: put_resp.output.version_id, // physical version
                        },
                    )
                    .await
                    .unwrap();

                    // let end_time = Instant::now();

                    // println!("PUT OBJECT: complete upload time (in control plane) : {} ms", (end_time - start_time).as_millis());

                    e_tag
                });
            });

        // let start_time = Instant::now();

        let mut e_tags = Vec::new();
        while let Some(Ok(e_tag)) = tasks.join_next().await {
            e_tags.push(e_tag);
        }

        // let end_time = Instant::now();

        // println!("PUT OBJECT: time for completing upload : {} ms", (end_time1 - end_time).as_millis());

        // write_time_duration_to_file("PUT OBJECT".to_string(), (end_time - start_time).as_millis(), "measure.txt");

        Ok(S3Response::new(PutObjectOutput {
            e_tag: e_tags.pop(),
            version_id: vid, // logical version
            ..Default::default()
        }))
    }

    #[tracing::instrument(level = "info")]
    async fn delete_objects(
        &self,
        req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        // let start_time = Instant::now();

        // Send start delete objects request
        let object_identifiers: Vec<_> = req
            .input
            .delete
            .objects
            .iter()
            // .map(|obj| obj.key.clone())
            .collect();

        // let version_enabled = apis::check_version_setting(
        //     &self.dir_conf,
        //     models::HeadBucketRequest {
        //         bucket: req.input.bucket.clone(),
        //     },
        // )
        // .await
        // .unwrap();

        if self.version_enable == *"NULL"
            && req
                .input
                .delete
                .objects
                .iter()
                .any(|obj| obj.version_id.is_some())
        {
            return Err(s3s::S3Error::with_message(
                s3s::S3ErrorCode::NoSuchKey,
                "Version is not enabled.",
            ));
        }

        // transform the object identifiers vcector to HashMap
        let mut id_map: HashMap<String, Vec<i32>> = HashMap::new();
        for obj in &req.input.delete.objects {
            if id_map.contains_key(&obj.key) {
                if let Some(version_id) = obj.version_id.clone() {
                    id_map
                        .get_mut(&obj.key)
                        .unwrap()
                        .push(version_id.parse::<i32>().unwrap());
                }
            } else {
                // allocate a new vector to store the version ids
                // if we don't set version ids explicitly, the vector will be empty
                // this indicates we want to delete all versions of the object
                id_map.insert(obj.key.clone(), vec![]);
            }
        }

        let delete_objects_resp = match apis::start_delete_objects(
            &self.dir_conf,
            models::DeleteObjectsRequest {
                bucket: req.input.bucket.clone(),
                object_identifiers: id_map,
                multipart_upload_ids: None,
            },
        )
        .await
        {
            Ok(resp) => resp,
            Err(_) => {
                // TODO: fail silent (assume object exists error), fix this
                // 1) Error handling
                //      404: return delete success
                //      Other errors: return delete failures
                // 2) Partial Failure: some object can be deleted, some not
                //      Deal with it on server & dataplane side

                let res = S3Response::new(DeleteObjectsOutput {
                    deleted: Some(
                        object_identifiers
                            .iter()
                            .map(|object| DeletedObject {
                                key: Some(object.key.clone()),
                                delete_marker: false,
                                // version_id: delete_objects_resp.
                                ..Default::default()
                            })
                            .collect(),
                    ),
                    errors: None,
                    ..Default::default()
                });

                // let end_time = Instant::now();
                // write_time_duration_to_file(
                //     "DELETE OBJECTS".to_string(),
                //     (end_time - start_time).as_millis(),
                //     "measure.txt",
                // );

                return Ok(res);
            }
        };

        let delete_markers_map = delete_objects_resp.delete_markers;
        let op_type_map = delete_objects_resp.op_type;

        // Delete objects in actual storages
        let mut tasks = tokio::task::JoinSet::new();
        for (key, locators) in delete_objects_resp.locators {
            for locator in locators {
                let client: Arc<Box<dyn ObjectStoreClient>> =
                    self.store_clients.get(&locator.tag).unwrap().clone();
                let bucket_name = locator.bucket.clone();
                let key_clone = key.clone(); // Clone the key here
                let version_id = locator.version_id.clone();
                let version = locator.version;
                let op_type_key = op_type_map[&key_clone].clone();

                tasks.spawn(async move {
                    let delete_object_response = client
                        .delete_object(S3Request::new(new_delete_object_request(
                            bucket_name.clone(),
                            locator.key,
                            version_id.clone(), // physical version
                        )))
                        .await;

                    match delete_object_response {
                        Ok(_resp) => Ok((
                            key_clone.clone(),
                            locator.id,
                            version, // logical version
                            op_type_key,
                        )),
                        Err(_) => Err((
                            key_clone.clone(),
                            locator.id,
                            version, // logical version
                            op_type_key,
                            format!("Failed to delete object with key: {}", key_clone),
                        )),
                    }
                });
            }
        }

        // Collect results and complete delete objects
        let mut key_to_results: HashMap<
            String,
            Vec<Result<(i32, Option<String>, String), (i32, Option<String>, String, String)>>,
        > = HashMap::new();
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(res) => match res {
                    Ok((key, id, version, op_type)) => {
                        key_to_results.entry(key).or_default().push(Ok((
                            id,
                            version.map(|id| id.to_string()),
                            op_type,
                        )));
                    }
                    Err((key, id, version, op_type, err)) => {
                        key_to_results.entry(key).or_default().push(Err((
                            id,
                            version.map(|id| id.to_string()),
                            op_type,
                            err,
                        )));
                    }
                },
                // TODO: how to handle this?
                Err(err) => {
                    return Err(s3s::S3Error::with_message(
                        s3s::S3ErrorCode::InternalError,
                        format!("Error deleting objects: {}", err),
                    ));
                }
            }
        }

        let mut deleted_objects = DeletedObjects::default();
        let mut errors = Errors::default();

        for (key, results) in key_to_results {
            let (successes, fails): (Vec<_>, Vec<_>) = results.into_iter().partition(Result::is_ok);

            // complete delete objects
            let completed_ids: Vec<_> = successes.into_iter().filter_map(Result::ok).collect();
            // we might need to deal with fails ids
            let fails_empty = fails.is_empty();
            let fails_ids: Vec<_> = fails.into_iter().filter_map(Result::err).collect();
            let err_msg = fails_ids
                .iter()
                .map(|fail| fail.3.clone())
                .collect::<Vec<String>>()
                .join(",");
            let mut ids = Vec::new();
            let mut versions = Vec::new();
            let mut op_types = Vec::new();
            for (id, version, op_type) in &completed_ids {
                ids.push(*id);
                versions.push(version.clone());
                op_types.push(op_type.clone());
            }
            if !completed_ids.is_empty() {
                apis::complete_delete_objects(
                    &self.dir_conf,
                    models::DeleteObjectsIsCompleted {
                        ids,
                        multipart_upload_ids: None,
                        op_type: op_types,
                    },
                )
                .await
                .unwrap();
            }

            if self.version_enable == *"NULL" {
                if fails_empty {
                    // if version is not enabled, the version vector must be empty
                    deleted_objects.push(DeletedObject {
                        key: Some(key.clone()),
                        delete_marker: false,
                        ..Default::default()
                    });
                } else {
                    errors.push(Error {
                        key: Some(key),
                        code: Some("InternalError".to_string()),
                        message: Some(err_msg),
                        ..Default::default()
                    });
                }
            } else {
                // if we failed to delete the logical object, and this object is not a delete marker,
                // we need to add it to the errors vector
                if !fails_empty && !delete_markers_map[&key].delete_marker {
                    errors.push(Error {
                        key: Some(key),
                        code: Some("InternalError".to_string()),
                        message: Some(err_msg),
                        ..Default::default()
                    });
                } else {
                    // now the version vector can be empty or not
                    // first dealing with the success vector
                    if versions.is_empty() {
                        deleted_objects.push(DeletedObject {
                            key: Some(key.clone()),
                            delete_marker: true,
                            delete_marker_version_id: delete_markers_map[&key]
                                .version_id
                                .as_ref()
                                .map(|id| id.to_string()), // logical version
                            version_id: None,
                        });
                    } else {
                        for version in &versions {
                            deleted_objects.push(DeletedObject {
                                key: Some(key.clone()),
                                delete_marker: true,
                                delete_marker_version_id: delete_markers_map[&key]
                                    .version_id
                                    .as_ref()
                                    .map(|id| id.to_string()), // logical version
                                version_id: version.clone(),
                            });
                        }
                    }

                    let mut fails_ids_vec = Vec::new();
                    // let mut fails_version_vec = Vec::new();
                    let mut fails_op_type_vec = Vec::new();

                    // deal with fails vector, fails_ids vector might be empty
                    // println!("fails_ids: {:?}", fails_ids);
                    for (id, _, op_type, _) in &fails_ids {
                        fails_ids_vec.push(*id);
                        // fails_version_vec.push(version);
                        fails_op_type_vec.push(op_type.clone());
                    }
                    if !fails_ids_vec.is_empty() {
                        apis::complete_delete_objects(
                            &self.dir_conf,
                            models::DeleteObjectsIsCompleted {
                                ids: fails_ids_vec,
                                multipart_upload_ids: None,
                                op_type: fails_op_type_vec,
                            },
                        )
                        .await
                        .unwrap();
                    }
                }
            }
        }

        // let end_time = Instant::now();
        // write_time_duration_to_file(
        //     "DELETE OBJECTS".to_string(),
        //     (end_time - start_time).as_millis(),
        //     "measure.txt",
        // );

        Ok(S3Response::new(DeleteObjectsOutput {
            deleted: Some(deleted_objects),
            errors: if errors.is_empty() {
                None
            } else {
                Some(errors)
            },
            ..Default::default()
        }))
    }

    #[tracing::instrument(level = "info")]
    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        let delete = Delete {
            objects: vec![ObjectIdentifier {
                key: req.input.key.clone(),
                version_id: req.input.version_id.clone(), // logical version
            }],
            ..Default::default()
        };

        let delete_object_input = new_delete_objects_request(req.input.bucket.clone(), delete);
        let delete_object_req = S3Request::new(delete_object_input);

        match self.delete_objects(delete_object_req).await {
            Ok(delete_objects_resp) => {
                let delete_markers = delete_objects_resp
                    .output
                    .deleted
                    .as_ref()
                    .unwrap()
                    .iter()
                    .find(|obj| obj.key.as_ref() == Some(&req.input.key))
                    .unwrap();
                if delete_objects_resp
                    .output
                    .deleted
                    .as_ref()
                    .and_then(|objs| {
                        objs.iter()
                            .find(|obj| obj.key.as_ref() == Some(&req.input.key))
                    })
                    .is_some()
                {
                    Ok(S3Response::new(DeleteObjectOutput {
                        delete_marker: delete_markers.delete_marker,
                        version_id: req.input.version_id, // logical version
                        ..Default::default()
                    }))
                } else {
                    Err(s3s::S3Error::with_message(
                        s3s::S3ErrorCode::InternalError,
                        "Object not deleted successfully".to_string(),
                    ))
                }
            }
            Err(e) => Err(s3s::S3Error::with_message(
                s3s::S3ErrorCode::InternalError,
                format!("Error deleting object: {}", e),
            )),
        }
    }

    // when the version is suspended, we are also allowed to copy objects
    // but the version id returned will be null
    #[tracing::instrument(level = "info")]
    async fn copy_object(
        &self,
        req: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        let start_time = Instant::now();

        //TODO for future: people commonly use copy_object to modify the metadata, instead of creatig a new today.

        let (src_bucket, src_key, src_version_id) = match &req.input.copy_source {
            CopySource::Bucket {
                bucket,
                key,
                version_id,
            } => (bucket, key, version_id),
            CopySource::AccessPoint { .. } => panic!("AccessPoint not supported"),
        };
        let [dst_bucket, dst_key] = [&req.input.bucket, &req.input.key];
        // the version id could be none
        let copy_version_id = src_version_id.as_ref().map(|id| id.parse::<i32>().unwrap());

        let src_locator = self
            .locate_object(src_bucket.to_string(), src_key.to_string(), copy_version_id)
            .await?;
        let dst_locator = self
            .locate_object(dst_bucket.to_string(), dst_key.to_string(), None)
            .await?;

        if src_locator.is_none() {
            return Err(s3s::S3Error::with_message(
                s3s::S3ErrorCode::NoSuchKey,
                "Source object not found",
            ));
        };

        let version_enabled = apis::check_version_setting(
            &self.dir_conf,
            models::HeadBucketRequest {
                bucket: src_bucket.to_string(),
            },
        )
        .await
        .unwrap();

        if copy_version_id.is_some() && !version_enabled {
            return Err(s3s::S3Error::with_message(
                s3s::S3ErrorCode::InternalError,
                "Version is not enabled.",
            ));
        }

        // if version is enabled, we should be allowed to copy multiple times
        if let Some(dst_locator) = dst_locator {
            if !version_enabled {
                return Ok(S3Response::new(CopyObjectOutput {
                    copy_object_result: Some(CopyObjectResult {
                        e_tag: dst_locator.etag,
                        last_modified: Some(string_to_timestamp(
                            dst_locator.last_modified.as_ref().unwrap(),
                        )),
                        ..Default::default()
                    }),
                    copy_source_version_id: src_locator
                        .clone()
                        .unwrap()
                        .version
                        .map(|id| id.to_string()), // logical version
                    version_id: dst_locator.version.map(|id| id.to_string()), // logical version
                    ..Default::default()
                }));
            }
        };

        let version = src_locator.clone().unwrap().version;

        let upload_resp = apis::start_upload(
            &self.dir_conf,
            models::StartUploadRequest {
                bucket: dst_bucket.to_string(),
                key: dst_key.to_string(),
                client_from_region: self.client_from_region.clone(),
                version_id: version, // logical version
                is_multipart: false,
                copy_src_bucket: Some(src_bucket.to_string()),
                copy_src_key: Some(src_key.to_string()),
            },
        )
        .await
        .unwrap();
        let vid = upload_resp.locators[0].version.map(|id| id.to_string()); // logical version
        let mut tasks = tokio::task::JoinSet::new();
        upload_resp
            .locators
            .into_iter()
            .zip(
                upload_resp
                    .copy_src_buckets
                    .into_iter()
                    .zip(upload_resp.copy_src_keys.into_iter()),
            )
            .for_each(|(locator, (src_bucket, src_key))| {
                let conf = self.dir_conf.clone();
                let client = self.store_clients.get(&locator.tag).unwrap().clone();
                let version_clone = locator.version_id.clone(); // physical version

                tasks.spawn(async move {
                    let copy_resp = client
                        .copy_object(S3Request::new(new_copy_object_request(
                            src_bucket.clone(),
                            src_key.clone(),
                            locator.bucket.clone(),
                            locator.key.clone(),
                            version_clone, // physical version
                        )))
                        .await
                        .unwrap();

                    let etag = copy_resp.output.copy_object_result.unwrap().e_tag.unwrap();

                    let head_output = client
                        .head_object(S3Request::new(new_head_object_request(
                            locator.bucket.clone(),
                            locator.key.clone(),
                            copy_resp.output.version_id.clone(), // physical version
                        )))
                        .await
                        .unwrap();

                    apis::complete_upload(
                        &conf,
                        models::PatchUploadIsCompleted {
                            id: locator.id,
                            size: head_output.output.content_length as u64,
                            etag: etag.clone(),
                            last_modified: timestamp_to_string(
                                head_output.output.last_modified.unwrap(),
                            ),
                            version_id: head_output.output.version_id, // physical version
                        },
                    )
                    .await
                    .unwrap();

                    etag
                });
            });

        let mut e_tags = Vec::new();
        while let Some(Ok(e_tag)) = tasks.join_next().await {
            e_tags.push(e_tag);
        }

        let res = S3Response::new(CopyObjectOutput {
            copy_object_result: Some(CopyObjectResult {
                e_tag: e_tags.pop(),
                ..Default::default()
            }),
            copy_source_version_id: version.map(|id| id.to_string()), // logical version
            version_id: vid,                                          // logical version
            ..Default::default()
        });

        let end_time = Instant::now();
        write_time_duration_to_file(
            "COPY OBJECT".to_string(),
            (end_time - start_time).as_millis(),
            "measure.txt",
        );

        Ok(res)
    }

    #[tracing::instrument(level = "info")]
    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {

        // let start_time: Instant = Instant::now();

        let locator = self
            .locate_object(req.input.bucket.clone(), req.input.key.clone(), None)
            .await?;

        // let version_enabled = apis::check_version_setting(
        //     &self.dir_conf,
        //     models::HeadBucketRequest {
        //         bucket: req.input.bucket.clone(),
        //     },
        // )
        // .await
        // .unwrap();

        if locator.is_some() && self.version_enable == *"NULL" {
            return Err(s3s::S3Error::with_message(
                s3s::S3ErrorCode::InternalError,
                "Object already exists.",
            ));
        };

        let upload_resp = apis::start_upload(
            &self.dir_conf,
            models::StartUploadRequest {
                bucket: req.input.bucket.clone(),
                key: req.input.key.clone(),
                client_from_region: self.client_from_region.clone(),
                version_id: None,
                is_multipart: true,
                copy_src_bucket: None,
                copy_src_key: None,
            },
        )
        .await
        .unwrap();

        assert!(upload_resp.multipart_upload_id.is_some());

        // it seems we don't need to do this in parallel.
        // this is just a metadata change afterall
        for locator in upload_resp.locators.iter() {
            let client = self.store_clients.get(&locator.tag).unwrap().clone();

            let resp = client
                .create_multipart_upload(S3Request::new(new_create_multipart_upload_request(
                    locator.bucket.clone(),
                    locator.key.clone(),
                )))
                .await
                .unwrap();

            let physical_multipart_id = resp.output.upload_id.unwrap();

            apis::set_multipart_id(
                &self.dir_conf,
                models::PatchUploadMultipartUploadId {
                    id: locator.id,
                    multipart_upload_id: physical_multipart_id,
                },
            )
            .await
            .unwrap();
        }

        let res = S3Response::new(CreateMultipartUploadOutput {
            bucket: Some(req.input.bucket.clone()),
            key: Some(req.input.key.clone()),
            upload_id: upload_resp.multipart_upload_id,
            ..Default::default()
        });

        // let end_time = Instant::now();
        // write_time_duration_to_file("CREATE MULTIPART UPLOAD".to_string(), (end_time - start_time).as_millis(), "measure.txt");

        Ok(res)
    }

    #[tracing::instrument(level = "info")]
    async fn abort_multipart_upload(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        let start_time = Instant::now();

        let mut id_map = HashMap::new();
        id_map.insert(req.input.key.clone(), vec![]);
        let delete_start_response = apis::start_delete_objects(
            &self.dir_conf,
            models::DeleteObjectsRequest {
                bucket: req.input.bucket.clone(),
                object_identifiers: id_map,
                multipart_upload_ids: Some(vec![req.input.upload_id.clone()]),
            },
        )
        .await
        .unwrap();

        for locators in delete_start_response.locators.values() {
            for locator in locators {
                let client = self.store_clients.get(&locator.tag).unwrap().clone();

                client
                    .abort_multipart_upload(S3Request::new(new_abort_multipart_upload_request(
                        locator.bucket.clone(),
                        locator.key.clone(),
                        locator.multipart_upload_id.as_ref().cloned().unwrap(),
                    )))
                    .await
                    .unwrap();

                apis::complete_delete_objects(
                    &self.dir_conf,
                    models::DeleteObjectsIsCompleted {
                        ids: vec![locator.id],
                        multipart_upload_ids: Some(vec![locator
                            .multipart_upload_id
                            .as_ref()
                            .cloned()
                            .unwrap()]),
                        op_type: vec![String::from("delete")], // TODO: might need to change this
                    },
                )
                .await
                .unwrap();
            }
        }

        let end_time = Instant::now();
        write_time_duration_to_file(
            "ABORT MULTIPART UPLOAD".to_string(),
            (end_time - start_time).as_millis(),
            "measure.txt",
        );

        Ok(S3Response::new(AbortMultipartUploadOutput::default()))
    }

    #[tracing::instrument(level = "info")]
    async fn list_multipart_uploads(
        &self,
        req: S3Request<ListMultipartUploadsInput>,
    ) -> S3Result<S3Response<ListMultipartUploadsOutput>> {
        let start_time = Instant::now();

        let resp = apis::list_multipart_uploads(
            &self.dir_conf,
            models::ListObjectRequest {
                bucket: req.input.bucket.clone(),
                prefix: req.input.prefix.clone(),
                start_after: None,
                max_keys: None,
            },
        )
        .await
        .unwrap();

        let res = S3Response::new(ListMultipartUploadsOutput {
            uploads: Some(
                resp.into_iter()
                    .map(|u| MultipartUpload {
                        key: Some(u.key),
                        upload_id: Some(u.upload_id),
                        ..Default::default()
                    })
                    .collect(),
            ),
            bucket: Some(req.input.bucket.clone()),
            prefix: req.input.prefix.clone(),
            ..Default::default()
        });

        let end_time = Instant::now();
        write_time_duration_to_file(
            "LIST MULTIPART UPLOADS".to_string(),
            (end_time - start_time).as_millis(),
            "measure.txt",
        );

        Ok(res)
    }

    #[tracing::instrument(level = "info")]
    async fn list_parts(
        &self,
        req: S3Request<ListPartsInput>,
    ) -> S3Result<S3Response<ListPartsOutput>> {
        let start_time = Instant::now();

        match apis::list_parts(
            &self.dir_conf,
            models::ListPartsRequest {
                bucket: req.input.bucket.clone(),
                key: req.input.key.clone(),
                upload_id: req.input.upload_id.clone(),
                part_number: None,
            },
        )
        .await
        {
            Ok(resp) => {
                let res = S3Response::new(ListPartsOutput {
                    parts: Some(
                        resp.into_iter()
                            .map(|p| Part {
                                part_number: p.part_number,
                                size: p.size as i64,
                                e_tag: Some(p.etag),
                                ..Default::default()
                            })
                            .collect(),
                    ),
                    bucket: Some(req.input.bucket.clone()),
                    key: Some(req.input.key.clone()),
                    upload_id: Some(req.input.upload_id.clone()),
                    ..Default::default()
                });
                let end_time = Instant::now();
                write_time_duration_to_file(
                    "LIST PARTS".to_string(),
                    (end_time - start_time).as_millis(),
                    "measure.txt",
                );
                Ok(res)
            }
            Err(err) => {
                let end_time = Instant::now();
                write_time_duration_to_file(
                    "LIST PARTS".to_string(),
                    (end_time - start_time).as_millis(),
                    "measure.txt",
                );

                error!("list_parts failed: {:?}", err);
                Err(s3s::S3Error::with_message(
                    s3s::S3ErrorCode::InternalError,
                    "list_parts failed",
                ))
            }
        }
    }

    #[tracing::instrument(level = "info")]
    async fn upload_part(
        &self,
        req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {

        // let start_time = Instant::now();

        let resp = apis::continue_upload(
            &self.dir_conf,
            models::ContinueUploadRequest {
                bucket: req.input.bucket.clone(),
                key: req.input.key.clone(),
                multipart_upload_id: req.input.upload_id.clone(),
                client_from_region: self.client_from_region.clone(),
                version_id: None,
                do_list_parts: Some(false),
                copy_src_bucket: None,
                copy_src_key: None,
            },
        )
        .await
        .unwrap();

        let mut tasks = tokio::task::JoinSet::new();
        let request_template = clone_upload_part_request(&req.input, None);
        let content_length = req
            .input
            .body
            .as_ref()
            .unwrap()
            .remaining_length()
            .exact()
            .unwrap();
        let (body_clones, _) = split_streaming_blob(req.input.body.unwrap(), resp.len());
        resp.into_iter()
            .zip(body_clones.into_iter())
            .for_each(|(locator, body)| {
                let conf = self.dir_conf.clone();
                let client = self.store_clients.get(&locator.tag).unwrap().clone();
                let req = S3Request::new(clone_upload_part_request(&request_template, Some(body)))
                    .map_input(|mut i| {
                        i.bucket = locator.bucket.clone();
                        i.key = locator.key.clone();
                        i.upload_id = locator.multipart_upload_id.clone();
                        i
                    });
                let part_number = req.input.part_number;

                tasks.spawn(async move {
                    let resp: UploadPartOutput = client.upload_part(req).await.unwrap().output;

                    let etag = resp.e_tag.unwrap();

                    apis::append_part(
                        &conf,
                        models::PatchUploadMultipartUploadPart {
                            id: locator.id,
                            part_number,
                            etag: etag,
                            size: content_length as u64,
                        },
                    )
                    .await
                    .unwrap();
                });
            });

        while (tasks.join_next().await).is_some() {}

        let parts = apis::list_parts(
            &self.dir_conf,
            models::ListPartsRequest {
                bucket: req.input.bucket.clone(),
                key: req.input.key.clone(),
                upload_id: req.input.upload_id.clone(),
                part_number: Some(req.input.part_number),
            },
        )
        .await
        .unwrap();
        assert!(parts.len() == 1);

        // let end_time = Instant::now();
        // write_time_duration_to_file("UPLOAD PART".to_string(), (end_time - start_time).as_millis(), "measure.txt");

        Ok(S3Response::new(UploadPartOutput {
            e_tag: Some(parts[0].etag.clone()),
            ..Default::default()
        }))
    }

    #[tracing::instrument(level = "info")]
    async fn upload_part_copy(
        &self,
        req: S3Request<UploadPartCopyInput>,
    ) -> S3Result<S3Response<UploadPartCopyOutput>> {
        let start_time = Instant::now();

        let (src_bucket, src_key, version_id) = match &req.input.copy_source {
            CopySource::Bucket {
                bucket,
                key,
                version_id,
            } => (bucket.to_string(), key.to_string(), version_id),
            CopySource::AccessPoint { .. } => panic!("AccessPoint not supported"),
        };

        let version_enabled = apis::check_version_setting(
            &self.dir_conf,
            models::HeadBucketRequest {
                bucket: src_bucket.clone(),
            },
        )
        .await
        .unwrap();

        if version_id.is_some() && !version_enabled {
            return Err(s3s::S3Error::with_message(
                s3s::S3ErrorCode::InternalError,
                "Version is not enabled.",
            ));
        }

        let vid = version_id.as_ref().map(|id| id.parse::<i32>().unwrap()); // logical version

        let src_locator = self
            .locate_object(src_bucket.clone(), src_key.clone(), vid)
            .await
            .unwrap();
        if src_locator.is_none() {
            return Err(s3s::S3Error::with_message(
                s3s::S3ErrorCode::NoSuchKey,
                "Source object not found",
            ));
        };
        let mut content_length = src_locator.as_ref().unwrap().size.unwrap();
        let mut request_range = None;
        if let Some(range) = &req.input.copy_source_range {
            let (start, end) = parse_range(range);
            if let Some(end) = end {
                content_length = end - start + 1;
            } else {
                content_length -= start;
            }
            request_range = Some(range.clone());
        }

        let locators = apis::continue_upload(
            &self.dir_conf,
            models::ContinueUploadRequest {
                bucket: req.input.bucket.clone(),
                key: req.input.key.clone(),
                multipart_upload_id: req.input.upload_id.clone(),
                client_from_region: self.client_from_region.clone(),
                version_id: vid, // logical version
                do_list_parts: Some(false),
                copy_src_bucket: Some(src_bucket.clone()),
                copy_src_key: Some(src_key.clone()),
            },
        )
        .await
        .unwrap();

        let mut tasks = tokio::task::JoinSet::new();

        locators.into_iter().for_each(|locator| {
            let conf = self.dir_conf.clone();
            let request_range = request_range.clone();
            let client = self.store_clients.get(&locator.tag).unwrap().clone();
            let part_number = req.input.part_number;
            tasks.spawn(async move {
                let req = new_upload_part_copy_request(
                    locator.bucket.clone(),
                    locator.key.clone(),
                    locator.multipart_upload_id.clone(),
                    part_number,
                    CopySourceInfo {
                        bucket: locator.copy_src_bucket.clone().unwrap(),
                        key: locator.copy_src_key.clone().unwrap(),
                        version_id: locator.version_id.clone(), // logical version
                    },
                    request_range,
                );
                let copy_resp = client.upload_part_copy(S3Request::new(req)).await.unwrap();

                apis::append_part(
                    &conf,
                    models::PatchUploadMultipartUploadPart {
                        id: locator.id,
                        part_number,
                        etag: copy_resp.output.copy_part_result.unwrap().e_tag.unwrap(),
                        size: content_length, // we actually can't head object this :(, this is a part.
                    },
                )
                .await
                .unwrap();
            });
        });

        while (tasks.join_next().await).is_some() {}

        let parts = apis::list_parts(
            &self.dir_conf,
            models::ListPartsRequest {
                bucket: req.input.bucket.clone(),
                key: req.input.key.clone(),
                upload_id: req.input.upload_id.clone(),
                part_number: Some(req.input.part_number),
            },
        )
        .await
        .unwrap();
        assert!(parts.len() == 1, "parts: {parts:?}");

        let res = S3Response::new(UploadPartCopyOutput {
            copy_part_result: Some(CopyPartResult {
                e_tag: Some(parts[0].etag.clone()),
                ..Default::default()
            }),
            copy_source_version_id: vid.map(|id| id.to_string()), // logical version
            ..Default::default()
        });

        let end_time = Instant::now();
        write_time_duration_to_file(
            "UPLOAD PART COPY".to_string(),
            (end_time - start_time).as_millis(),
            "measure.txt",
        );

        Ok(res)
    }

    #[tracing::instrument(level = "info")]
    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {

        // let start_time = Instant::now();

        let locators = match apis::continue_upload(
            &self.dir_conf,
            models::ContinueUploadRequest {
                bucket: req.input.bucket.clone(),
                key: req.input.key.clone(),
                multipart_upload_id: req.input.upload_id.clone(),
                client_from_region: self.client_from_region.clone(),
                version_id: None,
                do_list_parts: Some(true),
                copy_src_bucket: None,
                copy_src_key: None,
            },
        )
        .await
        {
            Ok(resp) => resp,
            Err(err) => {
                error!("continue upload failed: {:?}", err);
                return Err(s3s::S3Error::with_message(
                    s3s::S3ErrorCode::InternalError,
                    "continue upload failed",
                ));
            }
        };

        let logical_parts_set = req
            .input
            .multipart_upload
            .unwrap()
            .parts
            .as_ref()
            .unwrap()
            .iter()
            .map(|p| (p.part_number))
            .collect::<HashSet<_>>();

        let vid = locators[0].version; // logical version

        for locator in locators {
            let client = self.store_clients.get(&locator.tag).unwrap().clone();

            let mut physical_parts_list: Vec<_> = locator
                .parts
                .unwrap()
                .into_iter()
                .map(|p| {
                    let mut part = CompletedPart::default();
                    assert!(logical_parts_set.contains(&p.part_number));
                    part.part_number = p.part_number;
                    part.e_tag = Some(p.etag);
                    part
                })
                .collect();

            physical_parts_list.sort_by_key(|p| p.part_number);

            assert!(physical_parts_list.len() == logical_parts_set.len());

            let resp = client
                .complete_multipart_upload(S3Request::new(new_complete_multipart_request(
                    locator.bucket.clone(),
                    locator.key.clone(),
                    locator.multipart_upload_id.clone(),
                    CompletedMultipartUpload {
                        parts: Some(physical_parts_list),
                    },
                )))
                .await
                .unwrap();

            let head_resp = client
                .head_object(S3Request::new(new_head_object_request(
                    locator.bucket.clone(),
                    locator.key.clone(),
                    resp.output.version_id.clone(), // physical version
                )))
                .await
                .unwrap();

            apis::complete_upload(
                &self.dir_conf,
                models::PatchUploadIsCompleted {
                    id: locator.id,
                    etag: resp.output.e_tag.unwrap(),
                    size: head_resp.output.content_length as u64,
                    last_modified: timestamp_to_string(head_resp.output.last_modified.unwrap()),
                    version_id: resp.output.version_id, // physical version
                },
            )
            .await
            .unwrap();
        }

        let head_resp = apis::head_object(
            &self.dir_conf,
            models::HeadObjectRequest {
                bucket: req.input.bucket.clone(),
                key: req.input.key.clone(),
                version_id: vid, // logical version
            },
        )
        .await
        .unwrap();

        let res = S3Response::new(CompleteMultipartUploadOutput {
            bucket: Some(req.input.bucket.clone()),
            key: Some(req.input.key.clone()),
            e_tag: Some(head_resp.etag),
            version_id: head_resp.version_id.map(|id| id.to_string()), // logical version
            ..Default::default()
        });

        // let end_time = Instant::now();
        // write_time_duration_to_file("COMPLETE MULTIPART UPLOAD".to_string(), (end_time - start_time).as_millis(), "measure.txt");

        Ok(res)
    }

    #[tracing::instrument(level = "info")]
    async fn put_bucket_versioning(
        &self,
        req: S3Request<PutBucketVersioningInput>,
    ) -> S3Result<S3Response<PutBucketVersioningOutput>> {
        let versioning = match req
            .input
            .versioning_configuration
            .status
            .clone()
            .unwrap()
            .as_str()
        {
            "Enabled" => true,
            "Suspended" => false,
            _ => {
                return Err(s3s::S3Error::with_message(
                    s3s::S3ErrorCode::InternalError,
                    "Failed to change bucket versioning setting".to_string(),
                ))
            }
        };
        let resp = apis::put_bucket_versioning(
            &self.dir_conf,
            models::PutBucketVersioningRequest {
                bucket: req.input.bucket.clone(),
                versioning,
            },
        )
        .await;

        if let Ok(locators) = resp {
            for locator in locators {
                let client = self.store_clients.get(&locator.tag).unwrap().clone();
                let resp = client
                    .put_bucket_versioning(S3Request::new(new_put_bucket_versioning_request(
                        locator.bucket.clone(),
                        VersioningConfiguration {
                            status: Some(
                                req.input.versioning_configuration.status.clone().unwrap(),
                            ),
                            ..Default::default()
                        },
                    )))
                    .await;
                if resp.is_err() {
                    panic!("Failed to change bucket versioning setting");
                }
            }
            Ok(S3Response::new(PutBucketVersioningOutput::default()))
        } else {
            panic!("Failed to change bucket versioning setting");
        }
    }
}

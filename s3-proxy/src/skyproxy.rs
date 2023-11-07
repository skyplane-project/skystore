use crate::objstore_client::ObjectStoreClient;
use crate::utils::stream_utils::split_streaming_blob;
use crate::utils::type_utils::*;

use s3s::dto::*;
use s3s::stream::ByteStream;
use s3s::{S3Request, S3Response, S3Result, S3};

use chrono::Utc;
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use skystore_rust_client::apis::configuration::Configuration;
use skystore_rust_client::apis::default_api as apis;
use skystore_rust_client::models;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::SystemTime;
use tracing::error;

pub struct SkyProxy {
    pub store_clients: HashMap<String, Arc<Box<dyn ObjectStoreClient>>>,
    pub dir_conf: Configuration,
    pub client_from_region: String,
    pub policy: String,
    pub skystore_bucket_prefix: String,
}

impl SkyProxy {
    pub async fn new(
        regions: Vec<String>,
        client_from_region: String,
        local: bool,
        local_server: bool,
        policy: String,
        skystore_bucket_prefix: String,
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
                        skystore_bucket_name,
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
                            if http::StatusCode::INTERNAL_SERVER_ERROR == e.status_code().unwrap() {
                                // Bucket already exists, no action needed
                            } else {
                                panic!("Failed to create bucket: {}", e);
                            }
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
                "http://3.101.67.150:3000".to_string()
            },
            ..Default::default()
        };

        apis::healthz(&dir_conf)
            .await
            .expect("directory service not healthy");

        Self {
            store_clients,
            dir_conf,
            client_from_region,
            policy,
            skystore_bucket_prefix,
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
            policy: self.policy.clone(),
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
            .field("policy", &self.policy)
            .finish()
    }
}

impl SkyProxy {
    async fn locate_object(
        &self,
        bucket: String,
        key: String,
    ) -> Result<Option<models::LocateObjectResponse>, s3s::S3Error> {
        let locate_resp = apis::locate_object(
            &self.dir_conf,
            models::LocateObjectRequest {
                bucket,
                key,
                client_from_region: self.client_from_region.clone(),
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
            // if bucket name starts with skystore-, then it's a skybucket already created during initialization
            // so we don't need to create it again
            if bucket_name.starts_with("skystore-") {
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

            tasks.spawn(async move {
                let create_bucket_req = new_create_bucket_request(bucket_name.clone(), None);
                client
                    .create_bucket(S3Request::new(create_bucket_req))
                    .await
                    .unwrap();

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
                        last_modified: Some(string_to_timestamp(&obj.last_modified)),
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
        // Directly call the control plane instead of going through the object store
        let head_resp = apis::head_object(
            &self.dir_conf,
            models::HeadObjectRequest {
                bucket: req.input.bucket.clone(),
                key: req.input.key.clone(),
            },
        )
        .await;

        let output = match head_resp {
            Ok(resp) => HeadObjectOutput {
                content_length: resp.size as i64,
                e_tag: Some(resp.etag),
                last_modified: Some(string_to_timestamp(&resp.last_modified)),
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
                let policy = self.policy.clone();

                let src_bucket = src_locator.bucket.clone();
                let src_key = src_locator.key.clone();

                tasks.spawn(async move {
                    let copy_resp = client
                        .copy_object(S3Request::new(new_copy_object_request(
                            src_bucket,
                            src_key,
                            locator.bucket.clone(),
                            locator.key.clone(),
                        )))
                        .await
                        .unwrap();

                    let etag = copy_resp.output.copy_object_result.unwrap().e_tag.unwrap();

                    let head_output = client
                        .head_object(S3Request::new(new_head_object_request(
                            locator.bucket.clone(),
                            locator.key.clone(),
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
                            policy: Some(policy),
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
        let bucket = req.input.bucket.clone();
        let key = req.input.key.clone();

        let locator = self.locate_object(bucket.clone(), key.clone()).await?;

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
                                input
                            }))
                            .await?;
                        let data = get_resp.output.body.unwrap();

                        // Spawn a background task to store the object in the local object store
                        let dir_conf_clone = self.dir_conf.clone();
                        let client_from_region_clone = self.client_from_region.clone();
                        let store_clients_clone = self.store_clients.clone();
                        let policy = self.policy.clone();

                        let mut input_blobs = split_streaming_blob(data, 2); // locators.len() + 1
                        let response_blob = input_blobs.pop();

                        tokio::spawn(async move {
                            let start_upload_resp_result = apis::start_upload(
                                &dir_conf_clone,
                                models::StartUploadRequest {
                                    bucket: bucket.clone(),
                                    key: key.clone(),
                                    client_from_region: client_from_region_clone.clone(),
                                    is_multipart: false,
                                    copy_src_bucket: None,
                                    copy_src_key: None,
                                    policy: Some(policy.clone()),
                                },
                            )
                            .await;

                            // In case of multi-concurrent GET request with copy_on_read policy,
                            // only upload if start_upload returns successful, this indicates that the object is not in the local object store
                            // status neither pending nor ready
                            if let Ok(start_upload_resp) = start_upload_resp_result {
                                let locators = start_upload_resp.locators;
                                let request_template = clone_put_object_request(
                                    &new_put_object_request(bucket.clone(), key.clone()),
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
                                        )))
                                        .await
                                        .unwrap();
                                    apis::complete_upload(
                                        &dir_conf_clone,
                                        models::PatchUploadIsCompleted {
                                            id: locator.id,
                                            size: head_resp.output.content_length as u64,
                                            etag: e_tag.clone(),
                                            last_modified: timestamp_to_string(
                                                head_resp.output.last_modified.unwrap(),
                                            ),
                                            policy: Some(policy.clone()),
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
                            ..Default::default()
                        });
                        return Ok(response);
                    } else {
                        return self
                            .store_clients
                            .get(&self.client_from_region)
                            .unwrap()
                            .get_object(req.map_input(|mut input: GetObjectInput| {
                                input.bucket = location.bucket;
                                input.key = location.key;
                                input
                            }))
                            .await;
                    }
                } else {
                    return self
                        .store_clients
                        .get(&location.tag)
                        .unwrap()
                        .get_object(req.map_input(|mut input: GetObjectInput| {
                            input.bucket = location.bucket;
                            input.key = location.key;
                            input
                        }))
                        .await;
                }
            }
            None => Err(s3s::S3Error::with_message(
                s3s::S3ErrorCode::NoSuchKey,
                "Object not found",
            )),
        }
    }

    #[tracing::instrument(level = "info")]
    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        // Idempotent PUT
        let locator = self
            .locate_object(req.input.bucket.clone(), req.input.key.clone())
            .await?;
        if let Some(locator) = locator {
            return Ok(S3Response::new(PutObjectOutput {
                e_tag: locator.etag,
                ..Default::default()
            }));
        }

        let start_upload_resp = apis::start_upload(
            &self.dir_conf,
            models::StartUploadRequest {
                bucket: req.input.bucket.clone(),
                key: req.input.key.clone(),
                client_from_region: self.client_from_region.clone(),
                is_multipart: false,
                copy_src_bucket: None,
                copy_src_key: None,
                policy: Some(self.policy.clone()),
            },
        )
        .await
        .unwrap();

        let mut tasks = tokio::task::JoinSet::new();
        let locators = start_upload_resp.locators;
        let request_template = clone_put_object_request(&req.input, None);
        let input_blobs = split_streaming_blob(req.input.body.unwrap(), locators.len());

        locators
            .into_iter()
            .zip(input_blobs.into_iter())
            .for_each(|(locator, input_blob)| {
                let conf = self.dir_conf.clone();
                let client: Arc<Box<dyn ObjectStoreClient>> =
                    self.store_clients.get(&locator.tag).unwrap().clone();
                let policy_clone = self.policy.clone();
                let req = S3Request::new(clone_put_object_request(
                    &request_template,
                    Some(input_blob),
                ))
                .map_input(|mut input| {
                    input.bucket = locator.bucket.clone();
                    input.key = locator.key.clone();
                    input
                });

                let (content_length, bucket, key) = (
                    req.input.content_length,
                    locator.bucket.clone(),
                    locator.key.clone(),
                );

                tasks.spawn(async move {
                    let put_resp = client.put_object(req).await.unwrap();
                    let e_tag = put_resp.output.e_tag.unwrap();

                    // Retrieve the object metatada through HEAD request.
                    // So we get the proper size, etag, and last_modified.
                    // No need to fetch from S3 if content length is provided, assume (size = input.content_length, last_modified=current time)
                    let (size_to_set, last_modified) = match content_length {
                        Some(length) => (length, current_timestamp_string()),
                        None => {
                            // Fetch from S3 when content_length is not provided
                            let head_resp = client
                                .head_object(S3Request::new(new_head_object_request(bucket, key)))
                                .await
                                .unwrap();
                            (
                                head_resp.output.content_length,
                                timestamp_to_string(head_resp.output.last_modified.unwrap()),
                            )
                        }
                    };

                    apis::complete_upload(
                        &conf,
                        models::PatchUploadIsCompleted {
                            id: locator.id,
                            size: size_to_set as u64,
                            etag: e_tag.clone(),
                            last_modified,
                            policy: Some(policy_clone),
                        },
                    )
                    .await
                    .unwrap();

                    e_tag
                });
            });

        let mut e_tags = Vec::new();
        while let Some(Ok(e_tag)) = tasks.join_next().await {
            e_tags.push(e_tag);
        }

        Ok(S3Response::new(PutObjectOutput {
            e_tag: e_tags.pop(),
            ..Default::default()
        }))
    }

    #[tracing::instrument(level = "info")]
    async fn delete_objects(
        &self,
        req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        // Send start delete objects request
        let keys: Vec<_> = req
            .input
            .delete
            .objects
            .iter()
            .map(|obj| obj.key.clone())
            .collect();
        let delete_objects_resp = match apis::start_delete_objects(
            &self.dir_conf,
            models::DeleteObjectsRequest {
                bucket: req.input.bucket.clone(),
                keys: keys.clone(),
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
                return Ok(S3Response::new(DeleteObjectsOutput {
                    deleted: Some(
                        keys.iter()
                            .map(|key| DeletedObject {
                                key: Some(key.clone()),
                                delete_marker: false,
                                ..Default::default()
                            })
                            .collect(),
                    ),
                    errors: None,
                    ..Default::default()
                }));
            }
        };

        // Delete objects in actual storages
        let mut tasks = tokio::task::JoinSet::new();
        for (key, locators) in delete_objects_resp.locators {
            for locator in locators {
                let client: Arc<Box<dyn ObjectStoreClient>> =
                    self.store_clients.get(&locator.tag).unwrap().clone();
                let bucket_name = locator.bucket.clone();
                let key_clone = key.clone(); // Clone the key here

                tasks.spawn(async move {
                    let delete_object_response = client
                        .delete_object(S3Request::new(new_delete_object_request(
                            bucket_name.clone(),
                            locator.key,
                        )))
                        .await;

                    match delete_object_response {
                        Ok(_resp) => Ok((key_clone.clone(), locator.id)),
                        Err(_) => Err((
                            key_clone.clone(),
                            format!("Failed to delete object with key: {}", key_clone),
                        )),
                    }
                });
            }
        }

        // Collect results and complete delete objects
        let mut key_to_results: HashMap<String, Vec<Result<i32, String>>> = HashMap::new();
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(res) => match res {
                    Ok((key, id)) => {
                        key_to_results.entry(key).or_default().push(Ok(id));
                    }
                    Err((key, err)) => {
                        key_to_results.entry(key).or_default().push(Err(err));
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
            if !completed_ids.is_empty() {
                apis::complete_delete_objects(
                    &self.dir_conf,
                    models::DeleteObjectsIsCompleted {
                        ids: completed_ids,
                        multipart_upload_ids: None,
                    },
                )
                .await
                .unwrap();
            }

            if fails.is_empty() {
                deleted_objects.push(DeletedObject {
                    key: Some(key),
                    delete_marker: false, // TODO: add versioning support
                    ..Default::default()
                });
            } else {
                errors.push(Error {
                    key: Some(key),
                    code: Some("InternalError".to_string()),
                    message: Some(fails.into_iter().map(Result::unwrap_err).collect()),
                    ..Default::default()
                });
            }
        }

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
                version_id: req.input.version_id.clone(),
            }],
            ..Default::default()
        };

        let delete_object_input = new_delete_objects_request(req.input.bucket.clone(), delete);
        let delete_object_req = S3Request::new(delete_object_input);

        match self.delete_objects(delete_object_req).await {
            Ok(delete_objects_resp) => {
                if delete_objects_resp
                    .output
                    .deleted
                    .and_then(|objs| {
                        objs.into_iter()
                            .find(|obj| obj.key.as_ref() == Some(&req.input.key))
                    })
                    .is_some()
                {
                    Ok(S3Response::new(DeleteObjectOutput {
                        delete_marker: false, // TODO: add versioning support
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

    #[tracing::instrument(level = "info")]
    async fn copy_object(
        &self,
        req: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        //TODO for future: people commonly use copy_object to modify the metadata, instead of creatig a new today.

        let [src_bucket, src_key] = match &req.input.copy_source {
            CopySource::Bucket { bucket, key, .. } => [bucket, key],
            CopySource::AccessPoint { .. } => panic!("AccessPoint not supported"),
        };
        let [dst_bucket, dst_key] = [&req.input.bucket, &req.input.key];

        let src_locator = self
            .locate_object(src_bucket.to_string(), src_key.to_string())
            .await?;
        let dst_locator = self
            .locate_object(dst_bucket.to_string(), dst_key.to_string())
            .await?;

        if src_locator.is_none() {
            return Err(s3s::S3Error::with_message(
                s3s::S3ErrorCode::NoSuchKey,
                "Source object not found",
            ));
        };

        if let Some(dst_locator) = dst_locator {
            return Ok(S3Response::new(CopyObjectOutput {
                copy_object_result: Some(CopyObjectResult {
                    e_tag: dst_locator.etag,
                    last_modified: Some(string_to_timestamp(
                        dst_locator.last_modified.as_ref().unwrap(),
                    )),
                    ..Default::default()
                }),
                ..Default::default()
            }));
        };

        let upload_resp = apis::start_upload(
            &self.dir_conf,
            models::StartUploadRequest {
                bucket: dst_bucket.to_string(),
                key: dst_key.to_string(),
                client_from_region: self.client_from_region.clone(),
                is_multipart: false,
                copy_src_bucket: Some(src_bucket.to_string()),
                copy_src_key: Some(src_key.to_string()),
                policy: Some(self.policy.clone()),
            },
        )
        .await
        .unwrap();

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
                let policy_clone = self.policy.clone();

                tasks.spawn(async move {
                    let copy_resp = client
                        .copy_object(S3Request::new(new_copy_object_request(
                            src_bucket.clone(),
                            src_key.clone(),
                            locator.bucket.clone(),
                            locator.key.clone(),
                        )))
                        .await
                        .unwrap();

                    let etag = copy_resp.output.copy_object_result.unwrap().e_tag.unwrap();

                    let head_output = client
                        .head_object(S3Request::new(new_head_object_request(
                            locator.bucket.clone(),
                            locator.key.clone(),
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
                            policy: Some(policy_clone),
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

        Ok(S3Response::new(CopyObjectOutput {
            copy_object_result: Some(CopyObjectResult {
                e_tag: e_tags.pop(),
                ..Default::default()
            }),
            ..Default::default()
        }))
    }

    #[tracing::instrument(level = "info")]
    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        let locator = self
            .locate_object(req.input.bucket.clone(), req.input.key.clone())
            .await?;

        if locator.is_some() {
            return Err(s3s::S3Error::with_message(
                s3s::S3ErrorCode::InternalError,
                "Object already exists. TODO for multipart upload overwite.",
            ));
        };

        let policy = self.policy.clone();
        let upload_resp = apis::start_upload(
            &self.dir_conf,
            models::StartUploadRequest {
                bucket: req.input.bucket.clone(),
                key: req.input.key.clone(),
                client_from_region: self.client_from_region.clone(),
                is_multipart: true,
                copy_src_bucket: None,
                copy_src_key: None,
                policy: Some(policy),
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

        Ok(S3Response::new(CreateMultipartUploadOutput {
            bucket: Some(req.input.bucket.clone()),
            key: Some(req.input.key.clone()),
            upload_id: upload_resp.multipart_upload_id,
            ..Default::default()
        }))
    }

    #[tracing::instrument(level = "info")]
    async fn abort_multipart_upload(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        let delete_start_response = apis::start_delete_objects(
            &self.dir_conf,
            models::DeleteObjectsRequest {
                bucket: req.input.bucket.clone(),
                keys: vec![req.input.key.clone()],
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
                    },
                )
                .await
                .unwrap();
            }
        }

        Ok(S3Response::new(AbortMultipartUploadOutput::default()))
    }

    #[tracing::instrument(level = "info")]
    async fn list_multipart_uploads(
        &self,
        req: S3Request<ListMultipartUploadsInput>,
    ) -> S3Result<S3Response<ListMultipartUploadsOutput>> {
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

        Ok(S3Response::new(ListMultipartUploadsOutput {
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
        }))
    }

    #[tracing::instrument(level = "info")]
    async fn list_parts(
        &self,
        req: S3Request<ListPartsInput>,
    ) -> S3Result<S3Response<ListPartsOutput>> {
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
            Ok(resp) => Ok(S3Response::new(ListPartsOutput {
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
            })),
            Err(err) => {
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
        let resp = apis::continue_upload(
            &self.dir_conf,
            models::ContinueUploadRequest {
                bucket: req.input.bucket.clone(),
                key: req.input.key.clone(),
                multipart_upload_id: req.input.upload_id.clone(),
                client_from_region: self.client_from_region.clone(),
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
        let body_clones = split_streaming_blob(req.input.body.unwrap(), resp.len());
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
                            etag: etag.clone(),
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
        let [src_bucket, src_key] = match &req.input.copy_source {
            CopySource::Bucket { bucket, key, .. } => [bucket.to_string(), key.to_string()],
            CopySource::AccessPoint { .. } => panic!("AccessPoint not supported"),
        };

        let src_locator = self
            .locate_object(src_bucket.clone(), src_key.clone())
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
                    locator.copy_src_bucket.clone().unwrap(),
                    locator.copy_src_key.clone().unwrap(),
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

        Ok(S3Response::new(UploadPartCopyOutput {
            copy_part_result: Some(CopyPartResult {
                e_tag: Some(parts[0].etag.clone()),
                ..Default::default()
            }),
            ..Default::default()
        }))
    }

    #[tracing::instrument(level = "info")]
    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        let locators = match apis::continue_upload(
            &self.dir_conf,
            models::ContinueUploadRequest {
                bucket: req.input.bucket.clone(),
                key: req.input.key.clone(),
                multipart_upload_id: req.input.upload_id.clone(),
                client_from_region: self.client_from_region.clone(),
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
                    policy: Some(self.policy.clone()),
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
            },
        )
        .await
        .unwrap();

        Ok(S3Response::new(CompleteMultipartUploadOutput {
            bucket: Some(req.input.bucket.clone()),
            key: Some(req.input.key.clone()),
            e_tag: Some(head_resp.etag),
            ..Default::default()
        }))
    }
}

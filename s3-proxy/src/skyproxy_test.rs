#[cfg(test)]
mod tests {
    use crate::skyproxy::SkyProxy;
    use crate::utils::type_utils::*;
    use lazy_static::lazy_static;
    use s3s::dto::*;
    use s3s::{S3Request, S3};
    use serial_test::serial;

    lazy_static! {
        static ref REGIONS: Vec<String> = vec![
            "aws:us-west-1".to_string(),
            "aws:us-east-1".to_string(),
            "gcp:us-west1".to_string(),
            "aws:eu-central-1".to_string(),
            "aws:us-west-1".to_string(),
        ];
        static ref CLIENT_FROM_REGION: String = "aws:us-west-1".to_string();
    }

    fn generate_unique_bucket_name() -> String {
        let timestamp = chrono::Utc::now().timestamp_nanos();
        format!("my-bucket-{}", timestamp)
    }

    async fn setup_sky_proxy() -> SkyProxy {
        SkyProxy::new(
            REGIONS.clone(),
            CLIENT_FROM_REGION.clone(),
            true,
            true,
            "push".to_string(),
        )
        .await
    }

    #[tokio::test]
    #[serial]
    async fn test_constructor() {
        let proxy = setup_sky_proxy().await;
        assert!(!proxy.store_clients.is_empty());
    }

    #[tokio::test]
    #[serial]
    async fn test_list_objects() {
        let proxy = setup_sky_proxy().await;

        // create a bucket
        let bucket_name = generate_unique_bucket_name();
        let request = new_create_bucket_request(bucket_name.to_string(), None);
        let req = S3Request::new(request);
        proxy.create_bucket(req).await.unwrap().output;

        let request = new_list_objects_v2_input(bucket_name.to_string(), None);
        let req = S3Request::new(request);
        let resp = proxy.list_objects_v2(req).await.unwrap().output;
        assert!(resp.contents.is_some());
    }

    #[tokio::test]
    #[serial]
    async fn test_put_then_get() {
        let proxy = setup_sky_proxy().await;

        // create a bucket
        let bucket_name = generate_unique_bucket_name();
        let request = new_create_bucket_request(bucket_name.to_string(), None);
        let req = S3Request::new(request);
        proxy.create_bucket(req).await.unwrap().output;

        {
            let mut request = new_put_object_request(bucket_name.to_string(), "my-key".to_string());
            let body = "abcdefg".to_string().into_bytes();
            request.body = Some(s3s::Body::from(body).into());

            let req = S3Request::new(request);
            let resp = proxy.put_object(req).await.unwrap().output;
            assert!(resp.e_tag.is_some());
        }

        {
            let request =
                new_list_objects_v2_input(bucket_name.to_string(), Some("my-key".to_string()));
            let req = S3Request::new(request);
            let resp = proxy.list_objects_v2(req).await.unwrap().output;
            assert!(resp.contents.is_some());
            assert!(resp.contents.unwrap().len() == 1);
        }
        // GET object with "X-SKYSTORE-PULL" header
        {
            let request = new_get_object_request(bucket_name.to_string(), "my-key".to_string());
            let mut req = S3Request::new(request);
            req.headers.insert(
                "X-SKYSTORE-PULL",
                http::HeaderValue::from_str("true").expect("Invalid header value"),
            );

            let resp = proxy.get_object(req).await.unwrap().output;
            assert!(resp.body.is_some());

            let resp_body = resp.body.unwrap();

            use tokio_stream::StreamExt;

            let result_bytes = resp_body
                .map(|chunk| chunk.unwrap())
                .collect::<Vec<_>>()
                .await;

            let body = result_bytes.concat();
            assert!(body == "abcdefg".to_string().into_bytes());
        }
        // GET object without "X-SKYSTORE-PULL" header
        {
            let request = new_get_object_request(bucket_name.to_string(), "my-key".to_string());
            let req = S3Request::new(request);
            let resp = proxy.get_object(req).await.unwrap().output;
            assert!(resp.body.is_some());

            let resp_body = resp.body.unwrap();

            use tokio_stream::StreamExt;

            let result_bytes = resp_body
                .map(|chunk| chunk.unwrap())
                .collect::<Vec<_>>()
                .await;

            let body = result_bytes.concat();
            assert!(body == "abcdefg".to_string().into_bytes());
        }
    }

    #[tokio::test]
    #[serial]
    async fn test_delete_objects() {
        let proxy = setup_sky_proxy().await;

        let bucket_name = generate_unique_bucket_name();
        let request = new_create_bucket_request(bucket_name.to_string(), None);
        let req = S3Request::new(request);
        proxy.create_bucket(req).await.unwrap().output;

        for i in 0..3 {
            let mut request =
                new_put_object_request(bucket_name.to_string(), format!("my-key-{}", i));
            let body = format!("data-{}", i).into_bytes();
            request.body = Some(s3s::Body::from(body).into());

            let req = S3Request::new(request);
            proxy.put_object(req).await.unwrap().output;
        }

        let delete = Delete {
            objects: vec![
                ObjectIdentifier {
                    key: "my-key-0".to_string(),
                    version_id: None,
                },
                ObjectIdentifier {
                    key: "my-key-1".to_string(),
                    version_id: None,
                },
            ],
            ..Default::default()
        };

        let delete_objects_input = new_delete_objects_request(bucket_name.to_string(), delete);
        let delete_objects_req = S3Request::new(delete_objects_input);
        proxy
            .delete_objects(delete_objects_req)
            .await
            .unwrap()
            .output;

        // Verify objects are deleted
        let list_request = new_list_objects_v2_input(bucket_name.to_string(), None);
        let list_resp = proxy
            .list_objects_v2(S3Request::new(list_request))
            .await
            .unwrap()
            .output;
        assert!(list_resp.contents.unwrap().len() == 1);
    }

    #[tokio::test]
    #[serial]
    async fn test_delete_object() {
        let proxy = setup_sky_proxy().await;

        let bucket_name = generate_unique_bucket_name();
        let request = new_create_bucket_request(bucket_name.to_string(), None);
        let req = S3Request::new(request);
        proxy.create_bucket(req).await.unwrap().output;

        {
            let mut request =
                new_put_object_request(bucket_name.to_string(), "my-single-key".to_string());
            let body = "single-data".to_string().into_bytes();
            request.body = Some(s3s::Body::from(body).into());

            let req = S3Request::new(request);
            proxy.put_object(req).await.unwrap().output;
        }

        let delete_object_input =
            new_delete_object_request(bucket_name.to_string(), "my-single-key".to_string());
        let delete_object_req = S3Request::new(delete_object_input);
        proxy.delete_object(delete_object_req).await.unwrap().output;

        // Verify a single object was deleted
        let list_request = new_list_objects_v2_input(bucket_name.to_string(), None);
        let list_resp = proxy
            .list_objects_v2(S3Request::new(list_request))
            .await
            .unwrap()
            .output;
        assert!(list_resp.contents.unwrap().is_empty());
    }

    #[tokio::test]
    #[serial]
    async fn test_copy_object() {
        let proxy = setup_sky_proxy().await;

        // create a bucket
        let bucket_name = generate_unique_bucket_name();
        let request = new_create_bucket_request(bucket_name.to_string(), None);
        let req = S3Request::new(request);
        proxy.create_bucket(req).await.unwrap().output;

        // put an object to my-bucket/my-copy-key
        {
            let mut request =
                new_put_object_request(bucket_name.to_string(), "my-copy-key".to_string());
            let body = "abcdefg".to_string().into_bytes();
            request.body = Some(s3s::Body::from(body).into());

            let req = S3Request::new(request);
            let resp = proxy.put_object(req).await.unwrap().output;
            assert!(resp.e_tag.is_some());
        }

        // copy the object
        {
            let request = new_copy_object_request(
                bucket_name.to_string(),
                "my-copy-key".to_string(),
                bucket_name.to_string(),
                "my-copy-key-copy".to_string(),
            );
            let req = S3Request::new(request);
            let resp = proxy.copy_object(req).await.unwrap().output;
            assert!(resp.copy_object_result.is_some());
        }

        // get the object at my-copy-key-copy
        {
            let request =
                new_get_object_request(bucket_name.to_string(), "my-copy-key-copy".to_string());
            let req = S3Request::new(request);
            let resp = proxy.get_object(req).await.unwrap().output;
            assert!(resp.body.is_some());

            let resp_body = resp.body.unwrap();

            use tokio_stream::StreamExt;

            let result_bytes = resp_body
                .map(|chunk| chunk.unwrap())
                .collect::<Vec<_>>()
                .await;

            let body = result_bytes.concat();
            assert!(body == "abcdefg".to_string().into_bytes());
        }
    }

    #[tokio::test]
    #[serial]
    // #[ignore = "UploadPartCopy is not implemented in the emulator."]
    async fn test_multipart_flow() {
        let proxy = setup_sky_proxy().await;

        // create a bucket
        let bucket_name = generate_unique_bucket_name();
        let request = new_create_bucket_request(bucket_name.to_string(), None);
        let req = S3Request::new(request);
        proxy.create_bucket(req).await.unwrap().output;

        // AWS's The minimal multipart upload size is 5Mb
        // which is pretty sad but we have to test it against real service here.
        let part_size = 10 * 1024 * 1024;

        // initiate multipart upload
        let upload_id = {
            let request = new_create_multipart_upload_request(
                bucket_name.to_string(),
                "my-multipart-key".to_string(),
            );
            let req = S3Request::new(request);
            let resp = proxy.create_multipart_upload(req).await.unwrap().output;
            assert!(resp.upload_id.is_some());
            resp.upload_id.unwrap()
        };

        // test list multipart upload contains upload_id
        {
            let request = new_list_multipart_uploads_request(
                bucket_name.to_string(),
                "my-multipart-key".to_string(),
            );
            let req = S3Request::new(request);
            let resp = proxy.list_multipart_uploads(req).await.unwrap().output;
            assert!(resp.uploads.is_some());
            let uploads = resp.uploads.unwrap();
            assert!(!uploads.is_empty());
            let found_upload = uploads
                .iter()
                .find(|upload: &&MultipartUpload| upload.upload_id == Some(upload_id.clone()));
            assert!(found_upload.is_some());
        }

        // upload part 1
        let etag1 = {
            let mut request = new_upload_part_request(
                bucket_name.to_string(),
                "my-multipart-key".to_string(),
                upload_id.clone(),
                1,
            );
            let body: Vec<u8> = vec![0; part_size];
            // let body: Vec<u8> = vec![0; 6];
            request.body = Some(s3s::Body::from(body).into());

            let req = S3Request::new(request);
            let resp = proxy.upload_part(req).await.unwrap().output;

            resp.e_tag.unwrap()
        };

        // list parts
        {
            let request = new_list_parts_request(
                bucket_name.to_string(),
                "my-multipart-key".to_string(),
                upload_id.clone(),
            );
            let req = S3Request::new(request);
            let resp = proxy.list_parts(req).await.unwrap().output;
            assert!(resp.parts.is_some());
            let parts = resp.parts.unwrap();
            assert!(parts.len() == 1);
            assert!(parts[0].part_number == 1);
        }

        // test upload part copy
        let etag2 = {
            // start by uploading a simple object
            let mut request =
                new_put_object_request(bucket_name.to_string(), "my-copy-src-key".to_string());
            let body: Vec<u8> = vec![0; part_size];
            request.body = Some(s3s::Body::from(body).into());
            let req = S3Request::new(request);
            let resp = proxy.put_object(req).await.unwrap().output;
            assert!(resp.e_tag.is_some());

            // now issue a copy part request
            let request = new_upload_part_copy_request(
                bucket_name.to_string(),
                "my-multipart-key".to_string(),
                upload_id.clone(),
                2,
                bucket_name.to_string(),
                "my-copy-src-key".to_string(),
            );

            let req = S3Request::new(request);
            let resp = proxy.upload_part_copy(req).await.unwrap().output;
            assert!(resp.copy_part_result.is_some());
            resp.copy_part_result.unwrap().e_tag.unwrap()
        };

        // complete the upload
        {
            let request = new_complete_multipart_request(
                bucket_name.to_string(),
                "my-multipart-key".to_string(),
                upload_id.clone(),
                CompletedMultipartUpload {
                    parts: Some(vec![
                        CompletedPart {
                            e_tag: Some(etag1),
                            part_number: 1,
                            checksum_crc32: None,
                            checksum_crc32c: None,
                            checksum_sha1: None,
                            checksum_sha256: None,
                        },
                        CompletedPart {
                            e_tag: Some(etag2),
                            part_number: 2,
                            checksum_crc32: None,
                            checksum_crc32c: None,
                            checksum_sha1: None,
                            checksum_sha256: None,
                        },
                    ]),
                },
            );
            let req = S3Request::new(request);
            let resp = proxy.complete_multipart_upload(req).await.unwrap().output;
            assert!(resp.e_tag.is_some());

            // We should able to get the content of the object
            let request =
                new_get_object_request(bucket_name.to_string(), "my-multipart-key".to_string());
            let req = S3Request::new(request);
            let resp = proxy.get_object(req).await.unwrap().output;
            assert!(resp.body.is_some());

            let resp_body = resp.body.unwrap();

            use tokio_stream::StreamExt;

            let result_bytes = resp_body
                .map(|chunk| chunk.unwrap())
                .collect::<Vec<_>>()
                .await;

            let body = result_bytes.concat();
            assert!(body.len() == part_size * 2);
            // assert!(body.len() == 6 * 2);
            assert!(body[0] == 0);
        }
    }

    #[tokio::test]
    #[serial]
    async fn test_multipart_many_parts() {
        let proxy = setup_sky_proxy().await;

        // create a bucket
        let bucket_name = generate_unique_bucket_name();
        let request = new_create_bucket_request(bucket_name.to_string(), None);
        let req = S3Request::new(request);
        proxy.create_bucket(req).await.unwrap().output;

        // initiate multipart upload
        let upload_id = {
            let request = new_create_multipart_upload_request(
                bucket_name.to_string(),
                "my-multipart-many-parts-key".to_string(),
            );
            let req = S3Request::new(request);
            let resp = proxy.create_multipart_upload(req).await.unwrap().output;
            assert!(resp.upload_id.is_some());
            resp.upload_id.unwrap()
        };

        // upload 100 parts
        let mut etags = Vec::new();
        for i in 1..=40 {
            let mut request = new_upload_part_request(
                bucket_name.to_string(),
                "my-multipart-many-parts-key".to_string(),
                upload_id.clone(),
                i,
            );
            let body: Vec<u8> = vec![0; 5 * 1024 * 1024];
            request.body = Some(s3s::Body::from(body).into());

            let req = S3Request::new(request);
            let resp = proxy.upload_part(req).await.unwrap().output;

            etags.push(resp.e_tag.unwrap());
        }

        // complete the upload
        {
            let request = new_complete_multipart_request(
                bucket_name.to_string(),
                "my-multipart-many-parts-key".to_string(),
                upload_id.clone(),
                CompletedMultipartUpload {
                    parts: Some(
                        etags
                            .iter()
                            .enumerate()
                            .map(|(i, etag)| CompletedPart {
                                e_tag: Some(etag.clone()),
                                part_number: i as i32 + 1,
                                checksum_crc32: None,
                                checksum_crc32c: None,
                                checksum_sha1: None,
                                checksum_sha256: None,
                            })
                            .collect(),
                    ),
                },
            );
            let req = S3Request::new(request);
            let resp = proxy.complete_multipart_upload(req).await.unwrap().output;
            assert!(resp.e_tag.is_some());

            // We should able to get the content of the object
            let request = new_get_object_request(
                bucket_name.to_string(),
                "my-multipart-many-parts-key".to_string(),
            );
            let req = S3Request::new(request);
            let resp = proxy.get_object(req).await.unwrap().output;
            assert!(resp.body.is_some());

            let resp_body = resp.body.unwrap();

            use tokio_stream::StreamExt;

            let result_bytes = resp_body
                .map(|chunk| chunk.unwrap())
                .collect::<Vec<_>>()
                .await;

            let body = result_bytes.concat();
            assert!(body.len() == 40 * 5 * 1024 * 1024);
        }
    }

    #[tokio::test]
    #[serial]
    async fn test_abort_multipart_upload() {
        let proxy = setup_sky_proxy().await;

        let bucket_name = generate_unique_bucket_name();
        let request = new_create_bucket_request(bucket_name.to_string(), None);
        let req = S3Request::new(request);
        proxy.create_bucket(req).await.unwrap().output;

        let upload_id = {
            let request = new_create_multipart_upload_request(
                bucket_name.to_string(),
                "my-abort-multipart-test-key".to_string(),
            );
            let req = S3Request::new(request);
            let resp = proxy.create_multipart_upload(req).await.unwrap().output;
            assert!(resp.upload_id.is_some());
            resp.upload_id.unwrap()
        };

        let _ = {
            let mut request = new_upload_part_request(
                bucket_name.to_string(),
                "my-abort-multipart-test-key".to_string(),
                upload_id.clone(),
                1,
            );
            let body: Vec<u8> = vec![0; 5 * 1024 * 1024]; // 5MB, minimum size for a part
            request.body = Some(s3s::Body::from(body).into());

            let req = S3Request::new(request);
            let resp = proxy.upload_part(req).await.unwrap().output;

            resp.e_tag.unwrap()
        };

        // Abort the multipart upload
        {
            let request = new_abort_multipart_upload_request(
                bucket_name.to_string(),
                "my-abort-multipart-test-key".to_string(),
                upload_id.clone(),
            );
            let req = S3Request::new(request);

            proxy.abort_multipart_upload(req).await.unwrap().output;
        };

        // Check that the upload ID is no longer listed
        {
            let request = new_list_multipart_uploads_request(
                bucket_name.to_string(),
                "my-abort-multipart-test-key".to_string(),
            );
            let req = S3Request::new(request);
            let resp = proxy.list_multipart_uploads(req).await.unwrap().output;
            assert!(resp.uploads.is_some());
            let uploads = resp.uploads.unwrap();
            let found_upload = uploads
                .iter()
                .find(|upload: &&MultipartUpload| upload.upload_id == Some(upload_id.clone()));
            assert!(found_upload.is_none());
        }

        // Check that we can't list parts using the upload ID
        {
            let request = new_list_parts_request(
                bucket_name.to_string(),
                "my-abort-multipart-test-key".to_string(),
                upload_id.clone(),
            );
            let req = S3Request::new(request);
            let resp = proxy.list_parts(req).await;
            assert!(resp.is_err());
        }

        // Check that we can't get the object
        {
            let request = new_get_object_request(
                bucket_name.to_string(),
                "my-abort-multipart-test-key".to_string(),
            );
            let req = S3Request::new(request);
            let resp = proxy.get_object(req).await;
            assert!(resp.is_err());
        }
    }
}

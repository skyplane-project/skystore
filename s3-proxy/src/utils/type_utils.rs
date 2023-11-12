use s3s::dto::*;

#[allow(dead_code)]
pub fn new_create_bucket_request(bucket: String, region: Option<String>) -> CreateBucketInput {
    let mut builder = CreateBucketInput::builder();
    builder.set_bucket(bucket);
    if let Some(r) = region {
        builder.set_create_bucket_configuration(Some(CreateBucketConfiguration {
            location_constraint: Some(BucketLocationConstraint::from(r)),
        }));
    }
    builder.build().unwrap()
}

pub fn new_put_bucket_versioning_request(
    bucket: String,
    versioning_configuration: VersioningConfiguration,
) -> PutBucketVersioningInput {
    let mut builder = PutBucketVersioningInput::builder();
    builder.set_bucket(bucket);
    builder.set_versioning_configuration(versioning_configuration);
    builder.build().unwrap()
}

#[allow(dead_code)]
pub fn new_head_bucket_request(bucket: String, _region: Option<String>) -> HeadBucketInput {
    let mut builder = HeadBucketInput::builder();
    builder.set_bucket(bucket);
    builder.build().unwrap()
}

#[allow(dead_code)]
pub fn new_delete_bucket_request(bucket: String) -> DeleteBucketInput {
    let mut builder = DeleteBucketInput::builder();
    builder.set_bucket(bucket);
    builder.build().unwrap()
}

#[allow(dead_code)]
pub fn new_list_buckets_input() -> ListBucketsInput {
    ListBucketsInput::default()
}

#[allow(dead_code)]
pub fn new_list_objects_v2_input(bucket: String, prefix: Option<String>) -> ListObjectsV2Input {
    let mut builder = ListObjectsV2Input::builder();
    builder.set_bucket(bucket);
    builder.set_prefix(prefix);
    builder.build().unwrap()
}

#[allow(dead_code)]
pub fn new_put_object_request(bucket: String, key: String) -> PutObjectInput {
    let mut builder = PutObjectInput::builder();
    builder.set_bucket(bucket);
    builder.set_key(key);
    builder.build().unwrap()
}

#[allow(dead_code)]
pub fn new_get_object_request(bucket: String, key: String) -> GetObjectInput {
    let mut builder = GetObjectInput::builder();
    builder.set_bucket(bucket);
    builder.set_key(key);
    builder.build().unwrap()
}

pub fn new_delete_object_request(bucket: String, key: String) -> DeleteObjectInput {
    let mut builder = DeleteObjectInput::builder();
    builder.set_bucket(bucket);
    builder.set_key(key);
    builder.build().unwrap()
}

pub fn new_delete_objects_request(bucket: String, delete: Delete) -> DeleteObjectsInput {
    let mut builder = DeleteObjectsInput::builder();
    builder.set_bucket(bucket);
    builder.set_delete(delete);
    builder.build().unwrap()
}

pub fn new_copy_object_request(
    src_bucket: String,
    src_key: String,
    dst_bucket: String,
    dst_key: String,
) -> CopyObjectInput {
    let mut builder = CopyObjectInput::builder();
    builder.set_bucket(dst_bucket);
    builder.set_key(dst_key);
    builder.set_copy_source(CopySource::Bucket {
        bucket: src_bucket.into(),
        key: src_key.into(),
        version_id: None,
    });
    builder.build().unwrap()
}

pub fn new_head_object_request(
    bucket: String,
    key: String,
    version_id: Option<String>,
) -> HeadObjectInput {
    let mut builder = HeadObjectInput::builder();
    builder.set_bucket(bucket);
    builder.set_key(key);
    builder.set_version_id(version_id);
    builder.build().unwrap()
}

pub fn new_create_multipart_upload_request(
    bucket: String,
    key: String,
) -> CreateMultipartUploadInput {
    let mut builder = CreateMultipartUploadInput::builder();
    builder.set_bucket(bucket);
    builder.set_key(key);
    builder.build().unwrap()
}

pub fn new_abort_multipart_upload_request(
    bucket: String,
    key: String,
    upload_id: String,
) -> AbortMultipartUploadInput {
    let mut builder = AbortMultipartUploadInput::builder();
    builder.set_bucket(bucket);
    builder.set_key(key);
    builder.set_upload_id(upload_id);
    builder.build().unwrap()
}

#[allow(dead_code)]
pub fn new_list_multipart_uploads_request(
    bucket: String,
    prefix: String,
) -> ListMultipartUploadsInput {
    let mut builder = ListMultipartUploadsInput::builder();
    builder.set_bucket(bucket);
    builder.set_prefix(Some(prefix));
    builder.build().unwrap()
}

#[allow(dead_code)]
pub fn new_upload_part_request(
    bucket: String,
    key: String,
    upload_id: String,
    part_number: i32,
) -> UploadPartInput {
    let mut builder = UploadPartInput::builder();
    builder.set_bucket(bucket);
    builder.set_key(key);
    builder.set_upload_id(upload_id);
    builder.set_part_number(part_number);
    builder.build().unwrap()
}

#[allow(dead_code)]
pub fn new_list_parts_request(bucket: String, key: String, upload_id: String) -> ListPartsInput {
    let mut builder = ListPartsInput::builder();
    builder.set_bucket(bucket);
    builder.set_key(key);
    builder.set_upload_id(upload_id);
    builder.build().unwrap()
}

pub fn new_complete_multipart_request(
    bucket: String,
    key: String,
    upload_id: String,
    multipart_upload: CompletedMultipartUpload,
) -> CompleteMultipartUploadInput {
    let mut builder = CompleteMultipartUploadInput::builder();
    builder.set_bucket(bucket);
    builder.set_key(key);
    builder.set_upload_id(upload_id);
    builder.set_multipart_upload(Some(multipart_upload));
    builder.build().unwrap()
}

pub fn new_upload_part_copy_request(
    bucket: String,
    key: String,
    upload_id: String,
    part_number: i32,
    copy_source_bucket: String,
    copy_source_key: String,
    copy_source_range: Option<String>, // e.g. "bytes=0-100"
) -> UploadPartCopyInput {
    let mut builder = UploadPartCopyInput::builder();
    builder.set_bucket(bucket);
    builder.set_key(key);
    builder.set_upload_id(upload_id);
    builder.set_part_number(part_number);
    builder.set_copy_source(CopySource::Bucket {
        bucket: copy_source_bucket.into(),
        key: copy_source_key.into(),
        version_id: None,
    });
    builder.set_copy_source_range(copy_source_range);
    builder.build().unwrap()
}

pub fn clone_put_object_request(
    inp: &PutObjectInput,
    body: Option<StreamingBlob>,
) -> PutObjectInput {
    PutObjectInput {
        body,
        acl: inp.acl.clone(),
        bucket: inp.bucket.clone(),
        key: inp.key.clone(),
        bucket_key_enabled: inp.bucket_key_enabled,
        cache_control: inp.cache_control.clone(),
        checksum_algorithm: inp.checksum_algorithm.clone(),
        checksum_crc32: inp.checksum_crc32.clone(),
        checksum_crc32c: inp.checksum_crc32c.clone(),
        checksum_sha1: inp.checksum_sha1.clone(),
        checksum_sha256: inp.checksum_sha256.clone(),
        content_disposition: inp.content_disposition.clone(),
        content_encoding: inp.content_encoding.clone(),
        content_language: inp.content_language.clone(),
        content_length: inp.content_length,
        content_md5: inp.content_md5.clone(),
        content_type: inp.content_type.clone(),
        expected_bucket_owner: inp.expected_bucket_owner.clone(),
        expires: inp.expires.as_ref().map(clone_timestamp),
        grant_full_control: inp.grant_full_control.clone(),
        grant_read: inp.grant_read.clone(),
        grant_read_acp: inp.grant_read_acp.clone(),
        grant_write_acp: inp.grant_write_acp.clone(),
        metadata: inp.metadata.clone(),
        object_lock_legal_hold_status: inp.object_lock_legal_hold_status.clone(),
        object_lock_mode: inp.object_lock_mode.clone(),
        object_lock_retain_until_date: inp
            .object_lock_retain_until_date
            .as_ref()
            .map(clone_timestamp),
        request_payer: inp.request_payer.clone(),
        sse_customer_algorithm: inp.sse_customer_algorithm.clone(),
        sse_customer_key: inp.sse_customer_key.clone(),
        sse_customer_key_md5: inp.sse_customer_key_md5.clone(),
        ssekms_encryption_context: inp.ssekms_encryption_context.clone(),
        ssekms_key_id: inp.ssekms_key_id.clone(),
        server_side_encryption: inp.server_side_encryption.clone(),
        storage_class: inp.storage_class.clone(),
        tagging: inp.tagging.clone(),
        website_redirect_location: inp.website_redirect_location.clone(),
    }
}

pub fn clone_upload_part_request(
    inp: &UploadPartInput,
    body: Option<StreamingBlob>,
) -> UploadPartInput {
    UploadPartInput {
        body,
        bucket: inp.bucket.clone(),
        checksum_algorithm: inp.checksum_algorithm.clone(),
        checksum_crc32: inp.checksum_crc32.clone(),
        checksum_crc32c: inp.checksum_crc32c.clone(),
        checksum_sha1: inp.checksum_sha1.clone(),
        checksum_sha256: inp.checksum_sha256.clone(),
        content_length: inp.content_length,
        content_md5: inp.content_md5.clone(),
        expected_bucket_owner: inp.expected_bucket_owner.clone(),
        key: inp.key.clone(),
        part_number: inp.part_number,
        request_payer: inp.request_payer.clone(),
        sse_customer_algorithm: inp.sse_customer_algorithm.clone(),
        sse_customer_key: inp.sse_customer_key.clone(),
        sse_customer_key_md5: inp.sse_customer_key_md5.clone(),
        upload_id: inp.upload_id.clone(),
    }
}

pub fn current_timestamp_string() -> String {
    let formatted_string = time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap();
    formatted_string.to_string()
}

pub fn clone_timestamp(timestamp: &Timestamp) -> Timestamp {
    let mut buf: Vec<u8> = Vec::new();
    timestamp
        .format(TimestampFormat::EpochSeconds, &mut buf)
        .unwrap();
    Timestamp::from(
        time::OffsetDateTime::from_unix_timestamp(
            std::str::from_utf8(&buf).unwrap().parse::<i64>().unwrap(),
        )
        .unwrap(),
    )
}

pub fn timestamp_to_string(timestamp: Timestamp) -> String {
    let mut buf = Vec::new();
    timestamp
        .format(TimestampFormat::DateTime, &mut buf)
        .unwrap();
    String::from_utf8(buf).unwrap()
}

pub fn string_to_timestamp(timestamp: &str) -> Timestamp {
    let fmt = if timestamp.contains('.') {
        "%Y-%m-%dT%H:%M:%S.%f"
    } else {
        "%Y-%m-%dT%H:%M:%S"
    };

    Timestamp::from(
        time::OffsetDateTime::from_unix_timestamp(
            chrono::NaiveDateTime::parse_from_str(timestamp, fmt)
                .unwrap()
                .timestamp(),
        )
        .unwrap(),
    )
}

use skystore_rust_client::apis::Error;
pub fn locate_response_is_404<T>(error: &Error<T>) -> bool {
    match error {
        Error::ResponseError(err) => err.status == 404,
        _ => false,
    }
}

// based on s3, the range HTTP header it supports:
// Int {
//     /// first position
//     first: u64,
//     /// last position
//     last: Option<u64>,
// },
pub fn parse_range(range: &str) -> (u64, Option<u64>) {
    // range: e.g. "bytes=0-100"
    assert!(range.starts_with("bytes="));
    let suffix = &range[6..];
    let parts = suffix.split('-').collect::<Vec<&str>>();
    let start = parts[0].parse::<u64>().unwrap();
    // "bytes=100-"
    if parts[1].is_empty() {
        return (start, None);
    }
    let end = parts[1].parse::<u64>().unwrap();
    (start, Some(end))
}

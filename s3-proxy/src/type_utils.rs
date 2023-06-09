use s3s::dto::*;

#[allow(dead_code)]
pub fn new_list_objects_v2_input(bucket: String, prefix: Option<String>) -> ListObjectsV2Input {
    ListObjectsV2Input {
        bucket,
        continuation_token: None,
        delimiter: None,
        encoding_type: None,
        expected_bucket_owner: None,
        fetch_owner: None,
        max_keys: None,
        prefix,
        request_payer: None,
        start_after: None,
    }
}

#[allow(dead_code)]
pub fn new_put_object_request(bucket: String, key: String) -> PutObjectInput {
    PutObjectInput {
        bucket,
        key,
        body: None,
        acl: None,
        bucket_key_enabled: None,
        cache_control: None,
        checksum_algorithm: None,
        checksum_crc32: None,
        checksum_crc32c: None,
        checksum_sha1: None,
        checksum_sha256: None,
        content_disposition: None,
        content_encoding: None,
        content_language: None,
        content_length: None,
        content_md5: None,
        content_type: None,
        expected_bucket_owner: None,
        expires: None,
        grant_full_control: None,
        grant_read: None,
        grant_read_acp: None,
        grant_write_acp: None,
        metadata: None,
        object_lock_legal_hold_status: None,
        object_lock_mode: None,
        object_lock_retain_until_date: None,
        request_payer: None,
        sse_customer_algorithm: None,
        sse_customer_key: None,
        sse_customer_key_md5: None,
        ssekms_encryption_context: None,
        ssekms_key_id: None,
        server_side_encryption: None,
        storage_class: None,
        tagging: None,
        website_redirect_location: None,
    }
}

#[allow(dead_code)]
pub fn new_get_object_request(bucket: String, key: String) -> GetObjectInput {
    GetObjectInput {
        bucket,
        key,
        expected_bucket_owner: None,
        part_number: None,
        range: None,
        request_payer: None,
        response_cache_control: None,
        response_content_disposition: None,
        response_content_encoding: None,
        response_content_language: None,
        response_content_type: None,
        response_expires: None,
        sse_customer_algorithm: None,
        sse_customer_key: None,
        sse_customer_key_md5: None,
        version_id: None,
        if_match: None,
        if_modified_since: None,
        if_none_match: None,
        if_unmodified_since: None,
        checksum_mode: None,
    }
}

pub fn new_copy_object_request(
    src_bucket: String,
    src_key: String,
    dst_bucket: String,
    dst_key: String,
) -> CopyObjectInput {
    CopyObjectInput {
        acl: None,
        bucket: dst_bucket,
        bucket_key_enabled: None,
        cache_control: None,
        checksum_algorithm: None,
        content_disposition: None,
        content_encoding: None,
        content_language: None,
        content_type: None,
        copy_source: CopySource::Bucket {
            bucket: src_bucket.into(),
            key: src_key.into(),
            version_id: None,
        },
        copy_source_if_match: None,
        copy_source_if_modified_since: None,
        copy_source_if_none_match: None,
        copy_source_if_unmodified_since: None,
        copy_source_sse_customer_algorithm: None,
        copy_source_sse_customer_key: None,
        copy_source_sse_customer_key_md5: None,
        expected_bucket_owner: None,
        expected_source_bucket_owner: None,
        expires: None,
        grant_full_control: None,
        grant_read: None,
        grant_read_acp: None,
        grant_write_acp: None,
        key: dst_key,
        metadata: None,
        metadata_directive: None,
        object_lock_legal_hold_status: None,
        object_lock_mode: None,
        object_lock_retain_until_date: None,
        request_payer: None,
        sse_customer_algorithm: None,
        sse_customer_key: None,
        sse_customer_key_md5: None,
        ssekms_encryption_context: None,
        ssekms_key_id: None,
        server_side_encryption: None,
        storage_class: None,
        tagging: None,
        tagging_directive: None,
        website_redirect_location: None,
    }
}

pub fn new_head_object_request(bucket: String, key: String) -> HeadObjectInput {
    HeadObjectInput {
        bucket,
        key,
        checksum_mode: None,
        expected_bucket_owner: None,
        if_match: None,
        if_modified_since: None,
        if_none_match: None,
        if_unmodified_since: None,
        part_number: None,
        range: None,
        request_payer: None,
        sse_customer_algorithm: None,
        sse_customer_key: None,
        sse_customer_key_md5: None,
        version_id: None,
    }
}

pub fn new_create_multipart_upload_request(
    bucket: String,
    key: String,
) -> CreateMultipartUploadInput {
    CreateMultipartUploadInput {
        acl: None,
        bucket,
        bucket_key_enabled: None,
        cache_control: None,
        checksum_algorithm: None,
        content_disposition: None,
        content_encoding: None,
        content_language: None,
        content_type: None,
        expected_bucket_owner: None,
        expires: None,
        grant_full_control: None,
        grant_read: None,
        grant_read_acp: None,
        grant_write_acp: None,
        key,
        metadata: None,
        object_lock_legal_hold_status: None,
        object_lock_mode: None,
        object_lock_retain_until_date: None,
        request_payer: None,
        sse_customer_algorithm: None,
        sse_customer_key: None,
        sse_customer_key_md5: None,
        ssekms_encryption_context: None,
        ssekms_key_id: None,
        server_side_encryption: None,
        storage_class: None,
        tagging: None,
        website_redirect_location: None,
    }
}

#[allow(dead_code)]
pub fn new_list_multipart_uploads_request(
    bucket: String,
    prefix: String,
) -> ListMultipartUploadsInput {
    ListMultipartUploadsInput {
        bucket,
        delimiter: None,
        encoding_type: None,
        expected_bucket_owner: None,
        key_marker: None,
        max_uploads: None,
        prefix: Some(prefix),
        upload_id_marker: None,
    }
}

#[allow(dead_code)]
pub fn new_upload_part_request(
    bucket: String,
    key: String,
    upload_id: String,
    part_number: i32,
) -> UploadPartInput {
    UploadPartInput {
        body: None,
        bucket,
        checksum_algorithm: None,
        checksum_crc32: None,
        checksum_crc32c: None,
        checksum_sha1: None,
        checksum_sha256: None,
        content_length: None,
        content_md5: None,
        expected_bucket_owner: None,
        key,
        part_number,
        request_payer: None,
        sse_customer_algorithm: None,
        sse_customer_key: None,
        sse_customer_key_md5: None,
        upload_id,
    }
}

#[allow(dead_code)]
pub fn new_list_parts_request(bucket: String, key: String, upload_id: String) -> ListPartsInput {
    ListPartsInput {
        bucket,
        expected_bucket_owner: None,
        key,
        max_parts: None,
        part_number_marker: None,
        request_payer: None,
        sse_customer_algorithm: None,
        sse_customer_key: None,
        sse_customer_key_md5: None,
        upload_id,
    }
}

pub fn new_complete_multipart_request(
    bucket: String,
    key: String,
    upload_id: String,
    multipart_upload: CompletedMultipartUpload,
) -> CompleteMultipartUploadInput {
    CompleteMultipartUploadInput {
        bucket,
        checksum_crc32: None,
        checksum_crc32c: None,
        checksum_sha1: None,
        checksum_sha256: None,
        expected_bucket_owner: None,
        key,
        multipart_upload: Some(multipart_upload),
        request_payer: None,
        sse_customer_algorithm: None,
        sse_customer_key: None,
        sse_customer_key_md5: None,
        upload_id,
    }
}

pub fn new_upload_part_copy_request(
    bucket: String,
    key: String,
    upload_id: String,
    part_number: i32,
    copy_source_bucket: String,
    copy_source_key: String,
) -> UploadPartCopyInput {
    UploadPartCopyInput {
        bucket,
        copy_source: CopySource::Bucket {
            bucket: copy_source_bucket.into(),
            key: copy_source_key.into(),
            version_id: None,
        },
        copy_source_if_match: None,
        copy_source_if_modified_since: None,
        copy_source_if_none_match: None,
        copy_source_if_unmodified_since: None,
        copy_source_range: None,
        copy_source_sse_customer_algorithm: None,
        copy_source_sse_customer_key: None,
        copy_source_sse_customer_key_md5: None,
        expected_bucket_owner: None,
        expected_source_bucket_owner: None,
        key,
        part_number,
        request_payer: None,
        sse_customer_algorithm: None,
        sse_customer_key: None,
        sse_customer_key_md5: None,
        upload_id,
    }
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
        expires: inp.expires.clone(),
        grant_full_control: inp.grant_full_control.clone(),
        grant_read: inp.grant_read.clone(),
        grant_read_acp: inp.grant_read_acp.clone(),
        grant_write_acp: inp.grant_write_acp.clone(),
        metadata: inp.metadata.clone(),
        object_lock_legal_hold_status: inp.object_lock_legal_hold_status.clone(),
        object_lock_mode: inp.object_lock_mode.clone(),
        object_lock_retain_until_date: inp.object_lock_retain_until_date.clone(),
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

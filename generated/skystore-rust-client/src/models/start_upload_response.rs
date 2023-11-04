/*
 * FastAPI
 *
 * No description provided (generated by Openapi Generator https://github.com/openapitools/openapi-generator)
 *
 * The version of the OpenAPI document: 0.1.0
 *
 * Generated by: https://openapi-generator.tech
 */

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StartUploadResponse {
    #[serde(rename = "locators")]
    pub locators: Vec<crate::models::LocateObjectResponse>,
    #[serde(
        rename = "multipart_upload_id",
        skip_serializing_if = "Option::is_none"
    )]
    pub multipart_upload_id: Option<String>,
    #[serde(rename = "copy_src_buckets")]
    pub copy_src_buckets: Vec<String>,
    #[serde(rename = "copy_src_keys")]
    pub copy_src_keys: Vec<String>,
}

impl StartUploadResponse {
    pub fn new(
        locators: Vec<crate::models::LocateObjectResponse>,
        copy_src_buckets: Vec<String>,
        copy_src_keys: Vec<String>,
    ) -> StartUploadResponse {
        StartUploadResponse {
            locators,
            multipart_upload_id: None,
            copy_src_buckets,
            copy_src_keys,
        }
    }
}

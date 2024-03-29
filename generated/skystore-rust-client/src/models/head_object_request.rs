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
pub struct HeadObjectRequest {
    #[serde(rename = "bucket")]
    pub bucket: String,
    #[serde(rename = "key")]
    pub key: String,
}

impl HeadObjectRequest {
    pub fn new(bucket: String, key: String) -> HeadObjectRequest {
        HeadObjectRequest { bucket, key }
    }
}

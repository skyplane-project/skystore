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
pub struct LocateObjectRequest {
    #[serde(rename = "bucket")]
    pub bucket: String,
    #[serde(rename = "key")]
    pub key: String,
    #[serde(rename = "client_from_region")]
    pub client_from_region: String,
}

impl LocateObjectRequest {
    pub fn new(bucket: String, key: String, client_from_region: String) -> LocateObjectRequest {
        LocateObjectRequest {
            bucket,
            key,
            client_from_region,
        }
    }
}



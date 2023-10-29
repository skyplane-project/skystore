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
pub struct DeleteObjectsResponse {
    #[serde(rename = "locators")]
    pub locators: ::std::collections::HashMap<String, Vec<crate::models::LocateObjectResponse>>,
    #[serde(rename = "locator_status_res")]
    pub locator_status_res: ::std::collections::HashMap<String, i32>,
}

impl DeleteObjectsResponse {
    pub fn new(
        locators: ::std::collections::HashMap<String, Vec<crate::models::LocateObjectResponse>>,
        locator_status_res: ::std::collections::HashMap<String, i32>,
    ) -> DeleteObjectsResponse {
        DeleteObjectsResponse {
            locators,
            locator_status_res,
        }
    }
}

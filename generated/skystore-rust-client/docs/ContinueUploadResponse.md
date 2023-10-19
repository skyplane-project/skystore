# ContinueUploadResponse

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **i32** |  | 
**tag** | **String** |  | 
**cloud** | **String** |  | 
**bucket** | **String** |  | 
**region** | **String** |  | 
**key** | **String** |  | 
**status** | Option<[**crate::models::Status**](Status.md)> |  | [optional]
**size** | Option<**u32**> |  | [optional]
**last_modified** | Option<**String**> |  | [optional]
**etag** | Option<**String**> |  | [optional]
**multipart_upload_id** | **String** |  | 
**parts** | Option<[**Vec<crate::models::ContinueUploadPhysicalPart>**](ContinueUploadPhysicalPart.md)> |  | [optional]
**copy_src_bucket** | Option<**String**> |  | [optional]
**copy_src_key** | Option<**String**> |  | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)



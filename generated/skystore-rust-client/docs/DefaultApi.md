# \DefaultApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**append_part**](DefaultApi.md#append_part) | **PATCH** /append_part | Append Part
[**complete_upload**](DefaultApi.md#complete_upload) | **PATCH** /complete_upload | Complete Upload
[**continue_upload**](DefaultApi.md#continue_upload) | **POST** /continue_upload | Continue Upload
[**head_object**](DefaultApi.md#head_object) | **POST** /head_object | Head Object
[**healthz**](DefaultApi.md#healthz) | **GET** /healthz | Healthz
[**list_multipart_uploads**](DefaultApi.md#list_multipart_uploads) | **POST** /list_multipart_uploads | List Multipart Uploads
[**list_objects**](DefaultApi.md#list_objects) | **POST** /list_objects | List Objects
[**list_parts**](DefaultApi.md#list_parts) | **POST** /list_parts | List Parts
[**locate_object**](DefaultApi.md#locate_object) | **POST** /locate_object | Locate Object
[**set_multipart_id**](DefaultApi.md#set_multipart_id) | **PATCH** /set_multipart_id | Set Multipart Id
[**start_upload**](DefaultApi.md#start_upload) | **POST** /start_upload | Start Upload



## append_part

> serde_json::Value append_part(patch_upload_multipart_upload_part)
Append Part

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**patch_upload_multipart_upload_part** | [**PatchUploadMultipartUploadPart**](PatchUploadMultipartUploadPart.md) |  | [required] |

### Return type

[**serde_json::Value**](serde_json::Value.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## complete_upload

> serde_json::Value complete_upload(patch_upload_is_completed)
Complete Upload

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**patch_upload_is_completed** | [**PatchUploadIsCompleted**](PatchUploadIsCompleted.md) |  | [required] |

### Return type

[**serde_json::Value**](serde_json::Value.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## continue_upload

> Vec<crate::models::ContinueUploadResponse> continue_upload(continue_upload_request)
Continue Upload

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**continue_upload_request** | [**ContinueUploadRequest**](ContinueUploadRequest.md) |  | [required] |

### Return type

[**Vec<crate::models::ContinueUploadResponse>**](ContinueUploadResponse.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## head_object

> crate::models::HeadObjectResponse head_object(head_object_request)
Head Object

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**head_object_request** | [**HeadObjectRequest**](HeadObjectRequest.md) |  | [required] |

### Return type

[**crate::models::HeadObjectResponse**](HeadObjectResponse.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## healthz

> crate::models::HealthcheckResponse healthz()
Healthz

### Parameters

This endpoint does not need any parameter.

### Return type

[**crate::models::HealthcheckResponse**](HealthcheckResponse.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## list_multipart_uploads

> Vec<crate::models::MultipartResponse> list_multipart_uploads(list_object_request)
List Multipart Uploads

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**list_object_request** | [**ListObjectRequest**](ListObjectRequest.md) |  | [required] |

### Return type

[**Vec<crate::models::MultipartResponse>**](MultipartResponse.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## list_objects

> Vec<crate::models::ObjectResponse> list_objects(list_object_request)
List Objects

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**list_object_request** | [**ListObjectRequest**](ListObjectRequest.md) |  | [required] |

### Return type

[**Vec<crate::models::ObjectResponse>**](ObjectResponse.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## list_parts

> Vec<crate::models::LogicalPartResponse> list_parts(list_parts_request)
List Parts

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**list_parts_request** | [**ListPartsRequest**](ListPartsRequest.md) |  | [required] |

### Return type

[**Vec<crate::models::LogicalPartResponse>**](LogicalPartResponse.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## locate_object

> crate::models::LocateObjectResponse locate_object(locate_object_request)
Locate Object

Given the logical object information, return one or zero physical object locators.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**locate_object_request** | [**LocateObjectRequest**](LocateObjectRequest.md) |  | [required] |

### Return type

[**crate::models::LocateObjectResponse**](LocateObjectResponse.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## set_multipart_id

> serde_json::Value set_multipart_id(patch_upload_multipart_upload_id)
Set Multipart Id

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**patch_upload_multipart_upload_id** | [**PatchUploadMultipartUploadId**](PatchUploadMultipartUploadId.md) |  | [required] |

### Return type

[**serde_json::Value**](serde_json::Value.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## start_upload

> crate::models::StartUploadResponse start_upload(start_upload_request)
Start Upload

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**start_upload_request** | [**StartUploadRequest**](StartUploadRequest.md) |  | [required] |

### Return type

[**crate::models::StartUploadResponse**](StartUploadResponse.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


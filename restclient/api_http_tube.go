/*
Function Stream Service

Manage Function Stream Resources

API version: 1.0.0
*/

// Code generated by OpenAPI Generator (https://openapi-generator.tech); DO NOT EDIT.

package restclient

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// HttpTubeAPIService HttpTubeAPI service
type HttpTubeAPIService service

type ApiTriggerHttpTubeEndpointRequest struct {
	ctx        context.Context
	ApiService *HttpTubeAPIService
	endpoint   string
	body       *string
}

func (r ApiTriggerHttpTubeEndpointRequest) Body(body string) ApiTriggerHttpTubeEndpointRequest {
	r.body = &body
	return r
}

func (r ApiTriggerHttpTubeEndpointRequest) Execute() (*http.Response, error) {
	return r.ApiService.TriggerHttpTubeEndpointExecute(r)
}

/*
TriggerHttpTubeEndpoint trigger the http tube endpoint

	@param ctx context.Context - for authentication, logging, cancellation, deadlines, tracing, etc. Passed from http.Request or context.Background().
	@param endpoint Endpoint
	@return ApiTriggerHttpTubeEndpointRequest
*/
func (a *HttpTubeAPIService) TriggerHttpTubeEndpoint(ctx context.Context, endpoint string) ApiTriggerHttpTubeEndpointRequest {
	return ApiTriggerHttpTubeEndpointRequest{
		ApiService: a,
		ctx:        ctx,
		endpoint:   endpoint,
	}
}

// Execute executes the request
func (a *HttpTubeAPIService) TriggerHttpTubeEndpointExecute(r ApiTriggerHttpTubeEndpointRequest) (*http.Response, error) {
	var (
		localVarHTTPMethod = http.MethodPost
		localVarPostBody   interface{}
		formFiles          []formFile
	)

	localBasePath, err := a.client.cfg.ServerURLWithContext(r.ctx, "HttpTubeAPIService.TriggerHttpTubeEndpoint")
	if err != nil {
		return nil, &GenericOpenAPIError{error: err.Error()}
	}

	localVarPath := localBasePath + "/api/v1/http-tube/{endpoint}"
	localVarPath = strings.Replace(localVarPath, "{"+"endpoint"+"}", url.PathEscape(parameterValueToString(r.endpoint, "endpoint")), -1)

	localVarHeaderParams := make(map[string]string)
	localVarQueryParams := url.Values{}
	localVarFormParams := url.Values{}
	if r.body == nil {
		return nil, reportError("body is required and must be specified")
	}

	// to determine the Content-Type header
	localVarHTTPContentTypes := []string{"application/json"}

	// set Content-Type header
	localVarHTTPContentType := selectHeaderContentType(localVarHTTPContentTypes)
	if localVarHTTPContentType != "" {
		localVarHeaderParams["Content-Type"] = localVarHTTPContentType
	}

	// to determine the Accept header
	localVarHTTPHeaderAccepts := []string{}

	// set Accept header
	localVarHTTPHeaderAccept := selectHeaderAccept(localVarHTTPHeaderAccepts)
	if localVarHTTPHeaderAccept != "" {
		localVarHeaderParams["Accept"] = localVarHTTPHeaderAccept
	}
	// body params
	localVarPostBody = r.body
	req, err := a.client.prepareRequest(r.ctx, localVarPath, localVarHTTPMethod, localVarPostBody, localVarHeaderParams, localVarQueryParams, localVarFormParams, formFiles)
	if err != nil {
		return nil, err
	}

	localVarHTTPResponse, err := a.client.callAPI(req)
	if err != nil || localVarHTTPResponse == nil {
		return localVarHTTPResponse, err
	}

	localVarBody, err := io.ReadAll(localVarHTTPResponse.Body)
	localVarHTTPResponse.Body.Close()
	localVarHTTPResponse.Body = io.NopCloser(bytes.NewBuffer(localVarBody))
	if err != nil {
		return localVarHTTPResponse, err
	}

	if localVarHTTPResponse.StatusCode >= 300 {
		newErr := &GenericOpenAPIError{
			body:  localVarBody,
			error: localVarHTTPResponse.Status,
		}
		return localVarHTTPResponse, newErr
	}

	return localVarHTTPResponse, nil
}
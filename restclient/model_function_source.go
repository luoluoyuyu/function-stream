/*
Function Stream API

No description provided (generated by Openapi Generator https://github.com/openapitools/openapi-generator)

API version: 0.1.0
*/

// Code generated by OpenAPI Generator (https://openapi-generator.tech); DO NOT EDIT.

package restclient

import (
	"encoding/json"
)

// checks if the FunctionSource type satisfies the MappedNullable interface at compile time
var _ MappedNullable = &FunctionSource{}

// FunctionSource struct for FunctionSource
type FunctionSource struct {
	Config map[string]interface{} `json:"config,omitempty"`
	Type   NullableString         `json:"type,omitempty"`
}

// NewFunctionSource instantiates a new FunctionSource object
// This constructor will assign default values to properties that have it defined,
// and makes sure properties required by API are set, but the set of arguments
// will change when the set of required properties is changed
func NewFunctionSource() *FunctionSource {
	this := FunctionSource{}
	return &this
}

// NewFunctionSourceWithDefaults instantiates a new FunctionSource object
// This constructor will only assign default values to properties that have it defined,
// but it doesn't guarantee that properties required by API are set
func NewFunctionSourceWithDefaults() *FunctionSource {
	this := FunctionSource{}
	return &this
}

// GetConfig returns the Config field value if set, zero value otherwise.
func (o *FunctionSource) GetConfig() map[string]interface{} {
	if o == nil || IsNil(o.Config) {
		var ret map[string]interface{}
		return ret
	}
	return o.Config
}

// GetConfigOk returns a tuple with the Config field value if set, nil otherwise
// and a boolean to check if the value has been set.
func (o *FunctionSource) GetConfigOk() (map[string]interface{}, bool) {
	if o == nil || IsNil(o.Config) {
		return map[string]interface{}{}, false
	}
	return o.Config, true
}

// HasConfig returns a boolean if a field has been set.
func (o *FunctionSource) HasConfig() bool {
	if o != nil && !IsNil(o.Config) {
		return true
	}

	return false
}

// SetConfig gets a reference to the given map[string]interface{} and assigns it to the Config field.
func (o *FunctionSource) SetConfig(v map[string]interface{}) {
	o.Config = v
}

// GetType returns the Type field value if set, zero value otherwise (both if not set or set to explicit null).
func (o *FunctionSource) GetType() string {
	if o == nil || IsNil(o.Type.Get()) {
		var ret string
		return ret
	}
	return *o.Type.Get()
}

// GetTypeOk returns a tuple with the Type field value if set, nil otherwise
// and a boolean to check if the value has been set.
// NOTE: If the value is an explicit nil, `nil, true` will be returned
func (o *FunctionSource) GetTypeOk() (*string, bool) {
	if o == nil {
		return nil, false
	}
	return o.Type.Get(), o.Type.IsSet()
}

// HasType returns a boolean if a field has been set.
func (o *FunctionSource) HasType() bool {
	if o != nil && o.Type.IsSet() {
		return true
	}

	return false
}

// SetType gets a reference to the given NullableString and assigns it to the Type field.
func (o *FunctionSource) SetType(v string) {
	o.Type.Set(&v)
}

// SetTypeNil sets the value for Type to be an explicit nil
func (o *FunctionSource) SetTypeNil() {
	o.Type.Set(nil)
}

// UnsetType ensures that no value is present for Type, not even an explicit nil
func (o *FunctionSource) UnsetType() {
	o.Type.Unset()
}

func (o FunctionSource) MarshalJSON() ([]byte, error) {
	toSerialize, err := o.ToMap()
	if err != nil {
		return []byte{}, err
	}
	return json.Marshal(toSerialize)
}

func (o FunctionSource) ToMap() (map[string]interface{}, error) {
	toSerialize := map[string]interface{}{}
	if !IsNil(o.Config) {
		toSerialize["config"] = o.Config
	}
	if o.Type.IsSet() {
		toSerialize["type"] = o.Type.Get()
	}
	return toSerialize, nil
}

type NullableFunctionSource struct {
	value *FunctionSource
	isSet bool
}

func (v NullableFunctionSource) Get() *FunctionSource {
	return v.value
}

func (v *NullableFunctionSource) Set(val *FunctionSource) {
	v.value = val
	v.isSet = true
}

func (v NullableFunctionSource) IsSet() bool {
	return v.isSet
}

func (v *NullableFunctionSource) Unset() {
	v.value = nil
	v.isSet = false
}

func NewNullableFunctionSource(val *FunctionSource) *NullableFunctionSource {
	return &NullableFunctionSource{value: val, isSet: true}
}

func (v NullableFunctionSource) MarshalJSON() ([]byte, error) {
	return json.Marshal(v.value)
}

func (v *NullableFunctionSource) UnmarshalJSON(src []byte) error {
	v.isSet = true
	return json.Unmarshal(src, &v.value)
}
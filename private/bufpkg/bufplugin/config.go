// Copyright 2020-2022 Buf Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package bufplugin defines a plugin.
//
// TODO: This package is largely unfinished - this is primarily meant to
// propose the new OptionConfig structure and how it would be mapped to
// the pluginv1alpha1.Parameter type.
//
// TODO: All of these exported types would normally go in bufplugin.go, but
// they're placed here separately so that it can be more easily reviewed and
// understood independently.
package bufplugin

const (
	// OptionTypeBool is the bool option type.
	OptionTypeBool OptionType = 1
	// OptionTypeString is the string option type.
	OptionTypeString OptionType = 2
	// OptionTypeInt32 is the int32 option type.
	OptionTypeInt32 OptionType = 3
	// OptionTypeFloat32 is the float32 option type.
	OptionTypeFloat32 OptionType = 4
)

// OptionType is an option type. The set of possible option types
// is based on the pluginv1alpha1.Parameter message.
type OptionType int

// ExternalConfig represents the external plugin configuration.
//
// TODO: For now this just specifies the structured
// 'opt' value so that it can be demonstrated in
// the Image -> GenerateRequest conversion.
type ExternalConfig struct {
	Version string                 `json:"version,omitempty" yaml:"version,omitempty"`
	Opt     []ExternalOptionConfig `json:"opt,omitempty" yaml:"opt,omitempty"`
}

// ExternalOptionConfig represents the external plugin option
// config.
type ExternalOptionConfig struct {
	Name    string `json:"name,omitempty" yaml:"name,omitempty"`
	Type    string `json:"type,omitempty" yaml:"type,omitempty"`
	Default string `json:"default,omitempty" yaml:"default,omitempty"`
}

// Config is the internal plugin config.
type Config struct {
	OptionConfig []*OptionConfig
}

// OptionConfig is the internal plugin option config.
type OptionConfig struct {
	Name string
	Type OptionType
	// TODO: We might actually want to use an interface value here
	// and convert the string representation to its typed equivalent.
	//
	//  type OptionValue interface {
	//    internal()
	//  }
	//
	//  // OptionValueInt32 implements OptionValue as a wrapper around
	//  // a int32 option value.
	//  type OptionValueInt32 struct {
	//    Value int32
	//  }
	//
	//  func (o *OptionValueInt32) internal() {}
	//
	// With this, the caller could perform a simple type switch to retrieve
	// the concrete value.
	//
	//  var number int32
	//  switch value := optionValue.(type) {
	//  case *OptionValueInt32:
	//    number = value.Value
	//  }
	//
	Default string
}

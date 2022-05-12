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

package bufimageutil

import (
	"context"
	"errors"
	"fmt"

	"github.com/bufbuild/buf/private/bufpkg/bufimage"
	pluginv1alpha1 "github.com/bufbuild/buf/private/gen/proto/go/buf/alpha/plugin/v1alpha1"
	"github.com/bufbuild/buf/private/pkg/protosource"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

var (
	// ErrImageFilterTypeNotFound is returned from ImageFilteredByTypes when
	// a specified type cannot be found in an image.
	ErrImageFilterTypeNotFound = errors.New("not found")

	// ErrImageFilterTypeIsImport is returned from ImageFilteredByTypes when
	// a specified type name is declared in a module dependency.
	ErrImageFilterTypeIsImport = errors.New("type declared in imported module")
)

// NewInputFiles converts the ImageFiles to InputFiles.
//
// Since protosource is a pkg package, it cannot depend on bufmoduleref, which has the
// definition for bufmoduleref.ModuleIdentity, so we have our own interfaces for this
// in protosource. Given Go's type system, we need to do a conversion here.
func NewInputFiles(imageFiles []bufimage.ImageFile) []protosource.InputFile {
	inputFiles := make([]protosource.InputFile, len(imageFiles))
	for i, imageFile := range imageFiles {
		inputFiles[i] = newInputFile(imageFile)
	}
	return inputFiles
}

// FreeMessageRangeStrings gets the free MessageRange strings for the target files.
//
// Recursive.
func FreeMessageRangeStrings(
	ctx context.Context,
	filePaths []string,
	image bufimage.Image,
) ([]string, error) {
	var s []string
	for _, filePath := range filePaths {
		imageFile := image.GetFile(filePath)
		if imageFile == nil {
			return nil, fmt.Errorf("unexpected nil image file: %q", filePath)
		}
		file, err := protosource.NewFile(newInputFile(imageFile))
		if err != nil {
			return nil, err
		}
		for _, message := range file.Messages() {
			s = freeMessageRangeStringsRec(s, message)
		}
	}
	return s, nil
}

// NewPluginFiles converts the Image to []*pluginv1alpha1.File.
func NewPluginFiles(image bufimage.Image) ([]*pluginv1alpha1.File, error) {
	registryFiles, err := protodesc.NewFiles(bufimage.ImageToFileDescriptorSet(image))
	if err != nil {
		return nil, err
	}
	// First pass to register all the types (including the extensions).
	var rangeError error
	registryTypes := new(protoregistry.Types)
	registryFiles.RangeFiles(
		func(file protoreflect.FileDescriptor) bool {
			if err := registerAllExtensions(registryTypes, file); err != nil {
				rangeError = err
				return false
			}
			return true
		},
	)
	if rangeError != nil {
		return nil, rangeError
	}
	unmarshalOptions := proto.UnmarshalOptions{
		Resolver: registryTypes,
	}
	// Second pass to create all of the plugin files.
	pluginFiles := make([]*pluginv1alpha1.File, 0, len(image.Files()))
	registryFiles.RangeFiles(
		func(fileDescriptor protoreflect.FileDescriptor) bool {
			pluginFile, err := pluginFileForFileDescriptor(
				fileDescriptor,
				unmarshalOptions,
			)
			if err != nil {
				rangeError = err
				return false
			}
			pluginFiles = append(pluginFiles, pluginFile)
			return true
		},
	)
	if rangeError != nil {
		return nil, rangeError
	}
	return pluginFiles, nil
}

func pluginFileForFileDescriptor(
	fileDescriptor protoreflect.FileDescriptor,
	unmarshaler proto.UnmarshalOptions,
) (*pluginv1alpha1.File, error) {
	syntax, err := pluginSyntaxForFileDescriptor(fileDescriptor)
	if err != nil {
		return nil, err
	}
	pluginOptions, err := pluginOptionsForDescriptor(
		fileDescriptor,
		fileDescriptor.Options(),
		unmarshaler,
	)
	if err != nil {
		return nil, err
	}
	// TODO: Add all of the other types (e.g. messages, enums, etc).
	pluginFile := &pluginv1alpha1.File{
		Path:    fileDescriptor.Path(),
		Syntax:  syntax,
		Package: pluginPackageForFileDescriptor(fileDescriptor),
		Options: pluginOptions,
	}
	return pluginFile, nil
}

func pluginSyntaxForFileDescriptor(
	fileDescriptor protoreflect.FileDescriptor,
) (*pluginv1alpha1.Syntax, error) {
	var value pluginv1alpha1.Syntax_Value
	switch syntax := fileDescriptor.Syntax(); syntax {
	case protoreflect.Proto2:
		value = pluginv1alpha1.Syntax_VALUE_PROTO2
	case protoreflect.Proto3:
		value = pluginv1alpha1.Syntax_VALUE_PROTO3
	default:
		return nil, fmt.Errorf("unrecognized syntax %v", syntax.String())
	}
	sourceLocation := fileDescriptor.SourceLocations().ByPath(
		protoreflect.SourcePath([]int32{12}),
	)
	return &pluginv1alpha1.Syntax{
		Value: value,
		SourceInfo: pluginSourceInfoForSourceLocation(
			fileDescriptor.Path(),
			sourceLocation,
		),
	}, nil
}

func pluginPackageForFileDescriptor(
	fileDescriptor protoreflect.FileDescriptor,
) *pluginv1alpha1.Package {
	sourceLocation := fileDescriptor.SourceLocations().ByPath(
		protoreflect.SourcePath([]int32{2}),
	)
	return &pluginv1alpha1.Package{
		Name: string(fileDescriptor.Package()),
		SourceInfo: pluginSourceInfoForSourceLocation(
			fileDescriptor.Path(),
			sourceLocation,
		),
	}
}

func pluginOptionsForDescriptor(
	fileDescriptor protoreflect.FileDescriptor,
	options protoreflect.ProtoMessage,
	unmarshaler proto.UnmarshalOptions,
) ([]*pluginv1alpha1.Option, error) {
	bytes, err := proto.Marshal(options)
	if err != nil {
		return nil, err
	}
	if resetter, ok := options.(interface{ Reset() }); ok {
		// If this option value has a Reset method, use it.
		resetter.Reset()
	}
	if err := unmarshaler.Unmarshal(bytes, options); err != nil {
		return nil, err
	}
	// TODO: The path prefix constants are defined in protosource, but
	// they're un-exported.
	//
	// Now that we need them in multiple places (Managed Mode also uses
	// them), we should consolidate these and import them from a single
	// location.
	var pathPrefix []int32
	switch options.(type) {
	case *descriptorpb.FileOptions:
		pathPrefix = []int32{8}
	case *descriptorpb.EnumOptions:
		pathPrefix = []int32{3}
	case *descriptorpb.EnumValueOptions:
		pathPrefix = []int32{3}
	case *descriptorpb.MessageOptions:
		pathPrefix = []int32{7}
	case *descriptorpb.FieldOptions:
		pathPrefix = []int32{8}
	case *descriptorpb.OneofOptions:
		pathPrefix = []int32{2}
	case *descriptorpb.ServiceOptions:
		pathPrefix = []int32{3}
	case *descriptorpb.MethodOptions:
		pathPrefix = []int32{4}
	}
	var rangeError error
	var pluginOptions []*pluginv1alpha1.Option
	options.ProtoReflect().Range(
		// TODO: This ranges over the fields in an undefined order.
		// We ought to preserve the order specified in the .proto
		// source file (like the FileDescriptorProto) to make this
		// deterministic. This is also relevant for determining where
		// the SourceLocation is defined.
		//
		// If we can't find a good way to range over the values in
		// a deterministic order, the least we could do is sort the
		// result so that our output is still deterministic.
		func(fieldDescriptor protoreflect.FieldDescriptor, v protoreflect.Value) bool {
			// Custom options might have multiple source locations associated
			// with them. For example,
			//
			//  syntax = "proto2";
			//
			//  // Leading comments on (custom).
			//  option (custom).foo = "abc"
			//
			//  // More leading comments on (custom).
			//  option (custom).bar = "def"
			//
			// TODO: How are source locations distinguishable across options that
			// have the same pathPrefix (e.g. FileOptions and FieldOptions
			// both have a [8] prefix). We need to take a closer look at SourceCodeInfo
			// here.
			sourceLocation := fileDescriptor.SourceLocations().ByPath(
				append(pathPrefix, int32(fieldDescriptor.Number())),
			)
			pluginOption := &pluginv1alpha1.Option{
				Name: string(fieldDescriptor.Name()),
				SourceInfo: pluginSourceInfoForSourceLocation(
					fileDescriptor.Path(),
					sourceLocation,
				),
			}
			switch typed := v.Interface().(type) {
			case nil:
				// The option value should be 'nil' here,
				// so there's nothing for us to do.
			case bool:
				pluginOption.Value = &pluginv1alpha1.Option_BoolValue{
					BoolValue: v.Bool(),
				}
			case int32:
				pluginOption.Value = &pluginv1alpha1.Option_Int32Value{
					Int32Value: int32(v.Int()),
				}
			case int64:
				pluginOption.Value = &pluginv1alpha1.Option_Int64Value{
					Int64Value: v.Int(),
				}
			case uint32:
				pluginOption.Value = &pluginv1alpha1.Option_Uint32Value{
					Uint32Value: uint32(v.Uint()),
				}
			case uint64:
				pluginOption.Value = &pluginv1alpha1.Option_Uint64Value{
					Uint64Value: v.Uint(),
				}
			case float32:
				pluginOption.Value = &pluginv1alpha1.Option_FloatValue{
					FloatValue: float32(v.Float()),
				}
			case float64:
				pluginOption.Value = &pluginv1alpha1.Option_DoubleValue{
					DoubleValue: v.Float(),
				}
			case string:
				pluginOption.Value = &pluginv1alpha1.Option_StringValue{
					StringValue: v.String(),
				}
			case []byte:
				pluginOption.Value = &pluginv1alpha1.Option_BytesValue{
					BytesValue: v.Bytes(),
				}
			case protoreflect.EnumNumber:
				// Enums are encoded as int32 values.
				//
				// https://developers.google.com/protocol-buffers/docs/proto3#enum
				pluginOption.Value = &pluginv1alpha1.Option_Int32Value{
					Int32Value: int32(v.Enum()),
				}
			case protoreflect.Message:
				// TODO: For now, we reuse the bytes_value field to encode
				// arbitrary messages, too.
				//
				// We might want to define a separate field name with a bytes
				// type to make this more clear, but it would be redundant.
				valueBytes, err := proto.Marshal(typed.(proto.Message))
				if err != nil {
					rangeError = err
					return false
				}
				pluginOption.Value = &pluginv1alpha1.Option_BytesValue{
					BytesValue: valueBytes,
				}
			default:
				rangeError = fmt.Errorf("unexpected option: found %T", typed)
				return false
			}
			pluginOptions = append(pluginOptions, pluginOption)
			return true
		},
	)
	if rangeError != nil {
		return nil, rangeError
	}
	return pluginOptions, nil
}

func pluginSourceInfoForSourceLocation(
	filePath string,
	sourceLocation protoreflect.SourceLocation,
) *pluginv1alpha1.SourceInfo {
	if len(sourceLocation.Path) == 0 {
		return nil
	}
	return &pluginv1alpha1.SourceInfo{
		FilePath:                filePath,
		StartLine:               int32(sourceLocation.StartLine),
		EndLine:                 int32(sourceLocation.EndLine),
		StartColumn:             int32(sourceLocation.StartColumn),
		EndColumn:               int32(sourceLocation.EndColumn),
		LeadingComments:         sourceLocation.LeadingComments,
		TrailingComments:        sourceLocation.TrailingComments,
		LeadingDetachedComments: sourceLocation.LeadingDetachedComments,
	}
}

func registerAllExtensions(
	registryTypes *protoregistry.Types,
	descriptor interface {
		Messages() protoreflect.MessageDescriptors
		Extensions() protoreflect.ExtensionDescriptors
	},
) error {
	messages := descriptor.Messages()
	for i := 0; i < messages.Len(); i++ {
		if err := registerAllExtensions(registryTypes, messages.Get(i)); err != nil {
			return err
		}
	}
	extensions := descriptor.Extensions()
	for i := 0; i < extensions.Len(); i++ {
		if err := registryTypes.RegisterExtension(dynamicpb.NewExtensionType(extensions.Get(i))); err != nil {
			return err
		}
	}
	return nil
}

// ImageFilteredByTypes returns a minimal image containing only the descriptors
// required to define those types. The resulting contains only files in which
// those descriptors and their transitive closure of required descriptors, with
// each file only contains the minimal required types and imports.
//
// Although this returns a new bufimage.Image, it mutates the original image's
// underlying file's `descriptorpb.FileDescriptorProto` and the old image should
// not continue to be used.
//
// A descriptor is said to require another descriptor if the dependent
// descriptor is needed to accurately and completely describe that descriptor.
// For the follwing types that includes:
//
//    Messages
//     - messages & enums referenced in fields
//     - proto2 extension declarations for this field
//     - custom options for the message, its fields, and the file in which the
//       message is defined
//     - the parent message if this message is a nested definition
//
//    Enums
//     - Custom options used in the enum, enum values, and the file
//       in which the message is defined
//     - the parent message if this message is a nested definition
//
//    Services
//     - request & response types referenced in methods
//     - custom options for the service, its methods, and the file
//       in which the message is defined
//
// As an example, consider the following proto structure:
//
//   --- foo.proto ---
//   package pkg;
//   message Foo {
//     optional Bar bar = 1;
//     extensions 2 to 3;
//   }
//   message Bar { ... }
//   message Baz {
//     other.Qux qux = 1 [(other.my_option).field = "buf"];
//   }
//   --- baz.proto ---
//   package other;
//   extend Foo {
//     optional Qux baz = 2;
//   }
//   message Qux{ ... }
//   message Quux{ ... }
//   extend google.protobuf.FieldOptions {
//     optional Quux my_option = 51234;
//   }
//
// A filtered image for type `pkg.Foo` would include
//   files:      [foo.proto, bar.proto]
//   messages:   [pkg.Foo, pkg.Bar, other.Qux]
//   extensions: [other.baz]
//
// A filtered image for type `pkg.Bar` would include
//   files:      [foo.proto]
//   messages:   [pkg.Bar]
//
//  A filtered image for type `pkg.Baz` would include
//   files:      [foo.proto, bar.proto]
//   messages:   [pkg.Baz, other.Quux, other.Qux]
//   extensions: [other.my_option]
//
func ImageFilteredByTypes(image bufimage.Image, types ...string) (bufimage.Image, error) {
	imageIndex, err := newImageIndexForImage(image)
	if err != nil {
		return nil, err
	}
	// Check types exist
	startingDescriptors := make([]protosource.NamedDescriptor, 0, len(types))
	for _, typeName := range types {
		descriptor, ok := imageIndex.NameToDescriptor[typeName]
		if !ok {
			return nil, fmt.Errorf("filtering by type %q: %w", typeName, ErrImageFilterTypeNotFound)
		}
		if image.GetFile(descriptor.File().Path()).IsImport() {
			return nil, fmt.Errorf("filtering by type %q: %w", typeName, ErrImageFilterTypeIsImport)
		}
		startingDescriptors = append(startingDescriptors, descriptor)
	}
	// Find all types to include in filtered image.
	seen := make(map[string]struct{})
	neededDescriptors := []descriptorAndDirects{}
	for _, startingDescriptor := range startingDescriptors {
		closure, err := descriptorTransitiveClosure(startingDescriptor, imageIndex, seen)
		if err != nil {
			return nil, err
		}
		neededDescriptors = append(neededDescriptors, closure...)
	}
	// By file is more convenient but we loose dfs order from
	// descriptorTransitiveClosure, if images need files ordered by
	// dependencies may need to rethink this.
	descriptorsByFile := make(map[string][]descriptorAndDirects)
	for _, descriptor := range neededDescriptors {
		descriptorsByFile[descriptor.Descriptor.File().Path()] = append(
			descriptorsByFile[descriptor.Descriptor.File().Path()],
			descriptor,
		)
	}
	// Create a new image with only the required descriptors.
	var includedFiles []bufimage.ImageFile
	for file, descriptors := range descriptorsByFile {
		importsRequired := make(map[string]struct{})
		typesToKeep := make(map[string]struct{})
		for _, descriptor := range descriptors {
			typesToKeep[descriptor.Descriptor.FullName()] = struct{}{}
			for _, importedDescdescriptor := range descriptor.Directs {
				if importedDescdescriptor.File() != descriptor.Descriptor.File() {
					importsRequired[importedDescdescriptor.File().Path()] = struct{}{}
				}
			}
		}

		imageFile := image.GetFile(file)
		includedFiles = append(includedFiles, imageFile)
		imageFileDescriptor := imageFile.Proto()
		// While employing
		// https://github.com/golang/go/wiki/SliceTricks#filter-in-place,
		// also keep a record of which index moved where so we can fixup
		// the file's PublicDependency/WeakDependency fields.
		indexFromTo := make(map[int32]int32)
		indexTo := 0
		for indexFrom, importPath := range imageFileDescriptor.GetDependency() {
			// TODO: this only filters the existing imports down to
			// the ones requested, if there was a type we picked up
			// through a public import in a dependent file may
			// filter out that file here, a type not to be found. We
			// may need to add the file directly (or have a file
			// with public import only inserted in the middle). See
			// TestTransitivePublicFail.
			if _, ok := importsRequired[importPath]; ok {
				indexFromTo[int32(indexFrom)] = int32(indexTo)
				imageFileDescriptor.Dependency[indexTo] = importPath
				indexTo++
			}
		}
		imageFileDescriptor.Dependency = imageFileDescriptor.Dependency[:indexTo]
		var i int
		for _, indexFrom := range imageFileDescriptor.PublicDependency {
			if indexTo, ok := indexFromTo[indexFrom]; ok {
				imageFileDescriptor.PublicDependency[i] = indexTo
				i++
			}
		}
		imageFileDescriptor.PublicDependency = imageFileDescriptor.PublicDependency[:i]
		i = 0
		for _, indexFrom := range imageFileDescriptor.WeakDependency {
			if indexTo, ok := indexFromTo[indexFrom]; ok {
				imageFileDescriptor.WeakDependency[i] = indexTo
				i++
			}
		}
		imageFileDescriptor.WeakDependency = imageFileDescriptor.WeakDependency[:i]

		prefix := ""
		if imageFileDescriptor.Package != nil {
			prefix = imageFileDescriptor.GetPackage() + "."
		}
		trimMessages, err := trimMessageDescriptor(imageFileDescriptor.MessageType, prefix, typesToKeep)
		if err != nil {
			return nil, err
		}
		imageFileDescriptor.MessageType = trimMessages
		trimEnums, err := trimEnumDescriptor(imageFileDescriptor.EnumType, prefix, typesToKeep)
		if err != nil {
			return nil, err
		}
		imageFileDescriptor.EnumType = trimEnums
		trimExtensions, err := trimExtensionDescriptors(imageFileDescriptor.Extension, prefix, typesToKeep)
		if err != nil {
			return nil, err
		}
		imageFileDescriptor.Extension = trimExtensions
		i = 0
		for _, serviceDescriptor := range imageFileDescriptor.Service {
			name := prefix + serviceDescriptor.GetName()
			if _, ok := typesToKeep[name]; ok {
				imageFileDescriptor.Service[i] = serviceDescriptor
				i++
			}
		}
		imageFileDescriptor.Service = imageFileDescriptor.Service[:i]

		// TODO: With some from/to mappings, perhaps even sourcecodeinfo
		// isn't too bad.
		imageFileDescriptor.SourceCodeInfo = nil
	}
	return bufimage.NewImage(includedFiles)
}

// trimMessageDescriptor removes (nested) messages and nested enums from a slice
// of message descriptors if their type names are not found in the toKeep map.
func trimMessageDescriptor(in []*descriptorpb.DescriptorProto, prefix string, toKeep map[string]struct{}) ([]*descriptorpb.DescriptorProto, error) {
	i := 0
	for _, messageDescriptor := range in {
		name := prefix + messageDescriptor.GetName()
		if _, ok := toKeep[name]; ok {
			trimMessages, err := trimMessageDescriptor(messageDescriptor.NestedType, name+".", toKeep)
			if err != nil {
				return nil, err
			}
			messageDescriptor.NestedType = trimMessages
			trimEnums, err := trimEnumDescriptor(messageDescriptor.EnumType, name+".", toKeep)
			if err != nil {
				return nil, err
			}
			messageDescriptor.EnumType = trimEnums
			trimExtensions, err := trimExtensionDescriptors(messageDescriptor.Extension, name+".", toKeep)
			if err != nil {
				return nil, err
			}
			messageDescriptor.Extension = trimExtensions
			in[i] = messageDescriptor
			i++
		}
	}
	return in[:i], nil
}

// trimEnumDescriptor removes enums from a slice of enum descriptors if their
// type names are not found in the toKeep map.
func trimEnumDescriptor(in []*descriptorpb.EnumDescriptorProto, prefix string, toKeep map[string]struct{}) ([]*descriptorpb.EnumDescriptorProto, error) {
	i := 0
	for _, enumDescriptor := range in {
		name := prefix + enumDescriptor.GetName()
		if _, ok := toKeep[name]; ok {
			in[i] = enumDescriptor
			i++
		}
	}
	return in[:i], nil
}

// trimExtensionDescriptors removes fields from a slice of field descriptors if their
// type names are not found in the toKeep map.
func trimExtensionDescriptors(in []*descriptorpb.FieldDescriptorProto, prefix string, toKeep map[string]struct{}) ([]*descriptorpb.FieldDescriptorProto, error) {
	i := 0
	for _, fieldDescriptor := range in {
		name := prefix + fieldDescriptor.GetName()
		if _, ok := toKeep[name]; ok {
			in[i] = fieldDescriptor
			i++
		}
	}
	return in[:i], nil
}

// descriptorAndDirects holds a protsource.NamedDescriptor and a list of all
// named descriptors it directly references. A directly referenced dependency is
// any type that if defined in a different file from the principal descriptor,
// an import statement would be required for the proto to compile.
type descriptorAndDirects struct {
	Descriptor protosource.NamedDescriptor
	Directs    []protosource.NamedDescriptor
}

func descriptorTransitiveClosure(namedDescriptor protosource.NamedDescriptor, imageIndex *imageIndex, seen map[string]struct{}) ([]descriptorAndDirects, error) {
	if _, ok := seen[namedDescriptor.FullName()]; ok {
		return nil, nil
	}
	seen[namedDescriptor.FullName()] = struct{}{}

	directDependencies := []protosource.NamedDescriptor{}
	transitiveDependencies := []descriptorAndDirects{}
	switch typedDesctriptor := namedDescriptor.(type) {
	case protosource.Message:
		for _, field := range typedDesctriptor.Fields() {
			switch field.Type() {
			case protosource.FieldDescriptorProtoTypeEnum,
				protosource.FieldDescriptorProtoTypeMessage,
				protosource.FieldDescriptorProtoTypeGroup:
				inputDescriptor, ok := imageIndex.NameToDescriptor[field.TypeName()]
				if !ok {
					return nil, fmt.Errorf("missing %q", field.TypeName())
				}
				directDependencies = append(directDependencies, inputDescriptor)
				recursiveDescriptors, err := descriptorTransitiveClosure(inputDescriptor, imageIndex, seen)
				if err != nil {
					return nil, err
				}
				transitiveDependencies = append(transitiveDependencies, recursiveDescriptors...)
			case protosource.FieldDescriptorProtoTypeDouble,
				protosource.FieldDescriptorProtoTypeFloat,
				protosource.FieldDescriptorProtoTypeInt64,
				protosource.FieldDescriptorProtoTypeUint64,
				protosource.FieldDescriptorProtoTypeInt32,
				protosource.FieldDescriptorProtoTypeFixed64,
				protosource.FieldDescriptorProtoTypeFixed32,
				protosource.FieldDescriptorProtoTypeBool,
				protosource.FieldDescriptorProtoTypeString,
				protosource.FieldDescriptorProtoTypeBytes,
				protosource.FieldDescriptorProtoTypeUint32,
				protosource.FieldDescriptorProtoTypeSfixed32,
				protosource.FieldDescriptorProtoTypeSfixed64,
				protosource.FieldDescriptorProtoTypeSint32,
				protosource.FieldDescriptorProtoTypeSint64:
				// nothing to explore for the field type, but
				// there might be custom field options
			default:
				return nil, fmt.Errorf("unknown field type %d", field.Type())
			}
			// fieldoptions
			explicitOptionDeps, recursedOptionDeps, err := exploreCustomOptions(field, imageIndex, seen)
			if err != nil {
				return nil, err
			}
			directDependencies = append(directDependencies, explicitOptionDeps...)
			transitiveDependencies = append(transitiveDependencies, recursedOptionDeps...)
		}
		// Extensions declared for this message
		for _, extendsDescriptor := range imageIndex.NameToExtensions[namedDescriptor.FullName()] {
			directDependencies = append(directDependencies, extendsDescriptor)
			recursiveDescriptors, err := descriptorTransitiveClosure(extendsDescriptor, imageIndex, seen)
			if err != nil {
				return nil, err
			}
			transitiveDependencies = append(transitiveDependencies, recursiveDescriptors...)
		}
		// Messages in which this message is nested
		if typedDesctriptor.Parent() != nil {
			directDependencies = append(directDependencies, typedDesctriptor.Parent())
			recursiveDescriptors, err := descriptorTransitiveClosure(typedDesctriptor.Parent(), imageIndex, seen)
			if err != nil {
				return nil, err
			}
			transitiveDependencies = append(transitiveDependencies, recursiveDescriptors...)
		}
		// Options for all oneofs in this message
		for _, oneOfDescriptor := range typedDesctriptor.Oneofs() {
			explicitOptionDeps, recursedOptionDeps, err := exploreCustomOptions(oneOfDescriptor, imageIndex, seen)
			if err != nil {
				return nil, err
			}
			directDependencies = append(directDependencies, explicitOptionDeps...)
			transitiveDependencies = append(transitiveDependencies, recursedOptionDeps...)
		}
		// Options
		explicitOptionDeps, recursedOptionDeps, err := exploreCustomOptions(typedDesctriptor, imageIndex, seen)
		if err != nil {
			return nil, err
		}
		directDependencies = append(directDependencies, explicitOptionDeps...)
		transitiveDependencies = append(transitiveDependencies, recursedOptionDeps...)
	case protosource.Enum:
		// Parent messages
		if typedDesctriptor.Parent() != nil {
			directDependencies = append(directDependencies, typedDesctriptor.Parent())
			recursiveDescriptors, err := descriptorTransitiveClosure(typedDesctriptor.Parent(), imageIndex, seen)
			if err != nil {
				return nil, err
			}
			transitiveDependencies = append(transitiveDependencies, recursiveDescriptors...)
		}
		for _, enumValue := range typedDesctriptor.Values() {
			explicitOptionDeps, recursedOptionDeps, err := exploreCustomOptions(enumValue, imageIndex, seen)
			if err != nil {
				return nil, err
			}
			directDependencies = append(directDependencies, explicitOptionDeps...)
			transitiveDependencies = append(transitiveDependencies, recursedOptionDeps...)
		}
		// Options
		explicitOptionDeps, recursedOptionDeps, err := exploreCustomOptions(typedDesctriptor, imageIndex, seen)
		if err != nil {
			return nil, err
		}
		directDependencies = append(directDependencies, explicitOptionDeps...)
		transitiveDependencies = append(transitiveDependencies, recursedOptionDeps...)
	case protosource.Service:
		for _, method := range typedDesctriptor.Methods() {
			inputDescriptor, ok := imageIndex.NameToDescriptor[method.InputTypeName()]
			if !ok {
				return nil, fmt.Errorf("missing %q", method.InputTypeName())
			}
			recursiveDescriptorsIn, err := descriptorTransitiveClosure(inputDescriptor, imageIndex, seen)
			if err != nil {
				return nil, err
			}
			transitiveDependencies = append(transitiveDependencies, recursiveDescriptorsIn...)
			directDependencies = append(directDependencies, inputDescriptor)

			outputDescriptor, ok := imageIndex.NameToDescriptor[method.OutputTypeName()]
			if !ok {
				return nil, fmt.Errorf("missing %q", method.OutputTypeName())
			}
			recursiveDescriptorsOut, err := descriptorTransitiveClosure(outputDescriptor, imageIndex, seen)
			if err != nil {
				return nil, err
			}
			transitiveDependencies = append(transitiveDependencies, recursiveDescriptorsOut...)
			directDependencies = append(directDependencies, outputDescriptor)

			// options
			explicitOptionDeps, recursedOptionDeps, err := exploreCustomOptions(method, imageIndex, seen)
			if err != nil {
				return nil, err
			}
			directDependencies = append(directDependencies, explicitOptionDeps...)
			transitiveDependencies = append(transitiveDependencies, recursedOptionDeps...)
		}
		// Options
		explicitOptionDeps, recursedOptionDeps, err := exploreCustomOptions(typedDesctriptor, imageIndex, seen)
		if err != nil {
			return nil, err
		}
		directDependencies = append(directDependencies, explicitOptionDeps...)
		transitiveDependencies = append(transitiveDependencies, recursedOptionDeps...)
	case protosource.Field:
		// Regular fields get handled by protosource.Message, only
		// protosource.Fields's for extends definitions should reach
		// here.
		if typedDesctriptor.Extendee() == "" {
			return nil, fmt.Errorf("expected extendee for field %q to not be empty", typedDesctriptor.FullName())
		}
		extendeeDescriptor, ok := imageIndex.NameToDescriptor[typedDesctriptor.Extendee()]
		if !ok {
			return nil, fmt.Errorf("missing %q", typedDesctriptor.Extendee())
		}
		directDependencies = append(directDependencies, extendeeDescriptor)
		recursiveDescriptors, err := descriptorTransitiveClosure(extendeeDescriptor, imageIndex, seen)
		if err != nil {
			return nil, err
		}
		transitiveDependencies = append(transitiveDependencies, recursiveDescriptors...)

		switch typedDesctriptor.Type() {
		case protosource.FieldDescriptorProtoTypeEnum,
			protosource.FieldDescriptorProtoTypeMessage,
			protosource.FieldDescriptorProtoTypeGroup:
			inputDescriptor, ok := imageIndex.NameToDescriptor[typedDesctriptor.TypeName()]
			if !ok {
				return nil, fmt.Errorf("missing %q", typedDesctriptor.TypeName())
			}
			directDependencies = append(directDependencies, inputDescriptor)
			recursiveDescriptors, err := descriptorTransitiveClosure(inputDescriptor, imageIndex, seen)
			if err != nil {
				return nil, err
			}
			transitiveDependencies = append(transitiveDependencies, recursiveDescriptors...)
		case protosource.FieldDescriptorProtoTypeDouble,
			protosource.FieldDescriptorProtoTypeFloat,
			protosource.FieldDescriptorProtoTypeInt64,
			protosource.FieldDescriptorProtoTypeUint64,
			protosource.FieldDescriptorProtoTypeInt32,
			protosource.FieldDescriptorProtoTypeFixed64,
			protosource.FieldDescriptorProtoTypeFixed32,
			protosource.FieldDescriptorProtoTypeBool,
			protosource.FieldDescriptorProtoTypeString,
			protosource.FieldDescriptorProtoTypeBytes,
			protosource.FieldDescriptorProtoTypeUint32,
			protosource.FieldDescriptorProtoTypeSfixed32,
			protosource.FieldDescriptorProtoTypeSfixed64,
			protosource.FieldDescriptorProtoTypeSint32,
			protosource.FieldDescriptorProtoTypeSint64:
			// nothing to follow, custom options handled below.
		default:
			return nil, fmt.Errorf("unknown field type %d", typedDesctriptor.Type())
		}
		explicitOptionDeps, recursedOptionDeps, err := exploreCustomOptions(typedDesctriptor, imageIndex, seen)
		if err != nil {
			return nil, err
		}
		directDependencies = append(directDependencies, explicitOptionDeps...)
		transitiveDependencies = append(transitiveDependencies, recursedOptionDeps...)
	default:
		return nil, fmt.Errorf("unexpected protosource type %T", typedDesctriptor)
	}

	explicitOptionDeps, recursedOptionDeps, err := exploreCustomOptions(namedDescriptor.File(), imageIndex, seen)
	if err != nil {
		return nil, err
	}
	directDependencies = append(directDependencies, explicitOptionDeps...)
	transitiveDependencies = append(transitiveDependencies, recursedOptionDeps...)

	return append(
		transitiveDependencies,
		descriptorAndDirects{Descriptor: namedDescriptor, Directs: directDependencies},
	), nil
}

func exploreCustomOptions(descriptor protosource.OptionExtensionDescriptor, imageIndex *imageIndex, seen map[string]struct{}) ([]protosource.NamedDescriptor, []descriptorAndDirects, error) {
	directDependencies := []protosource.NamedDescriptor{}
	transitiveDependencies := []descriptorAndDirects{}

	var optionName string
	switch descriptor.(type) {
	case protosource.File:
		optionName = "google.protobuf.FileOptions"
	case protosource.Message:
		optionName = "google.protobuf.MessageOptions"
	case protosource.Field:
		optionName = "google.protobuf.FieldOptions"
	case protosource.Oneof:
		optionName = "google.protobuf.OneofOptions"
	case protosource.Enum:
		optionName = "google.protobuf.EnumOptions"
	case protosource.EnumValue:
		optionName = "google.protobuf.EnumValueOptions"
	case protosource.Service:
		optionName = "google.protobuf.ServiceOptions"
	case protosource.Method:
		optionName = "google.protobuf.MethodOptions"
	default:
		return nil, nil, fmt.Errorf("unexpected type for exploring options %T", descriptor)
	}

	for _, n := range descriptor.PresentExtensionNumbers() {
		opts := imageIndex.NameToOptions[optionName]
		field, ok := opts[n]
		if !ok {
			return nil, nil, fmt.Errorf("cannot find ext no %d on %s", n, optionName)
		}
		directDependencies = append(directDependencies, field)
		recursiveDescriptors, err := descriptorTransitiveClosure(field, imageIndex, seen)
		if err != nil {
			return nil, nil, err
		}
		transitiveDependencies = append(transitiveDependencies, recursiveDescriptors...)
	}
	return directDependencies, transitiveDependencies, nil
}

func freeMessageRangeStringsRec(
	s []string,
	message protosource.Message,
) []string {
	for _, nestedMessage := range message.Messages() {
		s = freeMessageRangeStringsRec(s, nestedMessage)
	}
	if e := protosource.FreeMessageRangeString(message); e != "" {
		return append(s, e)
	}
	return s
}

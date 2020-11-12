// Copyright 2020 Buf Technologies, Inc.
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

package bufcorevalidate

import (
	"github.com/bufbuild/buf/internal/pkg/protodescriptor"
)

// ValidateFileInfoPath validates the FileInfo path.
func ValidateFileInfoPath(path string) error {
	return protodescriptor.ValidateProtoPath("root relative file path", path)
}

// ValidateFileInfoPaths validates the FileInfo paths.
func ValidateFileInfoPaths(paths []string) error {
	return protodescriptor.ValidateProtoPaths("root relative file path", paths)
}
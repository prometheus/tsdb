// Copyright 2019 The qiffang Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package fuse

import "errors"

// Server is empty struct because windows platform do not support fuse.
type Server struct {
}

// NewServer is a mock method because fuse does not support Windows platform.
func NewServer(original, mountpoint string, hook Hook) (*Server, error) {
	return nil, errors.New("Unsupported fuse.")
}

// Close is a mock method
func (s *Server) Close() (err error) {

}

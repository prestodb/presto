/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <stdint.h>
#include <cstddef>
#include <string>

namespace Azure::Storage::Files::DataLake::Models {
class PathProperties;
}

namespace facebook::velox::filesystems {

// Azurite Simulator does not yet support the DFS endpoint.
// (For more information, see https://github.com/Azure/Azurite/issues/553 and
// https://github.com/Azure/Azurite/issues/409).
// You can find a comparison between DFS and Blob endpoints here:
// https://github.com/Azure/Azurite/wiki/ADLS-Gen2-Implementation-Guidance
// To facilitate unit testing of file write scenarios, we define the
// AzureDatalakeFileClient which can be mocked during testing.

class AzureDataLakeFileClient {
 public:
  virtual ~AzureDataLakeFileClient() {}

  virtual void create() = 0;
  virtual Azure::Storage::Files::DataLake::Models::PathProperties
  getProperties() = 0;
  virtual void append(const uint8_t* buffer, size_t size, uint64_t offset) = 0;
  virtual void flush(uint64_t position) = 0;
  virtual void close() = 0;
  virtual std::string getUrl() = 0;
};
} // namespace facebook::velox::filesystems

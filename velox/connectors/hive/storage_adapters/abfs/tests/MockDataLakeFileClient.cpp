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

#include "velox/connectors/hive/storage_adapters/abfs/tests/MockDataLakeFileClient.h"

#include <filesystem>

#include <azure/storage/files/datalake.hpp>

namespace facebook::velox::filesystems {

void MockDataLakeFileClient::create() {
  fileStream_ = std::ofstream(
      filePath_,
      std::ios_base::out | std::ios_base::binary | std::ios_base::app);
}

PathProperties MockDataLakeFileClient::getProperties() {
  if (!std::filesystem::exists(filePath_)) {
    Azure::Storage::StorageException exp(filePath_ + "doesn't exists");
    exp.StatusCode = Azure::Core::Http::HttpStatusCode::NotFound;
    throw exp;
  }
  std::ifstream file(filePath_, std::ios::binary | std::ios::ate);
  uint64_t size = static_cast<uint64_t>(file.tellg());
  PathProperties ret;
  ret.FileSize = size;
  return ret;
}

void MockDataLakeFileClient::append(
    const uint8_t* buffer,
    size_t size,
    uint64_t offset) {
  fileStream_.seekp(offset);
  fileStream_.write(reinterpret_cast<const char*>(buffer), size);
}

void MockDataLakeFileClient::flush(uint64_t position) {
  fileStream_.flush();
}

void MockDataLakeFileClient::close() {
  fileStream_.flush();
  fileStream_.close();
}
} // namespace facebook::velox::filesystems

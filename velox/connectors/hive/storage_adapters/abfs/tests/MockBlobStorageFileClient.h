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

#include "velox/connectors/hive/storage_adapters/abfs/AbfsWriteFile.h"

#include "velox/exec/tests/utils/TempFilePath.h"

using namespace facebook::velox;
using namespace facebook::velox::filesystems::abfs;

namespace facebook::velox::filesystems::test {
// A mocked blob storage file client backend with local file store.
class MockBlobStorageFileClient : public IBlobStorageFileClient {
 public:
  MockBlobStorageFileClient() {
    auto tempFile = ::exec::test::TempFilePath::create();
    filePath_ = tempFile->path;
  }

  void create() override;
  PathProperties getProperties() override;
  void append(const uint8_t* buffer, size_t size, uint64_t offset) override;
  void flush(uint64_t position) override;
  void close() override;

  // for testing purpose to verify the written content if correct.
  std::string readContent() {
    std::ifstream inputFile(filePath_);
    std::string content;
    inputFile.seekg(0, std::ios::end);
    std::streamsize fileSize = inputFile.tellg();
    inputFile.seekg(0, std::ios::beg);
    content.resize(fileSize);
    inputFile.read(&content[0], fileSize);
    inputFile.close();
    return content;
  }

 private:
  std::string filePath_;
  std::ofstream fileStream_;
};
} // namespace facebook::velox::filesystems::test

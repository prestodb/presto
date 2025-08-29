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

#include "velox/exec/tests/utils/TempFilePath.h"

#include "velox/connectors/hive/storage_adapters/abfs/AzureDataLakeFileClient.h"

using namespace Azure::Storage::Files::DataLake::Models;

namespace facebook::velox::filesystems {

// A mock AzureDataLakeFileClient backend with local file store.
class MockDataLakeFileClient : public AzureDataLakeFileClient {
 public:
  MockDataLakeFileClient() {
    auto tempFile = velox::exec::test::TempFilePath::create();
    filePath_ = tempFile->getPath();
  }

  MockDataLakeFileClient(std::string_view filePath) : filePath_(filePath) {}

  std::string_view path() const {
    return filePath_;
  }

  void create() override;

  PathProperties getProperties() override;

  void append(const uint8_t* buffer, size_t size, uint64_t offset) override;

  void flush(uint64_t position) override;

  void close() override;

  std::string getUrl() override {
    return "testUrl";
  }

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
} // namespace facebook::velox::filesystems

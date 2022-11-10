/*
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

#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/Exchange.h"
#include "velox/exec/Operator.h"
#include "velox/serializers/PrestoSerializer.h"

namespace facebook::presto::operators {

class StorageBasedExchangeSource : public velox::exec::ExchangeSource {
 public:
  StorageBasedExchangeSource(
      const std::string& taskId,
      int destination,
      std::shared_ptr<velox::exec::ExchangeQueue> queue,
      const std::vector<std::string>& inputFolders,
      velox::memory::MemoryPool* pool)
      : ExchangeSource(taskId, destination, queue, pool) {
    if (inputFolders.size() > 0) {
      fileSystem_ = velox::filesystems::getFileSystem(inputFolders[0], {});
      for (auto& folder : inputFolders) {
        auto files = fileSystem_->list(folder);
        filenames_.insert(filenames_.end(), files.begin(), files.end());
      }
    }
  }

  bool shouldRequestLocked() override {
    return !atEnd_;
  }

  void request() override;

  void close() override {}

 private:
  /// List of broadcast filenames.
  std::vector<std::string> filenames_;

  std::unique_ptr<velox::ByteStream> bytes_;

  velox::serializer::presto::PrestoVectorSerde serde_;

  /// The index of the next file to be read.
  int currentFileIndex_ = 0;

  uint64_t currentFileOffset_ = 0;

  std::shared_ptr<velox::filesystems::FileSystem> fileSystem_ = nullptr;
};
} // namespace facebook::presto::operators
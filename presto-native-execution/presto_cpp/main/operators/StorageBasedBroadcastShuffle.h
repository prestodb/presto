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

#include "presto_cpp/main/operators/ShuffleInterface.h"
#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/Operator.h"

namespace facebook::presto::operators {

/// ShuffleInfo structs.
struct StorageBasedBroadcastShuffleInfo : public ShuffleInfo {
 public:
  StorageBasedBroadcastShuffleInfo(
      const std::string& name,
      std::optional<int> folderIndex,
      std::vector<std::string> broadcastFolders)
      : ShuffleInfo(name),
        folderIndex_(std::move(folderIndex)),
        broadcastFolders_(broadcastFolders) {}

  virtual ~StorageBasedBroadcastShuffleInfo() = default;

  const std::vector<std::string>& broadcastFolders() const {
    return broadcastFolders_;
  }

  const std::optional<int>& folderIndex() const {
    return folderIndex_;
  }

 private:
  const std::vector<std::string> broadcastFolders_;
  const std::optional<int> folderIndex_;
};

class StorageBasedBroadcastShuffle : public ShuffleInterface {
 public:
  static const std::string kBroadcastFileExtension; // .bin
  static const std::string kBroadcastTempFileExtension; // .dot

  StorageBasedBroadcastShuffle(
      const std::optional<std::vector<std::string>>& broadcastFolders,
      velox::memory::MemoryPool* pool,
      std::optional<int> folderIndex)
      : pool_(pool), writerIndex_(folderIndex), broadcastFolders_(std::move(broadcastFolders)) {
    if (broadcastFolders_.has_value() && broadcastFolders_.value().size() > 0) {
      fileSystem_ =
          velox::filesystems::getFileSystem(broadcastFolders_.value()[0], {});
      for (auto& folder : broadcastFolders_.value()) {
        auto files = fileSystem_->list(folder);
        for (auto& file : files) {
          // Make sure the file extension is correct.
          VELOX_CHECK(
              file.find(kBroadcastFileExtension) != std::string::npos &&
                  file.find(kBroadcastTempFileExtension) == std::string::npos,
              fmt::format("Invalid binary broadcast filename {}.", file));
        }
        inputFilenames_.insert(
            inputFilenames_.end(), files.begin(), files.end());
      }
    }
  }
  void collect(int32_t partition, std::string_view data) override {
    VELOX_UNSUPPORTED("Collect not supported for broadcast.");
  }
  void writeColumnar(
      std::string_view data,
      std::optional<std::string> writerId = std::nullopt,
      std::optional<int32_t> partition = std::nullopt) override;
  void noMoreData(bool success) override;
  bool hasNext(int32_t partition) const override;
  velox::BufferPtr next(int32_t partition, bool success) override;
  bool readyForRead() const override {
    return true;
  }

 private:
  velox::memory::MemoryPool* pool_;
  std::unique_ptr<velox::WriteFile> outputFile_ = nullptr;
  std::optional<std::string> outputFilename_ = std::nullopt;
  std::vector<std::string> inputFilenames_;
  std::shared_ptr<velox::filesystems::FileSystem> fileSystem_ = nullptr;
  std::unique_ptr<velox::ReadFile> currentReadFile_ = nullptr;
  /// The index of the next file to be read.
  const std::optional<int>& writerIndex_;
  const std::optional<std::vector<std::string>>& broadcastFolders_;
  int currentFileIndex_ = 0;
  uint64_t currentFileOffset_ = 0;
};

class StorageBasedBroadcastShuffleManager : public ShuffleManager {
 public:
  std::unique_ptr<ShuffleInterface> create(
      const ShuffleInfo& info,
      velox::memory::MemoryPool* pool) override {
    auto& shuffleInfo = dynamic_cast<const StorageBasedBroadcastShuffleInfo&>(info);
    auto shuffle = std::make_unique<StorageBasedBroadcastShuffle>(
        shuffleInfo.broadcastFolders(),
        pool,
        shuffleInfo.folderIndex());
    return shuffle;
  }
  ~StorageBasedBroadcastShuffleManager() = default;
};

} // namespace facebook::presto::operators
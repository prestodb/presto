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

#include "velox/connectors/hive/storage_adapters/abfs/AbfsFileSystem.h"

#include <fmt/format.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <glog/logging.h>

#include "velox/connectors/hive/storage_adapters/abfs/AbfsConfig.h"
#include "velox/connectors/hive/storage_adapters/abfs/AbfsReadFile.h"
#include "velox/connectors/hive/storage_adapters/abfs/AbfsUtil.h"
#include "velox/connectors/hive/storage_adapters/abfs/AbfsWriteFile.h"

namespace facebook::velox::filesystems {

class AbfsReadFile::Impl {
  constexpr static uint64_t kNaturalReadSize = 4 << 20; // 4M
  constexpr static uint64_t kReadConcurrency = 8;

 public:
  explicit Impl(std::string_view path, const config::ConfigBase& config) {
    auto abfsConfig = AbfsConfig(path, config);
    filePath_ = abfsConfig.filePath();
    fileClient_ = abfsConfig.getReadFileClient();
  }

  void initialize(const FileOptions& options) {
    if (options.fileSize.has_value()) {
      VELOX_CHECK_GE(
          options.fileSize.value(), 0, "File size must be non-negative");
      length_ = options.fileSize.value();
    }

    if (length_ != -1) {
      return;
    }

    try {
      auto properties = fileClient_->GetProperties();
      length_ = properties.Value.BlobSize;
    } catch (Azure::Storage::StorageException& e) {
      throwStorageExceptionWithOperationDetails("GetProperties", filePath_, e);
    }
    VELOX_CHECK_GE(length_, 0);
  }

  std::string_view pread(
      uint64_t offset,
      uint64_t length,
      void* buffer,
      File::IoStats* stats) const {
    preadInternal(offset, length, static_cast<char*>(buffer));
    return {static_cast<char*>(buffer), length};
  }

  std::string pread(uint64_t offset, uint64_t length, File::IoStats* stats)
      const {
    std::string result(length, 0);
    preadInternal(offset, length, result.data());
    return result;
  }

  uint64_t preadv(
      uint64_t offset,
      const std::vector<folly::Range<char*>>& buffers,
      File::IoStats* stats) const {
    size_t length = 0;
    auto size = buffers.size();
    for (auto& range : buffers) {
      length += range.size();
    }
    std::string result(length, 0);
    preadInternal(offset, length, static_cast<char*>(result.data()));
    size_t resultOffset = 0;
    for (auto range : buffers) {
      if (range.data()) {
        memcpy(range.data(), &(result.data()[resultOffset]), range.size());
      }
      resultOffset += range.size();
    }

    return length;
  }

  uint64_t preadv(
      folly::Range<const common::Region*> regions,
      folly::Range<folly::IOBuf*> iobufs,
      File::IoStats* stats) const {
    size_t length = 0;
    VELOX_CHECK_EQ(regions.size(), iobufs.size());
    for (size_t i = 0; i < regions.size(); ++i) {
      const auto& region = regions[i];
      auto& output = iobufs[i];
      output = folly::IOBuf(folly::IOBuf::CREATE, region.length);
      pread(region.offset, region.length, output.writableData(), stats);
      output.append(region.length);
      length += region.length;
    }

    return length;
  }

  uint64_t size() const {
    return length_;
  }

  uint64_t memoryUsage() const {
    return 3 * sizeof(std::string) + sizeof(int64_t);
  }

  bool shouldCoalesce() const {
    return false;
  }

  std::string getName() const {
    return filePath_;
  }

  uint64_t getNaturalReadSize() const {
    return kNaturalReadSize;
  }

 private:
  void preadInternal(uint64_t offset, uint64_t length, char* position) const {
    // Read the desired range of bytes.
    Azure::Core::Http::HttpRange range;
    range.Offset = offset;
    range.Length = length;

    Azure::Storage::Blobs::DownloadBlobOptions blob;
    blob.Range = range;
    auto response = fileClient_->Download(blob);
    response.Value.BodyStream->ReadToCount(
        reinterpret_cast<uint8_t*>(position), length);
  }

  std::string filePath_;
  std::unique_ptr<BlobClient> fileClient_;
  int64_t length_ = -1;
};

AbfsReadFile::AbfsReadFile(
    std::string_view path,
    const config::ConfigBase& config) {
  impl_ = std::make_shared<Impl>(path, config);
}

void AbfsReadFile::initialize(const FileOptions& options) {
  return impl_->initialize(options);
}

std::string_view AbfsReadFile::pread(
    uint64_t offset,
    uint64_t length,
    void* buffer,
    File::IoStats* stats) const {
  return impl_->pread(offset, length, buffer, stats);
}

std::string AbfsReadFile::pread(
    uint64_t offset,
    uint64_t length,
    File::IoStats* stats) const {
  return impl_->pread(offset, length, stats);
}

uint64_t AbfsReadFile::preadv(
    uint64_t offset,
    const std::vector<folly::Range<char*>>& buffers,
    File::IoStats* stats) const {
  return impl_->preadv(offset, buffers, stats);
}

uint64_t AbfsReadFile::preadv(
    folly::Range<const common::Region*> regions,
    folly::Range<folly::IOBuf*> iobufs,
    File::IoStats* stats) const {
  return impl_->preadv(regions, iobufs, stats);
}

uint64_t AbfsReadFile::size() const {
  return impl_->size();
}

uint64_t AbfsReadFile::memoryUsage() const {
  return impl_->memoryUsage();
}

bool AbfsReadFile::shouldCoalesce() const {
  return false;
}

std::string AbfsReadFile::getName() const {
  return impl_->getName();
}

uint64_t AbfsReadFile::getNaturalReadSize() const {
  return impl_->getNaturalReadSize();
}

AbfsFileSystem::AbfsFileSystem(std::shared_ptr<const config::ConfigBase> config)
    : FileSystem(config) {
  VELOX_CHECK_NOT_NULL(config.get());
}

std::string AbfsFileSystem::name() const {
  return "ABFS";
}

std::unique_ptr<ReadFile> AbfsFileSystem::openFileForRead(
    std::string_view path,
    const FileOptions& options) {
  auto abfsfile = std::make_unique<AbfsReadFile>(path, *config_);
  abfsfile->initialize(options);
  return abfsfile;
}

std::unique_ptr<WriteFile> AbfsFileSystem::openFileForWrite(
    std::string_view path,
    const FileOptions& /*unused*/) {
  return std::make_unique<AbfsWriteFile>(path, *config_);
}
} // namespace facebook::velox::filesystems

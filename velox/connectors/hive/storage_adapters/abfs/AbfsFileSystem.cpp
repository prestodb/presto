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

#include <azure/storage/blobs/blob_client.hpp>
#include <fmt/format.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <glog/logging.h>

#include "velox/common/file/File.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/connectors/hive/storage_adapters/abfs/AbfsReadFile.h"
#include "velox/connectors/hive/storage_adapters/abfs/AbfsWriteFile.h"
#include "velox/core/Config.h"

namespace facebook::velox::filesystems::abfs {
using namespace Azure::Storage::Blobs;

class AbfsConfig {
 public:
  AbfsConfig(const Config* config) : config_(config) {}

  std::string connectionString(const std::string& path) const {
    auto abfsAccount = AbfsAccount(path);
    auto key = abfsAccount.credKey();
    VELOX_USER_CHECK(
        config_->isValueExists(key), "Failed to find storage credentials");

    return abfsAccount.connectionString(config_->get(key).value());
  }

 private:
  const Config* config_;
};

class AbfsReadFile::Impl {
  constexpr static uint64_t kNaturalReadSize = 4 << 20; // 4M
  constexpr static uint64_t kReadConcurrency = 8;

 public:
  explicit Impl(const std::string& path, const std::string& connectStr) {
    auto abfsAccount = AbfsAccount(path);
    fileName_ = abfsAccount.filePath();
    fileClient_ =
        std::make_unique<BlobClient>(BlobClient::CreateFromConnectionString(
            connectStr, abfsAccount.fileSystem(), fileName_));
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
      throwStorageExceptionWithOperationDetails("GetProperties", fileName_, e);
    }

    VELOX_CHECK_GE(length_, 0);
  }

  std::string_view pread(uint64_t offset, uint64_t length, void* buffer) const {
    preadInternal(offset, length, static_cast<char*>(buffer));
    return {static_cast<char*>(buffer), length};
  }

  std::string pread(uint64_t offset, uint64_t length) const {
    std::string result(length, 0);
    preadInternal(offset, length, result.data());
    return result;
  }

  uint64_t preadv(
      uint64_t offset,
      const std::vector<folly::Range<char*>>& buffers) const {
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

  void preadv(
      folly::Range<const common::Region*> regions,
      folly::Range<folly::IOBuf*> iobufs) const {
    VELOX_CHECK_EQ(regions.size(), iobufs.size());
    for (size_t i = 0; i < regions.size(); ++i) {
      const auto& region = regions[i];
      auto& output = iobufs[i];
      output = folly::IOBuf(folly::IOBuf::CREATE, region.length);
      pread(region.offset, region.length, output.writableData());
      output.append(region.length);
    }
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
    return fileName_;
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

  std::string fileName_;
  std::unique_ptr<BlobClient> fileClient_;

  int64_t length_ = -1;
};

AbfsReadFile::AbfsReadFile(
    const std::string& path,
    const std::string& connectStr) {
  impl_ = std::make_shared<Impl>(path, connectStr);
}

void AbfsReadFile::initialize(const FileOptions& options) {
  return impl_->initialize(options);
}

std::string_view
AbfsReadFile::pread(uint64_t offset, uint64_t length, void* buffer) const {
  return impl_->pread(offset, length, buffer);
}

std::string AbfsReadFile::pread(uint64_t offset, uint64_t length) const {
  return impl_->pread(offset, length);
}

uint64_t AbfsReadFile::preadv(
    uint64_t offset,
    const std::vector<folly::Range<char*>>& buffers) const {
  return impl_->preadv(offset, buffers);
}

void AbfsReadFile::preadv(
    folly::Range<const common::Region*> regions,
    folly::Range<folly::IOBuf*> iobufs) const {
  return impl_->preadv(regions, iobufs);
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

class AbfsFileSystem::Impl {
 public:
  explicit Impl(const Config* config) : abfsConfig_(config) {
    LOG(INFO) << "Init Azure Blob file system";
  }

  ~Impl() {
    LOG(INFO) << "Dispose Azure Blob file system";
  }

  const std::string connectionString(const std::string& path) const {
    // Extract account name
    return abfsConfig_.connectionString(path);
  }

 private:
  const AbfsConfig abfsConfig_;
  std::shared_ptr<folly::Executor> ioExecutor_;
};

AbfsFileSystem::AbfsFileSystem(const std::shared_ptr<const Config>& config)
    : FileSystem(config) {
  impl_ = std::make_shared<Impl>(config.get());
}

std::string AbfsFileSystem::name() const {
  return "ABFS";
}

std::unique_ptr<ReadFile> AbfsFileSystem::openFileForRead(
    std::string_view path,
    const FileOptions& options) {
  auto abfsfile = std::make_unique<AbfsReadFile>(
      std::string(path), impl_->connectionString(std::string(path)));
  abfsfile->initialize(options);
  return abfsfile;
}

std::unique_ptr<WriteFile> AbfsFileSystem::openFileForWrite(
    std::string_view path,
    const FileOptions& /*unused*/) {
  auto abfsfile = std::make_unique<AbfsWriteFile>(
      std::string(path), impl_->connectionString(std::string(path)));
  abfsfile->initialize();
  return abfsfile;
}
} // namespace facebook::velox::filesystems::abfs

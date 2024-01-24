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

#include "velox/connectors/hive/storage_adapters/gcs/GCSFileSystem.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/file/File.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/connectors/hive/storage_adapters/gcs/GCSUtil.h"
#include "velox/core/Config.h"

#include <fmt/format.h>
#include <glog/logging.h>
#include <memory>
#include <stdexcept>

#include <google/cloud/storage/client.h>

namespace facebook::velox {
namespace {
namespace gcs = ::google::cloud::storage;
namespace gc = ::google::cloud;
// Reference: https://github.com/apache/arrow/issues/29916
// Change the default upload buffer size. In general, sending larger buffers is
// more efficient with GCS, as each buffer requires a roundtrip to the service.
// With formatted output (when using `operator<<`), keeping a larger buffer in
// memory before uploading makes sense.  With unformatted output (the only
// choice given gcs::io::OutputStream's API) it is better to let the caller
// provide as large a buffer as they want. The GCS C++ client library will
// upload this buffer with zero copies if possible.
auto constexpr kUploadBufferSize = 256 * 1024;

inline void checkGCSStatus(
    const gc::Status outcome,
    const std::string_view& errorMsgPrefix,
    const std::string& bucket,
    const std::string& key) {
  if (!outcome.ok()) {
    auto error = outcome.error_info();
    VELOX_FAIL(
        "{} due to: Path:'{}', SDK Error Type:{}, GCS Status Code:{},  Message:'{}'",
        errorMsgPrefix,
        gcsURI(bucket, key),
        error.domain(),
        getErrorStringFromGCSError(outcome.code()),
        outcome.message());
  }
}

class GCSReadFile final : public ReadFile {
 public:
  GCSReadFile(const std::string& path, std::shared_ptr<gcs::Client> client)
      : client_(std::move(client)) {
    // assumption it's a proper path
    setBucketAndKeyFromGCSPath(path, bucket_, key_);
  }

  // Gets the length of the file.
  // Checks if there are any issues reading the file.
  void initialize() {
    // Make it a no-op if invoked twice.
    if (length_ != -1) {
      return;
    }
    // get metadata and initialize length
    auto metadata = client_->GetObjectMetadata(bucket_, key_);
    if (!metadata.ok()) {
      checkGCSStatus(
          metadata.status(),
          "Failed to get metadata for GCS object",
          bucket_,
          key_);
    }
    length_ = (*metadata).size();
    VELOX_CHECK_GE(length_, 0);
  }

  std::string_view pread(uint64_t offset, uint64_t length, void* buffer)
      const override {
    preadInternal(offset, length, static_cast<char*>(buffer));
    return {static_cast<char*>(buffer), length};
  }

  std::string pread(uint64_t offset, uint64_t length) const override {
    std::string result(length, 0);
    char* position = result.data();
    preadInternal(offset, length, position);
    return result;
  }

  uint64_t preadv(
      uint64_t offset,
      const std::vector<folly::Range<char*>>& buffers) const override {
    // 'buffers' contains Ranges(data, size)  with some gaps (data = nullptr) in
    // between. This call must populate the ranges (except gap ranges)
    // sequentially starting from 'offset'. If a range pointer is nullptr, the
    // data from stream of size range.size() will be skipped.
    size_t length = 0;
    for (const auto range : buffers) {
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

  uint64_t size() const override {
    return length_;
  }

  uint64_t memoryUsage() const override {
    return sizeof(GCSReadFile) // this class
        + sizeof(gcs::Client) // pointee
        + kUploadBufferSize; // buffer size
  }

  bool shouldCoalesce() const final {
    return false;
  }

  std::string getName() const override {
    return key_;
  }

  uint64_t getNaturalReadSize() const override {
    return kUploadBufferSize;
  }

 private:
  // The assumption here is that "position" has space for at least "length"
  // bytes.
  void preadInternal(uint64_t offset, uint64_t length, char* position) const {
    gcs::ObjectReadStream stream = client_->ReadObject(
        bucket_, key_, gcs::ReadRange(offset, offset + length));
    if (!stream) {
      checkGCSStatus(
          stream.status(), "Failed to get GCS object", bucket_, key_);
    }

    stream.read(position, length);
    if (!stream) {
      checkGCSStatus(
          stream.status(), "Failed to get read object", bucket_, key_);
    }
    bytesRead_ += length;
  }

  std::shared_ptr<gcs::Client> client_;
  std::string bucket_;
  std::string key_;
  std::atomic<int64_t> length_ = -1;
};

class GCSWriteFile final : public WriteFile {
 public:
  explicit GCSWriteFile(
      const std::string& path,
      std::shared_ptr<gcs::Client> client)
      : client_(client) {
    setBucketAndKeyFromGCSPath(path, bucket_, key_);
  }

  ~GCSWriteFile() {
    close();
  }

  void initialize() {
    // Make it a no-op if invoked twice.
    if (size_ != -1) {
      return;
    }

    // Check that it doesn't exist, if it does throw an error
    auto object_metadata = client_->GetObjectMetadata(bucket_, key_);
    VELOX_CHECK(!object_metadata.ok(), "File already exists");

    auto stream = client_->WriteObject(bucket_, key_);
    checkGCSStatus(
        stream.last_status(),
        "Failed to open GCS object for writing",
        bucket_,
        key_);
    stream_ = std::move(stream);
    size_ = 0;
  }

  void append(const std::string_view data) override {
    VELOX_CHECK(isFileOpen(), "File is not open");
    stream_ << data;
    size_ += data.size();
  }

  void flush() override {
    if (isFileOpen()) {
      stream_.flush();
    }
  }

  void close() override {
    if (isFileOpen()) {
      stream_.flush();
      stream_.Close();
      closed_ = true;
    }
  }

  uint64_t size() const override {
    return size_;
  }

 private:
  inline bool isFileOpen() {
    return (!closed_ && stream_.IsOpen());
  }

  gcs::ObjectWriteStream stream_;
  std::shared_ptr<gcs::Client> client_;
  std::string bucket_;
  std::string key_;
  std::atomic<int64_t> size_{-1};
  std::atomic<bool> closed_{false};
};
} // namespace

namespace filesystems {
using namespace connector::hive;

auto constexpr kGCSInvalidPath = "File {} is not a valid gcs file";

class GCSFileSystem::Impl {
 public:
  Impl(const Config* config)
      : hiveConfig_(std::make_shared<HiveConfig>(
            std::make_shared<core::MemConfig>(config->values()))) {}

  ~Impl() = default;

  // Use the input Config parameters and initialize the GCSClient.
  void initializeClient() {
    auto options = gc::Options{};
    auto scheme = hiveConfig_->gcsScheme();
    if (scheme == "https") {
      options.set<gc::UnifiedCredentialsOption>(
          gc::MakeGoogleDefaultCredentials());
    } else {
      options.set<gc::UnifiedCredentialsOption>(gc::MakeInsecureCredentials());
    }
    options.set<gcs::UploadBufferSizeOption>(kUploadBufferSize);

    auto endpointOverride = hiveConfig_->gcsEndpoint();
    if (!endpointOverride.empty()) {
      options.set<gcs::RestEndpointOption>(scheme + "://" + endpointOverride);
    }

    auto cred = hiveConfig_->gcsCredentials();
    if (!cred.empty()) {
      auto credentials = gc::MakeServiceAccountCredentials(cred);
      options.set<gc::UnifiedCredentialsOption>(credentials);
    } else {
      LOG(WARNING) << "Config::gcsCredentials is empty";
    }

    client_ = std::make_shared<gcs::Client>(options);
  }

  std::shared_ptr<gcs::Client> getClient() const {
    return client_;
  }

 private:
  const std::shared_ptr<HiveConfig> hiveConfig_;
  std::shared_ptr<gcs::Client> client_;
};

GCSFileSystem::GCSFileSystem(std::shared_ptr<const Config> config)
    : FileSystem(config) {
  impl_ = std::make_shared<Impl>(config.get());
}

void GCSFileSystem::initializeClient() {
  impl_->initializeClient();
}

std::unique_ptr<ReadFile> GCSFileSystem::openFileForRead(
    std::string_view path,
    const FileOptions& /*unused*/) {
  const auto gcspath = gcsPath(path);
  auto gcsfile = std::make_unique<GCSReadFile>(gcspath, impl_->getClient());
  gcsfile->initialize();
  return gcsfile;
}

std::unique_ptr<WriteFile> GCSFileSystem::openFileForWrite(
    std::string_view path,
    const FileOptions& /*unused*/) {
  const auto gcspath = gcsPath(path);
  auto gcsfile = std::make_unique<GCSWriteFile>(gcspath, impl_->getClient());
  gcsfile->initialize();
  return gcsfile;
}

void GCSFileSystem::remove(std::string_view path) {
  if (!isGCSFile(path)) {
    VELOX_FAIL(kGCSInvalidPath, path);
  }

  // We assume 'path' is well-formed here.
  std::string bucket;
  std::string object;
  const auto file = gcsPath(path);
  setBucketAndKeyFromGCSPath(file, bucket, object);

  if (!object.empty()) {
    auto stat = impl_->getClient()->GetObjectMetadata(bucket, object);
    if (!stat.ok()) {
      checkGCSStatus(
          stat.status(),
          "Failed to get metadata for GCS object",
          bucket,
          object);
    }
  }
  auto ret = impl_->getClient()->DeleteObject(bucket, object);
  if (!ret.ok()) {
    checkGCSStatus(
        ret, "Failed to get metadata for GCS object", bucket, object);
  }
}

bool GCSFileSystem::exists(std::string_view path) {
  std::vector<std::string> result;
  if (!isGCSFile(path))
    VELOX_FAIL(kGCSInvalidPath, path);

  // We assume 'path' is well-formed here.
  const auto file = gcsPath(path);
  std::string bucket;
  std::string object;
  setBucketAndKeyFromGCSPath(file, bucket, object);
  using ::google::cloud::StatusOr;
  StatusOr<gcs::BucketMetadata> metadata =
      impl_->getClient()->GetBucketMetadata(bucket);

  return metadata.ok();
}

std::vector<std::string> GCSFileSystem::list(std::string_view path) {
  std::vector<std::string> result;
  if (!isGCSFile(path))
    VELOX_FAIL(kGCSInvalidPath, path);

  // We assume 'path' is well-formed here.
  const auto file = gcsPath(path);
  std::string bucket;
  std::string object;
  setBucketAndKeyFromGCSPath(file, bucket, object);
  for (auto&& metadata : impl_->getClient()->ListObjects(bucket)) {
    if (!metadata.ok()) {
      checkGCSStatus(
          metadata.status(),
          "Failed to get metadata for GCS object",
          bucket,
          object);
    }
    result.push_back(metadata->name());
  }

  return result;
}

std::string GCSFileSystem::name() const {
  return "GCS";
}

void GCSFileSystem::rename(std::string_view, std::string_view, bool) {
  VELOX_UNSUPPORTED("rename for GCS not implemented");
}

void GCSFileSystem::mkdir(std::string_view path) {
  VELOX_UNSUPPORTED("mkdir for GCS not implemented");
}

void GCSFileSystem::rmdir(std::string_view path) {
  VELOX_UNSUPPORTED("rmdir for GCS not implemented");
}

}; // namespace filesystems
}; // namespace facebook::velox

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

#include "velox/connectors/hive/storage_adapters/gcs/GcsFileSystem.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/config/Config.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/connectors/hive/storage_adapters/gcs/GcsReadFile.h"
#include "velox/connectors/hive/storage_adapters/gcs/GcsUtil.h"
#include "velox/connectors/hive/storage_adapters/gcs/GcsWriteFile.h"
#include "velox/core/QueryConfig.h"

#include <fmt/format.h>
#include <glog/logging.h>
#include <memory>
#include <stdexcept>

#include <google/cloud/storage/client.h>

namespace facebook::velox {
namespace filesystems {
using namespace connector::hive;
namespace gcs = ::google::cloud::storage;
namespace gc = ::google::cloud;

auto constexpr kGcsInvalidPath = "File {} is not a valid gcs file";

class GcsFileSystem::Impl {
 public:
  Impl(const config::ConfigBase* config)
      : hiveConfig_(std::make_shared<HiveConfig>(
            std::make_shared<config::ConfigBase>(config->rawConfigsCopy()))) {}

  ~Impl() = default;

  // Use the input Config parameters and initialize the GcsClient.
  void initializeClient() {
    constexpr std::string_view kHttpsScheme{"https://"};
    auto options = gc::Options{};
    auto endpointOverride = hiveConfig_->gcsEndpoint();
    // Use secure credentials by default.
    if (!endpointOverride.empty()) {
      options.set<gcs::RestEndpointOption>(endpointOverride);
      // Use Google default credentials if endpoint has https scheme.
      if (endpointOverride.find(kHttpsScheme) == 0) {
        options.set<gc::UnifiedCredentialsOption>(
            gc::MakeGoogleDefaultCredentials());
      } else {
        options.set<gc::UnifiedCredentialsOption>(
            gc::MakeInsecureCredentials());
      }
    } else {
      options.set<gc::UnifiedCredentialsOption>(
          gc::MakeGoogleDefaultCredentials());
    }
    options.set<gcs::UploadBufferSizeOption>(kUploadBufferSize);

    auto max_retry_count = hiveConfig_->gcsMaxRetryCount();
    if (max_retry_count) {
      options.set<gcs::RetryPolicyOption>(
          gcs::LimitedErrorCountRetryPolicy(max_retry_count.value()).clone());
    }

    auto max_retry_time = hiveConfig_->gcsMaxRetryTime();
    if (max_retry_time) {
      auto retry_time = std::chrono::duration_cast<std::chrono::milliseconds>(
          facebook::velox::config::toDuration(max_retry_time.value()));
      options.set<gcs::RetryPolicyOption>(
          gcs::LimitedTimeRetryPolicy(retry_time).clone());
    }

    auto credFile = hiveConfig_->gcsCredentialsPath();
    if (!credFile.empty() && std::filesystem::exists(credFile)) {
      std::ifstream jsonFile(credFile, std::ios::in);
      if (!jsonFile.is_open()) {
        LOG(WARNING) << "Error opening file " << credFile;
      } else {
        std::stringstream credsBuffer;
        credsBuffer << jsonFile.rdbuf();
        auto creds = credsBuffer.str();
        auto credentials = gc::MakeServiceAccountCredentials(std::move(creds));
        options.set<gc::UnifiedCredentialsOption>(credentials);
      }
    } else {
      LOG(WARNING)
          << "Config hive.gcs.json-key-file-path is empty or key file path not found";
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

GcsFileSystem::GcsFileSystem(std::shared_ptr<const config::ConfigBase> config)
    : FileSystem(config) {
  impl_ = std::make_shared<Impl>(config.get());
}

void GcsFileSystem::initializeClient() {
  impl_->initializeClient();
}

std::unique_ptr<ReadFile> GcsFileSystem::openFileForRead(
    std::string_view path,
    const FileOptions& options) {
  const auto gcspath = gcsPath(path);
  auto gcsfile = std::make_unique<GcsReadFile>(gcspath, impl_->getClient());
  gcsfile->initialize(options);
  return gcsfile;
}

std::unique_ptr<WriteFile> GcsFileSystem::openFileForWrite(
    std::string_view path,
    const FileOptions& /*unused*/) {
  const auto gcspath = gcsPath(path);
  auto gcsfile = std::make_unique<GcsWriteFile>(gcspath, impl_->getClient());
  gcsfile->initialize();
  return gcsfile;
}

void GcsFileSystem::remove(std::string_view path) {
  if (!isGcsFile(path)) {
    VELOX_FAIL(kGcsInvalidPath, path);
  }

  // We assume 'path' is well-formed here.
  std::string bucket;
  std::string object;
  const auto file = gcsPath(path);
  setBucketAndKeyFromGcsPath(file, bucket, object);

  if (!object.empty()) {
    auto stat = impl_->getClient()->GetObjectMetadata(bucket, object);
    if (!stat.ok()) {
      checkGcsStatus(
          stat.status(),
          "Failed to get metadata for GCS object",
          bucket,
          object);
    }
  }
  auto ret = impl_->getClient()->DeleteObject(bucket, object);
  if (!ret.ok()) {
    checkGcsStatus(
        ret, "Failed to get metadata for GCS object", bucket, object);
  }
}

bool GcsFileSystem::exists(std::string_view path) {
  std::vector<std::string> result;
  if (!isGcsFile(path))
    VELOX_FAIL(kGcsInvalidPath, path);

  // We assume 'path' is well-formed here.
  const auto file = gcsPath(path);
  std::string bucket;
  std::string object;
  setBucketAndKeyFromGcsPath(file, bucket, object);
  using ::google::cloud::StatusOr;
  StatusOr<gcs::BucketMetadata> metadata =
      impl_->getClient()->GetBucketMetadata(bucket);

  return metadata.ok();
}

std::vector<std::string> GcsFileSystem::list(std::string_view path) {
  std::vector<std::string> result;
  if (!isGcsFile(path))
    VELOX_FAIL(kGcsInvalidPath, path);

  // We assume 'path' is well-formed here.
  const auto file = gcsPath(path);
  std::string bucket;
  std::string object;
  setBucketAndKeyFromGcsPath(file, bucket, object);
  for (auto&& metadata : impl_->getClient()->ListObjects(bucket)) {
    if (!metadata.ok()) {
      checkGcsStatus(
          metadata.status(),
          "Failed to get metadata for GCS object",
          bucket,
          object);
    }
    result.push_back(metadata->name());
  }

  return result;
}

std::string GcsFileSystem::name() const {
  return "GCS";
}

void GcsFileSystem::rename(
    std::string_view originPath,
    std::string_view newPath,
    bool overwrite) {
  if (!isGcsFile(originPath)) {
    VELOX_FAIL(kGcsInvalidPath, originPath);
  }

  if (!isGcsFile(newPath)) {
    VELOX_FAIL(kGcsInvalidPath, newPath);
  }

  std::string originBucket;
  std::string originObject;
  const auto originFile = gcsPath(originPath);
  setBucketAndKeyFromGcsPath(originFile, originBucket, originObject);

  std::string newBucket;
  std::string newObject;
  const auto newFile = gcsPath(newPath);
  setBucketAndKeyFromGcsPath(newFile, newBucket, newObject);

  if (!overwrite) {
    auto objects = list(newPath);
    if (std::find(objects.begin(), objects.end(), newObject) != objects.end()) {
      VELOX_USER_FAIL(
          "Failed to rename object {} to {} with as {} exists.",
          originObject,
          newObject,
          newObject);
      return;
    }
  }

  // Copy the object to the new name.
  auto copyStats = impl_->getClient()->CopyObject(
      originBucket, originObject, newBucket, newObject);
  if (!copyStats.ok()) {
    checkGcsStatus(
        copyStats.status(),
        fmt::format(
            "Failed to rename for GCS object {}/{}",
            originBucket,
            originObject),
        originBucket,
        originObject);
  }

  // Delete the original object.
  auto delStatus = impl_->getClient()->DeleteObject(originBucket, originObject);
  if (!delStatus.ok()) {
    checkGcsStatus(
        delStatus,
        fmt::format(
            "Failed to delete for GCS object {}/{} after copy when renaming. And the copied object is at {}/{}",
            originBucket,
            originObject,
            newBucket,
            newObject),
        originBucket,
        originObject);
  }
}

void GcsFileSystem::mkdir(
    std::string_view path,
    const DirectoryOptions& options) {
  if (!isGcsFile(path)) {
    VELOX_FAIL(kGcsInvalidPath, path);
  }

  std::string bucket;
  std::string object;
  const auto file = gcsPath(path);
  setBucketAndKeyFromGcsPath(file, bucket, object);

  // Create an empty object to represent the directory.
  auto status = impl_->getClient()->InsertObject(bucket, object, "");

  checkGcsStatus(
      status.status(),
      fmt::format("Failed to mkdir for GCS object {}/{}", bucket, object),
      bucket,
      object);
}

void GcsFileSystem::rmdir(std::string_view path) {
  if (!isGcsFile(path)) {
    VELOX_FAIL(kGcsInvalidPath, path);
  }

  const auto file = gcsPath(path);
  std::string bucket;
  std::string object;
  setBucketAndKeyFromGcsPath(file, bucket, object);
  for (auto&& metadata : impl_->getClient()->ListObjects(bucket)) {
    checkGcsStatus(
        metadata.status(),
        fmt::format("Failed to rmdir for GCS object {}/{}", bucket, object),
        bucket,
        object);

    auto status = impl_->getClient()->DeleteObject(bucket, metadata->name());
    checkGcsStatus(
        metadata.status(),
        fmt::format(
            "Failed to delete for GCS object {}/{} when rmdir.",
            bucket,
            metadata->name()),
        bucket,
        metadata->name());
  }
}

} // namespace filesystems
} // namespace facebook::velox

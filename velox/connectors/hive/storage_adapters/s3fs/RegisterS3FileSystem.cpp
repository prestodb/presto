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

#ifdef VELOX_ENABLE_S3
#include "velox/connectors/hive/HiveConfig.h" // @manual
#include "velox/connectors/hive/storage_adapters/s3fs/S3FileSystem.h" // @manual
#include "velox/connectors/hive/storage_adapters/s3fs/S3Util.h" // @manual
#include "velox/dwio/common/FileSink.h"
#endif

#include "velox/connectors/hive/storage_adapters/s3fs/RegisterS3FileSystem.h" // @manual

namespace facebook::velox::filesystems {

#ifdef VELOX_ENABLE_S3
using FileSystemMap = folly::Synchronized<
    std::unordered_map<std::string, std::shared_ptr<FileSystem>>>;

/// Multiple S3 filesystems are supported.
/// Key is the endpoint value specified in the config using hive.s3.endpoint.
/// If the endpoint is empty, it will default to AWS S3.
FileSystemMap& fileSystems() {
  static FileSystemMap instances;
  return instances;
}

std::string getS3Identity(const std::shared_ptr<config::ConfigBase>& config) {
  HiveConfig hiveConfig = HiveConfig(config);
  auto endpoint = hiveConfig.s3Endpoint();
  if (!endpoint.empty()) {
    // The identity is the endpoint.
    return endpoint;
  }
  // Default key value.
  return "aws-s3-key";
}

std::shared_ptr<FileSystem> fileSystemGenerator(
    std::shared_ptr<const config::ConfigBase> properties,
    std::string_view /*filePath*/) {
  std::shared_ptr<config::ConfigBase> config =
      std::make_shared<config::ConfigBase>(
          std::unordered_map<std::string, std::string>());
  if (properties) {
    config = std::make_shared<config::ConfigBase>(properties->rawConfigsCopy());
  }
  const auto s3Identity = getS3Identity(config);

  return fileSystems().withWLock(
      [&](auto& instanceMap) -> std::shared_ptr<FileSystem> {
        initializeS3(config.get());
        auto iterator = instanceMap.find(s3Identity);
        if (iterator == instanceMap.end()) {
          auto fs = std::make_shared<S3FileSystem>(properties);
          instanceMap.insert({s3Identity, fs});
          return fs;
        }
        return iterator->second;
      });
}

std::unique_ptr<velox::dwio::common::FileSink> s3WriteFileSinkGenerator(
    const std::string& fileURI,
    const velox::dwio::common::FileSink::Options& options) {
  if (isS3File(fileURI)) {
    auto fileSystem =
        filesystems::getFileSystem(fileURI, options.connectorProperties);
    return std::make_unique<dwio::common::WriteFileSink>(
        fileSystem->openFileForWrite(fileURI, {{}, options.pool, std::nullopt}),
        fileURI,
        options.metricLogger,
        options.stats);
  }
  return nullptr;
}
#endif

void registerS3FileSystem() {
#ifdef VELOX_ENABLE_S3
  fileSystems().withWLock([&](auto& instanceMap) {
    if (instanceMap.empty()) {
      registerFileSystem(isS3File, std::function(fileSystemGenerator));
      dwio::common::FileSink::registerFactory(
          std::function(s3WriteFileSinkGenerator));
    }
  });
#endif
}

void finalizeS3FileSystem() {
#ifdef VELOX_ENABLE_S3
  bool singleUseCount = true;
  fileSystems().withWLock([&](auto& instanceMap) {
    for (const auto& [id, fs] : instanceMap) {
      singleUseCount &= (fs.use_count() == 1);
    }
    VELOX_CHECK(singleUseCount, "Cannot finalize S3FileSystem while in use");
    instanceMap.clear();
  });

  finalizeS3();
#endif
}

} // namespace facebook::velox::filesystems

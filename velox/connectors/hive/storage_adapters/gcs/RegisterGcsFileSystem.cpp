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

#ifdef VELOX_ENABLE_GCS
#include "velox/common/config/Config.h"
#include "velox/connectors/hive/storage_adapters/gcs/GcsFileSystem.h" // @manual
#include "velox/connectors/hive/storage_adapters/gcs/GcsUtil.h" // @manual
#include "velox/dwio/common/FileSink.h"
#endif

namespace facebook::velox::filesystems {

#ifdef VELOX_ENABLE_GCS

using FileSystemMap = folly::Synchronized<
    std::unordered_map<std::string, std::shared_ptr<FileSystem>>>;

/// Multiple GCS filesystems are supported.
FileSystemMap& gcsFileSystems() {
  static FileSystemMap instances;
  return instances;
}

std::function<std::shared_ptr<
    FileSystem>(std::shared_ptr<const config::ConfigBase>, std::string_view)>
gcsFileSystemGenerator() {
  static auto filesystemGenerator =
      [](std::shared_ptr<const config::ConfigBase> properties,
         std::string_view filePath) {
        const auto file = gcsPath(filePath);
        std::string bucket;
        std::string object;
        setBucketAndKeyFromGcsPath(file, bucket, object);
        auto cacheKey = fmt::format(
            "{}-{}",
            properties->get<std::string>("hive.gcs.endpoint").value(),
            bucket);

        // Check if an instance exists with a read lock (shared).
        auto fs = gcsFileSystems().withRLock(
            [&](auto& instanceMap) -> std::shared_ptr<FileSystem> {
              auto iterator = instanceMap.find(cacheKey);
              if (iterator != instanceMap.end()) {
                return iterator->second;
              }
              return nullptr;
            });
        if (fs != nullptr) {
          return fs;
        }

        return gcsFileSystems().withWLock(
            [&](auto& instanceMap) -> std::shared_ptr<FileSystem> {
              // Repeat the checks with a write lock.
              auto iterator = instanceMap.find(cacheKey);
              if (iterator != instanceMap.end()) {
                return iterator->second;
              }

              std::shared_ptr<GcsFileSystem> fs;
              if (properties != nullptr) {
                fs = std::make_shared<GcsFileSystem>(properties);
              } else {
                fs = std::make_shared<GcsFileSystem>(
                    std::make_shared<config::ConfigBase>(
                        std::unordered_map<std::string, std::string>()));
              }
              fs->initializeClient();

              instanceMap.insert({cacheKey, fs});
              return fs;
            });
      };
  return filesystemGenerator;
}

std::unique_ptr<velox::dwio::common::FileSink> gcsWriteFileSinkGenerator(
    const std::string& fileURI,
    const velox::dwio::common::FileSink::Options& options) {
  if (isGcsFile(fileURI)) {
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

void registerGcsFileSystem() {
#ifdef VELOX_ENABLE_GCS
  registerFileSystem(isGcsFile, gcsFileSystemGenerator());
  dwio::common::FileSink::registerFactory(
      std::function(gcsWriteFileSinkGenerator));
#endif
}

} // namespace facebook::velox::filesystems

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
#include "velox/connectors/hive/storage_adapters/s3fs/S3FileSystem.h"
#include "velox/connectors/hive/storage_adapters/s3fs/S3Util.h"
#include "velox/core/Config.h"
#include "velox/dwio/common/FileSink.h"
#endif

#include "velox/connectors/hive/storage_adapters/s3fs/RegisterS3FileSystem.h"

namespace facebook::velox::filesystems {

#ifdef VELOX_ENABLE_S3
folly::once_flag S3FSInstantiationFlag;

// Only one instance of S3FileSystem is supported for now.
// TODO: Support multiple S3FileSystem instances using a cache
static std::shared_ptr<S3FileSystem> s3fs = nullptr;

std::function<std::shared_ptr<
    FileSystem>(std::shared_ptr<const Config>, std::string_view)>
fileSystemGenerator() {
  static auto filesystemGenerator = [](std::shared_ptr<const Config> properties,
                                       std::string_view filePath) {
    folly::call_once(S3FSInstantiationFlag, [&properties]() {
      std::shared_ptr<S3FileSystem> fs;
      if (properties != nullptr) {
        initializeS3(properties.get());
        fs = std::make_shared<S3FileSystem>(properties);
      } else {
        auto config = std::make_shared<core::MemConfig>();
        initializeS3(config.get());
        fs = std::make_shared<S3FileSystem>(config);
      }
      s3fs = fs;
    });
    return s3fs;
  };
  return filesystemGenerator;
}

std::function<std::unique_ptr<velox::dwio::common::FileSink>(
    const std::string&,
    const velox::dwio::common::FileSink::Options& options)>
s3WriteFileSinkGenerator() {
  static auto s3WriteFileSink =
      [](const std::string& fileURI,
         const velox::dwio::common::FileSink::Options& options)
      -> std::unique_ptr<dwio::common::WriteFileSink> {
    if (isS3File(fileURI)) {
      auto fileSystem =
          filesystems::getFileSystem(fileURI, options.connectorProperties);
      return std::make_unique<dwio::common::WriteFileSink>(
          fileSystem->openFileForWrite(fileURI, {{}, options.pool}),
          fileURI,
          options.metricLogger,
          options.stats);
    }
    return nullptr;
  };

  return s3WriteFileSink;
}
#endif

void registerS3FileSystem() {
#ifdef VELOX_ENABLE_S3
  if (!s3fs) {
    registerFileSystem(isS3File, fileSystemGenerator());
    dwio::common::FileSink::registerFactory(s3WriteFileSinkGenerator());
  }
#endif
}

void finalizeS3FileSystem() {
#ifdef VELOX_ENABLE_S3
  VELOX_CHECK(
      !s3fs || (s3fs && s3fs.use_count() == 1),
      "Cannot finalize S3FileSystem while in use");
  s3fs.reset();
  finalizeS3();
#endif
}

} // namespace facebook::velox::filesystems

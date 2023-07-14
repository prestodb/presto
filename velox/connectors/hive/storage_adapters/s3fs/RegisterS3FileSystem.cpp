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
#endif

namespace facebook::velox::filesystems {

#ifdef VELOX_ENABLE_S3
folly::once_flag S3FSInstantiationFlag;

std::function<std::shared_ptr<
    FileSystem>(std::shared_ptr<const Config>, std::string_view)>
fileSystemGenerator() {
  static auto filesystemGenerator = [](std::shared_ptr<const Config> properties,
                                       std::string_view filePath) {
    // Only one instance of S3FileSystem is supported for now.
    // TODO: Support multiple S3FileSystem instances using a cache
    // Initialize on first access and reuse after that.
    static std::shared_ptr<FileSystem> s3fs;
    folly::call_once(S3FSInstantiationFlag, [&properties]() {
      std::shared_ptr<S3FileSystem> fs;
      if (properties != nullptr) {
        fs = std::make_shared<S3FileSystem>(properties);
      } else {
        fs =
            std::make_shared<S3FileSystem>(std::make_shared<core::MemConfig>());
      }
      fs->initializeClient();
      s3fs = fs;
    });
    return s3fs;
  };
  return filesystemGenerator;
}
#endif

void registerS3FileSystem() {
#ifdef VELOX_ENABLE_S3
  registerFileSystem(isS3File, fileSystemGenerator());
#endif
}

} // namespace facebook::velox::filesystems

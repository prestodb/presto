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
#include "velox/connectors/hive/storage_adapters/gcs/GCSFileSystem.h" // @manual
#include "velox/connectors/hive/storage_adapters/gcs/GCSUtil.h" // @manual
#endif

namespace facebook::velox::filesystems {

#ifdef VELOX_ENABLE_GCS
folly::once_flag GCSInstantiationFlag;

std::function<std::shared_ptr<
    FileSystem>(std::shared_ptr<const config::ConfigBase>, std::string_view)>
gcsFileSystemGenerator() {
  static auto filesystemGenerator =
      [](std::shared_ptr<const config::ConfigBase> properties,
         std::string_view filePath) {
        // Only one instance of GCSFileSystem is supported for now (follow S3
        // for now).
        // TODO: Support multiple GCSFileSystem instances using a cache
        // Initialize on first access and reuse after that.
        static std::shared_ptr<FileSystem> gcsfs;
        folly::call_once(GCSInstantiationFlag, [&properties]() {
          std::shared_ptr<GCSFileSystem> fs;
          if (properties != nullptr) {
            fs = std::make_shared<GCSFileSystem>(properties);
          } else {
            fs = std::make_shared<GCSFileSystem>(
                std::make_shared<config::ConfigBase>(
                    std::unordered_map<std::string, std::string>()));
          }
          fs->initializeClient();
          gcsfs = fs;
        });
        return gcsfs;
      };
  return filesystemGenerator;
}
#endif

void registerGCSFileSystem() {
#ifdef VELOX_ENABLE_GCS
  registerFileSystem(isGCSFile, gcsFileSystemGenerator());
#endif
}

} // namespace facebook::velox::filesystems

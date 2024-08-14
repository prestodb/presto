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

#ifdef VELOX_ENABLE_ABFS
#include "velox/common/config/Config.h"
#include "velox/connectors/hive/storage_adapters/abfs/AbfsFileSystem.h" // @manual
#include "velox/connectors/hive/storage_adapters/abfs/AbfsUtil.h" // @manual
#endif

namespace facebook::velox::filesystems::abfs {

#ifdef VELOX_ENABLE_ABFS
folly::once_flag abfsInitiationFlag;

std::shared_ptr<FileSystem> abfsFileSystemGenerator(
    std::shared_ptr<const config::ConfigBase> properties,
    std::string_view filePath) {
  static std::shared_ptr<FileSystem> filesystem;
  folly::call_once(abfsInitiationFlag, [&properties]() {
    filesystem = std::make_shared<AbfsFileSystem>(properties);
  });
  return filesystem;
}
#endif

void registerAbfsFileSystem() {
#ifdef VELOX_ENABLE_ABFS
  LOG(INFO) << "Register ABFS";
  registerFileSystem(isAbfsFile, std::function(abfsFileSystemGenerator));
#endif
}

} // namespace facebook::velox::filesystems::abfs

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

#ifdef VELOX_ENABLE_HDFS3
#include "folly/concurrency/ConcurrentHashMap.h"

#include "velox/common/config/Config.h"
#include "velox/connectors/hive/storage_adapters/hdfs/HdfsFileSystem.h" // @manual
#include "velox/connectors/hive/storage_adapters/hdfs/HdfsUtil.h" // @manual
#include "velox/dwio/common/FileSink.h"
#endif

namespace facebook::velox::filesystems {

#ifdef VELOX_ENABLE_HDFS3
std::mutex mtx;

std::function<std::shared_ptr<
    FileSystem>(std::shared_ptr<const config::ConfigBase>, std::string_view)>
hdfsFileSystemGenerator() {
  static auto filesystemGenerator = [](std::shared_ptr<const config::ConfigBase>
                                           properties,
                                       std::string_view filePath) {
    static folly::ConcurrentHashMap<std::string, std::shared_ptr<FileSystem>>
        filesystems;
    static folly::
        ConcurrentHashMap<std::string, std::shared_ptr<folly::once_flag>>
            hdfsInitiationFlags;
    HdfsServiceEndpoint endpoint =
        HdfsFileSystem::getServiceEndpoint(filePath, properties.get());
    std::string hdfsIdentity = endpoint.identity();
    if (filesystems.find(hdfsIdentity) != filesystems.end()) {
      return filesystems[hdfsIdentity];
    }
    std::unique_lock<std::mutex> lk(mtx, std::defer_lock);
    /// If the init flag for a given hdfs identity is not found,
    /// create one for init use. It's a singleton.
    if (hdfsInitiationFlags.find(hdfsIdentity) == hdfsInitiationFlags.end()) {
      lk.lock();
      if (hdfsInitiationFlags.find(hdfsIdentity) == hdfsInitiationFlags.end()) {
        std::shared_ptr<folly::once_flag> initiationFlagPtr =
            std::make_shared<folly::once_flag>();
        hdfsInitiationFlags.insert(hdfsIdentity, initiationFlagPtr);
      }
      lk.unlock();
    }
    folly::call_once(
        *hdfsInitiationFlags[hdfsIdentity].get(),
        [&properties, endpoint, hdfsIdentity]() {
          auto filesystem =
              std::make_shared<HdfsFileSystem>(properties, endpoint);
          filesystems.insert(hdfsIdentity, filesystem);
        });
    return filesystems[hdfsIdentity];
  };
  return filesystemGenerator;
}

std::function<std::unique_ptr<velox::dwio::common::FileSink>(
    const std::string&,
    const velox::dwio::common::FileSink::Options& options)>
hdfsWriteFileSinkGenerator() {
  static auto hdfsWriteFileSink =
      [](const std::string& fileURI,
         const velox::dwio::common::FileSink::Options& options) {
        if (HdfsFileSystem::isHdfsFile(fileURI)) {
          std::string pathSuffix =
              getHdfsPath(fileURI, HdfsFileSystem::kScheme);
          auto fileSystem =
              filesystems::getFileSystem(fileURI, options.connectorProperties);
          return std::make_unique<dwio::common::WriteFileSink>(
              fileSystem->openFileForWrite(pathSuffix),
              fileURI,
              options.metricLogger,
              options.stats);
        }
        return static_cast<std::unique_ptr<dwio::common::WriteFileSink>>(
            nullptr);
      };

  return hdfsWriteFileSink;
}
#endif

void registerHdfsFileSystem() {
#ifdef VELOX_ENABLE_HDFS3
  registerFileSystem(HdfsFileSystem::isHdfsFile, hdfsFileSystemGenerator());
  dwio::common::FileSink::registerFactory(hdfsWriteFileSinkGenerator());
#endif
}

} // namespace facebook::velox::filesystems

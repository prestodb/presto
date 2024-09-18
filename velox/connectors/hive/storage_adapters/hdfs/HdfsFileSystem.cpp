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
#include "velox/connectors/hive/storage_adapters/hdfs/HdfsFileSystem.h"
#include <hdfs/hdfs.h>
#include <mutex>
#include "velox/common/config/Config.h"
#include "velox/connectors/hive/storage_adapters/hdfs/HdfsReadFile.h"
#include "velox/connectors/hive/storage_adapters/hdfs/HdfsWriteFile.h"

namespace facebook::velox::filesystems {
std::string_view HdfsFileSystem::kScheme("hdfs://");

class HdfsFileSystem::Impl {
 public:
  // Keep config here for possible use in the future.
  explicit Impl(
      const config::ConfigBase* config,
      const HdfsServiceEndpoint& endpoint) {
    auto builder = hdfsNewBuilder();
    hdfsBuilderSetNameNode(builder, endpoint.host.c_str());
    hdfsBuilderSetNameNodePort(builder, atoi(endpoint.port.data()));
    hdfsClient_ = hdfsBuilderConnect(builder);
    hdfsFreeBuilder(builder);
    VELOX_CHECK_NOT_NULL(
        hdfsClient_,
        "Unable to connect to HDFS: {}, got error: {}.",
        endpoint.identity(),
        hdfsGetLastError());
  }

  ~Impl() {
    LOG(INFO) << "Disconnecting HDFS file system";
    int disconnectResult = hdfsDisconnect(hdfsClient_);
    if (disconnectResult != 0) {
      LOG(WARNING) << "hdfs disconnect failure in HdfsReadFile close: "
                   << errno;
    }
  }

  hdfsFS hdfsClient() {
    return hdfsClient_;
  }

 private:
  hdfsFS hdfsClient_;
};

HdfsFileSystem::HdfsFileSystem(
    const std::shared_ptr<const config::ConfigBase>& config,
    const HdfsServiceEndpoint& endpoint)
    : FileSystem(config) {
  impl_ = std::make_shared<Impl>(config.get(), endpoint);
}

std::string HdfsFileSystem::name() const {
  return "HDFS";
}

std::unique_ptr<ReadFile> HdfsFileSystem::openFileForRead(
    std::string_view path,
    const FileOptions& /*unused*/) {
  if (path.find(kScheme) == 0) {
    path.remove_prefix(kScheme.length());
  }
  if (auto index = path.find('/')) {
    path.remove_prefix(index);
  }

  return std::make_unique<HdfsReadFile>(impl_->hdfsClient(), path);
}

std::unique_ptr<WriteFile> HdfsFileSystem::openFileForWrite(
    std::string_view path,
    const FileOptions& /*unused*/) {
  return std::make_unique<HdfsWriteFile>(impl_->hdfsClient(), path);
}

bool HdfsFileSystem::isHdfsFile(const std::string_view filePath) {
  return filePath.find(kScheme) == 0;
}

/// Gets hdfs endpoint from a given file path. If not found, fall back to get a
/// fixed one from configuration.
HdfsServiceEndpoint HdfsFileSystem::getServiceEndpoint(
    const std::string_view filePath,
    const config::ConfigBase* config) {
  auto endOfIdentityInfo = filePath.find('/', kScheme.size());
  std::string hdfsIdentity{
      filePath.data(), kScheme.size(), endOfIdentityInfo - kScheme.size()};
  if (hdfsIdentity.empty()) {
    // Fall back to get a fixed endpoint from config.
    auto hdfsHost = config->get<std::string>("hive.hdfs.host");
    VELOX_CHECK(
        hdfsHost.hasValue(),
        "hdfsHost is empty, configuration missing for hdfs host");
    auto hdfsPort = config->get<std::string>("hive.hdfs.port");
    VELOX_CHECK(
        hdfsPort.hasValue(),
        "hdfsPort is empty, configuration missing for hdfs port");
    return HdfsServiceEndpoint{*hdfsHost, *hdfsPort};
  }

  auto hostAndPortSeparator = hdfsIdentity.find(':', 0);
  // In HDFS HA mode, the hdfsIdentity is a nameservice ID with no port.
  if (hostAndPortSeparator == std::string::npos) {
    return HdfsServiceEndpoint{hdfsIdentity, ""};
  }
  std::string host{hdfsIdentity.data(), 0, hostAndPortSeparator};
  std::string port{
      hdfsIdentity.data(),
      hostAndPortSeparator + 1,
      hdfsIdentity.size() - hostAndPortSeparator - 1};
  return HdfsServiceEndpoint{host, port};
}

void HdfsFileSystem::remove(std::string_view path) {
  VELOX_UNSUPPORTED("Does not support removing files from hdfs");
}

} // namespace facebook::velox::filesystems

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
#include "HdfsFileSystem.h"
#include <hdfs/hdfs.h>
#include "HdfsReadFile.h"
#include "velox/common/file/FileSystems.h"
#include "velox/core/Context.h"

namespace facebook::velox::filesystems {
folly::once_flag hdfsInitiationFlag;
std::string_view HdfsFileSystem::kScheme("hdfs://");

class HdfsFileSystem::Impl {
 public:
  explicit Impl(const Config* config) {
    auto endpointInfo = getServiceEndpoint(config);
    auto builder = hdfsNewBuilder();
    hdfsBuilderSetNameNode(builder, endpointInfo.host.c_str());
    hdfsBuilderSetNameNodePort(builder, endpointInfo.port);
    hdfsClient_ = hdfsBuilderConnect(builder);
    VELOX_CHECK_NOT_NULL(
        hdfsClient_,
        "Unable to connect to HDFS, got error: {}.",
        hdfsGetLastError())
  }

  ~Impl() {
    LOG(INFO) << "Disconnecting HDFS file system";
    int disconnectResult = hdfsDisconnect(hdfsClient_);
    if (disconnectResult != 0) {
      LOG(WARNING) << "hdfs disconnect failure in HdfsReadFile close: "
                   << errno;
    }
  }

  static HdfsServiceEndpoint getServiceEndpoint(const Config* config) {
    auto hdfsHost = config->get("hive.hdfs.host");
    VELOX_CHECK(
        hdfsHost.hasValue(),
        "hdfsHost is empty, configuration missing for hdfs host");
    auto hdfsPort = config->get("hive.hdfs.port");
    VELOX_CHECK(
        hdfsPort.hasValue(),
        "hdfsPort is empty, configuration missing for hdfs port");
    HdfsServiceEndpoint endpoint{*hdfsHost, atoi(hdfsPort->data())};
    return endpoint;
  }

  hdfsFS hdfsClient() {
    return hdfsClient_;
  }

 private:
  hdfsFS hdfsClient_;
};

HdfsFileSystem::HdfsFileSystem(const std::shared_ptr<const Config>& config)
    : FileSystem(config) {
  impl_ = std::make_shared<Impl>(config.get());
}

std::string HdfsFileSystem::name() const {
  return "HDFS";
}

std::unique_ptr<ReadFile> HdfsFileSystem::openFileForRead(
    std::string_view path) {
  if (path.find(kScheme) == 0) {
    path.remove_prefix(kScheme.length());
  }
  if (auto index = path.find('/')) {
    path.remove_prefix(index);
  }

  return std::make_unique<HdfsReadFile>(impl_->hdfsClient(), path);
}

std::unique_ptr<WriteFile> HdfsFileSystem::openFileForWrite(
    std::string_view path) {
  VELOX_UNSUPPORTED("Write to HDFS is unsupported");
}

bool HdfsFileSystem::isHdfsFile(const std::string_view filename) {
  return filename.find(kScheme) == 0;
}

static std::function<std::shared_ptr<FileSystem>(std::shared_ptr<const Config>)>
    filesystemGenerator = [](std::shared_ptr<const Config> properties) {
      static std::shared_ptr<FileSystem> filesystem;
      folly::call_once(hdfsInitiationFlag, [&properties]() {
        filesystem = std::make_shared<HdfsFileSystem>(properties);
      });
      return filesystem;
    };

void HdfsFileSystem::remove(std::string_view path) {
  VELOX_UNSUPPORTED("Does not support removing files from hdfs");
}

void registerHdfsFileSystem() {
  registerFileSystem(HdfsFileSystem::isHdfsFile, filesystemGenerator);
}
} // namespace facebook::velox::filesystems

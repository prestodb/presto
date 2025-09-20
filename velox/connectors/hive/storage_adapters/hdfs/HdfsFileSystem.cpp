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
#include "velox/common/config/Config.h"
#include "velox/connectors/hive/storage_adapters/hdfs/HdfsReadFile.h"
#include "velox/connectors/hive/storage_adapters/hdfs/HdfsWriteFile.h"
#include "velox/external/hdfs/ArrowHdfsInternal.h"

namespace facebook::velox::filesystems {
std::string_view HdfsFileSystem::kScheme("hdfs://");

std::string_view HdfsFileSystem::kViewfsScheme("viewfs://");

class HdfsFileSystem::Impl {
 public:
  // Keep config here for possible use in the future.
  explicit Impl(
      const config::ConfigBase* config,
      const HdfsServiceEndpoint& endpoint) {
    auto status = filesystems::arrow::io::internal::ConnectLibHdfs(&driver_);
    if (!status.ok()) {
      LOG(ERROR) << "ConnectLibHdfs failed due to: " << status.ToString();
    }

    // connect to HDFS with the builder object
    hdfsBuilder* builder = driver_->NewBuilder();
    if (endpoint.isViewfs) {
      // The default NameNode configuration will be used (from the XML
      // configuration files). See:
      // https://github.com/facebookincubator/velox/blob/main/velox/external/hdfs/hdfs.h#L289
      driver_->BuilderSetNameNode(builder, "default");
    } else {
      driver_->BuilderSetNameNode(builder, endpoint.host.c_str());
      driver_->BuilderSetNameNodePort(builder, atoi(endpoint.port.data()));
    }
    driver_->BuilderSetForceNewInstance(builder);
    hdfsClient_ = driver_->BuilderConnect(builder);
    VELOX_CHECK_NOT_NULL(
        hdfsClient_,
        "Unable to connect to HDFS: {}, got error: {}.",
        endpoint.identity(),
        driver_->GetLastExceptionRootCause());
  }

  ~Impl() {
    if (!closed_) {
      LOG(WARNING)
          << "The HdfsFileSystem instance is not closed upon destruction. You must explicitly call the close() API before JVM termination to ensure proper disconnection.";
    }
  }

  // The HdfsFileSystem::Disconnect operation requires the JVM method
  // definitions to be loaded within an active JVM process.
  // Therefore, it must be invoked before the JVM shuts down.

  // To address this, we’ve introduced a new close() API that performs the
  // disconnect operation. Third-party applications can call this close() method
  // prior to JVM termination to ensure proper cleanup.
  void close() {
    if (!closed_) {
      LOG(WARNING) << "Disconnecting HDFS file system";
      int disconnectResult = driver_->Disconnect(hdfsClient_);
      if (disconnectResult != 0) {
        LOG(WARNING) << "hdfs disconnect failure in HdfsReadFile close: "
                     << errno;
      }

      closed_ = true;
    }
  }

  hdfsFS hdfsClient() {
    return hdfsClient_;
  }

  filesystems::arrow::io::internal::LibHdfsShim* hdfsShim() {
    return driver_;
  }

 private:
  hdfsFS hdfsClient_;
  filesystems::arrow::io::internal::LibHdfsShim* driver_;
  bool closed_ = false;
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
  // Only remove the scheme for hdfs path.
  if (path.find(kScheme) == 0) {
    path.remove_prefix(kScheme.length());
    if (auto index = path.find('/')) {
      path.remove_prefix(index);
    }
  }
  return std::make_unique<HdfsReadFile>(
      impl_->hdfsShim(), impl_->hdfsClient(), path);
}

std::unique_ptr<WriteFile> HdfsFileSystem::openFileForWrite(
    std::string_view path,
    const FileOptions& /*unused*/) {
  return std::make_unique<HdfsWriteFile>(
      impl_->hdfsShim(), impl_->hdfsClient(), path);
}

void HdfsFileSystem::close() {
  impl_->close();
}

bool HdfsFileSystem::isHdfsFile(const std::string_view filePath) {
  return (filePath.find(kScheme) == 0) || (filePath.find(kViewfsScheme) == 0);
}

/// Gets hdfs endpoint from a given file path. If not found, fall back to get a
/// fixed one from configuration.
HdfsServiceEndpoint HdfsFileSystem::getServiceEndpoint(
    const std::string_view filePath,
    const config::ConfigBase* config) {
  if (filePath.find(kViewfsScheme) == 0) {
    return HdfsServiceEndpoint{"viewfs", "", true};
  }

  auto endOfIdentityInfo = filePath.find('/', kScheme.size());
  std::string hdfsIdentity{
      filePath.data(), kScheme.size(), endOfIdentityInfo - kScheme.size()};
  if (hdfsIdentity.empty()) {
    // Fall back to get a fixed endpoint from config.
    auto hdfsHost = config->get<std::string>("hive.hdfs.host");
    VELOX_CHECK(
        hdfsHost.has_value(),
        "hdfsHost is empty, configuration missing for hdfs host");
    auto hdfsPort = config->get<std::string>("hive.hdfs.port");
    VELOX_CHECK(
        hdfsPort.has_value(),
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
  // Only remove the scheme for hdfs path.
  if (path.find(kScheme) == 0) {
    path.remove_prefix(kScheme.length());
    if (auto index = path.find('/')) {
      path.remove_prefix(index);
    }
  }

  VELOX_CHECK_EQ(
      impl_->hdfsShim()->Delete(impl_->hdfsClient(), path.data(), 0),
      0,
      "Cannot delete file : {} in HDFS, error is : {}",
      path,
      impl_->hdfsShim()->GetLastExceptionRootCause());
}

std::vector<std::string> HdfsFileSystem::list(std::string_view path) {
  // Only remove the scheme for hdfs path.
  if (path.find(kScheme) == 0) {
    path.remove_prefix(kScheme.length());
    if (auto index = path.find('/')) {
      path.remove_prefix(index);
    }
  }

  std::vector<std::string> result;
  int numEntries;

  auto fileInfo = impl_->hdfsShim()->ListDirectory(
      impl_->hdfsClient(), path.data(), &numEntries);

  VELOX_CHECK_NOT_NULL(
      fileInfo,
      "Unable to list the files in path {}. got error: {}",
      path,
      impl_->hdfsShim()->GetLastExceptionRootCause());

  for (auto i = 0; i < numEntries; i++) {
    result.emplace_back(fileInfo[i].mName);
  }

  impl_->hdfsShim()->FreeFileInfo(fileInfo, numEntries);

  return result;
}

bool HdfsFileSystem::exists(std::string_view path) {
  // Only remove the scheme for hdfs path.
  if (path.find(kScheme) == 0) {
    path.remove_prefix(kScheme.length());
    if (auto index = path.find('/')) {
      path.remove_prefix(index);
    }
  }

  return impl_->hdfsShim()->Exists(impl_->hdfsClient(), path.data()) == 0;
}

void HdfsFileSystem::mkdir(
    std::string_view path,
    const DirectoryOptions& options) {
  // Only remove the scheme for hdfs path.
  if (path.find(kScheme) == 0) {
    path.remove_prefix(kScheme.length());
    if (auto index = path.find('/')) {
      path.remove_prefix(index);
    }
  }

  VELOX_CHECK_EQ(
      impl_->hdfsShim()->MakeDirectory(impl_->hdfsClient(), path.data()),
      0,
      "Cannot mkdir {} in HDFS, error is : {}",
      path,
      impl_->hdfsShim()->GetLastExceptionRootCause());
}

void HdfsFileSystem::rename(
    std::string_view path,
    std::string_view newPath,
    bool overWrite) {
  VELOX_CHECK_EQ(
      overWrite, false, "HdfsFileSystem::rename doesn't support overwrite");
  // Only remove the scheme for hdfs path.
  if (path.find(kScheme) == 0) {
    path.remove_prefix(kScheme.length());
    if (auto index = path.find('/')) {
      path.remove_prefix(index);
    }
  }

  // Only remove the scheme for hdfs path.
  if (newPath.find(kScheme) == 0) {
    newPath.remove_prefix(kScheme.length());
    if (auto index = newPath.find('/')) {
      newPath.remove_prefix(index);
    }
  }

  VELOX_CHECK_EQ(
      impl_->hdfsShim()->Rename(
          impl_->hdfsClient(), path.data(), newPath.data()),
      0,
      "Cannot rename file from {} to {} in HDFS, error is : {}",
      path,
      newPath,
      impl_->hdfsShim()->GetLastExceptionRootCause());
}

void HdfsFileSystem::rmdir(std::string_view path) {
  // Only remove the scheme for hdfs path.
  if (path.find(kScheme) == 0) {
    path.remove_prefix(kScheme.length());
    if (auto index = path.find('/')) {
      path.remove_prefix(index);
    }
  }

  VELOX_CHECK_EQ(
      impl_->hdfsShim()->Delete(
          impl_->hdfsClient(), path.data(), /*recursive=*/true),
      0,
      "Cannot remove directory {} recursively in HDFS, error is : {}",
      path,
      impl_->hdfsShim()->GetLastExceptionRootCause());
}

} // namespace facebook::velox::filesystems

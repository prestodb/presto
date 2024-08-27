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
#include "velox/common/file/FileSystems.h"

namespace facebook::velox::filesystems {
struct HdfsServiceEndpoint {
  HdfsServiceEndpoint(const std::string& hdfsHost, const std::string& hdfsPort)
      : host(hdfsHost), port(hdfsPort) {}

  /// In HDFS HA mode, the identity is a nameservice ID with no port, e.g.,
  /// the identity is nameservice_id for
  /// hdfs://nameservice_id/file/path/in/hdfs. Otherwise, a port must be
  /// contained, e.g., the identity is hdfs_namenode:9000 for
  /// hdfs://hdfs_namenode:9000/file/path/in/hdfs.
  std::string identity() const {
    return host + (port.empty() ? "" : ":" + port);
  }

  const std::string host;
  const std::string port;
};

/**
 * You can configure hdfs settings (timeouts etc) using configure file
 * which is given by environment parameter LIBHDFS3_CONF
 * or "hdfs-client.xml" in working directory.
 *
 * Internally you can use hdfsBuilderConfSetStr to configure the client
 */
class HdfsFileSystem : public FileSystem {
 public:
  explicit HdfsFileSystem(
      const std::shared_ptr<const config::ConfigBase>& config,
      const HdfsServiceEndpoint& endpoint);

  std::string name() const override;

  std::unique_ptr<ReadFile> openFileForRead(
      std::string_view path,
      const FileOptions& options = {}) override;

  std::unique_ptr<WriteFile> openFileForWrite(
      std::string_view path,
      const FileOptions& options = {}) override;

  void remove(std::string_view path) override;

  virtual void rename(
      std::string_view path,
      std::string_view newPath,
      bool overWrite = false) override {
    VELOX_UNSUPPORTED("rename for HDFs not implemented");
  }

  bool exists(std::string_view path) override {
    VELOX_UNSUPPORTED("exists for HDFS not implemented");
  }

  virtual std::vector<std::string> list(std::string_view path) override {
    VELOX_UNSUPPORTED("list for HDFS not implemented");
  }

  void mkdir(std::string_view path) override {
    VELOX_UNSUPPORTED("mkdir for HDFS not implemented");
  }

  void rmdir(std::string_view path) override {
    VELOX_UNSUPPORTED("rmdir for HDFS not implemented");
  }

  static bool isHdfsFile(std::string_view filename);

  /// The given filePath is used to infer hdfs endpoint. If hdfs identity is
  /// missing from filePath, the configured "hive.hdfs.host" & "hive.hdfs.port"
  /// will be used.
  static HdfsServiceEndpoint getServiceEndpoint(
      const std::string_view filePath,
      const config::ConfigBase* config);

  static std::string_view kScheme;

 protected:
  class Impl;
  std::shared_ptr<Impl> impl_;
};

} // namespace facebook::velox::filesystems

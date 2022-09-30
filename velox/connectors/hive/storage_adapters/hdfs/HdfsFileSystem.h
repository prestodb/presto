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
  std::string host;
  int port;
};

/**
 * You can configure hdfs settings (timeouts etc) using configure file
 * which is given by environment parameter LIBHDFS3_CONF
 * or "hdfs-client.xml" in working directory.
 *
 * Internally you can use hdfsBuilderConfSetStr to configure the client
 */
class HdfsFileSystem : public FileSystem {
 private:
  static std::string_view kScheme;

 public:
  explicit HdfsFileSystem(const std::shared_ptr<const Config>& config);

  std::string name() const override;

  std::unique_ptr<ReadFile> openFileForRead(std::string_view path) override;

  std::unique_ptr<WriteFile> openFileForWrite(std::string_view path) override;

  void remove(std::string_view path) override;

  bool exists(std::string_view path) override {
    VELOX_UNSUPPORTED("exists for HDFS not implemented");
  }

  virtual std::vector<std::string> list(std::string_view path) override {
    VELOX_UNSUPPORTED("list for HDFS not implemented");
  }

  static bool isHdfsFile(std::string_view filename);

 protected:
  class Impl;
  std::shared_ptr<Impl> impl_;
};

// Register the HDFS.
void registerHdfsFileSystem();
} // namespace facebook::velox::filesystems

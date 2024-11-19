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

#include "velox/common/config/Config.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"

#include <azure/storage/blobs/blob_container_client.hpp>
#include <azure/storage/blobs/block_blob_client.hpp>
#include <fmt/format.h>
#include <pwd.h>
#include <unistd.h>
#include <iostream>
#include "boost/process.hpp"

namespace facebook::velox::filesystems {

using namespace Azure::Storage::Blobs;
static std::string_view kAzuriteServerExecutableName{"azurite-blob"};
static std::string_view kAzuriteSearchPath{":/usr/bin/azurite"};

class AzuriteServer {
 public:
  AzuriteServer(int64_t port);

  const std::string connectionStr() const;

  void start();

  std::string URI() const;

  std::string fileURI() const;

  std::string container() const {
    return container_;
  }

  std::string file() const {
    return file_;
  }

  std::shared_ptr<const config::ConfigBase> hiveConfig(
      const std::unordered_map<std::string, std::string> configOverride = {})
      const;

  void stop();

  bool isRunning();

  void addFile(std::string source);

  virtual ~AzuriteServer();

 private:
  int64_t port_;
  const std::string account_{"test"};
  const std::string container_{"test"};
  const std::string file_{"test_file.txt"};
  const std::string key_{
      "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="};
  std::vector<std::string> commandOptions_;
  std::unique_ptr<::boost::process::child> serverProcess_;
  boost::filesystem::path exePath_;
  boost::process::environment env_;
};
} // namespace facebook::velox::filesystems

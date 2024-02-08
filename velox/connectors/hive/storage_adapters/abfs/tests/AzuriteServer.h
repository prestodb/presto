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

#include "velox/core/Config.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"

#include <azure/storage/blobs/blob_container_client.hpp>
#include <azure/storage/blobs/block_blob_client.hpp>
#include <fmt/format.h>
#include <pwd.h>
#include <unistd.h>
#include <iostream>
#include "boost/process.hpp"

namespace facebook::velox::filesystems::test {
using namespace Azure::Storage::Blobs;
static const std::string AzuriteServerExecutableName{"azurite-blob"};
static const std::string AzuriteSearchPath{":/usr/bin/azurite"};
static const std::string AzuriteAccountName{"test"};
static const std::string AzuriteContainerName{"test"};
// the default key of Azurite Server used for connection
static const std::string AzuriteAccountKey{
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="};
static const std::string AzuriteABFSEndpoint = fmt::format(
    "abfs://{}@{}.dfs.core.windows.net/",
    AzuriteContainerName,
    AzuriteAccountName);

class AzuriteServer {
 public:
  AzuriteServer(int64_t port);

  const std::string connectionStr() const;

  void start();

  void stop();

  bool isRunning();

  void addFile(std::string source, std::string destination);

  virtual ~AzuriteServer();

 private:
  int64_t port_;
  std::vector<std::string> commandOptions_;
  std::unique_ptr<::boost::process::child> serverProcess_;
  boost::filesystem::path exePath_;
  boost::process::environment env_;
};
} // namespace facebook::velox::filesystems::test

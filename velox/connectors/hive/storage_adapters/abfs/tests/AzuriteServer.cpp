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

#include "velox/connectors/hive/storage_adapters/abfs/tests/AzuriteServer.h"

namespace facebook::velox::filesystems::test {
const std::string AzuriteServer::connectionStr() const {
  return fmt::format(
      "DefaultEndpointsProtocol=http;AccountName={};AccountKey={};BlobEndpoint=http://127.0.0.1:{}/{};",
      AzuriteAccountName,
      AzuriteAccountKey,
      port_,
      AzuriteAccountName);
}

void AzuriteServer::start() {
  try {
    serverProcess_ = std::make_unique<boost::process::child>(
        env_, exePath_, commandOptions_);
    serverProcess_->wait_for(std::chrono::duration<int, std::milli>(5000));
    VELOX_CHECK_EQ(
        serverProcess_->exit_code(),
        383,
        "AzuriteServer process exited, code: ",
        serverProcess_->exit_code());
  } catch (const std::exception& e) {
    VELOX_FAIL("Failed to launch Azurite server: {}", e.what());
  }
}

void AzuriteServer::stop() {
  if (serverProcess_ && serverProcess_->valid()) {
    serverProcess_->terminate();
    serverProcess_->wait();
    serverProcess_.reset();
  }
}

bool AzuriteServer::isRunning() {
  if (serverProcess_) {
    return true;
  }
  return false;
}

// requires azurite executable to be on the PATH
AzuriteServer::AzuriteServer(int64_t port) : port_(port) {
  std::string dataLocation = fmt::format("/tmp/azurite_{}", port);
  std::string logFilePath = fmt::format("/tmp/azurite/azurite_{}.log", port);
  std::printf(
      "Launch azurite instance with port - %s, data location - %s, log file path - %s\n",
      std::to_string(port).c_str(),
      dataLocation.c_str(),
      logFilePath.c_str());
  commandOptions_ = {
      "--silent",
      "--blobPort",
      std::to_string(port),
      "--location",
      dataLocation,
      "--debug",
      logFilePath,
  };
  env_ = (boost::process::environment)boost::this_process::environment();
  env_["PATH"] = env_["PATH"].to_string() + AzuriteSearchPath;
  env_["AZURITE_ACCOUNTS"] =
      fmt::format("{}:{}", AzuriteAccountName, AzuriteAccountKey);
  auto path = env_["PATH"].to_vector();
  exePath_ = boost::process::search_path(
      AzuriteServerExecutableName,
      std::vector<boost::filesystem::path>(path.begin(), path.end()));
  std::printf("AzuriteServer executable path: %s\n", exePath_.c_str());
  if (exePath_.empty()) {
    VELOX_FAIL(
        "Failed to find azurite executable {}'", AzuriteServerExecutableName);
  }
}

void AzuriteServer::addFile(std::string source, std::string destination) {
  auto containerClient = BlobContainerClient::CreateFromConnectionString(
      connectionStr(), AzuriteContainerName);
  containerClient.CreateIfNotExists();
  auto blobClient = containerClient.GetBlockBlobClient(destination);
  blobClient.UploadFrom(source);
}

AzuriteServer::~AzuriteServer() {
  // stop();
}
} // namespace facebook::velox::filesystems::test

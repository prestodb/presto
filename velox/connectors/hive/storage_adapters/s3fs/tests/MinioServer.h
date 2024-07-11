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

#pragma once

#include "velox/core/Config.h"
#include "velox/exec/tests/utils/PortUtil.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"

#include "boost/process.hpp"

using namespace facebook::velox;

namespace {
constexpr char const* kMinioExecutableName{"minio-2022-05-26"};
constexpr char const* kMinioAccessKey{"minio"};
constexpr char const* kMinioSecretKey{"miniopass"};
} // namespace

// A minio server, managed as a child process.
// Adapted from the Apache Arrow library.
class MinioServer {
 public:
  MinioServer() : tempPath_(::exec::test::TempDirectoryPath::create()) {
    constexpr auto kHostAddressTemplate = "127.0.0.1:{}";
    auto ports = facebook::velox::exec::test::getFreePorts(2);
    connectionString_ = fmt::format(kHostAddressTemplate, ports[0]);
    consoleAddress_ = fmt::format(kHostAddressTemplate, ports[1]);
  }

  void start();

  void stop();

  void addBucket(const char* bucket) {
    const std::string path = tempPath_->getPath() + "/" + bucket;
    mkdir(path.c_str(), S_IRWXU | S_IRWXG);
  }

  std::string path() const {
    return tempPath_->getPath();
  }

  std::shared_ptr<const Config> hiveConfig(
      const std::unordered_map<std::string, std::string> configOverride = {})
      const {
    std::unordered_map<std::string, std::string> config({
        {"hive.s3.aws-access-key", accessKey_},
        {"hive.s3.aws-secret-key", secretKey_},
        {"hive.s3.endpoint", connectionString_},
        {"hive.s3.ssl.enabled", "false"},
        {"hive.s3.path-style-access", "true"},
    });

    // Update the default config map with the supplied configOverride map
    for (const auto& [configName, configValue] : configOverride) {
      config[configName] = configValue;
    }

    return std::make_shared<const core::MemConfig>(std::move(config));
  }

 private:
  const std::shared_ptr<exec::test::TempDirectoryPath> tempPath_;
  std::string connectionString_;
  std::string consoleAddress_;
  const std::string accessKey_ = kMinioAccessKey;
  const std::string secretKey_ = kMinioSecretKey;
  std::shared_ptr<::boost::process::child> serverProcess_;
};

void MinioServer::start() {
  boost::process::environment env = boost::this_process::environment();
  env["MINIO_ACCESS_KEY"] = accessKey_;
  env["MINIO_SECRET_KEY"] = secretKey_;

  auto exePath = boost::process::search_path(kMinioExecutableName);
  if (exePath.empty()) {
    VELOX_FAIL("Failed to find minio executable {}'", kMinioExecutableName);
  }

  const auto path = tempPath_->getPath();
  try {
    serverProcess_ = std::make_shared<boost::process::child>(
        env,
        exePath,
        "server",
        "--quiet",
        "--compat",
        "--address",
        connectionString_,
        "--console-address",
        consoleAddress_,
        path.c_str());
  } catch (const std::exception& e) {
    VELOX_FAIL("Failed to launch Minio server: {}", e.what());
  }
}

void MinioServer::stop() {
  if (serverProcess_ && serverProcess_->valid()) {
    // Brutal shutdown
    serverProcess_->terminate();
    serverProcess_->wait();
  }
}

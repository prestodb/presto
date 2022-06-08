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

#include "HdfsMiniCluster.h"

namespace facebook::velox::filesystems::test {
void HdfsMiniCluster::start() {
  try {
    serverProcess_ = std::make_unique<boost::process::child>(
        env_,
        exePath_,
        jarCommand,
        env_["HADOOP_HOME"].to_string() + miniclusterJar,
        miniclusterCommand,
        noMapReduceOption,
        formatNameNodeOption,
        httpPortOption,
        httpPort,
        nameNodePortOption,
        nameNodePort,
        configurationOption,
        turnOffPermissions);
    serverProcess_->wait_for(std::chrono::duration<int, std::milli>(60000));
    VELOX_CHECK_EQ(
        serverProcess_->exit_code(),
        383,
        "Minicluster process exited, code: ",
        serverProcess_->exit_code())
  } catch (const std::exception& e) {
    VELOX_FAIL("Failed to launch Minicluster server: {}", e.what());
  }
}

void HdfsMiniCluster::stop() {
  if (serverProcess_ && serverProcess_->valid()) {
    serverProcess_->terminate();
    serverProcess_->wait();
    serverProcess_.reset();
  }
}

bool HdfsMiniCluster::isRunning() {
  if (serverProcess_) {
    return true;
  }
  return false;
}

// requires hadoop executable to be on the PATH
HdfsMiniCluster::HdfsMiniCluster() {
  env_ = (boost::process::environment)boost::this_process::environment();
  env_["PATH"] = env_["PATH"].to_string() + hadoopSearchPath;
  auto path = env_["PATH"].to_vector();
  exePath_ = boost::process::search_path(
      miniClusterExecutableName,
      std::vector<boost::filesystem::path>(path.begin(), path.end()));
  if (exePath_.empty()) {
    VELOX_FAIL(
        "Failed to find minicluster executable {}'", miniClusterExecutableName);
  }
  boost::filesystem::path hadoopHomeDirectory = exePath_;
  hadoopHomeDirectory.remove_leaf().remove_leaf();
  setupEnvironment(hadoopHomeDirectory.string());
}

void HdfsMiniCluster::addFile(std::string source, std::string destination) {
  auto filePutProcess = std::make_shared<boost::process::child>(
      env_,
      exePath_,
      filesystemCommand,
      filesystemUrlOption,
      filesystemUrl,
      filePutOption,
      source,
      destination);
  bool isExited =
      filePutProcess->wait_for(std::chrono::duration<int, std::milli>(5000));
  if (!isExited) {
    VELOX_FAIL(
        "Failed to add file to hdfs, exit code: {}",
        filePutProcess->exit_code())
  }
}

HdfsMiniCluster::~HdfsMiniCluster() {
  stop();
}

void HdfsMiniCluster::setupEnvironment(const std::string& homeDirectory) {
  env_["HADOOP_HOME"] = homeDirectory;
  env_["HADOOP_INSTALL"] = homeDirectory;
  env_["HADOOP_MAPRED_HOME"] = homeDirectory;
  env_["HADOOP_COMMON_HOME"] = homeDirectory;
  env_["HADOOP_HDFS_HOME"] = homeDirectory;
  env_["YARN_HOME"] = homeDirectory;
  env_["HADOOP_COMMON_LIB_NATIVE_DIR"] = homeDirectory + "/lib/native";
  env_["HADOOP_CONF_DIR"] = homeDirectory;
  env_["HADOOP_PREFIX"] = homeDirectory;
  env_["HADOOP_LIBEXEC_DIR"] = homeDirectory + "/libexec";
  env_["HADOOP_CONF_DIR"] = homeDirectory + "/etc/hadoop";
}
} // namespace facebook::velox::filesystems::test

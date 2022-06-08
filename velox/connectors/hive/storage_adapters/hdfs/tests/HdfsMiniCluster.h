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

#include "velox/core/Context.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"

#include <pwd.h>
#include <unistd.h>
#include <iostream>
#include "boost/process.hpp"

namespace facebook::velox::filesystems::test {
static const std::string miniClusterExecutableName{"hadoop"};
static const std::string hadoopSearchPath{":/usr/local/hadoop-2.10.1/bin"};
static const std::string jarCommand{"jar"};
static const std::string miniclusterJar{
    "/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-2.10.1-tests.jar"};
static const std::string miniclusterCommand{"minicluster"};
static const std::string noMapReduceOption{"-nomr"};
static const std::string formatNameNodeOption{"-format"};
static const std::string httpPortOption{"-nnhttpport"};
static const std::string httpPort{"7676"};
static const std::string nameNodePortOption{"-nnport"};
static const std::string nameNodePort{"7878"};
static const std::string configurationOption{"-D"};
static const std::string turnOffPermissions{"dfs.permissions=false"};
static const std::string filesystemCommand{"fs"};
static const std::string filesystemUrlOption{"-fs"};
static const std::string filesystemUrl{"hdfs://localhost:" + nameNodePort};
static const std::string filePutOption{"-put"};

class HdfsMiniCluster {
 public:
  HdfsMiniCluster();

  void start();

  void stop();

  bool isRunning();

  void addFile(std::string source, std::string destination);
  virtual ~HdfsMiniCluster();

 private:
  void setupEnvironment(const std::string& homeDirectory);

  std::unique_ptr<::boost::process::child> serverProcess_;
  boost::filesystem::path exePath_;
  boost::process::environment env_;
};
} // namespace facebook::velox::filesystems::test

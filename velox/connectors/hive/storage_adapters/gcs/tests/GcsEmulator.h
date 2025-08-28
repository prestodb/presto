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

#include <boost/process.hpp>
#include <gmock/gmock-matchers.h>
#include <gmock/gmock-more-matchers.h>
#include <google/cloud/storage/client.h>
#include "gtest/gtest.h"

#include "velox/common/config/Config.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/connectors/hive/storage_adapters/gcs/GcsUtil.h"
#include "velox/exec/tests/utils/PortUtil.h"

namespace bp = boost::process;
namespace gc = google::cloud;
namespace gcs = google::cloud::storage;

namespace facebook::velox::filesystems {

static std::string_view kLoremIpsum =
    "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor"
    "incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis "
    "nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat."
    "Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu"
    "fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in"
    "culpa qui officia deserunt mollit anim id est laborum.";

class GcsEmulator : public testing::Environment {
 public:
  GcsEmulator() {
    auto port = std::to_string(exec::test::getFreePort());
    endpoint_ = "http://localhost:" + port;
    std::vector<std::string> names{"python3", "python"};
    // If the build script or application developer provides a value in the
    // PYTHON environment variable, then just use that.
    if (const auto* env = std::getenv("PYTHON")) {
      names = {env};
    }
    std::stringstream error;
    error << R"""({>>"Coud not start GCS emulator."
        " The following list of python interpreter names were used:"})""";
    for (const auto& interpreter : names) {
      auto exe_path = bp::search_path(interpreter);
      error << " " << interpreter;
      if (exe_path.empty()) {
        error << " (exe not found)";
        continue;
      }

      serverProcess_ = bp::child(
          boost::this_process::environment(),
          exe_path,
          "-m",
          "testbench",
          "--port",
          port,
          group_);
      if (serverProcess_.valid()) {
        return;
      }
      error << " (failed to start)";
      serverProcess_.terminate();
      serverProcess_.wait();
    }
    VELOX_FAIL(error.str());
  }

  ~GcsEmulator() override {
    // Brutal shutdown, kill the full process group because the GCS emulator
    // may launch additional children.
    group_.terminate();
    if (serverProcess_.valid()) {
      serverProcess_.wait();
    }
  }

  std::shared_ptr<const config::ConfigBase> hiveConfig(
      const std::unordered_map<std::string, std::string> configOverride = {})
      const {
    std::unordered_map<std::string, std::string> config(
        {{connector::hive::HiveConfig::kGcsEndpoint, endpoint_}});

    // Update the default config map with the supplied configOverride map
    for (const auto& [configName, configValue] : configOverride) {
      config[configName] = configValue;
    }

    return std::make_shared<const config::ConfigBase>(std::move(config));
  }

  const std::string& preexistingBucketName() {
    return bucketName_;
  }

  std::string_view preexistingObjectName() {
    return objectName_;
  }

  void bootstrap() {
    ASSERT_THAT(this, ::testing::NotNull());

    // Create a bucket and a small file in the testbench. This makes it easier
    // to bootstrap GcsFileSystem and its tests.
    auto client = gcs::Client(
        google::cloud::Options{}
            .set<gcs::RestEndpointOption>(this->endpoint_)
            .set<gc::UnifiedCredentialsOption>(gc::MakeInsecureCredentials()));

    auto bucket = client.CreateBucketForProject(
        bucketName_, "ignored-by-testbench", gcs::BucketMetadata{});
    ASSERT_TRUE(bucket.ok()) << "Failed to create bucket <" << bucketName_
                             << ">, status=" << bucket.status();

    auto object = client.InsertObject(bucketName_, objectName_, kLoremIpsum);
    ASSERT_TRUE(object.ok()) << "Failed to create object <" << objectName_
                             << ">, status=" << object.status();
  }

 private:
  std::string endpoint_;
  bp::child serverProcess_;
  bp::group group_;
  static std::string bucketName_;
  static std::string objectName_;
};

std::string GcsEmulator::bucketName_{"test1-gcs"};
std::string GcsEmulator::objectName_{"test-object-name"};

} // namespace facebook::velox::filesystems

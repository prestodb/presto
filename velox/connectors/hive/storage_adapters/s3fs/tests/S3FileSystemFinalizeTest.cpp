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

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/config/Config.h"
#include "velox/connectors/hive/storage_adapters/s3fs/S3FileSystem.h"

#include "gtest/gtest.h"

namespace facebook::velox {
namespace {

TEST(S3FileSystemFinalizeTest, finalize) {
  auto s3Config = std::make_shared<config::ConfigBase>(
      std::unordered_map<std::string, std::string>());
  ASSERT_TRUE(filesystems::initializeS3(s3Config.get()));
  ASSERT_FALSE(filesystems::initializeS3(s3Config.get()));
  {
    filesystems::S3FileSystem s3fs(s3Config);
    VELOX_ASSERT_THROW(
        filesystems::finalizeS3(), "Cannot finalize S3 while in use");
  }
  filesystems::finalizeS3();
  VELOX_ASSERT_THROW(
      filesystems::initializeS3(s3Config.get()),
      "Attempt to initialize S3 after it has been finalized.");
}

} // namespace
} // namespace facebook::velox

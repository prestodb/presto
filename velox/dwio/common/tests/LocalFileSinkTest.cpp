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

#include "velox/common/base/Fs.h"
#include "velox/dwio/common/DataSink.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"

#include <gtest/gtest.h>

using namespace ::testing;
using namespace facebook::velox::exec::test;

namespace facebook::velox::dwio::common {

TEST(LocalFileSinkTest, create) {
  LocalFileSink::registerFactory();

  auto root = TempDirectoryPath::create();
  auto filePath = fs::path(root->path) / "xxx/yyy/zzz/test_file.ext";

  ASSERT_FALSE(fs::exists(filePath.string()));

  auto localFileSink =
      DataSink::create(fmt::format("file:{}", filePath.string()));
  localFileSink->close();

  EXPECT_TRUE(fs::exists(filePath.string()));
}

} // namespace facebook::velox::dwio::common

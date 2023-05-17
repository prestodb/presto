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

#include <gtest/gtest.h>
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/exec/Exchange.h"
#include "velox/exec/Split.h"

namespace facebook::velox::exec {

TEST(SplitToStringTest, remoteSplit) {
  Split split{std::make_shared<RemoteConnectorSplit>("test")};
  ASSERT_EQ("Split: [Remote: test] -1", split.toString());

  split.groupId = 7;
  ASSERT_EQ("Split: [Remote: test] 7", split.toString());
}

TEST(SplitToStringTest, hiveSplit) {
  Split split{std::make_shared<connector::hive::HiveConnectorSplit>(
      "hive",
      "path/to/file.parquet",
      dwio::common::FileFormat::PARQUET,
      7,
      100)};
  ASSERT_EQ("Split: [Hive: path/to/file.parquet 7 - 100] -1", split.toString());

  split.groupId = 7;
  ASSERT_EQ("Split: [Hive: path/to/file.parquet 7 - 100] 7", split.toString());
}
} // namespace facebook::velox::exec

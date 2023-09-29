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

#include "velox/connectors/hive/storage_adapters/hdfs/HdfsUtil.h"

#include "gtest/gtest.h"

using namespace facebook::velox::filesystems;

TEST(HdfsUtilTest, getHdfsPath) {
  const std::string& kScheme = "hdfs://";
  std::string path1 =
      getHdfsPath("hdfs://hdfsCluster/user/hive/a.txt", kScheme);
  EXPECT_EQ("/user/hive/a.txt", path1);

  std::string path2 =
      getHdfsPath("hdfs://localhost:9000/user/hive/a.txt", kScheme);
  EXPECT_EQ("/user/hive/a.txt", path2);

  std::string path3 = getHdfsPath("hdfs:///user/hive/a.txt", kScheme);
  EXPECT_EQ("/user/hive/a.txt", path3);
}

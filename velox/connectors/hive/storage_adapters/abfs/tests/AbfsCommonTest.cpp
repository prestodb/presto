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

#include "velox/common/config/Config.h"
#include "velox/connectors/hive/storage_adapters/abfs/AbfsConfig.h"
#include "velox/connectors/hive/storage_adapters/abfs/AbfsUtil.h"

#include "gtest/gtest.h"

using namespace facebook::velox::filesystems;
using namespace facebook::velox;

TEST(AbfsUtilsTest, isAbfsFile) {
  EXPECT_FALSE(isAbfsFile("abfs:"));
  EXPECT_FALSE(isAbfsFile("abfss:"));
  EXPECT_FALSE(isAbfsFile("abfs:/"));
  EXPECT_FALSE(isAbfsFile("abfss:/"));
  EXPECT_TRUE(isAbfsFile("abfs://test@test.dfs.core.windows.net/test"));
  EXPECT_TRUE(isAbfsFile("abfss://test@test.dfs.core.windows.net/test"));
}

TEST(AbfsUtilsTest, abfsConfig) {
  const config::ConfigBase config(
      {{"fs.azure.account.key.efg.dfs.core.windows.net", "123"},
       {"fs.azure.account.key.foobar.dfs.core.windows.net", "456"},
       {"fs.azure.account.key.bar.dfs.core.windows.net", "789"}},
      false);

  auto abfsConfig =
      AbfsConfig("abfs://abc@efg.dfs.core.windows.net/file", config);
  EXPECT_EQ(abfsConfig.fileSystem(), "abc");
  EXPECT_EQ(abfsConfig.filePath(), "file");
  EXPECT_EQ(
      abfsConfig.connectionString(),
      "DefaultEndpointsProtocol=http;AccountName=efg;AccountKey=123;EndpointSuffix=core.windows.net;");

  auto abfssConfig = AbfsConfig(
      "abfss://abc@foobar.dfs.core.windows.net/sf_1/store_sales/ss_sold_date_sk=2450816/part-00002-a29c25f1-4638-494e-8428-a84f51dcea41.c000.snappy.parquet",
      config);
  EXPECT_EQ(abfssConfig.fileSystem(), "abc");
  EXPECT_EQ(
      abfssConfig.filePath(),
      "sf_1/store_sales/ss_sold_date_sk=2450816/part-00002-a29c25f1-4638-494e-8428-a84f51dcea41.c000.snappy.parquet");
  EXPECT_EQ(
      abfssConfig.connectionString(),
      "DefaultEndpointsProtocol=https;AccountName=foobar;AccountKey=456;EndpointSuffix=core.windows.net;");

  // Test with special character space.
  auto abfssConfigWithSpecialCharacters = AbfsConfig(
      "abfss://foo@bar.dfs.core.windows.net/main@dir/sub dir/test.txt", config);

  EXPECT_EQ(abfssConfigWithSpecialCharacters.fileSystem(), "foo");
  EXPECT_EQ(
      abfssConfigWithSpecialCharacters.filePath(), "main@dir/sub dir/test.txt");
}

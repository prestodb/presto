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

#include "velox/connectors/hive/storage_adapters/abfs/AbfsUtil.h"

#include "gtest/gtest.h"

using namespace facebook::velox::filesystems::abfs;

TEST(AbfsUtilsTest, isAbfsFile) {
  EXPECT_FALSE(isAbfsFile("abfs:"));
  EXPECT_FALSE(isAbfsFile("abfss:"));
  EXPECT_FALSE(isAbfsFile("abfs:/"));
  EXPECT_FALSE(isAbfsFile("abfss:/"));
  EXPECT_TRUE(isAbfsFile("abfs://test@test.dfs.core.windows.net/test"));
  EXPECT_TRUE(isAbfsFile("abfss://test@test.dfs.core.windows.net/test"));
}

TEST(AbfsUtilsTest, abfsAccount) {
  auto abfsAccount = AbfsAccount("abfs://test@test.dfs.core.windows.net/test");
  EXPECT_EQ(abfsAccount.accountNameWithSuffix(), "test.dfs.core.windows.net");
  EXPECT_EQ(abfsAccount.accountName(), "test");
  EXPECT_EQ(abfsAccount.endpointSuffix(), "core.windows.net");
  EXPECT_EQ(abfsAccount.fileSystem(), "test");
  EXPECT_EQ(abfsAccount.filePath(), "test");
  EXPECT_EQ(
      abfsAccount.credKey(), "fs.azure.account.key.test.dfs.core.windows.net");
  EXPECT_EQ(
      abfsAccount.connectionString("123"),
      "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=123;EndpointSuffix=core.windows.net");

  auto abfssAccount = AbfsAccount(
      "abfss://test@test.dfs.core.windows.net/sf_1/store_sales/ss_sold_date_sk=2450816/part-00002-a29c25f1-4638-494e-8428-a84f51dcea41.c000.snappy.parquet");
  EXPECT_EQ(abfssAccount.scheme(), "abfss");
  EXPECT_EQ(abfssAccount.accountNameWithSuffix(), "test.dfs.core.windows.net");
  EXPECT_EQ(abfssAccount.accountName(), "test");
  EXPECT_EQ(abfssAccount.endpointSuffix(), "core.windows.net");
  EXPECT_EQ(abfssAccount.fileSystem(), "test");
  EXPECT_EQ(
      abfssAccount.filePath(),
      "sf_1/store_sales/ss_sold_date_sk=2450816/part-00002-a29c25f1-4638-494e-8428-a84f51dcea41.c000.snappy.parquet");
  EXPECT_EQ(
      abfssAccount.credKey(), "fs.azure.account.key.test.dfs.core.windows.net");

  // test with special characters
  auto abfssAccountWithSpecialCharacters = AbfsAccount(
      "abfss://test@test.dfs.core.windows.net/main@dir/sub dir/test.txt");
  EXPECT_EQ(abfssAccountWithSpecialCharacters.scheme(), "abfss");
  EXPECT_EQ(
      abfssAccountWithSpecialCharacters.accountNameWithSuffix(),
      "test.dfs.core.windows.net");
  EXPECT_EQ(abfssAccountWithSpecialCharacters.accountName(), "test");
  EXPECT_EQ(
      abfssAccountWithSpecialCharacters.endpointSuffix(), "core.windows.net");
  EXPECT_EQ(abfssAccountWithSpecialCharacters.fileSystem(), "test");
  EXPECT_EQ(
      abfssAccountWithSpecialCharacters.filePath(),
      "main@dir/sub dir/test.txt");
  EXPECT_EQ(
      abfssAccountWithSpecialCharacters.credKey(),
      "fs.azure.account.key.test.dfs.core.windows.net");

  // china cloud
  auto abfsChinaCloudAccount =
      AbfsAccount("abfs://test@test.dfs.core.chinacloudapi.cn/test");
  EXPECT_EQ(abfsChinaCloudAccount.scheme(), "abfs");
  EXPECT_EQ(
      abfsChinaCloudAccount.accountNameWithSuffix(),
      "test.dfs.core.chinacloudapi.cn");
  EXPECT_EQ(abfsChinaCloudAccount.accountName(), "test");
  EXPECT_EQ(abfsChinaCloudAccount.endpointSuffix(), "core.chinacloudapi.cn");
  EXPECT_EQ(abfsChinaCloudAccount.fileSystem(), "test");
  EXPECT_EQ(abfsChinaCloudAccount.filePath(), "test");
  EXPECT_EQ(
      abfsChinaCloudAccount.credKey(),
      "fs.azure.account.key.test.dfs.core.chinacloudapi.cn");

  // us gov cloud
  auto abfsUsGovCloudAccount =
      AbfsAccount("abfs://test@test.dfs.core.usgovcloudapi.net/test");
  EXPECT_EQ(abfsUsGovCloudAccount.scheme(), "abfs");
  EXPECT_EQ(
      abfsUsGovCloudAccount.accountNameWithSuffix(),
      "test.dfs.core.usgovcloudapi.net");
  EXPECT_EQ(abfsUsGovCloudAccount.accountName(), "test");
  EXPECT_EQ(abfsUsGovCloudAccount.endpointSuffix(), "core.usgovcloudapi.net");
  EXPECT_EQ(abfsUsGovCloudAccount.fileSystem(), "test");
  EXPECT_EQ(abfsUsGovCloudAccount.filePath(), "test");
  EXPECT_EQ(
      abfsUsGovCloudAccount.credKey(),
      "fs.azure.account.key.test.dfs.core.usgovcloudapi.net");

  // germany cloud
  auto abfsGermanyCloudAccount =
      AbfsAccount("abfs://test@test.dfs.core.cloudapi.de/test");
  EXPECT_EQ(abfsGermanyCloudAccount.scheme(), "abfs");
  EXPECT_EQ(
      abfsGermanyCloudAccount.accountNameWithSuffix(),
      "test.dfs.core.cloudapi.de");
  EXPECT_EQ(abfsGermanyCloudAccount.accountName(), "test");
  EXPECT_EQ(abfsGermanyCloudAccount.endpointSuffix(), "core.cloudapi.de");
  EXPECT_EQ(abfsGermanyCloudAccount.fileSystem(), "test");
  EXPECT_EQ(abfsGermanyCloudAccount.filePath(), "test");
  EXPECT_EQ(
      abfsGermanyCloudAccount.credKey(),
      "fs.azure.account.key.test.dfs.core.cloudapi.de");
}

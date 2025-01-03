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

TEST(AbfsConfigTest, authType) {
  const config::ConfigBase config(
      {{"fs.azure.account.auth.type.efg.dfs.core.windows.net", "Custom"},
       {"fs.azure.account.key.efg.dfs.core.windows.net", "456"}},
      false);
  VELOX_ASSERT_USER_THROW(
      std::make_unique<AbfsConfig>(
          "abfss://foo@efg.dfs.core.windows.net/test.txt", config),
      "Unsupported auth type Custom, supported auth types are SharedKey, OAuth and SAS.");
}

TEST(AbfsConfigTest, clientSecretOAuth) {
  const config::ConfigBase config(
      {{"fs.azure.account.auth.type.efg.dfs.core.windows.net", "OAuth"},
       {"fs.azure.account.auth.type.bar1.dfs.core.windows.net", "OAuth"},
       {"fs.azure.account.auth.type.bar2.dfs.core.windows.net", "OAuth"},
       {"fs.azure.account.auth.type.bar3.dfs.core.windows.net", "OAuth"},
       {"fs.azure.account.oauth2.client.id.efg.dfs.core.windows.net", "test"},
       {"fs.azure.account.oauth2.client.secret.efg.dfs.core.windows.net",
        "test"},
       {"fs.azure.account.oauth2.client.endpoint.efg.dfs.core.windows.net",
        "https://login.microsoftonline.com/{TENANTID}/oauth2/token"},
       {"fs.azure.account.oauth2.client.id.bar2.dfs.core.windows.net", "test"},
       {"fs.azure.account.oauth2.client.id.bar3.dfs.core.windows.net", "test"},
       {"fs.azure.account.oauth2.client.secret.bar3.dfs.core.windows.net",
        "test"}},
      false);
  VELOX_ASSERT_USER_THROW(
      std::make_unique<AbfsConfig>(
          "abfss://foo@bar1.dfs.core.windows.net/test.txt", config),
      "Config fs.azure.account.oauth2.client.id.bar1.dfs.core.windows.net not found");
  VELOX_ASSERT_USER_THROW(
      std::make_unique<AbfsConfig>(
          "abfss://foo@bar2.dfs.core.windows.net/test.txt", config),
      "Config fs.azure.account.oauth2.client.secret.bar2.dfs.core.windows.net not found");
  VELOX_ASSERT_USER_THROW(
      std::make_unique<AbfsConfig>(
          "abfss://foo@bar3.dfs.core.windows.net/test.txt", config),
      "Config fs.azure.account.oauth2.client.endpoint.bar3.dfs.core.windows.net not found");
  auto abfsConfig =
      AbfsConfig("abfss://abc@efg.dfs.core.windows.net/file/test.txt", config);
  EXPECT_EQ(abfsConfig.tenentId(), "{TENANTID}");
  EXPECT_EQ(abfsConfig.authorityHost(), "https://login.microsoftonline.com/");
  auto readClient = abfsConfig.getReadFileClient();
  EXPECT_EQ(
      readClient->GetUrl(),
      "https://efg.blob.core.windows.net/abc/file/test.txt");
  auto writeClient = abfsConfig.getWriteFileClient();
  // GetUrl retrieves the value from the internal blob client, which represents
  // the blob's path as well.
  EXPECT_EQ(
      writeClient->getUrl(),
      "https://efg.blob.core.windows.net/abc/file/test.txt");
}

TEST(AbfsConfigTest, sasToken) {
  const config::ConfigBase config(
      {{"fs.azure.account.auth.type.efg.dfs.core.windows.net", "SAS"},
       {"fs.azure.account.auth.type.bar.dfs.core.windows.net", "SAS"},
       {"fs.azure.sas.fixed.token.bar.dfs.core.windows.net", "sas=test"}},
      false);
  VELOX_ASSERT_USER_THROW(
      std::make_unique<AbfsConfig>(
          "abfss://foo@efg.dfs.core.windows.net/test.txt", config),
      "Config fs.azure.sas.fixed.token.efg.dfs.core.windows.net not found");
  auto abfsConfig =
      AbfsConfig("abfs://abc@bar.dfs.core.windows.net/file", config);
  auto readClient = abfsConfig.getReadFileClient();
  EXPECT_EQ(
      readClient->GetUrl(),
      "http://bar.blob.core.windows.net/abc/file?sas=test");
  auto writeClient = abfsConfig.getWriteFileClient();
  // GetUrl retrieves the value from the internal blob client, which represents
  // the blob's path as well.
  EXPECT_EQ(
      writeClient->getUrl(),
      "http://bar.blob.core.windows.net/abc/file?sas=test");
}

TEST(AbfsConfigTest, sharedKey) {
  const config::ConfigBase config(
      {{"fs.azure.account.key.efg.dfs.core.windows.net", "123"},
       {"fs.azure.account.auth.type.efg.dfs.core.windows.net", "SharedKey"},
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

  VELOX_ASSERT_USER_THROW(
      std::make_unique<AbfsConfig>(
          "abfss://foo@otheraccount.dfs.core.windows.net/test.txt", config),
      "Config fs.azure.account.key.otheraccount.dfs.core.windows.net not found");
}

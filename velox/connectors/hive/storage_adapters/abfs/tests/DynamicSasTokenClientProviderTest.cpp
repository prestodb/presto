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

#include "velox/connectors/hive/storage_adapters/abfs/DynamicSasTokenClientProvider.h"
#include "velox/connectors/hive/storage_adapters/abfs/AzureClientProviderFactories.h"
#include "velox/connectors/hive/storage_adapters/abfs/RegisterAbfsFileSystem.h"

#include "gtest/gtest.h"

#include <azure/storage/blobs/blob_sas_builder.hpp>
#include <chrono>
#include <cstdint>

using namespace facebook::velox::filesystems;
using namespace facebook::velox;

namespace {

class MyDynamicAbfsSasTokenProvider : public SasTokenProvider {
 public:
  MyDynamicAbfsSasTokenProvider(int64_t expiration)
      : expirationSeconds_(expiration) {}

  std::string getSasToken(
      const std::string& fileSystem,
      const std::string& path,
      const std::string& operation) override {
    const auto lastSlash = path.find_last_of("/");
    const auto containerName = path.substr(0, lastSlash);
    const auto blobName = path.substr(lastSlash + 1);

    Azure::Storage::Sas::BlobSasBuilder sasBuilder;
    sasBuilder.ExpiresOn = Azure::DateTime::clock::now() +
        std::chrono::seconds(expirationSeconds_);
    sasBuilder.BlobContainerName = containerName;
    sasBuilder.BlobName = blobName;
    sasBuilder.Resource = Azure::Storage::Sas::BlobSasResource::Blob;
    sasBuilder.SetPermissions(
        Azure::Storage::Sas::BlobSasPermissions::Read &
        Azure::Storage::Sas::BlobSasPermissions::Write);

    std::string sasToken =
        sasBuilder.GenerateSasToken(Azure::Storage::StorageSharedKeyCredential(
            "test",
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="));

    // Remove the leading '?' from the SAS token.
    if (sasToken[0] == '?') {
      sasToken = sasToken.substr(1);
    }

    return sasToken;
  }

 private:
  int64_t expirationSeconds_;
};

} // namespace

TEST(DynamicSasTokenClientProviderTest, dynamicSasToken) {
  {
    const std::string account = "account1";
    const config::ConfigBase config(
        {{"fs.azure.account.auth.type.account1.dfs.core.windows.net", "SAS"},
         {"fs.azure.sas.token.renew.period.for.streams", "1"}},
        false);
    registerAzureClientProviderFactory(account, [](const std::string&) {
      auto sasTokenProvider =
          std::make_shared<MyDynamicAbfsSasTokenProvider>(3);
      return std::make_unique<DynamicSasTokenClientProvider>(sasTokenProvider);
    });

    auto abfsPath = std::make_shared<AbfsPath>(
        fmt::format("abfs://abc@{}.dfs.core.windows.net/file", account));
    auto readClient =
        AzureClientProviderFactories::getReadFileClient(abfsPath, config);
    auto writeClient =
        AzureClientProviderFactories::getWriteFileClient(abfsPath, config);

    auto readUrl = readClient->getUrl();
    auto writeUrl = writeClient->getUrl();

    // Let the current time pass 3 seconds to ensure the SAS token is expired.
    std::this_thread::sleep_for(std::chrono::seconds(3)); // NOLINT

    auto newReadUrl = readClient->getUrl();
    ASSERT_NE(readUrl, newReadUrl);
    // The SAS token should be reused.
    ASSERT_EQ(newReadUrl, readClient->getUrl());

    auto newWriteUrl = writeClient->getUrl();
    ASSERT_NE(writeUrl, newWriteUrl);
    // The SAS token should be reused.
    ASSERT_EQ(newWriteUrl, writeClient->getUrl());
  }

  {
    // SAS token expired by setting the renewal period to 120 seconds.
    const std::string account = "account2";
    const config::ConfigBase config(
        {{"fs.azure.account.auth.type.account2.dfs.core.windows.net", "SAS"},
         {"fs.azure.sas.token.renew.period.for.streams", "120"}},
        false);
    registerAzureClientProviderFactory(account, [](const std::string&) {
      auto sasTokenProvider =
          std::make_shared<MyDynamicAbfsSasTokenProvider>(60);
      return std::make_unique<DynamicSasTokenClientProvider>(sasTokenProvider);
    });

    auto abfsPath = std::make_shared<AbfsPath>(
        fmt::format("abfs://abc@{}.dfs.core.windows.net/file", account));
    auto readClient =
        AzureClientProviderFactories::getReadFileClient(abfsPath, config);
    auto writeClient =
        AzureClientProviderFactories::getWriteFileClient(abfsPath, config);

    auto readUrl = readClient->getUrl();
    auto writeUrl = writeClient->getUrl();

    // Let the current time pass 3 seconds to ensure the timestamp in the SAS
    // token is updated.
    std::this_thread::sleep_for(std::chrono::seconds(3)); // NOLINT

    // Sas token should be renewed because the time left is less than the
    // renewal period.
    ASSERT_NE(readUrl, readClient->getUrl());
    ASSERT_NE(writeUrl, writeClient->getUrl());
  }
}

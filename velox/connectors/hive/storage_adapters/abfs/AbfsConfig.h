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

#include <azure/core/credentials/credentials.hpp>
#include <azure/storage/blobs/blob_client.hpp>
#include <azure/storage/files/datalake.hpp>
#include <folly/hash/Hash.h>
#include <string>
#include "velox/connectors/hive/storage_adapters/abfs/AzureDataLakeFileClient.h"

using namespace Azure::Storage::Blobs;
using namespace Azure::Storage::Files::DataLake;

namespace facebook::velox::config {
class ConfigBase;
}

namespace facebook::velox::filesystems {

// This is used to specify the Azurite endpoint in testing.
static constexpr const char* kAzureBlobEndpoint{"fs.azure.blob-endpoint"};

// The authentication mechanism is set in `fs.azure.account.auth.type` (or the
// account specific variant). The supported values are SharedKey, OAuth and SAS.
static constexpr const char* kAzureAccountAuthType =
    "fs.azure.account.auth.type";

static constexpr const char* kAzureAccountKey = "fs.azure.account.key";

static constexpr const char* kAzureSASKey = "fs.azure.sas.fixed.token";

static constexpr const char* kAzureAccountOAuth2ClientId =
    "fs.azure.account.oauth2.client.id";

static constexpr const char* kAzureAccountOAuth2ClientSecret =
    "fs.azure.account.oauth2.client.secret";

// Token end point, this can be found through Azure portal. For example:
// https://login.microsoftonline.com/{TENANTID}/oauth2/token
static constexpr const char* kAzureAccountOAuth2ClientEndpoint =
    "fs.azure.account.oauth2.client.endpoint";

static constexpr const char* kAzureSharedKeyAuthType = "SharedKey";

static constexpr const char* kAzureOAuthAuthType = "OAuth";

static constexpr const char* kAzureSASAuthType = "SAS";

class AbfsConfig {
 public:
  explicit AbfsConfig(std::string_view path, const config::ConfigBase& config);

  std::unique_ptr<BlobClient> getReadFileClient();

  std::unique_ptr<AzureDataLakeFileClient> getWriteFileClient();

  std::string filePath() const {
    return filePath_;
  }

  /// Test only.
  std::string fileSystem() const {
    return fileSystem_;
  }

  /// Test only.
  std::string connectionString() const {
    return connectionString_;
  }

  /// Test only.
  std::string tenentId() const {
    return tenentId_;
  }

  /// Test only.
  std::string authorityHost() const {
    return authorityHost_;
  }

  /// Test only.
  static void setUpTestWriteClient(
      std::function<std::unique_ptr<AzureDataLakeFileClient>()> testClientFn) {
    testWriteClientFn_ = testClientFn;
  }

  /// Test only.
  static void tearDownTestWriteClient() {
    testWriteClientFn_ = nullptr;
  }

 private:
  std::string getUrl(bool withblobSuffix);

  std::string authType_;

  // Container name is called FileSystem in some Azure API.
  std::string fileSystem_;
  std::string filePath_;
  std::string connectionString_;

  bool isHttps_;
  std::string accountNameWithSuffix_;

  std::string sas_;

  std::string tenentId_;
  std::string authorityHost_;
  std::shared_ptr<Azure::Core::Credentials::TokenCredential> tokenCredential_;

  static std::function<std::unique_ptr<AzureDataLakeFileClient>()>
      testWriteClientFn_;
};

} // namespace facebook::velox::filesystems

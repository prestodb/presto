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

#include <azure/storage/files/datalake.hpp>
#include <folly/hash/Hash.h>
#include <string>

using namespace Azure::Storage::Blobs;
using namespace Azure::Storage::Files::DataLake;

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

// For performance, re - use SAS tokens until the expiry is within this number
// of seconds.
static constexpr const char* kAzureSasTokenRenewPeriod =
    "fs.azure.sas.token.renew.period.for.streams";

// Helper class to parse and extract information from a given ABFS path.
class AbfsPath {
 public:
  AbfsPath(std::string_view path);

  bool isHttps() const {
    return isHttps_;
  }

  std::string fileSystem() const {
    return fileSystem_;
  }

  std::string filePath() const {
    return filePath_;
  }

  std::string accountName() const {
    return accountName_;
  }

  std::string accountNameWithSuffix() const {
    return accountNameWithSuffix_;
  }

  std::string getUrl(bool withblobSuffix) const;

 private:
  bool isHttps_;

  // Container name is called FileSystem in some Azure API.
  std::string fileSystem_;
  std::string filePath_;
  std::string connectionString_;

  std::string accountName_;
  std::string accountNameWithSuffix_;
};

} // namespace facebook::velox::filesystems

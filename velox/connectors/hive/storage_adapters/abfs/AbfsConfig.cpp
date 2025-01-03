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

#include "velox/connectors/hive/storage_adapters/abfs/AbfsConfig.h"

#include "velox/common/config/Config.h"
#include "velox/connectors/hive/storage_adapters/abfs/AbfsUtil.h"

#include <azure/identity/client_secret_credential.hpp>

namespace facebook::velox::filesystems {

std::function<std::unique_ptr<AzureDataLakeFileClient>()>
    AbfsConfig::testWriteClientFn_;

class DataLakeFileClientWrapper final : public AzureDataLakeFileClient {
 public:
  DataLakeFileClientWrapper(std::unique_ptr<DataLakeFileClient> client)
      : client_(std::move(client)) {}

  void create() override {
    client_->Create();
  }

  Azure::Storage::Files::DataLake::Models::PathProperties getProperties()
      override {
    return client_->GetProperties().Value;
  }

  void append(const uint8_t* buffer, size_t size, uint64_t offset) override {
    auto bodyStream = Azure::Core::IO::MemoryBodyStream(buffer, size);
    client_->Append(bodyStream, offset);
  }

  void flush(uint64_t position) override {
    client_->Flush(position);
  }

  void close() override {
    // do nothing.
  }

  std::string getUrl() const override {
    return client_->GetUrl();
  }

 private:
  const std::unique_ptr<DataLakeFileClient> client_;
};

AbfsConfig::AbfsConfig(
    std::string_view path,
    const config::ConfigBase& config) {
  std::string_view file;
  isHttps_ = true;
  if (path.find(kAbfssScheme) == 0) {
    file = path.substr(kAbfssScheme.size());
  } else if (path.find(kAbfsScheme) == 0) {
    file = path.substr(kAbfsScheme.size());
    isHttps_ = false;
  } else {
    VELOX_FAIL("Invalid ABFS Path {}", path);
  }

  auto firstAt = file.find_first_of("@");
  fileSystem_ = file.substr(0, firstAt);
  auto firstSep = file.find_first_of("/");
  filePath_ = file.substr(firstSep + 1);
  accountNameWithSuffix_ = file.substr(firstAt + 1, firstSep - firstAt - 1);

  auto authTypeKey =
      fmt::format("{}.{}", kAzureAccountAuthType, accountNameWithSuffix_);
  authType_ = kAzureSharedKeyAuthType;
  if (config.valueExists(authTypeKey)) {
    authType_ = config.get<std::string>(authTypeKey).value();
  }
  if (authType_ == kAzureSharedKeyAuthType) {
    auto credKey =
        fmt::format("{}.{}", kAzureAccountKey, accountNameWithSuffix_);
    VELOX_USER_CHECK(
        config.valueExists(credKey), "Config {} not found", credKey);
    auto firstDot = accountNameWithSuffix_.find_first_of(".");
    auto accountName = accountNameWithSuffix_.substr(0, firstDot);
    auto endpointSuffix = accountNameWithSuffix_.substr(firstDot + 5);
    std::stringstream ss;
    ss << "DefaultEndpointsProtocol=" << (isHttps_ ? "https" : "http");
    ss << ";AccountName=" << accountName;
    ss << ";AccountKey=" << config.get<std::string>(credKey).value();
    ss << ";EndpointSuffix=" << endpointSuffix;

    if (config.valueExists(kAzureBlobEndpoint)) {
      ss << ";BlobEndpoint="
         << config.get<std::string>(kAzureBlobEndpoint).value();
    }
    ss << ";";
    connectionString_ = ss.str();
  } else if (authType_ == kAzureOAuthAuthType) {
    auto clientIdKey = fmt::format(
        "{}.{}", kAzureAccountOAuth2ClientId, accountNameWithSuffix_);
    auto clientSecretKey = fmt::format(
        "{}.{}", kAzureAccountOAuth2ClientSecret, accountNameWithSuffix_);
    auto clientEndpointKey = fmt::format(
        "{}.{}", kAzureAccountOAuth2ClientEndpoint, accountNameWithSuffix_);
    VELOX_USER_CHECK(
        config.valueExists(clientIdKey), "Config {} not found", clientIdKey);
    VELOX_USER_CHECK(
        config.valueExists(clientSecretKey),
        "Config {} not found",
        clientSecretKey);
    VELOX_USER_CHECK(
        config.valueExists(clientEndpointKey),
        "Config {} not found",
        clientEndpointKey);
    auto clientEndpoint = config.get<std::string>(clientEndpointKey).value();
    auto firstSep = clientEndpoint.find_first_of("/", /* https:// */ 8);
    authorityHost_ = clientEndpoint.substr(0, firstSep + 1);
    auto sedondSep = clientEndpoint.find_first_of("/", firstSep + 1);
    tenentId_ = clientEndpoint.substr(firstSep + 1, sedondSep - firstSep - 1);
    Azure::Identity::ClientSecretCredentialOptions options;
    options.AuthorityHost = authorityHost_;
    tokenCredential_ =
        std::make_shared<Azure::Identity::ClientSecretCredential>(
            tenentId_,
            config.get<std::string>(clientIdKey).value(),
            config.get<std::string>(clientSecretKey).value(),
            options);
  } else if (authType_ == kAzureSASAuthType) {
    auto sasKey = fmt::format("{}.{}", kAzureSASKey, accountNameWithSuffix_);
    VELOX_USER_CHECK(config.valueExists(sasKey), "Config {} not found", sasKey);
    sas_ = config.get<std::string>(sasKey).value();
  } else {
    VELOX_USER_FAIL(
        "Unsupported auth type {}, supported auth types are SharedKey, OAuth and SAS.",
        authType_);
  }
}

std::unique_ptr<BlobClient> AbfsConfig::getReadFileClient() {
  if (authType_ == kAzureSASAuthType) {
    auto url = getUrl(true);
    return std::make_unique<BlobClient>(fmt::format("{}?{}", url, sas_));
  } else if (authType_ == kAzureOAuthAuthType) {
    auto url = getUrl(true);
    return std::make_unique<BlobClient>(url, tokenCredential_);
  } else {
    return std::make_unique<BlobClient>(BlobClient::CreateFromConnectionString(
        connectionString_, fileSystem_, filePath_));
  }
}

std::unique_ptr<AzureDataLakeFileClient> AbfsConfig::getWriteFileClient() {
  if (testWriteClientFn_) {
    return testWriteClientFn_();
  }
  std::unique_ptr<DataLakeFileClient> client;
  if (authType_ == kAzureSASAuthType) {
    auto url = getUrl(false);
    client =
        std::make_unique<DataLakeFileClient>(fmt::format("{}?{}", url, sas_));
  } else if (authType_ == kAzureOAuthAuthType) {
    auto url = getUrl(false);
    client = std::make_unique<DataLakeFileClient>(url, tokenCredential_);
  } else {
    client = std::make_unique<DataLakeFileClient>(
        DataLakeFileClient::CreateFromConnectionString(
            connectionString_, fileSystem_, filePath_));
  }
  return std::make_unique<DataLakeFileClientWrapper>(std::move(client));
}

std::string AbfsConfig::getUrl(bool withblobSuffix) {
  std::string accountNameWithSuffixForUrl(accountNameWithSuffix_);
  if (withblobSuffix) {
    // We should use correct suffix for blob client.
    size_t start_pos = accountNameWithSuffixForUrl.find("dfs");
    if (start_pos != std::string::npos) {
      accountNameWithSuffixForUrl.replace(start_pos, 3, "blob");
    }
  }
  return fmt::format(
      "{}{}/{}/{}",
      isHttps_ ? "https://" : "http://",
      accountNameWithSuffixForUrl,
      fileSystem_,
      filePath_);
}

} // namespace facebook::velox::filesystems

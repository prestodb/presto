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

#include <azure/core/datetime.hpp>

namespace facebook::velox::filesystems {

namespace {

constexpr int64_t kDefaultSasTokenRenewPeriod = 120; // in seconds

Azure::DateTime getExpiry(const std::string_view& token) {
  if (token.empty()) {
    return Azure::DateTime::clock::time_point::min();
  }

  static constexpr std::string_view kSignedExpiry{"se="};
  static constexpr int32_t kSignedExpiryLen = 3;

  auto start = token.find(kSignedExpiry);
  if (start == std::string::npos) {
    return Azure::DateTime::clock::time_point::min();
  }
  start += kSignedExpiryLen;

  auto end = token.find("&", start);
  auto seValue = (end == std::string::npos)
      ? std::string(token.substr(start))
      : std::string(token.substr(start, end - start));

  seValue = Azure::Core::Url::Decode(seValue);
  auto seDate =
      Azure::DateTime::Parse(seValue, Azure::DateTime::DateFormat::Rfc3339);

  static constexpr std::string_view kSignedKeyExpiry = "ske=";
  static constexpr int32_t kSignedKeyExpiryLen = 4;

  start = token.find(kSignedKeyExpiry);
  if (start == std::string::npos) {
    return seDate;
  }
  start += kSignedKeyExpiryLen;

  end = token.find("&", start);
  auto skeValue = (end == std::string::npos)
      ? std::string(token.substr(start))
      : std::string(token.substr(start, end - start));

  skeValue = Azure::Core::Url::Decode(skeValue);
  auto skeDate =
      Azure::DateTime::Parse(skeValue, Azure::DateTime::DateFormat::Rfc3339);

  return std::min(skeDate, seDate);
}

bool isNearExpiry(Azure::DateTime expiration, int64_t minExpirationInSeconds) {
  if (expiration == Azure::DateTime::clock::time_point::min()) {
    return true;
  }
  auto remaining = std::chrono::duration_cast<std::chrono::seconds>(
                       expiration - Azure::DateTime::clock::now())
                       .count();
  return remaining <= minExpirationInSeconds;
}

class DynamicSasTokenDataLakeFileClient final : public AzureDataLakeFileClient {
 public:
  DynamicSasTokenDataLakeFileClient(
      const std::shared_ptr<AbfsPath>& abfsPath,
      const std::shared_ptr<SasTokenProvider>& sasKeyGenerator,
      int64_t sasTokenRenewPeriod)
      : abfsPath_(abfsPath),
        sasKeyGenerator_(sasKeyGenerator),
        sasTokenRenewPeriod_(sasTokenRenewPeriod) {}

  void create() override {
    getWriteClient()->Create();
  }

  Azure::Storage::Files::DataLake::Models::PathProperties getProperties()
      override {
    return getReadClient()->GetProperties().Value;
  }

  void append(const uint8_t* buffer, size_t size, uint64_t offset) override {
    auto bodyStream = Azure::Core::IO::MemoryBodyStream(buffer, size);
    getWriteClient()->Append(bodyStream, offset);
  }

  void flush(uint64_t position) override {
    getWriteClient()->Flush(position);
  }

  void close() override {}

  std::string getUrl() override {
    return getWriteClient()->GetUrl();
  }

 private:
  std::shared_ptr<AbfsPath> abfsPath_;
  std::shared_ptr<SasTokenProvider> sasKeyGenerator_;
  int64_t sasTokenRenewPeriod_;

  std::unique_ptr<DataLakeFileClient> writeClient_{nullptr};
  Azure::DateTime writeSasExpiration_{
      Azure::DateTime::clock::time_point::min()};

  std::unique_ptr<DataLakeFileClient> readClient_{nullptr};
  Azure::DateTime readSasExpiration_{Azure::DateTime::clock::time_point::min()};

  DataLakeFileClient* getWriteClient() {
    if (writeClient_ == nullptr ||
        isNearExpiry(writeSasExpiration_, sasTokenRenewPeriod_)) {
      const auto& sas = sasKeyGenerator_->getSasToken(
          abfsPath_->fileSystem(), abfsPath_->filePath(), kAbfsWriteOperation);
      writeSasExpiration_ = getExpiry(sas);
      writeClient_ = std::make_unique<DataLakeFileClient>(
          fmt::format("{}?{}", abfsPath_->getUrl(false), sas));
    }
    return writeClient_.get();
  }

  DataLakeFileClient* getReadClient() {
    if (readClient_ == nullptr ||
        isNearExpiry(readSasExpiration_, sasTokenRenewPeriod_)) {
      const auto& sas = sasKeyGenerator_->getSasToken(
          abfsPath_->fileSystem(), abfsPath_->filePath(), kAbfsReadOperation);
      readSasExpiration_ = getExpiry(sas);
      readClient_ = std::make_unique<DataLakeFileClient>(
          fmt::format("{}?{}", abfsPath_->getUrl(false), sas));
    }
    return readClient_.get();
  }
};

class DynamicSasTokenBlobClient : public AzureBlobClient {
 public:
  DynamicSasTokenBlobClient(
      const std::shared_ptr<AbfsPath>& abfsPath,
      const std::shared_ptr<SasTokenProvider>& sasTokenProvider,
      int64_t sasTokenRenewPeriod)
      : abfsPath_(abfsPath),
        sasTokenProvider_(sasTokenProvider),
        sasTokenRenewPeriod_(sasTokenRenewPeriod) {}

  Azure::Response<Azure::Storage::Blobs::Models::BlobProperties> getProperties()
      override {
    return getBlobClient()->GetProperties();
  }

  Azure::Response<Azure::Storage::Blobs::Models::DownloadBlobResult> download(
      const Azure::Storage::Blobs::DownloadBlobOptions& options) override {
    return getBlobClient()->Download(options);
  }

  std::string getUrl() override {
    return getBlobClient()->GetUrl();
  }

 private:
  std::shared_ptr<AbfsPath> abfsPath_;
  std::shared_ptr<SasTokenProvider> sasTokenProvider_;
  int64_t sasTokenRenewPeriod_;

  std::unique_ptr<Azure::Storage::Blobs::BlobClient> blobClient_{nullptr};
  Azure::DateTime sasExpiration_{Azure::DateTime::clock::time_point::min()};

  BlobClient* getBlobClient() {
    if (blobClient_ == nullptr ||
        isNearExpiry(sasExpiration_, sasTokenRenewPeriod_)) {
      const auto& sas = sasTokenProvider_->getSasToken(
          abfsPath_->fileSystem(), abfsPath_->filePath(), kAbfsReadOperation);
      sasExpiration_ = getExpiry(sas);
      blobClient_ = std::make_unique<Azure::Storage::Blobs::BlobClient>(
          fmt::format("{}?{}", abfsPath_->getUrl(true), sas));
    }
    return blobClient_.get();
  }
};

} // namespace

DynamicSasTokenClientProvider::DynamicSasTokenClientProvider(
    const std::shared_ptr<SasTokenProvider>& sasTokenProvider)
    : AzureClientProvider(), sasTokenProvider_(sasTokenProvider) {}

void DynamicSasTokenClientProvider::init(const config::ConfigBase& config) {
  sasTokenRenewPeriod_ = config.get<int64_t>(
      kAzureSasTokenRenewPeriod, kDefaultSasTokenRenewPeriod);
}

std::unique_ptr<AzureBlobClient>
DynamicSasTokenClientProvider::getReadFileClient(
    const std::shared_ptr<AbfsPath>& abfsPath,
    const config::ConfigBase& config) {
  init(config);
  return std::make_unique<DynamicSasTokenBlobClient>(
      abfsPath, sasTokenProvider_, sasTokenRenewPeriod_);
}

std::unique_ptr<AzureDataLakeFileClient>
DynamicSasTokenClientProvider::getWriteFileClient(
    const std::shared_ptr<AbfsPath>& abfsPath,
    const config::ConfigBase& config) {
  init(config);
  return std::make_unique<DynamicSasTokenDataLakeFileClient>(
      abfsPath, sasTokenProvider_, sasTokenRenewPeriod_);
}
} // namespace facebook::velox::filesystems

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

#include "velox/connectors/hive/storage_adapters/abfs/AzureBlobClient.h"
#include "velox/connectors/hive/storage_adapters/abfs/AzureClientProvider.h"
#include "velox/connectors/hive/storage_adapters/abfs/AzureClientProviderFactories.h"
#include "velox/connectors/hive/storage_adapters/abfs/AzureDataLakeFileClient.h"

namespace facebook::velox::filesystems {

/// SAS permissions reference:
/// https://learn.microsoft.com/en-us/rest/api/storageservices/create-service-sas#permissions-for-a-directory-container-or-blob
///
/// ReadClient uses "read" permission for Download and GetProperties.
/// WriteClient uses "read" permission for GetProperties, and "write" permission
/// for other operations.
static const std::string kAbfsReadOperation{"read"};
static const std::string kAbfsWriteOperation{"write"};

/// Interface for providing SAS tokens for ABFS file system operations.
/// Adapted from the Hadoop Azure implementation:
/// org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider
class SasTokenProvider {
 public:
  virtual ~SasTokenProvider() = default;

  virtual std::string getSasToken(
      const std::string& fileSystem,
      const std::string& path,
      const std::string& operation) = 0;
};

/// Client provider that dynamically refreshes SAS tokens based on the
/// expiration time of the token. A SasTokenProvider for retrieving SAS tokens
/// must be provided to this class. Example for generating the SAS token can be
/// found in:
/// https://github.com/Azure/azure-sdk-for-cpp/blob/3d917e7c178f0a49b189395a907180084857cc70/sdk/storage/azure-storage-blobs/samples/blob_sas.cpp
class DynamicSasTokenClientProvider : public AzureClientProvider {
 public:
  explicit DynamicSasTokenClientProvider(
      const std::shared_ptr<SasTokenProvider>& sasTokenProvider);

  std::unique_ptr<AzureBlobClient> getReadFileClient(
      const std::shared_ptr<AbfsPath>& abfsPath,
      const config::ConfigBase& config) override;

  std::unique_ptr<AzureDataLakeFileClient> getWriteFileClient(
      const std::shared_ptr<AbfsPath>& abfsPath,
      const config::ConfigBase& config) override;

 private:
  void init(const config::ConfigBase& config);

  std::shared_ptr<SasTokenProvider> sasTokenProvider_;
  int64_t sasTokenRenewPeriod_;
};

} // namespace facebook::velox::filesystems

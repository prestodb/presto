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

#include "velox/common/config/Config.h"
#include "velox/connectors/hive/storage_adapters/abfs/AbfsPath.h"
#include "velox/connectors/hive/storage_adapters/abfs/AzureBlobClient.h"
#include "velox/connectors/hive/storage_adapters/abfs/AzureClientProvider.h"
#include "velox/connectors/hive/storage_adapters/abfs/AzureDataLakeFileClient.h"

namespace facebook::velox::filesystems {

using AzureClientProviderFactory =
    std::function<std::unique_ptr<AzureClientProvider>(
        const std::string& account)>;

/// Handles the registration of Azure client providers and the creation of
/// AzureBlobClient and AzureDataLakeFileClient instances.
class AzureClientProviderFactories {
 public:
  /// Registers a factory for creating AzureClientProvider instances.
  /// Any existing factory registered for the specified account will be
  /// overwritten by recalling this method with the same account name.
  static void registerFactory(
      const std::string& account,
      const AzureClientProviderFactory& factory);

  /// Get the registered AzureClientProviderFactory for the specified
  /// account. Throws exception if no factory is registered for the account.
  static AzureClientProviderFactory getClientFactory(
      const std::string& account);

  /// Uses the registered AzureClientProviderFactory to create an
  /// AzureBlobClient for file read operations. Throws exception if no factory
  /// is registered for the account specified in `abfsPath`.
  static std::unique_ptr<AzureBlobClient> getReadFileClient(
      const std::shared_ptr<AbfsPath>& abfsPath,
      const config::ConfigBase& config);

  /// Uses the registered AzureClientProviderFactory to create an
  /// AzureDataLakeFileClient for file write operations. Throws exception if no
  /// factory is registered for the account specified in `abfsPath`.
  static std::unique_ptr<AzureDataLakeFileClient> getWriteFileClient(
      const std::shared_ptr<AbfsPath>& abfsPath,
      const config::ConfigBase& config);
};

} // namespace facebook::velox::filesystems

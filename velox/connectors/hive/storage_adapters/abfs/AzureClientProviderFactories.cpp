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

#include "velox/connectors/hive/storage_adapters/abfs/AzureClientProviderFactories.h"
#include "velox/connectors/hive/storage_adapters/abfs/AzureClientProviderImpl.h"

#include <folly/Synchronized.h>

namespace facebook::velox::filesystems {

namespace {

folly::Synchronized<
    std::unordered_map<std::string, AzureClientProviderFactory>>&
azureClientFactoryRegistry() {
  static folly::Synchronized<
      std::unordered_map<std::string, AzureClientProviderFactory>>
      factories;
  return factories;
}

} // namespace

void AzureClientProviderFactories::registerFactory(
    const std::string& account,
    const AzureClientProviderFactory& factory) {
  azureClientFactoryRegistry().withWLock([&](auto& factories) {
    auto [_, inserted] = factories.insert_or_assign(account, factory);
    LOG_IF(INFO, !inserted) << "AzureClientProviderFactory for account '"
                            << account << "' has been overridden.";
  });
}

AzureClientProviderFactory AzureClientProviderFactories::getClientFactory(
    const std::string& account) {
  return azureClientFactoryRegistry().withRLock(
      [&](const auto& factories) -> AzureClientProviderFactory {
        if (auto it = factories.find(account); it != factories.end()) {
          return it->second;
        }
        VELOX_USER_FAIL(
            "No AzureClientProviderFactory registered for account '{}'."
            "Please use `registerAzureClientProvider` or "
            "`registerAzureClientProviderFactory` to register a factory for "
            "the account before using it.",
            account);
      });
}

std::unique_ptr<AzureBlobClient>
AzureClientProviderFactories::getReadFileClient(
    const std::shared_ptr<AbfsPath>& abfsPath,
    const config::ConfigBase& config) {
  auto factory = getClientFactory(abfsPath->accountName());
  return factory(abfsPath->accountName())->getReadFileClient(abfsPath, config);
}

std::unique_ptr<AzureDataLakeFileClient>
AzureClientProviderFactories::getWriteFileClient(
    const std::shared_ptr<AbfsPath>& abfsPath,
    const config::ConfigBase& config) {
  auto factory = getClientFactory(abfsPath->accountName());
  return factory(abfsPath->accountName())->getWriteFileClient(abfsPath, config);
}

} // namespace facebook::velox::filesystems

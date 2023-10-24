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

namespace facebook::velox::filesystems::abfs {
AbfsAccount::AbfsAccount(const std::string path) {
  auto file = std::string("");
  if (path.find(kAbfssScheme) == 0) {
    file = std::string(path.substr(8));
    scheme_ = kAbfssScheme.substr(0, 5);
  } else {
    file = std::string(path.substr(7));
    scheme_ = kAbfsScheme.substr(0, 4);
  }

  auto firstAt = file.find_first_of("@");
  fileSystem_ = std::string(file.substr(0, firstAt));
  auto firstSep = file.find_first_of("/");
  filePath_ = std::string(file.substr(firstSep + 1));

  accountNameWithSuffix_ = file.substr(firstAt + 1, firstSep - firstAt - 1);
  auto firstDot = accountNameWithSuffix_.find_first_of(".");
  accountName_ = accountNameWithSuffix_.substr(0, firstDot);
  endpointSuffix_ = accountNameWithSuffix_.substr(firstDot + 5);
  credKey_ = fmt::format("fs.azure.account.key.{}", accountNameWithSuffix_);
}

const std::string AbfsAccount::accountNameWithSuffix() const {
  return accountNameWithSuffix_;
}

const std::string AbfsAccount::scheme() const {
  return scheme_;
}

const std::string AbfsAccount::accountName() const {
  return accountName_;
}

const std::string AbfsAccount::endpointSuffix() const {
  return endpointSuffix_;
}

const std::string AbfsAccount::fileSystem() const {
  return fileSystem_;
}

const std::string AbfsAccount::filePath() const {
  return filePath_;
}

const std::string AbfsAccount::credKey() const {
  return credKey_;
}

const std::string AbfsAccount::connectionString(
    const std::string accountKey) const {
  return fmt::format(
      "DefaultEndpointsProtocol=https;AccountName={};AccountKey={};EndpointSuffix={}",
      accountName(),
      accountKey,
      endpointSuffix());
}
} // namespace facebook::velox::filesystems::abfs

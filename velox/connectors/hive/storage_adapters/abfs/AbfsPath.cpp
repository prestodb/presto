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

#include "velox/connectors/hive/storage_adapters/abfs/AbfsPath.h"
#include "velox/connectors/hive/storage_adapters/abfs/AbfsUtil.h"

namespace facebook::velox::filesystems {

AbfsPath::AbfsPath(std::string_view path) {
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
  auto firstDot = accountNameWithSuffix_.find_first_of(".");
  accountName_ = accountNameWithSuffix_.substr(0, firstDot);
}

std::string AbfsPath::getUrl(bool withblobSuffix) const {
  std::string accountNameWithSuffixForUrl(accountNameWithSuffix_);
  if (withblobSuffix) {
    // We should use correct suffix for blob client.
    size_t startPos = accountNameWithSuffixForUrl.find("dfs");
    if (startPos != std::string::npos) {
      accountNameWithSuffixForUrl.replace(startPos, 3, "blob");
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

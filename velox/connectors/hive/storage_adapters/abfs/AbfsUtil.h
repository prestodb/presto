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
#include <azure/storage/common/storage_exception.hpp>
#include "velox/common/file/File.h"

#include <fmt/format.h>
#include <regex>

namespace facebook::velox::filesystems::abfs {
namespace {
constexpr std::string_view kAbfsScheme{"abfs://"};
constexpr std::string_view kAbfssScheme{"abfss://"};
} // namespace

inline bool isAbfsFile(const std::string_view filename) {
  return filename.find(kAbfsScheme) == 0 || filename.find(kAbfssScheme) == 0;
}

class AbfsAccount {
 public:
  explicit AbfsAccount(const std::string path);

  const std::string accountNameWithSuffix() const;

  const std::string scheme() const;

  const std::string accountName() const;

  const std::string endpointSuffix() const;

  const std::string fileSystem() const;

  const std::string filePath() const;

  const std::string credKey() const;

  const std::string connectionString(const std::string accountKey) const;

 private:
  std::string scheme_;
  std::string accountName_;
  std::string endpointSuffix_;
  std::string accountNameWithSuffix_;
  std::string fileSystem_;
  std::string filePath_;
  std::string path_;
  std::string credKey_;
};

inline const std::string throwStorageExceptionWithOperationDetails(
    std::string operation,
    std::string path,
    Azure::Storage::StorageException& error) {
  const auto errMsg = fmt::format(
      "Operation '{}' to path '{}' encountered azure storage exception, Details: '{}'.",
      operation,
      path,
      error.what());
  if (error.StatusCode == Azure::Core::Http::HttpStatusCode::NotFound) {
    VELOX_FILE_NOT_FOUND_ERROR(errMsg);
  }
  VELOX_FAIL(errMsg);
}

} // namespace facebook::velox::filesystems::abfs

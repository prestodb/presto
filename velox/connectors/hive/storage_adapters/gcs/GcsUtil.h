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
#include <google/cloud/storage/client.h>
#include "velox/common/base/Exceptions.h"

namespace facebook::velox {

// Reference: https://github.com/apache/arrow/issues/29916
// Change the default upload buffer size. In general, sending larger buffers is
// more efficient with GCS, as each buffer requires a roundtrip to the service.
// With formatted output (when using `operator<<`), keeping a larger buffer in
// memory before uploading makes sense.  With unformatted output (the only
// choice given gcs::io::OutputStream's API) it is better to let the caller
// provide as large a buffer as they want. The GCS C++ client library will
// upload this buffer with zero copies if possible.
auto constexpr kUploadBufferSize = 256 * 1024;

constexpr const char* kGcsDefaultCacheKeyPrefix = "gcs-default-key";

namespace {
constexpr const char* kSep{"/"};
constexpr std::string_view kGcsScheme{"gs://"};

} // namespace

std::string getErrorStringFromGcsError(const google::cloud::StatusCode& error);

inline bool isGcsFile(const std::string_view filename) {
  return (filename.substr(0, kGcsScheme.size()) == kGcsScheme);
}

inline void setBucketAndKeyFromGcsPath(
    const std::string& path,
    std::string& bucket,
    std::string& key) {
  auto firstSep = path.find_first_of(kSep);
  bucket = path.substr(0, firstSep);
  key = path.substr(firstSep + 1);
}

inline std::string gcsURI(std::string_view bucket) {
  std::stringstream ss;
  ss << kGcsScheme << bucket;
  return ss.str();
}

inline std::string gcsURI(std::string_view bucket, std::string_view key) {
  std::stringstream ss;
  ss << kGcsScheme << bucket << kSep << key;
  return ss.str();
}

inline std::string gcsPath(const std::string_view& path) {
  // Remove the prefix gcs:// from the given path
  return std::string(path.substr(kGcsScheme.length()));
}

void checkGcsStatus(
    const google::cloud::Status outcome,
    const std::string_view& errorMsgPrefix,
    const std::string& bucket,
    const std::string& key);

} // namespace facebook::velox

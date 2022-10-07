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

// Implementation of S3 filesystem and file interface.
// We provide a registration method for read and write files so the appropriate
// type of file can be constructed based on a filename. See the
// (register|generate)ReadFile and (register|generate)WriteFile functions.

#pragma once

#include <aws/s3/S3Errors.h>
#include <aws/s3/model/HeadObjectResult.h>

#include "velox/common/base/Exceptions.h"

namespace facebook::velox {

namespace {
constexpr std::string_view kSep{"/"};
// AWS S3 EMRFS, Hadoop block storage filesystem on-top of Amazon S3 buckets.
constexpr std::string_view kS3Scheme{"s3://"};
// This should not be mixed with s3 nor the s3a.
// S3A Hadoop 3.x (previous connectors "s3" and "s3n" are deprecated).
constexpr std::string_view kS3aScheme{"s3a://"};
// OSS Alibaba support S3 format, usage only with SSL.
constexpr std::string_view kOssScheme{"oss://"};
// From AWS documentation
constexpr int kS3MaxKeySize{1024};
} // namespace

inline bool isS3AwsFile(const std::string_view filename) {
  return filename.substr(0, kS3Scheme.size()) == kS3Scheme;
}

inline bool isS3aFile(const std::string_view filename) {
  return filename.substr(0, kS3aScheme.size()) == kS3aScheme;
}

inline bool isOssFile(const std::string_view filename) {
  return filename.substr(0, kOssScheme.size()) == kOssScheme;
}

inline bool isS3File(const std::string_view filename) {
  // TODO: Each prefix should be implemented as its own filesystem.
  return isS3AwsFile(filename) || isS3aFile(filename) || isOssFile(filename);
}

inline void bucketAndKeyFromS3Path(
    const std::string& path,
    std::string& bucket,
    std::string& key) {
  auto firstSep = path.find_first_of(kSep);
  bucket = path.substr(0, firstSep);
  key = path.substr(firstSep + 1);
}

// TODO: Correctness check for bucket name.
// 1. Length between 3 and 63:
//    3 < length(bucket) < 63
// 2. Mandatory label notation - regexp:
//    regexp="(^[a-z0-9])([.-]?[a-z0-9]+){2,62}([/]?$)"
// 3. Disallowed IPv4 notation - regexp:
//    regexp="^((25[0-5]|(2[0-4]|1\d|[1-9]|)\d)\.?\b){4}[/]?$"
inline std::string s3URI(const std::string& bucket) {
  return std::string(kS3Scheme) + bucket;
}

inline std::string s3URI(const std::string& bucket, const std::string& key) {
  return s3URI(bucket) + "/" + key;
}

inline std::string s3Path(const std::string_view& path) {
  // Remove one of the prefixes 's3://', 'oss://', 's3a://' if any from the
  // given path.
  // TODO: Each prefix should be implemented as its own filesystem.
  if (isS3AwsFile(path)) {
    return std::string(path.substr(kS3Scheme.length()));
  } else if (isS3aFile(path)) {
    return std::string(path.substr(kS3aScheme.length()));
  } else if (isOssFile(path)) {
    return std::string(path.substr(kOssScheme.length()));
  }
  return std::string(path);
}

inline Aws::String awsString(const std::string& s) {
  return Aws::String(s.begin(), s.end());
}

std::string getErrorStringFromS3Error(
    const Aws::Client::AWSError<Aws::S3::S3Errors>& error);

namespace {
inline std::string getS3BackendService(
    const Aws::Http::HeaderValueCollection& headers) {
  const auto it = headers.find("server");
  if (it != headers.end()) {
    return it->second;
  }
  return "Unknown";
}
} // namespace

#define VELOX_CHECK_AWS_OUTCOME(outcome, errorMsgPrefix, bucket, key)                                          \
  {                                                                                                            \
    if (!outcome.IsSuccess()) {                                                                                \
      auto error = outcome.GetError();                                                                         \
      VELOX_FAIL(                                                                                              \
          "{} due to: '{}'. Path:'{}', SDK Error Type:{}, HTTP Status Code:{}, S3 Service:'{}', Message:'{}'", \
          errorMsgPrefix,                                                                                      \
          getErrorStringFromS3Error(error),                                                                    \
          s3URI(bucket, key),                                                                                  \
          error.GetErrorType(),                                                                                \
          error.GetResponseCode(),                                                                             \
          getS3BackendService(error.GetResponseHeaders()),                                                     \
          error.GetMessage());                                                                                 \
    }                                                                                                          \
  }

} // namespace facebook::velox

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

#include "velox/connectors/hive/storage_adapters/gcs/GcsUtil.h"

namespace facebook::velox {

std::string getErrorStringFromGcsError(const google::cloud::StatusCode& code) {
  using ::google::cloud::StatusCode;

  switch (code) {
    case StatusCode::kNotFound:
      return "Resource not found";
    case StatusCode::kPermissionDenied:
      return "Access denied";
    case StatusCode::kUnavailable:
      return "Service unavailable";

    default:
      return "Unknown error";
  }
}

void checkGcsStatus(
    const google::cloud::Status outcome,
    const std::string_view& errorMsgPrefix,
    const std::string& bucket,
    const std::string& key) {
  if (!outcome.ok()) {
    const auto errMsg = fmt::format(
        "{} due to: Path:'{}', SDK Error Type:{}, GCS Status Code:{},  Message:'{}'",
        errorMsgPrefix,
        gcsURI(bucket, key),
        outcome.error_info().domain(),
        getErrorStringFromGcsError(outcome.code()),
        outcome.message());
    if (outcome.code() == google::cloud::StatusCode::kNotFound) {
      VELOX_FILE_NOT_FOUND_ERROR(errMsg);
    }
    VELOX_FAIL(errMsg);
  }
}

} // namespace facebook::velox

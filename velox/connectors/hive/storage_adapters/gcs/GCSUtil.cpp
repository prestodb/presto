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

#include "velox/connectors/hive/storage_adapters/gcs/GCSUtil.h"

namespace facebook::velox {

std::string getErrorStringFromGCSError(const google::cloud::StatusCode& code) {
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

} // namespace facebook::velox

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

#include "velox/type/Conversions.h"

DEFINE_bool(
    experimental_enable_legacy_cast,
    false,
    "Experimental feature flag for backward compatibility with previous output"
    " format of type conversions used for casting. This is a temporary solution"
    " that aims to facilitate a seamless transition for users who rely on the"
    " legacy behavior and hence can change in the future.");

namespace facebook::velox::util {

// This is based on Presto java's castToBoolean method.
Expected<bool> castToBoolean(const char* data, size_t len) {
  const auto& TU = static_cast<int (*)(int)>(std::toupper);

  if (len == 1) {
    auto character = TU(data[0]);
    if (character == 'T' || character == '1') {
      return true;
    }
    if (character == 'F' || character == '0') {
      return false;
    }
  }

  if ((len == 4) && (TU(data[0]) == 'T') && (TU(data[1]) == 'R') &&
      (TU(data[2]) == 'U') && (TU(data[3]) == 'E')) {
    return true;
  }

  if ((len == 5) && (TU(data[0]) == 'F') && (TU(data[1]) == 'A') &&
      (TU(data[2]) == 'L') && (TU(data[3]) == 'S') && (TU(data[4]) == 'E')) {
    return false;
  }

  const std::string errorMessage =
      fmt::format("Cannot cast {} to BOOLEAN", StringView(data, len));
  return folly::makeUnexpected(Status::UserError(errorMessage));
}
} // namespace facebook::velox::util

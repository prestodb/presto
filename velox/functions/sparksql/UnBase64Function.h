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

#include "velox/common/encode/Base64.h"
#include "velox/functions/Macros.h"

namespace facebook::velox::functions::sparksql {

// UnBase64Function decodes a base64-encoded input string into its original
// binary form. It uses the Base64 MIME decoding functions provided by
// velox::encoding. Returns a Status indicating success or error during
// decoding.
template <typename T>
struct UnBase64Function {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // Decodes the base64-encoded input and stores the result in 'result'.
  // Returns a Status object indicating success or failure.
  FOLLY_ALWAYS_INLINE Status
  call(out_type<Varbinary>& result, const arg_type<Varchar>& input) {
    auto decodedSize =
        encoding::Base64::calculateMimeDecodedSize(input.data(), input.size());
    if (decodedSize.hasError()) {
      return decodedSize.error();
    }
    result.resize(decodedSize.value());
    return encoding::Base64::decodeMime(
        input.data(), input.size(), result.data());
  }
};

} // namespace facebook::velox::functions::sparksql

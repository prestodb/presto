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

/// Encodes the input binary data into a Base64-encoded string using MIME
/// encoding.
template <typename T>
struct Base64Function {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Varbinary>& input) {
    result.resize(encoding::Base64::calculateMimeEncodedSize(input.size()));
    encoding::Base64::encodeMime(input.data(), input.size(), result.data());
  }
};
} // namespace facebook::velox::functions::sparksql

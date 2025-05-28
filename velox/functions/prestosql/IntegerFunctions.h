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

#define XXH_INLINE_ALL
#include <xxhash.h>

#include "velox/functions/Udf.h"
#include "velox/type/Type.h"

namespace facebook::velox::functions {

/// xxhash64(bigint) → bigint
/// Return a xxhash64 of input BigInt
template <typename T>
struct XxHash64BigIntFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE
  void call(out_type<int64_t>& result, const arg_type<int64_t>& input) {
    result = XXH64(&input, sizeof(input), 0);
  }
};

/// xxhash64(integer) → bigint
/// Return a xxhash64 of input Integer
template <typename T>
struct XxHash64IntegerFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE
  void call(out_type<int64_t>& result, const arg_type<int32_t>& input) {
    int64_t castedInput = static_cast<int64_t>(input);
    result = XXH64(&castedInput, sizeof(castedInput), 0);
  }
};

/// xxhash64(smallint) → bigint
/// Return a xxhash64 of input SmallInt
template <typename T>
struct XxHash64SmallIntFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE
  void call(out_type<int64_t>& result, const arg_type<int16_t>& input) {
    int64_t castedInput = static_cast<int64_t>(input);
    result = XXH64(&castedInput, sizeof(castedInput), 0);
  }
};

/// xxhash64(tinyint) → bigint
/// Return a xxhash64 of input TinyInt
template <typename T>
struct XxHash64TinyIntFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE
  void call(out_type<int64_t>& result, const arg_type<int8_t>& input) {
    int64_t castedInput = static_cast<int64_t>(input);
    result = XXH64(&castedInput, sizeof(castedInput), 0);
  }
};

// combine_hash(bigint, bigint) → bigint
template <typename T>
struct CombineHashFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE
  void call(
      out_type<int64_t>& result,
      const arg_type<int64_t>& previousHashValue,
      const arg_type<int64_t>& input) {
    result = static_cast<int64_t>(
        31 * static_cast<uint64_t>(previousHashValue) +
        static_cast<uint64_t>(input));
  }
};

} // namespace facebook::velox::functions

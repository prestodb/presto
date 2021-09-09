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

#include "velox/functions/Udf.h"
#include "velox/functions/lib/string/StringImpl.h"

namespace facebook::velox::functions {
/**
 * chr(n) → varchar
 * Returns the Unicode code point n as a single character string.
 **/
VELOX_UDF_BEGIN(chr)
FOLLY_ALWAYS_INLINE bool call(
    out_type<Varchar>& result,
    const int64_t& codePoint) {
  stringImpl::codePointToString(result, codePoint);
  return true;
}
VELOX_UDF_END();

/**
 * codepoint(string) → integer
 * Returns the Unicode code point of the only character of string.
 **/
VELOX_UDF_BEGIN(codepoint)
FOLLY_ALWAYS_INLINE bool call(
    int32_t& result,
    const arg_type<Varchar>& inputChar) {
  result = stringImpl::charToCodePoint(inputChar);
  return true;
}
VELOX_UDF_END();

/**
 * Presto variant
 * xxhash64(varbinary) → varbinary
 * Return an 8-byte binary to hash64 of input (varbinary such as string)
 * Always returns true since this function is not null sensitive
 */
template <typename To, typename From>
VELOX_UDF_BEGIN(xxhash64)
FOLLY_ALWAYS_INLINE
    bool call(out_type<To>& result, const arg_type<From>& input) {
  return stringImpl::xxhash64(result, input);
}
VELOX_UDF_END();

/**
 * HIVE variant
 * xxhash64(string) → bigint
 * Return an 64-bit integer equal to hash64 of input
 * Always returns true since this function is not null sensitive
 */
template <typename To, typename From>
VELOX_UDF_BEGIN(xxhash64int)
FOLLY_ALWAYS_INLINE bool call(
    out_type<To>& result,
    const arg_type<From>& input,
    const int64_t seed = 0) {
  return stringImpl::xxhash64int(result, input, seed);
}
VELOX_UDF_END();

// md5(varbinary) → varbinary
template <typename To, typename From>
VELOX_UDF_BEGIN(md5)
FOLLY_ALWAYS_INLINE
    bool call(out_type<To>& result, const arg_type<From>& input) {
  return stringImpl::md5(result, input);
}
VELOX_UDF_END();

/**
 * md5(varchar, int) → varchar
 * generate the md5 in varchar, the result is based on the given radix (we only
 * supports radix=16 or radix=10 for now).
 **/
template <typename To, typename From>
VELOX_UDF_BEGIN(md5_radix)
FOLLY_ALWAYS_INLINE bool call(
    out_type<To>& result,
    const arg_type<From>& input,
    const int32_t radix = 16) {
  stringImpl::md5_radix(result, input, radix);
  return true;
}
VELOX_UDF_END();

VELOX_UDF_BEGIN(to_hex)
FOLLY_ALWAYS_INLINE bool call(
    out_type<Varbinary>& result,
    const arg_type<Varchar>& input) {
  return stringImpl::toHex(result, input);
}
VELOX_UDF_END();

VELOX_UDF_BEGIN(from_hex)
FOLLY_ALWAYS_INLINE bool call(
    out_type<Varchar>& result,
    const arg_type<Varbinary>& input) {
  return stringImpl::fromHex(result, input);
}
VELOX_UDF_END();
} // namespace facebook::velox::functions

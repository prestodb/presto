/*
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
#include "presto_cpp/external/xxhash.h"
#include "velox/functions/Macros.h"

using namespace facebook::velox;
namespace facebook::presto::functions {

template <typename T>
struct KeySamplingPercentFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<double>& result,
      const arg_type<Varchar>& string) {
    static constexpr auto kTypeLength = sizeof(int64_t);
    static_assert(
        sizeof(result) == kTypeLength,
        "Incorrect result size, expected 8-bytes ");

    int64_t hash =
        folly::Endian::swap64(XXH64(string.data(), string.size(), 0));
    memcpy(&result, &hash, kTypeLength);
    result = fmod(abs(folly::Endian::big(result)), 100) / 100;
  }
};
} // namespace facebook::presto::functions
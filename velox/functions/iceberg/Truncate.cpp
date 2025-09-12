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

#include "velox/functions/iceberg/Truncate.h"
#include "velox/functions/Macros.h"
#include "velox/functions/Registerer.h"
#include "velox/functions/lib/string/StringImpl.h"

namespace facebook::velox::functions::iceberg {
namespace {
// truncate(width, input) -> truncatedValue
// For numeric values, truncate to the nearest lower multiple of width works
// consistently for both positive and negative values of input, even in
// languages where the % operator can return a negative remainder. The width is
// used to truncate decimal values is applied using unscaled value to avoid
// additional (and potentially conflicting) parameters. For string values, it
// truncates a valid UTF-8 string with no more than width code points. In
// contrast to strings, binary values do not have an assumed encoding and are
// truncated to width bytes.
template <typename TExec>
struct TruncateFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  // ASCII input always produces ASCII result.
  static constexpr bool is_default_ascii_behavior = true;

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& inputTypes,
      const core::QueryConfig& /*config*/,
      const int32_t* /*width*/,
      const arg_type<Varchar>* /*input*/) {
    inputIsVarbinary_ = inputTypes[1]->isVarbinary();
  }

  template <typename T>
  FOLLY_ALWAYS_INLINE Status call(T& out, int32_t width, T input) {
    VELOX_USER_RETURN_LE(width, 0, "Invalid truncate width");
    // Truncate to the nearest lower multiple of width.
    out = input - ((input % width) + width) % width;
    return Status::OK();
  }

  FOLLY_ALWAYS_INLINE Status call(
      out_type<Varchar>& result,
      int32_t width,
      const arg_type<Varchar>& input) {
    VELOX_USER_RETURN_LE(width, 0, "Invalid truncate width");
    const auto truncatedwidth = inputIsVarbinary_
        ? stringImpl::cappedByteLength<true>(input, width)
        : stringImpl::cappedByteLength<false>(input, width);
    result = StringView(input.data(), truncatedwidth);
    return Status::OK();
  }

  FOLLY_ALWAYS_INLINE Status callAscii(
      out_type<Varchar>& result,
      int32_t width,
      const arg_type<Varchar>& input) {
    VELOX_USER_RETURN_LE(width, 0, "Invalid truncate width");
    auto truncatedwidth = stringImpl::cappedLength<true>(input, width);
    result = StringView(input.data(), truncatedwidth);
    return Status::OK();
  }

 private:
  bool inputIsVarbinary_ = false;
};
} // namespace

void registerTruncateFunctions(const std::string& prefix) {
  registerFunction<TruncateFunction, int8_t, int32_t, int8_t>(
      {prefix + "truncate"});
  registerFunction<TruncateFunction, int16_t, int32_t, int16_t>(
      {prefix + "truncate"});
  registerFunction<TruncateFunction, int32_t, int32_t, int32_t>(
      {prefix + "truncate"});
  registerFunction<TruncateFunction, int64_t, int32_t, int64_t>(
      {prefix + "truncate"});
  registerFunction<TruncateFunction, Varchar, int32_t, Varchar>(
      {prefix + "truncate"});
  registerFunction<TruncateFunction, Varbinary, int32_t, Varbinary>(
      {prefix + "truncate"});
  registerFunction<
      TruncateFunction,
      LongDecimal<P1, S1>,
      int32_t,
      LongDecimal<P1, S1>>({prefix + "truncate"});

  registerFunction<
      TruncateFunction,
      ShortDecimal<P1, S1>,
      int32_t,
      ShortDecimal<P1, S1>>({prefix + "truncate"});
}

} // namespace facebook::velox::functions::iceberg

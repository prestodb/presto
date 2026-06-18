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

#include "velox/functions/Macros.h"
#include "velox/functions/lib/string/StringImpl.h"

namespace facebook::presto::hive::functions {

/// The InitCapFunction capitalizes the first character of each word in a
/// string, and lowercases the rest.
template <typename T>
struct InitCapFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // ASCII input always produces ASCII result. This is required for ASCII fast
  // path
  static constexpr bool is_default_ascii_behavior = true;

  FOLLY_ALWAYS_INLINE void call(
      out_type<velox::Varchar>& result,
      const arg_type<velox::Varchar>& input) {
    velox::functions::stringImpl::initcap<
        /*strictSpace=*/false,
        /*isAscii=*/false,
        /*turkishCasing=*/true,
        /*greekFinalSigma=*/true>(result, input);
  }

  FOLLY_ALWAYS_INLINE void callAscii(
      out_type<velox::Varchar>& result,
      const arg_type<velox::Varchar>& input) {
    velox::functions::stringImpl::initcap<
        /*strictSpace=*/false,
        /*isAscii=*/true,
        /*turkishCasing=*/true,
        /*greekFinalSigma=*/true>(result, input);
  }
};

} // namespace facebook::presto::hive::functions

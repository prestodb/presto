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
#include "velox/functions/sparksql/Size.h"

#include "velox/core/QueryConfig.h"

#include "velox/functions/Macros.h"
#include "velox/functions/Registerer.h"

namespace facebook::velox::functions::sparksql {
namespace {

template <typename TExecParams>
struct Size {
  VELOX_DEFINE_FUNCTION_TYPES(TExecParams);

  template <typename TInput>
  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& /*config*/,
      const TInput* /*input*/,
      const bool* legacySizeOfNull) {
    if (legacySizeOfNull == nullptr) {
      VELOX_USER_FAIL("Constant legacySizeOfNull is expected.");
    }
    legacySizeOfNull_ = *legacySizeOfNull;
  }

  template <typename TInput>
  FOLLY_ALWAYS_INLINE bool callNullable(
      int32_t& out,
      const TInput* input,
      const bool* /*legacySizeOfNull*/) {
    if (input == nullptr) {
      if (legacySizeOfNull_) {
        out = -1;
        return true;
      }
      return false;
    }
    out = input->size();
    return true;
  }

 private:
  // If true, returns -1 for null input. Otherwise, returns null.
  bool legacySizeOfNull_;
};
} // namespace

void registerSize(const std::string& prefix) {
  registerFunction<Size, int32_t, Array<Any>, bool>({prefix});
  registerFunction<Size, int32_t, Map<Any, Any>, bool>({prefix});
}

} // namespace facebook::velox::functions::sparksql

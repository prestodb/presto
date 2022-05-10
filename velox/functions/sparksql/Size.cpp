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
      const core::QueryConfig& config,
      const TInput* input /*input*/) {
    legacySizeOfNull_ = config.get<bool>("legacySizeOfNull", true);
  }

  template <typename TInput>
  FOLLY_ALWAYS_INLINE bool callNullable(int64_t& out, const TInput* input) {
    if (input == nullptr) {
      if (legacySizeOfNull_) {
        out = -1;
        return true;
      } else {
        return false;
      }
    }
    out = input->size();
    return true;
  }

 private:
  bool legacySizeOfNull_;
};
} // namespace

void registerSize(const std::string& prefix) {
  registerFunction<Size, int64_t, Array<Any>>({prefix});
  registerFunction<Size, int64_t, Map<Any, Any>>({prefix});
}

} // namespace facebook::velox::functions::sparksql

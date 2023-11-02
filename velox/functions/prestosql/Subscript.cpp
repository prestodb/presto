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

#include "velox/functions/lib/SubscriptUtil.h"

namespace facebook::velox::functions {

namespace {

/// Subscript operator for arrays and maps (to be mapped from usages like
/// array[0] and map['key']).
/// - does not allow negative indices for arrays
/// - does not allow out of bounds accesses for arrays (throws)
/// - index starts at 1 for arrays.
class SubscriptFunction : public SubscriptImpl<
                              /* allowNegativeIndices */ false,
                              /* nullOnNegativeIndices */ false,
                              /* allowOutOfBound */ false,
                              /* indexStartsAtOne */ true> {
 public:
  explicit SubscriptFunction(bool allowcaching) : SubscriptImpl(allowcaching) {}

  bool canPushdown() const override {
    return true;
  }
};

} // namespace

void registerSubscriptFunction(
    const std::string& name,
    bool enableCaching = true) {
  exec::registerStatefulVectorFunction(
      name,
      SubscriptFunction::signatures(),
      [enableCaching](
          const std::string&,
          const std::vector<exec::VectorFunctionArg>& inputArgs,
          const velox::core::QueryConfig& config) {
        static const auto kSubscriptStateLess =
            std::make_shared<SubscriptFunction>(false);
        if (inputArgs[0].type->isArray()) {
          return kSubscriptStateLess;
        } else {
          return std::make_shared<SubscriptFunction>(
              enableCaching && config.isExpressionEvaluationCacheEnabled());
        }
      });
}

} // namespace facebook::velox::functions

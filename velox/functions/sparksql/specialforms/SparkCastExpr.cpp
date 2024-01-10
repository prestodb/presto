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

#include "velox/functions/sparksql/specialforms/SparkCastExpr.h"

namespace facebook::velox::functions::sparksql {

exec::ExprPtr SparkCastCallToSpecialForm::constructSpecialForm(
    const TypePtr& type,
    std::vector<exec::ExprPtr>&& compiledChildren,
    bool trackCpuUsage,
    const core::QueryConfig& /*config*/) {
  VELOX_CHECK_EQ(
      compiledChildren.size(),
      1,
      "CAST statements expect exactly 1 argument, received {}.",
      compiledChildren.size());
  return std::make_shared<SparkCastExpr>(
      type,
      std::move(compiledChildren[0]),
      trackCpuUsage,
      false,
      std::make_shared<SparkCastHooks>());
}

exec::ExprPtr SparkTryCastCallToSpecialForm::constructSpecialForm(
    const TypePtr& type,
    std::vector<exec::ExprPtr>&& compiledChildren,
    bool trackCpuUsage,
    const core::QueryConfig& /*config*/) {
  VELOX_CHECK_EQ(
      compiledChildren.size(),
      1,
      "TRY CAST statements expect exactly 1 argument, received {}.",
      compiledChildren.size());
  return std::make_shared<SparkCastExpr>(
      type,
      std::move(compiledChildren[0]),
      trackCpuUsage,
      true,
      std::make_shared<SparkCastHooks>());
}
} // namespace facebook::velox::functions::sparksql

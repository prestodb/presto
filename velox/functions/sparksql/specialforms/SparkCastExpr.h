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

#include "velox/expression/CastExpr.h"
#include "velox/functions/sparksql/specialforms/SparkCastHooks.h"

namespace facebook::velox::functions::sparksql {

class SparkCastExpr : public exec::CastExpr {
 public:
  /// @param type The target type of the cast expression
  /// @param expr The expression to cast
  /// @param trackCpuUsage Whether to track CPU usage
  SparkCastExpr(
      TypePtr type,
      exec::ExprPtr&& expr,
      bool trackCpuUsage,
      bool nullOnFailure,
      std::shared_ptr<SparkCastHooks> hooks)
      : exec::CastExpr(
            type,
            std::move(expr),
            trackCpuUsage,
            nullOnFailure,
            hooks) {}
};

class SparkCastCallToSpecialForm : public exec::CastCallToSpecialForm {
 public:
  exec::ExprPtr constructSpecialForm(
      const TypePtr& type,
      std::vector<exec::ExprPtr>&& compiledChildren,
      bool trackCpuUsage,
      const core::QueryConfig& config) override;
};

class SparkTryCastCallToSpecialForm : public exec::TryCastCallToSpecialForm {
 public:
  exec::ExprPtr constructSpecialForm(
      const TypePtr& type,
      std::vector<exec::ExprPtr>&& compiledChildren,
      bool trackCpuUsage,
      const core::QueryConfig& config) override;
};
} // namespace facebook::velox::functions::sparksql

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
#include "velox/functions/prestosql/json/SIMDJsonWrapper.h"

namespace facebook::velox {
/// Custom operator for casts from and to Json type.
class JsonCastOperator : public exec::CastOperator {
 public:
  bool isSupportedFromType(const TypePtr& other) const override;

  bool isSupportedToType(const TypePtr& other) const override;

  void castTo(
      const BaseVector& input,
      exec::EvalCtx& context,
      const SelectivityVector& rows,
      const TypePtr& resultType,
      VectorPtr& result) const override;

  void castTo(
      const BaseVector& input,
      exec::EvalCtx& context,
      const SelectivityVector& rows,
      const TypePtr& resultType,
      VectorPtr& result,
      const std::shared_ptr<exec::CastHooks>& hooks) const override;

  void castFrom(
      const BaseVector& input,
      exec::EvalCtx& context,
      const SelectivityVector& rows,
      const TypePtr& resultType,
      VectorPtr& result) const override;

 private:
  template <TypeKind kind>
  void castFromJson(
      const BaseVector& input,
      exec::EvalCtx& context,
      const SelectivityVector& rows,
      BaseVector& result) const;

  mutable folly::once_flag initializeErrors_;
  mutable std::exception_ptr errors_[simdjson::NUM_ERROR_CODES];
  mutable std::string paddedInput_;
};
} // namespace facebook::velox

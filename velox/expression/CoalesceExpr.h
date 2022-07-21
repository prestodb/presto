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

#include "velox/expression/SpecialForm.h"

namespace facebook::velox::exec {

const char* const kCoalesce = "coalesce";

class CoalesceExpr : public SpecialForm {
 public:
  CoalesceExpr(
      TypePtr type,
      std::vector<ExprPtr>&& inputs,
      bool inputsSupportFlatNoNullsFastPath);

  void evalSpecialForm(
      const SelectivityVector& rows,
      EvalCtx& context,
      VectorPtr& result) override;

  bool propagatesNulls() const override {
    return false;
  }
};
} // namespace facebook::velox::exec

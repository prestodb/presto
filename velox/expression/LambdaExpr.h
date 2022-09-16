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

class LambdaExpr : public SpecialForm {
 public:
  LambdaExpr(
      TypePtr type,
      RowTypePtr&& signature,
      std::vector<std::shared_ptr<FieldReference>>&& capture,
      std::shared_ptr<Expr>&& body,
      bool trackCpuUsage)
      : SpecialForm(
            std::move(type),
            std::vector<std::shared_ptr<Expr>>(),
            "lambda",
            false /* supportsFlatNoNullsFastPath */,
            trackCpuUsage),
        signature_(std::move(signature)),
        capture_(std::move(capture)),
        body_(std::move(body)) {
    for (auto& field : capture_) {
      distinctFields_.push_back(field.get());
    }
  }

  std::string toString(bool recursive = true) const override;

  bool propagatesNulls() const override {
    // A null capture does not result in a null function.
    return false;
  }

  void evalSpecialForm(
      const SelectivityVector& rows,
      EvalCtx& context,
      VectorPtr& result) override;

 private:
  void makeTypeWithCapture(EvalCtx& context);

  RowTypePtr signature_;
  std::vector<std::shared_ptr<FieldReference>> capture_;
  ExprPtr body_;
  // Filled on first use.
  RowTypePtr typeWithCapture_;
  std::vector<column_index_t> captureChannels_;
};
} // namespace facebook::velox::exec

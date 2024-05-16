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

#include "velox/expression/Expr.h"
#include "velox/expression/RegisterSpecialForm.h"
#include "velox/parse/Expressions.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/tests/utils/VectorMaker.h"

namespace facebook::velox::functions::test {

class FunctionBenchmarkBase {
 public:
  FunctionBenchmarkBase() {
    parse::registerTypeResolver();
    exec::registerFunctionCallToSpecialForms();
  }

  void setTimezone(const std::string& value) {
    queryCtx_->testingOverrideConfigUnsafe({
        {core::QueryConfig::kSessionTimezone, value},
    });
  }

  void setAdjustTimestampToTimezone(const std::string& value) {
    queryCtx_->testingOverrideConfigUnsafe(
        {{core::QueryConfig::kAdjustTimestampToTimezone, value}});
  }

  exec::ExprSet compileExpression(
      const std::string& text,
      const TypePtr& rowType) {
    auto untyped = parse::parseExpr(text, options_);
    auto typed =
        core::Expressions::inferTypes(untyped, rowType, execCtx_.pool());
    return exec::ExprSet({typed}, &execCtx_);
  }

  void evaluate(
      exec::ExprSet& exprSet,
      const RowVectorPtr& data,
      const SelectivityVector& rows,
      VectorPtr& result) {
    exec::EvalCtx evalCtx(&execCtx_, &exprSet, data.get());
    std::vector<VectorPtr> results{result};
    exprSet.eval(rows, evalCtx, results);

    // If result was nullptr, we need to pick up the value that was created.
    if (!result) {
      result = results[0];
    }
  }

  VectorPtr evaluate(
      exec::ExprSet& exprSet,
      const RowVectorPtr& data,
      const SelectivityVector& rows) {
    VectorPtr result;
    evaluate(exprSet, data, rows, result);
    return result;
  }

  VectorPtr evaluate(exec::ExprSet& exprSet, const RowVectorPtr& data) {
    SelectivityVector rows(data->size());
    return evaluate(exprSet, data, rows);
  }

  VectorPtr evaluate(const std::string& expression, const RowVectorPtr& data) {
    auto exprSet = compileExpression(expression, asRowType(data->type()));
    return evaluate(exprSet, data);
  }

  facebook::velox::test::VectorMaker& maker() {
    return vectorMaker_;
  }

  memory::MemoryPool* pool() {
    return execCtx_.pool();
  }

 protected:
  std::shared_ptr<core::QueryCtx> queryCtx_{core::QueryCtx::create()};
  std::shared_ptr<memory::MemoryPool> pool_{
      memory::memoryManager()->addLeafPool()};
  core::ExecCtx execCtx_{pool_.get(), queryCtx_.get()};
  facebook::velox::test::VectorMaker vectorMaker_{execCtx_.pool()};
  parse::ParseOptions options_;
};
} // namespace facebook::velox::functions::test

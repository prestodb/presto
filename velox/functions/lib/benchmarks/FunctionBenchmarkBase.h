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
#include "velox/parse/Expressions.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/tests/VectorMaker.h"

namespace facebook::velox::functions::test {

class FunctionBenchmarkBase {
 public:
  FunctionBenchmarkBase() {
    parse::registerTypeResolver();
  }

  exec::ExprSet compileExpression(
      const std::string& text,
      const TypePtr& rowType) {
    auto untyped = parse::parseExpr(text, options_);
    auto typed =
        core::Expressions::inferTypes(untyped, rowType, execCtx_.pool());
    return exec::ExprSet({typed}, &execCtx_);
  }

  VectorPtr evaluate(exec::ExprSet& exprSet, const RowVectorPtr& data) {
    SelectivityVector rows(data->size());
    exec::EvalCtx evalCtx(&execCtx_, &exprSet, data.get());
    std::vector<VectorPtr> results(1);
    exprSet.eval(rows, &evalCtx, &results);
    return results[0];
  }

  facebook::velox::test::VectorMaker& maker() {
    return vectorMaker_;
  }

  memory::MemoryPool* pool() {
    return execCtx_.pool();
  }

 protected:
  std::shared_ptr<core::QueryCtx> queryCtx_{core::QueryCtx::createForTest()};
  std::unique_ptr<memory::MemoryPool> pool_{
      memory::getDefaultScopedMemoryPool()};
  core::ExecCtx execCtx_{pool_.get(), queryCtx_.get()};
  facebook::velox::test::VectorMaker vectorMaker_{execCtx_.pool()};
  parse::ParseOptions options_;
};
} // namespace facebook::velox::functions::test

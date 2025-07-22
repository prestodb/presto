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

#include "velox/vector/ComplexVector.h"

namespace facebook::velox::exec {
class ExprSet;
}

namespace facebook::velox::core {

class ITypedExpr;

// Exposes expression evaluation functionality of the engine to other parts of
// the code base.  Connector may use it, for example, to evaluate pushed down
// filters.  This is not thread safe and serializing operations is the
// responsibility of the caller.  This is self-contained and does not reference
// objects from the thread which constructs this.  Passing this between threads
// is allowed as long as uses are sequential.  May reference query-level
// structures like QueryCtx.
class ExpressionEvaluator {
 public:
  virtual ~ExpressionEvaluator() = default;

  // Compiles an expression. Returns an instance of exec::ExprSet that can be
  // used to evaluate that expression on multiple vectors using evaluate method.
  virtual std::unique_ptr<exec::ExprSet> compile(
      const std::shared_ptr<const ITypedExpr>& expression) = 0;

  virtual std::unique_ptr<exec::ExprSet> compile(
      const std::vector<std::shared_ptr<const ITypedExpr>>& expressions) = 0;

  // Evaluates previously compiled expression on the specified rows.
  // Re-uses result vector if it is not null.
  virtual void evaluate(
      exec::ExprSet* exprSet,
      const SelectivityVector& rows,
      const RowVector& input,
      VectorPtr& result) = 0;

  virtual void evaluate(
      exec::ExprSet* exprSet,
      const SelectivityVector& rows,
      const RowVector& input,
      std::vector<VectorPtr>& results) = 0;

  // Memory pool used to construct input or output vectors.
  virtual memory::MemoryPool* pool() = 0;
};

} // namespace facebook::velox::core

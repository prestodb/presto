/*
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
#include "velox/expression/Expr.h"
#include "velox/expression/VectorFunction.h"

namespace facebook::velox::functions {
namespace {

class RowFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      exec::Expr* caller,
      exec::EvalCtx* context,
      VectorPtr* result) const override {
    auto argsCopy = args;
    RowVectorPtr row = std::make_shared<RowVector>(
        context->pool(),
        caller->type(),
        BufferPtr(nullptr),
        rows.size(),
        std::move(argsCopy),
        0 /*nullCount*/);
    if (*result) {
      BaseVector::ensureWritable(rows, caller->type(), context->pool(), result);
      (*result)->copy(row.get(), rows, nullptr);
    } else {
      *result = std::move(row);
    }
  }

  bool isDefaultNullBehavior() const override {
    return false;
  }
};
} // namespace

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_concat_row,
    std::vector<std::shared_ptr<exec::FunctionSignature>>{},
    std::make_unique<RowFunction>());

} // namespace facebook::velox::functions

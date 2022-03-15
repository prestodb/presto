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
#include "velox/expression/EvalCtx.h"
#include "velox/expression/VectorFunction.h"

namespace facebook::velox::functions {
namespace {
class CoalesceFunction : public exec::VectorFunction {
 public:
  bool isDefaultNullBehavior() const override {
    return false;
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx* context,
      VectorPtr* result) const override {
    BaseVector::ensureWritable(rows, args[0]->type(), args[0]->pool(), result);

    // null positions to populate
    exec::LocalSelectivityVector activeRowsHolder(context, rows.end());
    auto activeRows = activeRowsHolder.get();
    *activeRows = rows;

    // positions to be copied from the next argument
    exec::LocalSelectivityVector copyRowsHolder(context, rows.end());
    auto copyRows = copyRowsHolder.get();
    for (int i = 0; i < args.size(); i++) {
      auto& arg = args[i];
      const uint64_t* rawNulls = arg->flatRawNulls(*activeRows);
      if (!rawNulls) {
        (*result)->copy(arg.get(), *activeRows, nullptr);
        return; // no nulls left
      }

      if (i == 0) {
        // initialize result by copying all rows from the first argument
        (*result)->copy(arg.get(), *activeRows, nullptr);
      } else {
        *copyRows = *activeRows;
        copyRows->deselectNulls(rawNulls, 0, activeRows->end());
        if (copyRows->hasSelections()) {
          (*result)->copy(arg.get(), *copyRows, nullptr);
        } else {
          continue;
        }
      }

      activeRows->deselectNonNulls(rawNulls, 0, activeRows->end());
      if (!activeRows->hasSelections()) {
        // no nulls left
        return;
      }
    }
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // T... -> T
    return {exec::FunctionSignatureBuilder()
                .typeVariable("T")
                .returnType("T")
                .argumentType("T")
                .variableArity()
                .build()};
  }
};
} // namespace

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_coalesce,
    CoalesceFunction::signatures(),
    std::make_unique<CoalesceFunction>());
} // namespace facebook::velox::functions

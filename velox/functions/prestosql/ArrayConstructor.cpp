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
#include "velox/functions/prestosql/ArrayConstructor.h"
#include "velox/expression/Expr.h"
#include "velox/expression/VectorFunction.h"

namespace facebook::velox::functions {
namespace {

class ArrayConstructor : public exec::VectorFunction {
 public:
  bool isDefaultNullBehavior() const override {
    return false;
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& finalResults) const override {
    auto numArgs = args.size();
    VectorPtr localResult;
    if (!resultCached_ && rows.end() < kReuseThreshold) {
      BaseVector::ensureWritable(
          rows, outputType, context.pool(), resultCached_);
      localResult = resultCached_;
    } else if (resultCached_.unique() && rows.end() < kReuseThreshold) {
      resultCached_->prepareForReuse();
      resultCached_->resize(rows.end());
      localResult = resultCached_;
    } else {
      context.ensureWritable(rows, outputType, finalResults);
      localResult = finalResults;
    }

    localResult->clearNulls(rows);
    auto arrayResult = localResult->as<ArrayVector>();
    auto sizes = arrayResult->mutableSizes(rows.end());
    auto rawSizes = sizes->asMutable<int32_t>();
    auto offsets = arrayResult->mutableOffsets(rows.end());
    auto rawOffsets = offsets->asMutable<int32_t>();

    auto elementsResult = arrayResult->elements();

    // append to the end of the "elements" vector
    auto baseOffset = elementsResult->size();

    if (args.empty()) {
      rows.applyToSelected([&](vector_size_t row) {
        rawSizes[row] = 0;
        rawOffsets[row] = baseOffset;
      });
    } else {
      elementsResult->resize(baseOffset + numArgs * rows.countSelected());

      vector_size_t offset = baseOffset;
      rows.applyToSelected([&](vector_size_t row) {
        rawSizes[row] = numArgs;
        rawOffsets[row] = offset;
        for (int i = 0; i < numArgs; i++) {
          elementsResult->copy(args[i].get(), offset++, row, 1);
        }
      });
    }
    if (localResult != finalResults) {
      context.moveOrCopyResult(localResult, rows, finalResults);
    }
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    return {
        // () -> array(unknown)
        exec::FunctionSignatureBuilder().returnType("array(unknown)").build(),
        // T... -> array(T)
        exec::FunctionSignatureBuilder()
            .typeVariable("T")
            .returnType("array(T)")
            .argumentType("T")
            .variableArity()
            .build(),
    };
  }

 private:
  mutable VectorPtr resultCached_;
  static constexpr vector_size_t kReuseThreshold = 100;
};
} // namespace

void registerArrayConstructor(const std::string& name) {
  exec::registerStatefulVectorFunction(
      name, ArrayConstructor::signatures(), [](const auto&, const auto&) {
        return std::make_shared<ArrayConstructor>();
      });
}

} // namespace facebook::velox::functions

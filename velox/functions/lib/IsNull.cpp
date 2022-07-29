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

template <bool IsNotNULL>
class IsNullFunction : public exec::VectorFunction {
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
    BaseVector* arg = args[0].get();
    if (rows.isAllSelected()) {
      if (!arg->mayHaveNulls()) {
        // no nulls
        *result =
            BaseVector::createConstant(IsNotNULL, rows.end(), context->pool());
        return;
      }

      context->ensureWritable(rows, BOOLEAN(), *result);
      FlatVector<bool>* flatResult = (*result)->asFlatVector<bool>();
      flatResult->clearNulls(rows);
      flatResult->mutableRawValues<int64_t>();
      auto rawNulls = arg->flatRawNulls(rows);
      memcpy(
          flatResult->mutableRawValues<int64_t>(),
          rawNulls,
          bits::nbytes(rows.end()));
      if constexpr (!IsNotNULL) {
        bits::negate(flatResult->mutableRawValues<char>(), rows.end());
      }
      return;
    }

    context->ensureWritable(rows, BOOLEAN(), *result);
    FlatVector<bool>* flatResult = (*result)->asFlatVector<bool>();
    if (!arg->mayHaveNulls()) {
      rows.applyToSelected(
          [&](vector_size_t i) { flatResult->set(i, IsNotNULL); });
    } else {
      auto rawNulls = arg->flatRawNulls(rows);
      if constexpr (!IsNotNULL) {
        rows.applyToSelected([&](vector_size_t i) {
          flatResult->set(i, bits::isBitNull(rawNulls, i));
        });
      } else {
        rows.applyToSelected([&](vector_size_t i) {
          flatResult->set(i, !bits::isBitNull(rawNulls, i));
        });
      }
    }
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // T -> boolean
    return {exec::FunctionSignatureBuilder()
                .typeVariable("T")
                .returnType("boolean")
                .argumentType("T")
                .build()};
  }
};
} // namespace

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_is_null,
    IsNullFunction<false>::signatures(),
    std::make_unique<IsNullFunction</*IsNotNUll=*/false>>());

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_is_not_null,
    IsNullFunction<true>::signatures(),
    std::make_unique<IsNullFunction</*IsNotNUll=*/true>>());
} // namespace facebook::velox::functions

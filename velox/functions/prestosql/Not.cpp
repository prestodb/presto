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
#include "velox/expression/VectorFunction.h"

namespace facebook::velox::functions {
namespace {

class NotFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx* context,
      VectorPtr* result) const override {
    VELOX_CHECK_EQ(
        args.size(), 1, "Incorrect number of arguments passed to not");
    const auto& input = args[0];

    VELOX_CHECK_EQ(
        input->type()->kind(), TypeKind::BOOLEAN, "Unsupported input type");

    BufferPtr negated;

    // Input may be constant or flat.
    if (input->isConstantEncoding()) {
      bool value = input->as<ConstantVector<bool>>()->valueAt(0);
      negated =
          AlignedBuffer::allocate<bool>(rows.size(), context->pool(), !value);
    } else {
      negated = AlignedBuffer::allocate<bool>(rows.size(), context->pool());
      auto rawNegated = negated->asMutable<char>();

      auto rawInput = input->asFlatVector<bool>()->rawValues<uint64_t>();

      memcpy(rawNegated, rawInput, bits::nbytes(rows.end()));
      bits::negate(rawNegated, rows.end());
    }

    auto localResult = std::make_shared<FlatVector<bool>>(
        context->pool(),
        nullptr,
        rows.size(),
        negated,
        std::vector<BufferPtr>{});

    context->moveOrCopyResult(localResult, rows, result);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // boolean -> boolean
    return {exec::FunctionSignatureBuilder()
                .returnType("boolean")
                .argumentType("boolean")
                .build()};
  }
};

} // namespace

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_not,
    NotFunction::signatures(),
    std::make_unique<NotFunction>());
} // namespace facebook::velox::functions

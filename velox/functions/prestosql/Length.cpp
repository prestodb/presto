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
#include "velox/functions/lib/StringEncodingUtils.h"
#include "velox/functions/lib/string/StringImpl.h"

namespace facebook {
namespace velox {
namespace functions {

namespace {
/**
 * Length of the input UTF8 string in characters.
 * Have an Ascii fast path optimization
 **/
class LengthFunction : public exec::VectorFunction {
 private:
  // String encoding wrappable function
  template <StringEncodingMode stringEncoding>
  struct ApplyInternalString {
    static void apply(
        const SelectivityVector& rows,
        const DecodedVector* decodedInput,
        FlatVector<int64_t>* resultFlatVector) {
      rows.applyToSelected([&](int row) {
        auto result = stringImpl::length<stringEncoding>(
            decodedInput->valueAt<StringView>(row));
        resultFlatVector->set(row, result);
      });
    }
  };

 public:
  bool isDefaultNullBehavior() const override {
    return true;
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      exec::Expr* /* unused */,
      exec::EvalCtx* context,
      VectorPtr* result) const override {
    auto inputArg = args.at(0);
    // Decode input argument
    exec::LocalDecodedVector inputHolder(context, *inputArg, rows);
    auto decodedInput = inputHolder.get();

    // Prepare output vector
    BaseVector::ensureWritable(rows, BIGINT(), context->pool(), result);
    auto* resultFlatVector = (*result)->as<FlatVector<int64_t>>();

    if (inputArg->typeKind() == TypeKind::VARCHAR) {
      auto stringEncoding = getStringEncodingOrUTF8(inputArg.get(), rows);
      StringEncodingTemplateWrapper<ApplyInternalString>::apply(
          stringEncoding, rows, decodedInput, resultFlatVector);
      return;
    }
    VELOX_UNREACHABLE();
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // varchar -> bigint
    return {exec::FunctionSignatureBuilder()
                .returnType("bigint")
                .argumentType("varchar")
                .build()};
  }
};
} // namespace

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_length,
    LengthFunction::signatures(),
    std::make_unique<LengthFunction>());

} // namespace functions
} // namespace velox
} // namespace facebook

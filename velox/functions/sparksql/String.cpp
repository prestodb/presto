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

namespace facebook::velox::functions::sparksql {
namespace {

class Length : public exec::VectorFunction {
  bool ensureStringEncodingSetAtAllInputs() const override {
    return true;
  }

  void apply(
      const SelectivityVector& selected,
      std::vector<VectorPtr>& args,
      exec::Expr*,
      exec::EvalCtx* context,
      VectorPtr* result) const override {
    VELOX_CHECK_EQ(args.size(), 1);
    VELOX_CHECK(
        args[0]->typeKind() == TypeKind::VARCHAR ||
        args[0]->typeKind() == TypeKind::VARBINARY);
    exec::LocalDecodedVector input(context, *args[0], selected);
    BaseVector::ensureWritable(selected, INTEGER(), context->pool(), result);
    auto* output = (*result)->as<FlatVector<int32_t>>();

    if (args[0]->typeKind() == TypeKind::VARCHAR &&
        getStringEncodingOrUTF8(args[0].get()) != StringEncodingMode::ASCII) {
      selected.applyToSelected([&](vector_size_t row) {
        const StringView str = input->valueAt<StringView>(row);
        output->set(row, lengthUnicode(str.data(), str.size()));
      });
    } else {
      selected.applyToSelected([&](vector_size_t row) {
        output->set(row, input->valueAt<StringView>(row).size());
      });
    }
  }
};

} // namespace

std::vector<std::shared_ptr<exec::FunctionSignature>> lengthSignatures() {
  return {
      exec::FunctionSignatureBuilder()
          .returnType("INTEGER")
          .argumentType("VARCHAR")
          .build(),
      exec::FunctionSignatureBuilder()
          .returnType("INTEGER")
          .argumentType("VARBINARY")
          .build(),
  };
}

std::shared_ptr<exec::VectorFunction> makeLength(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  static const auto kLengthFunction = std::make_shared<Length>();
  return kLengthFunction;
}

} // namespace facebook::velox::functions::sparksql

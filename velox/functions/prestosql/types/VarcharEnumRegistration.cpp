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

#include "velox/functions/prestosql/types/VarcharEnumRegistration.h"
#include "velox/expression/CastExpr.h"
#include "velox/functions/prestosql/types/VarcharEnumType.h"

namespace facebook::velox {
namespace {
class VarcharEnumCastOperator : public exec::CastOperator {
 public:
  static const std::shared_ptr<const CastOperator>& get() {
    static const std::shared_ptr<const CastOperator> kInstance =
        std::make_shared<const VarcharEnumCastOperator>();

    return kInstance;
  }

  // Casting is only supported from VARCHAR type.
  bool isSupportedFromType(const TypePtr& other) const override {
    return VARCHAR()->equivalent(*other);
  }

  // Casting is only supported to VARCHAR type.
  bool isSupportedToType(const TypePtr& other) const override {
    return VARCHAR()->equivalent(*other);
  }

  void castTo(
      const BaseVector& input,
      exec::EvalCtx& context,
      const SelectivityVector& rows,
      const TypePtr& resultType,
      VectorPtr& result) const override {
    context.ensureWritable(rows, resultType, result);
    auto* flatResult = result->asChecked<FlatVector<StringView>>();
    flatResult->clearNulls(rows);

    auto enumType = asVarcharEnum(resultType);
    const auto* varcharVector = input.as<SimpleVector<StringView>>();
    rows.applyToSelected([&](vector_size_t row) {
      const std::string varcharToCast = varcharVector->valueAt(row).str();
      if (!enumType->containsValue(varcharToCast)) {
        context.setStatus(
            row,
            Status::UserError(
                "No value '{}' in {}", varcharToCast, enumType->enumName()));
        return;
      }
      flatResult->set(row, StringView(varcharToCast));
    });
  }

  void castFrom(
      const BaseVector& input,
      exec::EvalCtx& context,
      const SelectivityVector& rows,
      const TypePtr& resultType,
      VectorPtr& result) const override {
    context.ensureWritable(rows, resultType, result);
    auto* flatResult = result->asChecked<FlatVector<StringView>>();
    flatResult->copy(&input, rows, nullptr);
  }
};

class VarcharEnumTypeFactory : public CustomTypeFactory {
 public:
  TypePtr getType(const std::vector<TypeParameter>& parameters) const override {
    VELOX_CHECK_EQ(
        parameters.size(),
        1,
        "Expected exactly one type parameters for VarcharEnumType");
    VELOX_CHECK(
        parameters[0].varcharEnumLiteral.has_value(),
        "VarcharEnumType parameter must be varcharEnumLiteral");
    return VARCHAR_ENUM(parameters[0].varcharEnumLiteral.value());
  }

  exec::CastOperatorPtr getCastOperator() const override {
    return VarcharEnumCastOperator::get();
  }

  AbstractInputGeneratorPtr getInputGenerator(
      const InputGeneratorConfig& /*config*/) const override {
    return nullptr;
  }
};
} // namespace

void registerVarcharEnumType() {
  registerCustomType(
      "varchar_enum", std::make_unique<const VarcharEnumTypeFactory>());
}
} // namespace facebook::velox

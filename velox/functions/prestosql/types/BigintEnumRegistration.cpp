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

#include "velox/functions/prestosql/types/BigintEnumRegistration.h"
#include "velox/expression/CastExpr.h"
#include "velox/functions/prestosql/types/BigintEnumType.h"

namespace facebook::velox {
namespace {
class BigintEnumCastOperator : public exec::CastOperator {
 public:
  static const std::shared_ptr<const CastOperator>& get() {
    static const std::shared_ptr<const CastOperator> kInstance =
        std::make_shared<const BigintEnumCastOperator>();

    return kInstance;
  }

  // Casting is supported from all integer types.
  bool isSupportedFromType(const TypePtr& other) const override {
    return BIGINT()->equivalent(*other) || TINYINT()->equivalent(*other) ||
        SMALLINT()->equivalent(*other) || INTEGER()->equivalent(*other);
  }

  // Casting is only supported to BIGINT type.
  bool isSupportedToType(const TypePtr& other) const override {
    return BIGINT()->equivalent(*other);
  }

  void castTo(
      const BaseVector& input,
      exec::EvalCtx& context,
      const SelectivityVector& rows,
      const TypePtr& resultType,
      VectorPtr& result) const override {
    switch (input.typeKind()) {
      case TypeKind::TINYINT:
        castFromInteger<int8_t>(input, context, rows, resultType, result);
        break;
      case TypeKind::SMALLINT:
        castFromInteger<int16_t>(input, context, rows, resultType, result);
        break;
      case TypeKind::INTEGER:
        castFromInteger<int32_t>(input, context, rows, resultType, result);
        break;
      case TypeKind::BIGINT:
        castFromInteger<int64_t>(input, context, rows, resultType, result);
        break;
      default:
        VELOX_UNREACHABLE(
            "Cannot cast {} to {} type",
            input.type()->toString(),
            resultType->toString());
    }
  }

  void castFrom(
      const BaseVector& input,
      exec::EvalCtx& context,
      const SelectivityVector& rows,
      const TypePtr& resultType,
      VectorPtr& result) const override {
    context.ensureWritable(rows, resultType, result);
    auto* flatResult = result->asChecked<FlatVector<int64_t>>();
    flatResult->copy(&input, rows, nullptr);
  }

 private:
  template <typename T>
  void castFromInteger(
      const BaseVector& input,
      exec::EvalCtx& context,
      const SelectivityVector& rows,
      const TypePtr& resultType,
      VectorPtr& result) const {
    context.ensureWritable(rows, resultType, result);
    auto* flatResult = result->asChecked<FlatVector<int64_t>>();
    flatResult->clearNulls(rows);

    auto enumType = asBigintEnum(resultType);
    const auto* intVector = input.as<SimpleVector<T>>();
    rows.applyToSelected([&](vector_size_t row) {
      const int64_t intToCast = intVector->valueAt(row);
      if (!enumType->containsValue(intToCast)) {
        context.setStatus(
            row,
            Status::UserError(
                "No value '{}' in {}", intToCast, enumType->enumName()));
        return;
      }
      flatResult->set(row, intToCast);
    });
  }
};

class BigintEnumTypeFactory : public CustomTypeFactory {
 public:
  TypePtr getType(const std::vector<TypeParameter>& parameters) const override {
    VELOX_CHECK_EQ(
        parameters.size(),
        1,
        "Expected exactly one type parameters for BigintEnumType");
    VELOX_CHECK(
        parameters[0].longEnumLiteral.has_value(),
        "BigintEnumType parameter must be longEnumLiteral");
    return BIGINT_ENUM(parameters[0].longEnumLiteral.value());
  }

  exec::CastOperatorPtr getCastOperator() const override {
    return BigintEnumCastOperator::get();
  }

  AbstractInputGeneratorPtr getInputGenerator(
      const InputGeneratorConfig& /*config*/) const override {
    return nullptr;
  }
};
} // namespace

void registerBigintEnumType() {
  registerCustomType(
      "bigint_enum", std::make_unique<const BigintEnumTypeFactory>());
}
} // namespace facebook::velox

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
#include "velox/type/Filter.h"

namespace facebook::velox::functions {
namespace {

template <typename T, typename U = T>
std::optional<std::pair<std::vector<T>, bool>> toValues(
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  std::vector<T> values;
  values.reserve(inputArgs.size());
  bool nullAllowed = false;

  // First input is the value to test. The rest are values of the in-list.
  for (auto i = 1; i < inputArgs.size(); i++) {
    const auto& constantValue = inputArgs[i].constantValue;
    if (!constantValue) {
      continue;
    }

    if (constantValue->typeKind() == TypeKind::UNKNOWN) {
      nullAllowed = true;
      continue;
    }

    auto constantInput =
        std::dynamic_pointer_cast<ConstantVector<U>>(constantValue);
    if (!constantInput) {
      return std::nullopt;
    }
    if (constantInput->isNullAt(0)) {
      nullAllowed = true;
    } else {
      values.emplace_back(constantInput->valueAt(0));
    }
  }

  // In case we didn't find any values (constant or null).
  if (values.empty() && !nullAllowed) {
    return std::nullopt;
  }

  return std::optional<std::pair<std::vector<T>, bool>>(
      {std::move(values), nullAllowed});
}

template <typename T>
std::unique_ptr<common::Filter> createBigintValuesFilter(
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  auto valuesPair = toValues<int64_t, T>(inputArgs);
  if (!valuesPair.has_value()) {
    return nullptr;
  }

  const auto& values = valuesPair.value().first;
  bool nullAllowed = valuesPair.value().second;

  VELOX_USER_CHECK(
      !values.empty(),
      "IN predicate expects at least one non-null value in the in-list");
  if (values.size() == 1) {
    return std::make_unique<common::BigintRange>(
        values[0], values[0], nullAllowed);
  }

  return common::createBigintValues(values, nullAllowed);
}

std::unique_ptr<common::Filter> createBytesValuesFilter(
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  auto valuesPair = toValues<std::string, StringView>(inputArgs);
  if (!valuesPair.has_value()) {
    return nullptr;
  }

  const auto& values = valuesPair.value().first;
  bool nullAllowed = valuesPair.value().second;

  VELOX_USER_CHECK(
      !values.empty(),
      "IN predicate expects at least one non-null value in the in-list");
  if (values.size() == 1) {
    return std::make_unique<common::BytesRange>(
        values[0], false, false, values[0], false, false, nullAllowed);
  }

  return std::make_unique<common::BytesValues>(values, nullAllowed);
}

class InPredicate : public exec::VectorFunction {
 public:
  explicit InPredicate(std::unique_ptr<common::Filter> filter)
      : filter_{std::move(filter)} {}

  static std::shared_ptr<InPredicate> create(
      const std::string& /*name*/,
      const std::vector<exec::VectorFunctionArg>& inputArgs) {
    VELOX_CHECK_GE(inputArgs.size(), 2);
    auto inListType = inputArgs[1].type;
    std::unique_ptr<common::Filter> filter;

    switch (inListType->kind()) {
      case TypeKind::BIGINT:
        filter = createBigintValuesFilter<int64_t>(inputArgs);
        break;
      case TypeKind::INTEGER:
        filter = createBigintValuesFilter<int32_t>(inputArgs);
        break;
      case TypeKind::SMALLINT:
        filter = createBigintValuesFilter<int16_t>(inputArgs);
        break;
      case TypeKind::TINYINT:
        filter = createBigintValuesFilter<int8_t>(inputArgs);
        break;
      case TypeKind::VARCHAR:
        filter = createBytesValuesFilter(inputArgs);
        break;
      default:
        VELOX_UNSUPPORTED(
            "Unsupported in-list type for IN predicate: {}",
            inListType->toString());
    }
    return std::make_shared<InPredicate>(std::move(filter));
  }

  // x IN (2, null) returns null when x != 2 and true when x == 2
  // null for x always produces null
  bool isDefaultNullBehavior() const override {
    return false;
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      exec::Expr* /* unused */,
      exec::EvalCtx* context,
      VectorPtr* result) const override {
    const auto& input = args[0];
    switch (input->typeKind()) {
      case TypeKind::BIGINT:
        applyTyped<int64_t>(rows, input, context, result, [&](int64_t value) {
          return filter_->testInt64(value);
        });
        break;
      case TypeKind::INTEGER:
        applyTyped<int32_t>(rows, input, context, result, [&](int32_t value) {
          return filter_->testInt64(value);
        });
        break;
      case TypeKind::SMALLINT:
        applyTyped<int16_t>(rows, input, context, result, [&](int16_t value) {
          return filter_->testInt64(value);
        });
        break;
      case TypeKind::TINYINT:
        applyTyped<int8_t>(rows, input, context, result, [&](int8_t value) {
          return filter_->testInt64(value);
        });
        break;
      case TypeKind::VARCHAR:
        applyTyped<StringView>(
            rows, input, context, result, [&](StringView value) {
              return filter_->testBytes(value.data(), value.size());
            });
        break;
      default:
        VELOX_UNSUPPORTED(
            "Unsupported input type for the IN predicate: {}",
            input->type()->toString());
    }
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // tinyint|smallint|integer|bigint|varchar... -> boolean
    std::vector<std::shared_ptr<exec::FunctionSignature>> signatures;
    for (auto& type : {"tinyint", "smallint", "integer", "bigint", "varchar"}) {
      signatures.emplace_back(exec::FunctionSignatureBuilder()
                                  .returnType("boolean")
                                  .argumentType(type)
                                  .argumentType(type)
                                  .variableArity()
                                  .build());
    }
    return signatures;
  }

 private:
  static VectorPtr createBoolConstantNull(
      vector_size_t size,
      exec::EvalCtx* context) {
    return BaseVector::createConstant(
        variant(TypeKind::BOOLEAN), size, context->pool());
  }

  static VectorPtr
  createBoolConstant(bool value, vector_size_t size, exec::EvalCtx* context) {
    return BaseVector::createConstant(value, size, context->pool());
  }

  template <typename T, typename F>
  void applyTyped(
      const SelectivityVector& rows,
      const VectorPtr& arg,
      exec::EvalCtx* context,
      VectorPtr* result,
      F testFunction) const {
    VELOX_CHECK(filter_, "IN predicate supports only constant IN list");

    // Indicates whether result can be true or null only, e.g. no false results.
    const bool passOrNull = filter_->testNull();

    exec::LocalDecodedVector holder(context, *arg, rows);
    auto decoder = holder.get();
    if (decoder->isConstantMapping()) {
      VectorPtr localResult;
      if (decoder->isNullAt(0)) {
        localResult = createBoolConstantNull(rows.size(), context);
      } else {
        bool pass = testFunction(decoder->valueAt<T>(0));
        if (!pass && passOrNull) {
          localResult = createBoolConstantNull(rows.size(), context);
        } else {
          localResult = createBoolConstant(pass, rows.size(), context);
        }
      }

      context->moveOrCopyResult(localResult, rows, result);
      return;
    }

    BaseVector::ensureWritable(rows, BOOLEAN(), context->pool(), result);
    auto boolResult = std::dynamic_pointer_cast<FlatVector<bool>>(*result);

    auto rawValues = boolResult->mutableRawValues<uint64_t>();

    if (decoder->mayHaveNulls() || passOrNull) {
      rows.applyToSelected([&](auto row) {
        if (decoder->isNullAt(row)) {
          boolResult->setNull(row, true);
        } else {
          bool pass = testFunction(decoder->valueAt<T>(row));
          if (!pass && passOrNull) {
            boolResult->setNull(row, true);
          } else {
            bits::setBit(rawValues, row, pass);
          }
        }
      });
    } else {
      rows.applyToSelected([&](auto row) {
        bool pass = testFunction(decoder->valueAt<T>(row));
        bits::setBit(rawValues, row, pass);
      });
    }
  }

  const std::unique_ptr<common::Filter> filter_;
};
} // namespace

VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_in,
    InPredicate::signatures(),
    InPredicate::create);
} // namespace facebook::velox::functions

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

// Read from innermost vector of type SimpleVector<U> and return vector<T>
// Results are de-duped
template <typename T, typename U = T>
std::optional<std::pair<std::vector<T>, bool>> toValues(
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  const auto& constantValue = inputArgs[1].constantValue;
  if (!constantValue) {
    return std::nullopt;
  }

  auto constantInput =
      std::dynamic_pointer_cast<ConstantVector<ComplexType>>(constantValue);
  if (constantInput->isNullAt(0)) {
    // The whole list is null. In will always be null.
    return std::optional<std::pair<std::vector<T>, bool>>({{}, true});
  }
  auto arrayVector = dynamic_cast<const ArrayVector*>(
      constantInput->valueVector()->wrappedVector());
  auto elementsVector = arrayVector->elements()->as<SimpleVector<U>>();
  auto offset = arrayVector->offsetAt(constantInput->index());
  auto size = arrayVector->sizeAt(constantInput->index());
  VELOX_USER_CHECK_GT(size, 0, "IN list must not be empty");

  bool nullAllowed = false;
  std::vector<T> values;
  values.reserve(size);

  for (auto i = offset; i < offset + size; i++) {
    if (elementsVector->isNullAt(i)) {
      nullAllowed = true;
    } else {
      values.emplace_back(elementsVector->valueAt(i));
    }
  }

  // In-place sort, remove duplicates, and later std::move to save memory
  std::sort(values.begin(), values.end());
  auto last = std::unique(values.begin(), values.end());
  values.resize(std::distance(values.begin(), last));

  return std::optional<std::pair<std::vector<T>, bool>>(
      {std::move(values), nullAllowed});
}

// Creates a filter for constant values. A null filter means either
// no values or only null values. The boolean is true if the list is
// non-empty and consists of nulls only.
template <typename T>
std::pair<std::unique_ptr<common::Filter>, bool> createBigintValuesFilter(
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  auto valuesPair = toValues<int64_t, T>(inputArgs);
  if (!valuesPair.has_value()) {
    return {nullptr, false};
  }

  const auto& values = valuesPair.value().first;
  bool nullAllowed = valuesPair.value().second;

  if (values.empty() && nullAllowed) {
    return {nullptr, true};
  }
  VELOX_USER_CHECK(
      !values.empty(),
      "IN predicate expects at least one non-null value in the in-list");
  if (values.size() == 1) {
    return {
        std::make_unique<common::BigintRange>(
            values[0], values[0], nullAllowed),
        false};
  }

  return {common::createBigintValues(values, nullAllowed), false};
}

// For double, cast double to Int64 and reuse Int64 filters
// For float, cast float to Int32 and promote to Int64
template <typename T>
std::pair<std::unique_ptr<common::Filter>, bool>
createFloatingPointValuesFilter(
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  auto valuesPair = toValues<T, T>(inputArgs);
  if (!valuesPair.has_value()) {
    return {nullptr, false};
  }

  auto& values = valuesPair.value().first;
  bool nullAllowed = valuesPair.value().second;

  if (values.empty() && nullAllowed) {
    return {nullptr, true};
  }
  VELOX_USER_CHECK(
      !values.empty(),
      "IN predicate expects at least one non-null value in the in-list");

  if (values.size() == 1) {
    return {
        std::make_unique<common::FloatingPointRange<T>>(
            values[0], false, false, values[0], false, false, nullAllowed),
        false};
  }

  std::vector<int64_t> intValues(values.size());
  for (size_t i = 0; i < values.size(); ++i) {
    if constexpr (std::is_same_v<T, float>) {
      if (values[i] == float{}) {
        values[i] = 0;
      }
      intValues[i] = reinterpret_cast<const int32_t&>(
          values[i]); // silently promote to int64
    } else {
      if (values[i] == double{}) {
        values[i] = 0;
      }
      intValues[i] = reinterpret_cast<const int64_t&>(values[i]);
    }
  }
  return {common::createBigintValues(intValues, nullAllowed), false};
}

// See createBigintValuesFilter.
template <typename T>
std::pair<std::unique_ptr<common::Filter>, bool> createHugeintValuesFilter(
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  auto valuesPair = toValues<int128_t, T>(inputArgs);
  if (!valuesPair.has_value()) {
    return {nullptr, false};
  }

  const auto& values = valuesPair.value().first;
  bool nullAllowed = valuesPair.value().second;

  if (values.empty() && nullAllowed) {
    return {nullptr, true};
  }
  VELOX_USER_CHECK(
      !values.empty(),
      "IN predicate expects at least one non-null value in the in-list");
  if (values.size() == 1) {
    return {
        std::make_unique<common::HugeintRange>(
            values[0], values[0], nullAllowed),
        false};
  }

  return {common::createHugeintValues(values, nullAllowed), false};
}

// See createBigintValuesFilter.
std::pair<std::unique_ptr<common::Filter>, bool> createBytesValuesFilter(
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  auto valuesPair = toValues<std::string, StringView>(inputArgs);
  if (!valuesPair.has_value()) {
    return {nullptr, false};
  }

  const auto& values = valuesPair.value().first;
  bool nullAllowed = valuesPair.value().second;
  if (values.empty() && nullAllowed) {
    return {nullptr, true};
  }

  VELOX_USER_CHECK(
      !values.empty(),
      "IN predicate expects at least one value in the in-list");
  if (values.size() == 1) {
    return {
        std::make_unique<common::BytesRange>(
            values[0], false, false, values[0], false, false, nullAllowed),
        false};
  }

  return {std::make_unique<common::BytesValues>(values, nullAllowed), false};
}

class InPredicate : public exec::VectorFunction {
 public:
  explicit InPredicate(std::unique_ptr<common::Filter> filter, bool alwaysNull)
      : filter_{std::move(filter)}, alwaysNull_(alwaysNull) {}

  static std::shared_ptr<InPredicate> create(
      const std::string& /*name*/,
      const std::vector<exec::VectorFunctionArg>& inputArgs,
      const core::QueryConfig& /*config*/) {
    VELOX_CHECK_EQ(inputArgs.size(), 2);
    auto inListType = inputArgs[1].type;
    VELOX_CHECK_EQ(inListType->kind(), TypeKind::ARRAY);
    std::pair<std::unique_ptr<common::Filter>, bool> filter;

    switch (inListType->childAt(0)->kind()) {
      case TypeKind::HUGEINT:
        filter = createHugeintValuesFilter<int128_t>(inputArgs);
        break;
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
      case TypeKind::REAL:
        filter = createFloatingPointValuesFilter<float>(inputArgs);
        break;
      case TypeKind::DOUBLE:
        filter = createFloatingPointValuesFilter<double>(inputArgs);
        break;
      case TypeKind::BOOLEAN:
        // Hack: using BIGINT filter for bool, which is essentially "int1_t".
        filter = createBigintValuesFilter<bool>(inputArgs);
        break;
      case TypeKind::VARCHAR:
      case TypeKind::VARBINARY:
        filter = createBytesValuesFilter(inputArgs);
        break;
      case TypeKind::UNKNOWN:
        filter = {nullptr, true};
        break;
      default:
        VELOX_UNSUPPORTED(
            "Unsupported in-list type for IN predicate: {}",
            inListType->toString());
    }
    return std::make_shared<InPredicate>(
        std::move(filter.first), filter.second);
  }

  // x IN (2, null) returns null when x != 2 and true when x == 2.
  // Null for x always produces null, regardless of 'IN' list.
  bool isDefaultNullBehavior() const override {
    return true;
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    const auto& input = args[0];
    switch (input->typeKind()) {
      case TypeKind::HUGEINT:
        applyTyped<int128_t>(rows, input, context, result, [&](int128_t value) {
          return filter_->testInt128(value);
        });
        break;
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
      case TypeKind::REAL:
        applyTyped<float>(rows, input, context, result, [&](float value) {
          auto* derived =
              dynamic_cast<common::FloatingPointRange<float>*>(filter_.get());
          if (derived) {
            return filter_->testFloat(value);
          }
          if (value == float{}) {
            value = 0;
          }
          return filter_->testInt64(reinterpret_cast<const int32_t&>(value));
        });
        break;
      case TypeKind::DOUBLE:
        applyTyped<double>(rows, input, context, result, [&](double value) {
          auto* derived =
              dynamic_cast<common::FloatingPointRange<double>*>(filter_.get());
          if (derived) {
            return filter_->testDouble(value);
          }
          if (value == double{}) {
            value = 0;
          }
          return filter_->testInt64(reinterpret_cast<const int64_t&>(value));
        });
        break;
      case TypeKind::BOOLEAN:
        applyTyped<bool>(rows, input, context, result, [&](bool value) {
          return filter_->testInt64(value);
        });
        break;
      case TypeKind::VARCHAR:
      case TypeKind::VARBINARY:
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
    // boolean|tinyint|smallint|integer|bigint|varchar... -> boolean
    std::vector<std::shared_ptr<exec::FunctionSignature>> signatures;
    for (auto& type :
         {"boolean",
          "tinyint",
          "smallint",
          "integer",
          "bigint",
          "varchar",
          "varbinary",
          "double",
          "real",
          "date"}) {
      signatures.emplace_back(exec::FunctionSignatureBuilder()
                                  .returnType("boolean")
                                  .argumentType(type)
                                  .argumentType(fmt::format("array({})", type))
                                  .build());
      signatures.emplace_back(exec::FunctionSignatureBuilder()
                                  .returnType("boolean")
                                  .argumentType(type)
                                  .argumentType("array(unknown)")
                                  .build());
    }
    signatures.emplace_back(
        exec::FunctionSignatureBuilder()
            .returnType("boolean")
            .integerVariable("precision")
            .integerVariable("scale")
            .argumentType("DECIMAL(precision, scale)")
            .argumentType("array(DECIMAL(precision, scale))")
            .build());
    signatures.emplace_back(exec::FunctionSignatureBuilder()
                                .returnType("boolean")
                                .integerVariable("precision")
                                .integerVariable("scale")
                                .argumentType("DECIMAL(precision, scale)")
                                .argumentType("array(unknown)")
                                .build());

    return signatures;
  }

 private:
  static VectorPtr createBoolConstantNull(
      vector_size_t size,
      exec::EvalCtx& context) {
    return std::make_shared<ConstantVector<bool>>(
        context.pool(), size, true /*isNull*/, BOOLEAN(), false);
  }

  static VectorPtr
  createBoolConstant(bool value, vector_size_t size, exec::EvalCtx& context) {
    return std::make_shared<ConstantVector<bool>>(
        context.pool(), size, false /*isNull*/, BOOLEAN(), std::move(value));
  }

  template <typename T, typename F>
  void applyTyped(
      const SelectivityVector& rows,
      const VectorPtr& arg,
      exec::EvalCtx& context,
      VectorPtr& result,
      F&& testFunction) const {
    if (alwaysNull_) {
      auto localResult = createBoolConstantNull(rows.end(), context);
      context.moveOrCopyResult(localResult, rows, result);
      return;
    }
    VELOX_CHECK(filter_, "IN predicate supports only constant IN list");

    // Indicates whether result can be true or null only, e.g. no false results.
    const bool passOrNull = filter_->testNull();

    if (arg->isConstantEncoding()) {
      auto simpleArg = arg->asUnchecked<SimpleVector<T>>();
      VectorPtr localResult;
      if (simpleArg->isNullAt(rows.begin())) {
        localResult = createBoolConstantNull(rows.end(), context);
      } else {
        bool pass = testFunction(simpleArg->valueAt(rows.begin()));
        if (!pass && passOrNull) {
          localResult = createBoolConstantNull(rows.end(), context);
        } else {
          localResult = createBoolConstant(pass, rows.end(), context);
        }
      }

      context.moveOrCopyResult(localResult, rows, result);
      return;
    }

    VELOX_CHECK_EQ(arg->encoding(), VectorEncoding::Simple::FLAT);
    auto flatArg = arg->asUnchecked<FlatVector<T>>();

    context.ensureWritable(rows, BOOLEAN(), result);
    result->clearNulls(rows);
    auto* boolResult = result->asUnchecked<FlatVector<bool>>();

    auto* rawResults = boolResult->mutableRawValues<uint64_t>();

    if (flatArg->mayHaveNulls() || passOrNull) {
      rows.applyToSelected([&](auto row) {
        if (flatArg->isNullAt(row)) {
          boolResult->setNull(row, true);
        } else {
          bool pass = testFunction(flatArg->valueAtFast(row));
          if (!pass && passOrNull) {
            boolResult->setNull(row, true);
          } else {
            bits::setBit(rawResults, row, pass);
          }
        }
      });
    } else {
      rows.applyToSelected([&](auto row) {
        bool pass = testFunction(flatArg->valueAtFast(row));
        bits::setBit(rawResults, row, pass);
      });
    }
  }

  const std::unique_ptr<common::Filter> filter_;
  const bool alwaysNull_;
};
} // namespace

VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_in,
    InPredicate::signatures(),
    InPredicate::create);
} // namespace facebook::velox::functions

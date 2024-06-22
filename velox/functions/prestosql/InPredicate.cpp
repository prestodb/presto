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
#include "velox/expression/DecodedArgs.h"
#include "velox/expression/VectorFunction.h"
#include "velox/type/Filter.h"

namespace facebook::velox::functions {
namespace {

// Returns NULL if
// - input value is NULL or contains NULL;
// - input value doesn't match any of the in-list values, but some of in-list
// values are NULL or contain NULL.
class ComplexTypeInPredicate : public exec::VectorFunction {
 public:
  struct ComplexValue {
    BaseVector* vector;
    vector_size_t index;
  };

  struct ComplexValueHash {
    size_t operator()(ComplexValue value) const {
      return value.vector->hashValueAt(value.index);
    }
  };

  struct ComplexValueEqualTo {
    bool operator()(ComplexValue left, ComplexValue right) const {
      return left.vector->equalValueAt(right.vector, left.index, right.index);
    }
  };

  using ComplexSet =
      folly::F14FastSet<ComplexValue, ComplexValueHash, ComplexValueEqualTo>;

  ComplexTypeInPredicate(
      ComplexSet uniqueValues,
      bool hasNull,
      VectorPtr originalValues)
      : uniqueValues_{std::move(uniqueValues)},
        hasNull_{hasNull},
        originalValues_{std::move(originalValues)} {}

  static std::shared_ptr<exec::VectorFunction>
  create(const VectorPtr& values, vector_size_t offset, vector_size_t size) {
    ComplexSet uniqueValues;
    bool hasNull = false;

    for (auto i = offset; i < offset + size; i++) {
      if (values->containsNullAt(i)) {
        hasNull = true;
      } else {
        uniqueValues.insert({values.get(), i});
      }
    }

    return std::make_shared<ComplexTypeInPredicate>(
        std::move(uniqueValues), hasNull, values);
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    const auto& arg = args[0];

    context.ensureWritable(rows, BOOLEAN(), result);
    result->clearNulls(rows);
    auto* boolResult = result->asUnchecked<FlatVector<bool>>();

    rows.applyToSelected([&](vector_size_t row) {
      if (arg->containsNullAt(row)) {
        boolResult->setNull(row, true);
      } else {
        const bool found = uniqueValues_.contains({arg.get(), row});
        if (!found && hasNull_) {
          boolResult->setNull(row, true);
        } else {
          boolResult->set(row, found);
        }
      }
    });
  }

 private:
  // Set of unique values to check against. This set doesn't include any value
  // that is null or contains null.
  const ComplexSet uniqueValues_;

  // Boolean indicating whether one of the value was null or contained null.
  const bool hasNull_;

  // Vector being referenced by the 'uniqueValues_'.
  const VectorPtr originalValues_;
};

// Read 'size' values from 'valuesVector' starting at 'offset', de-duplicate
// remove nulls and sort. Return a list of unique non-null values sorted in
// ascending order and a boolean indicating whether there were any null values.
template <typename T, typename U = T>
std::pair<std::vector<T>, bool> toValues(
    const VectorPtr& valuesVector,
    vector_size_t offset,
    vector_size_t size) {
  auto simpleValues = valuesVector->as<SimpleVector<U>>();

  bool nullAllowed = false;
  std::vector<T> values;
  values.reserve(size);

  for (auto i = offset; i < offset + size; i++) {
    if (simpleValues->isNullAt(i)) {
      nullAllowed = true;
    } else {
      if constexpr (std::is_same_v<U, Timestamp>) {
        values.emplace_back(simpleValues->valueAt(i).toMillis());
      } else {
        values.emplace_back(simpleValues->valueAt(i));
      }
    }
  }

  // In-place sort, remove duplicates, and later std::move to save memory
  std::sort(values.begin(), values.end());
  auto last = std::unique(values.begin(), values.end());
  values.resize(std::distance(values.begin(), last));

  return {std::move(values), nullAllowed};
}

// Creates a filter for constant values. A null filter means either
// no values or only null values. The boolean is true if the list is
// non-empty and consists of nulls only.
template <typename T>
std::pair<std::unique_ptr<common::Filter>, bool> createBigintValuesFilter(
    const VectorPtr& valuesVector,
    vector_size_t offset,
    vector_size_t size) {
  auto valuesPair = toValues<int64_t, T>(valuesVector, offset, size);

  const auto& values = valuesPair.first;
  bool nullAllowed = valuesPair.second;

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
    const VectorPtr& valuesVector,
    vector_size_t offset,
    vector_size_t size) {
  auto valuesPair = toValues<T, T>(valuesVector, offset, size);

  auto& values = valuesPair.first;
  bool nullAllowed = valuesPair.second;

  if (values.empty() && nullAllowed) {
    return {nullptr, true};
  }
  VELOX_USER_CHECK(
      !values.empty(),
      "IN predicate expects at least one non-null value in the in-list");
  // Avoid using FloatingPointRange for optimization of a single value in-list
  // as it does not support NaN as a bound for specifying a range.
  std::vector<int64_t> intValues(values.size());
  for (size_t i = 0; i < values.size(); ++i) {
    if (std::isnan(values[i])) {
      // We de-normalize NaN values to ensure different binary representations
      // are treated the same.
      values[i] = std::numeric_limits<T>::quiet_NaN();
    }
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
    const VectorPtr& valuesVector,
    vector_size_t offset,
    vector_size_t size) {
  auto valuesPair = toValues<int128_t, T>(valuesVector, offset, size);

  const auto& values = valuesPair.first;
  bool nullAllowed = valuesPair.second;

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
    const VectorPtr& valuesVector,
    vector_size_t offset,
    vector_size_t size) {
  auto valuesPair =
      toValues<std::string, StringView>(valuesVector, offset, size);

  const auto& values = valuesPair.first;
  bool nullAllowed = valuesPair.second;
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

/// x IN (2, null) returns null when x != 2 and true when x == 2.
/// Null for x always produces null, regardless of 'IN' list.
class InPredicate : public exec::VectorFunction {
 public:
  explicit InPredicate(std::unique_ptr<common::Filter> filter, bool alwaysNull)
      : filter_{std::move(filter)}, alwaysNull_(alwaysNull) {}

  static std::shared_ptr<exec::VectorFunction> create(
      const std::string& /*name*/,
      const std::vector<exec::VectorFunctionArg>& inputArgs,
      const core::QueryConfig& /*config*/) {
    VELOX_CHECK_GE(inputArgs.size(), 2);
    auto inListType = inputArgs[1].type;

    VELOX_CHECK_EQ(inListType->kind(), TypeKind::ARRAY);
    VELOX_CHECK_EQ(2, inputArgs.size());

    const auto& values = inputArgs[1].constantValue;
    VELOX_USER_CHECK_NOT_NULL(
        values, "IN predicate supports only constant IN list");

    if (values->isNullAt(0)) {
      return std::make_shared<InPredicate>(nullptr, true);
    }

    VELOX_CHECK_EQ(values->typeKind(), TypeKind::ARRAY);

    auto constantInput =
        std::dynamic_pointer_cast<ConstantVector<ComplexType>>(values);

    auto arrayVector = dynamic_cast<const ArrayVector*>(
        constantInput->valueVector()->wrappedVector());
    vector_size_t arrayIndex =
        constantInput->valueVector()->wrappedIndex(constantInput->index());
    auto offset = arrayVector->offsetAt(arrayIndex);
    auto size = arrayVector->sizeAt(arrayIndex);
    try {
      VELOX_USER_CHECK_GT(size, 0, "IN list must not be empty");
    } catch (...) {
      return std::make_shared<exec::AlwaysFailingVectorFunction>(
          std::current_exception());
    }

    const auto& elements = arrayVector->elements();

    std::pair<std::unique_ptr<common::Filter>, bool> filter;

    switch (inListType->childAt(0)->kind()) {
      case TypeKind::HUGEINT:
        filter = createHugeintValuesFilter<int128_t>(elements, offset, size);
        break;
      case TypeKind::BIGINT:
        filter = createBigintValuesFilter<int64_t>(elements, offset, size);
        break;
      case TypeKind::INTEGER:
        filter = createBigintValuesFilter<int32_t>(elements, offset, size);
        break;
      case TypeKind::SMALLINT:
        filter = createBigintValuesFilter<int16_t>(elements, offset, size);
        break;
      case TypeKind::TINYINT:
        filter = createBigintValuesFilter<int8_t>(elements, offset, size);
        break;
      case TypeKind::REAL:
        filter = createFloatingPointValuesFilter<float>(elements, offset, size);
        break;
      case TypeKind::DOUBLE:
        filter =
            createFloatingPointValuesFilter<double>(elements, offset, size);
        break;
      case TypeKind::BOOLEAN:
        // Hack: using BIGINT filter for bool, which is essentially "int1_t".
        filter = createBigintValuesFilter<bool>(elements, offset, size);
        break;
      case TypeKind::TIMESTAMP:
        filter = createBigintValuesFilter<Timestamp>(elements, offset, size);
        break;
      case TypeKind::VARCHAR:
      case TypeKind::VARBINARY:
        filter = createBytesValuesFilter(elements, offset, size);
        break;
      case TypeKind::UNKNOWN:
        filter = {nullptr, true};
        break;
      case TypeKind::ARRAY:
        [[fallthrough]];
      case TypeKind::MAP:
        [[fallthrough]];
      case TypeKind::ROW:
        return ComplexTypeInPredicate::create(elements, offset, size);
      default:
        VELOX_UNSUPPORTED(
            "Unsupported in-list type for IN predicate: {}",
            inListType->toString());
    }
    return std::make_shared<InPredicate>(
        std::move(filter.first), filter.second);
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    if (alwaysNull_) {
      auto localResult = createBoolConstantNull(rows.end(), context);
      context.moveOrCopyResult(localResult, rows, result);
      return;
    }

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
          if (value == float{}) {
            value = 0;
          } else if (std::isnan(value)) {
            // We de-normalize NaN values to ensure different binary
            // representations
            // are treated the same.
            value = std::numeric_limits<float>::quiet_NaN();
          }
          return filter_->testInt64(reinterpret_cast<const int32_t&>(value));
        });
        break;
      case TypeKind::DOUBLE:
        applyTyped<double>(rows, input, context, result, [&](double value) {
          if (value == double{}) {
            value = 0;
          } else if (std::isnan(value)) {
            // We de-normalize NaN values to ensure different binary
            // representations
            // are treated the same.
            value = std::numeric_limits<double>::quiet_NaN();
          }
          return filter_->testInt64(reinterpret_cast<const int64_t&>(value));
        });
        break;
      case TypeKind::BOOLEAN:
        applyTyped<bool>(rows, input, context, result, [&](bool value) {
          return filter_->testInt64(value);
        });
        break;
      case TypeKind::TIMESTAMP:
        applyTyped<Timestamp>(
            rows, input, context, result, [&](Timestamp value) {
              return filter_->testInt64(value.toMillis());
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
    return {
        // (T, array(T)) -> boolean for constant IN lists.
        exec::FunctionSignatureBuilder()
            .typeVariable("T")
            .returnType("boolean")
            .argumentType("T")
            .constantArgumentType("array(T)")
            .build(),
        exec::FunctionSignatureBuilder()
            .typeVariable("T")
            .returnType("boolean")
            .argumentType("T")
            .constantArgumentType("array(unknown)")
            .build(),
    };
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

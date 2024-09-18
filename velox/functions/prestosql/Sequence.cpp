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

#include <iostream>
#include "velox/expression/DecodedArgs.h"
#include "velox/expression/VectorFunction.h"
#include "velox/functions/prestosql/DateTimeImpl.h"
#include "velox/vector/ConstantVector.h"

namespace facebook::velox::functions {
namespace {

template <typename T>
int64_t toInt64(T value);

template <>
int64_t toInt64(int64_t value) {
  return value;
}

template <>
int64_t toInt64(int32_t value) {
  return value;
}

template <>
int64_t toInt64(Timestamp value) {
  return value.toMillis();
}

using Days = int64_t;
using Months = int32_t;
template <typename T, typename K>
T add(T value, K step, int32_t sequence);

template <>
int64_t add(int64_t value, int64_t step, int32_t sequence) {
  const auto delta = (int128_t)step * (int128_t)sequence;
  // Since step is calcuated from start and stop,
  // the sum of 'value' and 'add' is within int64_t.
  return value + delta;
}

template <>
int32_t add(int32_t value, int64_t step, int32_t sequence) {
  const auto delta = (int128_t)step * (int128_t)sequence;
  return value + delta;
}

template <>
Timestamp add(Timestamp value, int64_t step, int32_t sequence) {
  const auto delta = (int128_t)step * (int128_t)sequence;
  return Timestamp::fromMillis(value.toMillis() + delta);
}

template <>
int32_t add(int32_t value, Months step, int32_t sequence) {
  return addToDate(value, DateTimeUnit::kMonth, step * sequence);
}

template <>
Timestamp add(Timestamp value, Months step, int32_t sequence) {
  return addToTimestamp(value, DateTimeUnit::kMonth, step * sequence);
}

template <typename T>
int128_t getStepCount(T start, T end, int32_t step) {
  VELOX_FAIL("Unexpected start/end type for argument INTERVAL_YEAR_MONTH");
}

int128_t getStepCount(int32_t start, int32_t end, int32_t step) {
  return diffDate(DateTimeUnit::kMonth, start, end) / step + 1;
}

int128_t getStepCount(Timestamp start, Timestamp end, int32_t step) {
  return diffTimestamp(DateTimeUnit::kMonth, start, end) / step + 1;
}

// See documentation at https://prestodb.io/docs/current/functions/array.html
template <typename T, typename K>
class SequenceFunction : public exec::VectorFunction {
 public:
  static constexpr int32_t kMaxResultEntries = 10'000;

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    exec::DecodedArgs decodedArgs(rows, args, context);
    auto startVector = decodedArgs.at(0);
    auto stopVector = decodedArgs.at(1);
    DecodedVector* stepVector = nullptr;
    bool isIntervalYearMonth = false;
    if (args.size() == 3) {
      stepVector = decodedArgs.at(2);
      isIntervalYearMonth = args[2]->type()->isIntervalYearMonth();
    }

    const auto numRows = rows.end();
    auto pool = context.pool();
    vector_size_t numElements = 0;

    BufferPtr sizes = allocateSizes(numRows, pool);
    BufferPtr offsets = allocateOffsets(numRows, pool);
    auto rawSizes = sizes->asMutable<vector_size_t>();
    auto rawOffsets = offsets->asMutable<vector_size_t>();

    const bool isDate = args[0]->type()->isDate();
    context.applyToSelectedNoThrow(rows, [&](auto row) {
      rawSizes[row] = checkArguments(
          startVector,
          stopVector,
          stepVector,
          row,
          isDate,
          isIntervalYearMonth);
      numElements += rawSizes[row];
    });

    VectorPtr elements =
        BaseVector::create(outputType->childAt(0), numElements, pool);
    auto rawElements = elements->asFlatVector<T>()->mutableRawValues();

    vector_size_t elementsOffset = 0;
    context.applyToSelectedNoThrow(rows, [&](auto row) {
      const auto sequenceCount = rawSizes[row];
      if (sequenceCount) {
        rawOffsets[row] = elementsOffset;
        writeToElements(
            rawElements + elementsOffset,
            isDate,
            isIntervalYearMonth,
            sequenceCount,
            startVector,
            stopVector,
            stepVector,
            row);
        elementsOffset += rawSizes[row];
      }
    });
    context.moveOrCopyResult(
        std::make_shared<ArrayVector>(
            pool, outputType, nullptr, numRows, offsets, sizes, elements),
        rows,
        result);
  }

 private:
  static vector_size_t checkArguments(
      DecodedVector* startVector,
      DecodedVector* stopVector,
      DecodedVector* stepVector,
      vector_size_t row,
      bool isDate,
      bool isYearMonth) {
    T start = startVector->valueAt<T>(row);
    T stop = stopVector->valueAt<T>(row);
    auto step = getStep(
        toInt64(start), toInt64(stop), stepVector, row, isDate, isYearMonth);
    VELOX_USER_CHECK_NE(step, 0, "step must not be zero");
    VELOX_USER_CHECK(
        step > 0 ? stop >= start : stop <= start,
        "sequence stop value should be greater than or equal to start value if "
        "step is greater than zero otherwise stop should be less than or equal to start");
    int128_t sequenceCount;
    if (isYearMonth) {
      sequenceCount = getStepCount(start, stop, step);
    } else {
      sequenceCount =
          ((int128_t)toInt64(stop) - (int128_t)toInt64(start)) / step +
          1; // prevent overflow
    }
    VELOX_USER_CHECK_LE(
        sequenceCount,
        kMaxResultEntries,
        "result of sequence function must not have more than 10000 entries");
    return sequenceCount;
  }

  static void writeToElements(
      T* elements,
      bool isDate,
      bool isYearMonth,
      vector_size_t sequenceCount,
      DecodedVector* startVector,
      DecodedVector* stopVector,
      DecodedVector* stepVector,
      vector_size_t row) {
    auto start = startVector->valueAt<T>(row);
    auto stop = stopVector->valueAt<T>(row);
    auto step = getStep(
        toInt64(start), toInt64(stop), stepVector, row, isDate, isYearMonth);
    for (auto sequence = 0; sequence < sequenceCount; ++sequence) {
      elements[sequence] = add(start, step, sequence);
    }
  }

  static K getStep(
      int64_t start,
      int64_t stop,
      DecodedVector* stepVector,
      vector_size_t row,
      bool isDate,
      bool isYearMonth) {
    if (!stepVector) {
      return (stop >= start ? 1 : -1);
    }
    auto step = stepVector->valueAt<K>(row);
    if (!isDate || isYearMonth) {
      return step;
    }
    // Handle Date
    VELOX_USER_CHECK(
        step % kMillisInDay == 0,
        "sequence step must be a day interval if start and end values are dates");
    return step / kMillisInDay;
  }
};

std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
  std::vector<std::shared_ptr<exec::FunctionSignature>> signatures;
  signatures = {
      exec::FunctionSignatureBuilder()
          .returnType("array(bigint)")
          .argumentType("bigint")
          .argumentType("bigint")
          .build(),
      exec::FunctionSignatureBuilder()
          .returnType("array(bigint)")
          .argumentType("bigint")
          .argumentType("bigint")
          .argumentType("bigint")
          .build(),
      exec::FunctionSignatureBuilder()
          .returnType("array(date)")
          .argumentType("date")
          .argumentType("date")
          .build(),
      exec::FunctionSignatureBuilder()
          .returnType("array(date)")
          .argumentType("date")
          .argumentType("date")
          .argumentType("interval day to second")
          .build(),
      exec::FunctionSignatureBuilder()
          .returnType("array(date)")
          .argumentType("date")
          .argumentType("date")
          .argumentType("interval year to month")
          .build(),
      exec::FunctionSignatureBuilder()
          .returnType("array(timestamp)")
          .argumentType("timestamp")
          .argumentType("timestamp")
          .argumentType("interval day to second")
          .build(),
      exec::FunctionSignatureBuilder()
          .returnType("array(timestamp)")
          .argumentType("timestamp")
          .argumentType("timestamp")
          .argumentType("interval year to month")
          .build()};
  return signatures;
}

std::shared_ptr<exec::VectorFunction> create(
    const std::string& /* name */,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/) {
  if (inputArgs[0].type->isDate()) {
    if (inputArgs.size() > 2 && inputArgs[2].type->isIntervalYearMonth()) {
      return std::make_shared<SequenceFunction<int32_t, int32_t>>();
    }
    return std::make_shared<SequenceFunction<int32_t, int64_t>>();
  }

  switch (inputArgs[0].type->kind()) {
    case TypeKind::BIGINT:
      return std::make_shared<SequenceFunction<int64_t, int64_t>>();
    case TypeKind::TIMESTAMP:
      if (inputArgs.size() > 2 && inputArgs[2].type->isIntervalYearMonth()) {
        return std::make_shared<SequenceFunction<Timestamp, int32_t>>();
      }
      return std::make_shared<SequenceFunction<Timestamp, int64_t>>();
    default:
      VELOX_UNREACHABLE();
  }
}
} // namespace

VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION(udf_sequence, signatures(), create);
} // namespace facebook::velox::functions

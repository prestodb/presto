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

#pragma once

#include "velox/common/base/IOUtils.h"
#include "velox/exec/Aggregate.h"
#include "velox/type/HugeInt.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::functions::aggregate {

/**
 *  LongDecimalWithOverflowState has the following fields:
 *    SUM: Total sum so far.
 *    COUNT: Total number of rows so far.
 *    OVERFLOW: Total count of net overflow or underflow so far.
 */
struct LongDecimalWithOverflowState {
 public:
  void mergeWith(const StringView& serializedData) {
    VELOX_CHECK_EQ(serializedData.size(), serializedSize());
    auto serialized = serializedData.data();
    common::InputByteStream stream(serialized);
    count += stream.read<int64_t>();
    overflow += stream.read<int64_t>();
    uint64_t lowerSum = stream.read<uint64_t>();
    int64_t upperSum = stream.read<int64_t>();
    overflow += DecimalUtil::addWithOverflow(
        this->sum, HugeInt::build(upperSum, lowerSum), this->sum);
  }

  void serialize(StringView& serialized) {
    VELOX_CHECK_EQ(serialized.size(), serializedSize());
    char* outputBuffer = const_cast<char*>(serialized.data());
    common::OutputByteStream outStream(outputBuffer);
    outStream.append((char*)&count, sizeof(int64_t));
    outStream.append((char*)&overflow, sizeof(int64_t));
    uint64_t lower = HugeInt::lower(sum);
    int64_t upper = HugeInt::upper(sum);
    outStream.append((char*)&lower, sizeof(int64_t));
    outStream.append((char*)&upper, sizeof(int64_t));
  }

  /*
   * Total size = sizeOf(count) + sizeOf(overflow) + sizeOf(sum)
   *            = 8 + 8 + 16 = 32.
   */
  inline static size_t serializedSize() {
    return sizeof(int64_t) * 4;
  }

  int128_t sum{0};
  int64_t count{0};
  int64_t overflow{0};
};

template <typename TResultType, typename TInputType = TResultType>
class DecimalAggregate : public exec::Aggregate {
 public:
  explicit DecimalAggregate(TypePtr resultType) : exec::Aggregate(resultType) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(LongDecimalWithOverflowState);
  }

  int32_t accumulatorAlignmentSize() const override {
    return static_cast<int32_t>(sizeof(int128_t));
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decodedRaw_.decode(*args[0], rows);
    if (decodedRaw_.isConstantMapping()) {
      if (!decodedRaw_.isNullAt(0)) {
        auto value = decodedRaw_.valueAt<TInputType>(0);
        rows.applyToSelected([&](vector_size_t i) {
          updateNonNullValue(groups[i], TResultType(value));
        });
      }
    } else if (decodedRaw_.mayHaveNulls()) {
      rows.applyToSelected([&](vector_size_t i) {
        if (decodedRaw_.isNullAt(i)) {
          return;
        }
        updateNonNullValue(
            groups[i], TResultType(decodedRaw_.valueAt<TInputType>(i)));
      });
    } else if (!exec::Aggregate::numNulls_ && decodedRaw_.isIdentityMapping()) {
      auto data = decodedRaw_.data<TInputType>();
      rows.applyToSelected([&](vector_size_t i) {
        updateNonNullValue<false>(groups[i], TResultType(data[i]));
      });
    } else {
      rows.applyToSelected([&](vector_size_t i) {
        updateNonNullValue(
            groups[i], TResultType(decodedRaw_.valueAt<TInputType>(i)));
      });
    }
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decodedRaw_.decode(*args[0], rows);
    if (decodedRaw_.isConstantMapping()) {
      if (!decodedRaw_.isNullAt(0)) {
        auto value = decodedRaw_.valueAt<TInputType>(0);
        rows.template applyToSelected([&](vector_size_t i) {
          updateNonNullValue(group, TResultType(value));
        });
      }
    } else if (decodedRaw_.mayHaveNulls()) {
      rows.applyToSelected([&](vector_size_t i) {
        if (!decodedRaw_.isNullAt(i)) {
          updateNonNullValue(
              group, TResultType(decodedRaw_.valueAt<TInputType>(i)));
        }
      });
    } else if (!exec::Aggregate::numNulls_ && decodedRaw_.isIdentityMapping()) {
      const TInputType* data = decodedRaw_.data<TInputType>();
      LongDecimalWithOverflowState accumulator;
      rows.applyToSelected([&](vector_size_t i) {
        accumulator.overflow += DecimalUtil::addWithOverflow(
            accumulator.sum, data[i], accumulator.sum);
      });
      accumulator.count = rows.countSelected();
      char rawData[LongDecimalWithOverflowState::serializedSize()];
      StringView serialized(
          rawData, LongDecimalWithOverflowState::serializedSize());
      accumulator.serialize(serialized);
      mergeAccumulators<false>(group, serialized);
    } else {
      LongDecimalWithOverflowState accumulator;
      rows.applyToSelected([&](vector_size_t i) {
        accumulator.overflow += DecimalUtil::addWithOverflow(
            accumulator.sum,
            decodedRaw_.valueAt<TInputType>(i),
            accumulator.sum);
      });
      accumulator.count = rows.countSelected();
      char rawData[LongDecimalWithOverflowState::serializedSize()];
      StringView serialized(
          rawData, LongDecimalWithOverflowState::serializedSize());
      accumulator.serialize(serialized);
      mergeAccumulators(group, serialized);
    }
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    decodedPartial_.decode(*args[0], rows);
    auto intermediateFlatVector =
        dynamic_cast<const FlatVector<StringView>*>(decodedPartial_.base());
    if (decodedPartial_.isConstantMapping()) {
      if (!decodedPartial_.isNullAt(0)) {
        auto decodedIndex = decodedPartial_.index(0);
        auto serializedAccumulator =
            intermediateFlatVector->valueAt(decodedIndex);
        rows.applyToSelected([&](vector_size_t i) {
          clearNull(groups[i]);
          auto accumulator = decimalAccumulator(groups[i]);
          accumulator->mergeWith(serializedAccumulator);
        });
      }
    } else if (decodedPartial_.mayHaveNulls()) {
      rows.applyToSelected([&](vector_size_t i) {
        if (decodedPartial_.isNullAt(i)) {
          return;
        }
        clearNull(groups[i]);
        auto decodedIndex = decodedPartial_.index(i);
        auto serializedAccumulator =
            intermediateFlatVector->valueAt(decodedIndex);
        auto accumulator = decimalAccumulator(groups[i]);
        accumulator->mergeWith(serializedAccumulator);
      });
    } else {
      rows.applyToSelected([&](vector_size_t i) {
        clearNull(groups[i]);
        auto decodedIndex = decodedPartial_.index(i);
        auto serializedAccumulator =
            intermediateFlatVector->valueAt(decodedIndex);
        auto accumulator = decimalAccumulator(groups[i]);
        accumulator->mergeWith(serializedAccumulator);
      });
    }
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    decodedPartial_.decode(*args[0], rows);
    auto intermediateFlatVector =
        dynamic_cast<const FlatVector<StringView>*>(decodedPartial_.base());

    if (decodedPartial_.isConstantMapping()) {
      if (!decodedPartial_.isNullAt(0)) {
        auto decodedIndex = decodedPartial_.index(0);
        auto serializedAccumulator =
            intermediateFlatVector->valueAt(decodedIndex);
        if (rows.hasSelections()) {
          clearNull(group);
        }
        rows.applyToSelected([&](vector_size_t i) {
          mergeAccumulators(group, serializedAccumulator);
        });
      }
    } else if (decodedPartial_.mayHaveNulls()) {
      rows.applyToSelected([&](vector_size_t i) {
        if (decodedPartial_.isNullAt(i)) {
          return;
        }
        clearNull(group);
        auto decodedIndex = decodedPartial_.index(i);
        auto serializedAccumulator =
            intermediateFlatVector->valueAt(decodedIndex);
        mergeAccumulators(group, serializedAccumulator);
      });
    } else {
      if (rows.hasSelections()) {
        clearNull(group);
      }
      rows.applyToSelected([&](vector_size_t i) {
        auto decodedIndex = decodedPartial_.index(i);
        auto serializedAccumulator =
            intermediateFlatVector->valueAt(decodedIndex);
        mergeAccumulators(group, serializedAccumulator);
      });
    }
  }

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto stringViewVector = (*result)->as<FlatVector<StringView>>();
    stringViewVector->resize(numGroups);
    uint64_t* rawNulls = nullptr;
    rawNulls = getRawNulls(stringViewVector);
    for (auto i = 0; i < numGroups; ++i) {
      auto accumulator = decimalAccumulator(groups[i]);
      if (isNull(groups[i])) {
        stringViewVector->setNull(i, true);
      } else {
        clearNull(rawNulls, i);
        auto size = accumulator->serializedSize();
        char* rawBuffer = stringViewVector->getRawStringBufferWithSpace(size);
        StringView serialized(rawBuffer, size);
        accumulator->serialize(serialized);
        stringViewVector->setNoCopy(i, serialized);
      }
    }
  }

  virtual TResultType computeFinalValue(
      LongDecimalWithOverflowState* accumulator) = 0;

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto vector = (*result)->as<FlatVector<TResultType>>();
    VELOX_CHECK(vector);
    vector->resize(numGroups);
    uint64_t* rawNulls = getRawNulls(vector);

    TResultType* rawValues = vector->mutableRawValues();
    for (int32_t i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      if (isNull(group)) {
        vector->setNull(i, true);
      } else {
        clearNull(rawNulls, i);
        auto accumulator = decimalAccumulator(group);
        rawValues[i] = computeFinalValue(accumulator);
      }
    }
  }

  template <bool tableHasNulls = true>
  void mergeAccumulators(char* group, const StringView& serialized) {
    if constexpr (tableHasNulls) {
      exec::Aggregate::clearNull(group);
    }
    auto accumulator = decimalAccumulator(group);
    accumulator->mergeWith(serialized);
  }

  template <bool tableHasNulls = true>
  void updateNonNullValue(char* group, TResultType value) {
    if constexpr (tableHasNulls) {
      exec::Aggregate::clearNull(group);
    }
    auto accumulator = decimalAccumulator(group);
    accumulator->overflow +=
        DecimalUtil::addWithOverflow(accumulator->sum, value, accumulator->sum);
    accumulator->count += 1;
  }

 protected:
  void initializeNewGroupsInternal(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    setAllNulls(groups, indices);
    for (auto i : indices) {
      new (groups[i] + offset_) LongDecimalWithOverflowState();
    }
  }

 private:
  inline LongDecimalWithOverflowState* decimalAccumulator(char* group) {
    return exec::Aggregate::value<LongDecimalWithOverflowState>(group);
  }

  DecodedVector decodedRaw_;
  DecodedVector decodedPartial_;
};

} // namespace facebook::velox::functions::aggregate

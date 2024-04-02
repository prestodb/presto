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

#include "velox/functions/sparksql/aggregates/BloomFilterAggAggregate.h"

#include "velox/common/base/BloomFilter.h"
#include "velox/exec/Aggregate.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::functions::aggregate::sparksql {

namespace {

struct BloomFilterAccumulator {
  explicit BloomFilterAccumulator(HashStringAllocator* allocator)
      : bloomFilter{StlAllocator<uint64_t>(allocator)} {}

  int32_t serializedSize() const {
    return bloomFilter.serializedSize();
  }

  void serialize(char* output) const {
    return bloomFilter.serialize(output);
  }

  void mergeWith(StringView& serialized) {
    bloomFilter.merge(serialized.data());
  }

  bool initialized() const {
    return bloomFilter.isSet();
  }

  void init(int32_t capacity) {
    if (!bloomFilter.isSet()) {
      bloomFilter.reset(capacity);
    }
  }

  void insert(int64_t value) {
    bloomFilter.insert(folly::hasher<int64_t>()(value));
  }

  BloomFilter<StlAllocator<uint64_t>> bloomFilter;
};

class BloomFilterAggAggregate : public exec::Aggregate {
 public:
  explicit BloomFilterAggAggregate(
      const TypePtr& resultType,
      const core::QueryConfig& config)
      : Aggregate(resultType),
        defaultExpectedNumItems_(config.sparkBloomFilterExpectedNumItems()),
        defaultNumBits_(config.sparkBloomFilterNumBits()),
        maxNumBits_(config.sparkBloomFilterMaxNumBits()) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(BloomFilterAccumulator);
  }

  bool isFixedSize() const override {
    return false;
  }

  static FOLLY_ALWAYS_INLINE void checkBloomFilterNotNull(
      DecodedVector& decoded,
      vector_size_t idx) {
    VELOX_USER_CHECK(
        !decoded.isNullAt(idx),
        "First argument of bloom_filter_agg cannot be null");
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decodeArguments(rows, args);
    computeCapacity();
    auto mayHaveNulls = decodedRaw_.mayHaveNulls();
    rows.applyToSelected([&](vector_size_t row) {
      if (mayHaveNulls) {
        checkBloomFilterNotNull(decodedRaw_, row);
      }
      auto group = groups[row];
      auto tracker = trackRowSize(group);
      auto accumulator = value<BloomFilterAccumulator>(group);
      accumulator->init(capacity_);
      accumulator->insert(decodedRaw_.valueAt<int64_t>(row));
    });
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    VELOX_CHECK_EQ(args.size(), 1);
    decodedIntermediate_.decode(*args[0], rows);
    rows.applyToSelected([&](auto row) {
      if (UNLIKELY(decodedIntermediate_.isNullAt(row))) {
        return;
      }
      auto group = groups[row];
      auto tracker = trackRowSize(group);
      auto serialized = decodedIntermediate_.valueAt<StringView>(row);
      auto accumulator = value<BloomFilterAccumulator>(group);
      accumulator->mergeWith(serialized);
    });
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decodeArguments(rows, args);
    computeCapacity();
    auto tracker = trackRowSize(group);
    auto accumulator = value<BloomFilterAccumulator>(group);
    accumulator->init(capacity_);
    if (decodedRaw_.isConstantMapping()) {
      // All values are same, just do for the first.
      checkBloomFilterNotNull(decodedRaw_, 0);
      accumulator->insert(decodedRaw_.valueAt<int64_t>(0));
      return;
    }
    auto mayHaveNulls = decodedRaw_.mayHaveNulls();
    rows.applyToSelected([&](vector_size_t row) {
      if (mayHaveNulls) {
        checkBloomFilterNotNull(decodedRaw_, row);
      }
      accumulator->insert(decodedRaw_.valueAt<int64_t>(row));
    });
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    VELOX_CHECK_EQ(args.size(), 1);
    decodedIntermediate_.decode(*args[0], rows);
    auto tracker = trackRowSize(group);
    auto accumulator = value<BloomFilterAccumulator>(group);
    rows.applyToSelected([&](auto row) {
      if (UNLIKELY(decodedIntermediate_.isNullAt(row))) {
        return;
      }
      auto serialized = decodedIntermediate_.valueAt<StringView>(row);
      accumulator->mergeWith(serialized);
    });
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    VELOX_CHECK(result);
    auto flatResult = (*result)->asUnchecked<FlatVector<StringView>>();
    flatResult->resize(numGroups);

    int32_t totalSize = getTotalSize(groups, numGroups);
    char* rawBuffer = flatResult->getRawStringBufferWithSpace(totalSize);
    for (vector_size_t i = 0; i < numGroups; ++i) {
      auto group = groups[i];
      auto accumulator = value<BloomFilterAccumulator>(group);
      if (UNLIKELY(!accumulator->initialized())) {
        flatResult->setNull(i, true);
        continue;
      }

      auto size = accumulator->serializedSize();
      VELOX_DCHECK(!StringView::isInline(size));
      accumulator->serialize(rawBuffer);
      StringView serialized = StringView(rawBuffer, size);
      rawBuffer += size;
      flatResult->setNoCopy(i, serialized);
    }
  }

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    extractValues(groups, numGroups, result);
  }

 protected:
  void initializeNewGroupsInternal(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    setAllNulls(groups, indices);
    for (auto i : indices) {
      new (groups[i] + offset_) BloomFilterAccumulator(allocator_);
    }
  }

 private:
  void decodeArguments(
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args) {
    VELOX_USER_CHECK(args.size() > 0);
    decodedRaw_.decode(*args[0], rows);
    if (args.size() > 1) {
      DecodedVector decodedEstimatedNumItems(*args[1], rows);
      setConstantArgument(
          "estimatedNumItems", estimatedNumItems_, decodedEstimatedNumItems);
      if (args.size() > 2) {
        VELOX_CHECK_EQ(args.size(), 3);
        DecodedVector decodedNumBits(*args[2], rows);
        setConstantArgument("numBits", numBits_, decodedNumBits);
      } else {
        numBits_ = estimatedNumItems_ * 8;
      }
    } else {
      estimatedNumItems_ = defaultExpectedNumItems_;
      numBits_ = defaultNumBits_;
    }
  }

  void computeCapacity() {
    if (capacity_ == kMissingArgument) {
      int64_t numBits = std::min(numBits_, maxNumBits_);
      capacity_ = numBits / 16;
    }
  }

  int32_t getTotalSize(char** groups, int32_t numGroups) const {
    int32_t totalSize = 0;
    for (vector_size_t i = 0; i < numGroups; ++i) {
      auto group = groups[i];
      auto accumulator = value<BloomFilterAccumulator>(group);
      if (UNLIKELY(!accumulator->initialized())) {
        continue;
      }

      auto size = accumulator->serializedSize();
      VELOX_DCHECK(!StringView::isInline(size));
      totalSize += size;
    }
    return totalSize;
  }

  static void setConstantArgument(
      const char* name,
      int64_t& currentValue,
      const DecodedVector& vector) {
    VELOX_CHECK(
        vector.isConstantMapping(),
        "{} argument must be constant for all input rows",
        name);
    int64_t newValue = vector.valueAt<int64_t>(0);
    VELOX_USER_CHECK_GT(newValue, 0, "{} must be positive", name);
    if (currentValue == kMissingArgument) {
      currentValue = newValue;
    } else {
      VELOX_USER_CHECK_EQ(
          newValue,
          currentValue,
          "{} argument must be constant for all input rows",
          name);
    }
  }

  static constexpr int64_t kMissingArgument = -1;
  const int64_t defaultExpectedNumItems_;
  const int64_t defaultNumBits_;
  const int64_t maxNumBits_;

  // Reusable instance of DecodedVector for decoding input vectors.
  DecodedVector decodedRaw_;
  DecodedVector decodedIntermediate_;
  int64_t estimatedNumItems_ = kMissingArgument;
  int64_t numBits_ = kMissingArgument;
  int32_t capacity_ = kMissingArgument;
};

} // namespace

exec::AggregateRegistrationResult registerBloomFilterAggAggregate(
    const std::string& name,
    bool withCompanionFunctions,
    bool overwrite) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures{
      exec::AggregateFunctionSignatureBuilder()
          .argumentType("bigint")
          .constantArgumentType("bigint")
          .constantArgumentType("bigint")
          .intermediateType("varbinary")
          .returnType("varbinary")
          .build(),
      exec::AggregateFunctionSignatureBuilder()
          .argumentType("bigint")
          .constantArgumentType("bigint")
          .intermediateType("varbinary")
          .returnType("varbinary")
          .build(),
      exec::AggregateFunctionSignatureBuilder()
          .argumentType("bigint")
          .intermediateType("varbinary")
          .returnType("varbinary")
          .build()};

  return exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step /* step */,
          const std::vector<TypePtr>& /* argTypes */,
          const TypePtr& resultType,
          const core::QueryConfig& config) -> std::unique_ptr<exec::Aggregate> {
        return std::make_unique<BloomFilterAggAggregate>(resultType, config);
      },
      withCompanionFunctions,
      overwrite);
}
} // namespace facebook::velox::functions::aggregate::sparksql

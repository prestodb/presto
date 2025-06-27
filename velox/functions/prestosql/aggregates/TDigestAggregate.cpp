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
// #include <functional>
#include "velox/common/memory/HashStringAllocator.h"
#include "velox/exec/Aggregate.h"
#include "velox/functions/lib/TDigest.h"
#include "velox/functions/prestosql/aggregates/AggregateNames.h"
#include "velox/vector/FlatVector.h"

using namespace facebook::velox::exec;

namespace facebook::velox::aggregate::prestosql {

namespace {

struct TDigestAccumulator {
  explicit TDigestAccumulator(HashStringAllocator* allocator)
      : digest(StlAllocator<double>(allocator)) {}

  double compression = 0.0;
  facebook::velox::functions::TDigest<StlAllocator<double>> digest;
};

template <typename T>
class TDigestAggregate : public exec::Aggregate {
 private:
  bool hasWeight_;
  bool hasCompression_;
  double compression = 0.0;
  DecodedVector decodedValue_;
  DecodedVector decodedWeight_;
  DecodedVector decodedCompression_;

 public:
  TDigestAggregate(
      bool hasWeight,
      bool hasCompression,
      const TypePtr& resultType)
      : exec::Aggregate(resultType),
        hasWeight_{hasWeight},
        hasCompression_{hasCompression} {}

  void decodeArguments(
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args) {
    size_t argIndex = 0;
    decodedValue_.decode(*args[argIndex++], rows, true);
    if (hasWeight_) {
      decodedWeight_.decode(*args[argIndex++], rows, true);
    }
    if (hasCompression_) {
      decodedCompression_.decode(*args[argIndex++], rows, true);
    }
    VELOX_CHECK_EQ(argIndex, args.size());
  }

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(TDigestAccumulator);
  }

  int32_t accumulatorAlignmentSize() const override {
    return alignof(TDigestAccumulator);
  }

  bool isFixedSize() const override {
    return false;
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    extractCommon(groups, numGroups, result);
  }

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    extractCommon(groups, numGroups, result);
  }

  TDigestAccumulator* getAccumulator(char* group) {
    return value<TDigestAccumulator>(group);
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decodeArguments(rows, args);
    std::vector<int16_t> positions;
    rows.applyToSelected([&](vector_size_t row) {
      if (decodedValue_.isNullAt(row)) {
        return;
      }
      double value = decodedValue_.valueAt<double>(row);
      if (std::isnan(value)) {
        VELOX_USER_FAIL("Cannot add NaN to t-digest");
      }
      auto* accumulator = getAccumulator(groups[row]);
      if (hasCompression_) {
        checkAndSetCompression(accumulator, row);
      }
      if (hasWeight_) {
        if (!decodedWeight_.isNullAt(row)) {
          int64_t weight = decodedWeight_.valueAt<int64_t>(row);
          VELOX_USER_CHECK_GT(weight, 0, "weight must be > 0");
          accumulator->digest.add(positions, value, weight);
        } else {
          VELOX_USER_FAIL("Weight value must be greater than zero.");
        }
      } else {
        accumulator->digest.add(positions, value);
      }
    });
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    addIntermediate<false>(groups, rows, args);
  }

  template <bool kSingleGroup>
  void addIntermediate(
      std::conditional_t<kSingleGroup, char*, char**> group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args) {
    DecodedVector decodedSerializedDigests;
    TDigestAccumulator* accumulator = nullptr;
    decodedSerializedDigests.decode(*args[0], rows);
    std::vector<int16_t> positions;
    rows.applyToSelected([&](vector_size_t row) {
      // Skip null serialized digests
      if (decodedSerializedDigests.isNullAt(row)) {
        return;
      }
      if constexpr (kSingleGroup) {
        if (!accumulator) {
          accumulator = getAccumulator(group);
        }
      } else {
        accumulator = getAccumulator(group[row]);
      }
      auto serialized =
          decodedSerializedDigests.valueAt<facebook::velox::StringView>(row);
      accumulator->digest.mergeDeserialized(positions, serialized.data());
    });
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decodeArguments(rows, args);
    auto* accumulator = getAccumulator(group);
    std::vector<int16_t> positions;
    rows.applyToSelected([&](vector_size_t row) {
      if (decodedValue_.isNullAt(row)) {
        return;
      }
      double value = decodedValue_.valueAt<double>(row);
      if (std::isnan(value)) {
        VELOX_USER_FAIL("Cannot add NaN to t-digest");
      }
      if (hasCompression_) {
        checkAndSetCompression(accumulator, row);
      }
      if (hasWeight_) {
        if (!decodedWeight_.isNullAt(row)) {
          int64_t weight = decodedWeight_.valueAt<int64_t>(row);
          VELOX_USER_CHECK_GT(weight, 0, "weight must be > 0");
          accumulator->digest.add(positions, value, weight);
        } else {
          VELOX_USER_FAIL("Weight value must be greater than zero.");
        }
      } else {
        accumulator->digest.add(positions, value);
      }
    });
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    addIntermediate<true>(group, rows, args);
  }

 protected:
  void initializeNewGroupsInternal(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    exec::Aggregate::setAllNulls(groups, indices);
    for (auto i : indices) {
      auto group = groups[i];
      new (group + offset_) TDigestAccumulator(allocator_);
    }
  }

  void destroyInternal(folly::Range<char**> groups) override {
    for (auto group : groups) {
      if (isInitialized(group)) {
        value<TDigestAccumulator>(group)->~TDigestAccumulator();
      }
    }
  }

  void checkAndSetCompression(
      TDigestAccumulator* accumulator,
      vector_size_t row) {
    double compressionValue = decodedCompression_.valueAt<double>(row);
    VELOX_USER_CHECK(
        !std::isnan(compressionValue), "Compression factor must not be NaN.");
    VELOX_USER_CHECK_GT(
        compressionValue, 0, "Compression factor must be positive.");
    VELOX_USER_CHECK_LE(
        compressionValue, 1000, "Compression must be at most 1000");
    // Ensure compression is at least 10.
    compressionValue = std::max(compressionValue, 10.0);
    // Set compression if not set
    if (compression == 0.0) {
      compression = compressionValue;
    } else if (compression != compressionValue) {
      VELOX_USER_FAIL("Compression factor must be same for all rows");
    }
    if (accumulator->compression == 0.0) {
      accumulator->compression = compression;
    } else if (accumulator->compression != compressionValue) {
      VELOX_USER_FAIL("Compression factor must be same for all rows");
    }
    // Set compression at most once.
    if (accumulator->digest.compression() != compression) {
      accumulator->digest.setCompression(compression);
    }
  }

  void extractCommon(char** groups, int32_t numGroups, VectorPtr* result) {
    // If there are no groups, ensure the result vector is empty or
    // appropriately initialized
    if (numGroups == 0) {
      (*result)->resize(0);
      return;
    }
    auto flatResult = (*result)->asFlatVector<facebook::velox::StringView>();
    flatResult->resize(numGroups);
    std::vector<int16_t> positions;
    for (int32_t i = 0; i < numGroups; ++i) {
      auto group = groups[i];
      if (!group) {
        flatResult->setNull(i, true);
        continue;
      }
      auto accumulator = getAccumulator(group);
      if ((!isInitialized(group)) || accumulator->digest.size() == 0) {
        flatResult->setNull(i, true);
        continue;
      }
      accumulator->digest.compress(positions);
      auto size = accumulator->digest.serializedByteSize();
      facebook::velox::StringView serialized;
      if (facebook::velox::StringView::isInline(size)) {
        std::string buffer(size, '\0');
        accumulator->digest.serialize(buffer.data());
        serialized = facebook::velox::StringView::makeInline(buffer);
      } else {
        char* rawBuffer = flatResult->getRawStringBufferWithSpace(size);
        accumulator->digest.serialize(rawBuffer);
        serialized = facebook::velox::StringView(rawBuffer, size);
      }
      flatResult->setNoCopy(i, serialized);
    }
  }
};
} // namespace

void registerTDigestAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  std::vector<std::shared_ptr<AggregateFunctionSignature>> signatures;
  for (const auto& signature :
       {AggregateFunctionSignatureBuilder()
            .returnType("tdigest(double)")
            .intermediateType("varbinary")
            .argumentType("double")
            .build(),
        AggregateFunctionSignatureBuilder()
            .returnType("tdigest(double)")
            .intermediateType("varbinary")
            .argumentType("double")
            .argumentType("bigint")
            .build(),
        AggregateFunctionSignatureBuilder()
            .returnType("tdigest(double)")
            .intermediateType("varbinary")
            .argumentType("double")
            .argumentType("bigint")
            .argumentType("double")
            .build()}) {
    signatures.push_back(signature);
  }
  auto name = prefix + kTDigestAgg;
  exec::registerAggregateFunction(
      name,
      signatures,
      [name](
          core::AggregationNode::Step /*step*/,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultTypes,
          const core::QueryConfig& /*config*/)
          -> std::unique_ptr<exec::Aggregate> {
        if (argTypes.empty() || argTypes[0]->kind() != TypeKind::DOUBLE) {
          VELOX_USER_FAIL(
              "The first argument of {} must be of type DOUBLE", name);
        }
        bool hasWeight =
            argTypes.size() >= 2 && argTypes[1]->kind() == TypeKind::BIGINT;
        bool hasCompression =
            argTypes.size() >= 3 && argTypes[2]->kind() == TypeKind::DOUBLE;
        VELOX_USER_CHECK_EQ(
            argTypes.size(),
            1 + hasWeight + hasCompression,
            "Wrong number of arguments passed to {}",
            name);
        if (hasWeight) {
          VELOX_USER_CHECK_EQ(
              argTypes[1]->kind(),
              TypeKind::BIGINT,
              "The type of the weight argument of {} must be BIGINT",
              name);
        }
        if (hasCompression) {
          VELOX_USER_CHECK_EQ(
              argTypes[2]->kind(),
              TypeKind::DOUBLE,
              "The type of the compression argument of {} must be DOUBLE",
              name);
        }
        return std::make_unique<TDigestAggregate<double>>(
            hasWeight, hasCompression, resultTypes);
      },
      {true /*orderSensitive*/, false /*companionFunction*/},
      false /*companionFunction*/,
      overwrite);
}
} // namespace facebook::velox::aggregate::prestosql

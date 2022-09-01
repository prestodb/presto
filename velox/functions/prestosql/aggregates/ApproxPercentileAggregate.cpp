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
#include "velox/common/base/IOUtils.h"
#include "velox/common/base/Macros.h"
#include "velox/common/base/RandomUtil.h"
#include "velox/common/memory/HashStringAllocator.h"
#include "velox/exec/Aggregate.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/functions/lib/KllSketch.h"
#include "velox/functions/prestosql/aggregates/AggregateNames.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::aggregate {
namespace {

template <typename T>
using KllSketch = functions::kll::KllSketch<T, StlAllocator<T>>;

// Accumulator to buffer large count values in addition to the KLL
// sketch itself.
template <typename T>
struct KllSketchAccumulator {
  explicit KllSketchAccumulator(HashStringAllocator* allocator)
      : allocator_(allocator),
        sketch_(
            functions::kll::kDefaultK,
            StlAllocator<T>(allocator),
            random::getSeed()),
        largeCountValues_(StlAllocator<std::pair<T, int64_t>>(allocator)) {}

  void setAccuracy(double value) {
    k_ = functions::kll::kFromEpsilon(value);
    sketch_.setK(k_);
  }

  void append(T value) {
    sketch_.insert(value);
  }

  void append(T value, int64_t count) {
    constexpr size_t kMaxBufferSize = 4096;
    constexpr int64_t kMinCountToBuffer = 512;
    if (count < kMinCountToBuffer) {
      for (int i = 0; i < count; ++i) {
        sketch_.insert(value);
      }
    } else {
      largeCountValues_.emplace_back(value, count);
      if (largeCountValues_.size() >= kMaxBufferSize) {
        flush();
      }
    }
  }

  void append(const char* deserializedSketch) {
    sketch_.mergeDeserialized(deserializedSketch);
  }

  void append(const std::vector<const char*>& sketches) {
    sketch_.mergeDeserialized(folly::Range(sketches.begin(), sketches.end()));
  }

  void finalize() {
    if (!largeCountValues_.empty()) {
      flush();
    }
    sketch_.compact();
  }

  const KllSketch<T>& getSketch() {
    return sketch_;
  }

 private:
  uint16_t k_;
  HashStringAllocator* allocator_;
  KllSketch<T> sketch_;
  std::vector<std::pair<T, int64_t>, StlAllocator<std::pair<T, int64_t>>>
      largeCountValues_;

  void flush() {
    std::vector<KllSketch<T>> sketches;
    sketches.reserve(largeCountValues_.size());
    for (auto [x, n] : largeCountValues_) {
      sketches.push_back(KllSketch<T>::fromRepeatedValue(
          x, n, k_, StlAllocator<T>(allocator_), random::getSeed()));
    }
    sketch_.merge(folly::Range(sketches.begin(), sketches.end()));
    largeCountValues_.clear();
  }
};

// The following variations are possible:
//  x, percentile
//  x, weight, percentile
//  x, percentile, accuracy
//  x, weight, percentile, accuracy
template <typename T>
class ApproxPercentileAggregate : public exec::Aggregate {
 public:
  ApproxPercentileAggregate(
      bool hasWeight,
      bool hasAccuracy,
      const TypePtr& resultType)
      : exec::Aggregate(resultType),
        hasWeight_{hasWeight},
        hasAccuracy_(hasAccuracy) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(KllSketchAccumulator<T>);
  }

  bool isFixedSize() const override {
    return false;
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    exec::Aggregate::setAllNulls(groups, indices);
    for (auto i : indices) {
      auto group = groups[i];
      new (group + offset_) KllSketchAccumulator<T>(allocator_);
    }
  }

  void destroy(folly::Range<char**> groups) override {
    for (auto group : groups) {
      value<KllSketchAccumulator<T>>(group)->~KllSketchAccumulator<T>();
    }
  }

  void finalize(char** groups, int32_t numGroups) override {
    for (auto i = 0; i < numGroups; ++i) {
      value<KllSketchAccumulator<T>>(groups[i])->finalize();
    }
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    VELOX_CHECK(result);
    auto flatResult = (*result)->asFlatVector<T>();

    extract(
        groups,
        numGroups,
        flatResult,
        [&](const KllSketch<T>& digest,
            FlatVector<T>* result,
            vector_size_t index) {
          result->set(index, digest.estimateQuantile(percentile_));
        });
  }

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    VELOX_CHECK(result);
    auto flatResult = (*result)->asFlatVector<StringView>();

    extract(
        groups,
        numGroups,
        flatResult,
        [&](const KllSketch<T>& digest,
            FlatVector<StringView>* result,
            vector_size_t index) { serializeDigest(digest, result, index); });
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decodeArguments(rows, args);
    checkSetPercentile();
    checkSetAccuracy();

    if (hasWeight_) {
      rows.applyToSelected([&](auto row) {
        if (decodedValue_.isNullAt(row) || decodedWeight_.isNullAt(row)) {
          return;
        }

        auto tracker = trackRowSize(groups[row]);
        auto accumulator = initRawAccumulator(groups[row]);
        auto value = decodedValue_.valueAt<T>(row);
        auto weight = decodedWeight_.valueAt<int64_t>(row);
        VELOX_USER_CHECK_GE(
            weight,
            1,
            "The value of the weight parameter must be greater than or equal to 1.");
        accumulator->append(value, weight);
      });
    } else {
      if (decodedValue_.mayHaveNulls()) {
        rows.applyToSelected([&](auto row) {
          if (decodedValue_.isNullAt(row)) {
            return;
          }

          auto accumulator = initRawAccumulator(groups[row]);
          accumulator->append(decodedValue_.valueAt<T>(row));
        });
      } else {
        rows.applyToSelected([&](auto row) {
          auto accumulator = initRawAccumulator(groups[row]);
          accumulator->append(decodedValue_.valueAt<T>(row));
        });
      }
    }
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decodedDigest_.decode(*args[0], rows, true);

    rows.applyToSelected([&](auto row) {
      if (decodedDigest_.isNullAt(row)) {
        return;
      }
      auto tracker = trackRowSize(groups[row]);
      auto accumulator = value<KllSketchAccumulator<T>>(groups[row]);
      auto digest = getDeserializedDigest(row);
      if (accuracy_ != kMissingNormalizedValue) {
        accumulator->setAccuracy(accuracy_);
      }
      accumulator->append(digest);
    });
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decodeArguments(rows, args);
    checkSetPercentile();
    checkSetAccuracy();

    auto tracker = trackRowSize(group);
    auto accumulator = initRawAccumulator(group);

    if (hasWeight_) {
      rows.applyToSelected([&](auto row) {
        if (decodedValue_.isNullAt(row) || decodedWeight_.isNullAt(row)) {
          return;
        }

        auto value = decodedValue_.valueAt<T>(row);
        auto weight = decodedWeight_.valueAt<int64_t>(row);
        VELOX_USER_CHECK_GE(
            weight,
            1,
            "The value of the weight parameter must be greater than or equal to 1.");
        accumulator->append(value, weight);
      });
    } else {
      if (decodedValue_.mayHaveNulls()) {
        rows.applyToSelected([&](auto row) {
          if (decodedValue_.isNullAt(row)) {
            return;
          }

          accumulator->append(decodedValue_.valueAt<T>(row));
        });
      } else {
        rows.applyToSelected([&](auto row) {
          accumulator->append(decodedValue_.valueAt<T>(row));
        });
      }
    }
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decodedDigest_.decode(*args[0], rows, true);
    auto accumulator = value<KllSketchAccumulator<T>>(group);

    auto tracker = trackRowSize(group);
    std::vector<const char*> digests;
    digests.reserve(rows.end());

    rows.applyToSelected([&](auto row) {
      if (decodedDigest_.isNullAt(row)) {
        return;
      }
      digests.push_back(getDeserializedDigest(row));
    });

    if (!digests.empty()) {
      if (accuracy_ != kMissingNormalizedValue) {
        accumulator->setAccuracy(accuracy_);
      }
      accumulator->append(digests);
    }
  }

 private:
  template <typename ExtractResult, typename ExtractFunc>
  void extract(
      char** groups,
      int32_t numGroups,
      FlatVector<ExtractResult>* result,
      ExtractFunc extractFunction) {
    VELOX_CHECK(result);
    result->resize(numGroups);

    uint64_t* rawNulls = nullptr;
    if (result->mayHaveNulls()) {
      BufferPtr nulls = result->mutableNulls(result->size());
      rawNulls = nulls->asMutable<uint64_t>();
    }

    for (auto i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      auto accumulator = value<KllSketchAccumulator<T>>(group);
      if (accumulator->getSketch().totalCount() == 0) {
        result->setNull(i, true);
      } else {
        if (rawNulls) {
          bits::clearBit(rawNulls, i);
        }
        extractFunction(accumulator->getSketch(), result, i);
      }
    }
  }

  void decodeArguments(
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args) {
    size_t argIndex = 0;
    decodedValue_.decode(*args[argIndex++], rows, true);
    if (hasWeight_) {
      decodedWeight_.decode(*args[argIndex++], rows, true);
    }
    decodedPercentile_.decode(*args[argIndex++], rows, true);
    if (hasAccuracy_) {
      decodedAccuracy_.decode(*args[argIndex++], rows, true);
    }
    VELOX_CHECK_EQ(argIndex, args.size());
  }

  void checkSetPercentile() {
    VELOX_CHECK(
        decodedPercentile_.isConstantMapping(),
        "Percentile argument must be constant for all input rows");

    auto percentile = decodedPercentile_.valueAt<double>(0);
    checkSetPercentile(percentile);
  }

  void checkSetPercentile(double percentile) {
    VELOX_USER_CHECK_GE(percentile, 0, "Percentile must be between 0 and 1");
    VELOX_USER_CHECK_LE(percentile, 1, "Percentile must be between 0 and 1");

    if (percentile_ == kMissingNormalizedValue) {
      percentile_ = percentile;
    } else {
      VELOX_USER_CHECK_EQ(
          percentile,
          percentile_,
          "Percentile argument must be constant for all input rows");
    }
  }

  void checkSetAccuracy() {
    if (!hasAccuracy_) {
      return;
    }
    VELOX_CHECK(
        decodedAccuracy_.isConstantMapping(),
        "Accuracy argument must be constant for all input rows");
    checkSetAccuracy(decodedAccuracy_.valueAt<double>(0));
  }

  void checkSetAccuracy(double accuracy) {
    VELOX_USER_CHECK(
        0 < accuracy && accuracy <= 1, "Accuracy must be between 0 and 1");
    if (accuracy_ == kMissingNormalizedValue) {
      accuracy_ = accuracy;
    } else {
      VELOX_USER_CHECK_EQ(
          accuracy,
          accuracy_,
          "Accuracy argument must be constant for all input rows");
    }
  }

  KllSketchAccumulator<T>* initRawAccumulator(char* group) {
    auto accumulator = value<KllSketchAccumulator<T>>(group);
    if (hasAccuracy_) {
      accumulator->setAccuracy(accuracy_);
    }
    return accumulator;
  }

  void serializeDigest(
      const KllSketch<T>& digest,
      FlatVector<StringView>* result,
      vector_size_t index) {
    auto size =
        sizeof percentile_ + sizeof accuracy_ + digest.serializedByteSize();
    Buffer* buffer = result->getBufferWithSpace(size);
    char* data = buffer->asMutable<char>() + buffer->size();
    common::OutputByteStream stream(data);
    stream.appendOne(percentile_);
    stream.appendOne(accuracy_);
    digest.serialize(data + stream.offset());
    buffer->setSize(buffer->size() + size);
    result->setNoCopy(index, StringView(data, size));
  }

  const char* getDeserializedDigest(vector_size_t row) {
    auto data = decodedDigest_.valueAt<StringView>(row);
    common::InputByteStream stream(data.data());
    auto percentile = stream.read<double>();
    checkSetPercentile(percentile);
    if (auto accuracy = stream.read<double>();
        accuracy != kMissingNormalizedValue) {
      checkSetAccuracy(accuracy);
    }
    // If 'data' is inline, this function will return a local
    // address. Assert data is not inline.
    VELOX_DCHECK(!data.isInline());
    // Some compilers cannot deduce that the StringView cannot be inline from
    // the assert above. Suppress warning.
    VELOX_SUPPRESS_RETURN_LOCAL_ADDR_WARNING
    return data.data() + stream.offset();
    VELOX_UNSUPPRESS_RETURN_LOCAL_ADDR_WARNING
  }

  static constexpr double kMissingNormalizedValue = -1;
  const bool hasWeight_;
  const bool hasAccuracy_;
  double percentile_{kMissingNormalizedValue};
  double accuracy_{kMissingNormalizedValue};
  DecodedVector decodedValue_;
  DecodedVector decodedWeight_;
  DecodedVector decodedPercentile_;
  DecodedVector decodedAccuracy_;
  DecodedVector decodedDigest_;
};

bool registerApproxPercentile(const std::string& name) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;
  for (const auto& inputType :
       {"tinyint", "smallint", "integer", "bigint", "real", "double"}) {
    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .returnType(inputType)
                             .intermediateType("varbinary")
                             .argumentType(inputType)
                             .argumentType("double")
                             .build());

    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .returnType(inputType)
                             .intermediateType("varbinary")
                             .argumentType(inputType)
                             .argumentType("bigint")
                             .argumentType("double")
                             .build());

    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .returnType(inputType)
                             .intermediateType("varbinary")
                             .argumentType(inputType)
                             .argumentType("double")
                             .argumentType("double")
                             .build());

    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .returnType(inputType)
                             .intermediateType("varbinary")
                             .argumentType(inputType)
                             .argumentType("bigint")
                             .argumentType("double")
                             .argumentType("double")
                             .build());
  }
  exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType) -> std::unique_ptr<exec::Aggregate> {
        auto isRawInput = exec::isRawInput(step);
        auto hasWeight =
            argTypes.size() >= 2 && argTypes[1]->kind() == TypeKind::BIGINT;
        bool hasAccuracy = argTypes.size() == (hasWeight ? 4 : 3);

        if (isRawInput) {
          VELOX_USER_CHECK_EQ(
              argTypes.size(),
              2 + hasWeight + hasAccuracy,
              "Wrong number of arguments passed to {}",
              name);
          if (hasWeight) {
            VELOX_USER_CHECK_EQ(
                argTypes[1]->kind(),
                TypeKind::BIGINT,
                "The type of the weight argument of {} must be BIGINT",
                name);
          }
          if (hasAccuracy) {
            VELOX_USER_CHECK_EQ(
                argTypes.back()->kind(),
                TypeKind::DOUBLE,
                "The type of the accuracy argument of {} must be DOUBLE",
                name);
          }
          VELOX_USER_CHECK_EQ(
              argTypes[argTypes.size() - 1 - hasAccuracy]->kind(),
              TypeKind::DOUBLE,
              "The type of the percentile argument of {} must be DOUBLE",
              name);
        } else {
          VELOX_USER_CHECK_EQ(
              argTypes.size(),
              1,
              "The type of partial result for {} must be VARBINARY",
              name);
          VELOX_USER_CHECK_GE(
              argTypes[0]->kind(),
              TypeKind::VARBINARY,
              "The type of partial result for {} must be VARBINARY",
              name);
        }

        if (!isRawInput && exec::isPartialOutput(step)) {
          return std::make_unique<ApproxPercentileAggregate<double>>(
              false, false, VARBINARY());
        }

        TypePtr type = isRawInput ? argTypes[0] : resultType;

        switch (type->kind()) {
          case TypeKind::TINYINT:
            return std::make_unique<ApproxPercentileAggregate<int8_t>>(
                hasWeight, hasAccuracy, resultType);
          case TypeKind::SMALLINT:
            return std::make_unique<ApproxPercentileAggregate<int16_t>>(
                hasWeight, hasAccuracy, resultType);
          case TypeKind::INTEGER:
            return std::make_unique<ApproxPercentileAggregate<int32_t>>(
                hasWeight, hasAccuracy, resultType);
          case TypeKind::BIGINT:
            return std::make_unique<ApproxPercentileAggregate<int64_t>>(
                hasWeight, hasAccuracy, resultType);
          case TypeKind::REAL:
            return std::make_unique<ApproxPercentileAggregate<float>>(
                hasWeight, hasAccuracy, resultType);
          case TypeKind::DOUBLE:
            return std::make_unique<ApproxPercentileAggregate<double>>(
                hasWeight, hasAccuracy, resultType);
          default:
            VELOX_USER_FAIL(
                "Unsupported input type for {} aggregation {}",
                name,
                type->toString());
        }
      });
  return true;
}

static bool FB_ANONYMOUS_VARIABLE(g_AggregateFunction) =
    registerApproxPercentile(kApproxPercentile);

} // namespace
} // namespace facebook::velox::aggregate

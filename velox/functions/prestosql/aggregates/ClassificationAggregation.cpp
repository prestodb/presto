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

#include "velox/functions/prestosql/aggregates/ClassificationAggregation.h"
#include "velox/common/base/IOUtils.h"
#include "velox/exec/Aggregate.h"
#include "velox/functions/prestosql/aggregates/AggregateNames.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::aggregate::prestosql {
namespace {

enum class ClassificationType {
  kFallout = 0,
  kPrecision = 1,
  kRecall = 2,
  kMissRate = 3,
  kThresholds = 4,
};

/// Struct to represent the bucket of the FixedDoubleHistogram
/// at a given index.
struct Bucket {
  Bucket(double _left, double _right, double _weight)
      : left(_left), right(_right), weight(_weight) {}
  const double left;
  const double right;
  const double weight;
};

/// Fixed-bucket histogram of weights as doubles. For each bucket, it stores the
/// total weight accumulated.
class FixedDoubleHistogram {
 public:
  explicit FixedDoubleHistogram(HashStringAllocator* allocator)
      : weights_(StlAllocator<double>(allocator)) {}

  void resizeWeights() {
    validateParameters(bucketCount_, min_, max_);
    weights_.resize(bucketCount_);
  }

  /// API to support the case when bucket is created without a bucketCount
  /// count.
  void tryInit(int64_t bucketCount) {
    if (bucketCount_ == -1) {
      bucketCount_ = bucketCount;
      resizeWeights();
    }
  }

  /// Add weight to bucket based on the value of the prediction.
  void add(double pred, double weight) {
    if (weight == 0) {
      return;
    }
    if (weight < 0) {
      VELOX_USER_FAIL("Weight must be non-negative.");
    }
    if (pred < kMinPredictionValue || pred > kMaxPredictionValue) {
      VELOX_USER_FAIL(
          "Prediction value must be between {} and {}",
          kMinPredictionValue,
          kMaxPredictionValue);
    }
    auto index = getIndexForValue(bucketCount_, min_, max_, pred);
    weights_.at(index) += weight;
    totalWeights_ += weight;
    maxUsedIndex_ = std::max(maxUsedIndex_, index);
  }

  /// Returns a bucket in this histogram at a given index.
  Bucket getBucket(int64_t index) {
    return Bucket(
        getLeftValueForIndex(bucketCount_, min_, max_, index),
        getRightValueForIndex(bucketCount_, min_, max_, index),
        weights_.at(index));
  }

  /// The size of the histogram is represented by maxUsedIndex_, which
  /// represents the largest index in the buckets with a non-zero accrued value.
  /// This helps us avoid O(n) operation for the size of the histogram.
  int64_t size() const {
    return maxUsedIndex_ + 1;
  }

  int64_t bucketCount() const {
    return bucketCount_;
  }

  /// The state of the histogram can be serialized into a buffer. The format is
  /// represented as [header][bucketCount][min][max][weights]. The header is
  /// used to identify the version of the serialization format. The bucketCount,
  /// min, and max are used to represent the parameters of the histogram.
  /// Weights are the number of weights (equal to number of buckets) in the
  /// histogram.
  size_t serialize(char* output) const {
    VELOX_CHECK(output);
    common::OutputByteStream stream(output);
    size_t bytesUsed = 0;
    stream.append(
        reinterpret_cast<const char*>(&kSerializationVersionHeader),
        sizeof(kSerializationVersionHeader));
    bytesUsed += sizeof(kSerializationVersionHeader);

    stream.append(
        reinterpret_cast<const char*>(&bucketCount_), sizeof(bucketCount_));
    bytesUsed += sizeof(bucketCount_);

    stream.append(reinterpret_cast<const char*>(&min_), sizeof(min_));
    bytesUsed += sizeof(min_);

    stream.append(reinterpret_cast<const char*>(&max_), sizeof(max_));
    bytesUsed += sizeof(max_);

    for (auto weight : weights_) {
      stream.append(reinterpret_cast<const char*>(&weight), sizeof(weight));
      bytesUsed += sizeof(weight);
    }

    return bytesUsed;
  }

  /// Merges the current histogram with another histogram represented as a
  /// buffer.
  void mergeWith(const char* data, size_t expectedSize) {
    auto input = common::InputByteStream(data);
    deserialize(*this, input, expectedSize);
  }

  size_t serializationSize() const {
    return sizeof(kSerializationVersionHeader) + sizeof(bucketCount_) +
        sizeof(min_) + sizeof(max_) + (weights_.size() * sizeof(double));
  }

  /// This represents the total accrued weights in the bucket. The value is
  /// cached to avoid recomputing it every time it is needed.
  double totalWeights() const {
    return totalWeights_;
  }

 private:
  /// Deserializes the histogram from a buffer.
  static void deserialize(
      FixedDoubleHistogram& histogram,
      common::InputByteStream& in,
      size_t expectedSize) {
    if (FOLLY_UNLIKELY(expectedSize < minDeserializedBufferSize())) {
      VELOX_USER_FAIL(
          "Cannot deserialize FixedDoubleHistogram. Expected size: {}, actual size: {}",
          minDeserializedBufferSize(),
          expectedSize);
    }

    uint8_t version;
    in.copyTo(&version, 1);
    VELOX_CHECK_EQ(version, kSerializationVersionHeader);

    int64_t bucketCount;
    double min;
    double max;
    in.copyTo(&bucketCount, 1);
    in.copyTo(&min, 1);
    in.copyTo(&max, 1);

    /// This accounts for the case when the histogram is not initialized yet.

    if (histogram.bucketCount_ == -1) {
      histogram.bucketCount_ = bucketCount;
      histogram.min_ = min;
      histogram.max_ = max;
      histogram.resizeWeights();
    } else {
      /// When merging histograms, all the parameters except for the values
      /// accrued inside the buckets must be the same.
      if (histogram.bucketCount_ != bucketCount) {
        VELOX_USER_FAIL(
            "Bucket count must be constant."
            "Left bucket count: {}, right bucket count: {}",
            histogram.bucketCount_,
            bucketCount);
      }

      if (histogram.min_ != min || histogram.max_ != max) {
        VELOX_USER_FAIL(
            "Cannot merge histograms with different min/max values. "
            "Left min: {}, left max: {}, right min: {}, right max: {}",
            histogram.min_,
            histogram.max_,
            min,
            max);
      }
    }

    for (int64_t i = 0; i < bucketCount; ++i) {
      double weight;
      in.copyTo(&weight, 1);
      histogram.weights_[i] += weight;
      histogram.totalWeights_ += weight;
      if (weight != 0) {
        histogram.maxUsedIndex_ = std::max(histogram.maxUsedIndex_, i);
      }
    }
    const size_t bytesRead = sizeof(kSerializationVersionHeader) +
        sizeof(bucketCount) + sizeof(min) + sizeof(max) +
        (bucketCount * sizeof(double));
    VELOX_CHECK_EQ(bytesRead, expectedSize);
    return;
  }

  /// The minimium size of a valid buffer to deserialize a histogram.
  static constexpr size_t minDeserializedBufferSize() {
    return (
        sizeof(kSerializationVersionHeader) + sizeof(int64_t) + sizeof(double) +
        /// 2 Reresents the minimum number of buckets.
        sizeof(double) + 2 * sizeof(double));
  }

  /// Returns the index of the bucket in the histogram that contains the
  /// value. This is done by mapping value to [min, max) and then mapping that
  /// value to the corresponding bucket.
  static int64_t
  getIndexForValue(int64_t bucketCount, double min, double max, double value) {
    VELOX_CHECK(
        value >= min && value <= max,
        fmt::format(
            "Value must be within range: {} [{}, {}]", value, min, max));
    return std::min(
        static_cast<int64_t>((bucketCount * (value - min)) / (max - min)),
        bucketCount - 1);
  }

  static double getLeftValueForIndex(
      int64_t bucketCount,
      double min,
      double max,
      int64_t index) {
    return min + index * (max - min) / bucketCount;
  }

  static double getRightValueForIndex(
      int64_t bucketCount,
      double min,
      double max,
      int64_t index) {
    return std::min(
        max, getLeftValueForIndex(bucketCount, min, max, index + 1));
  }

  void validateParameters(int64_t bucketCount, double min, double max) {
    VELOX_CHECK_LE(bucketCount, weights_.max_size());

    if (bucketCount < 2) {
      VELOX_USER_FAIL("Bucket count must be at least 2.0");
    }

    if (min >= max) {
      VELOX_USER_FAIL("Min must be less than max. Min: {}, max: {}", min, max);
    }
  }

  static constexpr double kMinPredictionValue = 0.0;
  static constexpr double kMaxPredictionValue = 1.0;
  static constexpr uint8_t kSerializationVersionHeader = 1;
  std::vector<double, StlAllocator<double>> weights_;
  double totalWeights_{0};
  int64_t bucketCount_{-1};
  double min_{0};
  double max_{1.0};
  int64_t maxUsedIndex_{-1};
};

template <ClassificationType type>
struct Accumulator {
  explicit Accumulator(HashStringAllocator* allocator)
      : trueWeights_(allocator), falseWeights_(allocator) {}

  void
  setWeights(int64_t bucketCount, bool outcome, double pred, double weight) {
    VELOX_CHECK_EQ(bucketCount, trueWeights_.bucketCount());
    VELOX_CHECK_EQ(bucketCount, falseWeights_.bucketCount());

    /// Similar to Java Presto, the max prediction value for the histogram
    /// is set to be 0.99999999999 in order to ensure bin corresponding to 1
    /// is not reached.
    static const double kMaxPredictionValue = 0.99999999999;
    pred = std::min(pred, kMaxPredictionValue);
    outcome ? trueWeights_.add(pred, weight) : falseWeights_.add(pred, weight);
  }

  void tryInit(int64_t bucketCount) {
    trueWeights_.tryInit(bucketCount);
    falseWeights_.tryInit(bucketCount);
  }

  vector_size_t size() const {
    return trueWeights_.size();
  }

  size_t serialize(char* output) const {
    size_t bytes = trueWeights_.serialize(output);
    return bytes + falseWeights_.serialize(output + bytes);
  }

  size_t serializationSize() const {
    return trueWeights_.serializationSize() + falseWeights_.serializationSize();
  }

  void mergeWith(StringView serialized) {
    auto input = serialized.data();
    VELOX_CHECK_EQ(serialized.size() % 2, 0);
    const size_t bufferSize = serialized.size() / 2;
    trueWeights_.mergeWith(input, bufferSize);
    falseWeights_.mergeWith(input + serialized.size() / 2, bufferSize);
  }

  void extractValues(FlatVector<double>* flatResult, vector_size_t offset) {
    const double totalTrueWeight = trueWeights_.totalWeights();
    const double totalFalseWeight = falseWeights_.totalWeights();

    double runningFalseWeight = 0;
    double runningTrueWeight = 0;
    int64_t trueWeightIndex = 0;
    while (trueWeightIndex < trueWeights_.size() &&
           totalTrueWeight > runningTrueWeight) {
      auto trueBucketResult = trueWeights_.getBucket(trueWeightIndex);
      auto falseBucketResult = falseWeights_.getBucket(trueWeightIndex);

      const double falsePositive = totalFalseWeight - runningFalseWeight;
      const double negative = totalFalseWeight;

      if constexpr (type == ClassificationType::kFallout) {
        flatResult->set(offset + trueWeightIndex, falsePositive / negative);
      } else if constexpr (type == ClassificationType::kPrecision) {
        const double truePositive = (totalTrueWeight - runningTrueWeight);
        const double totalPositives = truePositive + falsePositive;
        flatResult->set(
            offset + trueWeightIndex, truePositive / totalPositives);
      } else if constexpr (type == ClassificationType::kRecall) {
        const double truePositive = (totalTrueWeight - runningTrueWeight);
        flatResult->set(
            offset + trueWeightIndex, truePositive / totalTrueWeight);
      } else if constexpr (type == ClassificationType::kMissRate) {
        flatResult->set(
            offset + trueWeightIndex, runningTrueWeight / totalTrueWeight);
      } else if constexpr (type == ClassificationType::kThresholds) {
        flatResult->set(offset + trueWeightIndex, trueBucketResult.left);
      } else {
        VELOX_UNREACHABLE("Not expected to be called.");
      }

      runningTrueWeight += trueBucketResult.weight;
      runningFalseWeight += falseBucketResult.weight;
      trueWeightIndex += 1;
    }
  }

 private:
  FixedDoubleHistogram trueWeights_;
  FixedDoubleHistogram falseWeights_;
};

template <ClassificationType type>
class ClassificationAggregation : public exec::Aggregate {
 public:
  explicit ClassificationAggregation(
      TypePtr resultType,
      bool useDefaultWeight = false)
      : Aggregate(std::move(resultType)), useDefaultWeight_(useDefaultWeight) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(Accumulator<type>);
  }

  bool isFixedSize() const override {
    return false;
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decodeArguments(rows, args);
    auto accumulator = value<Accumulator<type>>(group);

    auto tracker = trackRowSize(group);
    rows.applyToSelected([&](auto row) {
      if (decodedBuckets_.isNullAt(row) || decodedOutcome_.isNullAt(row) ||
          decodedPred_.isNullAt(row) ||
          (!useDefaultWeight_ && decodedWeight_.isNullAt(row))) {
        return;
      }
      clearNull(group);
      accumulator->tryInit(decodedBuckets_.valueAt<int64_t>(row));
      accumulator->setWeights(
          decodedBuckets_.valueAt<int64_t>(row),
          decodedOutcome_.valueAt<bool>(row),
          decodedPred_.valueAt<double>(row),
          useDefaultWeight_ ? 1.0 : decodedWeight_.valueAt<double>(row));
    });
  }

  // Step 4.
  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decodeArguments(rows, args);

    rows.applyToSelected([&](vector_size_t row) {
      if (decodedBuckets_.isNullAt(row) || decodedOutcome_.isNullAt(row) ||
          decodedPred_.isNullAt(row) ||
          (!useDefaultWeight_ && decodedWeight_.isNullAt(row))) {
        return;
      }

      auto& group = groups[row];
      auto tracker = trackRowSize(group);

      clearNull(group);
      auto* accumulator = value<Accumulator<type>>(group);
      accumulator->tryInit(decodedBuckets_.valueAt<int64_t>(row));

      accumulator->setWeights(
          decodedBuckets_.valueAt<int64_t>(row),
          decodedOutcome_.valueAt<bool>(row),
          decodedPred_.valueAt<double>(row),
          useDefaultWeight_ ? 1.0 : decodedWeight_.valueAt<double>(row));
    });
  }

  // Step 5.
  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    VELOX_CHECK(result);
    auto flatResult = (*result)->asFlatVector<StringView>();
    flatResult->resize(numGroups);

    uint64_t* rawNulls = nullptr;
    if (flatResult->mayHaveNulls()) {
      BufferPtr& nulls = flatResult->mutableNulls(flatResult->size());
      rawNulls = nulls->asMutable<uint64_t>();
    }

    for (auto i = 0; i < numGroups; ++i) {
      auto group = groups[i];
      if (isNull(group)) {
        flatResult->setNull(i, true);
        continue;
      }

      if (rawNulls) {
        bits::clearBit(rawNulls, i);
      }
      auto accumulator = value<Accumulator<type>>(group);
      auto serializationSize = accumulator->serializationSize();
      char* rawBuffer =
          flatResult->getRawStringBufferWithSpace(serializationSize);

      VELOX_CHECK_EQ(accumulator->serialize(rawBuffer), serializationSize);
      auto sv = StringView(rawBuffer, serializationSize);
      flatResult->set(i, std::move(sv));
    }
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto vector = (*result)->as<ArrayVector>();
    VELOX_CHECK(vector);
    vector->resize(numGroups);

    vector_size_t numValues = 0;
    uint64_t* rawNulls = getRawNulls(result->get());

    for (auto i = 0; i < numGroups; ++i) {
      auto* group = groups[i];
      auto* accumulator = value<Accumulator<type>>(group);
      const auto size = accumulator->size();
      if (isNull(group)) {
        clearNull(rawNulls, i);
        continue;
      }

      clearNull(rawNulls, i);
      numValues += size;
    }

    auto flatResults = vector->elements()->asFlatVector<double>();
    flatResults->resize(numValues);

    auto* rawOffsets = vector->offsets()->asMutable<vector_size_t>();
    auto* rawSizes = vector->sizes()->asMutable<vector_size_t>();

    vector_size_t offset = 0;
    for (auto i = 0; i < numGroups; ++i) {
      auto* group = groups[i];

      if (isNull(group)) {
        continue;
      }
      auto* accumulator = value<Accumulator<type>>(group);
      const vector_size_t size = accumulator->size();

      rawOffsets[i] = offset;
      rawSizes[i] = size;

      accumulator->extractValues(flatResults, offset);

      offset += size;
    }
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    VELOX_CHECK_EQ(args.size(), 1);
    decodedAcc_.decode(*args[0], rows);

    rows.applyToSelected([&](auto row) {
      if (decodedAcc_.isNullAt(row)) {
        return;
      }

      auto group = groups[row];
      auto tracker = trackRowSize(group);
      clearNull(group);

      auto serialized = decodedAcc_.valueAt<StringView>(row);

      auto accumulator = value<Accumulator<type>>(group);
      accumulator->mergeWith(serialized);
    });
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    VELOX_CHECK_EQ(args.size(), 1);
    decodedAcc_.decode(*args[0], rows);
    auto tracker = trackRowSize(group);

    rows.applyToSelected([&](auto row) {
      if (decodedAcc_.isNullAt(row)) {
        return;
      }

      clearNull(group);

      auto serialized = decodedAcc_.valueAt<StringView>(row);

      auto accumulator = value<Accumulator<type>>(group);
      accumulator->mergeWith(serialized);
    });
  }

 protected:
  void initializeNewGroupsInternal(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    exec::Aggregate::setAllNulls(groups, indices);
    for (auto i : indices) {
      auto group = groups[i];
      new (group + offset_) Accumulator<type>(allocator_);
    }
  }

  void destroyInternal(folly::Range<char**> groups) override {
    destroyAccumulators<Accumulator<type>>(groups);
  }

 private:
  void decodeArguments(
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args) {
    decodedBuckets_.decode(*args[0], rows, true);
    decodedOutcome_.decode(*args[1], rows, true);
    decodedPred_.decode(*args[2], rows, true);
    if (!useDefaultWeight_) {
      decodedWeight_.decode(*args[3], rows, true);
    }
  }

  DecodedVector decodedAcc_;
  DecodedVector decodedBuckets_;
  DecodedVector decodedOutcome_;
  DecodedVector decodedPred_;
  DecodedVector decodedWeight_;
  const bool useDefaultWeight_{false};
};

template <ClassificationType T>
void registerAggregateFunctionImpl(
    const std::string& name,
    bool withCompanionFunctions,
    bool overwrite,
    const std::vector<std::shared_ptr<exec::AggregateFunctionSignature>>&
        signatures) {
  exec::registerAggregateFunction(
      name,
      signatures,
      [](core::AggregationNode::Step,
         const std::vector<TypePtr>& args,
         const TypePtr& resultType,
         const core::QueryConfig& /*config*/)
          -> std::unique_ptr<exec::Aggregate> {
        if (args.size() == 4) {
          return std::make_unique<ClassificationAggregation<T>>(resultType);
        } else {
          return std::make_unique<ClassificationAggregation<T>>(
              resultType, true);
        }
      },
      withCompanionFunctions,
      overwrite);
}
} // namespace

void registerClassificationFunctions(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  const auto signatures =
      std::vector<std::shared_ptr<exec::AggregateFunctionSignature>>{
          exec::AggregateFunctionSignatureBuilder()
              .returnType("array(double)")
              .intermediateType("varbinary")
              .argumentType("bigint")
              .argumentType("boolean")
              .argumentType("double")
              .build(),
          exec::AggregateFunctionSignatureBuilder()
              .returnType("array(double)")
              .intermediateType("varbinary")
              .argumentType("bigint")
              .argumentType("boolean")
              .argumentType("double")
              .argumentType("double")
              .build()};
  registerAggregateFunctionImpl<ClassificationType::kFallout>(
      prefix + kClassificationFallout,
      withCompanionFunctions,
      overwrite,
      signatures);
  registerAggregateFunctionImpl<ClassificationType::kPrecision>(
      prefix + kClassificationPrecision,
      withCompanionFunctions,
      overwrite,
      signatures);
  registerAggregateFunctionImpl<ClassificationType::kRecall>(
      prefix + kClassificationRecall,
      withCompanionFunctions,
      overwrite,
      signatures);
  registerAggregateFunctionImpl<ClassificationType::kMissRate>(
      prefix + kClassificationMissRate,
      withCompanionFunctions,
      overwrite,
      signatures);
  registerAggregateFunctionImpl<ClassificationType::kThresholds>(
      prefix + kClassificationThreshold,
      withCompanionFunctions,
      overwrite,
      signatures);
}

} // namespace facebook::velox::aggregate::prestosql

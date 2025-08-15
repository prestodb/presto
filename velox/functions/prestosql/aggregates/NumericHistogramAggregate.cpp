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
#include "velox/exec/Aggregate.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/functions/prestosql/aggregates/AggregateNames.h"

#include <algorithm>
#include <limits>
#include <queue>

#include <vector>
#include "velox/exec/SimpleAggregateAdapter.h"

using namespace facebook::velox::exec;
namespace facebook::velox::aggregate::prestosql {

namespace {

// todo(wangke): move it to functions/lib
class NumericHistogram {
 private:
  class Entry {
   public:
    int id_;
    double value_;
    double weight_;
    double penalty_;
    Entry* right_;
    Entry* left_;
    mutable bool valid_;

    Entry(
        int id,
        double value,
        double weight,
        Entry* right = nullptr,
        Entry* left = nullptr)
        : id_(id),
          value_(value),
          weight_(weight),
          right_(right),
          left_(left),
          valid_(true) {
      if (right != nullptr) {
        penalty_ = computePenalty(value, weight, right->value_, right->weight_);
      } else {
        penalty_ = std::numeric_limits<double>::quiet_NaN();
      }
      if (left != nullptr) {
        left->right_ = this;
      }
      if (right != nullptr) {
        right->left_ = this;
      }
    }

    struct Compare {
      bool operator()(const Entry* current, const Entry* other) const {
        const auto penaltyComparison =
            doubleCompare(current->penalty_, other->penalty_);
        if (penaltyComparison == 0) {
          return current->id_ > other->id_;
        }
        return penaltyComparison == 1;
      }
    };

    void invalidate() {
      valid_ = false;
    }
  };

  double static computePenalty(
      double value1,
      double weight1,
      double value2,
      double weight2) {
    double weight = weight1 + weight2;
    double squaredDifference = (value1 - value2) * (value1 - value2);
    double proportionsProduct =
        (weight1 * weight2) / ((weight1 + weight2) * (weight1 + weight2));
    return weight * squaredDifference * proportionsProduct;
  }

  int maxBuckets_;
  int nextIndex_;
  std::vector<double, StlAllocator<double>> values_;
  std::vector<double, StlAllocator<double>> weights_;
  static const int ENTRY_BUFFER_SIZE = 100;

 public:
  NumericHistogram(int maxBuckets, HashStringAllocator* allocator)
      : maxBuckets_(maxBuckets),
        nextIndex_(0),
        values_{StlAllocator<double>(allocator)},
        weights_{StlAllocator<double>(allocator)} {
    VELOX_USER_CHECK_GE(
        maxBuckets,
        2,
        "numeric_histogram bucket count must be greater than one");
    values_.reserve(maxBuckets_ + ENTRY_BUFFER_SIZE);
    weights_.reserve(maxBuckets_ + ENTRY_BUFFER_SIZE);
  }

  NumericHistogram(common::InputByteStream& in, HashStringAllocator* allocator)
      : values_{StlAllocator<double>(allocator)},
        weights_{StlAllocator<double>(allocator)} {
    maxBuckets_ = in.read<int>();
    VELOX_USER_CHECK_GE(
        maxBuckets_,
        2,
        "numeric_histogram bucket count must be greater than one");
    values_.reserve(maxBuckets_ + ENTRY_BUFFER_SIZE);
    weights_.reserve(maxBuckets_ + ENTRY_BUFFER_SIZE);
    nextIndex_ = in.read<int>();
    for (int i = 0; i < nextIndex_; ++i) {
      values_.push_back(in.read<double>());
      weights_.push_back(in.read<double>());
    }
  }

  size_t serializedSize() const {
    size_t size = sizeof(maxBuckets_) + sizeof(nextIndex_) +
        nextIndex_ * sizeof(double) // size of values_
        + nextIndex_ * sizeof(double); // size of weights_
    return size;
  }

  void serialize(velox::common::OutputByteStream& out) {
    out.appendOne(maxBuckets_);
    out.appendOne(nextIndex_);
    VELOX_CHECK_GE(values_.size(), nextIndex_);
    for (int i = 0; i < nextIndex_; ++i) {
      out.appendOne(values_[i]);
      out.appendOne(weights_[i]);
    }
  }

  // Add a value to the histogram
  void add(double value, double weight = 1.0) {
    if (nextIndex_ >= maxBuckets_ + ENTRY_BUFFER_SIZE) {
      compact();
    }
    if (nextIndex_ < values_.size()) {
      values_[nextIndex_] = value;
      weights_[nextIndex_] = weight;
    } else {
      values_.push_back(value);
      weights_.push_back(weight);
    }
    // After compact, nextIndex_ should be equal to maxbuckets

    ++nextIndex_;
  }

  void mergeWith(NumericHistogram& other) {
    VELOX_CHECK_GE(other.values_.size(), other.nextIndex_);
    VELOX_CHECK_GE(other.weights_.size(), other.nextIndex_);
    for (auto i = 0; i < other.nextIndex_; ++i) {
      add(other.values_[i], other.weights_[i]);
    }
    compact();
  }

  // Compact does the following:
  // 1. Merge the same buckets, which may or may not reduce nextIndex_ (if there
  // are no elements with the same value)
  // 2. If step1 did not reduce it enough, mergeAndReduceBuckets will
  // merge and reduce it until nextIndex_ is smaller than maxBuckets
  void compact() {
    mergeSameBuckets();
    mergeAndReduceBuckets();
  }

  // Initialize a priority queue with Entries for each value and weight
  std::priority_queue<Entry*, std::vector<Entry*>, Entry::Compare> initQueue(
      std::vector<Entry*>& allocatedEntries) {
    VELOX_CHECK_GE(values_.size(), nextIndex_);
    VELOX_CHECK_GE(weights_.size(), nextIndex_);

    std::priority_queue<Entry*, std::vector<Entry*>, Entry::Compare> queue;
    Entry* right = new Entry(
        nextIndex_ - 1, values_[nextIndex_ - 1], weights_[nextIndex_ - 1]);
    allocatedEntries.push_back(right);
    queue.push(right);
    for (int i = nextIndex_ - 2; i >= 0; i--) {
      Entry* current = new Entry(i, values_[i], weights_[i], right);
      queue.push(current);
      allocatedEntries.push_back(current);
      right = current;
    }

    return queue;
  }

  // Merge and reduce buckets to max number of buckets
  void mergeAndReduceBuckets() {
    if (nextIndex_ <= maxBuckets_) {
      return;
    }

    std::vector<Entry*> allocatedEntries;
    auto queue = initQueue(allocatedEntries);

    // Merge and reduce entries in queue until the count is equal to targetCount
    while (nextIndex_ > maxBuckets_) {
      Entry* current = queue.top();
      queue.pop();
      if (!current->valid_) {
        // already replaced, move on
        continue;
      }
      nextIndex_--;
      current->invalidate();

      // Right is guaranteed to exist because we set the penalty of the last
      // bucket to infinity so the first current in the queue can never be the
      // last bucket
      Entry* right = current->right_;

      VELOX_CHECK(right != nullptr, "Right entry is null");
      VELOX_CHECK(right->valid_, "Right entry is not valid");

      right->invalidate();

      // Merge "current" with "right" and mark "right" as invalid so we don't
      // visit it again
      double newWeight = current->weight_ + right->weight_;
      double newValue = (current->value_ * current->weight_ +
                         right->value_ * right->weight_) /
          newWeight;

      Entry* merged =
          new Entry(current->id_, newValue, newWeight, right->right_);

      allocatedEntries.push_back(merged);
      queue.push(merged);

      // Update left's penalty
      Entry* left = current->left_;
      if (left != nullptr) {
        VELOX_CHECK(left->valid_, "Left entry is not valid");

        left->invalidate();
        Entry* newLeft = new Entry(
            left->id_, left->value_, left->weight_, merged, left->left_);
        allocatedEntries.push_back(newLeft);
        queue.push(newLeft);
      }
    }
    // Re populate values_ and weights_ with the reduced queue
    nextIndex_ = 0;
    while (!queue.empty()) {
      Entry* entry = queue.top();
      queue.pop();
      if (entry->valid_) {
        values_[nextIndex_] = entry->value_;
        weights_[nextIndex_] = entry->weight_;
        ++nextIndex_;
      }
    }

    sortValuesAndWeights();
    for (Entry* entry : allocatedEntries) {
      delete entry;
    }
  }
  // Following Java Standard:
  // https://docs.oracle.com/javase/7/docs/api/java/lang/Double.html#compare(double,%20double)
  static int doubleCompare(double d1, double d2) {
    // Handle NaN: NaN is considered greater than any other value, including
    // infinity
    if (std::isnan(d1)) {
      return std::isnan(d2) ? 0 : 1;
    }
    if (std::isnan(d2)) {
      return -1;
    }
    // Handle infinities
    if (d1 == d2) {
      // Covers +0.0 vs -0.0, +Inf vs +Inf, -Inf vs -Inf
      return 0;
    }
    if (d1 < d2) {
      return -1;
    }
    return 1;
  }

  // Merge same buckets in place and update nextIndex_
  void mergeSameBuckets() {
    sortValuesAndWeights();
    VELOX_CHECK_GE(values_.size(), nextIndex_);
    VELOX_CHECK_GE(weights_.size(), nextIndex_);

    int current = 0;
    for (int i = 1; i < nextIndex_; ++i) {
      if (doubleCompare(values_[current], values_[i]) == 0) {
        weights_[current] += weights_[i];
      } else {
        ++current;
        values_[current] = values_[i];
        weights_[current] = weights_[i];
      }
    }

    nextIndex_ = current + 1;
  }

  // Sort the values and weights
  void sortValuesAndWeights() {
    std::vector<std::pair<double, double>> pairs(nextIndex_);
    VELOX_CHECK_GE(values_.size(), nextIndex_);
    VELOX_CHECK_GE(weights_.size(), nextIndex_);
    for (int i = 0; i < nextIndex_; ++i) {
      pairs[i] = {values_[i], weights_[i]};
    }
    std::sort(
        pairs.begin(),
        pairs.end(),
        [this](std::pair<double, double>& a, std::pair<double, double>& b) {
          return doubleCompare(a.first, b.first) == -1;
        });
    for (int i = 0; i < nextIndex_; ++i) {
      values_[i] = pairs[i].first;
      weights_[i] = pairs[i].second;
    }
  }

  void getBuckets(exec::out_type<Map<double, double>>& out) {
    compact();
    VELOX_CHECK_GE(values_.size(), nextIndex_);
    VELOX_CHECK_GE(weights_.size(), nextIndex_);
    for (int i = 0; i < nextIndex_; ++i) {
      auto [keyWriter, valueWriter] = out.add_item();
      keyWriter = values_[i];
      valueWriter = weights_[i];
    }
  }

  void getBuckets(exec::out_type<Map<float, float>>& out) {
    compact();
    VELOX_CHECK_GE(values_.size(), nextIndex_);
    VELOX_CHECK_GE(weights_.size(), nextIndex_);
    for (int i = 0; i < nextIndex_; ++i) {
      auto [keyWriter, valueWriter] = out.add_item();
      keyWriter = values_[i];
      valueWriter = weights_[i];
    }
  }
};

template <typename TValue, typename TWeight, typename Func>
struct NumericHistogramAccumulator {
  static constexpr bool is_fixed_size_ = false;
  static constexpr bool use_external_memory_ = true;
  static constexpr bool is_aligned_ = true;

  std::optional<NumericHistogram> histogram_;

  NumericHistogramAccumulator() = delete;

  explicit NumericHistogramAccumulator(
      HashStringAllocator* /*allocator*/,
      Func* /*fn*/) {}

  bool addInput(
      HashStringAllocator* allocator,
      exec::arg_type<int64_t> buckets,
      exec::arg_type<TValue> value) {
    if (!histogram_.has_value()) {
      histogram_.emplace(buckets, allocator);
    }
    histogram_.value().add(value);
    return true;
  }

  bool addInput(
      HashStringAllocator* allocator,
      exec::arg_type<int64_t> buckets,
      exec::arg_type<TValue> value,
      exec::arg_type<TWeight> weight) {
    if (!histogram_.has_value()) {
      histogram_.emplace(buckets, allocator);
    }
    histogram_.value().add(value, weight);
    return true;
  }

  void combine(
      HashStringAllocator* allocator,
      exec::arg_type<Varbinary> other) {
    common::InputByteStream stream((other.data()));
    auto otherHistogram = NumericHistogram(stream, allocator);

    if (!histogram_.has_value()) {
      histogram_.emplace(otherHistogram);
    } else {
      histogram_.value().mergeWith(otherHistogram);
    }
  }

  bool writeIntermediateResult(exec::out_type<Varbinary>& out) {
    if (histogram_.has_value()) {
      const auto serializedSize = histogram_.value().serializedSize();
      out.reserve(serializedSize);
      common::OutputByteStream stream(out.data());
      histogram_.value().serialize(stream);

      out.resize(serializedSize);
      return true;
    } else {
      return false;
    }
  }

  // According to the query plan, The output type is Map<TValue, TValue> rather
  // than Map<TValue, TWeight>
  bool writeFinalResult(exec::out_type<Map<TValue, TValue>>& out) {
    if (histogram_.has_value()) {
      histogram_.value().getBuckets(out);
      return true;
    }

    return false;
  }
};

template <typename TValue, typename TWeight, int NumArgs>
class NumericHistogramAggregate {};

template <typename TValue, typename TWeight>
class NumericHistogramAggregate<TValue, TWeight, 2> {
 public:
  using InputType = Row<int64_t, TValue>;

  using IntermediateType = Varbinary;

  using OutputType = Map<TValue, TValue>;

  using AccumulatorType = NumericHistogramAccumulator<
      TValue,
      TWeight,
      NumericHistogramAggregate<TValue, TWeight, 2>>;
};

template <typename TValue, typename TWeight>
class NumericHistogramAggregate<TValue, TWeight, 3> {
 public:
  using InputType = Row<int64_t, TValue, TWeight>;

  using IntermediateType = Varbinary;

  using OutputType = Map<TValue, TValue>;

  using AccumulatorType = NumericHistogramAccumulator<
      TValue,
      TWeight,
      NumericHistogramAggregate<TValue, TWeight, 3>>;
};
} // namespace

void registerNumericHistogramAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;
  const auto valueTypes = {"real", "double"};
  // todo(wangke): We can enhance the function by adding more types, at least
  // REAL type in weight, though we don't support real weight in Presto
  const auto weightTypes = {"real", "double"};
  for (const auto& valueType : valueTypes) {
    const auto returnType = fmt::format("map({}, {})", valueType, valueType);
    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .returnType(returnType)
                             .intermediateType("varbinary")
                             .argumentType("bigint")
                             .argumentType(valueType)
                             .build());
    for (const auto& weightType : weightTypes) {
      signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                               .returnType(returnType)
                               .intermediateType("varbinary")
                               .argumentType("bigint")
                               .argumentType(valueType)
                               .argumentType(weightType)
                               .build());
    }
  }
  auto name = prefix + kNumericHistogram;

  exec::registerAggregateFunction(
      name,
      signatures,
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType,
          const core::QueryConfig& /*config*/)
          -> std::unique_ptr<exec::Aggregate> {
        VELOX_USER_CHECK_GE(
            argTypes.size(), 2, "{} takes at least two arguments", name);
        VELOX_USER_CHECK_LE(
            argTypes.size(), 3, "{} takes at most three arguments", name);
        if (argTypes[0]->kind() != TypeKind::BIGINT) {
          VELOX_USER_FAIL(
              "Aggregation {}: Buckets must be bigint {}, but is {}",
              name,
              argTypes[0]->kindName());
        }
        if (argTypes[1]->kind() != TypeKind::REAL &&
            argTypes[1]->kind() != TypeKind::DOUBLE) {
          VELOX_USER_FAIL(
              "Aggregation {}: Value must be REAL or DOUBLE {}, but is {}",
              name,
              argTypes[1]->kindName());
        }
        if (argTypes.size() == 2) {
          switch (argTypes[1]->kind()) {
            case TypeKind::REAL:
              return std::make_unique<exec::SimpleAggregateAdapter<
                  NumericHistogramAggregate<float, double, 2>>>(
                  step, argTypes, resultType);
            case TypeKind::DOUBLE:
              return std::make_unique<exec::SimpleAggregateAdapter<
                  NumericHistogramAggregate<double, double, 2>>>(
                  step, argTypes, resultType);

            default:
              VELOX_USER_FAIL("Unknown input type for {} aggregation {}", name);
          }
        } else {
          // argTypes.size() == 3
          if (argTypes[2]->kind() != TypeKind::REAL &&
              argTypes[2]->kind() != TypeKind::DOUBLE) {
            VELOX_USER_FAIL(
                "Aggregation {}: Weight must be REAL or DOUBLE {}, but is {}",
                name,
                argTypes[1]->kindName());
          }
          switch (argTypes[1]->kind()) {
            case TypeKind::REAL:
              switch (argTypes[2]->kind()) {
                case TypeKind::REAL:
                  return std::make_unique<exec::SimpleAggregateAdapter<
                      NumericHistogramAggregate<float, float, 3>>>(
                      step, argTypes, resultType);
                case TypeKind::DOUBLE:
                  return std::make_unique<exec::SimpleAggregateAdapter<
                      NumericHistogramAggregate<float, double, 3>>>(
                      step, argTypes, resultType);
                default:
                  VELOX_USER_FAIL(
                      "Unknown weight type for aggregation {}", name);
              }
            case TypeKind::DOUBLE:
              switch (argTypes[2]->kind()) {
                case TypeKind::REAL:
                  return std::make_unique<exec::SimpleAggregateAdapter<
                      NumericHistogramAggregate<double, float, 3>>>(
                      step, argTypes, resultType);
                case TypeKind::DOUBLE:
                  return std::make_unique<exec::SimpleAggregateAdapter<
                      NumericHistogramAggregate<double, double, 3>>>(
                      step, argTypes, resultType);
                default:
                  VELOX_USER_FAIL(
                      "Unknown weight type for aggregation {}", name);
              }
            default:
              VELOX_USER_FAIL("Unknown input type for {} aggregation {}", name);
          }
        }
      },
      withCompanionFunctions,
      overwrite);
}
} // namespace facebook::velox::aggregate::prestosql

/*
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
#include <folly/stats/TDigest.h>
#include "velox/aggregates/AggregateNames.h"
#include "velox/aggregates/IOUtils.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/HashStringAllocator.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::aggregate {
namespace {
int32_t serializedSize(const folly::TDigest& digest) {
  return sizeof(size_t) + // maxSize
      4 * sizeof(double) + // sum, count, min, max
      sizeof(size_t) + // number of centroids
      2 * sizeof(double) * digest.getCentroids().size();
}

template <typename TByteStream>
void serialize(const folly::TDigest& digest, TByteStream& output) {
  output.appendOne(digest.maxSize());
  output.appendOne(digest.sum());
  output.appendOne(digest.count());
  output.appendOne(digest.min());
  output.appendOne(digest.max());

  const auto& centroids = digest.getCentroids();
  output.appendOne(centroids.size());
  for (const auto& centroid : centroids) {
    output.appendOne(centroid.mean());
    output.appendOne(centroid.weight());
  }
}

template <typename TByteStream>
folly::TDigest deserialize(TByteStream& input) {
  auto maxSize = input.template read<size_t>();
  auto sum = input.template read<double>();
  auto count = input.template read<double>();
  auto min = input.template read<double>();
  auto max = input.template read<double>();

  auto centroidCount = input.template read<size_t>();
  std::vector<folly::TDigest::Centroid> centroids;
  centroids.reserve(centroidCount);
  for (auto i = 0; i < centroidCount; i++) {
    auto mean = input.template read<double>();
    auto weight = input.template read<double>();
    centroids.emplace_back(folly::TDigest::Centroid(mean, weight));
  }

  return folly::TDigest(std::move(centroids), sum, count, max, min, maxSize);
}

template <typename T>
folly::TDigest singleValueDigest(T v, int64_t count) {
  return folly::TDigest(
      {folly::TDigest::Centroid(v, count)}, v * count, count, v, v);
}

struct TDigestAccumulator {
  explicit TDigestAccumulator(exec::HashStringAllocator* allocator)
      : values_{exec::StlAllocator<double>(allocator)},
        largeCountValues_{exec::StlAllocator<double>(allocator)},
        largeCounts_{exec::StlAllocator<int64_t>(allocator)} {}

  void write(
      const folly::TDigest& digest,
      exec::HashStringAllocator* allocator) {
    if (!begin_) {
      begin_ = allocator->allocate(serializedSize(digest));
    }

    ByteStream stream(allocator);
    allocator->extendWrite({begin_, begin_->begin()}, stream);
    serialize(digest, stream);
    allocator->finishWrite(stream, 0);
  }

  folly::TDigest read() const {
    VELOX_CHECK(begin_);

    ByteStream inStream;
    exec::HashStringAllocator::prepareRead(begin_, inStream);
    return deserialize(inStream);
  }

  bool hasValue() const {
    return begin_ != nullptr;
  }

  void destroy(exec::HashStringAllocator* allocator) {
    if (begin_) {
      allocator->free(begin_);
    }
  }

  template <typename T>
  void append(T v, exec::HashStringAllocator* allocator) {
    values_.emplace_back((double)v);

    if (values_.size() >= kMaxBufferSize) {
      flush(allocator);
    }
  }

  template <typename T>
  void append(T v, int64_t count, exec::HashStringAllocator* allocator) {
    static const int64_t kMaxCountToBuffer = 99;

    if (values_.size() + count <= kMaxBufferSize &&
        count <= kMaxCountToBuffer) {
      values_.reserve(values_.size() + count);
      for (auto i = 0; i < count; i++) {
        values_.emplace_back((double)v);
      }

      if (values_.size() >= kMaxBufferSize) {
        flush(allocator);
      }
    } else {
      largeCountValues_.emplace_back(v);
      largeCounts_.emplace_back(count);
      if (largeCountValues_.size() >= kMaxBufferSize) {
        flush(allocator);
      }
    }
  }

  void append(folly::TDigest digest, exec::HashStringAllocator* allocator) {
    if (hasValue()) {
      auto currentDigest = read();
      std::vector<folly::TDigest> digests = {
          std::move(currentDigest), std::move(digest)};
      auto combinedDigest =
          folly::TDigest::merge(folly::Range(digests.data(), digests.size()));
      write(combinedDigest, allocator);
    } else {
      write(digest, allocator);
    }
  }

  void flush(exec::HashStringAllocator* allocator) {
    if (!values_.empty()) {
      folly::TDigest digest{hasValue() ? read() : folly::TDigest()};
      digest = digest.merge(values_);
      values_.clear();
      write(digest, allocator);
    }

    if (!largeCountValues_.empty()) {
      std::vector<folly::TDigest> digests;
      digests.reserve(largeCountValues_.size() + 1);
      for (auto i = 0; i < largeCountValues_.size(); i++) {
        digests.emplace_back(
            singleValueDigest(largeCountValues_[i], largeCounts_[i]));
      }

      if (hasValue()) {
        digests.emplace_back(read());
      }

      auto combinedDigest =
          folly::TDigest::merge(folly::Range(digests.data(), digests.size()));
      largeCountValues_.clear();
      largeCounts_.clear();

      write(combinedDigest, allocator);
    }
  }

 private:
  // Maximum number of values to accumulate before updating TDigest.
  static const size_t kMaxBufferSize = 4096;

  exec::HashStringAllocator::Header* begin_{nullptr};

  std::vector<double, exec::StlAllocator<double>> values_;

  std::vector<double, exec::StlAllocator<double>> largeCountValues_;
  std::vector<int64_t, exec::StlAllocator<int64_t>> largeCounts_;
};

// The following variations are possible:
//  x, percentile
//  x, weight, percentile
//  x, percentile, accuracy (not supported yet)
//  x, weight, percentile, accuracy (not supported yet)
template <typename T>
class ApproxPercentileAggregate : public exec::Aggregate {
 public:
  ApproxPercentileAggregate(
      core::AggregationNode::Step step,
      bool hasWeight,
      const TypePtr& resultType)
      : exec::Aggregate(step, resultType), hasWeight_{hasWeight} {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(TDigestAccumulator);
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    exec::Aggregate::setAllNulls(groups, indices);
    for (auto i : indices) {
      auto group = groups[i];
      new (group + offset_) TDigestAccumulator(allocator_);
    }
  }

  void destroy(folly::Range<char**> groups) override {
    for (auto group : groups) {
      auto accumulator = value<TDigestAccumulator>(group);
      accumulator->destroy(allocator_);
    }
  }

  void finalize(char** groups, int32_t numGroups) override {
    for (auto i = 0; i < numGroups; ++i) {
      auto accumulator = value<TDigestAccumulator>(groups[i]);
      accumulator->flush(allocator_);
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
        [&](const folly::TDigest& digest,
            FlatVector<T>* result,
            vector_size_t index) {
          result->set(index, (T)digest.estimateQuantile(percentile_));
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
        [&](const folly::TDigest& digest,
            FlatVector<StringView>* result,
            vector_size_t index) {
          auto size = sizeof(double) /*percentile*/ + serializedSize(digest);
          Buffer* buffer = flatResult->getBufferWithSpace(size);
          StringView serialized(buffer->as<char>() + buffer->size(), size);
          OutputByteStream stream(buffer->asMutable<char>() + buffer->size());
          stream.appendOne(percentile_);
          serialize(digest, stream);
          buffer->setSize(buffer->size() + size);
          result->setNoCopy(index, serialized);
        });
  }

 protected:
  void updatePartial(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decodeArguments(rows, args);
    checkSetPercentile();

    if (hasWeight_) {
      rows.applyToSelected([&](auto row) {
        if (decodedValue_.isNullAt(row) || decodedWeight_.isNullAt(row)) {
          return;
        }

        auto accumulator = value<TDigestAccumulator>(groups[row]);
        auto value = decodedValue_.valueAt<T>(row);
        auto weight = decodedWeight_.valueAt<int64_t>(row);
        VELOX_USER_CHECK_GE(
            weight,
            1,
            "The value of the weight parameter must be greater than or equal to 1.");
        accumulator->append(value, weight, allocator_);
      });
    } else {
      if (decodedValue_.mayHaveNulls()) {
        rows.applyToSelected([&](auto row) {
          if (decodedValue_.isNullAt(row)) {
            return;
          }

          auto accumulator = value<TDigestAccumulator>(groups[row]);
          accumulator->append(decodedValue_.valueAt<T>(row), allocator_);
        });
      } else {
        rows.applyToSelected([&](auto row) {
          auto accumulator = value<TDigestAccumulator>(groups[row]);
          accumulator->append(decodedValue_.valueAt<T>(row), allocator_);
        });
      }
    }
  }

  void updateFinal(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decodedDigest_.decode(*args[0], rows, true);

    rows.applyToSelected([&](auto row) {
      if (decodedDigest_.isNullAt(row)) {
        return;
      }

      auto serialized = decodedDigest_.valueAt<StringView>(row);
      InputByteStream stream(serialized.data());
      auto percentile = stream.read<double>();
      checkSetPercentile(percentile);
      auto digest = deserialize(stream);

      auto accumulator = value<TDigestAccumulator>(groups[row]);
      accumulator->append(std::move(digest), allocator_);
    });
  }

  void updateSingleGroupPartial(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decodeArguments(rows, args);
    checkSetPercentile();

    auto accumulator = value<TDigestAccumulator>(group);

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
        accumulator->append(value, weight, allocator_);
      });
    } else {
      if (decodedValue_.mayHaveNulls()) {
        rows.applyToSelected([&](auto row) {
          if (decodedValue_.isNullAt(row)) {
            return;
          }

          accumulator->append(decodedValue_.valueAt<T>(row), allocator_);
        });
      } else {
        rows.applyToSelected([&](auto row) {
          accumulator->append(decodedValue_.valueAt<T>(row), allocator_);
        });
      }
    }
  }

  void updateSingleGroupFinal(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decodedDigest_.decode(*args[0], rows, true);
    auto accumulator = value<TDigestAccumulator>(group);

    std::vector<folly::TDigest> digests;
    digests.reserve(rows.end() + 1);
    if (accumulator->hasValue()) {
      digests.emplace_back(accumulator->read());
    }

    rows.applyToSelected([&](auto row) {
      if (decodedDigest_.isNullAt(row)) {
        return;
      }

      auto serialized = decodedDigest_.valueAt<StringView>(row);
      InputByteStream stream(serialized.data());
      auto percentile = stream.read<double>();
      checkSetPercentile(percentile);

      digests.emplace_back(deserialize(stream));
    });

    if (!digests.empty()) {
      auto digest =
          folly::TDigest::merge(folly::Range(digests.data(), digests.size()));
      accumulator->write(digest, allocator_);
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
      auto accumulator = value<TDigestAccumulator>(group);
      if (!accumulator->hasValue()) {
        result->setNull(i, true);
      } else {
        if (rawNulls) {
          bits::clearBit(rawNulls, i);
        }
        auto digest = accumulator->read();
        extractFunction(digest, result, i);
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

    // TODO Add support for accuracy parameter
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

    if (percentile_ < 0) {
      percentile_ = percentile;
    } else {
      VELOX_USER_CHECK_EQ(
          percentile,
          percentile_,
          "Percentile argument must be constant for all input rows");
    }
  }

  const bool hasWeight_;
  double percentile_{-1.0};
  DecodedVector decodedValue_;
  DecodedVector decodedWeight_;
  DecodedVector decodedPercentile_;
  DecodedVector decodedDigest_;
};

bool registerApproxPercentile(const std::string& name) {
  exec::AggregateFunctions().Register(
      name,
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType) -> std::unique_ptr<exec::Aggregate> {
        auto isRawInput = exec::isRawInput(step);
        auto isPartialOutput = exec::isPartialOutput(step);
        auto hasWeight = argTypes.size() == 3;

        TypePtr type = isRawInput ? argTypes[0] : resultType;

        if (isRawInput) {
          VELOX_USER_CHECK_GE(
              argTypes.size(), 2, "{} takes 2 or 3 arguments", name);
          VELOX_USER_CHECK_LE(
              argTypes.size(), 3, "{} takes 2 or 3 arguments", name);

          if (hasWeight) {
            VELOX_USER_CHECK_EQ(
                argTypes[1]->kind(),
                TypeKind::BIGINT,
                "The type of the weight argument of {} must be BIGINT",
                name);
          }

          VELOX_USER_CHECK_EQ(
              argTypes.back()->kind(),
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

        if (step == core::AggregationNode::Step::kIntermediate) {
          return std::make_unique<ApproxPercentileAggregate<double>>(
              step, false, VARBINARY());
        }

        auto aggResultType =
            isPartialOutput ? VARBINARY() : (isRawInput ? type : resultType);

        switch (type->kind()) {
          case TypeKind::TINYINT:
            return std::make_unique<ApproxPercentileAggregate<int8_t>>(
                step, hasWeight, aggResultType);
          case TypeKind::SMALLINT:
            return std::make_unique<ApproxPercentileAggregate<int16_t>>(
                step, hasWeight, aggResultType);
          case TypeKind::INTEGER:
            return std::make_unique<ApproxPercentileAggregate<int32_t>>(
                step, hasWeight, aggResultType);
          case TypeKind::BIGINT:
            return std::make_unique<ApproxPercentileAggregate<int64_t>>(
                step, hasWeight, aggResultType);
          case TypeKind::REAL:
            return std::make_unique<ApproxPercentileAggregate<float>>(
                step, hasWeight, aggResultType);
          case TypeKind::DOUBLE:
            return std::make_unique<ApproxPercentileAggregate<double>>(
                step, hasWeight, aggResultType);
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

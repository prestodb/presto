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

#include "velox/exec/Aggregate.h"
#include "velox/functions/lib/TDigest.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::aggregate::prestosql {

// TDigest accumulator for merge operations
struct TDigestAccumulator {
  explicit TDigestAccumulator(HashStringAllocator* allocator)
      : digest_(StlAllocator<double>(allocator)) {}

  void mergeWith(
      StringView serialized,
      HashStringAllocator* /*allocator*/,
      std::vector<int16_t>& positions) {
    if (serialized.empty()) {
      return;
    }
    digest_.mergeDeserialized(positions, serialized.data());
  }

  int64_t serializedSize(std::vector<int16_t>& positions) {
    digest_.compress(positions);
    return digest_.serializedByteSize();
  }

  void serialize(char* outputBuffer) {
    digest_.serialize(outputBuffer);
  }

 private:
  facebook::velox::functions::TDigest<StlAllocator<double>> digest_;
};

class MergeTDigestAggregate : public exec::Aggregate {
 public:
  explicit MergeTDigestAggregate(const TypePtr& resultType)
      : exec::Aggregate(resultType) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(TDigestAccumulator);
  }

  int32_t accumulatorAlignmentSize() const override {
    return alignof(TDigestAccumulator);
  }

  bool accumulatorUsesExternalMemory() const override {
    return true;
  }

  bool isFixedSize() const override {
    return false;
  }

  void toIntermediate(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      VectorPtr& result) const final {
    singleInputAsIntermediate(rows, args, result);
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    extractAccumulators(groups, numGroups, result);
  }

  // This method needs to be thread-safe as it may be called concurrently during
  // spilling operations.
  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    VELOX_CHECK(result);
    auto* flatResult = (*result)->asFlatVector<StringView>();
    std::vector<int16_t> positions;
    extract(
        groups,
        numGroups,
        flatResult,
        [&](TDigestAccumulator* accumulator,
            FlatVector<StringView>* result,
            vector_size_t index) {
          auto size = accumulator->serializedSize(positions);
          StringView serialized;
          if (StringView::isInline(size)) {
            std::string buffer(size, '\0');
            accumulator->serialize(buffer.data());
            serialized = StringView::makeInline(buffer);
          } else {
            char* rawBuffer = flatResult->getRawStringBufferWithSpace(size);
            accumulator->serialize(rawBuffer);
            serialized = StringView(rawBuffer, size);
          }
          result->setNoCopy(index, serialized);
        });
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    addIntermediateResults(groups, rows, args, false /*unused*/);
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decodedTDigest_.decode(*args[0], rows, true);
    std::vector<int16_t> positions;

    rows.applyToSelected([&](auto row) {
      if (decodedTDigest_.isNullAt(row)) {
        return;
      }

      auto group = groups[row];
      auto tracker = trackRowSize(group);
      clearNull(group);

      mergeToAccumulator(group, row, positions);
    });
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    auto tracker = trackRowSize(group);
    addSingleGroupIntermediateResults(group, rows, args, false /*unused*/);
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decodedTDigest_.decode(*args[0], rows, true);
    std::vector<int16_t> positions;

    auto tracker = trackRowSize(group);
    rows.applyToSelected([&](auto row) {
      if (decodedTDigest_.isNullAt(row)) {
        return;
      }

      clearNull(group);
      mergeToAccumulator(group, row, positions);
    });
  }

 protected:
  void initializeNewGroupsInternal(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    setAllNulls(groups, indices);
    for (auto i : indices) {
      auto group = groups[i];
      new (group + offset_) TDigestAccumulator(allocator_);
    }
  }

  void destroyInternal(folly::Range<char**> groups) override {
    destroyAccumulators<TDigestAccumulator>(groups);
  }

 private:
  void mergeToAccumulator(
      char* group,
      const vector_size_t row,
      std::vector<int16_t>& positions) {
    auto serialized = decodedTDigest_.valueAt<StringView>(row);
    TDigestAccumulator* accumulator = value<TDigestAccumulator>(group);
    accumulator->mergeWith(serialized, allocator_, positions);
  }

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
      BufferPtr& nulls = result->mutableNulls(result->size());
      rawNulls = nulls->asMutable<uint64_t>();
    }

    for (auto i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      if (isNull(group)) {
        // Set null for null input
        result->setNull(i, true);
      } else {
        if (rawNulls) {
          bits::clearBit(rawNulls, i);
        }
        auto accumulator = value<TDigestAccumulator>(group);
        extractFunction(accumulator, result, i);
      }
    }
  }
  DecodedVector decodedTDigest_;
};

inline std::unique_ptr<exec::Aggregate> createMergeTDigestAggregate(
    const TypePtr& resultType) {
  return std::make_unique<MergeTDigestAggregate>(resultType);
}

} // namespace facebook::velox::aggregate::prestosql

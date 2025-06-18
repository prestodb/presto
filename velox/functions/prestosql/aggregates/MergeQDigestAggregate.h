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
#include "velox/functions/lib/QuantileDigest.h"
#include "velox/functions/prestosql/types/QDigestType.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::aggregate::prestosql {

template <typename T>
struct QDigestAccumulator {
  explicit QDigestAccumulator(HashStringAllocator* allocator)
      : digest_(
            StlAllocator<T>(allocator),
            facebook::velox::functions::qdigest::kUninitializedMaxError) {}

  void mergeWith(StringView serialized) {
    if (serialized.empty()) {
      return;
    }
    digest_.mergeSerialized(serialized.data());
  }

  int64_t serializedSize() {
    return digest_.serializedByteSize();
  }

  void serialize(char* outputBuffer) {
    // merge(qdigest) result and intermediate result should aways have maxError
    // set
    VELOX_USER_CHECK_NE(
        digest_.getMaxError(),
        facebook::velox::functions::qdigest::kUninitializedMaxError,
        "QDigest maxError is uninitialized");
    digest_.serialize(outputBuffer);
  }

 private:
  facebook::velox::functions::qdigest::QuantileDigest<T, StlAllocator<T>>
      digest_;
};

template <typename T>
class MergeQDigestAggregate : public exec::Aggregate {
 public:
  explicit MergeQDigestAggregate(const TypePtr& resultType)
      : exec::Aggregate(resultType) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(QDigestAccumulator<T>);
  }

  int32_t accumulatorAlignmentSize() const override {
    return alignof(QDigestAccumulator<T>);
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

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    VELOX_CHECK(result);
    auto* flatResult = (*result)->asFlatVector<StringView>();
    extract(groups, numGroups, flatResult);
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
    decodedQDigest_.decode(*args[0], rows, true);

    rows.applyToSelected([&](auto row) {
      if (decodedQDigest_.isNullAt(row)) {
        return;
      }

      auto group = groups[row];
      auto tracker = trackRowSize(group);
      clearNull(group);

      mergeToAccumulator(group, row);
    });
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    addSingleGroupIntermediateResults(group, rows, args, false /*unused*/);
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decodedQDigest_.decode(*args[0], rows, true);

    auto tracker = trackRowSize(group);
    rows.applyToSelected([&](auto row) {
      if (decodedQDigest_.isNullAt(row)) {
        return;
      }

      clearNull(group);
      mergeToAccumulator(group, row);
    });
  }

 protected:
  void initializeNewGroupsInternal(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    setAllNulls(groups, indices);
    for (auto i : indices) {
      auto group = groups[i];
      new (group + offset_) QDigestAccumulator<T>(allocator_);
    }
  }

  void destroyInternal(folly::Range<char**> groups) override {
    destroyAccumulators<QDigestAccumulator<T>>(groups);
  }

 private:
  void mergeToAccumulator(char* group, const vector_size_t row) {
    auto serialized = decodedQDigest_.valueAt<StringView>(row);
    QDigestAccumulator<T>* accumulator = value<QDigestAccumulator<T>>(group);
    accumulator->mergeWith(serialized);
  }

  void
  extract(char** groups, int32_t numGroups, FlatVector<StringView>* result) {
    VELOX_CHECK(result);
    result->resize(numGroups);

    for (auto i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      if (isNull(group)) {
        // Set null for null input
        result->setNull(i, true);
      } else {
        auto accumulator = value<QDigestAccumulator<T>>(group);
        auto size = accumulator->serializedSize();
        StringView serialized;
        if (StringView::isInline(size)) {
          std::string buffer(size, '\0');
          accumulator->serialize(buffer.data());
          serialized = StringView::makeInline(buffer);
        } else {
          char* rawBuffer = result->getRawStringBufferWithSpace(size);
          accumulator->serialize(rawBuffer);
          serialized = StringView(rawBuffer, size);
        }
        result->setNoCopy(i, serialized);
      }
    }
  }
  DecodedVector decodedQDigest_;
};

inline std::unique_ptr<exec::Aggregate> createMergeQDigestAggregate(
    const TypePtr& resultType,
    const TypePtr& argType = nullptr) {
  VELOX_CHECK_NOT_NULL(argType);
  if (*argType == *QDIGEST(BIGINT())) {
    return std::make_unique<MergeQDigestAggregate<int64_t>>(resultType);
  } else if (*argType == *QDIGEST(REAL())) {
    return std::make_unique<MergeQDigestAggregate<float>>(resultType);
  } else if (*argType == *QDIGEST(DOUBLE())) {
    return std::make_unique<MergeQDigestAggregate<double>>(resultType);
  }
  VELOX_UNSUPPORTED("QDigest {} type is not supported.", argType->toString());
}
} // namespace facebook::velox::aggregate::prestosql

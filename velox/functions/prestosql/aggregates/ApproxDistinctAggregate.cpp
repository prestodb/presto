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
#define XXH_INLINE_ALL
#include <xxhash.h> // @manual=third-party//xxHash:xxhash

#include "velox/common/hyperloglog/DenseHll.h"
#include "velox/common/hyperloglog/HllUtils.h"
#include "velox/common/hyperloglog/Murmur3Hash128.h"
#include "velox/common/hyperloglog/SparseHll.h"
#include "velox/common/memory/HashStringAllocator.h"
#include "velox/exec/Aggregate.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/functions/prestosql/aggregates/AggregateNames.h"
#include "velox/functions/prestosql/aggregates/ApproxDistinctAggregates.h"
#include "velox/functions/prestosql/types/HyperLogLogRegistration.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"

using facebook::velox::common::hll::DenseHll;
using facebook::velox::common::hll::SparseHll;

namespace facebook::velox::aggregate::prestosql {

namespace {

template <typename T, bool HllAsFinalResult>
inline uint64_t hashOne(T value) {
  if constexpr (HllAsFinalResult) {
    if constexpr (std::is_same_v<T, int64_t>) {
      return common::hll::Murmur3Hash128::hash64ForLong(value, 0);
    } else if constexpr (std::is_same_v<T, double>) {
      return common::hll::Murmur3Hash128::hash64ForLong(
          *reinterpret_cast<int64_t*>(&value), 0);
    }
    return common::hll::Murmur3Hash128::hash64(&value, sizeof(T), 0);
  } else {
    return XXH64(&value, sizeof(T), 0);
  }
}

// Use timestamp.toMillis() to compute hash value.
template <>
inline uint64_t hashOne<Timestamp, false>(Timestamp value) {
  return hashOne<int64_t, false>(value.toMillis());
}

template <>
inline uint64_t hashOne<Timestamp, true>(Timestamp /*value*/) {
  VELOX_UNREACHABLE("approx_set(timestamp) is not supported.");
}

template <>
inline uint64_t hashOne<StringView, false>(StringView value) {
  return XXH64(value.data(), value.size(), 0);
}

template <>
inline uint64_t hashOne<StringView, true>(StringView value) {
  return common::hll::Murmur3Hash128::hash64(value.data(), value.size(), 0);
}

template <typename T, bool HllAsFinalResult>
struct HllAccumulator {
  explicit HllAccumulator(HashStringAllocator* allocator)
      : sparseHll_{allocator}, denseHll_{allocator} {}

  void setIndexBitLength(int8_t indexBitLength) {
    indexBitLength_ = indexBitLength;
    sparseHll_.setSoftMemoryLimit(
        DenseHll::estimateInMemorySize(indexBitLength_));
  }

  void append(T value) {
    const auto hash = hashOne<T, HllAsFinalResult>(value);

    if (isSparse_) {
      if (sparseHll_.insertHash(hash)) {
        toDense();
      }
    } else {
      denseHll_.insertHash(hash);
    }
  }

  int64_t cardinality() const {
    return isSparse_ ? sparseHll_.cardinality() : denseHll_.cardinality();
  }

  void mergeWith(StringView serialized, HashStringAllocator* allocator) {
    auto input = serialized.data();
    if (SparseHll::canDeserialize(input)) {
      if (isSparse_) {
        sparseHll_.mergeWith(input);
        if (indexBitLength_ < 0) {
          setIndexBitLength(DenseHll::deserializeIndexBitLength(input));
        }
        if (sparseHll_.overLimit()) {
          toDense();
        }
      } else {
        SparseHll other{input, allocator};
        other.toDense(denseHll_);
      }
    } else if (DenseHll::canDeserialize(input)) {
      if (isSparse_) {
        if (indexBitLength_ < 0) {
          setIndexBitLength(DenseHll::deserializeIndexBitLength(input));
        }
        toDense();
      }
      denseHll_.mergeWith(input);
    } else {
      VELOX_USER_FAIL("Unexpected type of HLL");
    }
  }

  int32_t serializedSize() {
    return isSparse_ ? sparseHll_.serializedSize() : denseHll_.serializedSize();
  }

  void serialize(char* outputBuffer) {
    return isSparse_ ? sparseHll_.serialize(indexBitLength_, outputBuffer)
                     : denseHll_.serialize(outputBuffer);
  }

 private:
  void toDense() {
    isSparse_ = false;
    denseHll_.initialize(indexBitLength_);
    sparseHll_.toDense(denseHll_);
    sparseHll_.reset();
  }

  bool isSparse_{true};
  int8_t indexBitLength_{-1};
  SparseHll sparseHll_;
  DenseHll denseHll_;
};

template <>
struct HllAccumulator<bool, false> {
  explicit HllAccumulator(HashStringAllocator* /*allocator*/) {}

  void append(bool value) {
    approxDistinctState_ |= (1 << value);
  }

  int64_t cardinality() const {
    return (approxDistinctState_ & 1) + ((approxDistinctState_ & 2) >> 1);
  }

  void mergeWith(
      StringView /*serialized*/,
      HashStringAllocator* /*allocator*/) {
    VELOX_UNREACHABLE(
        "APPROX_DISTINCT<BOOLEAN> unsupported mergeWith(StringView, HashStringAllocator*)");
  }

  void mergeWith(int8_t data) {
    approxDistinctState_ |= data;
  }

  int32_t serializedSize() const {
    return sizeof(int8_t);
  }

  void serialize(char* /*outputBuffer*/) {
    VELOX_UNREACHABLE("APPROX_DISTINCT<BOOLEAN> unsupported serialize(char*)");
  }

  void setIndexBitLength(int8_t /*indexBitLength*/) {}

  int8_t getState() const {
    return approxDistinctState_;
  }

 private:
  int8_t approxDistinctState_{0};
};

template <typename T, bool HllAsFinalResult>
class ApproxDistinctAggregate : public exec::Aggregate {
 public:
  explicit ApproxDistinctAggregate(
      const TypePtr& resultType,
      bool hllAsRawInput,
      double defaultError)
      : exec::Aggregate(resultType),
        hllAsFinalResult_{HllAsFinalResult},
        hllAsRawInput_{hllAsRawInput},
        indexBitLength_{common::hll::toIndexBitLength(defaultError)} {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(HllAccumulator<T, HllAsFinalResult>);
  }

  int32_t accumulatorAlignmentSize() const override {
    return alignof(HllAccumulator<T, HllAsFinalResult>);
  }

  bool isFixedSize() const override {
    return false;
  }

  bool supportsToIntermediate() const final {
    return hllAsRawInput_;
  }

  void toIntermediate(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      VectorPtr& result) const final {
    singleInputAsIntermediate(rows, args, result);
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    if (hllAsFinalResult_) {
      extractAccumulators(groups, numGroups, result);
    } else {
      VELOX_CHECK(result);
      auto flatResult = (*result)->asFlatVector<int64_t>();

      extract<true>(
          groups,
          numGroups,
          flatResult,
          [](HllAccumulator<T, HllAsFinalResult>* accumulator,
             FlatVector<int64_t>* result,
             vector_size_t index) {
            result->set(index, accumulator->cardinality());
          });
    }
  }

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    VELOX_CHECK(result);
    if constexpr (std::is_same_v<T, bool>) {
      static_assert(!HllAsFinalResult);
      auto* flatResult = (*result)->asFlatVector<int8_t>();

      for (auto i = 0; i < numGroups; ++i) {
        char* group = groups[i];
        auto* accumulator = value<HllAccumulator<bool, false>>(group);
        flatResult->set(i, accumulator->getState());
      }

    } else {
      auto* flatResult = (*result)->asFlatVector<StringView>();

      extract<false>(
          groups,
          numGroups,
          flatResult,
          [&](HllAccumulator<T, HllAsFinalResult>* accumulator,
              FlatVector<StringView>* result,
              vector_size_t index) {
            auto size = accumulator->serializedSize();
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
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    if (hllAsRawInput_) {
      addIntermediateResults(groups, rows, args, false /*unused*/);
    } else {
      decodeArguments(rows, args);

      rows.applyToSelected([&](auto row) {
        if (decodedValue_.isNullAt(row)) {
          return;
        }

        auto group = groups[row];
        auto tracker = trackRowSize(group);
        auto accumulator = value<HllAccumulator<T, HllAsFinalResult>>(group);
        clearNull(group);
        accumulator->setIndexBitLength(indexBitLength_);
        accumulator->append(decodedValue_.valueAt<T>(row));
      });
    }
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decodedHll_.decode(*args[0], rows, true);

    rows.applyToSelected([&](auto row) {
      if (decodedHll_.isNullAt(row)) {
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
    auto tracker = trackRowSize(group);
    if (hllAsRawInput_) {
      addSingleGroupIntermediateResults(group, rows, args, false /*unused*/);
    } else {
      decodeArguments(rows, args);

      rows.applyToSelected([&](auto row) {
        if (decodedValue_.isNullAt(row)) {
          return;
        }

        auto accumulator = value<HllAccumulator<T, HllAsFinalResult>>(group);
        clearNull(group);
        accumulator->setIndexBitLength(indexBitLength_);

        accumulator->append(decodedValue_.valueAt<T>(row));
      });
    }
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decodedHll_.decode(*args[0], rows, true);

    auto tracker = trackRowSize(group);
    rows.applyToSelected([&](auto row) {
      if (decodedHll_.isNullAt(row)) {
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
      new (group + offset_) HllAccumulator<T, HllAsFinalResult>(allocator_);
    }
  }

  void destroyInternal(folly::Range<char**> groups) override {
    destroyAccumulators<HllAccumulator<T, HllAsFinalResult>>(groups);
  }

 private:
  void mergeToAccumulator(char* group, const vector_size_t row) {
    if constexpr (std::is_same_v<T, bool>) {
      static_assert(!HllAsFinalResult);
      value<HllAccumulator<bool, false>>(group)->mergeWith(
          decodedHll_.valueAt<int8_t>(row));
    } else {
      auto serialized = decodedHll_.valueAt<StringView>(row);
      HllAccumulator<T, HllAsFinalResult>* accumulator =
          value<HllAccumulator<T, HllAsFinalResult>>(group);
      accumulator->mergeWith(serialized, allocator_);
    }
  }

  template <
      bool convertNullToZero,
      typename ExtractResult,
      typename ExtractFunc>
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
        if constexpr (convertNullToZero) {
          // This condition is for approx_distinct. approx_distinct is an
          // approximation of count(distinct), hence, it makes sense for it to
          // be consistent with count(distinct) which returns 0 for null input.
          result->set(i, 0);
        } else {
          result->setNull(i, true);
        }
      } else {
        if (rawNulls) {
          bits::clearBit(rawNulls, i);
        }

        auto accumulator = value<HllAccumulator<T, HllAsFinalResult>>(group);
        extractFunction(accumulator, result, i);
      }
    }
  }

  void decodeArguments(
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args) {
    decodedValue_.decode(*args[0], rows, true);
    if (args.size() > 1) {
      decodedMaxStandardError_.decode(*args[1], rows, true);
      checkSetMaxStandardError(rows);
    }
  }

  void checkSetMaxStandardError(const SelectivityVector& rows) {
    if (decodedMaxStandardError_.isConstantMapping()) {
      const auto maxStandardError = decodedMaxStandardError_.valueAt<double>(0);
      checkSetMaxStandardError(maxStandardError);
      return;
    }

    rows.applyToSelected([&](auto row) {
      VELOX_USER_CHECK(
          !decodedMaxStandardError_.isNullAt(row),
          "Max standard error cannot be null");
      const auto maxStandardError =
          decodedMaxStandardError_.valueAt<double>(row);
      if (maxStandardError_ == -1) {
        checkSetMaxStandardError(maxStandardError);
      } else {
        VELOX_USER_CHECK_EQ(
            maxStandardError,
            maxStandardError_,
            "Max standard error argument must be constant for all input rows");
      }
    });
  }

  void checkSetMaxStandardError(double error) {
    common::hll::checkMaxStandardError(error);

    if (maxStandardError_ < 0) {
      maxStandardError_ = error;
      indexBitLength_ = common::hll::toIndexBitLength(error);
    } else {
      VELOX_USER_CHECK_EQ(
          error,
          maxStandardError_,
          "Max standard error argument must be constant for all input rows");
    }
  }

  /// Boolean indicating whether final result is approximate cardinality of the
  /// input set or serialized HLL.
  const bool hllAsFinalResult_;

  /// Boolean indicating whether raw input contains elements of the set or
  /// serialized HLLs.
  const bool hllAsRawInput_;

  int8_t indexBitLength_;
  double maxStandardError_{-1};
  DecodedVector decodedValue_;
  DecodedVector decodedMaxStandardError_;
  DecodedVector decodedHll_;
};

template <TypeKind kind>
std::unique_ptr<exec::Aggregate> createApproxDistinct(
    const TypePtr& resultType,
    bool hllAsFinalResult,
    bool hllAsRawInput,
    double defaultError) {
  using T = typename TypeTraits<kind>::NativeType;
  if (hllAsFinalResult) {
    if constexpr (kind == TypeKind::BOOLEAN) {
      VELOX_UNREACHABLE("approx_set(boolean) is not supported.");
    } else {
      return std::make_unique<ApproxDistinctAggregate<T, true>>(
          resultType, hllAsRawInput, defaultError);
    }
  } else {
    return std::make_unique<ApproxDistinctAggregate<T, false>>(
        resultType, hllAsRawInput, defaultError);
  }
}

exec::AggregateRegistrationResult registerApproxDistinct(
    const std::string& name,
    bool hllAsFinalResult,
    bool hllAsRawInput,
    bool withCompanionFunctions,
    bool overwrite,
    double defaultError) {
  auto returnType = hllAsFinalResult ? "hyperloglog" : "bigint";

  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;
  if (hllAsRawInput) {
    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .returnType(returnType)
                             .intermediateType("varbinary")
                             .argumentType("hyperloglog")
                             .build());
  } else {
    auto inputTypes = hllAsFinalResult
        ? std::vector<std::string>{"bigint", "double", "varchar", "unknown"}
        : std::vector<std::string>{
              "tinyint",
              "smallint",
              "integer",
              "bigint",
              "hugeint",
              "real",
              "double",
              "varchar",
              "varbinary",
              "timestamp",
              "date",
              "unknown"};
    for (const auto& inputType : inputTypes) {
      signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                               .returnType(returnType)
                               .intermediateType("varbinary")
                               .argumentType(inputType)
                               .build());

      signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                               .returnType(returnType)
                               .intermediateType("varbinary")
                               .argumentType(inputType)
                               .argumentType("double")
                               .build());
    }

    if (!hllAsFinalResult) {
      signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                               .returnType("bigint")
                               .intermediateType("tinyint")
                               .argumentType("boolean")
                               .build());
    }

    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .integerVariable("a_precision")
                             .integerVariable("a_scale")
                             .returnType(returnType)
                             .intermediateType("varbinary")
                             .argumentType("DECIMAL(a_precision, a_scale)")
                             .build());
    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .integerVariable("a_precision")
                             .integerVariable("a_scale")
                             .returnType(returnType)
                             .intermediateType("varbinary")
                             .argumentType("DECIMAL(a_precision, a_scale)")
                             .argumentType("double")
                             .build());
  }

  return exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name, hllAsFinalResult, hllAsRawInput, defaultError](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType,
          const core::QueryConfig& /*config*/)
          -> std::unique_ptr<exec::Aggregate> {
        if (argTypes[0]->isUnKnown()) {
          if (hllAsFinalResult) {
            return std::make_unique<
                ApproxDistinctAggregate<UnknownValue, true>>(
                resultType, hllAsRawInput, defaultError);
          } else {
            return std::make_unique<
                ApproxDistinctAggregate<UnknownValue, false>>(
                resultType, hllAsRawInput, defaultError);
          }
        }
        if (exec::isPartialInput(step) && argTypes[0]->isTinyint()) {
          // This condition only applies to approx_distinct(boolean).
          return std::make_unique<ApproxDistinctAggregate<bool, false>>(
              resultType, hllAsRawInput, defaultError);
        }
        return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
            createApproxDistinct,
            argTypes[0]->kind(),
            resultType,
            hllAsFinalResult,
            hllAsRawInput,
            defaultError);
      },
      withCompanionFunctions,
      overwrite);
}

} // namespace

void registerApproxDistinctAggregates(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  registerHyperLogLogType();
  registerApproxDistinct(
      prefix + kApproxDistinct,
      false,
      false,
      withCompanionFunctions,
      overwrite,
      common::hll::kDefaultApproxDistinctStandardError);
  // approx_set and merge are already companion functions themselves. Don't
  // register companion functions for them.
  registerApproxDistinct(
      prefix + kApproxSet,
      true,
      false,
      false,
      overwrite,
      common::hll::kDefaultApproxSetStandardError);
  registerApproxDistinct(
      prefix + kMerge,
      true,
      true,
      false,
      overwrite,
      common::hll::kDefaultApproxSetStandardError);
}

} // namespace facebook::velox::aggregate::prestosql

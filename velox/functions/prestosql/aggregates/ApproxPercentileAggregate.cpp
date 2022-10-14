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

namespace facebook::velox::aggregate::prestosql {

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

  void append(const typename KllSketch<T>::View& view) {
    sketch_.mergeViews(folly::Range(&view, 1));
  }

  void append(const std::vector<typename KllSketch<T>::View>& views) {
    sketch_.mergeViews(views);
  }

  void finalize() {
    if (!largeCountValues_.empty()) {
      flush();
    }
    sketch_.compact();
  }

  const KllSketch<T>& getSketch() const {
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

enum IntermediateTypeChildIndex {
  kPercentiles = 0,
  kPercentilesIsArray = 1,
  kAccuracy = 2,
  kK = 3,
  kN = 4,
  kMinValue = 5,
  kMaxValue = 6,
  kItems = 7,
  kLevels = 8,
};

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
    if (percentiles_ && percentiles_->isArray) {
      folly::Range percentiles(
          percentiles_->values.begin(), percentiles_->values.end());
      auto arrayResult = (*result)->asUnchecked<ArrayVector>();
      vector_size_t elementsCount = 0;
      for (auto i = 0; i < numGroups; ++i) {
        char* group = groups[i];
        auto accumulator = value<KllSketchAccumulator<T>>(group);
        if (accumulator->getSketch().totalCount() > 0) {
          elementsCount += percentiles.size();
        }
      }
      arrayResult->elements()->resize(elementsCount);
      elementsCount = 0;
      auto rawValues =
          arrayResult->elements()->asFlatVector<T>()->mutableRawValues();
      extract(
          groups,
          numGroups,
          arrayResult,
          [&](const KllSketch<T>& digest,
              ArrayVector* result,
              vector_size_t index) {
            digest.estimateQuantiles(percentiles, rawValues + elementsCount);
            result->setOffsetAndSize(index, elementsCount, percentiles.size());
            elementsCount += percentiles.size();
          });
    } else {
      extract(
          groups,
          numGroups,
          (*result)->asFlatVector<T>(),
          [&](const KllSketch<T>& digest,
              FlatVector<T>* result,
              vector_size_t index) {
            VELOX_DCHECK_EQ(percentiles_->values.size(), 1);
            result->set(
                index, digest.estimateQuantile(percentiles_->values.back()));
          });
    }
  }

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    VELOX_CHECK(result);
    auto rowResult = (*result)->as<RowVector>();
    VELOX_CHECK(rowResult);
    auto pool = rowResult->pool();

    if (percentiles_) {
      auto& values = percentiles_->values;
      auto size = values.size();
      auto elements =
          BaseVector::create<FlatVector<double>>(DOUBLE(), size, pool);
      std::copy(values.begin(), values.end(), elements->mutableRawValues());
      auto array = std::make_shared<ArrayVector>(
          pool,
          ARRAY(DOUBLE()),
          nullptr,
          1,
          AlignedBuffer::allocate<vector_size_t>(1, pool, 0),
          AlignedBuffer::allocate<vector_size_t>(1, pool, size),
          std::move(elements));
      rowResult->childAt(kPercentiles) =
          BaseVector::wrapInConstant(numGroups, 0, std::move(array));
      rowResult->childAt(kPercentilesIsArray) =
          std::make_shared<ConstantVector<bool>>(
              pool, numGroups, false, bool(percentiles_->isArray));
    } else {
      rowResult->childAt(kPercentiles) = BaseVector::wrapInConstant(
          numGroups,
          0,
          std::make_shared<ArrayVector>(
              pool,
              ARRAY(DOUBLE()),
              AlignedBuffer::allocate<bool>(1, pool, bits::kNull),
              1,
              AlignedBuffer::allocate<vector_size_t>(1, pool, 0),
              AlignedBuffer::allocate<vector_size_t>(1, pool, 0),
              nullptr));
      rowResult->childAt(kPercentilesIsArray) =
          std::make_shared<ConstantVector<bool>>(pool, numGroups, true, false);
    }
    rowResult->childAt(kAccuracy) = std::make_shared<ConstantVector<double>>(
        pool,
        numGroups,
        accuracy_ == kMissingNormalizedValue,
        double(accuracy_));
    auto k = rowResult->childAt(kK)->asFlatVector<int32_t>();
    auto n = rowResult->childAt(kN)->asFlatVector<int64_t>();
    auto minValue = rowResult->childAt(kMinValue)->asFlatVector<T>();
    auto maxValue = rowResult->childAt(kMaxValue)->asFlatVector<T>();
    auto items = rowResult->childAt(kItems)->as<ArrayVector>();
    auto levels = rowResult->childAt(kLevels)->as<ArrayVector>();

    rowResult->resize(numGroups);
    k->resize(numGroups);
    n->resize(numGroups);
    minValue->resize(numGroups);
    maxValue->resize(numGroups);
    items->resize(numGroups);
    levels->resize(numGroups);

    auto itemsElements = items->elements()->asFlatVector<T>();
    auto levelsElements = levels->elements()->asFlatVector<int32_t>();
    size_t itemsCount = 0;
    vector_size_t levelsCount = 0;
    for (int i = 0; i < numGroups; ++i) {
      auto accumulator = value<const KllSketchAccumulator<T>>(groups[i]);
      auto v = accumulator->getSketch().toView();
      itemsCount += v.items.size();
      levelsCount += v.levels.size();
    }
    VELOX_CHECK_LE(itemsCount, std::numeric_limits<vector_size_t>::max());
    itemsElements->resetNulls();
    itemsElements->resize(itemsCount);
    levelsElements->resetNulls();
    levelsElements->resize(levelsCount);

    auto rawItems = itemsElements->mutableRawValues();
    auto rawLevels = levelsElements->mutableRawValues();
    itemsCount = 0;
    levelsCount = 0;
    for (int i = 0; i < numGroups; ++i) {
      auto accumulator = value<const KllSketchAccumulator<T>>(groups[i]);
      auto v = accumulator->getSketch().toView();
      if (v.n == 0) {
        rowResult->setNull(i, true);
      } else {
        rowResult->setNull(i, false);
        k->set(i, v.k);
        n->set(i, v.n);
        minValue->set(i, v.minValue);
        maxValue->set(i, v.maxValue);
        std::copy(v.items.begin(), v.items.end(), rawItems + itemsCount);
        items->setOffsetAndSize(i, itemsCount, v.items.size());
        itemsCount += v.items.size();
        std::copy(v.levels.begin(), v.levels.end(), rawLevels + levelsCount);
        levels->setOffsetAndSize(i, levelsCount, v.levels.size());
        levelsCount += v.levels.size();
      }
    }
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decodeArguments(rows, args);

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
    addIntermediate<false>(groups, rows, args);
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decodeArguments(rows, args);

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
    addIntermediate<true>(group, rows, args);
  }

 private:
  template <typename VectorType, typename ExtractFunc>
  void extract(
      char** groups,
      int32_t numGroups,
      VectorType* result,
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
    checkSetPercentile(rows, *args[argIndex++]);
    if (hasAccuracy_) {
      decodedAccuracy_.decode(*args[argIndex++], rows, true);
      checkSetAccuracy();
    }
    VELOX_CHECK_EQ(argIndex, args.size());
  }

  void checkSetPercentile(
      const SelectivityVector& rows,
      const BaseVector& vec) {
    DecodedVector decoded(vec, rows);
    VELOX_CHECK(
        decoded.isConstantMapping(),
        "Percentile argument must be constant for all input rows");
    bool isArray;
    const double* data;
    vector_size_t len;
    auto i = decoded.index(0);
    if (decoded.base()->typeKind() == TypeKind::DOUBLE) {
      isArray = false;
      data =
          decoded.base()->asUnchecked<ConstantVector<double>>()->rawValues() +
          i;
      len = 1;
    } else if (decoded.base()->typeKind() == TypeKind::ARRAY) {
      isArray = true;
      auto arrays = decoded.base()->asUnchecked<ArrayVector>();
      auto elements = arrays->elements()->asFlatVector<double>();
      data = elements->rawValues() + arrays->offsetAt(i);
      len = arrays->sizeAt(i);
    } else {
      VELOX_UNREACHABLE();
    }
    checkSetPercentile(isArray, data, len);
  }

  void checkSetPercentile(bool isArray, const double* data, vector_size_t len) {
    if (!percentiles_) {
      VELOX_USER_CHECK_GT(len, 0, "Percentile cannot be empty");
      percentiles_ = {
          .values = std::vector<double>(len),
          .isArray = isArray,
      };
      for (vector_size_t i = 0; i < len; ++i) {
        VELOX_USER_CHECK_GE(data[i], 0, "Percentile must be between 0 and 1");
        VELOX_USER_CHECK_LE(data[i], 1, "Percentile must be between 0 and 1");
        percentiles_->values[i] = data[i];
      }
    } else {
      VELOX_USER_CHECK_EQ(
          isArray,
          percentiles_->isArray,
          "Percentile argument must be constant for all input rows");
      VELOX_USER_CHECK_EQ(
          len,
          percentiles_->values.size(),
          "Percentile argument must be constant for all input rows");
      for (vector_size_t i = 0; i < len; ++i) {
        VELOX_USER_CHECK_EQ(
            data[i],
            percentiles_->values[i],
            "Percentile argument must be constant for all input rows");
      }
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
    if (accuracy_ != kMissingNormalizedValue) {
      accumulator->setAccuracy(accuracy_);
    }
    return accumulator;
  }

  template <bool kSingleGroup>
  void addIntermediate(
      std::conditional_t<kSingleGroup, char*, char**> group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args) {
    VELOX_CHECK_EQ(args.size(), 1);
    DecodedVector decoded(*args[0], rows);
    auto rowVec = decoded.base()->as<RowVector>();
    VELOX_CHECK(rowVec);
    DecodedVector percentiles(*rowVec->childAt(kPercentiles), rows);
    auto percentileIsArray =
        rowVec->childAt(kPercentilesIsArray)->asUnchecked<SimpleVector<bool>>();
    auto accuracy =
        rowVec->childAt(kAccuracy)->asUnchecked<SimpleVector<double>>();
    auto k = rowVec->childAt(kK)->asUnchecked<SimpleVector<int32_t>>();
    auto n = rowVec->childAt(kN)->asUnchecked<SimpleVector<int64_t>>();
    auto minValue = rowVec->childAt(kMinValue)->asUnchecked<SimpleVector<T>>();
    auto maxValue = rowVec->childAt(kMaxValue)->asUnchecked<SimpleVector<T>>();
    auto items = rowVec->childAt(kItems)->asUnchecked<ArrayVector>();
    auto levels = rowVec->childAt(kLevels)->asUnchecked<ArrayVector>();

    auto rawItems = items->elements()->asFlatVector<T>()->rawValues();
    auto rawLevels =
        levels->elements()->asFlatVector<int32_t>()->rawValues<uint32_t>();
    KllSketchAccumulator<T>* accumulator = nullptr;
    std::vector<typename KllSketch<T>::View> views;
    if constexpr (kSingleGroup) {
      views.reserve(rows.end());
    }
    rows.applyToSelected([&](auto row) {
      if (decoded.isNullAt(row)) {
        return;
      }
      int i = decoded.index(row);
      if (percentileIsArray->isNullAt(i)) {
        return;
      }
      if (!accumulator) {
        int j = percentiles.index(i);
        auto percentilesBase = percentiles.base()->asUnchecked<ArrayVector>();
        auto rawPercentiles =
            percentilesBase->elements()->asFlatVector<double>()->rawValues();
        checkSetPercentile(
            percentileIsArray->valueAt(i),
            rawPercentiles + percentilesBase->offsetAt(j),
            percentilesBase->sizeAt(j));
        if (!accuracy->isNullAt(i)) {
          checkSetAccuracy(accuracy->valueAt(i));
        }
      }
      if constexpr (kSingleGroup) {
        if (!accumulator) {
          accumulator = initRawAccumulator(group);
        }
      } else {
        accumulator = initRawAccumulator(group[row]);
      }
      typename KllSketch<T>::View v{
          .k = static_cast<uint32_t>(k->valueAt(i)),
          .n = static_cast<size_t>(n->valueAt(i)),
          .minValue = minValue->valueAt(i),
          .maxValue = maxValue->valueAt(i),
          .items =
              {rawItems + items->offsetAt(i),
               static_cast<size_t>(items->sizeAt(i))},
          .levels =
              {rawLevels + levels->offsetAt(i),
               static_cast<size_t>(levels->sizeAt(i))},
      };
      if constexpr (kSingleGroup) {
        views.push_back(v);
      } else {
        auto tracker = trackRowSize(group[row]);
        accumulator->append(v);
      }
    });
    if constexpr (kSingleGroup) {
      if (!views.empty()) {
        auto tracker = trackRowSize(group);
        accumulator->append(views);
      }
    }
  }

  struct Percentiles {
    std::vector<double> values;
    bool isArray;
  };

  static constexpr double kMissingNormalizedValue = -1;
  const bool hasWeight_;
  const bool hasAccuracy_;
  std::optional<Percentiles> percentiles_;
  double accuracy_{kMissingNormalizedValue};
  DecodedVector decodedValue_;
  DecodedVector decodedWeight_;
  DecodedVector decodedAccuracy_;
  DecodedVector decodedDigest_;
};

bool validPercentileType(const Type& type) {
  if (type.kind() == TypeKind::DOUBLE) {
    return true;
  }
  if (type.kind() != TypeKind::ARRAY) {
    return false;
  }
  return type.as<TypeKind::ARRAY>().elementType()->kind() == TypeKind::DOUBLE;
}

void addSignatures(
    const std::string& inputType,
    const std::string& percentileType,
    const std::string& returnType,
    std::vector<std::shared_ptr<exec::AggregateFunctionSignature>>&
        signatures) {
  auto intermediateType = fmt::format(
      "row(array(double), boolean, double, integer, bigint, {0}, {0}, array({0}), array(integer))",
      inputType);
  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .returnType(returnType)
                           .intermediateType(intermediateType)
                           .argumentType(inputType)
                           .argumentType(percentileType)
                           .build());
  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .returnType(returnType)
                           .intermediateType(intermediateType)
                           .argumentType(inputType)
                           .argumentType("bigint")
                           .argumentType(percentileType)
                           .build());
  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .returnType(returnType)
                           .intermediateType(intermediateType)
                           .argumentType(inputType)
                           .argumentType(percentileType)
                           .argumentType("double")
                           .build());
  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .returnType(returnType)
                           .intermediateType(intermediateType)
                           .argumentType(inputType)
                           .argumentType("bigint")
                           .argumentType(percentileType)
                           .argumentType("double")
                           .build());
}

bool registerApproxPercentile(const std::string& name) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;
  for (const auto& inputType :
       {"tinyint", "smallint", "integer", "bigint", "real", "double"}) {
    addSignatures(inputType, "double", inputType, signatures);
    addSignatures(
        inputType,
        "array(double)",
        fmt::format("array({})", inputType),
        signatures);
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
          VELOX_USER_CHECK(
              validPercentileType(*argTypes[argTypes.size() - 1 - hasAccuracy]),
              "The type of the percentile argument of {} must be DOUBLE or ARRAY(DOUBLE)",
              name);
        } else {
          VELOX_USER_CHECK_EQ(
              argTypes.size(),
              1,
              "The type of partial result for {} must be ROW",
              name);
          VELOX_USER_CHECK_EQ(
              argTypes[0]->kind(),
              TypeKind::ROW,
              "The type of partial result for {} must be ROW",
              name);
        }

        TypePtr type;
        if (!isRawInput && exec::isPartialOutput(step)) {
          type = argTypes[0]->asRow().childAt(kMinValue);
        } else if (isRawInput) {
          type = argTypes[0];
        } else if (resultType->isArray()) {
          type = resultType->as<TypeKind::ARRAY>().elementType();
        } else {
          type = resultType;
        }

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

} // namespace

void registerApproxPercentileAggregate() {
  registerApproxPercentile(kApproxPercentile);
}

} // namespace facebook::velox::aggregate::prestosql

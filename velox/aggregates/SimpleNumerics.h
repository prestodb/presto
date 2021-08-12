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
#pragma once

#include "velox/aggregates/AggregationHook.h"
#include "velox/exec/Aggregate.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/LazyVector.h"

namespace facebook::velox::aggregate {

template <typename TInput, typename TAccumulator, typename TResult>
class SimpleNumericAggregate : public exec::Aggregate {
 protected:
  explicit SimpleNumericAggregate(
      core::AggregationNode::Step step,
      TypePtr resultType)
      : Aggregate(step, resultType) {}

 public:
  void finalize(char** /* unused */, int32_t /* unused */) override {}

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    extractValues(groups, numGroups, result);
  }

 protected:
  template <typename ExtractOneValue>
  void doExtractValues(
      char** groups,
      int32_t numGroups,
      VectorPtr* result,
      ExtractOneValue extractOneValue) {
    VELOX_CHECK_EQ((*result)->encoding(), VectorEncoding::Simple::FLAT);
    auto vector = (*result)->as<FlatVector<TResult>>();
    VELOX_CHECK(
        vector,
        "Unexpected type of the result vector: {}",
        (*result)->type()->toString());
    VELOX_CHECK_EQ(vector->elementSize(), sizeof(TResult));
    vector->resize(numGroups);
    TResult* rawValues = vector->mutableRawValues();
    uint64_t* rawNulls = getRawNulls(vector);
    for (int32_t i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      if (isNull(group)) {
        vector->setNull(i, true);
      } else {
        clearNull(rawNulls, i);
        rawValues[i] = extractOneValue(group);
      }
    }
  }

  template <bool tableHasNulls, typename UpdateSingleValue>
  void updateGroups(
      char** groups,
      const SelectivityVector& rows,
      const VectorPtr& arg,
      UpdateSingleValue updateSingleValue,
      bool mayPushdown) {
    DecodedVector decoded(*arg, rows, !mayPushdown);
    auto encoding = decoded.base()->encoding();
    auto indices = decoded.indices();
    if (encoding == VectorEncoding::Simple::LAZY) {
      SimpleCallableHook<TInput, TAccumulator, UpdateSingleValue> hook(
          exec::Aggregate::offset_,
          exec::Aggregate::nullByte_,
          exec::Aggregate::nullMask_,
          groups,
          &this->exec::Aggregate::numNulls_,
          updateSingleValue);
      decoded.base()->as<const LazyVector>()->load(
          RowSet(indices, arg->size()), &hook);
      return;
    }

    if (decoded.isConstantMapping()) {
      if (!decoded.isNullAt(0)) {
        auto value = decoded.valueAt<TInput>(0);
        rows.applyToSelected([&](vector_size_t i) {
          updateNonNullValue<tableHasNulls>(
              groups[i], value, updateSingleValue);
        });
      }
    } else if (decoded.mayHaveNulls()) {
      rows.applyToSelected([&](vector_size_t i) {
        if (decoded.isNullAt(i)) {
          return;
        }
        updateNonNullValue<tableHasNulls>(
            groups[i], decoded.valueAt<TInput>(i), updateSingleValue);
      });
    } else if (decoded.isIdentityMapping() && !std::is_same_v<TInput, bool>) {
      auto data = decoded.data<TInput>();
      rows.applyToSelected([&](vector_size_t i) {
        updateNonNullValue<tableHasNulls>(
            groups[i], data[i], updateSingleValue);
      });
    } else {
      rows.applyToSelected([&](vector_size_t i) {
        updateNonNullValue<tableHasNulls>(
            groups[i], decoded.valueAt<TInput>(i), updateSingleValue);
      });
    }
  }

  template <typename UpdateSingle, typename UpdateDuplicate>
  void updateOneGroup(
      char* group,
      const SelectivityVector& allRows,
      const VectorPtr& arg,
      UpdateSingle updateSingleValue,
      UpdateDuplicate updateDuplicateValues,
      bool /*mayPushdown*/,
      TAccumulator initialValue) {
    DecodedVector decoded(*arg, allRows);
    if (decoded.isConstantMapping()) {
      if (!decoded.isNullAt(0)) {
        updateDuplicateValues(
            initialValue, decoded.valueAt<TInput>(0), allRows.end());
        updateNonNullValue<true>(group, initialValue, updateSingleValue);
      }
    } else if (decoded.mayHaveNulls()) {
      for (vector_size_t i = 0; i < allRows.end(); i++) {
        if (!decoded.isNullAt(i)) {
          updateNonNullValue<true>(
              group, decoded.valueAt<TInput>(i), updateSingleValue);
        }
      }
    } else {
      for (vector_size_t i = 0; i < allRows.end(); i++) {
        updateSingleValue(initialValue, decoded.valueAt<TInput>(i));
      }
      updateNonNullValue<true>(group, initialValue, updateSingleValue);
    }
  }

  template <typename THook>
  void
  pushdown(char** groups, const SelectivityVector& rows, const VectorPtr& arg) {
    DecodedVector decoded(*arg, rows, false);
    auto indices = decoded.indices();
    THook hook(
        exec::Aggregate::offset_,
        exec::Aggregate::nullByte_,
        exec::Aggregate::nullMask_,
        groups,
        &this->exec::Aggregate::numNulls_);
    decoded.base()->as<const LazyVector>()->load(
        RowSet(indices, arg->size()), &hook);
  }

 private:
  template <bool tableHasNulls, typename Update>
  inline void
  updateNonNullValue(char* group, TInput value, Update updateValue) {
    if constexpr (tableHasNulls) {
      exec::Aggregate::clearNull(group);
    }
    updateValue(*exec::Aggregate::value<TAccumulator>(group), value);
  }
};

} // namespace facebook::velox::aggregate

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

#include "velox/dwio/common/Range.h"
#include "velox/dwio/dwrf/writer/StatisticsBuilder.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::dwrf {

class StatisticsBuilderUtils {
 public:
  static void addValues(
      StatisticsBuilder& builder,
      const VectorPtr& vector,
      const common::Ranges& ranges);

  static void addValues(
      BooleanStatisticsBuilder& builder,
      const VectorPtr& vector,
      const common::Ranges& ranges);

  static void addValues(
      BooleanStatisticsBuilder& builder,
      const DecodedVector& vector,
      const common::Ranges& ranges);

  template <typename INT>
  static void addValues(
      IntegerStatisticsBuilder& builder,
      const VectorPtr& vector,
      const common::Ranges& ranges);

  template <typename INT>
  static void addValues(
      IntegerStatisticsBuilder& builder,
      const DecodedVector& vector,
      const common::Ranges& ranges);

  template <typename FLOAT>
  static void addValues(
      DoubleStatisticsBuilder& builder,
      const VectorPtr& vector,
      const common::Ranges& ranges);

  static void addValues(
      StringStatisticsBuilder& builder,
      const VectorPtr& vector,
      const common::Ranges& ranges);

  static void addValues(
      BinaryStatisticsBuilder& builder,
      const VectorPtr& vector,
      const common::Ranges& ranges);
};

template <typename INT>
void StatisticsBuilderUtils::addValues(
    IntegerStatisticsBuilder& builder,
    const VectorPtr& vector,
    const common::Ranges& ranges) {
  auto nulls = vector->rawNulls();
  auto vals = vector->asFlatVector<INT>()->rawValues();
  if (vector->mayHaveNulls()) {
    for (auto& pos : ranges) {
      if (bits::isBitNull(nulls, pos)) {
        builder.setHasNull();
      } else {
        builder.addValues(vals[pos]);
      }
    }
  } else {
    for (auto& pos : ranges) {
      builder.addValues(vals[pos]);
    }
  }
}

template <typename INT>
void StatisticsBuilderUtils::addValues(
    IntegerStatisticsBuilder& builder,
    const DecodedVector& vector,
    const common::Ranges& ranges) {
  if (vector.mayHaveNulls()) {
    for (auto& pos : ranges) {
      if (vector.isNullAt(pos)) {
        builder.setHasNull();
      } else {
        builder.addValues(vector.valueAt<INT>(pos));
      }
    }
  } else {
    for (auto& pos : ranges) {
      builder.addValues(vector.valueAt<INT>(pos));
    }
  }
}

template <typename FLOAT>
void StatisticsBuilderUtils::addValues(
    DoubleStatisticsBuilder& builder,
    const VectorPtr& vector,
    const common::Ranges& ranges) {
  auto nulls = vector->rawNulls();
  auto vals = vector->asFlatVector<FLOAT>()->rawValues();
  if (vector->mayHaveNulls()) {
    for (auto& pos : ranges) {
      if (bits::isBitNull(nulls, pos)) {
        builder.setHasNull();
      } else {
        builder.addValues(vals[pos]);
      }
    }
  } else {
    for (auto& pos : ranges) {
      builder.addValues(vals[pos]);
    }
  }
}

} // namespace facebook::velox::dwrf

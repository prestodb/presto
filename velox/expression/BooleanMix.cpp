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
#include "velox/expression/BooleanMix.h"

namespace facebook::velox::exec {

namespace {

/// Checks if bits in specified positions are all set, all unset or mixed.
BooleanMix refineBooleanMixNonNull(
    const uint64_t* bits,
    const SelectivityVector& rows) {
  int32_t first = bits::findFirstBit(bits, rows.begin(), rows.end());
  if (first < 0) {
    return BooleanMix::kAllFalse;
  }
  if (first == rows.begin() && bits::isAllSet(bits, rows.begin(), rows.end())) {
    return BooleanMix::kAllTrue;
  }
  return BooleanMix::kMixNonNull;
}
} // namespace

BooleanMix getFlatBool(
    BaseVector* vector,
    const SelectivityVector& activeRows,
    EvalCtx& context,
    BufferPtr* tempValues,
    BufferPtr* tempNulls,
    bool mergeNullsToValues,
    const uint64_t** valuesOut,
    const uint64_t** nullsOut) {
  VELOX_CHECK_EQ(vector->typeKind(), TypeKind::BOOLEAN);
  const auto size = activeRows.end();
  switch (vector->encoding()) {
    case VectorEncoding::Simple::FLAT: {
      auto values =
          vector->asUnchecked<FlatVector<bool>>()->rawValues<uint64_t>();
      if (!values) {
        return BooleanMix::kAllNull;
      }
      auto nulls = vector->rawNulls();
      if (nulls && mergeNullsToValues) {
        uint64_t* mergedValues;
        BaseVector::ensureBuffer<bool>(
            size, context.pool(), tempValues, &mergedValues);

        // NOTE: false bit in 'nulls' indicate null.
        bits::andBits(
            mergedValues, values, nulls, activeRows.begin(), activeRows.end());

        bits::andBits(
            mergedValues,
            activeRows.asRange().bits(),
            activeRows.begin(),
            activeRows.end());

        *valuesOut = mergedValues;
        return refineBooleanMixNonNull(mergedValues, activeRows);
      }
      *valuesOut = values;
      if (!mergeNullsToValues) {
        *nullsOut = nulls;
      }
      return nulls ? BooleanMix::kMix
                   : refineBooleanMixNonNull(values, activeRows);
    }
    case VectorEncoding::Simple::CONSTANT: {
      if (vector->isNullAt(0)) {
        return BooleanMix::kAllNull;
      }
      return vector->asUnchecked<ConstantVector<bool>>()->valueAt(0)
          ? BooleanMix::kAllTrue
          : BooleanMix::kAllFalse;
    }
    default: {
      uint64_t* nullsToSet = nullptr;
      uint64_t* valuesToSet = nullptr;
      if (vector->mayHaveNulls() && !mergeNullsToValues) {
        BaseVector::ensureBuffer<bool>(
            size, context.pool(), tempNulls, &nullsToSet);
        memset(nullsToSet, bits::kNotNullByte, bits::nbytes(size));
      }
      BaseVector::ensureBuffer<bool>(
          size, context.pool(), tempValues, &valuesToSet);
      memset(valuesToSet, 0, bits::nbytes(size));
      DecodedVector decoded(*vector, activeRows);
      auto values = decoded.data<uint64_t>();
      auto nulls = decoded.nulls();
      auto indices = decoded.indices();
      auto nullIndices = decoded.nullIndices();
      activeRows.applyToSelected([&](int32_t i) {
        auto index = indices[i];
        bool isNull =
            nulls && bits::isBitNull(nulls, nullIndices ? nullIndices[i] : i);
        if (mergeNullsToValues && nulls) {
          if (!isNull && bits::isBitSet(values, index)) {
            bits::setBit(valuesToSet, i);
          }
        } else if (!isNull && bits::isBitSet(values, index)) {
          bits::setBit(valuesToSet, i);
        }
        if (nullsToSet && isNull) {
          bits::setNull(nullsToSet, i);
        }
      });
      if (!mergeNullsToValues) {
        *nullsOut = nullsToSet;
      }
      *valuesOut = valuesToSet;
      return nullsToSet ? BooleanMix::kMix
                        : refineBooleanMixNonNull(valuesToSet, activeRows);
    }
  }
}
} // namespace facebook::velox::exec

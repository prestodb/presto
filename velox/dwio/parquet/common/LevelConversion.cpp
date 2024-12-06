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

// Adapted from Apache Arrow.

#include "velox/dwio/parquet/common/LevelConversion.h"

#include <cassert> // Required for bitmap_writer.h below.
#include <limits>
#include <optional>

#include "arrow/util/bitmap_writer.h"

#include "velox/common/base/Exceptions.h"
#include "velox/dwio/parquet/common/LevelConversionUtil.h"

namespace facebook::velox::parquet {
namespace {

template <typename OffsetType>
void DefRepLevelsToListInfo(
    const int16_t* defLevels,
    const int16_t* repLevels,
    int64_t numDefLevels,
    LevelInfo levelInfo,
    ValidityBitmapInputOutput* output,
    OffsetType* offsets) {
  OffsetType* origPos = offsets;
  std::optional<::arrow::internal::FirstTimeBitmapWriter> validBitsWriter;
  if (output->validBits) {
    validBitsWriter.emplace(
        output->validBits,
        output->validBitsOffset,
        output->valuesReadUpperBound);
  }
  for (int x = 0; x < numDefLevels; x++) {
    // Skip items that belong to empty or null ancestor lists and further nested
    // lists.
    if (defLevels[x] < levelInfo.repeatedAncestorDefLevel ||
        repLevels[x] > levelInfo.repLevel) {
      continue;
    }

    if (repLevels[x] == levelInfo.repLevel) {
      // A continuation of an existing list.
      // offsets can be null for structs with repeated children (we don't need
      // to know offsets until we get to the children).
      if (offsets != nullptr) {
        if (FOLLY_UNLIKELY(
                *offsets == std::numeric_limits<OffsetType>::max())) {
          VELOX_FAIL("List index overflow.");
        }
        *offsets += 1;
      }
    } else {
      if (FOLLY_UNLIKELY(
              (validBitsWriter.has_value() &&
               validBitsWriter->position() >= output->valuesReadUpperBound) ||
              (offsets - origPos) >= output->valuesReadUpperBound)) {
        VELOX_FAIL(
            "Definition levels exceeded upper bound: {}",
            output->valuesReadUpperBound);
      }

      // current_rep < list repLevel i.e. start of a list (ancestor empty lists
      // are filtered out above). offsets can be null for structs with repeated
      // children (we don't need to know offsets until we get to the children).
      if (offsets != nullptr) {
        ++offsets;
        // Use cumulative offsets because variable size lists are more common
        // than fixed size lists so it should be cheaper to make these
        // cumulative and subtract when validating fixed size lists.
        *offsets = *(offsets - 1);
        if (defLevels[x] >= levelInfo.defLevel) {
          if (FOLLY_UNLIKELY(
                  *offsets == std::numeric_limits<OffsetType>::max())) {
            VELOX_FAIL("List index overflow.");
          }
          *offsets += 1;
        }
      }

      if (validBitsWriter.has_value()) {
        // the levelInfo def level for lists reflects element present level.
        // the prior level distinguishes between empty lists.
        if (defLevels[x] >= levelInfo.defLevel - 1) {
          validBitsWriter->Set();
        } else {
          output->nullCount++;
          validBitsWriter->Clear();
        }
        validBitsWriter->Next();
      }
    }
  }
  if (validBitsWriter.has_value()) {
    validBitsWriter->Finish();
  }
  if (offsets != nullptr) {
    output->valuesRead = offsets - origPos;
  } else if (validBitsWriter.has_value()) {
    output->valuesRead = validBitsWriter->position();
  }
  if (output->nullCount > 0 && levelInfo.nullSlotUsage > 1) {
    VELOX_FAIL(
        "Null values with nullSlotUsage > 1 not supported."
        "(i.e. FixedSizeLists with null values are not supported)");
  }
}

} // namespace

void DefLevelsToBitmap(
    const int16_t* defLevels,
    int64_t numDefLevels,
    LevelInfo levelInfo,
    ValidityBitmapInputOutput* output) {
  // It is simpler to rely on repLevel here until PARQUET-1899 is done and the
  // code is deleted in a follow-up release.
  if (levelInfo.repLevel > 0) {
    DefLevelsToBitmapSimd</*has_repeated_parent=*/true>(
        defLevels, numDefLevels, levelInfo, output);
  } else {
    DefLevelsToBitmapSimd</*has_repeated_parent=*/false>(
        defLevels, numDefLevels, levelInfo, output);
  }
}

uint64_t TestOnlyExtractBitsSoftware(uint64_t bitmap, uint64_t selectBitmap) {
  return ExtractBitsSoftware(bitmap, selectBitmap);
}

void DefRepLevelsToList(
    const int16_t* defLevels,
    const int16_t* repLevels,
    int64_t numDefLevels,
    LevelInfo levelInfo,
    ValidityBitmapInputOutput* output,
    int32_t* offsets) {
  DefRepLevelsToListInfo<int32_t>(
      defLevels, repLevels, numDefLevels, levelInfo, output, offsets);
}

void DefRepLevelsToList(
    const int16_t* defLevels,
    const int16_t* repLevels,
    int64_t numDefLevels,
    LevelInfo levelInfo,
    ValidityBitmapInputOutput* output,
    int64_t* offsets) {
  DefRepLevelsToListInfo<int64_t>(
      defLevels, repLevels, numDefLevels, levelInfo, output, offsets);
}

void DefRepLevelsToBitmap(
    const int16_t* defLevels,
    const int16_t* repLevels,
    int64_t numDefLevels,
    LevelInfo levelInfo,
    ValidityBitmapInputOutput* output) {
  // DefRepLevelsToListInfo assumes it for the actual list method and this
  // method is for parent structs, so we need to bump def and ref level.
  levelInfo.repLevel += 1;
  levelInfo.defLevel += 1;
  DefRepLevelsToListInfo<int32_t>(
      defLevels,
      repLevels,
      numDefLevels,
      levelInfo,
      output,
      /*offsets=*/nullptr);
}

} // namespace facebook::velox::parquet

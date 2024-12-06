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

#pragma once

#include <cstdint>

namespace facebook::velox::parquet {

struct LevelInfo {
  LevelInfo()
      : nullSlotUsage(1),
        defLevel(0),
        repLevel(0),
        repeatedAncestorDefLevel(0) {}
  LevelInfo(
      int32_t null_slots,
      int32_t definitionLevel,
      int32_t repetitionLevel,
      int32_t repeatedAncestorDefinitionLevel)
      : nullSlotUsage(null_slots),
        defLevel(static_cast<int16_t>(definitionLevel)),
        repLevel(static_cast<int16_t>(repetitionLevel)),
        repeatedAncestorDefLevel(
            static_cast<int16_t>(repeatedAncestorDefinitionLevel)) {}

  bool operator==(const LevelInfo& b) const {
    return nullSlotUsage == b.nullSlotUsage && defLevel == b.defLevel &&
        repLevel == b.repLevel &&
        repeatedAncestorDefLevel == b.repeatedAncestorDefLevel;
  }

  bool HasNullableValues() const {
    return repeatedAncestorDefLevel < defLevel;
  }

  // How many slots an undefined but present (i.e. null) element in
  // parquet consumes when decoding to Arrow.
  // "Slot" is used in the same context as the Arrow specification
  // (i.e. a value holder).
  // This is only ever >1 for descendents of FixedSizeList.
  int32_t nullSlotUsage = 1;

  // The definition level at which the value for the field
  // is considered not null (definition levels greater than
  // or equal to this value indicate a not-null
  // value for the field). For list fields definition levels
  // greater than or equal to this field indicate a present,
  // possibly null, child value.
  int16_t defLevel = 0;

  // The repetition level corresponding to this element
  // or the closest repeated ancestor.  Any repetition
  // level less than this indicates either a new list OR
  // an empty list (which is determined in conjunction
  // with definition levels).
  int16_t repLevel = 0;

  // The definition level indicating the level at which the closest
  // repeated ancestor is not empty.  This is used to discriminate
  // between a value less than |defLevel| being null or excluded entirely.
  // For instance if we have an arrow schema like:
  // list(struct(f0: int)).  Then then there are the following
  // definition levels:
  //   0 = null list
  //   1 = present but empty list.
  //   2 = a null value in the list
  //   3 = a non null struct but null integer.
  //   4 = a present integer.
  // When reconstructing, the struct and integer arrays'
  // repeatedAncestorDefLevel would be 2.  Any
  // defLevel < 2 indicates that there isn't a corresponding
  // child value in the list.
  // i.e. [null, [], [null], [{f0: null}], [{f0: 1}]]
  // has the def levels [0, 1, 2, 3, 4].  The actual
  // struct array is only of length 3: [not-set, set, set] and
  // the int array is also of length 3: [N/A, null, 1].
  //
  int16_t repeatedAncestorDefLevel = 0;

  /// Increments level for a optional node.
  void IncrementOptional() {
    defLevel++;
  }

  /// Increments levels for the repeated node.  Returns
  /// the previous ancestor_list_defLevel.
  int16_t IncrementRepeated() {
    int16_t lastRepeatedAncestor = repeatedAncestorDefLevel;

    // Repeated fields add both a repetition and definition level. This is used
    // to distinguish between an empty list and a list with an item in it.
    ++repLevel;
    ++defLevel;
    // For levels >= repeated_ancenstor_defLevel it indicates the list was
    // non-null and had at least one element.  This is important
    // for later decoding because we need to add a slot for these
    // values.  for levels < current_defLevel no slots are added
    // to arrays.
    repeatedAncestorDefLevel = defLevel;
    return lastRepeatedAncestor;
  }
};

// Input/Output structure for reconstructed validity bitmaps.
struct ValidityBitmapInputOutput {
  // Input only.
  // The maximum number of valuesRead expected (actual
  // values read must be less than or equal to this value).
  // If this number is exceeded methods will throw a
  // ParquetException. Exceeding this limit indicates
  // either a corrupt or incorrectly written file.
  int64_t valuesReadUpperBound = 0;
  // Output only. The number of values added to the encountered
  // (this is logically the count of the number of elements
  // for an Arrow array).
  int64_t valuesRead = 0;
  // Input/Output. The number of nulls encountered.
  int64_t nullCount = 0;
  // Output only. The validity bitmap to populate. Maybe be null only
  // for DefRepLevelsToListInfo (if all that is needed is list offsets).
  uint8_t* validBits = nullptr;
  // Input only, offset into validBits to start at.
  int64_t validBitsOffset = 0;
};

//  Converts defLevels to validity bitmaps for non-list arrays and structs that
//  have at least one member that is not a list and has no list descendents. For
//  lists use DefRepLevelsToList and structs where all descendants contain a
//  list use DefRepLevelsToBitmap.
void DefLevelsToBitmap(
    const int16_t* defLevels,
    int64_t numDefLevels,
    LevelInfo levelInfo,
    ValidityBitmapInputOutput* output);

// Reconstructs a validity bitmap and list offsets for a list arrays based on
// def/rep levels. The first element of offsets will not be modified if
// repLevels starts with a new list.  The first element of offsets will be used
// when calculating the next offset.  See documentation onf DefLevelsToBitmap
// for when to use this method vs the other ones in this file for
// reconstruction.
//
// Offsets must be sized to 1 + valuesReadUpperBound.
void DefRepLevelsToList(
    const int16_t* defLevels,
    const int16_t* repLevels,
    int64_t numDefLevels,
    LevelInfo levelInfo,
    ValidityBitmapInputOutput* output,
    int32_t* offsets);

void DefRepLevelsToList(
    const int16_t* defLevels,
    const int16_t* repLevels,
    int64_t numDefLevels,
    LevelInfo levelInfo,
    ValidityBitmapInputOutput* output,
    int64_t* offsets);

// Reconstructs a validity bitmap for a struct every member is a list or has
// a list descendant.  See documentation on DefLevelsToBitmap for when more
// details on this method compared to the other ones defined above.
void DefRepLevelsToBitmap(
    const int16_t* defLevels,
    const int16_t* repLevels,
    int64_t numDefLevels,
    LevelInfo levelInfo,
    ValidityBitmapInputOutput* output);

// This is exposed to ensure we can properly test a software simulated pext
// function (i.e. it isn't hidden by runtime dispatch).
uint64_t TestOnlyExtractBitsSoftware(uint64_t bitmap, uint64_t selection);

} // namespace facebook::velox::parquet

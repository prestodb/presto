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

#include "velox/common/base/PrefixSortConfig.h"
#include "velox/common/memory/MemoryAllocator.h"
#include "velox/exec/RowContainer.h"
#include "velox/exec/prefixsort/PrefixSortAlgorithm.h"
#include "velox/exec/prefixsort/PrefixSortEncoder.h"

namespace facebook::velox::exec {

namespace detail {

FOLLY_ALWAYS_INLINE void stdSort(
    std::vector<char*>& rows,
    RowContainer* rowContainer,
    const std::vector<CompareFlags>& compareFlags) {
  std::sort(
      rows.begin(), rows.end(), [&](const char* leftRow, const char* rightRow) {
        for (auto i = 0; i < compareFlags.size(); ++i) {
          if (auto result = rowContainer->compare(
                  leftRow, rightRow, i, compareFlags[i])) {
            return result < 0;
          }
        }
        return false;
      });
}
}; // namespace detail

/// The layout of prefix-sort buffer, a prefix entry includes:
/// 1. normalized keys
/// 2. non-normalized data ptr for semi-normalized types such as
/// string_view`s ptr, it will be filled when support Varchar.
/// 3. the row address ptr point to RowContainer`s rows is added at the end of
/// prefix.
struct PrefixSortLayout {
  /// Number of bytes to store a prefix, it equals to:
  /// normalizedKeySize_ + 8 (non-normalized-ptr) + 8(row address).
  const uint64_t entrySize;

  /// If a sort key supports normalization and can be added to the prefix
  /// sort buffer, it is called a normalized key.
  const uint32_t normalizedBufferSize;

  const uint32_t numNormalizedKeys;

  /// The num of sort keys include normalized and non-normalized.
  const uint32_t numKeys;

  /// CompareFlags of all sort keys.
  const std::vector<CompareFlags> compareFlags;

  /// Whether the sort keys contains normalized key.
  /// It equals to 'numNormalizedKeys == 0', a little faster.
  const bool noNormalizedKeys;

  /// Whether the sort keys contains non-normalized key.
  const bool hasNonNormalizedKey;

  /// Offsets of normalized keys, used to find write locations when
  /// extracting columns
  const std::vector<uint32_t> prefixOffsets;

  /// The encoders for normalized keys.
  const std::vector<prefixsort::PrefixSortEncoder> encoders;

  /// Align the buffer size to 8 so that long compare can replace byte compare
  /// during ‘memcmp’
  const int32_t padding;

  static PrefixSortLayout makeSortLayout(
      const std::vector<TypePtr>& types,
      const std::vector<CompareFlags>& compareFlags,
      uint32_t maxNormalizedKeySize);
};

class PrefixSort {
 public:
  PrefixSort(
      memory::MemoryPool* pool,
      RowContainer* rowContainer,
      const PrefixSortLayout& sortLayout);

  /// Follow the steps below to sort the data in RowContainer:
  /// 1. Allocate a contiguous block of memory to store normalized keys.
  /// 2. Extract the sort keys from the RowContainer. If the key can be
  /// normalized, normalize it. For this kind of keys can be normalized，we
  /// combine them with the original row address ptr and store them
  /// together into a buffer, called 'Prefix'.
  /// 3. Sort the prefixes data we got in step 2.
  /// For keys can normalized(All fixed width types), we use 'memcmp' to compare
  /// the normalized binary string.
  /// For keys can not normalized, we use RowContainer`s compare method to
  /// compare value.
  /// For keys can part-normalized(Varchar, Row etc.), we will store the
  /// normalized part and points to raw data in prefix, and custom the points
  /// compare. The compare strategy will be defined in PrefixSortLayout as
  /// follow-up, we treat this part as non-normalized until we implement all
  /// fixed width types.
  /// For complex types, e.g. ROW that can be converted to scalar types will be
  /// supported.
  /// 4. Extract the original row address ptr from prefixes (previously stored
  /// them in the prefix buffer) into the input rows vector.
  ///
  /// @param rows The result of RowContainer::listRows(), assuming that the
  /// caller (SortBuffer etc.) has already got the result.
  FOLLY_ALWAYS_INLINE static void sort(
      std::vector<char*>& rows,
      memory::MemoryPool* pool,
      RowContainer* rowContainer,
      const std::vector<CompareFlags>& compareFlags,
      const velox::common::PrefixSortConfig& config) {
    if (rowContainer->numRows() < config.threshold) {
      detail::stdSort(rows, rowContainer, compareFlags);
      return;
    }
    VELOX_DCHECK_EQ(rowContainer->keyTypes().size(), compareFlags.size());
    const auto sortLayout = PrefixSortLayout::makeSortLayout(
        rowContainer->keyTypes(), compareFlags, config.maxNormalizedKeySize);
    // All keys can not normalize, skip the binary string compare opt.
    // Putting this outside sort-internal helps with inline std-sort.
    if (sortLayout.noNormalizedKeys) {
      detail::stdSort(rows, rowContainer, compareFlags);
      return;
    }

    PrefixSort prefixSort(pool, rowContainer, sortLayout);
    prefixSort.sortInternal(rows);
  }

 private:
  void sortInternal(std::vector<char*>& rows);

  int compareAllNormalizedKeys(char* left, char* right);

  int comparePartNormalizedKeys(char* left, char* right);

  void extractRowToPrefix(char* row, char* prefix);

  // Return the reference of row address ptr for read/write.
  FOLLY_ALWAYS_INLINE char*& getAddressFromPrefix(char* prefix) {
    return *reinterpret_cast<char**>(prefix + sortLayout_.normalizedBufferSize);
  }

  memory::MemoryPool* const pool_;
  const PrefixSortLayout sortLayout_;
  RowContainer* const rowContainer_;
};
} // namespace facebook::velox::exec

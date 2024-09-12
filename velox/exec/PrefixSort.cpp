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
#include "velox/exec/PrefixSort.h"

using namespace facebook::velox::exec::prefixsort;

namespace facebook::velox::exec {

namespace {

// For alignment, 8 is faster than 4.
// If the alignment is changed from 8 to 4, you need to change bitswap64
// to bitswap32.
const int32_t kAlignment = 8;

template <typename T>
FOLLY_ALWAYS_INLINE void encodeRowColumn(
    const PrefixSortLayout& prefixSortLayout,
    const uint32_t index,
    const RowColumn& rowColumn,
    char* const row,
    char* const prefix) {
  std::optional<T> value;
  if (RowContainer::isNullAt(row, rowColumn.nullByte(), rowColumn.nullMask())) {
    value = std::nullopt;
  } else {
    value = *(reinterpret_cast<T*>(row + rowColumn.offset()));
  }
  prefixSortLayout.encoders[index].encode(
      value, prefix + prefixSortLayout.prefixOffsets[index]);
}

FOLLY_ALWAYS_INLINE void extractRowColumnToPrefix(
    TypeKind typeKind,
    const PrefixSortLayout& prefixSortLayout,
    const uint32_t index,
    const RowColumn& rowColumn,
    char* const row,
    char* const prefix) {
  switch (typeKind) {
    case TypeKind::SMALLINT: {
      encodeRowColumn<int16_t>(prefixSortLayout, index, rowColumn, row, prefix);
      return;
    }
    case TypeKind::INTEGER: {
      encodeRowColumn<int32_t>(prefixSortLayout, index, rowColumn, row, prefix);
      return;
    }
    case TypeKind::BIGINT: {
      encodeRowColumn<int64_t>(prefixSortLayout, index, rowColumn, row, prefix);
      return;
    }
    case TypeKind::REAL: {
      encodeRowColumn<float>(prefixSortLayout, index, rowColumn, row, prefix);
      return;
    }
    case TypeKind::DOUBLE: {
      encodeRowColumn<double>(prefixSortLayout, index, rowColumn, row, prefix);
      return;
    }
    case TypeKind::TIMESTAMP: {
      encodeRowColumn<Timestamp>(
          prefixSortLayout, index, rowColumn, row, prefix);
      return;
    }
    default:
      VELOX_UNSUPPORTED(
          "prefix-sort does not support type kind: {}",
          mapTypeKindToName(typeKind));
  }
}

FOLLY_ALWAYS_INLINE int32_t alignmentPadding(int32_t size, int32_t alignment) {
  auto extra = size % alignment;
  return extra == 0 ? 0 : alignment - extra;
}

FOLLY_ALWAYS_INLINE void bitsSwapByWord(uint64_t* address, int32_t bytes) {
  while (bytes != 0) {
    *address = __builtin_bswap64(*address);
    ++address;
    bytes -= kAlignment;
  }
}

FOLLY_ALWAYS_INLINE int
compareByWord(uint64_t* left, uint64_t* right, int32_t bytes) {
  while (bytes != 0) {
    if (*left == *right) {
      ++left;
      ++right;
      bytes -= kAlignment;
      continue;
    }
    if (*left > *right) {
      return 1;
    } else {
      return -1;
    }
  }
  return 0;
}

} // namespace

PrefixSortLayout PrefixSortLayout::makeSortLayout(
    const std::vector<TypePtr>& types,
    const std::vector<CompareFlags>& compareFlags,
    uint32_t maxNormalizedKeySize) {
  uint32_t normalizedKeySize = 0;
  uint32_t numNormalizedKeys = 0;
  const uint32_t numKeys = types.size();
  std::vector<uint32_t> prefixOffsets;
  std::vector<PrefixSortEncoder> encoders;

  // Calculate encoders and prefix-offsets, and stop the loop if a key that
  // cannot be normalized is encountered.
  for (auto i = 0; i < numKeys; ++i) {
    if (normalizedKeySize > maxNormalizedKeySize) {
      break;
    }
    std::optional<uint32_t> encodedSize =
        PrefixSortEncoder::encodedSize(types[i]->kind());
    if (encodedSize.has_value()) {
      prefixOffsets.push_back(normalizedKeySize);
      encoders.push_back(
          {compareFlags[i].ascending, compareFlags[i].nullsFirst});
      normalizedKeySize += encodedSize.value();
      numNormalizedKeys++;
    } else {
      break;
    }
  }
  auto padding = alignmentPadding(normalizedKeySize, kAlignment);
  normalizedKeySize += padding;
  return PrefixSortLayout{
      normalizedKeySize + sizeof(char*),
      normalizedKeySize,
      numNormalizedKeys,
      numKeys,
      compareFlags,
      numNormalizedKeys == 0,
      numNormalizedKeys < numKeys,
      std::move(prefixOffsets),
      std::move(encoders),
      padding};
}

FOLLY_ALWAYS_INLINE int PrefixSort::compareAllNormalizedKeys(
    char* left,
    char* right) {
  return compareByWord(
      (uint64_t*)left, (uint64_t*)right, sortLayout_.normalizedBufferSize);
}

int PrefixSort::comparePartNormalizedKeys(char* left, char* right) {
  int result = compareAllNormalizedKeys(left, right);
  if (result != 0) {
    return result;
  }
  // If prefixes are equal, compare the left sort keys with rowContainer.
  char* leftAddress = getAddressFromPrefix(left);
  char* rightAddress = getAddressFromPrefix(right);
  for (auto i = sortLayout_.numNormalizedKeys; i < sortLayout_.numKeys; ++i) {
    result = rowContainer_->compare(
        leftAddress, rightAddress, i, sortLayout_.compareFlags[i]);
    if (result != 0) {
      return result;
    }
  }
  return result;
}

PrefixSort::PrefixSort(
    memory::MemoryPool* pool,
    RowContainer* rowContainer,
    const PrefixSortLayout& sortLayout)
    : pool_(pool), sortLayout_(sortLayout), rowContainer_(rowContainer) {}

void PrefixSort::extractRowToPrefix(char* row, char* prefix) {
  for (auto i = 0; i < sortLayout_.numNormalizedKeys; i++) {
    extractRowColumnToPrefix(
        rowContainer_->keyTypes()[i]->kind(),
        sortLayout_,
        i,
        rowContainer_->columnAt(i),
        row,
        prefix);
  }
  simd::memset(
      prefix + sortLayout_.normalizedBufferSize - sortLayout_.padding,
      0,
      sortLayout_.padding);
  // When comparing in std::memcmp, each byte is compared. If it is changed to
  // compare every 8 bytes, the number of comparisons will be reduced and the
  // performance will be improved.
  // Use uint64_t compare to implement the above-mentioned comparison of every 8
  // bytes, assuming the system is little-endian, need to reverse bytes for
  // every 8 bytes.
  bitsSwapByWord((uint64_t*)prefix, sortLayout_.normalizedBufferSize);
  // Set row address.
  getAddressFromPrefix(prefix) = row;
}

void PrefixSort::sortInternal(std::vector<char*>& rows) {
  const auto numRows = rows.size();
  const auto entrySize = sortLayout_.entrySize;
  memory::ContiguousAllocation prefixAllocation;
  // 1. Allocate prefixes data.
  {
    const auto numPages =
        memory::AllocationTraits::numPages(numRows * entrySize);
    pool_->allocateContiguous(numPages, prefixAllocation);
  }
  char* const prefixes = prefixAllocation.data<char>();

  // 2. Extract rows to prefixes with row address.
  for (auto i = 0; i < rows.size(); ++i) {
    extractRowToPrefix(rows[i], prefixes + entrySize * i);
  }

  // 3. Sort prefixes with row address.
  {
    const auto swapBuffer = AlignedBuffer::allocate<char>(entrySize, pool_);
    PrefixSortRunner sortRunner(entrySize, swapBuffer->asMutable<char>());
    const auto start = prefixes;
    const auto end = prefixes + numRows * entrySize;
    if (sortLayout_.hasNonNormalizedKey) {
      sortRunner.quickSort(start, end, [&](char* a, char* b) {
        return comparePartNormalizedKeys(a, b);
      });
    } else {
      sortRunner.quickSort(start, end, [&](char* a, char* b) {
        return compareAllNormalizedKeys(a, b);
      });
    }
  }
  // 4. Output sorted row addresses.
  for (int i = 0; i < rows.size(); i++) {
    rows[i] = getAddressFromPrefix(prefixes + i * entrySize);
  }
}

} // namespace facebook::velox::exec

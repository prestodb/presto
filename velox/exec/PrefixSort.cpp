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
static constexpr int32_t kAlignment = 8;

template <typename T>
FOLLY_ALWAYS_INLINE void encodeRowColumn(
    const PrefixSortLayout& prefixSortLayout,
    column_index_t index,
    const RowColumn& rowColumn,
    char* row,
    char* prefixBuffer) {
  std::optional<T> value;
  if (RowContainer::isNullAt(row, rowColumn.nullByte(), rowColumn.nullMask())) {
    value = std::nullopt;
  } else {
    value = *(reinterpret_cast<T*>(row + rowColumn.offset()));
  }
  prefixSortLayout.encoders[index].encode(
      value, prefixBuffer + prefixSortLayout.prefixOffsets[index]);
}

FOLLY_ALWAYS_INLINE void extractRowColumnToPrefix(
    TypeKind typeKind,
    const PrefixSortLayout& prefixSortLayout,
    uint32_t index,
    const RowColumn& rowColumn,
    char* row,
    char* prefixBuffer) {
  switch (typeKind) {
    case TypeKind::SMALLINT: {
      encodeRowColumn<int16_t>(
          prefixSortLayout, index, rowColumn, row, prefixBuffer);
      return;
    }
    case TypeKind::INTEGER: {
      encodeRowColumn<int32_t>(
          prefixSortLayout, index, rowColumn, row, prefixBuffer);
      return;
    }
    case TypeKind::BIGINT: {
      encodeRowColumn<int64_t>(
          prefixSortLayout, index, rowColumn, row, prefixBuffer);
      return;
    }
    case TypeKind::REAL: {
      encodeRowColumn<float>(
          prefixSortLayout, index, rowColumn, row, prefixBuffer);
      return;
    }
    case TypeKind::DOUBLE: {
      encodeRowColumn<double>(
          prefixSortLayout, index, rowColumn, row, prefixBuffer);
      return;
    }
    case TypeKind::TIMESTAMP: {
      encodeRowColumn<Timestamp>(
          prefixSortLayout, index, rowColumn, row, prefixBuffer);
      return;
    }
    default:
      VELOX_UNSUPPORTED(
          "prefix-sort does not support type kind: {}",
          mapTypeKindToName(typeKind));
  }
}

FOLLY_ALWAYS_INLINE int32_t alignmentPadding(int32_t size, int32_t alignment) {
  const auto extra = size % alignment;
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
  const uint32_t numKeys = types.size();
  std::vector<uint32_t> prefixOffsets;
  prefixOffsets.reserve(numKeys);
  std::vector<PrefixSortEncoder> encoders;
  encoders.reserve(numKeys);

  // Calculate encoders and prefix-offsets, and stop the loop if a key that
  // cannot be normalized is encountered.
  uint32_t normalizedKeySize{0};
  uint32_t numNormalizedKeys{0};
  for (auto i = 0; i < numKeys; ++i) {
    if (normalizedKeySize > maxNormalizedKeySize) {
      break;
    }
    const std::optional<uint32_t> encodedSize =
        PrefixSortEncoder::encodedSize(types[i]->kind());
    if (!encodedSize.has_value()) {
      break;
    }
    prefixOffsets.push_back(normalizedKeySize);
    encoders.push_back({compareFlags[i].ascending, compareFlags[i].nullsFirst});
    normalizedKeySize += encodedSize.value();
    ++numNormalizedKeys;
  }

  const auto numPaddingBytes = alignmentPadding(normalizedKeySize, kAlignment);
  normalizedKeySize += numPaddingBytes;

  return PrefixSortLayout{
      normalizedKeySize + sizeof(char*),
      normalizedKeySize,
      numNormalizedKeys,
      numKeys,
      compareFlags,
      numNormalizedKeys != 0,
      numNormalizedKeys < numKeys,
      std::move(prefixOffsets),
      std::move(encoders),
      numPaddingBytes};
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

  // If prefixes are equal, compare the remaining sort keys with rowContainer.
  char* leftRow = getRowAddrFromPrefixBuffer(left);
  char* rightRow = getRowAddrFromPrefixBuffer(right);
  for (auto i = sortLayout_.numNormalizedKeys; i < sortLayout_.numKeys; ++i) {
    result = rowContainer_->compare(
        leftRow, rightRow, i, sortLayout_.compareFlags[i]);
    if (result != 0) {
      return result;
    }
  }
  return result;
}

PrefixSort::PrefixSort(
    const RowContainer* rowContainer,
    const PrefixSortLayout& sortLayout,
    memory::MemoryPool* pool)
    : rowContainer_(rowContainer), sortLayout_(sortLayout), pool_(pool) {}

void PrefixSort::extractRowAndEncodePrefixKeys(char* row, char* prefixBuffer) {
  for (auto i = 0; i < sortLayout_.numNormalizedKeys; ++i) {
    extractRowColumnToPrefix(
        rowContainer_->keyTypes()[i]->kind(),
        sortLayout_,
        i,
        rowContainer_->columnAt(i),
        row,
        prefixBuffer);
  }

  simd::memset(
      prefixBuffer + sortLayout_.normalizedBufferSize -
          sortLayout_.numPaddingBytes,
      0,
      sortLayout_.numPaddingBytes);

  // When comparing in std::memcmp, each byte is compared. If it is changed to
  // compare every 8 bytes, the number of comparisons will be reduced and the
  // performance will be improved.
  // Use uint64_t compare to implement the above-mentioned comparison of every 8
  // bytes, assuming the system is little-endian, need to reverse bytes for
  // every 8 bytes.
  bitsSwapByWord((uint64_t*)prefixBuffer, sortLayout_.normalizedBufferSize);

  // Set row address.
  getRowAddrFromPrefixBuffer(prefixBuffer) = row;
}

// static.
uint32_t PrefixSort::maxRequiredBytes(
    const RowContainer* rowContainer,
    const std::vector<CompareFlags>& compareFlags,
    const velox::common::PrefixSortConfig& config,
    memory::MemoryPool* pool) {
  if (rowContainer->numRows() < config.threshold) {
    return 0;
  }
  VELOX_CHECK_EQ(rowContainer->keyTypes().size(), compareFlags.size());
  const auto sortLayout = PrefixSortLayout::makeSortLayout(
      rowContainer->keyTypes(), compareFlags, config.maxNormalizedKeySize);
  if (!sortLayout.hasNormalizedKeys) {
    return 0;
  }

  const PrefixSort prefixSort(rowContainer, sortLayout, pool);
  return prefixSort.maxRequiredBytes();
}

// static
void PrefixSort::stdSort(
    std::vector<char*, memory::StlAllocator<char*>>& rows,
    const RowContainer* rowContainer,
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

uint32_t PrefixSort::maxRequiredBytes() const {
  const auto numRows = rowContainer_->numRows();
  const auto numPages =
      memory::AllocationTraits::numPages(numRows * sortLayout_.entrySize);
  // Prefix data size + swap buffer size.
  return memory::AllocationTraits::pageBytes(numPages) +
      pool_->preferredSize(checkedPlus<size_t>(
          sortLayout_.entrySize, AlignedBuffer::kPaddedSize)) +
      2 * pool_->alignment();
}

void PrefixSort::sortInternal(
    std::vector<char*, memory::StlAllocator<char*>>& rows) {
  const auto numRows = rows.size();
  const auto entrySize = sortLayout_.entrySize;
  memory::ContiguousAllocation prefixBufferAlloc;
  // Allocates prefix sort buffer.
  {
    const auto numPages =
        memory::AllocationTraits::numPages(numRows * entrySize);
    pool_->allocateContiguous(numPages, prefixBufferAlloc);
  }
  char* prefixBuffer = prefixBufferAlloc.data<char>();

  // Extracts rows, and stores the serialized normalized keys plus the row
  // address (in row container) to prefix sort buffer.
  for (auto i = 0; i < rows.size(); ++i) {
    extractRowAndEncodePrefixKeys(rows[i], prefixBuffer + entrySize * i);
  }

  // Sort rows with the normalized prefix keys.
  {
    const auto swapBuffer = AlignedBuffer::allocate<char>(entrySize, pool_);
    PrefixSortRunner sortRunner(entrySize, swapBuffer->asMutable<char>());
    auto* prefixBufferStart = prefixBuffer;
    auto* prefixBufferEnd = prefixBuffer + numRows * entrySize;
    if (sortLayout_.hasNonNormalizedKey) {
      sortRunner.quickSort(
          prefixBufferStart, prefixBufferEnd, [&](char* lhs, char* rhs) {
            return comparePartNormalizedKeys(lhs, rhs);
          });
    } else {
      sortRunner.quickSort(
          prefixBufferStart, prefixBufferEnd, [&](char* lhs, char* rhs) {
            return compareAllNormalizedKeys(lhs, rhs);
          });
    }
  }

  // Output sorted row addresses.
  for (auto i = 0; i < rows.size(); ++i) {
    rows[i] = getRowAddrFromPrefixBuffer(prefixBuffer + i * entrySize);
  }
}

} // namespace facebook::velox::exec

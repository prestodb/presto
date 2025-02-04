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

#include "velox/common/memory/ByteStream.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/type/DecimalUtil.h"
#include "velox/type/Type.h"
#include "velox/vector/DictionaryVector.h"
#include "velox/vector/VectorStream.h"

namespace facebook::velox::serializer::presto::detail {
class VectorStream;

constexpr int8_t kCompressedBitMask = 1;
constexpr int8_t kEncryptedBitMask = 2;
constexpr int8_t kCheckSumBitMask = 4;
// uncompressed size comes after the number of rows and the codec
constexpr int32_t kSizeInBytesOffset{4 + 1};
// There header for a page is:
// + number of rows (4 bytes)
// + codec (1 byte)
// + uncompressed size (4 bytes)
// + size (4 bytes) (this is the compressed size if the data is compressed,
//                   otherwise it's uncompressed size again)
// + checksum (8 bytes)
//
// See https://prestodb.io/docs/current/develop/serialized-page.html for a
// detailed specification of the format.
constexpr int32_t kHeaderSize{kSizeInBytesOffset + 4 + 4 + 8};
static inline const std::string_view kByteArray{"BYTE_ARRAY"};
static inline const std::string_view kShortArray{"SHORT_ARRAY"};
static inline const std::string_view kIntArray{"INT_ARRAY"};
static inline const std::string_view kLongArray{"LONG_ARRAY"};
static inline const std::string_view kInt128Array{"INT128_ARRAY"};
static inline const std::string_view kVariableWidth{"VARIABLE_WIDTH"};
static inline const std::string_view kArray{"ARRAY"};
static inline const std::string_view kMap{"MAP"};
static inline const std::string_view kRow{"ROW"};
static inline const std::string_view kRLE{"RLE"};
static inline const std::string_view kDictionary{"DICTIONARY"};

void initBitsToMapOnce();

FOLLY_ALWAYS_INLINE int128_t toJavaDecimalValue(int128_t value) {
  // Presto Java UnscaledDecimal128 representation uses signed magnitude
  // representation. Only negative values differ in this representation.
  if (value < 0) {
    value *= -1;
    value |= DecimalUtil::kInt128Mask;
  }
  return value;
}

FOLLY_ALWAYS_INLINE int128_t toJavaUuidValue(int128_t value) {
  // Presto Java UuidType expects 2 long values with MSB first.
  // int128 will be serialized with [lower, upper], so swap here
  // to adjust the order.
  return DecimalUtil::bigEndian(value);
}

inline void writeInt32(OutputStream* out, int32_t value) {
  out->write(reinterpret_cast<char*>(&value), sizeof(value));
}

inline void writeInt64(OutputStream* out, int64_t value) {
  out->write(reinterpret_cast<char*>(&value), sizeof(value));
}

std::string_view typeToEncodingName(const TypePtr& type);

template <typename RangeType>
inline int32_t rangesTotalSize(const folly::Range<const RangeType*>& ranges) {
  int32_t total = 0;
  for (auto& range : ranges) {
    total += range.size;
  }
  return total;
}

// Returns ranges for the non-null rows of an array  or map. 'rows' gives the
// rows. nulls is the nulls of the array/map or nullptr if no nulls. 'offsets'
// and 'sizes' are the offsets and sizes of the array/map.Returns the number of
// index ranges. Obtains the ranges from 'rangesHolder'. If 'sizesPtr' is
// non-null, gets returns  the sizes for the inner ranges in 'sizesHolder'. If
// 'stream' is non-null, writes the lengths and nulls for the array/map into
// 'stream'.
int32_t rowsToRanges(
    folly::Range<const vector_size_t*> rows,
    const uint64_t* rawNulls,
    const vector_size_t* offsets,
    const vector_size_t* sizes,
    vector_size_t** sizesPtr,
    ScratchPtr<IndexRange>& rangesHolder,
    ScratchPtr<vector_size_t*>* sizesHolder,
    VectorStream* stream,
    Scratch& scratch);

template <typename T>
vector_size_t computeSelectedIndices(
    const DictionaryVector<T>* dictionaryVector,
    const folly::Range<const IndexRange*>& ranges,
    Scratch& scratch,
    vector_size_t* selectedIndices) {
  // Create a bit set to track which values in the Dictionary are used.
  ScratchPtr<uint64_t, 64> usedIndicesHolder(scratch);
  auto* usedIndices = usedIndicesHolder.get(
      bits::nwords(dictionaryVector->valueVector()->size()));
  simd::memset(usedIndices, 0, usedIndicesHolder.size() * sizeof(uint64_t));

  auto* indices = dictionaryVector->indices()->template as<vector_size_t>();
  for (const auto& range : ranges) {
    for (auto i = 0; i < range.size; ++i) {
      bits::setBit(usedIndices, indices[range.begin + i]);
    }
  }

  // Convert the bitset to a list of the used indices.
  return simd::indicesOfSetBits(
      usedIndices, 0, dictionaryVector->valueVector()->size(), selectedIndices);
}

// Represents sizes  of a flush. If the sizes are equal, no compression is
// applied. Otherwise 'compressedSize' must be less than 'uncompressedSize'.
struct FlushSizes {
  int64_t uncompressedSize;
  int64_t compressedSize;
};

FlushSizes flushStreams(
    std::vector<VectorStream>& streams,
    int32_t numRows,
    const StreamArena& arena,
    folly::io::Codec& codec,
    float minCompressionRatio,
    OutputStream* out);

FOLLY_ALWAYS_INLINE bool needCompression(const folly::io::Codec& codec) {
  return codec.type() != folly::io::CodecType::NO_COMPRESSION;
}

inline int64_t computeChecksum(
    PrestoOutputStreamListener* listener,
    int codecMarker,
    int numRows,
    int uncompressedSize) {
  auto result = listener->crc();
  result.process_bytes(&codecMarker, 1);
  result.process_bytes(&numRows, 4);
  result.process_bytes(&uncompressedSize, 4);
  return result.checksum();
}

void serializeColumn(
    const VectorPtr& vector,
    const folly::Range<const IndexRange*>& ranges,
    VectorStream* stream,
    Scratch& scratch);

void serializeColumn(
    const VectorPtr& vector,
    const folly::Range<const vector_size_t*>& rows,
    VectorStream* stream,
    Scratch& scratch);
} // namespace facebook::velox::serializer::presto::detail

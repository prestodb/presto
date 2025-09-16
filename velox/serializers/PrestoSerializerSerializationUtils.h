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
#include <folly/IPAddressV6.h>

#include "velox/common/memory/ByteStream.h"
#include "velox/functions/prestosql/types/IPPrefixType.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/serializers/VectorStream.h"
#include "velox/type/DecimalUtil.h"
#include "velox/type/Type.h"
#include "velox/vector/DictionaryVector.h"
#include "velox/vector/VectorStream.h"

namespace facebook::velox::serializer::presto::detail {

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

FOLLY_ALWAYS_INLINE std::array<int8_t, ipaddress::kIPPrefixBytes>
toJavaIPPrefixType(int128_t currentIpBytes, int8_t prefix) {
  std::array<int8_t, ipaddress::kIPPrefixBytes> byteArray{{0}};
  memcpy(&byteArray[0], &currentIpBytes, sizeof(currentIpBytes));
  memcpy(&byteArray[sizeof(currentIpBytes)], &prefix, sizeof(prefix));
  if constexpr (folly::kIsLittleEndian) {
    std::reverse(byteArray.begin(), byteArray.begin() + sizeof(currentIpBytes));
    return byteArray;
  } else {
    return byteArray;
  }
}

FOLLY_ALWAYS_INLINE int128_t
reverseIpAddressByteOrder(int128_t currentIpBytes) {
  return DecimalUtil::bigEndian(currentIpBytes);
}

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

inline int32_t rangesTotalSize(const folly::Range<const IndexRange*>& ranges) {
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

FOLLY_ALWAYS_INLINE bool needCompression(
    const folly::compression::Codec& codec) {
  return codec.type() != folly::compression::CodecType::NO_COMPRESSION;
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

inline char getCodecMarker() {
  char marker = 0;
  marker |= kCheckSumBitMask;
  return marker;
}

inline void flushSerialization(
    int32_t numRows,
    int32_t uncompressedSize,
    int32_t serializationSize,
    char codecMask,
    const std::unique_ptr<folly::IOBuf>& iobuf,
    OutputStream* output,
    PrestoOutputStreamListener* listener) {
  output->write(&codecMask, 1);
  writeInt32(output, uncompressedSize);
  writeInt32(output, serializationSize);
  auto crcOffset = output->tellp();
  // Write zero checksum
  writeInt64(output, 0);
  // Number of columns and stream content. Unpause CRC.
  if (listener) {
    listener->resume();
  }
  for (auto range : *iobuf) {
    output->write(reinterpret_cast<const char*>(range.data()), range.size());
  }
  // Pause CRC computation
  if (listener) {
    listener->pause();
  }
  const int32_t endSize = output->tellp();
  // Fill in crc
  int64_t crc = 0;
  if (listener) {
    crc = computeChecksum(listener, codecMask, numRows, uncompressedSize);
  }
  output->seekp(crcOffset);
  writeInt64(output, crc);
  output->seekp(endSize);
}

template <typename Allocator>
inline int64_t flushUncompressed(
    std::vector<VectorStream, Allocator>& streams,
    int32_t numRows,
    OutputStream* out,
    PrestoOutputStreamListener* listener) {
  int32_t offset = out->tellp();

  char codecMask = 0;
  if (listener) {
    codecMask = getCodecMarker();
  }
  // Pause CRC computation
  if (listener) {
    listener->pause();
  }

  writeInt32(out, numRows);
  out->write(&codecMask, 1);

  // Make space for uncompressedSizeInBytes & sizeInBytes
  writeInt32(out, 0);
  writeInt32(out, 0);
  // Write zero checksum.
  writeInt64(out, 0);

  // Number of columns and stream content. Unpause CRC.
  if (listener) {
    listener->resume();
  }
  writeInt32(out, streams.size());

  for (auto& stream : streams) {
    stream.flush(out);
  }

  // Pause CRC computation
  if (listener) {
    listener->pause();
  }

  // Fill in uncompressedSizeInBytes & sizeInBytes
  int32_t size = (int32_t)out->tellp() - offset;
  const int32_t uncompressedSize = size - kHeaderSize;
  int64_t crc = 0;
  if (listener) {
    crc = computeChecksum(listener, codecMask, numRows, uncompressedSize);
  }

  out->seekp(offset + kSizeInBytesOffset);
  writeInt32(out, uncompressedSize);
  writeInt32(out, uncompressedSize);
  writeInt64(out, crc);
  out->seekp(offset + size);
  return uncompressedSize;
}

template <typename Allocator>
inline FlushSizes flushCompressed(
    std::vector<VectorStream, Allocator>& streams,
    const StreamArena& arena,
    folly::compression::Codec& codec,
    int32_t numRows,
    float minCompressionRatio,
    OutputStream* output,
    PrestoOutputStreamListener* listener) {
  char codecMask = kCompressedBitMask;
  if (listener) {
    codecMask |= kCheckSumBitMask;
  }

  // Pause CRC computation
  if (listener) {
    listener->pause();
  }

  writeInt32(output, numRows);

  IOBufOutputStream out(*(arena.pool()), nullptr, arena.size());
  writeInt32(&out, streams.size());

  for (auto& stream : streams) {
    stream.flush(&out);
  }

  const int32_t uncompressedSize = out.tellp();
  VELOX_CHECK_LE(
      uncompressedSize,
      codec.maxUncompressedLength(),
      "UncompressedSize exceeds limit");
  auto iobuf = out.getIOBuf();
  const auto compressedBuffer = codec.compress(iobuf.get());
  const int32_t compressedSize = compressedBuffer->computeChainDataLength();
  if (compressedSize > uncompressedSize * minCompressionRatio) {
    flushSerialization(
        numRows,
        uncompressedSize,
        uncompressedSize,
        codecMask & ~kCompressedBitMask,
        iobuf,
        output,
        listener);
    return {uncompressedSize, uncompressedSize};
  }
  flushSerialization(
      numRows,
      uncompressedSize,
      compressedSize,
      codecMask,
      compressedBuffer,
      output,
      listener);
  return {uncompressedSize, compressedSize};
}

template <typename Allocator>
inline FlushSizes flushStreams(
    std::vector<VectorStream, Allocator>& streams,
    int32_t numRows,
    const StreamArena& arena,
    folly::compression::Codec& codec,
    float minCompressionRatio,
    OutputStream* out) {
  auto listener = dynamic_cast<PrestoOutputStreamListener*>(out->listener());
  // Reset CRC computation
  if (listener) {
    listener->reset();
  }

  if (!needCompression(codec)) {
    const auto size = flushUncompressed(streams, numRows, out, listener);
    return {size, size};
  } else {
    return flushCompressed(
        streams, arena, codec, numRows, minCompressionRatio, out, listener);
  }
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

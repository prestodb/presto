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
#include "velox/serializers/PrestoSerializer.h"

#include <optional>

#include <folly/lang/Bits.h>

#include "velox/common/base/Crc.h"
#include "velox/common/base/RawVector.h"
#include "velox/common/memory/ByteStream.h"
#include "velox/vector/BiasVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DictionaryVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/VectorTypeUtils.h"

namespace facebook::velox::serializer::presto {

using SerdeOpts = PrestoVectorSerde::PrestoOptions;

namespace {
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

int64_t computeChecksum(
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

int64_t computeChecksum(
    ByteInputStream* source,
    int codecMarker,
    int numRows,
    int32_t uncompressedSize,
    int32_t compressedSize) {
  const auto offset = source->tellp();
  const bool compressed = codecMarker & kCompressedBitMask;
  if (compressed) {
    VELOX_CHECK_LT(compressedSize, uncompressedSize);
  }
  const int32_t dataSize = compressed ? compressedSize : uncompressedSize;
  bits::Crc32 crc32;
  if (FOLLY_UNLIKELY(source->remainingSize() < dataSize)) {
    VELOX_FAIL(
        "Tried to read {} bytes, larger than what's remained in source {} "
        "bytes. Source details: {}",
        dataSize,
        source->remainingSize(),
        source->toString());
  }
  auto remainingBytes = dataSize;
  while (remainingBytes > 0) {
    auto data = source->nextView(remainingBytes);
    if (FOLLY_UNLIKELY(data.size() == 0)) {
      VELOX_FAIL(
          "Reading 0 bytes from source. Source details: {}",
          source->toString());
    }
    crc32.process_bytes(data.data(), data.size());
    remainingBytes -= data.size();
  }

  crc32.process_bytes(&codecMarker, 1);
  crc32.process_bytes(&numRows, 4);
  crc32.process_bytes(&uncompressedSize, 4);
  auto checksum = crc32.checksum();

  source->seekp(offset);

  return checksum;
}

char getCodecMarker() {
  char marker = 0;
  marker |= kCheckSumBitMask;
  return marker;
}

bool isCompressedBitSet(int8_t codec) {
  return (codec & kCompressedBitMask) == kCompressedBitMask;
}

bool isEncryptedBitSet(int8_t codec) {
  return (codec & kEncryptedBitMask) == kEncryptedBitMask;
}

bool isChecksumBitSet(int8_t codec) {
  return (codec & kCheckSumBitMask) == kCheckSumBitMask;
}

std::string_view typeToEncodingName(const TypePtr& type) {
  switch (type->kind()) {
    case TypeKind::BOOLEAN:
      return kByteArray;
    case TypeKind::TINYINT:
      return kByteArray;
    case TypeKind::SMALLINT:
      return kShortArray;
    case TypeKind::INTEGER:
      return kIntArray;
    case TypeKind::BIGINT:
      return kLongArray;
    case TypeKind::HUGEINT:
      return kInt128Array;
    case TypeKind::REAL:
      return kIntArray;
    case TypeKind::DOUBLE:
      return kLongArray;
    case TypeKind::VARCHAR:
      return kVariableWidth;
    case TypeKind::VARBINARY:
      return kVariableWidth;
    case TypeKind::TIMESTAMP:
      return kLongArray;
    case TypeKind::ARRAY:
      return kArray;
    case TypeKind::MAP:
      return kMap;
    case TypeKind::ROW:
      return kRow;
    case TypeKind::UNKNOWN:
      return kByteArray;
    default:
      VELOX_FAIL("Unknown type kind: {}", static_cast<int>(type->kind()));
  }
}

PrestoVectorSerde::PrestoOptions toPrestoOptions(
    const VectorSerde::Options* options) {
  if (options == nullptr) {
    return PrestoVectorSerde::PrestoOptions();
  }
  auto prestoOptions =
      dynamic_cast<const PrestoVectorSerde::PrestoOptions*>(options);
  VELOX_CHECK_NOT_NULL(
      prestoOptions, "Serde options are not Presto-compatible");
  return *prestoOptions;
}

FOLLY_ALWAYS_INLINE bool needCompression(const folly::io::Codec& codec) {
  return codec.type() != folly::io::CodecType::NO_COMPRESSION;
}

using StructNullsMap =
    folly::F14FastMap<int64_t, std::pair<raw_vector<uint64_t>, int32_t>>;

auto& structNullsMap() {
  thread_local std::unique_ptr<StructNullsMap> map;
  return map;
}

std::pair<const uint64_t*, int32_t> getStructNulls(int64_t position) {
  auto& map = structNullsMap();
  auto it = map->find(position);
  if (it == map->end()) {
    return {nullptr, 0};
  }
  return {it->second.first.data(), it->second.second};
}

template <typename T>
T readInt(std::string_view* source) {
  assert(source->size() >= sizeof(T));
  auto value = folly::loadUnaligned<T>(source->data());
  source->remove_prefix(sizeof(T));
  return value;
}

struct PrestoHeader {
  int32_t numRows;
  int8_t pageCodecMarker;
  int32_t uncompressedSize;
  int32_t compressedSize;
  int64_t checksum;

  static PrestoHeader read(ByteInputStream* source) {
    PrestoHeader header;
    header.numRows = source->read<int32_t>();
    header.pageCodecMarker = source->read<int8_t>();
    header.uncompressedSize = source->read<int32_t>();
    header.compressedSize = source->read<int32_t>();
    header.checksum = source->read<int64_t>();

    VELOX_CHECK_GE(header.numRows, 0);
    VELOX_CHECK_GE(header.uncompressedSize, 0);
    VELOX_CHECK_GE(header.compressedSize, 0);

    return header;
  }

  static std::optional<PrestoHeader> read(std::string_view* source) {
    if (source->size() < kHeaderSize) {
      return std::nullopt;
    }

    PrestoHeader header;
    header.numRows = readInt<int32_t>(source);
    header.pageCodecMarker = readInt<int8_t>(source);
    header.uncompressedSize = readInt<int32_t>(source);
    header.compressedSize = readInt<int32_t>(source);
    header.checksum = readInt<int64_t>(source);

    if (header.numRows < 0) {
      return std::nullopt;
    }
    if (header.uncompressedSize < 0) {
      return std::nullopt;
    }
    if (header.compressedSize < 0) {
      return std::nullopt;
    }

    return header;
  }
};

template <typename T>
int32_t checkValuesSize(
    const BufferPtr& values,
    const BufferPtr& nulls,
    int32_t size,
    int32_t offset) {
  auto bufferSize = (std::is_same_v<T, bool>) ? values->size() * 8
                                              : values->size() / sizeof(T);
  // If all nulls, values does not have to be sized for vector size.
  if (nulls && bits::isAllSet(nulls->as<uint64_t>(), 0, size + offset, false)) {
    return 0;
  }
  VELOX_CHECK_LE(offset + size, bufferSize);
  return bufferSize;
}

template <typename T>
void readValues(
    ByteInputStream* source,
    vector_size_t size,
    vector_size_t offset,
    const BufferPtr& nulls,
    vector_size_t nullCount,
    const BufferPtr& values) {
  if (nullCount) {
    auto bufferSize = checkValuesSize<T>(values, nulls, size, offset);
    auto rawValues = values->asMutable<T>();
    int32_t toClear = offset;
    bits::forEachSetBit(
        nulls->as<uint64_t>(), offset, offset + size, [&](int32_t row) {
          // Set the values between the last non-null and this to type default.
          for (; toClear < row; ++toClear) {
            VELOX_CHECK_LT(toClear, bufferSize);
            rawValues[toClear] = T();
          }
          VELOX_CHECK_LT(row, bufferSize);
          rawValues[row] = source->read<T>();
          toClear = row + 1;
        });
  } else {
    source->readBytes(
        values->asMutable<uint8_t>() + offset * sizeof(T), size * sizeof(T));
  }
}

template <>
void readValues<bool>(
    ByteInputStream* source,
    vector_size_t size,
    vector_size_t offset,
    const BufferPtr& nulls,
    vector_size_t nullCount,
    const BufferPtr& values) {
  auto rawValues = values->asMutable<uint64_t>();
  auto bufferSize = checkValuesSize<bool>(values, nulls, size, offset);
  if (nullCount) {
    int32_t toClear = offset;
    bits::forEachSetBit(
        nulls->as<uint64_t>(), offset, offset + size, [&](int32_t row) {
          // Set the values between the last non-null and this to type default.
          for (; toClear < row; ++toClear) {
            VELOX_CHECK_LT(toClear, bufferSize);
            bits::clearBit(rawValues, toClear);
          }
          VELOX_CHECK_LT(row, bufferSize);
          bits::setBit(rawValues, row, (source->read<int8_t>() != 0));
          toClear = row + 1;
        });
  } else {
    for (int32_t row = offset; row < offset + size; ++row) {
      bits::setBit(rawValues, row, (source->read<int8_t>() != 0));
    }
  }
}

Timestamp readTimestamp(ByteInputStream* source) {
  int64_t millis = source->read<int64_t>();
  return Timestamp::fromMillis(millis);
}

template <>
void readValues<Timestamp>(
    ByteInputStream* source,
    vector_size_t size,
    vector_size_t offset,
    const BufferPtr& nulls,
    vector_size_t nullCount,
    const BufferPtr& values) {
  auto rawValues = values->asMutable<Timestamp>();
  checkValuesSize<Timestamp>(values, nulls, size, offset);
  if (nullCount) {
    int32_t toClear = offset;
    bits::forEachSetBit(
        nulls->as<uint64_t>(), offset, offset + size, [&](int32_t row) {
          // Set the values between the last non-null and this to type default.
          for (; toClear < row; ++toClear) {
            rawValues[toClear] = Timestamp();
          }
          rawValues[row] = readTimestamp(source);
          toClear = row + 1;
        });
  } else {
    for (int32_t row = offset; row < offset + size; ++row) {
      rawValues[row] = readTimestamp(source);
    }
  }
}

Timestamp readLosslessTimestamp(ByteInputStream* source) {
  int64_t seconds = source->read<int64_t>();
  uint64_t nanos = source->read<uint64_t>();
  return Timestamp(seconds, nanos);
}

void readLosslessTimestampValues(
    ByteInputStream* source,
    vector_size_t size,
    vector_size_t offset,
    const BufferPtr& nulls,
    vector_size_t nullCount,
    const BufferPtr& values) {
  auto rawValues = values->asMutable<Timestamp>();
  checkValuesSize<Timestamp>(values, nulls, size, offset);
  if (nullCount > 0) {
    int32_t toClear = offset;
    bits::forEachSetBit(
        nulls->as<uint64_t>(), offset, offset + size, [&](int32_t row) {
          // Set the values between the last non-null and this to type default.
          for (; toClear < row; ++toClear) {
            rawValues[toClear] = Timestamp();
          }
          rawValues[row] = readLosslessTimestamp(source);
          toClear = row + 1;
        });
  } else {
    for (int32_t row = offset; row < offset + size; ++row) {
      rawValues[row] = readLosslessTimestamp(source);
    }
  }
}

int128_t readJavaDecimal(ByteInputStream* source) {
  // ByteInputStream does not support reading int128_t values.
  auto low = source->read<int64_t>();
  auto high = source->read<int64_t>();
  // 'high' is in signed magnitude representation.
  if (high < 0) {
    // Remove the sign bit before building the int128 value.
    // Negate the value.
    return -1 * HugeInt::build(high & DecimalUtil::kInt64Mask, low);
  }
  return HugeInt::build(high, low);
}

void readDecimalValues(
    ByteInputStream* source,
    vector_size_t size,
    vector_size_t offset,
    const BufferPtr& nulls,
    vector_size_t nullCount,
    const BufferPtr& values) {
  auto rawValues = values->asMutable<int128_t>();
  if (nullCount) {
    checkValuesSize<int128_t>(values, nulls, size, offset);

    int32_t toClear = offset;
    bits::forEachSetBit(
        nulls->as<uint64_t>(), offset, offset + size, [&](int32_t row) {
          // Set the values between the last non-null and this to type default.
          for (; toClear < row; ++toClear) {
            rawValues[toClear] = 0;
          }
          rawValues[row] = readJavaDecimal(source);
          toClear = row + 1;
        });
  } else {
    for (int32_t row = 0; row < size; ++row) {
      rawValues[offset + row] = readJavaDecimal(source);
    }
  }
}

/// When deserializing vectors under row vectors that introduce
/// nulls, the child vector must have a gap at the place where a
/// parent RowVector has a null. So, if there is a parent RowVector
/// that adds a null, 'incomingNulls' is the bitmap where a null
/// denotes a null in the parent RowVector(s). 'numIncomingNulls' is
/// the number of bits in this bitmap, i.e. the number of rows in
/// the parentRowVector. 'size' is the size of the child vector
/// being deserialized. This size does not include rows where a
/// parent RowVector has nulls.
vector_size_t sizeWithIncomingNulls(
    vector_size_t size,
    int32_t numIncomingNulls) {
  return numIncomingNulls == 0 ? size : numIncomingNulls;
}

// Fills the nulls of 'result' from the serialized nulls in
// 'source'. Adds nulls from 'incomingNulls' so that the null flags
// gets padded with extra nulls where a parent RowVector has a
// null. Returns the number of nulls in the result.
vector_size_t readNulls(
    ByteInputStream* source,
    vector_size_t size,
    vector_size_t resultOffset,
    const uint64_t* incomingNulls,
    int32_t numIncomingNulls,
    BaseVector& result) {
  VELOX_DCHECK_LE(
      result.size(), resultOffset + (incomingNulls ? numIncomingNulls : size));
  if (source->readByte() == 0) {
    if (incomingNulls) {
      auto* rawNulls = result.mutableRawNulls();
      bits::copyBits(
          incomingNulls, 0, rawNulls, resultOffset, numIncomingNulls);
    } else {
      result.clearNulls(resultOffset, resultOffset + size);
    }
    return incomingNulls
        ? numIncomingNulls - bits::countBits(incomingNulls, 0, numIncomingNulls)
        : 0;
  }

  const auto numNewValues = sizeWithIncomingNulls(size, numIncomingNulls);

  const bool noPriorNulls = (result.rawNulls() == nullptr);
  // Allocate one extra byte in case we cannot use bits from the current last
  // partial byte.
  BufferPtr& nulls = result.mutableNulls(resultOffset + numNewValues + 8);
  if (noPriorNulls) {
    bits::fillBits(
        nulls->asMutable<uint64_t>(), 0, resultOffset, bits::kNotNull);
  }

  auto* rawNulls = nulls->asMutable<uint8_t>() + bits::nbytes(resultOffset);
  const auto numBytes = BaseVector::byteSize<bool>(size);

  source->readBytes(rawNulls, numBytes);
  bits::reverseBits(rawNulls, numBytes);
  bits::negate(reinterpret_cast<char*>(rawNulls), numBytes * 8);
  // Add incoming nulls if any.
  if (incomingNulls) {
    bits::scatterBits(
        size,
        numIncomingNulls,
        reinterpret_cast<const char*>(rawNulls),
        incomingNulls,
        reinterpret_cast<char*>(rawNulls));
  }

  // Shift bits if needed.
  if (bits::nbytes(resultOffset) * 8 > resultOffset) {
    bits::copyBits(
        nulls->asMutable<uint64_t>(),
        bits::nbytes(resultOffset) * 8,
        nulls->asMutable<uint64_t>(),
        resultOffset,
        numNewValues);
  }

  return BaseVector::countNulls(
      nulls, resultOffset, resultOffset + numNewValues);
}

template <typename T>
void read(
    ByteInputStream* source,
    const TypePtr& type,
    vector_size_t resultOffset,
    const uint64_t* incomingNulls,
    int32_t numIncomingNulls,
    velox::memory::MemoryPool* pool,
    const SerdeOpts& opts,
    VectorPtr& result) {
  const int32_t size = source->read<int32_t>();
  const auto numNewValues = sizeWithIncomingNulls(size, numIncomingNulls);
  result->resize(resultOffset + numNewValues);

  auto flatResult = result->asFlatVector<T>();
  auto nullCount = readNulls(
      source, size, resultOffset, incomingNulls, numIncomingNulls, *flatResult);

  BufferPtr values = flatResult->mutableValues(resultOffset + numNewValues);
  if constexpr (std::is_same_v<T, Timestamp>) {
    if (opts.useLosslessTimestamp) {
      readLosslessTimestampValues(
          source,
          numNewValues,
          resultOffset,
          flatResult->nulls(),
          nullCount,
          values);
      return;
    }
  }
  if (type->isLongDecimal()) {
    readDecimalValues(
        source,
        numNewValues,
        resultOffset,
        flatResult->nulls(),
        nullCount,
        values);
    return;
  }
  readValues<T>(
      source,
      numNewValues,
      resultOffset,
      flatResult->nulls(),
      nullCount,
      values);
}

template <>
void read<StringView>(
    ByteInputStream* source,
    const TypePtr& type,
    vector_size_t resultOffset,
    const uint64_t* incomingNulls,
    int32_t numIncomingNulls,
    velox::memory::MemoryPool* pool,
    const SerdeOpts& opts,
    VectorPtr& result) {
  const int32_t size = source->read<int32_t>();
  const int32_t numNewValues = sizeWithIncomingNulls(size, numIncomingNulls);

  result->resize(resultOffset + numNewValues);

  auto flatResult = result->as<FlatVector<StringView>>();
  BufferPtr values = flatResult->mutableValues(resultOffset + size);
  auto rawValues = values->asMutable<StringView>();
  int32_t lastOffset = 0;
  for (int32_t i = 0; i < numNewValues; ++i) {
    // Set the first int32_t of each StringView to be the offset.
    if (incomingNulls && bits::isBitNull(incomingNulls, i)) {
      *reinterpret_cast<int32_t*>(&rawValues[resultOffset + i]) = lastOffset;
      continue;
    }
    lastOffset = source->read<int32_t>();
    *reinterpret_cast<int32_t*>(&rawValues[resultOffset + i]) = lastOffset;
  }
  readNulls(
      source, size, resultOffset, incomingNulls, numIncomingNulls, *flatResult);

  const int32_t dataSize = source->read<int32_t>();
  if (dataSize == 0) {
    return;
  }

  auto* rawStrings =
      flatResult->getRawStringBufferWithSpace(dataSize, true /*exactSize*/);

  source->readBytes(rawStrings, dataSize);
  int32_t previousOffset = 0;
  auto rawChars = reinterpret_cast<char*>(rawStrings);
  for (int32_t i = 0; i < numNewValues; ++i) {
    int32_t offset = rawValues[resultOffset + i].size();
    rawValues[resultOffset + i] =
        StringView(rawChars + previousOffset, offset - previousOffset);
    previousOffset = offset;
  }
}

void readColumns(
    ByteInputStream* source,
    const std::vector<TypePtr>& types,
    vector_size_t resultOffset,
    const uint64_t* incomingNulls,
    int32_t numIncomingNulls,
    velox::memory::MemoryPool* pool,
    const SerdeOpts& opts,
    std::vector<VectorPtr>& result);

void readConstantVector(
    ByteInputStream* source,
    const TypePtr& type,
    vector_size_t resultOffset,
    const uint64_t* incomingNulls,
    int32_t numIncomingNulls,
    velox::memory::MemoryPool* pool,
    const SerdeOpts& opts,
    VectorPtr& result) {
  const auto size = source->read<int32_t>();
  const int32_t numNewValues = sizeWithIncomingNulls(size, numIncomingNulls);
  std::vector<TypePtr> childTypes = {type};
  std::vector<VectorPtr> children{BaseVector::create(type, 0, pool)};
  readColumns(source, childTypes, 0, nullptr, 0, pool, opts, children);
  VELOX_CHECK_EQ(1, children[0]->size());

  auto constantVector =
      BaseVector::wrapInConstant(numNewValues, 0, children[0]);

  // If there are no previous results, we output this as a constant. RowVectors
  // with top-level nulls can have child ConstantVector (even though they can't
  // have nulls explicitly set on them), so we don't need to try to apply
  // incomingNulls here.
  if (resultOffset == 0) {
    result = std::move(constantVector);
  } else {
    if (!incomingNulls &&
        opts.nullsFirst && // TODO remove when removing scatter nulls pass.
        result->encoding() == VectorEncoding::Simple::CONSTANT &&
        constantVector->equalValueAt(result.get(), 0, 0)) {
      result->resize(resultOffset + numNewValues);
      return;
    }
    result->resize(resultOffset + numNewValues);

    SelectivityVector rows(resultOffset + numNewValues, false);
    rows.setValidRange(resultOffset, resultOffset + numNewValues, true);
    rows.updateBounds();

    BaseVector::ensureWritable(rows, type, pool, result);
    result->copy(constantVector.get(), resultOffset, 0, numNewValues);
    if (incomingNulls) {
      bits::forEachUnsetBit(incomingNulls, 0, numNewValues, [&](auto row) {
        result->setNull(resultOffset + row, true);
      });
    }
  }
}

void readDictionaryVector(
    ByteInputStream* source,
    const TypePtr& type,
    vector_size_t resultOffset,
    const uint64_t* incomingNulls,
    int32_t numIncomingNulls,
    velox::memory::MemoryPool* pool,
    const SerdeOpts& opts,
    VectorPtr& result) {
  const auto size = source->read<int32_t>();
  const int32_t numNewValues = sizeWithIncomingNulls(size, numIncomingNulls);

  std::vector<TypePtr> childTypes = {type};
  std::vector<VectorPtr> children{BaseVector::create(type, 0, pool)};
  readColumns(source, childTypes, 0, nullptr, 0, pool, opts, children);

  // Read indices.
  BufferPtr indices = allocateIndices(numNewValues, pool);
  if (incomingNulls) {
    auto rawIndices = indices->asMutable<int32_t>();
    for (auto i = 0; i < numNewValues; ++i) {
      if (bits::isBitNull(incomingNulls, i)) {
        rawIndices[i] = 0;
      } else {
        rawIndices[i] = source->read<int32_t>();
      }
    }
  } else {
    source->readBytes(
        indices->asMutable<char>(), numNewValues * sizeof(int32_t));
  }

  // Skip 3 * 8 bytes of 'instance id'. Velox doesn't use 'instance id' for
  // dictionary vectors.
  source->skip(24);

  BufferPtr incomingNullsBuffer = nullptr;
  if (incomingNulls) {
    incomingNullsBuffer = AlignedBuffer::allocate<bool>(numIncomingNulls, pool);
    memcpy(
        incomingNullsBuffer->asMutable<char>(),
        incomingNulls,
        bits::nbytes(numIncomingNulls));
  }
  auto dictionaryVector = BaseVector::wrapInDictionary(
      incomingNullsBuffer, indices, numNewValues, children[0]);
  if (resultOffset == 0) {
    result = std::move(dictionaryVector);
  } else {
    result->resize(resultOffset + numNewValues);

    SelectivityVector rows(resultOffset + numNewValues, false);
    rows.setValidRange(resultOffset, resultOffset + numNewValues, true);
    rows.updateBounds();

    BaseVector::ensureWritable(rows, type, pool, result);
    result->copy(dictionaryVector.get(), resultOffset, 0, numNewValues);
  }
}

void readArrayVector(
    ByteInputStream* source,
    const TypePtr& type,
    vector_size_t resultOffset,
    const uint64_t* incomingNulls,
    int32_t numIncomingNulls,
    velox::memory::MemoryPool* pool,
    const SerdeOpts& opts,
    VectorPtr& result) {
  ArrayVector* arrayVector = result->as<ArrayVector>();

  const auto resultElementsOffset = arrayVector->elements()->size();

  std::vector<TypePtr> childTypes = {type->childAt(0)};
  std::vector<VectorPtr> children{arrayVector->elements()};
  readColumns(
      source,
      childTypes,
      resultElementsOffset,
      nullptr,
      0,
      pool,
      opts,
      children);

  const vector_size_t size = source->read<int32_t>();
  const auto numNewValues = sizeWithIncomingNulls(size, numIncomingNulls);
  arrayVector->resize(resultOffset + numNewValues);
  arrayVector->setElements(children[0]);

  BufferPtr offsets = arrayVector->mutableOffsets(resultOffset + numNewValues);
  auto rawOffsets = offsets->asMutable<vector_size_t>();
  BufferPtr sizes = arrayVector->mutableSizes(resultOffset + numNewValues);
  auto rawSizes = sizes->asMutable<vector_size_t>();
  int32_t base = source->read<int32_t>();
  for (int32_t i = 0; i < numNewValues; ++i) {
    if (incomingNulls && bits::isBitNull(incomingNulls, i)) {
      rawOffsets[resultOffset + i] = 0;
      rawSizes[resultOffset + i] = 0;
      continue;
    }
    int32_t offset = source->read<int32_t>();
    rawOffsets[resultOffset + i] = resultElementsOffset + base;
    rawSizes[resultOffset + i] = offset - base;
    base = offset;
  }

  readNulls(
      source,
      size,
      resultOffset,
      incomingNulls,
      numIncomingNulls,
      *arrayVector);
}

void readMapVector(
    ByteInputStream* source,
    const TypePtr& type,
    vector_size_t resultOffset,
    const uint64_t* incomingNulls,
    int32_t numIncomingNulls,
    velox::memory::MemoryPool* pool,
    const SerdeOpts& opts,
    VectorPtr& result) {
  MapVector* mapVector = result->as<MapVector>();
  const auto resultElementsOffset = mapVector->mapKeys()->size();
  std::vector<TypePtr> childTypes = {type->childAt(0), type->childAt(1)};
  std::vector<VectorPtr> children{mapVector->mapKeys(), mapVector->mapValues()};
  readColumns(
      source,
      childTypes,
      resultElementsOffset,
      nullptr,
      0,
      pool,
      opts,
      children);

  int32_t hashTableSize = source->read<int32_t>();
  if (hashTableSize != -1) {
    // Skip over serialized hash table from Presto wire format.
    source->skip(hashTableSize * sizeof(int32_t));
  }

  const vector_size_t size = source->read<int32_t>();
  const vector_size_t numNewValues =
      sizeWithIncomingNulls(size, numIncomingNulls);
  mapVector->resize(resultOffset + numNewValues);
  mapVector->setKeysAndValues(children[0], children[1]);

  BufferPtr offsets = mapVector->mutableOffsets(resultOffset + numNewValues);
  auto rawOffsets = offsets->asMutable<vector_size_t>();
  BufferPtr sizes = mapVector->mutableSizes(resultOffset + numNewValues);
  auto rawSizes = sizes->asMutable<vector_size_t>();
  int32_t base = source->read<int32_t>();
  for (int32_t i = 0; i < numNewValues; ++i) {
    if (incomingNulls && bits::isBitNull(incomingNulls, i)) {
      rawOffsets[resultOffset + i] = 0;
      rawSizes[resultOffset + i] = 0;
      continue;
    }
    int32_t offset = source->read<int32_t>();
    rawOffsets[resultOffset + i] = resultElementsOffset + base;
    rawSizes[resultOffset + i] = offset - base;
    base = offset;
  }

  readNulls(
      source, size, resultOffset, incomingNulls, numIncomingNulls, *mapVector);
}

void readRowVector(
    ByteInputStream* source,
    const TypePtr& type,
    vector_size_t resultOffset,
    const uint64_t* incomingNulls,
    int32_t numIncomingNulls,
    velox::memory::MemoryPool* pool,
    const SerdeOpts& opts,
    VectorPtr& result) {
  auto* row = result->asUnchecked<RowVector>();
  BufferPtr combinedNulls;
  const uint64_t* childNulls = incomingNulls;
  int32_t numChildNulls = numIncomingNulls;
  if (opts.nullsFirst) {
    const auto size = source->read<int32_t>();
    const auto numNewValues = sizeWithIncomingNulls(size, numIncomingNulls);
    row->resize(resultOffset + numNewValues);
    readNulls(
        source, size, resultOffset, incomingNulls, numIncomingNulls, *result);
    if (row->rawNulls()) {
      combinedNulls = AlignedBuffer::allocate<bool>(numNewValues, pool);
      bits::copyBits(
          row->rawNulls(),
          resultOffset,
          combinedNulls->asMutable<uint64_t>(),
          0,
          numNewValues);
      childNulls = combinedNulls->as<uint64_t>();
      numChildNulls = numNewValues;
    }
  } else {
    auto [structNulls, numStructNulls] = getStructNulls(source->tellp());
    // childNulls is the nulls added to the children, i.e. the nulls of this
    // struct combined with nulls of enclosing structs.
    if (structNulls) {
      if (incomingNulls) {
        combinedNulls = AlignedBuffer::allocate<bool>(numIncomingNulls, pool);
        bits::scatterBits(
            numStructNulls,
            numIncomingNulls,
            reinterpret_cast<const char*>(structNulls),
            incomingNulls,
            combinedNulls->asMutable<char>());
        childNulls = combinedNulls->as<uint64_t>();
        numChildNulls = numIncomingNulls;
      } else {
        childNulls = structNulls;
        numChildNulls = numStructNulls;
      }
    }
  }

  source->read<int32_t>(); // numChildren
  auto& children = row->children();

  const auto& childTypes = type->asRow().children();
  readColumns(
      source,
      childTypes,
      resultOffset,
      childNulls,
      numChildNulls,
      pool,
      opts,
      children);
  if (!opts.nullsFirst) {
    const auto size = source->read<int32_t>();
    const auto numNewValues = sizeWithIncomingNulls(size, numIncomingNulls);
    row->resize(resultOffset + numNewValues);
    // Read and discard the offsets. The number of offsets is not affected by
    // incomingNulls.
    source->skip((size + 1) * sizeof(int32_t));
    readNulls(
        source, size, resultOffset, incomingNulls, numIncomingNulls, *result);
  }
}

std::string readLengthPrefixedString(ByteInputStream* source) {
  int32_t size = source->read<int32_t>();
  std::string value;
  value.resize(size);
  source->readBytes(&value[0], size);
  return value;
}

void checkTypeEncoding(std::string_view encoding, const TypePtr& type) {
  const auto kindEncoding = typeToEncodingName(type);
  VELOX_USER_CHECK(
      encoding == kindEncoding,
      "Serialized encoding is not compatible with requested type: {}. Expected {}. Got {}.",
      type->kindName(),
      kindEncoding,
      encoding);
}

// This is used when there's a mismatch between the encoding in the serialized
// page and the expected output encoding. If the serialized encoding is
// BYTE_ARRAY, it may represent an all-null vector of the expected output type.
// We attempt to read the serialized page as an UNKNOWN type, check if all
// values are null, and set the columnResult accordingly. If all values are
// null, we return true; otherwise, we return false.
bool tryReadNullColumn(
    ByteInputStream* source,
    const TypePtr& columnType,
    vector_size_t resultOffset,
    const uint64_t* incomingNulls,
    int32_t numIncomingNulls,
    velox::memory::MemoryPool* pool,
    const SerdeOpts& opts,
    VectorPtr& columnResult) {
  auto unknownType = UNKNOWN();
  VectorPtr tempResult = BaseVector::create(unknownType, 0, pool);
  read<UnknownValue>(
      source,
      unknownType,
      0 /*resultOffset*/,
      incomingNulls,
      numIncomingNulls,
      pool,
      opts,
      tempResult);
  auto deserializedSize = tempResult->size();
  // Ensure it contains all null values.
  auto numNulls = BaseVector::countNulls(tempResult->nulls(), deserializedSize);
  if (deserializedSize != numNulls) {
    return false;
  }
  if (resultOffset == 0) {
    columnResult =
        BaseVector::createNullConstant(columnType, deserializedSize, pool);
  } else {
    columnResult->resize(resultOffset + deserializedSize);

    SelectivityVector nullRows(resultOffset + deserializedSize, false);
    nullRows.setValidRange(resultOffset, resultOffset + deserializedSize, true);
    nullRows.updateBounds();

    BaseVector::ensureWritable(nullRows, columnType, pool, columnResult);
    columnResult->addNulls(nullRows);
  }
  return true;
}

void readColumns(
    ByteInputStream* source,
    const std::vector<TypePtr>& types,
    vector_size_t resultOffset,
    const uint64_t* incomingNulls,
    int32_t numIncomingNulls,
    velox::memory::MemoryPool* pool,
    const SerdeOpts& opts,
    std::vector<VectorPtr>& results) {
  static const std::unordered_map<
      TypeKind,
      std::function<void(
          ByteInputStream * source,
          const TypePtr& type,
          vector_size_t resultOffset,
          const uint64_t* incomingNulls,
          int32_t numIncomingNulls,
          velox::memory::MemoryPool* pool,
          const SerdeOpts& opts,
          VectorPtr& result)>>
      readers = {
          {TypeKind::BOOLEAN, &read<bool>},
          {TypeKind::TINYINT, &read<int8_t>},
          {TypeKind::SMALLINT, &read<int16_t>},
          {TypeKind::INTEGER, &read<int32_t>},
          {TypeKind::BIGINT, &read<int64_t>},
          {TypeKind::HUGEINT, &read<int128_t>},
          {TypeKind::REAL, &read<float>},
          {TypeKind::DOUBLE, &read<double>},
          {TypeKind::TIMESTAMP, &read<Timestamp>},
          {TypeKind::VARCHAR, &read<StringView>},
          {TypeKind::VARBINARY, &read<StringView>},
          {TypeKind::ARRAY, &readArrayVector},
          {TypeKind::MAP, &readMapVector},
          {TypeKind::ROW, &readRowVector},
          {TypeKind::UNKNOWN, &read<UnknownValue>}};

  VELOX_CHECK_EQ(types.size(), results.size());

  for (int32_t i = 0; i < types.size(); ++i) {
    const auto& columnType = types[i];
    auto& columnResult = results[i];

    const auto encoding = readLengthPrefixedString(source);
    if (encoding == kRLE) {
      readConstantVector(
          source,
          columnType,
          resultOffset,
          incomingNulls,
          numIncomingNulls,
          pool,
          opts,
          columnResult);
    } else if (encoding == kDictionary) {
      readDictionaryVector(
          source,
          columnType,
          resultOffset,
          incomingNulls,
          numIncomingNulls,
          pool,
          opts,
          columnResult);
    } else {
      auto typeToEncoding = typeToEncodingName(columnType);
      if (encoding != typeToEncoding) {
        if (encoding == kByteArray &&
            tryReadNullColumn(
                source,
                columnType,
                resultOffset,
                incomingNulls,
                numIncomingNulls,
                pool,
                opts,
                columnResult)) {
          return;
        }
      }
      checkTypeEncoding(encoding, columnType);
      if (columnResult != nullptr &&
          (columnResult->encoding() == VectorEncoding::Simple::CONSTANT ||
           columnResult->encoding() == VectorEncoding::Simple::DICTIONARY)) {
        BaseVector::ensureWritable(
            SelectivityVector::empty(), types[i], pool, columnResult);
      }
      const auto it = readers.find(columnType->kind());
      VELOX_CHECK(
          it != readers.end(),
          "Column reader for type {} is missing",
          columnType->kindName());

      it->second(
          source,
          columnType,
          resultOffset,
          incomingNulls,
          numIncomingNulls,
          pool,
          opts,
          columnResult);
    }
  }
}

// Reads nulls into 'scratch' and returns count of non-nulls. If 'copy' is
// given, returns the null bits in 'copy'.
vector_size_t valueCount(
    ByteInputStream* source,
    vector_size_t size,
    Scratch& scratch,
    raw_vector<uint64_t>* copy = nullptr) {
  if (source->readByte() == 0) {
    return size;
  }
  ScratchPtr<uint64_t, 16> nullsHolder(scratch);
  auto rawNulls = nullsHolder.get(bits::nwords(size));
  auto numBytes = bits::nbytes(size);
  source->readBytes(rawNulls, numBytes);
  bits::reverseBits(reinterpret_cast<uint8_t*>(rawNulls), numBytes);
  bits::negate(reinterpret_cast<char*>(rawNulls), numBytes * 8);
  if (copy) {
    copy->resize(bits::nwords(size));
    memcpy(copy->data(), rawNulls, numBytes);
  }
  return bits::countBits(rawNulls, 0, size);
}

template <typename T>
void readStructNulls(
    ByteInputStream* source,
    const TypePtr& type,
    bool useLosslessTimestamp,
    Scratch& scratch) {
  const int32_t size = source->read<int32_t>();
  auto numValues = valueCount(source, size, scratch);

  if constexpr (std::is_same_v<T, Timestamp>) {
    source->skip(
        numValues *
        (useLosslessTimestamp ? sizeof(Timestamp) : sizeof(uint64_t)));
    return;
  }
  source->skip(numValues * sizeof(T));
}

template <>
void readStructNulls<StringView>(
    ByteInputStream* source,
    const TypePtr& type,
    bool /*useLosslessTimestamp*/,
    Scratch& scratch) {
  const int32_t size = source->read<int32_t>();
  source->skip(size * sizeof(int32_t));
  valueCount(source, size, scratch);
  const int32_t dataSize = source->read<int32_t>();
  source->skip(dataSize);
}

void readStructNullsColumns(
    ByteInputStream* source,
    const std::vector<TypePtr>& types,
    bool useLoasslessTimestamp,
    Scratch& scratch);

void readConstantVectorStructNulls(
    ByteInputStream* source,
    const TypePtr& type,
    bool useLosslessTimestamp,
    Scratch& scratch) {
  source->read<int32_t>(); // size
  std::vector<TypePtr> childTypes = {type};
  readStructNullsColumns(source, childTypes, useLosslessTimestamp, scratch);
}

void readDictionaryVectorStructNulls(
    ByteInputStream* source,
    const TypePtr& type,
    bool useLosslessTimestamp,
    Scratch& scratch) {
  const auto size = source->read<int32_t>();
  std::vector<TypePtr> childTypes = {type};
  readStructNullsColumns(source, childTypes, useLosslessTimestamp, scratch);

  // Skip indices.
  source->skip(sizeof(int32_t) * size);

  // Skip 3 * 8 bytes of 'instance id'. Velox doesn't use 'instance id' for
  // dictionary vectors.
  source->skip(24);
}

void readArrayVectorStructNulls(
    ByteInputStream* source,
    const TypePtr& type,
    bool useLosslessTimestamp,
    Scratch& scratch) {
  std::vector<TypePtr> childTypes = {type->childAt(0)};
  readStructNullsColumns(source, childTypes, useLosslessTimestamp, scratch);

  const vector_size_t size = source->read<int32_t>();

  source->skip((size + 1) * sizeof(int32_t));
  valueCount(source, size, scratch);
}

void readMapVectorStructNulls(
    ByteInputStream* source,
    const TypePtr& type,
    bool useLosslessTimestamp,
    Scratch& scratch) {
  std::vector<TypePtr> childTypes = {type->childAt(0), type->childAt(1)};
  readStructNullsColumns(source, childTypes, useLosslessTimestamp, scratch);

  int32_t hashTableSize = source->read<int32_t>();
  if (hashTableSize != -1) {
    // Skip over serialized hash table from Presto wire format.
    source->skip(hashTableSize * sizeof(int32_t));
  }

  const vector_size_t size = source->read<int32_t>();

  source->skip((1 + size) * sizeof(int32_t));
  valueCount(source, size, scratch);
}

void readRowVectorStructNulls(
    ByteInputStream* source,
    const TypePtr& type,
    bool useLosslessTimestamp,
    Scratch& scratch) {
  auto streamPos = source->tellp();
  source->read<int32_t>(); // numChildren
  const auto& childTypes = type->asRow().children();
  readStructNullsColumns(source, childTypes, useLosslessTimestamp, scratch);

  const auto size = source->read<int32_t>();
  // Read and discard the offsets. The number of offsets is not affected by
  // nulls.
  source->skip((size + 1) * sizeof(int32_t));
  raw_vector<uint64_t> nullsCopy;
  auto numNonNull = valueCount(source, size, scratch, &nullsCopy);
  if (size != numNonNull) {
    (*structNullsMap())[streamPos] =
        std::pair<raw_vector<uint64_t>, int32_t>(std::move(nullsCopy), size);
  }
}

void readStructNullsColumns(
    ByteInputStream* source,
    const std::vector<TypePtr>& types,
    bool useLosslessTimestamp,
    Scratch& scratch) {
  static const std::unordered_map<
      TypeKind,
      std::function<void(
          ByteInputStream * source,
          const TypePtr& type,
          bool useLosslessTimestamp,
          Scratch& scratch)>>
      readers = {
          {TypeKind::BOOLEAN, &readStructNulls<bool>},
          {TypeKind::TINYINT, &readStructNulls<int8_t>},
          {TypeKind::SMALLINT, &readStructNulls<int16_t>},
          {TypeKind::INTEGER, &readStructNulls<int32_t>},
          {TypeKind::BIGINT, &readStructNulls<int64_t>},
          {TypeKind::HUGEINT, &readStructNulls<int128_t>},
          {TypeKind::REAL, &readStructNulls<float>},
          {TypeKind::DOUBLE, &readStructNulls<double>},
          {TypeKind::TIMESTAMP, &readStructNulls<Timestamp>},
          {TypeKind::VARCHAR, &readStructNulls<StringView>},
          {TypeKind::VARBINARY, &readStructNulls<StringView>},
          {TypeKind::ARRAY, &readArrayVectorStructNulls},
          {TypeKind::MAP, &readMapVectorStructNulls},
          {TypeKind::ROW, &readRowVectorStructNulls},
          {TypeKind::UNKNOWN, &readStructNulls<UnknownValue>}};

  for (int32_t i = 0; i < types.size(); ++i) {
    const auto& columnType = types[i];

    const auto encoding = readLengthPrefixedString(source);
    if (encoding == kRLE) {
      readConstantVectorStructNulls(
          source, columnType, useLosslessTimestamp, scratch);
    } else if (encoding == kDictionary) {
      readDictionaryVectorStructNulls(
          source, columnType, useLosslessTimestamp, scratch);
    } else {
      checkTypeEncoding(encoding, columnType);
      const auto it = readers.find(columnType->kind());
      VELOX_CHECK(
          it != readers.end(),
          "Column reader for type {} is missing",
          columnType->kindName());

      it->second(source, columnType, useLosslessTimestamp, scratch);
    }
  }
}

void writeInt32(OutputStream* out, int32_t value) {
  out->write(reinterpret_cast<char*>(&value), sizeof(value));
}

void writeInt64(OutputStream* out, int64_t value) {
  out->write(reinterpret_cast<char*>(&value), sizeof(value));
}

class CountingOutputStream : public OutputStream {
 public:
  explicit CountingOutputStream() : OutputStream{nullptr} {}

  void write(const char* /*s*/, std::streamsize count) override {
    pos_ += count;
    if (numBytes_ < pos_) {
      numBytes_ = pos_;
    }
  }

  std::streampos tellp() const override {
    return pos_;
  }

  void seekp(std::streampos pos) override {
    pos_ = pos;
  }

  std::streamsize size() const {
    return numBytes_;
  }

 private:
  std::streamsize numBytes_{0};
  std::streampos pos_{0};
};

raw_vector<uint64_t>& threadTempNulls() {
  thread_local raw_vector<uint64_t> temp;
  return temp;
}

// Appendable container for serialized values. To append a value at a
// time, call appendNull or appendNonNull first. Then call appendLength if the
// type has a length. A null value has a length of 0. Then call appendValue if
// the value was not null.
class VectorStream {
 public:
  // This constructor takes an optional encoding and vector. In cases where the
  // vector (data) is not available when the stream is created, callers can also
  // manually specify the encoding, which only applies to the top level stream.
  // If both are specified, `encoding` takes precedence over the actual
  // encoding of `vector`. Only 'flat' encoding can take precedence over the
  // input data encoding.
  VectorStream(
      const TypePtr& type,
      std::optional<VectorEncoding::Simple> encoding,
      std::optional<VectorPtr> vector,
      StreamArena* streamArena,
      int32_t initialNumRows,
      const SerdeOpts& opts)
      : type_(type),
        streamArena_(streamArena),
        useLosslessTimestamp_(opts.useLosslessTimestamp),
        nullsFirst_(opts.nullsFirst),
        isLongDecimal_(type_->isLongDecimal()),
        opts_(opts),
        encoding_(getEncoding(encoding, vector)),
        nulls_(streamArena, true, true),
        lengths_(streamArena),
        values_(streamArena) {
    if (initialNumRows == 0) {
      initializeHeader(typeToEncodingName(type), *streamArena);
      if (type_->size() > 0) {
        hasLengths_ = true;
        children_.resize(type_->size());
        for (int32_t i = 0; i < type_->size(); ++i) {
          children_[i] = std::make_unique<VectorStream>(
              type_->childAt(i),
              std::nullopt,
              getChildAt(vector, i),
              streamArena_,
              initialNumRows,
              opts_);
        }

        // The first element in the offsets in the wire format is always 0 for
        // nested types.
        lengths_.startWrite(sizeof(vector_size_t));
        lengths_.appendOne<int32_t>(0);
      }
      return;
    }

    if (encoding_.has_value()) {
      switch (encoding_.value()) {
        case VectorEncoding::Simple::CONSTANT: {
          initializeHeader(kRLE, *streamArena);
          isConstantStream_ = true;
          children_.emplace_back(std::make_unique<VectorStream>(
              type_,
              std::nullopt,
              std::nullopt,
              streamArena,
              initialNumRows,
              opts));
          return;
        }
        case VectorEncoding::Simple::DICTIONARY: {
          // For fix width types that are smaller than int32_t (the type for
          // indexes into the dictionary) dictionary encoding increases the
          // size, so we should flatten it.
          if (type->isFixedWidth() &&
              type->cppSizeInBytes() <= sizeof(int32_t)) {
            encoding_ = std::nullopt;
            break;
          }

          initializeHeader(kDictionary, *streamArena);
          values_.startWrite(initialNumRows * 4);
          isDictionaryStream_ = true;
          children_.emplace_back(std::make_unique<VectorStream>(
              type_,
              std::nullopt,
              std::nullopt,
              streamArena,
              initialNumRows,
              opts));
          return;
        }
        default:
          break;
      }
    }

    initializeFlatStream(vector, initialNumRows);
  }

  void flattenStream(const VectorPtr& vector, int32_t initialNumRows) {
    VELOX_CHECK_EQ(nullCount_, 0);
    VELOX_CHECK_EQ(nonNullCount_, 0);
    VELOX_CHECK_EQ(totalLength_, 0);

    if (!isConstantStream_ && !isDictionaryStream_) {
      return;
    }

    encoding_ = std::nullopt;
    isConstantStream_ = false;
    isDictionaryStream_ = false;
    children_.clear();

    initializeFlatStream(vector, initialNumRows);
  }

  std::optional<VectorEncoding::Simple> getEncoding(
      std::optional<VectorEncoding::Simple> encoding,
      std::optional<VectorPtr> vector) {
    if (encoding.has_value()) {
      return encoding;
    } else if (vector.has_value()) {
      return vector.value()->encoding();
    } else {
      return std::nullopt;
    }
  }

  std::optional<VectorPtr> getChildAt(
      std::optional<VectorPtr> vector,
      size_t idx) {
    if (!vector.has_value()) {
      return std::nullopt;
    }

    if ((*vector)->encoding() == VectorEncoding::Simple::ROW) {
      return (*vector)->as<RowVector>()->childAt(idx);
    }
    return std::nullopt;
  }

  void initializeHeader(std::string_view name, StreamArena& streamArena) {
    streamArena.newTinyRange(50, nullptr, &header_);
    header_.size = name.size() + sizeof(int32_t);
    *reinterpret_cast<int32_t*>(header_.buffer) = name.size();
    ::memcpy(header_.buffer + sizeof(int32_t), &name[0], name.size());
  }

  void appendNull() {
    if (nonNullCount_ && nullCount_ == 0) {
      nulls_.appendBool(false, nonNullCount_);
    }
    nulls_.appendBool(true, 1);
    ++nullCount_;
    if (hasLengths_) {
      appendLength(0);
    }
  }

  void appendNonNull(int32_t count = 1) {
    if (nullCount_ > 0) {
      nulls_.appendBool(false, count);
    }
    nonNullCount_ += count;
  }

  void appendLength(int32_t length) {
    totalLength_ += length;
    lengths_.appendOne<int32_t>(totalLength_);
  }

  void appendNulls(
      const uint64_t* nulls,
      int32_t begin,
      int32_t end,
      int32_t numNonNull) {
    VELOX_DCHECK_EQ(numNonNull, bits::countBits(nulls, begin, end));
    const auto numRows = end - begin;
    const auto numNulls = numRows - numNonNull;
    if (numNulls == 0 && nullCount_ == 0) {
      nonNullCount_ += numNonNull;
      return;
    }
    if (FOLLY_UNLIKELY(numNulls > 0 && nonNullCount_ > 0 && nullCount_ == 0)) {
      // There were only non-nulls up until now. Add the bits for them.
      nulls_.appendBool(false, nonNullCount_);
    }
    nullCount_ += numNulls;
    nonNullCount_ += numNonNull;

    if (FOLLY_LIKELY(end <= 64)) {
      const uint64_t inverted = ~nulls[0];
      nulls_.appendBitsFresh(&inverted, begin, end);
      return;
    }

    const int32_t firstWord = begin >> 6;
    const int32_t firstBit = begin & 63;
    const auto numWords = bits::nwords(numRows + firstBit);
    // The polarity of nulls is reverse in wire format. Make an inverted copy.
    uint64_t smallNulls[16];
    uint64_t* invertedNulls = smallNulls;
    if (numWords > sizeof(smallNulls) / sizeof(smallNulls[0])) {
      auto& tempNulls = threadTempNulls();
      tempNulls.resize(numWords + 1);
      invertedNulls = tempNulls.data();
    }
    for (auto i = 0; i < numWords; ++i) {
      invertedNulls[i] = ~nulls[i + firstWord];
    }
    nulls_.appendBitsFresh(invertedNulls, firstBit, firstBit + numRows);
  }

  // Appends a zero length for each null bit and a length from lengthFunc(row)
  // for non-nulls in rows.
  template <typename LengthFunc>
  void appendLengths(
      const uint64_t* nulls,
      folly::Range<const vector_size_t*> rows,
      int32_t numNonNull,
      LengthFunc lengthFunc) {
    const auto numRows = rows.size();
    if (nulls == nullptr) {
      appendNonNull(numRows);
      for (auto i = 0; i < numRows; ++i) {
        appendLength(lengthFunc(rows[i]));
      }
    } else {
      appendNulls(nulls, 0, numRows, numNonNull);
      for (auto i = 0; i < numRows; ++i) {
        if (bits::isBitSet(nulls, i)) {
          appendLength(lengthFunc(rows[i]));
        } else {
          appendLength(0);
        }
      }
    }
  }

  template <typename T>
  void append(folly::Range<const T*> values) {
    values_.append(values);
  }

  template <typename T>
  void appendOne(const T& value) {
    append(folly::Range(&value, 1));
  }

  bool isDictionaryStream() const {
    return isDictionaryStream_;
  }

  bool isConstantStream() const {
    return isConstantStream_;
  }

  VectorStream* childAt(int32_t index) {
    return children_[index].get();
  }

  ByteOutputStream& values() {
    return values_;
  }

  auto& nulls() {
    return nulls_;
  }

  // Returns the size to flush to OutputStream before calling `flush`.
  size_t serializedSize() {
    CountingOutputStream out;
    flush(&out);
    return out.size();
  }

  // Writes out the accumulated contents. Does not change the state.
  void flush(OutputStream* out) {
    out->write(reinterpret_cast<char*>(header_.buffer), header_.size);

    if (encoding_.has_value()) {
      switch (encoding_.value()) {
        case VectorEncoding::Simple::CONSTANT: {
          writeInt32(out, nonNullCount_);
          children_[0]->flush(out);
          return;
        }
        case VectorEncoding::Simple::DICTIONARY: {
          writeInt32(out, nonNullCount_);
          children_[0]->flush(out);
          values_.flush(out);

          // Write 24 bytes of 'instance id'.
          int64_t unused{0};
          writeInt64(out, unused);
          writeInt64(out, unused);
          writeInt64(out, unused);
          return;
        }
        default:
          break;
      }
    }

    switch (type_->kind()) {
      case TypeKind::ROW:
        if (opts_.nullsFirst) {
          writeInt32(out, nullCount_ + nonNullCount_);
          flushNulls(out);
        }

        writeInt32(out, children_.size());
        for (auto& child : children_) {
          child->flush(out);
        }
        if (!opts_.nullsFirst) {
          writeInt32(out, nullCount_ + nonNullCount_);
          lengths_.flush(out);
          flushNulls(out);
        }
        return;

      case TypeKind::ARRAY:
        children_[0]->flush(out);
        writeInt32(out, nullCount_ + nonNullCount_);
        lengths_.flush(out);
        flushNulls(out);
        return;

      case TypeKind::MAP: {
        children_[0]->flush(out);
        children_[1]->flush(out);
        // hash table size. -1 means not included in serialization.
        writeInt32(out, -1);
        writeInt32(out, nullCount_ + nonNullCount_);

        lengths_.flush(out);
        flushNulls(out);
        return;
      }

      case TypeKind::VARCHAR:
      case TypeKind::VARBINARY:
        writeInt32(out, nullCount_ + nonNullCount_);
        lengths_.flush(out);
        flushNulls(out);
        writeInt32(out, values_.size());
        values_.flush(out);
        return;

      default:
        writeInt32(out, nullCount_ + nonNullCount_);
        flushNulls(out);
        values_.flush(out);
    }
  }

  void flushNulls(OutputStream* out) {
    if (!nullCount_) {
      char zero = 0;
      out->write(&zero, 1);
    } else {
      char one = 1;
      out->write(&one, 1);
      nulls_.flush(out);
    }
  }

  bool isLongDecimal() const {
    return isLongDecimal_;
  }

  void clear() {
    encoding_ = std::nullopt;
    initializeHeader(typeToEncodingName(type_), *streamArena_);
    nonNullCount_ = 0;
    nullCount_ = 0;
    totalLength_ = 0;
    if (hasLengths_) {
      lengths_.startWrite(lengths_.size());
      if (type_->kind() == TypeKind::ROW || type_->kind() == TypeKind::ARRAY ||
          type_->kind() == TypeKind::MAP) {
        // A complex type has a 0 as first length.
        lengths_.appendOne<int32_t>(0);
      }
    }
    nulls_.startWrite(nulls_.size());
    values_.startWrite(values_.size());
    for (auto& child : children_) {
      child->clear();
    }
  }

 private:
  void initializeFlatStream(
      std::optional<VectorPtr> vector,
      vector_size_t initialNumRows) {
    initializeHeader(typeToEncodingName(type_), *streamArena_);
    nulls_.startWrite(1 + (initialNumRows / 8));

    switch (type_->kind()) {
      case TypeKind::ROW:
        [[fallthrough]];
      case TypeKind::ARRAY:
        [[fallthrough]];
      case TypeKind::MAP:
        hasLengths_ = true;
        lengths_.startWrite(initialNumRows * sizeof(vector_size_t));
        children_.resize(type_->size());
        for (int32_t i = 0; i < type_->size(); ++i) {
          children_[i] = std::make_unique<VectorStream>(
              type_->childAt(i),
              std::nullopt,
              getChildAt(vector, i),
              streamArena_,
              initialNumRows,
              opts_);
        }
        // The first element in the offsets in the wire format is always 0 for
        // nested types.
        lengths_.appendOne<int32_t>(0);
        break;
      case TypeKind::VARCHAR:
        [[fallthrough]];
      case TypeKind::VARBINARY:
        hasLengths_ = true;
        lengths_.startWrite(initialNumRows * sizeof(vector_size_t));
        if (values_.ranges().empty()) {
          values_.startWrite(initialNumRows * 10);
        }
        break;
      default:
        if (values_.ranges().empty()) {
          values_.startWrite(initialNumRows * 4);
        }
        break;
    }
  }

  const TypePtr type_;
  StreamArena* const streamArena_;
  /// Indicates whether to serialize timestamps with nanosecond precision.
  /// If false, they are serialized with millisecond precision which is
  /// compatible with presto.
  const bool useLosslessTimestamp_;
  const bool nullsFirst_;
  const bool isLongDecimal_;
  const SerdeOpts opts_;
  std::optional<VectorEncoding::Simple> encoding_;
  int32_t nonNullCount_{0};
  int32_t nullCount_{0};
  int32_t totalLength_{0};
  bool hasLengths_{false};
  ByteRange header_;
  ByteOutputStream nulls_;
  ByteOutputStream lengths_;
  ByteOutputStream values_;
  std::vector<std::unique_ptr<VectorStream>> children_;
  bool isDictionaryStream_{false};
  bool isConstantStream_{false};
};

template <>
inline void VectorStream::append(folly::Range<const StringView*> values) {
  for (auto& value : values) {
    auto size = value.size();
    appendLength(size);
    values_.appendStringView(value);
  }
}

template <>
void VectorStream::append(folly::Range<const Timestamp*> values) {
  if (opts_.useLosslessTimestamp) {
    for (auto& value : values) {
      appendOne(value.getSeconds());
      appendOne(value.getNanos());
    }
  } else {
    for (auto& value : values) {
      appendOne(value.toMillis());
    }
  }
}

template <>
void VectorStream::append(folly::Range<const bool*> values) {
  // A bool constant is serialized via this. Accessing consecutive
  // elements via bool& does not work, hence the flat serialization is
  // specialized one level above this.
  VELOX_CHECK(values.size() == 1);
  appendOne<uint8_t>(values[0] ? 1 : 0);
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

template <>
void VectorStream::append(folly::Range<const int128_t*> values) {
  for (auto& value : values) {
    int128_t val = value;
    if (isLongDecimal_) {
      val = toJavaDecimalValue(value);
    }
    values_.append<int128_t>(folly::Range(&val, 1));
  }
}

template <TypeKind kind>
void serializeFlatVector(
    const VectorPtr& vector,
    const folly::Range<const IndexRange*>& ranges,
    VectorStream* stream) {
  using T = typename TypeTraits<kind>::NativeType;
  auto* flatVector = vector->as<FlatVector<T>>();
  auto* rawValues = flatVector->rawValues();
  if (!flatVector->mayHaveNulls()) {
    for (auto& range : ranges) {
      stream->appendNonNull(range.size);
      stream->append<T>(folly::Range(&rawValues[range.begin], range.size));
    }
  } else {
    int32_t firstNonNull = -1;
    int32_t lastNonNull = -1;
    for (int32_t i = 0; i < ranges.size(); ++i) {
      const int32_t end = ranges[i].begin + ranges[i].size;
      for (int32_t offset = ranges[i].begin; offset < end; ++offset) {
        if (flatVector->isNullAt(offset)) {
          stream->appendNull();
          continue;
        }
        stream->appendNonNull();
        if (std::is_same_v<T, StringView>) {
          // Bunching consecutive non-nulls into one append does not work with
          // strings because the lengths will then get out of order with the
          // zero lengths produced by nulls.
          stream->appendOne(rawValues[offset]);
        } else if (firstNonNull == -1) {
          firstNonNull = offset;
          lastNonNull = offset;
        } else if (offset == lastNonNull + 1) {
          lastNonNull = offset;
        } else {
          stream->append<T>(folly::Range(
              &rawValues[firstNonNull], 1 + lastNonNull - firstNonNull));
          firstNonNull = offset;
          lastNonNull = offset;
        }
      }
    }
    if (firstNonNull != -1 && !std::is_same_v<T, StringView>) {
      stream->append<T>(folly::Range(
          &rawValues[firstNonNull], 1 + lastNonNull - firstNonNull));
    }
  }
}

template <>
void serializeFlatVector<TypeKind::BOOLEAN>(
    const VectorPtr& vector,
    const folly::Range<const IndexRange*>& ranges,
    VectorStream* stream) {
  auto flatVector = vector->as<FlatVector<bool>>();
  if (!vector->mayHaveNulls()) {
    for (int32_t i = 0; i < ranges.size(); ++i) {
      stream->appendNonNull(ranges[i].size);
      int32_t end = ranges[i].begin + ranges[i].size;
      for (int32_t offset = ranges[i].begin; offset < end; ++offset) {
        stream->appendOne<uint8_t>(flatVector->valueAtFast(offset) ? 1 : 0);
      }
    }
  } else {
    for (int32_t i = 0; i < ranges.size(); ++i) {
      int32_t end = ranges[i].begin + ranges[i].size;
      for (int32_t offset = ranges[i].begin; offset < end; ++offset) {
        if (vector->isNullAt(offset)) {
          stream->appendNull();
          continue;
        }
        stream->appendNonNull();
        stream->appendOne<uint8_t>(flatVector->valueAtFast(offset) ? 1 : 0);
      }
    }
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

void serializeWrapped(
    const VectorPtr& vector,
    const folly::Range<const IndexRange*>& ranges,
    VectorStream* stream,
    Scratch& scratch) {
  std::vector<IndexRange> newRanges;
  const bool mayHaveNulls = vector->mayHaveNulls();
  const VectorPtr& wrapped = BaseVector::wrappedVectorShared(vector);
  for (int32_t i = 0; i < ranges.size(); ++i) {
    const auto end = ranges[i].begin + ranges[i].size;
    for (int32_t offset = ranges[i].begin; offset < end; ++offset) {
      if (mayHaveNulls && vector->isNullAt(offset)) {
        // The wrapper added a null.
        if (!newRanges.empty()) {
          serializeColumn(wrapped, newRanges, stream, scratch);
          newRanges.clear();
        }
        stream->appendNull();
        continue;
      }
      const auto innerIndex = vector->wrappedIndex(offset);
      newRanges.push_back(IndexRange{innerIndex, 1});
    }
  }
  if (!newRanges.empty()) {
    serializeColumn(wrapped, newRanges, stream, scratch);
  }
}

void serializeRowVector(
    const VectorPtr& vector,
    const folly::Range<const IndexRange*>& ranges,
    VectorStream* stream,
    Scratch& scratch) {
  auto rowVector = vector->as<RowVector>();

  std::vector<IndexRange> childRanges;
  for (int32_t i = 0; i < ranges.size(); ++i) {
    auto begin = ranges[i].begin;
    auto end = begin + ranges[i].size;
    for (auto offset = begin; offset < end; ++offset) {
      if (rowVector->isNullAt(offset)) {
        stream->appendNull();
      } else {
        stream->appendNonNull();
        stream->appendLength(1);
        childRanges.push_back(IndexRange{offset, 1});
      }
    }
  }
  for (int32_t i = 0; i < rowVector->childrenSize(); ++i) {
    serializeColumn(
        rowVector->childAt(i), childRanges, stream->childAt(i), scratch);
  }
}

void serializeArrayVector(
    const VectorPtr& vector,
    const folly::Range<const IndexRange*>& ranges,
    VectorStream* stream,
    Scratch& scratch) {
  auto arrayVector = vector->as<ArrayVector>();
  auto rawSizes = arrayVector->rawSizes();
  auto rawOffsets = arrayVector->rawOffsets();
  std::vector<IndexRange> childRanges;
  childRanges.reserve(ranges.size());
  for (int32_t i = 0; i < ranges.size(); ++i) {
    int32_t begin = ranges[i].begin;
    int32_t end = begin + ranges[i].size;
    for (int32_t offset = begin; offset < end; ++offset) {
      if (arrayVector->isNullAt(offset)) {
        stream->appendNull();
      } else {
        stream->appendNonNull();
        auto size = rawSizes[offset];
        stream->appendLength(size);
        if (size > 0) {
          childRanges.emplace_back<IndexRange>({rawOffsets[offset], size});
        }
      }
    }
  }
  serializeColumn(
      arrayVector->elements(), childRanges, stream->childAt(0), scratch);
}

void serializeMapVector(
    const VectorPtr& vector,
    const folly::Range<const IndexRange*>& ranges,
    VectorStream* stream,
    Scratch& scratch) {
  auto mapVector = vector->as<MapVector>();
  auto rawSizes = mapVector->rawSizes();
  auto rawOffsets = mapVector->rawOffsets();
  std::vector<IndexRange> childRanges;
  childRanges.reserve(ranges.size());
  for (int32_t i = 0; i < ranges.size(); ++i) {
    int32_t begin = ranges[i].begin;
    int32_t end = begin + ranges[i].size;
    for (int32_t offset = begin; offset < end; ++offset) {
      if (mapVector->isNullAt(offset)) {
        stream->appendNull();
      } else {
        stream->appendNonNull();
        auto size = rawSizes[offset];
        stream->appendLength(size);
        if (size > 0) {
          childRanges.emplace_back<IndexRange>({rawOffsets[offset], size});
        }
      }
    }
  }
  serializeColumn(
      mapVector->mapKeys(), childRanges, stream->childAt(0), scratch);
  serializeColumn(
      mapVector->mapValues(), childRanges, stream->childAt(1), scratch);
}

static inline int32_t rangesTotalSize(
    const folly::Range<const IndexRange*>& ranges) {
  int32_t total = 0;
  for (auto& range : ranges) {
    total += range.size;
  }
  return total;
}

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

template <TypeKind Kind>
void serializeDictionaryVector(
    const VectorPtr& vector,
    const folly::Range<const IndexRange*>& ranges,
    VectorStream* stream,
    Scratch& scratch) {
  // Check if the stream was set up for dictionary (we had to know the
  // encoding type when creating VectorStream for that).
  if (!stream->isDictionaryStream()) {
    serializeWrapped(vector, ranges, stream, scratch);
    return;
  }

  auto numRows = rangesTotalSize(ranges);

  // Cannot serialize dictionary as PrestoPage dictionary if it has nulls.
  if (vector->nulls()) {
    stream->flattenStream(vector, numRows);
    serializeWrapped(vector, ranges, stream, scratch);
    return;
  }

  using T = typename KindToFlatVector<Kind>::WrapperType;
  auto dictionaryVector = vector->as<DictionaryVector<T>>();

  ScratchPtr<vector_size_t, 64> selectedIndicesHolder(scratch);
  auto* mutableSelectedIndices =
      selectedIndicesHolder.get(dictionaryVector->valueVector()->size());
  auto numUsed = computeSelectedIndices(
      dictionaryVector, ranges, scratch, mutableSelectedIndices);

  // If the values are fixed width and we aren't getting enough reuse to justify
  // the dictionary, flatten it.
  // For variable width types, rather than iterate over them computing their
  // size, we simply assume we'll get a benefit.
  if constexpr (TypeTraits<Kind>::isFixedWidth) {
    // This calculation admittdely ignores some constants, but if they really
    // make a difference, they're small so there's not much difference either
    // way.
    if (numUsed * vector->type()->cppSizeInBytes() +
            numRows * sizeof(int32_t) >=
        numRows * vector->type()->cppSizeInBytes()) {
      stream->flattenStream(vector, numRows);
      serializeWrapped(vector, ranges, stream, scratch);
      return;
    }
  }

  // If every element is unique the dictionary isn't giving us any benefit,
  // flatten it.
  if (numUsed == numRows) {
    stream->flattenStream(vector, numRows);
    serializeWrapped(vector, ranges, stream, scratch);
    return;
  }

  // Serialize the used elements from the Dictionary.
  serializeColumn(
      dictionaryVector->valueVector(),
      folly::Range<const vector_size_t*>(mutableSelectedIndices, numUsed),
      stream->childAt(0),
      scratch);

  // Create a mapping from the original indices to the indices in the shrunk
  // Dictionary of just used values.
  ScratchPtr<vector_size_t, 64> updatedIndicesHolder(scratch);
  auto* updatedIndices =
      updatedIndicesHolder.get(dictionaryVector->valueVector()->size());
  vector_size_t curIndex = 0;
  for (vector_size_t i = 0; i < numUsed; ++i) {
    updatedIndices[mutableSelectedIndices[i]] = curIndex++;
  }

  // Write out the indices, translating them using the above mapping.
  stream->appendNonNull(numRows);
  auto* indices = dictionaryVector->indices()->template as<vector_size_t>();
  for (const auto& range : ranges) {
    for (auto i = 0; i < range.size; ++i) {
      stream->appendOne(updatedIndices[indices[range.begin + i]]);
    }
  }
}

template <TypeKind kind>
void serializeConstantVectorImpl(
    const VectorPtr& vector,
    const folly::Range<const IndexRange*>& ranges,
    VectorStream* stream,
    Scratch& scratch) {
  using T = typename KindToFlatVector<kind>::WrapperType;
  auto constVector = vector->as<ConstantVector<T>>();
  if (constVector->valueVector() != nullptr) {
    serializeWrapped(vector, ranges, stream, scratch);
    return;
  }

  const int32_t count = rangesTotalSize(ranges);
  if (vector->isNullAt(0)) {
    for (int32_t i = 0; i < count; ++i) {
      stream->appendNull();
    }
    return;
  }

  const T value = constVector->valueAtFast(0);
  for (int32_t i = 0; i < count; ++i) {
    stream->appendNonNull();
    stream->appendOne(value);
  }
}

template <TypeKind Kind>
void serializeConstantVector(
    const VectorPtr& vector,
    const folly::Range<const IndexRange*>& ranges,
    VectorStream* stream,
    Scratch& scratch) {
  if (stream->isConstantStream()) {
    for (const auto& range : ranges) {
      stream->appendNonNull(range.size);
    }

    std::vector<IndexRange> newRanges;
    newRanges.push_back({0, 1});
    serializeConstantVectorImpl<Kind>(
        vector, newRanges, stream->childAt(0), scratch);
  } else {
    serializeConstantVectorImpl<Kind>(vector, ranges, stream, scratch);
  }
}

template <typename T>
void serializeBiasVector(
    const VectorPtr& vector,
    const folly::Range<const IndexRange*>& ranges,
    VectorStream* stream) {
  auto biasVector = vector->as<BiasVector<T>>();
  if (!vector->mayHaveNulls()) {
    for (int32_t i = 0; i < ranges.size(); ++i) {
      stream->appendNonNull(ranges[i].size);
      int32_t end = ranges[i].begin + ranges[i].size;
      for (int32_t offset = ranges[i].begin; offset < end; ++offset) {
        stream->appendOne(biasVector->valueAtFast(offset));
      }
    }
  } else {
    for (int32_t i = 0; i < ranges.size(); ++i) {
      int32_t end = ranges[i].begin + ranges[i].size;
      for (int32_t offset = ranges[i].begin; offset < end; ++offset) {
        if (biasVector->isNullAt(offset)) {
          stream->appendNull();
          continue;
        }
        stream->appendNonNull();
        stream->appendOne(biasVector->valueAtFast(offset));
      }
    }
  }
}

void serializeColumn(
    const VectorPtr& vector,
    const folly::Range<const IndexRange*>& ranges,
    VectorStream* stream,
    Scratch& scratch) {
  switch (vector->encoding()) {
    case VectorEncoding::Simple::FLAT:
      VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(
          serializeFlatVector, vector->typeKind(), vector, ranges, stream);
      break;
    case VectorEncoding::Simple::CONSTANT:
      VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
          serializeConstantVector,
          vector->typeKind(),
          vector,
          ranges,
          stream,
          scratch);
      break;
    case VectorEncoding::Simple::DICTIONARY:
      VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
          serializeDictionaryVector,
          vector->typeKind(),
          vector,
          ranges,
          stream,
          scratch);
      break;
    case VectorEncoding::Simple::BIASED:
      switch (vector->typeKind()) {
        case TypeKind::SMALLINT:
          serializeBiasVector<int16_t>(vector, ranges, stream);
          break;
        case TypeKind::INTEGER:
          serializeBiasVector<int32_t>(vector, ranges, stream);
          break;
        case TypeKind::BIGINT:
          serializeBiasVector<int64_t>(vector, ranges, stream);
          break;
        default:
          VELOX_FAIL(
              "Invalid biased vector type {}",
              static_cast<int>(vector->encoding()));
      }
      break;
    case VectorEncoding::Simple::ROW:
      serializeRowVector(vector, ranges, stream, scratch);
      break;
    case VectorEncoding::Simple::ARRAY:
      serializeArrayVector(vector, ranges, stream, scratch);
      break;
    case VectorEncoding::Simple::MAP:
      serializeMapVector(vector, ranges, stream, scratch);
      break;
    case VectorEncoding::Simple::LAZY:
      serializeColumn(
          BaseVector::loadedVectorShared(vector), ranges, stream, scratch);
      break;
    default:
      serializeWrapped(vector, ranges, stream, scratch);
  }
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
    Scratch& scratch) {
  auto numRows = rows.size();
  auto* innerRows = rows.data();
  auto* nonNullRows = innerRows;
  int32_t numInner = rows.size();
  ScratchPtr<vector_size_t, 64> nonNullHolder(scratch);
  ScratchPtr<vector_size_t, 64> innerRowsHolder(scratch);
  if (rawNulls) {
    ScratchPtr<uint64_t, 4> nullsHolder(scratch);
    auto* nulls = nullsHolder.get(bits::nwords(rows.size()));
    simd::gatherBits(rawNulls, rows, nulls);
    auto* mutableNonNullRows = nonNullHolder.get(numRows);
    auto* mutableInnerRows = innerRowsHolder.get(numRows);
    numInner = simd::indicesOfSetBits(nulls, 0, numRows, mutableNonNullRows);
    if (stream) {
      stream->appendLengths(
          nulls, rows, numInner, [&](auto row) { return sizes[row]; });
    }
    simd::transpose(
        rows.data(),
        folly::Range<const vector_size_t*>(mutableNonNullRows, numInner),
        mutableInnerRows);
    nonNullRows = mutableNonNullRows;
    innerRows = mutableInnerRows;
  } else if (stream) {
    stream->appendNonNull(rows.size());
    for (auto i = 0; i < rows.size(); ++i) {
      stream->appendLength(sizes[rows[i]]);
    }
  }
  vector_size_t** sizesOut = nullptr;
  if (sizesPtr) {
    sizesOut = sizesHolder->get(numInner);
  }
  auto ranges = rangesHolder.get(numInner);
  int32_t fill = 0;
  for (auto i = 0; i < numInner; ++i) {
    // Add the size of the length.
    if (sizesPtr) {
      *sizesPtr[rawNulls ? nonNullRows[i] : i] += sizeof(int32_t);
    }
    if (sizes[innerRows[i]] == 0) {
      continue;
    }
    if (sizesOut) {
      sizesOut[fill] = sizesPtr[rawNulls ? nonNullRows[i] : i];
    }
    ranges[fill].begin = offsets[innerRows[i]];
    ranges[fill].size = sizes[innerRows[i]];
    ++fill;
  }
  return fill;
}

template <typename T>
void copyWords(
    T* destination,
    const int32_t* indices,
    int32_t numIndices,
    const T* values,
    bool isLongDecimal = false) {
  if (std::is_same_v<T, int128_t> && isLongDecimal) {
    for (auto i = 0; i < numIndices; ++i) {
      reinterpret_cast<int128_t*>(destination)[i] = toJavaDecimalValue(
          reinterpret_cast<const int128_t*>(values)[indices[i]]);
    }
    return;
  }
  for (auto i = 0; i < numIndices; ++i) {
    destination[i] = values[indices[i]];
  }
}

template <typename T>
void copyWordsWithRows(
    T* destination,
    const int32_t* rows,
    const int32_t* indices,
    int32_t numIndices,
    const T* values,
    bool isLongDecimal = false) {
  if (!indices) {
    copyWords(destination, rows, numIndices, values, isLongDecimal);
    return;
  }
  if (std::is_same_v<T, int128_t> && isLongDecimal) {
    for (auto i = 0; i < numIndices; ++i) {
      reinterpret_cast<int128_t*>(destination)[i] = toJavaDecimalValue(
          reinterpret_cast<const int128_t*>(values)[rows[indices[i]]]);
    }
    return;
  }
  for (auto i = 0; i < numIndices; ++i) {
    destination[i] = values[rows[indices[i]]];
  }
}

template <typename T>
void appendNonNull(
    VectorStream* stream,
    const uint64_t* nulls,
    folly::Range<const vector_size_t*> rows,
    const T* values,
    Scratch& scratch) {
  auto numRows = rows.size();
  ScratchPtr<int32_t, 64> nonNullHolder(scratch);
  const int32_t* nonNullIndices;
  int32_t numNonNull;
  if (LIKELY(numRows <= 8)) {
    // Short batches need extra optimization. The set bits are prematerialized.
    uint8_t nullsByte = *reinterpret_cast<const uint8_t*>(nulls);
    numNonNull = __builtin_popcount(nullsByte);
    nonNullIndices =
        numNonNull == numRows ? nullptr : simd::byteSetBits(nullsByte);
  } else {
    auto mutableIndices = nonNullHolder.get(numRows);
    // Convert null flags to indices. This is much faster than checking bits one
    // by one, several bits per clock specially if mostly null or non-null. Even
    // worst case of half nulls is more than one row per clock.
    numNonNull = simd::indicesOfSetBits(nulls, 0, numRows, mutableIndices);
    nonNullIndices = numNonNull == numRows ? nullptr : mutableIndices;
  }
  stream->appendNulls(nulls, 0, rows.size(), numNonNull);
  ByteOutputStream& out = stream->values();

  if constexpr (sizeof(T) == 8) {
    AppendWindow<int64_t> window(out, scratch);
    int64_t* output = window.get(numNonNull);
    copyWordsWithRows(
        output,
        rows.data(),
        nonNullIndices,
        numNonNull,
        reinterpret_cast<const int64_t*>(values));
  } else if constexpr (sizeof(T) == 4) {
    AppendWindow<int32_t> window(out, scratch);
    int32_t* output = window.get(numNonNull);
    copyWordsWithRows(
        output,
        rows.data(),
        nonNullIndices,
        numNonNull,
        reinterpret_cast<const int32_t*>(values));
  } else {
    AppendWindow<T> window(out, scratch);
    T* output = window.get(numNonNull);
    copyWordsWithRows(
        output,
        rows.data(),
        nonNullIndices,
        numNonNull,
        values,
        stream->isLongDecimal());
  }
}

void appendStrings(
    const uint64_t* nulls,
    folly::Range<const vector_size_t*> rows,
    const StringView* views,
    VectorStream* stream,
    Scratch& scratch) {
  if (nulls == nullptr) {
    stream->appendLengths(nullptr, rows, rows.size(), [&](auto row) {
      return views[row].size();
    });
    for (auto i = 0; i < rows.size(); ++i) {
      const auto& view = views[rows[i]];
      stream->values().appendStringView(
          std::string_view(view.data(), view.size()));
    }
    return;
  }

  ScratchPtr<vector_size_t, 64> nonNullHolder(scratch);
  auto* nonNull = nonNullHolder.get(rows.size());
  const auto numNonNull =
      simd::indicesOfSetBits(nulls, 0, rows.size(), nonNull);
  stream->appendLengths(
      nulls, rows, numNonNull, [&](auto row) { return views[row].size(); });
  for (auto i = 0; i < numNonNull; ++i) {
    auto& view = views[rows[nonNull[i]]];
    stream->values().appendStringView(
        std::string_view(view.data(), view.size()));
  }
}

void appendTimestamps(
    const uint64_t* nulls,
    folly::Range<const vector_size_t*> rows,
    const Timestamp* timestamps,
    VectorStream* stream,
    Scratch& scratch) {
  if (nulls == nullptr) {
    stream->appendNonNull(rows.size());
    for (auto i = 0; i < rows.size(); ++i) {
      stream->appendOne(timestamps[rows[i]]);
    }
    return;
  }

  ScratchPtr<vector_size_t, 64> nonNullHolder(scratch);
  auto* nonNullRows = nonNullHolder.get(rows.size());
  const auto numNonNull =
      simd::indicesOfSetBits(nulls, 0, rows.size(), nonNullRows);
  stream->appendNulls(nulls, 0, rows.size(), numNonNull);
  for (auto i = 0; i < numNonNull; ++i) {
    stream->appendOne(timestamps[rows[nonNullRows[i]]]);
  }
}

template <TypeKind kind>
void serializeFlatVector(
    const VectorPtr& vector,
    const folly::Range<const vector_size_t*>& rows,
    VectorStream* stream,
    Scratch& scratch) {
  using T = typename TypeTraits<kind>::NativeType;
  auto* flatVector = vector->asUnchecked<FlatVector<T>>();
  auto* rawValues = flatVector->rawValues();
  if (!flatVector->mayHaveNulls()) {
    if (std::is_same_v<T, Timestamp>) {
      appendTimestamps(
          nullptr,
          rows,
          reinterpret_cast<const Timestamp*>(rawValues),
          stream,
          scratch);
      return;
    }

    if (std::is_same_v<T, StringView>) {
      appendStrings(
          nullptr,
          rows,
          reinterpret_cast<const StringView*>(rawValues),
          stream,
          scratch);
      return;
    }

    stream->appendNonNull(rows.size());
    AppendWindow<T> window(stream->values(), scratch);
    T* output = window.get(rows.size());
    copyWords(
        output, rows.data(), rows.size(), rawValues, stream->isLongDecimal());
    return;
  }

  ScratchPtr<uint64_t, 4> nullsHolder(scratch);
  uint64_t* nulls = nullsHolder.get(bits::nwords(rows.size()));
  simd::gatherBits(vector->rawNulls(), rows, nulls);
  if (std::is_same_v<T, Timestamp>) {
    appendTimestamps(
        nulls,
        rows,
        reinterpret_cast<const Timestamp*>(rawValues),
        stream,
        scratch);
    return;
  }
  if (std::is_same_v<T, StringView>) {
    appendStrings(
        nulls,
        rows,
        reinterpret_cast<const StringView*>(rawValues),
        stream,
        scratch);
    return;
  }
  appendNonNull(stream, nulls, rows, rawValues, scratch);
}

uint64_t bitsToBytesMap[256];

uint64_t bitsToBytes(uint8_t byte) {
  return bitsToBytesMap[byte];
}

template <>
void serializeFlatVector<TypeKind::BOOLEAN>(
    const VectorPtr& vector,
    const folly::Range<const vector_size_t*>& rows,
    VectorStream* stream,
    Scratch& scratch) {
  auto* flatVector = vector->as<FlatVector<bool>>();
  auto* rawValues = flatVector->rawValues<uint64_t*>();
  ScratchPtr<uint64_t, 4> bitsHolder(scratch);
  uint64_t* valueBits;
  int32_t numValueBits;
  if (!flatVector->mayHaveNulls()) {
    stream->appendNonNull(rows.size());
    valueBits = bitsHolder.get(bits::nwords(rows.size()));
    simd::gatherBits(
        reinterpret_cast<const uint64_t*>(rawValues), rows, valueBits);
    numValueBits = rows.size();
  } else {
    uint64_t* nulls = bitsHolder.get(bits::nwords(rows.size()));
    simd::gatherBits(vector->rawNulls(), rows, nulls);
    ScratchPtr<vector_size_t, 64> nonNullsHolder(scratch);
    auto* nonNulls = nonNullsHolder.get(rows.size());
    numValueBits = simd::indicesOfSetBits(nulls, 0, rows.size(), nonNulls);
    stream->appendNulls(nulls, 0, rows.size(), numValueBits);
    valueBits = nulls;
    simd::transpose(
        rows.data(),
        folly::Range<const vector_size_t*>(nonNulls, numValueBits),
        nonNulls);
    simd::gatherBits(
        reinterpret_cast<const uint64_t*>(rawValues),
        folly::Range<const vector_size_t*>(nonNulls, numValueBits),
        valueBits);
  }

  // 'valueBits' contains the non-null bools to be appended to the
  // stream. The wire format has a byte for each bit. Every full byte
  // is appended as a word. The partial byte is translated to a word
  // and its low bytes are appended.
  AppendWindow<uint8_t> window(stream->values(), scratch);
  uint8_t* output = window.get(numValueBits);
  const auto numBytes = bits::nbytes(numValueBits);
  for (auto i = 0; i < numBytes; ++i) {
    uint64_t word = bitsToBytes(reinterpret_cast<uint8_t*>(valueBits)[i]);
    if (i < numBytes - 1) {
      reinterpret_cast<uint64_t*>(output)[i] = word;
    } else {
      memcpy(output + i * 8, &word, numValueBits - i * 8);
    }
  }
}

void serializeWrapped(
    const VectorPtr& vector,
    const folly::Range<const vector_size_t*>& rows,
    VectorStream* stream,
    Scratch& scratch) {
  ScratchPtr<vector_size_t, 1> innerRowsHolder(scratch);
  const int32_t numRows = rows.size();
  int32_t numInner = 0;
  auto* innerRows = innerRowsHolder.get(numRows);
  bool mayHaveNulls = vector->mayHaveNulls();
  VectorPtr wrapped;
  if (vector->encoding() == VectorEncoding::Simple::DICTIONARY &&
      !mayHaveNulls) {
    // Dictionary with no nulls.
    auto* indices = vector->wrapInfo()->as<vector_size_t>();
    wrapped = vector->valueVector();
    simd::transpose(indices, rows, innerRows);
    numInner = numRows;
  } else {
    wrapped = BaseVector::wrappedVectorShared(vector);
    for (int32_t i = 0; i < rows.size(); ++i) {
      if (mayHaveNulls && vector->isNullAt(rows[i])) {
        // The wrapper added a null.
        if (numInner > 0) {
          serializeColumn(
              wrapped,
              folly::Range<const vector_size_t*>(innerRows, numInner),
              stream,
              scratch);
          numInner = 0;
        }
        stream->appendNull();
        continue;
      }
      innerRows[numInner++] = vector->wrappedIndex(rows[i]);
    }
  }

  if (numInner > 0) {
    serializeColumn(
        wrapped,
        folly::Range<const vector_size_t*>(innerRows, numInner),
        stream,
        scratch);
  }
}

template <>
void serializeFlatVector<TypeKind::UNKNOWN>(
    const VectorPtr& vector,
    const folly::Range<const vector_size_t*>& rows,
    VectorStream* stream,
    Scratch& scratch) {
  VELOX_CHECK_NOT_NULL(vector->rawNulls());
  for (auto i = 0; i < rows.size(); ++i) {
    VELOX_DCHECK(vector->isNullAt(rows[i]));
    stream->appendNull();
  }
}

template <>
void serializeFlatVector<TypeKind::OPAQUE>(
    const VectorPtr& vector,
    const folly::Range<const vector_size_t*>& ranges,
    VectorStream* stream,
    Scratch& scratch) {
  VELOX_UNSUPPORTED();
}

void serializeRowVector(
    const VectorPtr& vector,
    const folly::Range<const vector_size_t*>& rows,
    VectorStream* stream,
    Scratch& scratch) {
  auto rowVector = vector->as<RowVector>();
  ScratchPtr<uint64_t, 4> nullsHolder(scratch);
  ScratchPtr<vector_size_t, 64> innerRowsHolder(scratch);
  auto innerRows = rows.data();
  auto numInnerRows = rows.size();
  if (auto rawNulls = vector->rawNulls()) {
    auto nulls = nullsHolder.get(bits::nwords(rows.size()));
    simd::gatherBits(rawNulls, rows, nulls);
    auto* mutableInnerRows = innerRowsHolder.get(rows.size());
    numInnerRows =
        simd::indicesOfSetBits(nulls, 0, rows.size(), mutableInnerRows);
    stream->appendLengths(nulls, rows, numInnerRows, [](int32_t) { return 1; });
    simd::transpose(
        rows.data(),
        folly::Range<const vector_size_t*>(mutableInnerRows, numInnerRows),
        mutableInnerRows);
    innerRows = mutableInnerRows;
  } else {
    stream->appendLengths(
        nullptr, rows, rows.size(), [](int32_t) { return 1; });
  }
  for (int32_t i = 0; i < rowVector->childrenSize(); ++i) {
    serializeColumn(
        rowVector->childAt(i),
        folly::Range<const vector_size_t*>(innerRows, numInnerRows),
        stream->childAt(i),
        scratch);
  }
}

void serializeArrayVector(
    const VectorPtr& vector,
    const folly::Range<const vector_size_t*>& rows,
    VectorStream* stream,
    Scratch& scratch) {
  auto arrayVector = vector->as<ArrayVector>();

  ScratchPtr<IndexRange> rangesHolder(scratch);
  int32_t numRanges = rowsToRanges(
      rows,
      arrayVector->rawNulls(),
      arrayVector->rawOffsets(),
      arrayVector->rawSizes(),
      nullptr,
      rangesHolder,
      nullptr,
      stream,
      scratch);
  if (numRanges == 0) {
    return;
  }
  serializeColumn(
      arrayVector->elements(),
      folly::Range<const IndexRange*>(rangesHolder.get(), numRanges),
      stream->childAt(0),
      scratch);
}

void serializeMapVector(
    const VectorPtr& vector,
    const folly::Range<const vector_size_t*>& rows,
    VectorStream* stream,
    Scratch& scratch) {
  auto mapVector = vector->as<MapVector>();

  ScratchPtr<IndexRange> rangesHolder(scratch);
  int32_t numRanges = rowsToRanges(
      rows,
      mapVector->rawNulls(),
      mapVector->rawOffsets(),
      mapVector->rawSizes(),
      nullptr,
      rangesHolder,
      nullptr,
      stream,
      scratch);
  if (numRanges == 0) {
    return;
  }
  serializeColumn(
      mapVector->mapKeys(),
      folly::Range<const IndexRange*>(rangesHolder.get(), numRanges),
      stream->childAt(0),
      scratch);
  serializeColumn(
      mapVector->mapValues(),
      folly::Range<const IndexRange*>(rangesHolder.get(), numRanges),
      stream->childAt(1),
      scratch);
}

template <TypeKind kind>
void serializeConstantVector(
    const VectorPtr& vector,
    const folly::Range<const vector_size_t*>& rows,
    VectorStream* stream,
    Scratch& scratch) {
  using T = typename KindToFlatVector<kind>::WrapperType;
  auto constVector = vector->as<ConstantVector<T>>();
  if (constVector->valueVector()) {
    serializeWrapped(vector, rows, stream, scratch);
    return;
  }
  const auto numRows = rows.size();
  if (vector->isNullAt(0)) {
    for (int32_t i = 0; i < numRows; ++i) {
      stream->appendNull();
    }
    return;
  }

  T value = constVector->valueAtFast(0);
  for (int32_t i = 0; i < numRows; ++i) {
    stream->appendNonNull();
    stream->appendOne(value);
  }
}

template <typename T>
void serializeBiasVector(
    const BaseVector* vector,
    const folly::Range<const vector_size_t*>& rows,
    VectorStream* stream,
    Scratch& scratch) {
  VELOX_UNSUPPORTED();
}

void serializeColumn(
    const VectorPtr& vector,
    const folly::Range<const vector_size_t*>& rows,
    VectorStream* stream,
    Scratch& scratch) {
  switch (vector->encoding()) {
    case VectorEncoding::Simple::FLAT:
      VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(
          serializeFlatVector,
          vector->typeKind(),
          vector,
          rows,
          stream,
          scratch);
      break;
    case VectorEncoding::Simple::CONSTANT:
      VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
          serializeConstantVector,
          vector->typeKind(),
          vector,
          rows,
          stream,
          scratch);
      break;
    case VectorEncoding::Simple::BIASED:
      VELOX_UNSUPPORTED();
    case VectorEncoding::Simple::ROW:
      serializeRowVector(vector, rows, stream, scratch);
      break;
    case VectorEncoding::Simple::ARRAY:
      serializeArrayVector(vector, rows, stream, scratch);
      break;
    case VectorEncoding::Simple::MAP:
      serializeMapVector(vector, rows, stream, scratch);
      break;
    case VectorEncoding::Simple::LAZY:
      serializeColumn(
          BaseVector::loadedVectorShared(vector), rows, stream, scratch);
      break;
    default:
      serializeWrapped(vector, rows, stream, scratch);
  }
}

void expandRepeatedRanges(
    const BaseVector* vector,
    const vector_size_t* rawOffsets,
    const vector_size_t* rawSizes,
    const folly::Range<const IndexRange*>& ranges,
    vector_size_t** sizes,
    std::vector<IndexRange>* childRanges,
    std::vector<vector_size_t*>* childSizes) {
  for (int32_t i = 0; i < ranges.size(); ++i) {
    int32_t begin = ranges[i].begin;
    int32_t end = begin + ranges[i].size;
    bool hasNull = false;
    for (int32_t offset = begin; offset < end; ++offset) {
      if (vector->isNullAt(offset)) {
        hasNull = true;
      } else {
        // Add the size of the length.
        *sizes[i] += sizeof(int32_t);
        childRanges->push_back(
            IndexRange{rawOffsets[offset], rawSizes[offset]});
        childSizes->push_back(sizes[i]);
      }
    }

    if (hasNull) {
      // Add the size of the null bit mask.
      *sizes[i] += bits::nbytes(ranges[i].size);
    }
  }
}

template <TypeKind Kind>
void estimateFlatSerializedSize(
    const BaseVector* vector,
    const folly::Range<const IndexRange*>& ranges,
    vector_size_t** sizes) {
  auto valueSize = vector->type()->cppSizeInBytes();
  if (vector->mayHaveNulls()) {
    auto rawNulls = vector->rawNulls();
    for (int32_t i = 0; i < ranges.size(); ++i) {
      auto end = ranges[i].begin + ranges[i].size;
      auto numValues = bits::countBits(rawNulls, ranges[i].begin, end);
      // Add the size of the values.
      *(sizes[i]) += numValues * valueSize;
      // Add the size of the null bit mask if there are nulls in the range.
      if (numValues != ranges[i].size) {
        *(sizes[i]) += bits::nbytes(ranges[i].size);
      }
    }
  } else {
    for (int32_t i = 0; i < ranges.size(); ++i) {
      // Add the size of the values (there's not bit mask since there are no
      // nulls).
      *(sizes[i]) += ranges[i].size * valueSize;
    }
  }
}

void estimateFlatSerializedSizeVarcharOrVarbinary(
    const BaseVector* vector,
    const folly::Range<const IndexRange*>& ranges,
    vector_size_t** sizes) {
  auto strings = static_cast<const FlatVector<StringView>*>(vector);
  auto rawNulls = strings->rawNulls();
  auto rawValues = strings->rawValues();
  for (int32_t i = 0; i < ranges.size(); ++i) {
    auto end = ranges[i].begin + ranges[i].size;
    int32_t numNulls = 0;
    int32_t bytes = 0;
    for (int32_t offset = ranges[i].begin; offset < end; ++offset) {
      if (rawNulls && bits::isBitNull(rawNulls, offset)) {
        ++numNulls;
      } else {
        bytes += sizeof(int32_t) + rawValues[offset].size();
      }
    }
    *(sizes[i]) += bytes + bits::nbytes(numNulls) + 4 * numNulls;
  }
}

template <>
void estimateFlatSerializedSize<TypeKind::VARCHAR>(
    const BaseVector* vector,
    const folly::Range<const IndexRange*>& ranges,
    vector_size_t** sizes) {
  estimateFlatSerializedSizeVarcharOrVarbinary(vector, ranges, sizes);
}

template <>
void estimateFlatSerializedSize<TypeKind::VARBINARY>(
    const BaseVector* vector,
    const folly::Range<const IndexRange*>& ranges,
    vector_size_t** sizes) {
  estimateFlatSerializedSizeVarcharOrVarbinary(vector, ranges, sizes);
}

void estimateBiasedSerializedSize(
    const BaseVector* vector,
    const folly::Range<const IndexRange*>& ranges,
    vector_size_t** sizes) {
  auto valueSize = vector->type()->cppSizeInBytes();
  if (vector->mayHaveNulls()) {
    auto rawNulls = vector->rawNulls();
    for (int32_t i = 0; i < ranges.size(); ++i) {
      auto end = ranges[i].begin + ranges[i].size;
      int32_t numValues = bits::countBits(rawNulls, ranges[i].begin, end);
      *(sizes[i]) += numValues * valueSize + bits::nbytes(ranges[i].size);
    }
  } else {
    for (int32_t i = 0; i < ranges.size(); ++i) {
      *(sizes[i]) += ranges[i].size * valueSize;
    }
  }
}

void estimateSerializedSizeInt(
    const BaseVector* vector,
    const folly::Range<const IndexRange*>& ranges,
    vector_size_t** sizes,
    Scratch& scratch);

void estimateWrapperSerializedSize(
    const folly::Range<const IndexRange*>& ranges,
    vector_size_t** sizes,
    const BaseVector* wrapper,
    Scratch& scratch) {
  std::vector<IndexRange> newRanges;
  std::vector<vector_size_t*> newSizes;
  const BaseVector* wrapped = wrapper->wrappedVector();
  for (int32_t i = 0; i < ranges.size(); ++i) {
    int32_t numNulls = 0;
    auto end = ranges[i].begin + ranges[i].size;
    for (int32_t offset = ranges[i].begin; offset < end; ++offset) {
      if (!wrapper->isNullAt(offset)) {
        newRanges.push_back(IndexRange{wrapper->wrappedIndex(offset), 1});
        newSizes.push_back(sizes[i]);
      } else {
        ++numNulls;
      }
    }
    *sizes[i] += bits::nbytes(numNulls);
  }
  estimateSerializedSizeInt(wrapped, newRanges, newSizes.data(), scratch);
}

template <TypeKind Kind>
void estimateFlattenedConstantSerializedSize(
    const BaseVector* vector,
    const folly::Range<const IndexRange*>& ranges,
    vector_size_t** sizes,
    Scratch& scratch) {
  VELOX_CHECK(vector->encoding() == VectorEncoding::Simple::CONSTANT);
  using T = typename KindToFlatVector<Kind>::WrapperType;
  auto constantVector = vector->as<ConstantVector<T>>();
  if (constantVector->valueVector()) {
    estimateWrapperSerializedSize(ranges, sizes, vector, scratch);
    return;
  }
  int32_t elementSize = sizeof(T);
  if (constantVector->isNullAt(0)) {
    elementSize = 1;
  } else if (std::is_same_v<T, StringView>) {
    auto value = constantVector->valueAt(0);
    auto string = reinterpret_cast<const StringView*>(&value);
    elementSize = string->size();
  }
  for (int32_t i = 0; i < ranges.size(); ++i) {
    *sizes[i] += elementSize * ranges[i].size;
  }
}

void estimateSerializedSizeInt(
    const BaseVector* vector,
    const folly::Range<const IndexRange*>& ranges,
    vector_size_t** sizes,
    Scratch& scratch) {
  switch (vector->encoding()) {
    case VectorEncoding::Simple::FLAT:
      VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(
          estimateFlatSerializedSize,
          vector->typeKind(),
          vector,
          ranges,
          sizes);
      break;
    case VectorEncoding::Simple::CONSTANT:
      VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
          estimateFlattenedConstantSerializedSize,
          vector->typeKind(),
          vector,
          ranges,
          sizes,
          scratch);
      break;
    case VectorEncoding::Simple::DICTIONARY:
    case VectorEncoding::Simple::SEQUENCE:
      estimateWrapperSerializedSize(ranges, sizes, vector, scratch);
      break;
    case VectorEncoding::Simple::BIASED:
      estimateBiasedSerializedSize(vector, ranges, sizes);
      break;
    case VectorEncoding::Simple::ROW: {
      std::vector<IndexRange> childRanges;
      std::vector<vector_size_t*> childSizes;
      for (int32_t i = 0; i < ranges.size(); ++i) {
        auto begin = ranges[i].begin;
        auto end = begin + ranges[i].size;
        for (auto offset = begin; offset < end; ++offset) {
          *sizes[i] += sizeof(int32_t);
          if (!vector->isNullAt(offset)) {
            childRanges.push_back(IndexRange{offset, 1});
            childSizes.push_back(sizes[i]);
          }
        }
      }
      auto rowVector = vector->as<RowVector>();
      auto& children = rowVector->children();
      for (auto& child : children) {
        if (child) {
          estimateSerializedSizeInt(
              child.get(),
              folly::Range(childRanges.data(), childRanges.size()),
              childSizes.data(),
              scratch);
        }
      }
      break;
    }
    case VectorEncoding::Simple::MAP: {
      auto mapVector = vector->as<MapVector>();
      std::vector<IndexRange> childRanges;
      std::vector<vector_size_t*> childSizes;
      expandRepeatedRanges(
          mapVector,
          mapVector->rawOffsets(),
          mapVector->rawSizes(),
          ranges,
          sizes,
          &childRanges,
          &childSizes);
      estimateSerializedSizeInt(
          mapVector->mapKeys().get(), childRanges, childSizes.data(), scratch);
      estimateSerializedSizeInt(
          mapVector->mapValues().get(),
          childRanges,
          childSizes.data(),
          scratch);
      break;
    }
    case VectorEncoding::Simple::ARRAY: {
      auto arrayVector = vector->as<ArrayVector>();
      std::vector<IndexRange> childRanges;
      std::vector<vector_size_t*> childSizes;
      expandRepeatedRanges(
          arrayVector,
          arrayVector->rawOffsets(),
          arrayVector->rawSizes(),
          ranges,
          sizes,
          &childRanges,
          &childSizes);
      estimateSerializedSizeInt(
          arrayVector->elements().get(),
          childRanges,
          childSizes.data(),
          scratch);
      break;
    }
    case VectorEncoding::Simple::LAZY:
      estimateSerializedSizeInt(vector->loadedVector(), ranges, sizes, scratch);
      break;
    default:
      VELOX_CHECK(false, "Unsupported vector encoding {}", vector->encoding());
  }
}

void estimateSerializedSizeInt(
    const BaseVector* vector,
    const folly::Range<const vector_size_t*>& rows,
    vector_size_t** sizes,
    Scratch& scratch);

template <TypeKind Kind>
void estimateFlatSerializedSize(
    const BaseVector* vector,
    const folly::Range<const vector_size_t*>& rows,
    vector_size_t** sizes,
    Scratch& scratch) {
  const auto valueSize = vector->type()->cppSizeInBytes();
  const auto numRows = rows.size();
  if (vector->mayHaveNulls()) {
    auto rawNulls = vector->rawNulls();
    ScratchPtr<uint64_t, 4> nullsHolder(scratch);
    ScratchPtr<int32_t, 64> nonNullsHolder(scratch);
    auto nulls = nullsHolder.get(bits::nwords(numRows));
    simd::gatherBits(rawNulls, rows, nulls);
    auto nonNulls = nonNullsHolder.get(numRows);
    const auto numNonNull = simd::indicesOfSetBits(nulls, 0, numRows, nonNulls);
    for (int32_t i = 0; i < numNonNull; ++i) {
      *sizes[nonNulls[i]] += valueSize;
    }
  } else {
    VELOX_UNREACHABLE("Non null fixed width case handled before this");
  }
}

void estimateFlatSerializedSizeVarcharOrVarbinary(
    const BaseVector* vector,
    const folly::Range<const vector_size_t*>& rows,
    vector_size_t** sizes,
    Scratch& scratch) {
  const auto numRows = rows.size();
  auto strings = static_cast<const FlatVector<StringView>*>(vector);
  auto rawNulls = strings->rawNulls();
  auto rawValues = strings->rawValues();
  if (!rawNulls) {
    for (auto i = 0; i < rows.size(); ++i) {
      *sizes[i] += rawValues[rows[i]].size();
    }
  } else {
    ScratchPtr<uint64_t, 4> nullsHolder(scratch);
    ScratchPtr<int32_t, 64> nonNullsHolder(scratch);
    auto nulls = nullsHolder.get(bits::nwords(numRows));
    simd::gatherBits(rawNulls, rows, nulls);
    auto* nonNulls = nonNullsHolder.get(numRows);
    auto numNonNull = simd::indicesOfSetBits(nulls, 0, numRows, nonNulls);

    for (int32_t i = 0; i < numNonNull; ++i) {
      *sizes[nonNulls[i]] += rawValues[rows[nonNulls[i]]].size();
    }
  }
}

template <>
void estimateFlatSerializedSize<TypeKind::VARCHAR>(
    const BaseVector* vector,
    const folly::Range<const vector_size_t*>& rows,
    vector_size_t** sizes,
    Scratch& scratch) {
  estimateFlatSerializedSizeVarcharOrVarbinary(vector, rows, sizes, scratch);
}

template <>
void estimateFlatSerializedSize<TypeKind::VARBINARY>(
    const BaseVector* vector,
    const folly::Range<const vector_size_t*>& rows,
    vector_size_t** sizes,
    Scratch& scratch) {
  estimateFlatSerializedSizeVarcharOrVarbinary(vector, rows, sizes, scratch);
}

void estimateBiasedSerializedSize(
    const BaseVector* vector,
    const folly::Range<const vector_size_t*>& rows,
    vector_size_t** sizes,
    Scratch& scratch) {
  VELOX_UNSUPPORTED();
}

void estimateWrapperSerializedSize(
    const folly::Range<const vector_size_t*>& rows,
    vector_size_t** sizes,
    const BaseVector* wrapper,
    Scratch& scratch) {
  ScratchPtr<vector_size_t, 1> innerRowsHolder(scratch);
  ScratchPtr<vector_size_t*, 1> innerSizesHolder(scratch);
  const int32_t numRows = rows.size();
  int32_t numInner = 0;
  auto innerRows = innerRowsHolder.get(numRows);
  auto innerSizes = sizes;
  const BaseVector* wrapped;
  if (wrapper->encoding() == VectorEncoding::Simple::DICTIONARY &&
      !wrapper->rawNulls()) {
    // Dictionary with no nulls.
    auto* indices = wrapper->wrapInfo()->as<vector_size_t>();
    wrapped = wrapper->valueVector().get();
    simd::transpose(indices, rows, innerRows);
    numInner = numRows;
  } else {
    wrapped = wrapper->wrappedVector();
    innerSizes = innerSizesHolder.get(numRows);
    for (int32_t i = 0; i < rows.size(); ++i) {
      if (!wrapper->isNullAt(rows[i])) {
        innerRows[numInner] = wrapper->wrappedIndex(rows[i]);
        innerSizes[numInner] = sizes[i];
        ++numInner;
      }
    }
  }
  if (numInner == 0) {
    return;
  }
  estimateSerializedSizeInt(
      wrapped,
      folly::Range<const vector_size_t*>(innerRows, numInner),
      innerSizes,
      scratch);
}

template <TypeKind Kind>
void estimateFlattenedConstantSerializedSize(
    const BaseVector* vector,
    const folly::Range<const vector_size_t*>& rows,
    vector_size_t** sizes,
    Scratch& scratch) {
  VELOX_CHECK(vector->encoding() == VectorEncoding::Simple::CONSTANT);
  using T = typename KindToFlatVector<Kind>::WrapperType;
  auto constantVector = vector->as<ConstantVector<T>>();
  int32_t elementSize = sizeof(T);
  if (constantVector->isNullAt(0)) {
    elementSize = 1;
  } else if (vector->valueVector()) {
    auto values = constantVector->wrappedVector();
    vector_size_t* sizePtr = &elementSize;
    vector_size_t singleRow = constantVector->wrappedIndex(0);
    estimateSerializedSizeInt(
        values,
        folly::Range<const vector_size_t*>(&singleRow, 1),
        &sizePtr,
        scratch);
  } else if (std::is_same_v<T, StringView>) {
    auto value = constantVector->valueAt(0);
    auto string = reinterpret_cast<const StringView*>(&value);
    elementSize = string->size();
  }
  for (int32_t i = 0; i < rows.size(); ++i) {
    *sizes[i] += elementSize;
  }
}
void estimateSerializedSizeInt(
    const BaseVector* vector,
    const folly::Range<const vector_size_t*>& rows,
    vector_size_t** sizes,
    Scratch& scratch) {
  const auto numRows = rows.size();
  if (vector->type()->isFixedWidth() && !vector->mayHaveNullsRecursive()) {
    const auto elementSize = vector->type()->cppSizeInBytes();
    for (auto i = 0; i < numRows; ++i) {
      *sizes[i] += elementSize;
    }
    return;
  }
  switch (vector->encoding()) {
    case VectorEncoding::Simple::FLAT: {
      VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
          estimateFlatSerializedSize,
          vector->typeKind(),
          vector,
          rows,
          sizes,
          scratch);
      break;
    }
    case VectorEncoding::Simple::CONSTANT:
      VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
          estimateFlattenedConstantSerializedSize,
          vector->typeKind(),
          vector,
          rows,
          sizes,
          scratch);
      break;
    case VectorEncoding::Simple::DICTIONARY:
    case VectorEncoding::Simple::SEQUENCE:
      estimateWrapperSerializedSize(rows, sizes, vector, scratch);
      break;
    case VectorEncoding::Simple::BIASED:
      estimateBiasedSerializedSize(vector, rows, sizes, scratch);
      break;
    case VectorEncoding::Simple::ROW: {
      ScratchPtr<vector_size_t, 1> innerRowsHolder(scratch);
      ScratchPtr<vector_size_t*, 1> innerSizesHolder(scratch);
      ScratchPtr<uint64_t, 1> nullsHolder(scratch);
      auto* innerRows = rows.data();
      auto* innerSizes = sizes;
      const auto numRows = rows.size();
      int32_t numInner = numRows;
      if (vector->mayHaveNulls()) {
        auto nulls = nullsHolder.get(bits::nwords(numRows));
        simd::gatherBits(vector->rawNulls(), rows, nulls);
        auto mutableInnerRows = innerRowsHolder.get(numRows);
        numInner = simd::indicesOfSetBits(nulls, 0, numRows, mutableInnerRows);
        innerSizes = innerSizesHolder.get(numInner);
        for (auto i = 0; i < numInner; ++i) {
          innerSizes[i] = sizes[mutableInnerRows[i]];
        }
        simd::transpose(
            rows.data(),
            folly::Range<const vector_size_t*>(mutableInnerRows, numInner),
            mutableInnerRows);
        innerRows = mutableInnerRows;
      }
      auto rowVector = vector->as<RowVector>();
      auto& children = rowVector->children();
      for (auto& child : children) {
        if (child) {
          estimateSerializedSizeInt(
              child.get(),
              folly::Range(innerRows, numInner),
              innerSizes,
              scratch);
        }
      }
      break;
    }
    case VectorEncoding::Simple::MAP: {
      auto mapVector = vector->asUnchecked<MapVector>();
      ScratchPtr<IndexRange> rangeHolder(scratch);
      ScratchPtr<vector_size_t*> sizesHolder(scratch);
      const auto numRanges = rowsToRanges(
          rows,
          mapVector->rawNulls(),
          mapVector->rawOffsets(),
          mapVector->rawSizes(),
          sizes,
          rangeHolder,
          &sizesHolder,
          nullptr,
          scratch);
      if (numRanges == 0) {
        return;
      }
      estimateSerializedSizeInt(
          mapVector->mapKeys().get(),
          folly::Range<const IndexRange*>(rangeHolder.get(), numRanges),
          sizesHolder.get(),
          scratch);
      estimateSerializedSizeInt(
          mapVector->mapValues().get(),
          folly::Range<const IndexRange*>(rangeHolder.get(), numRanges),
          sizesHolder.get(),
          scratch);
      break;
    }
    case VectorEncoding::Simple::ARRAY: {
      auto arrayVector = vector->as<ArrayVector>();
      ScratchPtr<IndexRange> rangeHolder(scratch);
      ScratchPtr<vector_size_t*> sizesHolder(scratch);
      const auto numRanges = rowsToRanges(
          rows,
          arrayVector->rawNulls(),
          arrayVector->rawOffsets(),
          arrayVector->rawSizes(),
          sizes,
          rangeHolder,
          &sizesHolder,
          nullptr,
          scratch);
      if (numRanges == 0) {
        return;
      }
      estimateSerializedSizeInt(
          arrayVector->elements().get(),
          folly::Range<const IndexRange*>(rangeHolder.get(), numRanges),
          sizesHolder.get(),
          scratch);
      break;
    }
    case VectorEncoding::Simple::LAZY:
      estimateSerializedSizeInt(vector->loadedVector(), rows, sizes, scratch);
      break;
    default:
      VELOX_CHECK(false, "Unsupported vector encoding {}", vector->encoding());
  }
}

int64_t flushUncompressed(
    const std::vector<std::unique_ptr<VectorStream>>& streams,
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
    stream->flush(out);
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
namespace {
// Represents sizes  of a flush. If the sizes are equal, no compression is
// applied. Otherwise 'compressedSize' must be less than 'uncompressedSize'.
struct FlushSizes {
  int64_t uncompressedSize;
  int64_t compressedSize;
};
} // namespace

void flushSerialization(
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

FlushSizes flushCompressed(
    const std::vector<std::unique_ptr<VectorStream>>& streams,
    const StreamArena& arena,
    folly::io::Codec& codec,
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
    stream->flush(&out);
  }

  const int32_t uncompressedSize = out.tellp();
  VELOX_CHECK_LE(
      uncompressedSize,
      codec.maxUncompressedLength(),
      "UncompressedSize exceeds limit");
  auto iobuf = out.getIOBuf();
  const auto compressedBuffer = codec.compress(iobuf.get());
  const int32_t compressedSize = compressedBuffer->length();
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

FlushSizes flushStreams(
    const std::vector<std::unique_ptr<VectorStream>>& streams,
    int32_t numRows,
    const StreamArena& arena,
    folly::io::Codec& codec,
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

template <TypeKind Kind>
void estimateConstantSerializedSize(
    const VectorPtr& vector,
    const folly::Range<const IndexRange*>& ranges,
    vector_size_t** sizes,
    Scratch& scratch) {
  VELOX_CHECK(vector->encoding() == VectorEncoding::Simple::CONSTANT);
  using T = typename KindToFlatVector<Kind>::WrapperType;
  auto constantVector = vector->as<ConstantVector<T>>();
  vector_size_t elementSize = 0;
  if (constantVector->isNullAt(0)) {
    // There's just a bit mask for the one null.
    elementSize = 1;
  } else if (constantVector->valueVector()) {
    std::vector<IndexRange> newRanges;
    newRanges.push_back({constantVector->index(), 1});
    auto* elementSizePtr = &elementSize;
    // In PrestoBatchVectorSerializer we don't preserve the encodings for the
    // valueVector for a ConstantVector.
    estimateSerializedSizeInt(
        constantVector->valueVector().get(),
        newRanges,
        &elementSizePtr,
        scratch);
  } else if (std::is_same_v<T, StringView>) {
    auto value = constantVector->valueAt(0);
    auto string = reinterpret_cast<const StringView*>(&value);
    elementSize = string->size();
  } else {
    elementSize = sizeof(T);
  }

  for (int32_t i = 0; i < ranges.size(); ++i) {
    *sizes[i] += elementSize;
  }
}

template <TypeKind Kind>
void estimateDictionarySerializedSize(
    const VectorPtr& vector,
    const folly::Range<const IndexRange*>& ranges,
    vector_size_t** sizes,
    Scratch& scratch) {
  VELOX_CHECK(vector->encoding() == VectorEncoding::Simple::DICTIONARY);
  using T = typename KindToFlatVector<Kind>::WrapperType;
  auto dictionaryVector = vector->as<DictionaryVector<T>>();

  // We don't currently support serializing DictionaryVectors with nulls, so use
  // the flattened size.
  if (dictionaryVector->nulls()) {
    estimateWrapperSerializedSize(ranges, sizes, vector.get(), scratch);
    return;
  }

  // This will ultimately get passed to simd::transpose, so it needs to be a
  // raw_vector.
  raw_vector<vector_size_t> childIndices;
  std::vector<vector_size_t*> childSizes;
  for (int rangeIndex = 0; rangeIndex < ranges.size(); rangeIndex++) {
    ScratchPtr<vector_size_t, 64> selectedIndicesHolder(scratch);
    auto* mutableSelectedIndices =
        selectedIndicesHolder.get(dictionaryVector->valueVector()->size());
    auto numUsed = computeSelectedIndices(
        dictionaryVector,
        ranges.subpiece(rangeIndex, 1),
        scratch,
        mutableSelectedIndices);
    for (int i = 0; i < numUsed; i++) {
      childIndices.push_back(mutableSelectedIndices[i]);
      childSizes.push_back(sizes[rangeIndex]);
    }

    // Add the size of the indices.
    *sizes[rangeIndex] += sizeof(int32_t) * ranges[rangeIndex].size;
  }

  // In PrestoBatchVectorSerializer we don't preserve the encodings for the
  // valueVector for a DictionaryVector.
  estimateSerializedSizeInt(
      dictionaryVector->valueVector().get(),
      childIndices,
      childSizes.data(),
      scratch);
}

class PrestoBatchVectorSerializer : public BatchVectorSerializer {
 public:
  PrestoBatchVectorSerializer(memory::MemoryPool* pool, const SerdeOpts& opts)
      : pool_(pool),
        codec_(common::compressionKindToCodec(opts.compressionKind)),
        opts_(opts) {}

  void serialize(
      const RowVectorPtr& vector,
      const folly::Range<const IndexRange*>& ranges,
      Scratch& scratch,
      OutputStream* stream) override {
    const auto numRows = rangesTotalSize(ranges);
    const auto rowType = vector->type();
    const auto numChildren = vector->childrenSize();

    StreamArena arena(pool_);
    std::vector<std::unique_ptr<VectorStream>> streams(numChildren);
    for (int i = 0; i < numChildren; i++) {
      streams[i] = std::make_unique<VectorStream>(
          rowType->childAt(i),
          std::nullopt,
          vector->childAt(i),
          &arena,
          numRows,
          opts_);

      if (numRows > 0) {
        serializeColumn(vector->childAt(i), ranges, streams[i].get(), scratch);
      }
    }

    flushStreams(
        streams, numRows, arena, *codec_, opts_.minCompressionRatio, stream);
  }

  void estimateSerializedSize(
      VectorPtr vector,
      const folly::Range<const IndexRange*>& ranges,
      vector_size_t** sizes,
      Scratch& scratch) override {
    estimateSerializedSizeImpl(vector, ranges, sizes, scratch);
  }

 private:
  void estimateSerializedSizeImpl(
      const VectorPtr& vector,
      const folly::Range<const IndexRange*>& ranges,
      vector_size_t** sizes,
      Scratch& scratch) {
    switch (vector->encoding()) {
      case VectorEncoding::Simple::FLAT:
        VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(
            estimateFlatSerializedSize,
            vector->typeKind(),
            vector.get(),
            ranges,
            sizes);
        break;
      case VectorEncoding::Simple::CONSTANT:
        VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
            estimateConstantSerializedSize,
            vector->typeKind(),
            vector,
            ranges,
            sizes,
            scratch);
        break;
      case VectorEncoding::Simple::DICTIONARY:
        VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
            estimateDictionarySerializedSize,
            vector->typeKind(),
            vector,
            ranges,
            sizes,
            scratch);
        break;
      case VectorEncoding::Simple::ROW: {
        if (!vector->mayHaveNulls()) {
          // Add the size of the offsets in the Row encoding.
          for (int32_t i = 0; i < ranges.size(); ++i) {
            *sizes[i] += ranges[i].size * sizeof(int32_t);
          }

          auto rowVector = vector->as<RowVector>();
          auto& children = rowVector->children();
          for (auto& child : children) {
            if (child) {
              estimateSerializedSizeImpl(child, ranges, sizes, scratch);
            }
          }

          break;
        }

        std::vector<IndexRange> childRanges;
        std::vector<vector_size_t*> childSizes;
        for (int32_t i = 0; i < ranges.size(); ++i) {
          // Add the size of the nulls bit mask.
          *sizes[i] += bits::nbytes(ranges[i].size);

          auto begin = ranges[i].begin;
          auto end = begin + ranges[i].size;
          for (auto offset = begin; offset < end; ++offset) {
            // Add the size of the offset.
            *sizes[i] += sizeof(int32_t);
            if (!vector->isNullAt(offset)) {
              childRanges.push_back(IndexRange{offset, 1});
              childSizes.push_back(sizes[i]);
            }
          }
        }

        auto rowVector = vector->as<RowVector>();
        auto& children = rowVector->children();
        for (auto& child : children) {
          if (child) {
            estimateSerializedSizeImpl(
                child,
                folly::Range(childRanges.data(), childRanges.size()),
                childSizes.data(),
                scratch);
          }
        }

        break;
      }
      case VectorEncoding::Simple::MAP: {
        auto mapVector = vector->as<MapVector>();
        std::vector<IndexRange> childRanges;
        std::vector<vector_size_t*> childSizes;
        expandRepeatedRanges(
            mapVector,
            mapVector->rawOffsets(),
            mapVector->rawSizes(),
            ranges,
            sizes,
            &childRanges,
            &childSizes);
        estimateSerializedSizeImpl(
            mapVector->mapKeys(), childRanges, childSizes.data(), scratch);
        estimateSerializedSizeImpl(
            mapVector->mapValues(), childRanges, childSizes.data(), scratch);
        break;
      }
      case VectorEncoding::Simple::ARRAY: {
        auto arrayVector = vector->as<ArrayVector>();
        std::vector<IndexRange> childRanges;
        std::vector<vector_size_t*> childSizes;
        expandRepeatedRanges(
            arrayVector,
            arrayVector->rawOffsets(),
            arrayVector->rawSizes(),
            ranges,
            sizes,
            &childRanges,
            &childSizes);
        estimateSerializedSizeImpl(
            arrayVector->elements(), childRanges, childSizes.data(), scratch);
        break;
      }
      case VectorEncoding::Simple::LAZY:
        estimateSerializedSizeImpl(
            vector->as<LazyVector>()->loadedVectorShared(),
            ranges,
            sizes,
            scratch);
        break;
      default:
        VELOX_CHECK(
            false, "Unsupported vector encoding {}", vector->encoding());
    }
  }

  memory::MemoryPool* pool_;
  const std::unique_ptr<folly::io::Codec> codec_;
  SerdeOpts opts_;
};

class PrestoIterativeVectorSerializer : public IterativeVectorSerializer {
 public:
  PrestoIterativeVectorSerializer(
      const RowTypePtr& rowType,
      int32_t numRows,
      StreamArena* streamArena,
      const SerdeOpts& opts)
      : opts_(opts),
        streamArena_(streamArena),
        codec_(common::compressionKindToCodec(opts.compressionKind)) {
    const auto types = rowType->children();
    const auto numTypes = types.size();
    streams_.resize(numTypes);

    for (int i = 0; i < numTypes; ++i) {
      streams_[i] = std::make_unique<VectorStream>(
          types[i], std::nullopt, std::nullopt, streamArena, numRows, opts);
    }
  }

  void append(
      const RowVectorPtr& vector,
      const folly::Range<const IndexRange*>& ranges,
      Scratch& scratch) override {
    const auto numNewRows = rangesTotalSize(ranges);
    if (numNewRows == 0) {
      return;
    }
    numRows_ += numNewRows;
    for (int32_t i = 0; i < vector->childrenSize(); ++i) {
      serializeColumn(vector->childAt(i), ranges, streams_[i].get(), scratch);
    }
  }

  void append(
      const RowVectorPtr& vector,
      const folly::Range<const vector_size_t*>& rows,
      Scratch& scratch) override {
    const auto numNewRows = rows.size();
    if (numNewRows == 0) {
      return;
    }
    numRows_ += numNewRows;
    for (int32_t i = 0; i < vector->childrenSize(); ++i) {
      serializeColumn(vector->childAt(i), rows, streams_[i].get(), scratch);
    }
  }

  size_t maxSerializedSize() const override {
    size_t dataSize = 4; // streams_.size()
    for (auto& stream : streams_) {
      dataSize += stream->serializedSize();
    }

    auto compressedSize = needCompression(*codec_)
        ? codec_->maxCompressedLength(dataSize)
        : dataSize;
    return kHeaderSize + compressedSize;
  }

  // The SerializedPage layout is:
  // numRows(4) | codec(1) | uncompressedSize(4) | compressedSize(4) |
  // checksum(8) | data
  void flush(OutputStream* out) override {
    constexpr int32_t kMaxCompressionAttemptsToSkip = 30;
    if (!needCompression(*codec_)) {
      flushStreams(
          streams_,
          numRows_,
          *streamArena_,
          *codec_,
          opts_.minCompressionRatio,
          out);
    } else {
      if (numCompressionToSkip_ > 0) {
        const auto noCompressionCodec = common::compressionKindToCodec(
            common::CompressionKind::CompressionKind_NONE);
        auto [size, ignore] = flushStreams(
            streams_, numRows_, *streamArena_, *noCompressionCodec, 1, out);
        stats_.compressionSkippedBytes += size;
        --numCompressionToSkip_;
        ++stats_.numCompressionSkipped;
      } else {
        auto [size, compressedSize] = flushStreams(
            streams_,
            numRows_,
            *streamArena_,
            *codec_,
            opts_.minCompressionRatio,
            out);
        stats_.compressionInputBytes += size;
        stats_.compressedBytes += compressedSize;
        if (compressedSize > size * opts_.minCompressionRatio) {
          numCompressionToSkip_ = std::min<int64_t>(
              kMaxCompressionAttemptsToSkip, 1 + stats_.numCompressionSkipped);
        }
      }
    }
  }

  std::unordered_map<std::string, RuntimeCounter> runtimeStats() override {
    std::unordered_map<std::string, RuntimeCounter> map;
    map.insert(
        {{"compressedBytes",
          RuntimeCounter(stats_.compressedBytes, RuntimeCounter::Unit::kBytes)},
         {"compressionInputBytes",
          RuntimeCounter(
              stats_.compressionInputBytes, RuntimeCounter::Unit::kBytes)},
         {"compressionSkippedBytes",
          RuntimeCounter(
              stats_.compressionSkippedBytes, RuntimeCounter::Unit::kBytes)}});
    return map;
  }

  void clear() override {
    numRows_ = 0;
    for (auto& stream : streams_) {
      stream->clear();
    }
  }

 private:
  struct CompressionStats {
    // Number of times compression was not attempted.
    int32_t numCompressionSkipped{0};

    // uncompressed size for which compression was attempted.
    int64_t compressionInputBytes{0};

    // Compressed bytes.
    int64_t compressedBytes{0};

    // Bytes for which compression was not attempted because of past
    // non-performance.
    int64_t compressionSkippedBytes{0};
  };

  const SerdeOpts opts_;
  StreamArena* const streamArena_;
  const std::unique_ptr<folly::io::Codec> codec_;

  int32_t numRows_{0};
  std::vector<std::unique_ptr<VectorStream>> streams_;

  // Count of forthcoming compressions to skip.
  int32_t numCompressionToSkip_{0};
  CompressionStats stats_;
};
} // namespace

void PrestoVectorSerde::estimateSerializedSize(
    const BaseVector* vector,
    const folly::Range<const IndexRange*>& ranges,
    vector_size_t** sizes,
    Scratch& scratch) {
  estimateSerializedSizeInt(vector->loadedVector(), ranges, sizes, scratch);
}

void PrestoVectorSerde::estimateSerializedSize(
    const BaseVector* vector,
    const folly::Range<const vector_size_t*> rows,
    vector_size_t** sizes,
    Scratch& scratch) {
  estimateSerializedSizeInt(vector->loadedVector(), rows, sizes, scratch);
}

std::unique_ptr<IterativeVectorSerializer>
PrestoVectorSerde::createIterativeSerializer(
    RowTypePtr type,
    int32_t numRows,
    StreamArena* streamArena,
    const Options* options) {
  const auto prestoOptions = toPrestoOptions(options);
  return std::make_unique<PrestoIterativeVectorSerializer>(
      type, numRows, streamArena, prestoOptions);
}

std::unique_ptr<BatchVectorSerializer> PrestoVectorSerde::createBatchSerializer(
    memory::MemoryPool* pool,
    const Options* options) {
  const auto prestoOptions = toPrestoOptions(options);
  return std::make_unique<PrestoBatchVectorSerializer>(pool, prestoOptions);
}

namespace {
bool hasNestedStructs(const TypePtr& type) {
  if (type->isRow()) {
    return true;
  }
  if (type->isArray()) {
    return hasNestedStructs(type->childAt(0));
  }
  if (type->isMap()) {
    return hasNestedStructs(type->childAt(0)) ||
        hasNestedStructs(type->childAt(1));
  }
  return false;
}

bool hasNestedStructs(const std::vector<TypePtr>& types) {
  for (auto& child : types) {
    if (hasNestedStructs(child)) {
      return true;
    }
  }
  return false;
}

void readTopColumns(
    ByteInputStream& source,
    const RowTypePtr& type,
    velox::memory::MemoryPool* pool,
    const RowVectorPtr& result,
    int32_t resultOffset,
    const SerdeOpts& opts,
    bool singleColumn = false) {
  int32_t numColumns = 1;
  if (!singleColumn) {
    numColumns = source.read<int32_t>();
  }
  auto& children = result->children();
  const auto& childTypes = type->asRow().children();
  // Bug for bug compatibility: Extra columns at the end are allowed for
  // non-compressed data.
  if (opts.compressionKind == common::CompressionKind_NONE) {
    VELOX_USER_CHECK_GE(
        numColumns,
        type->size(),
        "Number of columns in serialized data doesn't match "
        "number of columns requested for deserialization");
  } else {
    VELOX_USER_CHECK_EQ(
        numColumns,
        type->size(),
        "Number of columns in serialized data doesn't match "
        "number of columns requested for deserialization");
  }

  auto guard = folly::makeGuard([&]() { structNullsMap().reset(); });

  if (!opts.nullsFirst && hasNestedStructs(childTypes)) {
    structNullsMap() = std::make_unique<StructNullsMap>();
    Scratch scratch;
    auto position = source.tellp();
    readStructNullsColumns(
        &source, childTypes, opts.useLosslessTimestamp, scratch);
    source.seekp(position);
  }
  readColumns(
      &source, childTypes, resultOffset, nullptr, 0, pool, opts, children);
}
} // namespace

void PrestoVectorSerde::deserialize(
    ByteInputStream* source,
    velox::memory::MemoryPool* pool,
    RowTypePtr type,
    RowVectorPtr* result,
    vector_size_t resultOffset,
    const Options* options) {
  const auto prestoOptions = toPrestoOptions(options);
  const auto codec =
      common::compressionKindToCodec(prestoOptions.compressionKind);
  auto const header = PrestoHeader::read(source);

  int64_t actualCheckSum = 0;
  if (isChecksumBitSet(header.pageCodecMarker)) {
    actualCheckSum = computeChecksum(
        source,
        header.pageCodecMarker,
        header.numRows,
        header.uncompressedSize,
        header.compressedSize);
  }

  VELOX_CHECK_EQ(
      header.checksum, actualCheckSum, "Received corrupted serialized page.");

  if (resultOffset > 0) {
    VELOX_CHECK_NOT_NULL(*result);
    VELOX_CHECK(result->unique());
    (*result)->resize(resultOffset + header.numRows);
  } else if (*result && result->unique()) {
    VELOX_CHECK(
        *(*result)->type() == *type,
        "Unexpected type: {} vs. {}",
        (*result)->type()->toString(),
        type->toString());
    (*result)->prepareForReuse();
    (*result)->resize(header.numRows);
  } else {
    *result = BaseVector::create<RowVector>(type, header.numRows, pool);
  }

  VELOX_CHECK_EQ(
      header.checksum, actualCheckSum, "Received corrupted serialized page.");

  if (!isCompressedBitSet(header.pageCodecMarker)) {
    readTopColumns(*source, type, pool, *result, resultOffset, prestoOptions);
  } else {
    auto compressBuf = folly::IOBuf::create(header.compressedSize);
    source->readBytes(compressBuf->writableData(), header.compressedSize);
    compressBuf->append(header.compressedSize);
    auto uncompress =
        codec->uncompress(compressBuf.get(), header.uncompressedSize);
    ByteRange byteRange{
        uncompress->writableData(), (int32_t)uncompress->length(), 0};
    auto uncompressedSource =
        std::make_unique<BufferInputStream>(std::vector<ByteRange>{byteRange});
    readTopColumns(
        *uncompressedSource, type, pool, *result, resultOffset, prestoOptions);
  }
}

void PrestoVectorSerde::deserializeSingleColumn(
    ByteInputStream* source,
    velox::memory::MemoryPool* pool,
    TypePtr type,
    VectorPtr* result,
    const Options* options) {
  const auto prestoOptions = toPrestoOptions(options);
  VELOX_CHECK_EQ(
      prestoOptions.compressionKind,
      common::CompressionKind::CompressionKind_NONE);
  const bool useLosslessTimestamp = prestoOptions.useLosslessTimestamp;

  if (*result && result->unique()) {
    VELOX_CHECK(
        *(*result)->type() == *type,
        "Unexpected type: {} vs. {}",
        (*result)->type()->toString(),
        type->toString());
    (*result)->prepareForReuse();
  } else {
    *result = BaseVector::create(type, 0, pool);
  }

  auto rowType = ROW({"c0"}, {type});
  auto row = std::make_shared<RowVector>(
      pool, rowType, BufferPtr(nullptr), 0, std::vector<VectorPtr>{*result});
  readTopColumns(*source, rowType, pool, row, 0, prestoOptions, true);
  *result = row->childAt(0);
}

// static
void PrestoVectorSerde::registerVectorSerde() {
  auto toByte = [](int32_t number, int32_t bit) {
    return static_cast<uint64_t>(bits::isBitSet(&number, bit)) << (bit * 8);
  };
  for (auto i = 0; i < 256; ++i) {
    bitsToBytesMap[i] = toByte(i, 0) | toByte(i, 1) | toByte(i, 2) |
        toByte(i, 3) | toByte(i, 4) | toByte(i, 5) | toByte(i, 6) |
        toByte(i, 7);
  }
  velox::registerVectorSerde(std::make_unique<PrestoVectorSerde>());
}

namespace {
class PrestoVectorLexer {
 public:
  using Token = PrestoVectorSerde::Token;
  using TokenType = PrestoVectorSerde::TokenType;

  explicit PrestoVectorLexer(std::string_view source)
      : source_(source), committedPtr_(source.begin()) {}

  Status lex(std::vector<Token>& out) && {
    VELOX_RETURN_NOT_OK(lexHeader());

    int32_t numColumns;
    VELOX_RETURN_NOT_OK(lexInt(TokenType::NUM_COLUMNS, &numColumns));

    for (int32_t col = 0; col < numColumns; ++col) {
      VELOX_RETURN_NOT_OK(lexColumn());
    }

    VELOX_RETURN_IF(
        !source_.empty(), Status::Invalid("Source not fully consumed"));

    out = std::move(tokens_);
    return Status::OK();
  }

 private:
  Status lexHeader() {
    assertCommitted();

    const auto header = PrestoHeader::read(&source_);
    VELOX_RETURN_IF(
        !header.has_value(), Status::Invalid("PrestoPage header invalid"));
    VELOX_RETURN_IF(
        isCompressedBitSet(header->pageCodecMarker),
        Status::Invalid("Compression is not supported"));
    VELOX_RETURN_IF(
        isEncryptedBitSet(header->pageCodecMarker),
        Status::Invalid("Encryption is not supported"));
    VELOX_RETURN_IF(
        header->uncompressedSize != header->compressedSize,
        Status::Invalid(
            "Compressed size must match uncompressed size: {} != {}",
            header->uncompressedSize,
            header->compressedSize));
    VELOX_RETURN_IF(
        header->uncompressedSize != source_.size(),
        Status::Invalid(
            "Uncompressed size does not match content size: {} != {}",
            header->uncompressedSize,
            source_.size()));

    commit(TokenType::HEADER);

    return Status::OK();
  }

  Status lexColumEncoding(std::string& out) {
    assertCommitted();
    // Don't use readLengthPrefixedString because it doesn't validate the length
    int32_t encodingLength;
    VELOX_RETURN_NOT_OK(lexInt(TokenType::COLUMN_ENCODING, &encodingLength));
    // Control encoding length to avoid large allocations
    VELOX_RETURN_IF(
        encodingLength < 0 || encodingLength > 100,
        Status::Invalid("Invalid column encoding length: {}", encodingLength));

    std::string encoding;
    encoding.resize(encodingLength);
    VELOX_RETURN_NOT_OK(
        lexBytes(encodingLength, TokenType::COLUMN_ENCODING, encoding.data()));

    out = std::move(encoding);

    return Status::OK();
  }

  Status lexColumn() {
    std::string encoding;
    VELOX_RETURN_NOT_OK(lexColumEncoding(encoding));

    if (encoding == kByteArray) {
      VELOX_RETURN_NOT_OK(lexFixedArray<int8_t>(TokenType::BYTE_ARRAY));
    } else if (encoding == kShortArray) {
      VELOX_RETURN_NOT_OK(lexFixedArray<int16_t>(TokenType::SHORT_ARRAY));
    } else if (encoding == kIntArray) {
      VELOX_RETURN_NOT_OK(lexFixedArray<int32_t>(TokenType::INT_ARRAY));
    } else if (encoding == kLongArray) {
      VELOX_RETURN_NOT_OK(lexFixedArray<int64_t>(TokenType::LONG_ARRAY));
    } else if (encoding == kInt128Array) {
      VELOX_RETURN_NOT_OK(lexFixedArray<int128_t>(TokenType::INT128_ARRAY));
    } else if (encoding == kVariableWidth) {
      VELOX_RETURN_NOT_OK(lexVariableWidth());
    } else if (encoding == kArray) {
      VELOX_RETURN_NOT_OK(lexArray());
    } else if (encoding == kMap) {
      VELOX_RETURN_NOT_OK(lexMap());
    } else if (encoding == kRow) {
      VELOX_RETURN_NOT_OK(lexRow());
    } else if (encoding == kDictionary) {
      VELOX_RETURN_NOT_OK(lexDictionary());
    } else if (encoding == kRLE) {
      VELOX_RETURN_NOT_OK(lexRLE());
    } else {
      return Status::Invalid("Unknown encoding: {}", encoding);
    }

    return Status::OK();
  }

  template <typename T>
  Status lexFixedArray(TokenType tokenType) {
    int32_t numRows;
    VELOX_RETURN_NOT_OK(lexInt(TokenType::NUM_ROWS, &numRows));
    VELOX_RETURN_NOT_OK(lexNulls(numRows));
    const auto numBytes = numRows * sizeof(T);
    VELOX_RETURN_NOT_OK(lexBytes(numBytes, tokenType));
    return Status::OK();
  }

  Status lexVariableWidth() {
    int32_t numRows;
    VELOX_RETURN_NOT_OK(lexInt(TokenType::NUM_ROWS, &numRows));
    const auto numOffsetBytes = numRows * sizeof(int32_t);
    VELOX_RETURN_NOT_OK(lexBytes(numOffsetBytes, TokenType::OFFSETS));
    VELOX_RETURN_NOT_OK(lexNulls(numRows));
    int32_t dataBytes;
    VELOX_RETURN_NOT_OK(
        lexInt(TokenType::VARIABLE_WIDTH_DATA_SIZE, &dataBytes));
    VELOX_RETURN_NOT_OK(lexBytes(dataBytes, TokenType::VARIABLE_WIDTH_DATA));
    return Status::OK();
  }

  Status lexArray() {
    VELOX_RETURN_NOT_OK(lexColumn());
    int32_t numRows;
    VELOX_RETURN_NOT_OK(lexInt(TokenType::NUM_ROWS, &numRows));
    const auto offsetBytes = (numRows + 1) * sizeof(int32_t);
    VELOX_RETURN_NOT_OK(lexBytes(offsetBytes, TokenType::OFFSETS));
    VELOX_RETURN_NOT_OK(lexNulls(numRows));
    return Status::OK();
  }

  Status lexMap() {
    // Key column
    VELOX_RETURN_NOT_OK(lexColumn());
    // Value column
    VELOX_RETURN_NOT_OK(lexColumn());
    int32_t hashTableBytes;
    VELOX_RETURN_NOT_OK(lexInt(TokenType::HASH_TABLE_SIZE, &hashTableBytes));
    if (hashTableBytes != -1) {
      VELOX_RETURN_NOT_OK(lexBytes(hashTableBytes, TokenType::HASH_TABLE));
    }
    int32_t numRows;
    VELOX_RETURN_NOT_OK(lexInt(TokenType::NUM_ROWS, &numRows));
    const auto offsetBytes = (numRows + 1) * sizeof(int32_t);
    VELOX_RETURN_NOT_OK(lexBytes(offsetBytes, TokenType::OFFSETS));
    VELOX_RETURN_NOT_OK(lexNulls(numRows));
    return Status::OK();
  }

  Status lexRow() {
    int32_t numFields;
    VELOX_RETURN_NOT_OK(lexInt(TokenType::NUM_FIELDS, &numFields));
    for (int32_t field = 0; field < numFields; ++field) {
      VELOX_RETURN_NOT_OK(lexColumn());
    }
    int32_t numRows;
    VELOX_RETURN_NOT_OK(lexInt(TokenType::NUM_ROWS, &numRows));
    const auto offsetBytes = (numRows + 1) * sizeof(int32_t);
    VELOX_RETURN_NOT_OK(lexBytes(offsetBytes, TokenType::OFFSETS));
    VELOX_RETURN_NOT_OK(lexNulls(numRows));

    return Status::OK();
  }

  Status lexDictionary() {
    int32_t numRows;
    VELOX_RETURN_NOT_OK(lexInt(TokenType::NUM_ROWS, &numRows));
    // Dictionary column
    VELOX_RETURN_NOT_OK(lexColumn());
    const auto indicesBytes = numRows * sizeof(int32_t);
    VELOX_RETURN_NOT_OK(lexBytes(indicesBytes, TokenType::DICTIONARY_INDICES));
    // Dictionary ID
    VELOX_RETURN_NOT_OK(lexBytes(24, TokenType::DICTIONARY_ID));
    return Status::OK();
  }

  Status lexRLE() {
    // Num rows
    VELOX_RETURN_NOT_OK(lexInt<int32_t>(TokenType::NUM_ROWS));
    // RLE length one column
    VELOX_RETURN_NOT_OK(lexColumn());
    return Status::OK();
  }

  Status lexNulls(int32_t& numRows) {
    assertCommitted();
    VELOX_RETURN_IF(
        numRows < 0, Status::Invalid("Negative num rows: {}", numRows));

    int8_t hasNulls;
    VELOX_RETURN_NOT_OK(lexInt(TokenType::NULLS, &hasNulls));
    if (hasNulls != 0) {
      const auto numBytes = bits::nbytes(numRows);
      VELOX_RETURN_IF(
          numBytes > source_.size(),
          Status::Invalid(
              "More rows than bytes in source: {} > {}",
              numRows,
              source_.size()));
      if (nullsBuffer_.size() < numBytes) {
        constexpr auto eltBytes = sizeof(nullsBuffer_[0]);
        nullsBuffer_.resize(bits::roundUp(numBytes, eltBytes) / eltBytes);
      }
      auto* nulls = nullsBuffer_.data();
      VELOX_RETURN_NOT_OK(
          lexBytes(numBytes, TokenType::NULLS, reinterpret_cast<char*>(nulls)));

      bits::reverseBits(reinterpret_cast<uint8_t*>(nulls), numBytes);
      const auto numNulls = bits::countBits(nulls, 0, numRows);

      numRows -= numNulls;
    }
    commit(TokenType::NULLS);
    return Status::OK();
  }

  Status lexBytes(int32_t numBytes, TokenType tokenType, char* dst = nullptr) {
    assertCommitted();
    VELOX_RETURN_IF(
        numBytes < 0,
        Status::Invalid("Attempting to read negative numBytes: {}", numBytes));
    VELOX_RETURN_IF(
        numBytes > source_.size(),
        Status::Invalid(
            "Attempting to read more bytes than in source: {} > {}",
            numBytes,
            source_.size()));
    if (dst != nullptr) {
      std::copy(source_.begin(), source_.begin() + numBytes, dst);
    }
    source_.remove_prefix(numBytes);
    commit(tokenType);
    return Status::OK();
  }

  template <typename T>
  Status lexInt(TokenType tokenType, T* out = nullptr) {
    assertCommitted();
    VELOX_RETURN_IF(
        source_.size() < sizeof(T),
        Status::Invalid(
            "Source size less than int size: {} < {}",
            source_.size(),
            sizeof(T)));
    const auto value = readInt<T>(&source_);
    if (out != nullptr) {
      *out = value;
    }
    commit(tokenType);
    return Status::OK();
  }

  void assertCommitted() const {
    assert(committedPtr_ == source_.begin());
  }

  void commit(TokenType tokenType) {
    const auto newPtr = source_.begin();
    assert(committedPtr_ <= newPtr);
    assert(
        int64_t(newPtr - committedPtr_) <=
        int64_t(std::numeric_limits<uint32_t>::max()));
    if (newPtr != committedPtr_) {
      const uint32_t length = uint32_t(newPtr - committedPtr_);
      if (!tokens_.empty() && tokens_.back().tokenType == tokenType) {
        tokens_.back().length += length;
      } else {
        PrestoVectorSerde::Token token;
        token.tokenType = tokenType;
        token.length = length;
        tokens_.push_back(token);
      }
    }
    committedPtr_ = newPtr;
  }

  std::string_view source_;
  const char* committedPtr_;
  std::vector<uint64_t> nullsBuffer_;
  std::vector<Token> tokens_;
};
} // namespace

/* static */ Status PrestoVectorSerde::lex(
    std::string_view source,
    std::vector<Token>& out,
    const Options* options) {
  const auto prestoOptions = toPrestoOptions(options);

  VELOX_RETURN_IF(
      prestoOptions.useLosslessTimestamp,
      Status::Invalid(
          "Lossless timestamps are not supported, because they cannot be decoded without the Schema"));
  VELOX_RETURN_IF(
      prestoOptions.compressionKind !=
          common::CompressionKind::CompressionKind_NONE,
      Status::Invalid("Compression is not supported"));
  VELOX_RETURN_IF(
      prestoOptions.nullsFirst,
      Status::Invalid(
          "Nulls first encoding is not currently supported, but support can be added if needed"));

  return PrestoVectorLexer(source).lex(out);
}

} // namespace facebook::velox::serializer::presto

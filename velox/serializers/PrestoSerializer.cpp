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
#include "velox/common/base/Crc.h"
#include "velox/common/base/RawVector.h"
#include "velox/common/memory/ByteStream.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "velox/vector/BiasVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DictionaryVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/VectorTypeUtils.h"

namespace facebook::velox::serializer::presto {
namespace {
constexpr int8_t kCompressedBitMask = 1;
constexpr int8_t kEncryptedBitMask = 2;
constexpr int8_t kCheckSumBitMask = 4;
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
    int uncompressedSize) {
  auto offset = source->tellp();
  bits::Crc32 crc32;
  if (FOLLY_UNLIKELY(source->remainingSize() < uncompressedSize)) {
    VELOX_FAIL(
        "Tried to read {} bytes, larger than what's remained in source {} "
        "bytes. Source details: {}",
        uncompressedSize,
        source->remainingSize(),
        source->toString());
  }
  auto remainingBytes = uncompressedSize;
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

bool isChecksumBitSet(int8_t codec) {
  return (codec & kCheckSumBitMask) == kCheckSumBitMask;
}

std::string_view typeToEncodingName(const TypePtr& type) {
  switch (type->kind()) {
    case TypeKind::BOOLEAN:
      return "BYTE_ARRAY";
    case TypeKind::TINYINT:
      return "BYTE_ARRAY";
    case TypeKind::SMALLINT:
      return "SHORT_ARRAY";
    case TypeKind::INTEGER:
      return "INT_ARRAY";
    case TypeKind::BIGINT:
      return "LONG_ARRAY";
    case TypeKind::HUGEINT:
      return "INT128_ARRAY";
    case TypeKind::REAL:
      return "INT_ARRAY";
    case TypeKind::DOUBLE:
      return "LONG_ARRAY";
    case TypeKind::VARCHAR:
      return "VARIABLE_WIDTH";
    case TypeKind::VARBINARY:
      return "VARIABLE_WIDTH";
    case TypeKind::TIMESTAMP:
      return "LONG_ARRAY";
    case TypeKind::ARRAY:
      return "ARRAY";
    case TypeKind::MAP:
      return "MAP";
    case TypeKind::ROW:
      return isTimestampWithTimeZoneType(type) ? "LONG_ARRAY" : "ROW";
    case TypeKind::UNKNOWN:
      return "BYTE_ARRAY";
    default:
      VELOX_FAIL("Unknown type kind: {}", static_cast<int>(type->kind()));
  }
}

PrestoVectorSerde::PrestoOptions toPrestoOptions(
    const VectorSerde::Options* options) {
  if (options == nullptr) {
    return PrestoVectorSerde::PrestoOptions();
  }
  return *(static_cast<const PrestoVectorSerde::PrestoOptions*>(options));
}

FOLLY_ALWAYS_INLINE bool needCompression(const folly::io::Codec& codec) {
  return codec.type() != folly::io::CodecType::NO_COMPRESSION;
}

template <typename T>
void readValues(
    ByteInputStream* source,
    vector_size_t size,
    vector_size_t offset,
    BufferPtr nulls,
    vector_size_t nullCount,
    BufferPtr values) {
  if (nullCount) {
    auto rawValues = values->asMutable<T>();
    int32_t toClear = offset;
    bits::forEachSetBit(
        nulls->as<uint64_t>(), offset, offset + size, [&](int32_t row) {
          // Set the values between the last non-null and this to type default.
          for (; toClear < row; ++toClear) {
            rawValues[toClear] = T();
          }
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
    BufferPtr nulls,
    vector_size_t nullCount,
    BufferPtr values) {
  auto rawValues = values->asMutable<uint64_t>();
  if (nullCount) {
    int32_t toClear = offset;
    bits::forEachSetBit(
        nulls->as<uint64_t>(), offset, offset + size, [&](int32_t row) {
          // Set the values between the last non-null and this to type default.
          for (; toClear < row; ++toClear) {
            bits::clearBit(rawValues, toClear);
          }
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
    BufferPtr nulls,
    vector_size_t nullCount,
    BufferPtr values) {
  auto rawValues = values->asMutable<Timestamp>();
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
    BufferPtr nulls,
    vector_size_t nullCount,
    BufferPtr values) {
  auto rawValues = values->asMutable<Timestamp>();
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
    BufferPtr nulls,
    vector_size_t nullCount,
    BufferPtr values) {
  auto rawValues = values->asMutable<int128_t>();
  if (nullCount) {
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

vector_size_t readNulls(
    ByteInputStream* source,
    vector_size_t size,
    BaseVector& result,
    vector_size_t resultOffset) {
  if (source->readByte() == 0) {
    result.clearNulls(resultOffset, resultOffset + size);
    return 0;
  }

  const bool noPriorNulls = (result.rawNulls() == nullptr);

  // Allocate one extra byte in case we cannot use bits from the current last
  // partial byte.
  BufferPtr& nulls = result.mutableNulls(resultOffset + size + 8);
  if (noPriorNulls) {
    bits::fillBits(
        nulls->asMutable<uint64_t>(), 0, resultOffset, bits::kNotNull);
  }

  auto* rawNulls = nulls->asMutable<uint8_t>() + bits::nbytes(resultOffset);
  const auto numBytes = BaseVector::byteSize<bool>(size);

  source->readBytes(rawNulls, numBytes);
  bits::reverseBits(rawNulls, numBytes);
  bits::negate(reinterpret_cast<char*>(rawNulls), numBytes * 8);

  // Shift bits if needed.
  if (bits::nbytes(resultOffset) * 8 > resultOffset) {
    bits::copyBits(
        nulls->asMutable<uint64_t>(),
        bits::nbytes(resultOffset) * 8,
        nulls->asMutable<uint64_t>(),
        resultOffset,
        size);
  }

  return BaseVector::countNulls(nulls, resultOffset, resultOffset + size);
}

template <typename T>
void read(
    ByteInputStream* source,
    const TypePtr& type,
    velox::memory::MemoryPool* pool,
    VectorPtr& result,
    vector_size_t resultOffset,
    bool useLosslessTimestamp) {
  const int32_t size = source->read<int32_t>();
  result->resize(resultOffset + size);

  auto flatResult = result->asFlatVector<T>();
  auto nullCount = readNulls(source, size, *flatResult, resultOffset);

  BufferPtr values = flatResult->mutableValues(resultOffset + size);
  if constexpr (std::is_same_v<T, Timestamp>) {
    if (useLosslessTimestamp) {
      readLosslessTimestampValues(
          source, size, resultOffset, flatResult->nulls(), nullCount, values);
      return;
    }
  }
  if (type->isLongDecimal()) {
    readDecimalValues(
        source, size, resultOffset, flatResult->nulls(), nullCount, values);
    return;
  }
  readValues<T>(
      source, size, resultOffset, flatResult->nulls(), nullCount, values);
}

template <>
void read<StringView>(
    ByteInputStream* source,
    const TypePtr& type,
    velox::memory::MemoryPool* pool,
    VectorPtr& result,
    vector_size_t resultOffset,
    bool useLosslessTimestamp) {
  const int32_t size = source->read<int32_t>();

  result->resize(resultOffset + size);

  auto flatResult = result->as<FlatVector<StringView>>();
  BufferPtr values = flatResult->mutableValues(resultOffset + size);
  auto rawValues = values->asMutable<StringView>();
  for (int32_t i = 0; i < size; ++i) {
    // Set the first int32_t of each StringView to be the offset.
    *reinterpret_cast<int32_t*>(&rawValues[resultOffset + i]) =
        source->read<int32_t>();
  }
  readNulls(source, size, *flatResult, resultOffset);

  const int32_t dataSize = source->read<int32_t>();
  if (dataSize == 0) {
    return;
  }

  auto* rawStrings =
      flatResult->getRawStringBufferWithSpace(dataSize, true /*exactSize*/);

  source->readBytes(rawStrings, dataSize);
  int32_t previousOffset = 0;
  auto rawChars = reinterpret_cast<char*>(rawStrings);
  for (int32_t i = 0; i < size; ++i) {
    int32_t offset = rawValues[resultOffset + i].size();
    rawValues[resultOffset + i] =
        StringView(rawChars + previousOffset, offset - previousOffset);
    previousOffset = offset;
  }
}

void readColumns(
    ByteInputStream* source,
    velox::memory::MemoryPool* pool,
    const std::vector<TypePtr>& types,
    std::vector<VectorPtr>& result,
    vector_size_t resultOffset,
    bool useLosslessTimestamp);

void readConstantVector(
    ByteInputStream* source,
    const TypePtr& type,
    velox::memory::MemoryPool* pool,
    VectorPtr& result,
    vector_size_t resultOffset,
    bool useLosslessTimestamp) {
  const auto size = source->read<int32_t>();
  std::vector<TypePtr> childTypes = {type};
  std::vector<VectorPtr> children{BaseVector::create(type, 0, pool)};
  readColumns(source, pool, childTypes, children, 0, useLosslessTimestamp);
  VELOX_CHECK_EQ(1, children[0]->size());

  auto constantVector = BaseVector::wrapInConstant(size, 0, children[0]);
  if (resultOffset == 0) {
    result = std::move(constantVector);
  } else {
    result->resize(resultOffset + size);

    SelectivityVector rows(resultOffset + size, false);
    rows.setValidRange(resultOffset, resultOffset + size, true);
    rows.updateBounds();

    BaseVector::ensureWritable(rows, type, pool, result);
    result->copy(constantVector.get(), resultOffset, 0, size);
  }
}

void readDictionaryVector(
    ByteInputStream* source,
    const TypePtr& type,
    velox::memory::MemoryPool* pool,
    VectorPtr& result,
    vector_size_t resultOffset,
    bool useLosslessTimestamp) {
  const auto size = source->read<int32_t>();

  std::vector<TypePtr> childTypes = {type};
  std::vector<VectorPtr> children{BaseVector::create(type, 0, pool)};
  readColumns(source, pool, childTypes, children, 0, useLosslessTimestamp);

  // Read indices.
  BufferPtr indices = allocateIndices(size, pool);
  source->readBytes(indices->asMutable<char>(), size * sizeof(int32_t));

  // Skip 3 * 8 bytes of 'instance id'. Velox doesn't use 'instance id' for
  // dictionary vectors.
  source->skip(24);

  auto dictionaryVector =
      BaseVector::wrapInDictionary(nullptr, indices, size, children[0]);
  if (resultOffset == 0) {
    result = std::move(dictionaryVector);
  } else {
    result->resize(resultOffset + size);

    SelectivityVector rows(resultOffset + size, false);
    rows.setValidRange(resultOffset, resultOffset + size, true);
    rows.updateBounds();

    BaseVector::ensureWritable(rows, type, pool, result);
    result->copy(dictionaryVector.get(), resultOffset, 0, size);
  }
}

void readArrayVector(
    ByteInputStream* source,
    const TypePtr& type,
    velox::memory::MemoryPool* pool,
    VectorPtr& result,
    vector_size_t resultOffset,
    bool useLosslessTimestamp) {
  ArrayVector* arrayVector = result->as<ArrayVector>();

  const auto resultElementsOffset = arrayVector->elements()->size();

  std::vector<TypePtr> childTypes = {type->childAt(0)};
  std::vector<VectorPtr> children{arrayVector->elements()};
  readColumns(
      source,
      pool,
      childTypes,
      children,
      resultElementsOffset,
      useLosslessTimestamp);

  vector_size_t size = source->read<int32_t>();
  arrayVector->resize(resultOffset + size);
  arrayVector->setElements(children[0]);

  BufferPtr offsets = arrayVector->mutableOffsets(resultOffset + size);
  auto rawOffsets = offsets->asMutable<vector_size_t>();
  BufferPtr sizes = arrayVector->mutableSizes(resultOffset + size);
  auto rawSizes = sizes->asMutable<vector_size_t>();
  int32_t base = source->read<int32_t>();
  for (int32_t i = 0; i < size; ++i) {
    int32_t offset = source->read<int32_t>();
    rawOffsets[resultOffset + i] = resultElementsOffset + base;
    rawSizes[resultOffset + i] = offset - base;
    base = offset;
  }

  readNulls(source, size, *arrayVector, resultOffset);
}

void readMapVector(
    ByteInputStream* source,
    const TypePtr& type,
    velox::memory::MemoryPool* pool,
    VectorPtr& result,
    vector_size_t resultOffset,
    bool useLosslessTimestamp) {
  MapVector* mapVector = result->as<MapVector>();
  const auto resultElementsOffset = mapVector->mapKeys()->size();
  std::vector<TypePtr> childTypes = {type->childAt(0), type->childAt(1)};
  std::vector<VectorPtr> children{mapVector->mapKeys(), mapVector->mapValues()};
  readColumns(
      source,
      pool,
      childTypes,
      children,
      resultElementsOffset,
      useLosslessTimestamp);

  int32_t hashTableSize = source->read<int32_t>();
  if (hashTableSize != -1) {
    // Skip over serialized hash table from Presto wire format.
    source->skip(hashTableSize * sizeof(int32_t));
  }

  vector_size_t size = source->read<int32_t>();
  mapVector->resize(resultOffset + size);
  mapVector->setKeysAndValues(children[0], children[1]);

  BufferPtr offsets = mapVector->mutableOffsets(resultOffset + size);
  auto rawOffsets = offsets->asMutable<vector_size_t>();
  BufferPtr sizes = mapVector->mutableSizes(resultOffset + size);
  auto rawSizes = sizes->asMutable<vector_size_t>();
  int32_t base = source->read<int32_t>();
  for (int32_t i = 0; i < size; ++i) {
    int32_t offset = source->read<int32_t>();
    rawOffsets[resultOffset + i] = resultElementsOffset + base;
    rawSizes[resultOffset + i] = offset - base;
    base = offset;
  }

  readNulls(source, size, *mapVector, resultOffset);
}

int64_t packTimestampWithTimeZone(int64_t timestamp, int16_t timezone) {
  return timezone | (timestamp << 12);
}

void unpackTimestampWithTimeZone(
    int64_t packed,
    int64_t& timestamp,
    int16_t& timezone) {
  timestamp = packed >> 12;
  timezone = packed & 0xfff;
}

void readTimestampWithTimeZone(
    ByteInputStream* source,
    velox::memory::MemoryPool* pool,
    RowVector* result,
    vector_size_t resultOffset) {
  auto& timestamps = result->childAt(0);
  read<int64_t>(source, BIGINT(), pool, timestamps, resultOffset, false);

  auto rawTimestamps = timestamps->asFlatVector<int64_t>()->mutableRawValues();

  const auto size = timestamps->size();
  result->resize(size);

  auto& timezones = result->childAt(1);
  timezones->resize(size);
  auto rawTimezones = timezones->asFlatVector<int16_t>()->mutableRawValues();

  auto rawNulls = timestamps->rawNulls();
  for (auto i = resultOffset; i < size; ++i) {
    if (!rawNulls || !bits::isBitNull(rawNulls, i)) {
      unpackTimestampWithTimeZone(
          rawTimestamps[i], rawTimestamps[i], rawTimezones[i]);
      result->setNull(i, false);
    } else {
      result->setNull(i, true);
    }
  }
}

template <typename T>
void scatterValues(
    int32_t numValues,
    const vector_size_t* indices,
    T* data,
    vector_size_t offset) {
  for (auto index = numValues - 1; index >= 0; --index) {
    const auto destination = indices[index];
    if (destination == offset + index) {
      break;
    }
    data[destination] = data[offset + index];
  }
}

template <TypeKind kind>
void scatterFlatValues(
    vector_size_t scatterSize,
    const vector_size_t* scatter,
    BaseVector& vector,
    vector_size_t offset) {
  using T = typename TypeTraits<kind>::NativeType;
  auto* values = vector.asUnchecked<FlatVector<T>>()->mutableRawValues();
  scatterValues(scatterSize, scatter, values, offset);
}

template <>
void scatterFlatValues<TypeKind::BOOLEAN>(
    vector_size_t scatterSize,
    const vector_size_t* scatter,
    BaseVector& vector,
    vector_size_t offset) {
  auto* values = const_cast<uint64_t*>(vector.values()->as<uint64_t>());

  for (auto index = scatterSize - 1; index >= 0; --index) {
    const auto destination = scatter[index];
    if (destination == offset + index) {
      break;
    }
    bits::setBit(values, destination, bits::isBitSet(values, offset + index));
  }
}

void scatterStructNulls(
    vector_size_t size,
    vector_size_t scatterSize,
    const vector_size_t* scatter,
    const uint64_t* incomingNulls,
    RowVector& row,
    vector_size_t rowOffset);

// Scatters existing nulls and adds 'incomingNulls' to the gaps. 'oldSize' is
// the number of valid bits in the nulls of 'vector'. 'vector' must have been
// resized to the new size before calling this.
void scatterNulls(
    vector_size_t oldSize,
    const uint64_t* incomingNulls,
    BaseVector& vector) {
  const bool hasNulls = vector.mayHaveNulls();
  const auto size = vector.size();

  if (hasNulls) {
    auto* bits = reinterpret_cast<char*>(vector.mutableRawNulls());
    bits::scatterBits(oldSize, size, bits, incomingNulls, bits);
  } else {
    ::memcpy(vector.mutableRawNulls(), incomingNulls, bits::nbytes(size));
  }
}

// Scatters all rows in 'vector' starting from 'offset'. All these rows are
// expected to be non-null before this call. vector[offset + i] is copied into
// vector[offset + scatter[i]] for all i in [0, scatterSize). The gaps are
// filled with nulls. scatter[i] >= i for all i. scatter[i] >= scatter[j] for
// all i and j.
//
// @param size Size of the 'vector' after scatter. Includes trailing nulls.
// @param scatterSize Number of rows to scatter, starting from 'offset'.
// @param scatter Destination row numbers. A total of 'scatterSize' entries.
// @param incomingNulls A bitmap representation of the 'scatter' covering 'size'
// rows. First 'offset' bits should be kNotNull.
// @param vector Vector to modify.
// @param offset First row to scatter.
void scatterVector(
    int32_t size,
    vector_size_t scatterSize,
    const vector_size_t* scatter,
    const uint64_t* incomingNulls,
    VectorPtr& vector,
    vector_size_t offset) {
  const auto oldSize = vector->size();
  if (scatter != nullptr && scatterSize > 0) {
    // 'scatter' should have an entry for every row in 'vector' starting from
    // 'offset'.
    VELOX_CHECK_EQ(vector->size(), scatterSize + offset);

    // The new vector size should cover all 'scatter' destinations and
    // trailing nulls.
    VELOX_CHECK_GE(size, scatter[scatterSize - 1] + 1);
  }

  if (vector->encoding() == VectorEncoding::Simple::ROW) {
    vector->asUnchecked<RowVector>()->unsafeResize(size);
  } else {
    vector->resize(size);
  }

  switch (vector->encoding()) {
    case VectorEncoding::Simple::DICTIONARY: {
      VELOX_CHECK_EQ(0, offset, "Result offset is not supported yet")
      if (incomingNulls != nullptr) {
        auto* dictIndices =
            const_cast<vector_size_t*>(vector->wrapInfo()->as<vector_size_t>());
        scatterValues(scatterSize, scatter, dictIndices, 0);
        scatterNulls(oldSize, incomingNulls, *vector);
      }
      auto values = vector->valueVector();
      scatterVector(values->size(), 0, nullptr, nullptr, values, 0);
      break;
    }
    case VectorEncoding::Simple::CONSTANT: {
      VELOX_CHECK_EQ(0, offset, "Result offset is not supported yet")
      auto values = vector->valueVector();
      if (values != nullptr) {
        scatterVector(values->size(), 0, nullptr, nullptr, values, 0);
      }

      if (incomingNulls) {
        BaseVector::ensureWritable(
            SelectivityVector::empty(), vector->type(), vector->pool(), vector);
        scatterNulls(oldSize, incomingNulls, *vector);
      }
      break;
    }
    case VectorEncoding::Simple::ARRAY: {
      auto* array = vector->asUnchecked<ArrayVector>();
      if (incomingNulls) {
        auto offsets = const_cast<vector_size_t*>(array->rawOffsets());
        auto sizes = const_cast<vector_size_t*>(array->rawSizes());
        scatterValues(scatterSize, scatter, offsets, offset);
        scatterValues(scatterSize, scatter, sizes, offset);
        scatterNulls(oldSize, incomingNulls, *vector);
      }
      // Find the offset from which 'new' array elements start assuming that
      // array elements are written in order.
      vector_size_t elementOffset = 0;
      for (auto i = offset - 1; i >= 0; --i) {
        if (!array->isNullAt(i)) {
          elementOffset = array->offsetAt(i) + array->sizeAt(i);
          break;
        }
      }
      auto elements = array->elements();
      scatterVector(
          elements->size(), 0, nullptr, nullptr, elements, elementOffset);
      break;
    }
    case VectorEncoding::Simple::MAP: {
      auto* map = vector->asUnchecked<MapVector>();
      if (incomingNulls) {
        auto offsets = const_cast<vector_size_t*>(map->rawOffsets());
        auto sizes = const_cast<vector_size_t*>(map->rawSizes());
        scatterValues(scatterSize, scatter, offsets, offset);
        scatterValues(scatterSize, scatter, sizes, offset);
        scatterNulls(oldSize, incomingNulls, *vector);
      }
      // Find out the offset from which 'new' map keys and values start assuming
      // that map elements are written in order.
      vector_size_t elementOffset = 0;
      for (auto i = offset - 1; i >= 0; --i) {
        if (!map->isNullAt(i)) {
          elementOffset = map->offsetAt(i) + map->sizeAt(i);
          break;
        }
      }
      auto keys = map->mapKeys();
      scatterVector(keys->size(), 0, nullptr, nullptr, keys, elementOffset);
      auto values = map->mapValues();
      scatterVector(values->size(), 0, nullptr, nullptr, values, elementOffset);
      break;
    }
    case VectorEncoding::Simple::ROW: {
      auto* row = vector->asUnchecked<RowVector>();
      scatterStructNulls(row->size(), 0, nullptr, nullptr, *row, offset);
      break;
    }
    case VectorEncoding::Simple::FLAT: {
      if (incomingNulls != nullptr) {
        VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(
            scatterFlatValues,
            vector->typeKind(),
            scatterSize,
            scatter,
            *vector,
            offset);
        scatterNulls(oldSize, incomingNulls, *vector);
      }
      break;
    }
    default:
      VELOX_FAIL("Unsupported encoding in scatter: {}", vector->encoding());
  }
}

// A RowVector with nulls is serialized with children having a value
// only for rows where the struct is non-null. After deserializing,
// we do an extra traversal to add gaps into struct members where
// the containing struct is null. As we go down the struct tree, we
// merge child struct nulls into the nulls from enclosing structs so
// that the leaves only get scattered once, considering all nulls
// from all enclosing structs.
void scatterStructNulls(
    vector_size_t size,
    vector_size_t scatterSize,
    const vector_size_t* scatter,
    const uint64_t* incomingNulls,
    RowVector& row,
    vector_size_t rowOffset) {
  const auto oldSize = row.size();
  if (isTimestampWithTimeZoneType(row.type())) {
    // The timestamp with tz case is special. The child vectors are aligned with
    // the struct even if the struct has nulls.
    if (incomingNulls != nullptr) {
      scatterVector(
          size, scatterSize, scatter, incomingNulls, row.childAt(0), rowOffset);
      scatterVector(
          size, scatterSize, scatter, incomingNulls, row.childAt(1), rowOffset);
      row.unsafeResize(size);
      scatterNulls(oldSize, incomingNulls, row);
    }
    return;
  }

  const uint64_t* childIncomingNulls = incomingNulls;
  const vector_size_t* childScatter = scatter;
  auto childScatterSize = scatterSize;
  raw_vector<vector_size_t> innerScatter;
  raw_vector<uint64_t> childIncomingNullsVector;
  if (auto* rawNulls = row.rawNulls()) {
    innerScatter.resize(size);
    if (incomingNulls == nullptr) {
      childIncomingNulls = rawNulls;
      if (rowOffset > 0) {
        childIncomingNullsVector.resize(bits::nwords(size));
        auto* newNulls = childIncomingNullsVector.data();
        ::memcpy(newNulls, rawNulls, bits::nbytes(size));

        // Fill in first 'rowOffset' bits with kNotNull to avoid scattering
        // nulls for pre-existing rows.
        bits::fillBits(newNulls, 0, rowOffset, bits::kNotNull);
        childIncomingNulls = newNulls;
      }
      childScatterSize = simd::indicesOfSetBits(
          rawNulls, rowOffset, size, innerScatter.data());
    } else {
      childIncomingNullsVector.resize(bits::nwords(size));
      auto* newNulls = childIncomingNullsVector.data();
      bits::scatterBits(
          row.size(),
          size,
          reinterpret_cast<const char*>(rawNulls),
          incomingNulls,
          reinterpret_cast<char*>(newNulls));

      // Fill in first 'rowOffset' bits with kNotNull to avoid scattering nulls
      // for pre-existing rows.
      bits::fillBits(newNulls, 0, rowOffset, bits::kNotNull);
      childIncomingNulls = newNulls;
      childScatterSize = simd::indicesOfSetBits(
          childIncomingNulls, rowOffset, size, innerScatter.data());
    }
    childScatter = innerScatter.data();
  }
  for (auto i = 0; i < row.childrenSize(); ++i) {
    auto& child = row.childAt(i);
    if (child->encoding() == VectorEncoding::Simple::ROW) {
      scatterStructNulls(
          size,
          childScatterSize,
          childScatter,
          childIncomingNulls,
          *child->asUnchecked<RowVector>(),
          rowOffset);
    } else {
      scatterVector(
          size,
          childScatterSize,
          childScatter,
          childIncomingNulls,
          row.childAt(i),
          rowOffset);
    }
  }
  if (incomingNulls) {
    row.unsafeResize(size);
    scatterNulls(oldSize, incomingNulls, row);
  }
  // On return of scatter we check that child sizes match the struct size. This
  // is safe also if no scatter.
  for (auto i = 0; i < row.childrenSize(); ++i) {
    VELOX_CHECK_EQ(row.childAt(i)->size(), row.size());
  }
}

void readRowVector(
    ByteInputStream* source,
    const TypePtr& type,
    velox::memory::MemoryPool* pool,
    VectorPtr& result,
    vector_size_t resultOffset,
    bool useLosslessTimestamp) {
  auto* row = result->as<RowVector>();
  if (isTimestampWithTimeZoneType(type)) {
    readTimestampWithTimeZone(source, pool, row, resultOffset);
    return;
  }

  const int32_t numChildren = source->read<int32_t>();
  auto& children = row->children();

  const auto& childTypes = type->asRow().children();
  readColumns(
      source, pool, childTypes, children, resultOffset, useLosslessTimestamp);

  auto size = source->read<int32_t>();
  // Set the size of the row but do not alter the size of the
  // children. The children get adjusted in a separate pass over the
  // data. The parent and child size MUST be separate until the second pass.
  row->BaseVector::resize(resultOffset + size);
  for (int32_t i = 0; i <= size; ++i) {
    source->read<int32_t>();
  }
  readNulls(source, size, *result, resultOffset);
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

void readColumns(
    ByteInputStream* source,
    velox::memory::MemoryPool* pool,
    const std::vector<TypePtr>& types,
    std::vector<VectorPtr>& results,
    vector_size_t resultOffset,
    bool useLosslessTimestamp) {
  static const std::unordered_map<
      TypeKind,
      std::function<void(
          ByteInputStream * source,
          const TypePtr& type,
          velox::memory::MemoryPool* pool,
          VectorPtr& result,
          vector_size_t resultOffset,
          bool useLosslessTimestamp)>>
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
          pool,
          columnResult,
          resultOffset,
          useLosslessTimestamp);
    } else if (encoding == kDictionary) {
      readDictionaryVector(
          source,
          columnType,
          pool,
          columnResult,
          resultOffset,
          useLosslessTimestamp);
    } else {
      checkTypeEncoding(encoding, columnType);
      const auto it = readers.find(columnType->kind());
      VELOX_CHECK(
          it != readers.end(),
          "Column reader for type {} is missing",
          columnType->kindName());

      it->second(
          source,
          columnType,
          pool,
          columnResult,
          resultOffset,
          useLosslessTimestamp);
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
  VectorStream(
      const TypePtr& type,
      std::optional<VectorEncoding::Simple> encoding,
      StreamArena* streamArena,
      int32_t initialNumRows,
      bool useLosslessTimestamp)
      : type_(type),
        isLongDecimal_(type_->isLongDecimal()),
        encoding_(encoding),
        useLosslessTimestamp_(useLosslessTimestamp),
        nulls_(streamArena, true, true),
        lengths_(streamArena),
        values_(streamArena) {
    if (initialNumRows == 0) {
      initializeHeader(typeToEncodingName(type), *streamArena);
      return;
    }

    if (encoding_.has_value()) {
      switch (encoding_.value()) {
        case VectorEncoding::Simple::CONSTANT: {
          initializeHeader(kRLE, *streamArena);
          children_.emplace_back(std::make_unique<VectorStream>(
              type_,
              std::nullopt,
              streamArena,
              initialNumRows,
              useLosslessTimestamp));
          return;
        }
        case VectorEncoding::Simple::DICTIONARY: {
          initializeHeader(kDictionary, *streamArena);
          values_.startWrite(initialNumRows * 4);
          children_.emplace_back(std::make_unique<VectorStream>(
              type_,
              std::nullopt,
              streamArena,
              initialNumRows,
              useLosslessTimestamp));
          return;
        }
        default:
          break;
      }
    }

    initializeHeader(typeToEncodingName(type), *streamArena);
    nulls_.startWrite(1 + (initialNumRows / 8));

    switch (type_->kind()) {
      case TypeKind::ROW:
        if (isTimestampWithTimeZoneType(type_)) {
          values_.startWrite(initialNumRows * 4);
          break;
        }
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
              streamArena,
              initialNumRows,
              useLosslessTimestamp);
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
        values_.startWrite(initialNumRows * 10);
        break;
      default:
        values_.startWrite(initialNumRows * 4);
        break;
    }
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
        if (isTimestampWithTimeZoneType(type_)) {
          writeInt32(out, nullCount_ + nonNullCount_);
          flushNulls(out);
          values_.flush(out);
          return;
        }

        writeInt32(out, children_.size());
        for (auto& child : children_) {
          child->flush(out);
        }
        writeInt32(out, nullCount_ + nonNullCount_);
        lengths_.flush(out);
        flushNulls(out);
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

 private:
  const TypePtr type_;
  const bool isLongDecimal_;
  const std::optional<VectorEncoding::Simple> encoding_;
  /// Indicates whether to serialize timestamps with nanosecond precision.
  /// If false, they are serialized with millisecond precision which is
  /// compatible with presto.
  const bool useLosslessTimestamp_;

  int32_t nonNullCount_{0};
  int32_t nullCount_{0};
  int32_t totalLength_{0};
  bool hasLengths_{false};
  ByteRange header_;
  ByteOutputStream nulls_;
  ByteOutputStream lengths_;
  ByteOutputStream values_;
  std::vector<std::unique_ptr<VectorStream>> children_;
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
  if (useLosslessTimestamp_) {
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
    const BaseVector* vector,
    const folly::Range<const IndexRange*>& ranges,
    VectorStream* stream) {
  using T = typename TypeTraits<kind>::NativeType;
  auto* flatVector = dynamic_cast<const FlatVector<T>*>(vector);
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
    const BaseVector* vector,
    const folly::Range<const IndexRange*>& ranges,
    VectorStream* stream) {
  auto flatVector = dynamic_cast<const FlatVector<bool>*>(vector);
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
    const BaseVector* vector,
    const folly::Range<const IndexRange*>& ranges,
    VectorStream* stream);

void serializeColumn(
    const BaseVector* vector,
    const folly::Range<const vector_size_t*>& rows,
    VectorStream* stream,
    Scratch& scratch);

void serializeWrapped(
    const BaseVector* vector,
    const folly::Range<const IndexRange*>& ranges,
    VectorStream* stream) {
  std::vector<IndexRange> newRanges;
  const bool mayHaveNulls = vector->mayHaveNulls();
  const BaseVector* wrapped = vector->wrappedVector();
  for (int32_t i = 0; i < ranges.size(); ++i) {
    const auto end = ranges[i].begin + ranges[i].size;
    for (int32_t offset = ranges[i].begin; offset < end; ++offset) {
      if (mayHaveNulls && vector->isNullAt(offset)) {
        // The wrapper added a null.
        if (!newRanges.empty()) {
          serializeColumn(wrapped, newRanges, stream);
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
    serializeColumn(wrapped, newRanges, stream);
  }
}

void serializeTimestampWithTimeZone(
    const RowVector* rowVector,
    const folly::Range<const IndexRange*>& ranges,
    VectorStream* stream) {
  auto timestamps = rowVector->childAt(0)->as<SimpleVector<int64_t>>();
  auto timezones = rowVector->childAt(1)->as<SimpleVector<int16_t>>();
  for (const auto& range : ranges) {
    for (auto i = range.begin; i < range.begin + range.size; ++i) {
      if (rowVector->isNullAt(i)) {
        stream->appendNull();
      } else {
        stream->appendNonNull();
        stream->appendOne(packTimestampWithTimeZone(
            timestamps->valueAt(i), timezones->valueAt(i)));
      }
    }
  }
}

void serializeRowVector(
    const BaseVector* vector,
    const folly::Range<const IndexRange*>& ranges,
    VectorStream* stream) {
  auto rowVector = dynamic_cast<const RowVector*>(vector);

  if (isTimestampWithTimeZoneType(vector->type())) {
    serializeTimestampWithTimeZone(rowVector, ranges, stream);
    return;
  }

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
        rowVector->childAt(i).get(), childRanges, stream->childAt(i));
  }
}

void serializeArrayVector(
    const BaseVector* vector,
    const folly::Range<const IndexRange*>& ranges,
    VectorStream* stream) {
  auto arrayVector = dynamic_cast<const ArrayVector*>(vector);
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
      arrayVector->elements().get(), childRanges, stream->childAt(0));
}

void serializeMapVector(
    const BaseVector* vector,
    const folly::Range<const IndexRange*>& ranges,
    VectorStream* stream) {
  auto mapVector = dynamic_cast<const MapVector*>(vector);
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
  serializeColumn(mapVector->mapKeys().get(), childRanges, stream->childAt(0));
  serializeColumn(
      mapVector->mapValues().get(), childRanges, stream->childAt(1));
}

static inline int32_t rangesTotalSize(
    const folly::Range<const IndexRange*>& ranges) {
  int32_t total = 0;
  for (auto& range : ranges) {
    total += range.size;
  }
  return total;
}

template <TypeKind kind>
void serializeConstantVector(
    const BaseVector* vector,
    const folly::Range<const IndexRange*>& ranges,
    VectorStream* stream) {
  using T = typename KindToFlatVector<kind>::WrapperType;
  auto constVector = dynamic_cast<const ConstantVector<T>*>(vector);
  if (constVector->valueVector() != nullptr) {
    serializeWrapped(constVector, ranges, stream);
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

template <typename T>
void serializeBiasVector(
    const BaseVector* vector,
    const folly::Range<const IndexRange*>& ranges,
    VectorStream* stream) {
  auto biasVector = dynamic_cast<const BiasVector<T>*>(vector);
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
    const BaseVector* vector,
    const folly::Range<const IndexRange*>& ranges,
    VectorStream* stream) {
  switch (vector->encoding()) {
    case VectorEncoding::Simple::FLAT:
      VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(
          serializeFlatVector, vector->typeKind(), vector, ranges, stream);
      break;
    case VectorEncoding::Simple::CONSTANT:
      VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
          serializeConstantVector, vector->typeKind(), vector, ranges, stream);
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
      serializeRowVector(vector, ranges, stream);
      break;
    case VectorEncoding::Simple::ARRAY:
      serializeArrayVector(vector, ranges, stream);
      break;
    case VectorEncoding::Simple::MAP:
      serializeMapVector(vector, ranges, stream);
      break;
    case VectorEncoding::Simple::LAZY:
      serializeColumn(vector->loadedVector(), ranges, stream);
      break;
    default:
      serializeWrapped(vector, ranges, stream);
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
    const BaseVector* vector,
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
    const BaseVector* vector,
    const folly::Range<const vector_size_t*>& rows,
    VectorStream* stream,
    Scratch& scratch) {
  auto* flatVector = reinterpret_cast<const FlatVector<bool>*>(vector);
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
    const BaseVector* vector,
    const folly::Range<const vector_size_t*>& rows,
    VectorStream* stream,
    Scratch& scratch) {
  ScratchPtr<vector_size_t, 1> innerRowsHolder(scratch);
  const int32_t numRows = rows.size();
  int32_t numInner = 0;
  auto* innerRows = innerRowsHolder.get(numRows);
  const BaseVector* wrapped;
  if (vector->encoding() == VectorEncoding::Simple::DICTIONARY &&
      !vector->rawNulls()) {
    // Dictionary with no nulls.
    auto* indices = vector->wrapInfo()->as<vector_size_t>();
    wrapped = vector->valueVector().get();
    simd::transpose(indices, rows, innerRows);
    numInner = numRows;
  } else {
    wrapped = vector->wrappedVector();
    for (int32_t i = 0; i < rows.size(); ++i) {
      if (vector->isNullAt(rows[i])) {
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
    const BaseVector* vector,
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
    const BaseVector* vector,
    const folly::Range<const vector_size_t*>& ranges,
    VectorStream* stream,
    Scratch& scratch) {
  VELOX_UNSUPPORTED();
}

void serializeTimestampWithTimeZone(
    const RowVector* rowVector,
    const folly::Range<const vector_size_t*>& rows,
    VectorStream* stream,
    Scratch& scratch) {
  auto timestamps = rowVector->childAt(0)->as<SimpleVector<int64_t>>();
  auto timezones = rowVector->childAt(1)->as<SimpleVector<int16_t>>();
  for (auto i : rows) {
    if (rowVector->isNullAt(i)) {
      stream->appendNull();
    } else {
      stream->appendNonNull();
      stream->appendOne(packTimestampWithTimeZone(
          timestamps->valueAt(i), timezones->valueAt(i)));
    }
  }
}

void serializeRowVector(
    const BaseVector* vector,
    const folly::Range<const vector_size_t*>& rows,
    VectorStream* stream,
    Scratch& scratch) {
  auto rowVector = reinterpret_cast<const RowVector*>(vector);
  vector_size_t* childRows;
  int32_t numChildRows = 0;
  if (isTimestampWithTimeZoneType(vector->type())) {
    serializeTimestampWithTimeZone(rowVector, rows, stream, scratch);
    return;
  }
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
        rowVector->childAt(i).get(),
        folly::Range<const vector_size_t*>(innerRows, numInnerRows),
        stream->childAt(i),
        scratch);
  }
}

void serializeArrayVector(
    const BaseVector* vector,
    const folly::Range<const vector_size_t*>& rows,
    VectorStream* stream,
    Scratch& scratch) {
  auto arrayVector = reinterpret_cast<const ArrayVector*>(vector);

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
      arrayVector->elements().get(),
      folly::Range<const IndexRange*>(rangesHolder.get(), numRanges),
      stream->childAt(0));
}

void serializeMapVector(
    const BaseVector* vector,
    const folly::Range<const vector_size_t*>& rows,
    VectorStream* stream,
    Scratch& scratch) {
  auto mapVector = reinterpret_cast<const MapVector*>(vector);

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
      mapVector->mapKeys().get(),
      folly::Range<const IndexRange*>(rangesHolder.get(), numRanges),
      stream->childAt(0));
  serializeColumn(
      mapVector->mapValues().get(),
      folly::Range<const IndexRange*>(rangesHolder.get(), numRanges),
      stream->childAt(1));
}

template <TypeKind kind>
void serializeConstantVector(
    const BaseVector* vector,
    const folly::Range<const vector_size_t*>& rows,
    VectorStream* stream,
    Scratch& scratch) {
  using T = typename KindToFlatVector<kind>::WrapperType;
  auto constVector = dynamic_cast<const ConstantVector<T>*>(vector);
  if (constVector->valueVector()) {
    serializeWrapped(constVector, rows, stream, scratch);
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
    const BaseVector* vector,
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
      serializeColumn(vector->loadedVector(), rows, stream, scratch);
      break;
    default:
      serializeWrapped(vector, rows, stream, scratch);
  }
}

template <TypeKind Kind>
void serializeConstantColumn(
    const BaseVector* vector,
    const folly::Range<const IndexRange*>& ranges,
    VectorStream* stream) {
  for (const auto& range : ranges) {
    stream->appendNonNull(range.size);
  }

  std::vector<IndexRange> newRanges;
  newRanges.push_back({0, 1});
  serializeConstantVector<Kind>(vector, newRanges, stream->childAt(0));
}

template <TypeKind Kind>
void serializeDictionaryColumn(
    const BaseVector* vector,
    const folly::Range<const IndexRange*>& ranges,
    VectorStream* stream) {
  using T = typename KindToFlatVector<Kind>::WrapperType;

  auto dictionaryVector = dynamic_cast<const DictionaryVector<T>*>(vector);
  VELOX_CHECK_NULL(
      dictionaryVector->nulls(),
      "Cannot serialize dictionary vector with nulls");

  std::vector<IndexRange> childRanges;
  childRanges.push_back({0, dictionaryVector->valueVector()->size()});
  serializeColumn(
      dictionaryVector->valueVector().get(), childRanges, stream->childAt(0));

  const BufferPtr& indices = dictionaryVector->indices();
  auto* rawIndices = indices->as<vector_size_t>();
  for (const auto& range : ranges) {
    stream->appendNonNull(range.size);
    stream->append<int32_t>(folly::Range(&rawIndices[range.begin], range.size));
  }
}

void serializeEncodedColumn(
    const BaseVector* vector,
    const folly::Range<const IndexRange*>& ranges,
    VectorStream* stream) {
  switch (vector->encoding()) {
    case VectorEncoding::Simple::CONSTANT:
      VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
          serializeConstantColumn, vector->typeKind(), vector, ranges, stream);
      break;
    case VectorEncoding::Simple::DICTIONARY:
      VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
          serializeDictionaryColumn,
          vector->typeKind(),
          vector,
          ranges,
          stream);
      break;
    default:
      serializeColumn(vector, ranges, stream);
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
    *sizes[i] += sizeof(int32_t);
    for (int32_t offset = begin; offset < end; ++offset) {
      if (!vector->isNullAt(offset)) {
        childRanges->push_back(
            IndexRange{rawOffsets[offset], rawSizes[offset]});
        childSizes->push_back(sizes[i]);
      }
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
      *(sizes[i]) +=
          numValues * valueSize + bits::nbytes(ranges[i].size - numValues);
    }
  } else {
    for (int32_t i = 0; i < ranges.size(); ++i) {
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
void estimateConstantSerializedSize(
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
          estimateConstantSerializedSize,
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
  auto valueSize = vector->type()->cppSizeInBytes();
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
void estimateConstantSerializedSize(
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
          estimateConstantSerializedSize,
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

class PrestoVectorSerializer : public VectorSerializer {
 public:
  PrestoVectorSerializer(
      const RowTypePtr& rowType,
      std::vector<VectorEncoding::Simple> encodings,
      int32_t numRows,
      StreamArena* streamArena,
      bool useLosslessTimestamp,
      common::CompressionKind compressionKind)
      : streamArena_(streamArena),
        codec_(common::compressionKindToCodec(compressionKind)) {
    const auto types = rowType->children();
    const auto numTypes = types.size();
    streams_.resize(numTypes);
    for (int i = 0; i < numTypes; ++i) {
      std::optional<VectorEncoding::Simple> encoding;
      if (!encodings.empty()) {
        encoding = encodings[i];
      }
      streams_[i] = std::make_unique<VectorStream>(
          types[i], encoding, streamArena, numRows, useLosslessTimestamp);
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
      serializeColumn(vector->childAt(i).get(), ranges, streams_[i].get());
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
      serializeColumn(
          vector->childAt(i).get(), rows, streams_[i].get(), scratch);
    }
  }

  void appendEncoded(
      const RowVectorPtr& vector,
      const folly::Range<const IndexRange*>& ranges) {
    const auto numNewRows = rangesTotalSize(ranges);
    if (numNewRows == 0) {
      return;
    }
    numRows_ += numNewRows;
    for (int32_t i = 0; i < vector->childrenSize(); ++i) {
      serializeEncodedColumn(
          vector->childAt(i).get(), ranges, streams_[i].get());
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
    flushInternal(numRows_, out);
  }

  void flushEncoded(const RowVectorPtr& vector, OutputStream* out) {
    VELOX_CHECK_EQ(0, numRows_);

    std::vector<IndexRange> ranges{{0, vector->size()}};
    appendEncoded(vector, folly::Range(ranges.data(), ranges.size()));

    flushInternal(vector->size(), out);
  }

 private:
  void flushUncompressed(
      int32_t numRows,
      OutputStream* out,
      PrestoOutputStreamListener* listener) {
    int32_t offset = out->tellp();

    char codec = 0;
    if (listener) {
      codec = getCodecMarker();
    }
    // Pause CRC computation
    if (listener) {
      listener->pause();
    }

    writeInt32(out, numRows);
    out->write(&codec, 1);

    // Make space for uncompressedSizeInBytes & sizeInBytes
    writeInt32(out, 0);
    writeInt32(out, 0);
    // Write zero checksum.
    writeInt64(out, 0);

    // Number of columns and stream content. Unpause CRC.
    if (listener) {
      listener->resume();
    }
    writeInt32(out, streams_.size());

    for (auto& stream : streams_) {
      stream->flush(out);
    }

    // Pause CRC computation
    if (listener) {
      listener->pause();
    }

    // Fill in uncompressedSizeInBytes & sizeInBytes
    int32_t size = (int32_t)out->tellp() - offset;
    int32_t uncompressedSize = size - kHeaderSize;
    int64_t crc = 0;
    if (listener) {
      crc = computeChecksum(listener, codec, numRows, uncompressedSize);
    }

    out->seekp(offset + kSizeInBytesOffset);
    writeInt32(out, uncompressedSize);
    writeInt32(out, uncompressedSize);
    writeInt64(out, crc);
    out->seekp(offset + size);
  }

  void flushCompressed(
      int32_t numRows,
      OutputStream* output,
      PrestoOutputStreamListener* listener) {
    const int32_t offset = output->tellp();
    char codec = kCompressedBitMask;
    if (listener) {
      codec |= kCheckSumBitMask;
    }

    // Pause CRC computation
    if (listener) {
      listener->pause();
    }

    writeInt32(output, numRows);
    output->write(&codec, 1);

    IOBufOutputStream out(
        *(streamArena_->pool()), nullptr, streamArena_->size());
    writeInt32(&out, streams_.size());

    for (auto& stream : streams_) {
      stream->flush(&out);
    }

    const int32_t uncompressedSize = out.tellp();
    VELOX_CHECK_LE(
        uncompressedSize,
        codec_->maxUncompressedLength(),
        "UncompressedSize exceeds limit");
    auto compressed = codec_->compress(out.getIOBuf().get());
    const int32_t compressedSize = compressed->length();
    writeInt32(output, uncompressedSize);
    writeInt32(output, compressedSize);
    const int32_t crcOffset = output->tellp();
    writeInt64(output, 0); // Write zero checksum
    // Number of columns and stream content. Unpause CRC.
    if (listener) {
      listener->resume();
    }
    output->write(
        reinterpret_cast<const char*>(compressed->writableData()),
        compressed->length());
    // Pause CRC computation
    if (listener) {
      listener->pause();
    }
    const int32_t endSize = output->tellp();
    // Fill in crc
    int64_t crc = 0;
    if (listener) {
      crc = computeChecksum(listener, codec, numRows, compressedSize);
    }
    output->seekp(crcOffset);
    writeInt64(output, crc);
    output->seekp(endSize);
  }

  // Writes the contents to 'stream' in wire format
  void flushInternal(int32_t numRows, OutputStream* out) {
    auto listener = dynamic_cast<PrestoOutputStreamListener*>(out->listener());
    // Reset CRC computation
    if (listener) {
      listener->reset();
    }

    if (!needCompression(*codec_)) {
      flushUncompressed(numRows, out, listener);
    } else {
      flushCompressed(numRows, out, listener);
    }
  }

  static const int32_t kSizeInBytesOffset{4 + 1};
  static const int32_t kHeaderSize{kSizeInBytesOffset + 4 + 4 + 8};

  StreamArena* const streamArena_;
  const std::unique_ptr<folly::io::Codec> codec_;

  int32_t numRows_{0};
  std::vector<std::unique_ptr<VectorStream>> streams_;
};
} // namespace

void PrestoVectorSerde::estimateSerializedSize(
    VectorPtr vector,
    const folly::Range<const IndexRange*>& ranges,
    vector_size_t** sizes,
    Scratch& scratch) {
  estimateSerializedSizeInt(vector->loadedVector(), ranges, sizes, scratch);
}

void PrestoVectorSerde::estimateSerializedSize(
    VectorPtr vector,
    const folly::Range<const vector_size_t*> rows,
    vector_size_t** sizes,
    Scratch& scratch) {
  estimateSerializedSizeInt(vector->loadedVector(), rows, sizes, scratch);
}

std::unique_ptr<VectorSerializer> PrestoVectorSerde::createSerializer(
    RowTypePtr type,
    int32_t numRows,
    StreamArena* streamArena,
    const Options* options) {
  const auto prestoOptions = toPrestoOptions(options);
  return std::make_unique<PrestoVectorSerializer>(
      type,
      prestoOptions.encodings,
      numRows,
      streamArena,
      prestoOptions.useLosslessTimestamp,
      prestoOptions.compressionKind);
}

void PrestoVectorSerde::serializeEncoded(
    const RowVectorPtr& vector,
    StreamArena* streamArena,
    const Options* options,
    OutputStream* out) {
  auto serializer = createSerializer(
      asRowType(vector->type()), vector->size(), streamArena, options);

  static_cast<PrestoVectorSerializer*>(serializer.get())
      ->flushEncoded(vector, out);
}

void PrestoVectorSerde::deserialize(
    ByteInputStream* source,
    velox::memory::MemoryPool* pool,
    RowTypePtr type,
    RowVectorPtr* result,
    vector_size_t resultOffset,
    const Options* options) {
  const auto prestoOptions = toPrestoOptions(options);
  const bool useLosslessTimestamp = prestoOptions.useLosslessTimestamp;
  const auto codec =
      common::compressionKindToCodec(prestoOptions.compressionKind);
  const auto numRows = source->read<int32_t>();

  if (resultOffset > 0) {
    VELOX_CHECK_NOT_NULL(*result);
    VELOX_CHECK(result->unique());
    (*result)->resize(resultOffset + numRows);
  } else if (*result && result->unique()) {
    VELOX_CHECK(
        *(*result)->type() == *type,
        "Unexpected type: {} vs. {}",
        (*result)->type()->toString(),
        type->toString());
    (*result)->prepareForReuse();
    (*result)->resize(numRows);
  } else {
    *result = BaseVector::create<RowVector>(type, numRows, pool);
  }

  const auto pageCodecMarker = source->read<int8_t>();
  const auto uncompressedSize = source->read<int32_t>();
  const auto compressedSize = source->read<int32_t>();
  const auto checksum = source->read<int64_t>();

  int64_t actualCheckSum = 0;
  if (isChecksumBitSet(pageCodecMarker)) {
    actualCheckSum =
        computeChecksum(source, pageCodecMarker, numRows, compressedSize);
  }

  VELOX_CHECK_EQ(
      checksum, actualCheckSum, "Received corrupted serialized page.");

  VELOX_CHECK_EQ(
      needCompression(*codec),
      isCompressedBitSet(pageCodecMarker),
      "Compression kind {} should align with codec marker.",
      common::compressionKindToString(
          common::codecTypeToCompressionKind(codec->type())));

  auto& children = (*result)->children();
  const auto& childTypes = type->asRow().children();
  if (!needCompression(*codec)) {
    const auto numColumns = source->read<int32_t>();
    // TODO Fix call sites and tighten the check to _EQ.
    VELOX_USER_CHECK_GE(
        numColumns,
        type->size(),
        "Number of columns in serialized data doesn't match "
        "number of columns requested for deserialization");
    readColumns(
        source, pool, childTypes, children, resultOffset, useLosslessTimestamp);
  } else {
    auto compressBuf = folly::IOBuf::create(compressedSize);
    source->readBytes(compressBuf->writableData(), compressedSize);
    compressBuf->append(compressedSize);
    auto uncompress = codec->uncompress(compressBuf.get(), uncompressedSize);
    ByteRange byteRange{
        uncompress->writableData(), (int32_t)uncompress->length(), 0};
    ByteInputStream uncompressedSource({byteRange});

    const auto numColumns = uncompressedSource.read<int32_t>();
    VELOX_USER_CHECK_EQ(
        numColumns,
        type->size(),
        "Number of columns in serialized data doesn't match "
        "number of columns requested for deserialization");
    readColumns(
        &uncompressedSource,
        pool,
        childTypes,
        children,
        resultOffset,
        useLosslessTimestamp);
  }

  scatterStructNulls(
      (*result)->size(), 0, nullptr, nullptr, **result, resultOffset);
}

void testingScatterStructNulls(
    vector_size_t size,
    vector_size_t scatterSize,
    const vector_size_t* scatter,
    const uint64_t* incomingNulls,
    RowVector& row,
    vector_size_t rowOffset) {
  scatterStructNulls(size, scatterSize, scatter, incomingNulls, row, rowOffset);
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

} // namespace facebook::velox::serializer::presto

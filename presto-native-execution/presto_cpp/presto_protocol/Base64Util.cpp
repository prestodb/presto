/*
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
#include "presto_cpp/presto_protocol/Base64Util.h"
#include "velox/common/base/BitUtil.h"
#include "velox/common/encode/Base64.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::presto::protocol {
namespace {

static const char* kLongArray = "LONG_ARRAY";
static const char* kIntArray = "INT_ARRAY";
static const char* kShortArray = "SHORT_ARRAY";
static const char* kByteArray = "BYTE_ARRAY";
static const char* kVariableWidth = "VARIABLE_WIDTH";
static const char* kRle = "RLE";
static const char* kArray = "ARRAY";
static const char* kMap = "MAP";
static const char* kInt128Array = "INT128_ARRAY";
static const __int128_t kInt128Mask = ~(static_cast<__int128_t>(1) << 127);

struct ByteStream {
  explicit ByteStream(const char* data, int32_t offset = 0)
      : data_(data), offset_(offset) {}

  template <typename T>
  T read() {
    T value = *reinterpret_cast<const T*>(data_ + offset_);
    offset_ += sizeof(T);
    return value;
  }

  std::string readString(int32_t size) {
    std::string value(data_ + offset_, size);
    offset_ += size;
    return value;
  }

  void readBytes(int32_t size, char* buffer) {
    memcpy(buffer, data_ + offset_, size);
    offset_ += size;
  }

 private:
  const char* data_;
  int32_t offset_;
};

velox::BufferPtr
readNulls(int32_t count, ByteStream& stream, velox::memory::MemoryPool* pool) {
  bool mayHaveNulls = stream.read<bool>();
  if (!mayHaveNulls) {
    return nullptr;
  }

  velox::BufferPtr nulls = velox::AlignedBuffer::allocate<bool>(count, pool);

  auto numBytes = facebook::velox::bits::nbytes(count);
  stream.readBytes(numBytes, nulls->asMutable<char>());

  velox::bits::reverseBits(nulls->asMutable<uint8_t>(), numBytes);
  velox::bits::negate(nulls->asMutable<char>(), count);
  return nulls;
}

template <typename T, typename U>
velox::VectorPtr readScalarBlock(
    const velox::TypePtr& type,
    ByteStream& stream,
    velox::memory::MemoryPool* pool) {
  auto positionCount = stream.read<int32_t>();

  velox::BufferPtr nulls = readNulls(positionCount, stream, pool);
  const uint64_t* rawNulls = nulls == nullptr ? nullptr : nulls->as<uint64_t>();

  velox::BufferPtr buffer =
      velox::AlignedBuffer::allocate<T>(positionCount, pool);
  auto rawBuffer = buffer->asMutable<T>();
  for (auto i = 0; i < positionCount; i++) {
    if (!rawNulls || !velox::bits::isBitNull(rawNulls, i)) {
      rawBuffer[i] = stream.read<T>();
    }
  }

  switch (type->kind()) {
    case velox::TypeKind::BIGINT:
    case velox::TypeKind::INTEGER:
    case velox::TypeKind::SMALLINT:
    case velox::TypeKind::TINYINT:
    case velox::TypeKind::DOUBLE:
    case velox::TypeKind::REAL:
    case velox::TypeKind::VARCHAR:
    case velox::TypeKind::DATE:
    case velox::TypeKind::INTERVAL_DAY_TIME:
    case velox::TypeKind::SHORT_DECIMAL:
      return std::make_shared<velox::FlatVector<U>>(
          pool,
          type,
          nulls,
          positionCount,
          buffer,
          std::vector<velox::BufferPtr>{});
    case velox::TypeKind::LONG_DECIMAL: {
      for (auto i = 0; i < positionCount; i++) {
        // Convert signed magnitude form to 2's complement.
        if (rawBuffer[i] < 0) {
          rawBuffer[i] &= kInt128Mask;
          rawBuffer[i] *= -1;
        }
      }
      return std::make_shared<velox::FlatVector<velox::UnscaledLongDecimal>>(
          pool,
          type,
          nulls,
          positionCount,
          buffer,
          std::vector<velox::BufferPtr>{});
    }
    case velox::TypeKind::TIMESTAMP: {
      velox::BufferPtr timestamps =
          velox::AlignedBuffer::allocate<velox::Timestamp>(positionCount, pool);
      auto* rawTimestamps = timestamps->asMutable<velox::Timestamp>();
      for (auto i = 0; i < positionCount; i++) {
        rawTimestamps[i] = velox::Timestamp(
            rawBuffer[i] / 1000, (rawBuffer[i] % 1000) * 1000000);
      }
      return std::make_shared<velox::FlatVector<velox::Timestamp>>(
          pool,
          type,
          nulls,
          positionCount,
          timestamps,
          std::vector<velox::BufferPtr>{});
    }
    case velox::TypeKind::BOOLEAN: {
      velox::BufferPtr bits =
          velox::AlignedBuffer::allocate<bool>(positionCount, pool);
      auto* rawBits = bits->asMutable<uint64_t>();
      for (auto i = 0; i < positionCount; i++) {
        velox::bits::setBit(rawBits, i, rawBuffer[i] != 0);
      }
      return std::make_shared<velox::FlatVector<bool>>(
          pool,
          type,
          nulls,
          positionCount,
          bits,
          std::vector<velox::BufferPtr>{});
    }
    default:
      VELOX_FAIL("Unexpected Block type: {}" + type->toString());
  }
}

velox::VectorPtr readVariableWidthBlock(
    const velox::TypePtr& type,
    ByteStream& stream,
    velox::memory::MemoryPool* pool) {
  auto positionCount = stream.read<int32_t>();

  velox::BufferPtr offsets =
      velox::AlignedBuffer::allocate<int32_t>(positionCount + 1, pool);
  auto rawOffsets = offsets->asMutable<int32_t>();
  rawOffsets[0] = 0;
  for (auto i = 0; i < positionCount; i++) {
    rawOffsets[i + 1] = stream.read<int32_t>();
  }

  auto nulls = readNulls(positionCount, stream, pool);

  auto totalSize = stream.read<int32_t>();

  velox::BufferPtr stringBuffer =
      velox::AlignedBuffer::allocate<char>(totalSize, pool);
  auto rawString = stringBuffer->asMutable<char>();
  stream.readBytes(totalSize, rawString);

  velox::BufferPtr buffer =
      velox::AlignedBuffer::allocate<velox::StringView>(positionCount, pool);
  auto rawBuffer = buffer->asMutable<velox::StringView>();
  for (auto i = 0; i < positionCount; i++) {
    auto size = rawOffsets[i + 1] - rawOffsets[i];
    rawBuffer[i] = velox::StringView(rawString + rawOffsets[i], size);
  }

  return std::make_shared<velox::FlatVector<velox::StringView>>(
      pool,
      type,
      nulls,
      positionCount,
      buffer,
      std::vector<velox::BufferPtr>{stringBuffer});
}

template <velox::TypeKind Kind>
velox::VectorPtr readScalarBlock(
    const std::string& encoding,
    const velox::TypePtr& type,
    ByteStream& stream,
    velox::memory::MemoryPool* pool) {
  using T = typename velox::TypeTraits<Kind>::NativeType;

  if (encoding == kLongArray) {
    return readScalarBlock<int64_t, T>(type, stream, pool);
  }

  if (encoding == kIntArray) {
    return readScalarBlock<int32_t, T>(type, stream, pool);
  }

  if (encoding == kShortArray) {
    return readScalarBlock<int16_t, T>(type, stream, pool);
  }

  if (encoding == kByteArray) {
    return readScalarBlock<int8_t, T>(type, stream, pool);
  }

  if (encoding == kVariableWidth) {
    return readVariableWidthBlock(type, stream, pool);
  }

  if (encoding == kInt128Array) {
    return readScalarBlock<velox::int128_t, T>(type, stream, pool);
  }
  VELOX_UNREACHABLE();
}

velox::VectorPtr readRleBlock(
    const velox::TypePtr& type,
    ByteStream& stream,
    velox::memory::MemoryPool* pool) {
  // read number of rows - must be just one
  auto positionCount = stream.read<int32_t>();

  // skip the encoding of the values
  auto encodingLength = stream.read<int32_t>();
  auto encoding = stream.readString(encodingLength);

  auto innerCount = stream.read<int32_t>();
  VELOX_CHECK_EQ(
      innerCount,
      1,
      "Unexpected RLE block. Expected single inner position. Got {}",
      innerCount);

  auto nulls = readNulls(1, stream, pool);
  if (!nulls || !velox::bits::isBitNull(nulls->as<uint64_t>(), 0)) {
    throw std::runtime_error("Unexpected RLE block. Expected single null.");
  }

  if (type->kind() == velox::TypeKind::SHORT_DECIMAL ||
      type->kind() == velox::TypeKind::LONG_DECIMAL) {
    return velox::BaseVector::createNullConstant(type, positionCount, pool);
  }

  velox::TypeKind typeKind;
  if (encoding == kByteArray) {
    typeKind = velox::TypeKind::UNKNOWN;
  } else if (encoding == kLongArray) {
    typeKind = velox::TypeKind::BIGINT;
  } else if (encoding == kIntArray) {
    typeKind = velox::TypeKind::INTEGER;
  } else if (encoding == kShortArray) {
    typeKind = velox::TypeKind::SMALLINT;
  } else if (encoding == kVariableWidth) {
    typeKind = velox::TypeKind::VARCHAR;
  } else {
    VELOX_FAIL("Unexpected RLE block encoding: {}", encoding);
  }

  return velox::BaseVector::createConstant(
      velox::variant(typeKind), positionCount, pool);
}

void unpackTimestampWithTimeZone(
    int64_t packed,
    int64_t& timestamp,
    int16_t& timezone) {
  timestamp = packed >> 12;
  timezone = packed & 0xfff;
}

// Common code to read number of rows, nulls and offsets for ARRAY and MAP.
auto readArrayOrMapFinalPart(
    ByteStream& stream,
    velox::memory::MemoryPool* pool) {
  auto positionCount = stream.read<int32_t>();

  velox::BufferPtr offsets =
      velox::AlignedBuffer::allocate<int32_t>(positionCount + 1, pool);
  auto rawOffsets = offsets->asMutable<int32_t>();
  for (auto i = 0; i < positionCount + 1; i++) {
    rawOffsets[i] = stream.read<int32_t>();
  }

  velox::BufferPtr nulls = readNulls(positionCount, stream, pool);

  velox::BufferPtr sizes =
      velox::AlignedBuffer::allocate<int32_t>(positionCount, pool);
  auto rawSizes = sizes->asMutable<int32_t>();
  for (auto i = 0; i < positionCount; i++) {
    rawSizes[i] = rawOffsets[i + 1] - rawOffsets[i];
  }

  struct result {
    velox::BufferPtr nulls;
    int32_t positionCount;
    velox::BufferPtr offsets;
    velox::BufferPtr sizes;
  };
  return result{nulls, positionCount, offsets, sizes};
}

velox::VectorPtr readBlockInt(
    const velox::TypePtr& type,
    ByteStream& stream,
    velox::memory::MemoryPool* pool) {
  // read the encoding
  auto encodingLength = stream.read<int32_t>();
  std::string encoding = stream.readString(encodingLength);

  if (encoding == kArray) {
    auto elements = readBlockInt(type->asArray().elementType(), stream, pool);

    auto [nulls, positionCount, offsets, sizes] =
        readArrayOrMapFinalPart(stream, pool);
    const auto arrayType = ARRAY(elements->type());
    return std::make_shared<velox::ArrayVector>(
        pool, arrayType, nulls, positionCount, offsets, sizes, elements);
  }

  if (encoding == kMap) {
    auto keys = readBlockInt(type->asMap().keyType(), stream, pool);
    auto values = readBlockInt(type->asMap().valueType(), stream, pool);

    // We aren't using hashtable.
    auto hashtableSize = stream.read<int32_t>();
    for (auto i = 0; i < hashtableSize; i++) {
      stream.read<int32_t>();
    }

    auto [nulls, positionCount, offsets, sizes] =
        readArrayOrMapFinalPart(stream, pool);
    const auto mapType = MAP(keys->type(), values->type());
    return std::make_shared<velox::MapVector>(
        pool, mapType, nulls, positionCount, offsets, sizes, keys, values);
  }

  if (encoding == kRle) {
    return readRleBlock(type, stream, pool);
  }

  if (type->kind() == velox::TypeKind::SHORT_DECIMAL) {
    return readScalarBlock<velox::TypeKind::SHORT_DECIMAL>(
        encoding, type, stream, pool);
  }

  if (type->kind() == velox::TypeKind::LONG_DECIMAL) {
    return readScalarBlock<velox::TypeKind::LONG_DECIMAL>(
        encoding, type, stream, pool);
  }

  if (type->kind() == velox::TypeKind::ROW &&
      isTimestampWithTimeZoneType(type)) {
    auto positionCount = stream.read<int32_t>();

    auto timestamps =
        velox::BaseVector::create(velox::BIGINT(), positionCount, pool);
    auto rawTimestamps =
        timestamps->asFlatVector<int64_t>()->mutableRawValues();

    auto timezones =
        velox::BaseVector::create(velox::SMALLINT(), positionCount, pool);
    auto rawTimezones = timezones->asFlatVector<int16_t>()->mutableRawValues();

    velox::BufferPtr nulls = readNulls(positionCount, stream, pool);
    const uint64_t* rawNulls =
        nulls == nullptr ? nullptr : nulls->as<uint64_t>();

    for (auto i = 0; i < positionCount; i++) {
      if (!rawNulls || !velox::bits::isBitNull(rawNulls, i)) {
        int64_t unpacked = stream.read<int64_t>();
        unpackTimestampWithTimeZone(
            unpacked, rawTimestamps[i], rawTimezones[i]);
      }
    }

    return std::make_shared<velox::RowVector>(
        pool,
        velox::TIMESTAMP_WITH_TIME_ZONE(),
        nulls,
        positionCount,
        std::vector<velox::VectorPtr>{timestamps, timezones});
  };

  return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
      readScalarBlock, type->kind(), encoding, type, stream, pool);
}

} // namespace

velox::VectorPtr readBlock(
    const velox::TypePtr& type,
    const std::string& base64Encoded,
    velox::memory::MemoryPool* pool) {
  const std::string data = velox::encoding::Base64::decode(base64Encoded);

  ByteStream stream(data.data());
  return readBlockInt(type, stream, pool);
}

} // namespace facebook::presto::protocol

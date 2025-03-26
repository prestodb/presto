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
#include "velox/serializers/PrestoSerializerDeserializationUtils.h"

#include "velox/functions/prestosql/types/UuidType.h"

namespace facebook::velox::serializer::presto::detail {
namespace {
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

bool hasNestedStructs(const TypePtr& type) {
  if (isIPPrefixType(type)) {
    return false;
  }
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

// Reads nulls into 'scratch' and returns count of non-nulls. If 'copy' is
// given, returns the null bits in 'copy'.
vector_size_t valueCount(
    ByteInputStream* source,
    vector_size_t size,
    Scratch& scratch,
    raw_vector<uint64_t>* copy = nullptr) {
  if (source->readByte() == 0 || size == 0) {
    return size;
  }
  ScratchPtr<uint64_t, 16> nullsHolder(scratch);
  auto rawNulls = nullsHolder.get(bits::nwords(size));
  auto numBytes = bits::nbytes(size);
  source->readBytes(rawNulls, numBytes);
  bits::reverseBits(reinterpret_cast<uint8_t*>(rawNulls), numBytes);
  bits::negate(rawNulls, numBytes * 8);
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

std::string readLengthPrefixedString(ByteInputStream* source) {
  int32_t size = source->read<int32_t>();
  std::string value;
  value.resize(size);
  source->readBytes(&value[0], size);
  return value;
}

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

void checkTypeEncoding(std::string_view encoding, const TypePtr& type) {
  const auto kindEncoding = typeToEncodingName(type);
  VELOX_USER_CHECK(
      encoding == kindEncoding,
      "Serialized encoding is not compatible with requested type: {}. Expected {}. Got {}.",
      type->kindName(),
      kindEncoding,
      encoding);
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
      const auto it = readers.find(
          isIPPrefixType(columnType) ? TypeKind::VARCHAR : columnType->kind());
      VELOX_CHECK(
          it != readers.end(),
          "Column reader for type {} is missing",
          columnType->kindName());

      it->second(source, columnType, useLosslessTimestamp, scratch);
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
  bits::negate(reinterpret_cast<uint64_t*>(rawNulls), numBytes * 8);
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

    vector_size_t toClear = offset;
    bits::forEachSetBit(
        nulls->as<uint64_t>(), offset, offset + size, [&](vector_size_t row) {
          // Set the values between the last non-null and this to type default.
          for (; toClear < row; ++toClear) {
            rawValues[toClear] = 0;
          }
          rawValues[row] = readJavaDecimal(source);
          toClear = row + 1;
        });
  } else {
    for (vector_size_t row = 0; row < size; ++row) {
      rawValues[offset + row] = readJavaDecimal(source);
    }
  }
}

int128_t readIpAddress(ByteInputStream* source) {
  // Java stores ipaddress as a binary, and thus the binary
  // is always in big endian byte order. In Velox, ipaddress
  // is a custom type with underlying type of int128_t, which
  // is always stored as little endian byte order. This means
  // to ensure compatibility between the coordinator and velox,
  // we need to actually convert the 16 bytes read from coordinator
  // to little endian.
  const int128_t beIpIntAddr = source->read<int128_t>();
  return reverseIpAddressByteOrder(beIpIntAddr);
}

void readIPPrefixValues(
    ByteInputStream* source,
    const TypePtr& type,
    vector_size_t resultOffset,
    const uint64_t* incomingNulls,
    int32_t numIncomingNulls,
    VectorPtr& result) {
  VELOX_DCHECK(isIPPrefixType(type));

  // Read # number of rows
  const int32_t size = source->read<int32_t>();
  const int32_t numNewValues = sizeWithIncomingNulls(size, numIncomingNulls);

  result->resize(resultOffset + numNewValues);

  // Skip # of offsets since we expect IPPrefix to be fixed width of 17 bytes
  source->skip(size * sizeof(int32_t));

  // Read the null-byte and null-flag if present.
  [[maybe_unused]] const auto numNulls = readNulls(
      source, size, resultOffset, incomingNulls, numIncomingNulls, *result);

  // Read total number of bytes of ipprefix
  const int32_t ipprefixBytesSum = source->read<int32_t>();
  if (ipprefixBytesSum == 0) {
    return;
  }

  VELOX_DCHECK(
      (ipprefixBytesSum % ipaddress::kIPPrefixBytes) == 0,
      fmt::format(
          "Total sum of ipprefix bytes:{} is not divisible by:{}. rows:{} numNulls:{} totalSize:{}",
          ipprefixBytesSum,
          ipaddress::kIPPrefixBytes,
          size,
          numNulls,
          result->size()));

  VELOX_DCHECK(
      result->size() >= numNulls,
      fmt::format(
          "IPPrefix received more nulls:{} than total num of rows:{}.",
          result->size(),
          numNulls));

  VELOX_DCHECK(
      (ipprefixBytesSum == ((size - numNulls) * ipaddress::kIPPrefixBytes)),
      fmt::format(
          "IPPrefix received invalid number of non-null bytes. Got:{} Expected:{} rows:{} numNulls:{} totalSize:{} numIncomingNulls={} resultOffset={}.",
          ipprefixBytesSum,
          (size - numNulls) * ipaddress::kIPPrefixBytes,
          size,
          numNulls,
          result->size(),
          numIncomingNulls,
          resultOffset));

  auto row = result->asChecked<RowVector>();
  auto ip = row->childAt(0)->asChecked<FlatVector<int128_t>>();
  auto prefix = row->childAt(1)->asChecked<FlatVector<int8_t>>();

  for (int32_t i = 0; i < numNewValues; ++i) {
    if (row->isNullAt(resultOffset + i)) {
      continue;
    }
    // Read 16 bytes and reverse the byte order
    ip->set(resultOffset + i, readIpAddress(source));
    // Read 1 byte for the prefix order
    prefix->set(resultOffset + i, source->read<int8_t>());
  }
}

void readIpAddressValues(
    ByteInputStream* source,
    vector_size_t size,
    vector_size_t offset,
    const BufferPtr& nulls,
    vector_size_t nullCount,
    const BufferPtr& values) {
  auto rawValues = values->asMutable<int128_t>();
  if (nullCount) {
    checkValuesSize<int128_t>(values, nulls, size, offset);

    vector_size_t toClear = offset;
    bits::forEachSetBit(
        nulls->as<uint64_t>(), offset, offset + size, [&](vector_size_t row) {
          // Set the values between the last non-null and this to type default.
          for (; toClear < row; ++toClear) {
            rawValues[toClear] = 0;
          }
          rawValues[row] = readIpAddress(source);
          toClear = row + 1;
        });
  } else {
    for (vector_size_t row = 0; row < size; ++row) {
      rawValues[offset + row] = readIpAddress(source);
    }
  }
}

int128_t readUuidValue(ByteInputStream* source) {
  // ByteInputStream does not support reading int128_t values.
  // UUIDs are serialized as 2 uint64 values with msb value first.
  auto high = folly::Endian::big(source->read<uint64_t>());
  auto low = folly::Endian::big(source->read<uint64_t>());
  return HugeInt::build(high, low);
}

void readUuidValues(
    ByteInputStream* source,
    vector_size_t size,
    vector_size_t offset,
    const BufferPtr& nulls,
    vector_size_t nullCount,
    const BufferPtr& values) {
  auto rawValues = values->asMutable<int128_t>();
  if (nullCount) {
    checkValuesSize<int128_t>(values, nulls, size, offset);

    vector_size_t toClear = offset;
    bits::forEachSetBit(
        nulls->as<uint64_t>(), offset, offset + size, [&](vector_size_t row) {
          // Set the values between the last non-null and this to type default.
          for (; toClear < row; ++toClear) {
            rawValues[toClear] = 0;
          }
          rawValues[row] = readUuidValue(source);
          toClear = row + 1;
        });
  } else {
    for (vector_size_t row = 0; row < size; ++row) {
      rawValues[offset + row] = readUuidValue(source);
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

template <typename T>
void read(
    ByteInputStream* source,
    const TypePtr& type,
    vector_size_t resultOffset,
    const uint64_t* incomingNulls,
    int32_t numIncomingNulls,
    velox::memory::MemoryPool* pool,
    const PrestoVectorSerde::PrestoOptions& opts,
    VectorPtr& result) {
  const int32_t size = source->read<int32_t>();
  const auto numNewValues = sizeWithIncomingNulls(size, numIncomingNulls);
  result->resize(resultOffset + numNewValues);

  auto* flatResult = result->asUnchecked<FlatVector<T>>();
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
  if constexpr (std::is_same_v<T, int128_t>) {
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
  }
  if (isUuidType(type)) {
    readUuidValues(
        source,
        numNewValues,
        resultOffset,
        flatResult->nulls(),
        nullCount,
        values);
    return;
  }
  if (isIPAddressType(type)) {
    readIpAddressValues(
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
    const PrestoVectorSerde::PrestoOptions& opts,
    VectorPtr& result) {
  if (isIPPrefixType(type)) {
    return readIPPrefixValues(
        source, type, resultOffset, incomingNulls, numIncomingNulls, result);
  }
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

template <>
void read<OpaqueType>(
    ByteInputStream* source,
    const TypePtr& type,
    vector_size_t resultOffset,
    const uint64_t* incomingNulls,
    int32_t numIncomingNulls,
    memory::MemoryPool* pool,
    const PrestoVectorSerde::PrestoOptions&,
    VectorPtr& result) {
  // Opaque values are serialized by first converting them to string
  // then serializing them as if they were string. The deserializable
  // does the reverse operation.

  const int32_t size = source->read<int32_t>();
  const int32_t numNewValues = sizeWithIncomingNulls(size, numIncomingNulls);

  result->resize(resultOffset + numNewValues);

  auto opaqueType = std::dynamic_pointer_cast<const OpaqueType>(type);
  auto deserialization = opaqueType->getDeserializeFunc();

  auto flatResult = result->as<FlatVector<std::shared_ptr<void>>>();
  BufferPtr values = flatResult->mutableValues(resultOffset + size);

  auto rawValues = values->asMutable<std::shared_ptr<void>>();
  std::vector<int32_t> offsets;
  int32_t lastOffset = 0;
  for (int32_t i = 0; i < numNewValues; ++i) {
    // Set the first int32_t of each StringView to be the offset.
    if (incomingNulls && bits::isBitNull(incomingNulls, i)) {
      offsets.push_back(lastOffset);
      continue;
    }
    lastOffset = source->read<int32_t>();
    offsets.push_back(lastOffset);
  }
  readNulls(
      source, size, resultOffset, incomingNulls, numIncomingNulls, *flatResult);

  const int32_t dataSize = source->read<int32_t>();
  if (dataSize == 0) {
    return;
  }

  BufferPtr newBuffer = AlignedBuffer::allocate<char>(dataSize, pool);
  char* rawString = newBuffer->asMutable<char>();
  source->readBytes(rawString, dataSize);
  int32_t previousOffset = 0;
  for (int32_t i = 0; i < numNewValues; ++i) {
    int32_t offset = offsets[i];
    auto sv = StringView(rawString + previousOffset, offset - previousOffset);
    auto opaqueValue = deserialization(sv);
    rawValues[resultOffset + i] = opaqueValue;
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
    const PrestoVectorSerde::PrestoOptions& opts,
    std::vector<VectorPtr>& result);

void readArrayVector(
    ByteInputStream* source,
    const TypePtr& type,
    vector_size_t resultOffset,
    const uint64_t* incomingNulls,
    int32_t numIncomingNulls,
    velox::memory::MemoryPool* pool,
    const PrestoVectorSerde::PrestoOptions& opts,
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
    const PrestoVectorSerde::PrestoOptions& opts,
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
    const PrestoVectorSerde::PrestoOptions& opts,
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

void readConstantVector(
    ByteInputStream* source,
    const TypePtr& type,
    vector_size_t resultOffset,
    const uint64_t* incomingNulls,
    int32_t numIncomingNulls,
    velox::memory::MemoryPool* pool,
    const PrestoVectorSerde::PrestoOptions& opts,
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
    const PrestoVectorSerde::PrestoOptions& opts,
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
    const PrestoVectorSerde::PrestoOptions& opts,
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
    const PrestoVectorSerde::PrestoOptions& opts,
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
          const PrestoVectorSerde::PrestoOptions& opts,
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
          {TypeKind::OPAQUE, &read<OpaqueType>},
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

      // If the column is ipprefix, we need to force the reader to be
      // varbinary so that we can properly deserialize the data from Java.
      const auto it = readers.find(
          isIPPrefixType(columnType) ? TypeKind::VARBINARY
                                     : columnType->kind());
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
} // namespace

void readTopColumns(
    ByteInputStream& source,
    const RowTypePtr& type,
    velox::memory::MemoryPool* pool,
    const RowVectorPtr& result,
    int32_t resultOffset,
    const PrestoVectorSerde::PrestoOptions& opts,
    bool singleColumn) {
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
} // namespace facebook::velox::serializer::presto::detail

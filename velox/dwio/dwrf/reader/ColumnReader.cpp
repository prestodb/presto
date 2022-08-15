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

#include "velox/dwio/dwrf/reader/ColumnReader.h"
#include "velox/dwio/common/IntCodecCommon.h"
#include "velox/dwio/common/IntDecoder.h"
#include "velox/dwio/common/TypeUtils.h"
#include "velox/dwio/common/exception/Exceptions.h"
#include "velox/dwio/dwrf/common/DecoderUtil.h"
#include "velox/dwio/dwrf/reader/ConstantColumnReader.h"
#include "velox/dwio/dwrf/reader/FlatMapColumnReader.h"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DictionaryVector.h"
#include "velox/vector/FlatVector.h"

#include <folly/Likely.h>
#include <folly/Portability.h>
#include <folly/String.h>

namespace facebook::velox::dwrf {

using dwio::common::IntDecoder;
using dwio::common::typeutils::CompatChecker;
using memory::MemoryPool;

// Buffer size for reading length stream
constexpr uint64_t BUFFER_SIZE = 1024;

// it's possible stride dictionary only contains zero length string. In that
// case, we still need to make batch point to a valid address
std::array<char, 1> EMPTY_DICT;

namespace detail {

void fillTimestamps(
    Timestamp* timestamps,
    const uint64_t* nullsPtr,
    const int64_t* secondsPtr,
    const uint64_t* nanosPtr,
    vector_size_t numValues) {
  for (vector_size_t i = 0; i < numValues; i++) {
    if (!nullsPtr || !bits::isBitNull(nullsPtr, i)) {
      auto nanos = nanosPtr[i];
      uint64_t zeros = nanos & 0x7;
      nanos >>= 3;
      if (zeros != 0) {
        for (uint64_t j = 0; j <= zeros; ++j) {
          nanos *= 10;
        }
      }
      auto seconds = secondsPtr[i] + dwio::common::EPOCH_OFFSET;
      if (seconds < 0 && nanos != 0) {
        seconds -= 1;
      }
      timestamps[i] = Timestamp(seconds, nanos);
    }
  }
}

} // namespace detail

inline RleVersion convertRleVersion(proto::ColumnEncoding_Kind kind) {
  switch (static_cast<int64_t>(kind)) {
    case proto::ColumnEncoding_Kind_DIRECT:
    case proto::ColumnEncoding_Kind_DICTIONARY:
      return RleVersion_1;
    case proto::ColumnEncoding_Kind_DIRECT_V2:
    case proto::ColumnEncoding_Kind_DICTIONARY_V2:
      return RleVersion_2;
    default:
      DWIO_RAISE("Unknown encoding in convertRleVersion");
  }
}

template <typename T>
FlatVector<T>* resetIfWrongFlatVectorType(VectorPtr& result) {
  return detail::resetIfWrongVectorType<FlatVector<T>>(result);
}

BufferPtr ColumnReader::readNulls(
    vector_size_t numValues,
    VectorPtr& result,
    const uint64_t* incomingNulls) {
  BufferPtr nulls;
  readNulls(numValues, incomingNulls, &result, nulls);
  return nulls;
}

void ColumnReader::readNulls(
    vector_size_t numValues,
    const uint64_t* incomingNulls,
    VectorPtr* result,
    BufferPtr& nulls) {
  if (!notNullDecoder_ && !incomingNulls) {
    nulls = nullptr;
    if (result && *result) {
      (*result)->resetNulls();
    }
    return;
  }
  auto numBytes = bits::nbytes(numValues);
  if (result && *result) {
    nulls = (*result)->mutableNulls(numValues + (simd::kPadding * 8));
    detail::resetIfNotWritable(*result, nulls);
  }
  if (!nulls || nulls->capacity() < numBytes + simd::kPadding) {
    nulls =
        AlignedBuffer::allocate<char>(numBytes + simd::kPadding, &memoryPool_);
  }
  nulls->setSize(numBytes);
  auto* nullsPtr = nulls->asMutable<uint64_t>();
  if (!notNullDecoder_) {
    memcpy(nullsPtr, incomingNulls, numBytes);
    return;
  }
  memset(nullsPtr, bits::kNotNullByte, numBytes);
  notNullDecoder_->next(
      reinterpret_cast<char*>(nullsPtr), numValues, incomingNulls);
}

ColumnReader::ColumnReader(
    std::shared_ptr<const dwio::common::TypeWithId> nodeType,
    StripeStreams& stripe,
    FlatMapContext flatMapContext)
    : nodeType_(std::move(nodeType)),
      memoryPool_(stripe.getMemoryPool()),
      flatMapContext_(std::move(flatMapContext)) {
  EncodingKey encodingKey{nodeType_->id, flatMapContext_.sequence};
  std::unique_ptr<dwio::common::SeekableInputStream> stream =
      stripe.getStream(encodingKey.forKind(proto::Stream_Kind_PRESENT), false);
  if (stream) {
    notNullDecoder_ = createBooleanRleDecoder(std::move(stream), encodingKey);
  }
}

uint64_t ColumnReader::skip(uint64_t numValues) {
  if (notNullDecoder_) {
    // page through the values that we want to skip
    // and count how many are non-null
    std::array<char, BUFFER_SIZE> buffer;
    constexpr auto bitCount = BUFFER_SIZE * 8;
    uint64_t remaining = numValues;
    while (remaining > 0) {
      uint64_t chunkSize = std::min(remaining, bitCount);
      notNullDecoder_->next(buffer.data(), chunkSize, nullptr);
      remaining -= chunkSize;
      numValues -= bits::countNulls(
          reinterpret_cast<uint64_t*>(buffer.data()), 0, chunkSize);
    }
  }
  return numValues;
}

/**
 * Expand an array of bytes in place to the corresponding bigger.
 * Has to work backwards so that they data isn't clobbered during the
 * expansion.
 * @param buffer the array of chars and array of longs that need to be
 *        expanded
 * @param numValues the number of bytes to convert to longs
 */
template <typename From, typename To>
std::enable_if_t<std::is_same_v<From, bool>> expandBytes(
    To* buffer,
    uint64_t numValues) {
  for (size_t i = numValues - 1; i < numValues; --i) {
    buffer[i] = static_cast<To>(bits::isBitSet(buffer, i));
  }
}

template <typename From, typename To>
std::enable_if_t<std::is_same_v<From, int8_t>> expandBytes(
    To* buffer,
    uint64_t numValues) {
  auto from = reinterpret_cast<int8_t*>(buffer);
  for (size_t i = numValues - 1; i < numValues; --i) {
    buffer[i] = static_cast<To>(from[i]);
  }
}

template <typename DataType, typename RequestedType>
class ByteRleColumnReader : public ColumnReader {
 private:
  std::unique_ptr<ByteRleDecoder> rle;

 public:
  ByteRleColumnReader(
      std::shared_ptr<const dwio::common::TypeWithId> nodeType,
      StripeStreams& stripe,
      std::function<std::unique_ptr<ByteRleDecoder>(
          std::unique_ptr<dwio::common::SeekableInputStream>,
          const EncodingKey&)> creator,
      FlatMapContext flatMapContext)
      : ColumnReader(std::move(nodeType), stripe, std::move(flatMapContext)) {
    EncodingKey encodingKey{nodeType_->id, flatMapContext_.sequence};
    rle = creator(
        stripe.getStream(encodingKey.forKind(proto::Stream_Kind_DATA), true),
        encodingKey);
  }
  ~ByteRleColumnReader() override = default;

  uint64_t skip(uint64_t numValues) override;

  void next(uint64_t numValues, VectorPtr& result, const uint64_t* nulls)
      override;
};

template <typename DataType, typename RequestedType>
uint64_t ByteRleColumnReader<DataType, RequestedType>::skip(
    uint64_t numValues) {
  numValues = ColumnReader::skip(numValues);
  rle->skip(numValues);
  return numValues;
}

template <typename T>
VectorPtr makeFlatVector(
    MemoryPool* pool,
    BufferPtr nulls,
    size_t nullCount,
    size_t length,
    BufferPtr values) {
  auto flatVector = std::make_shared<FlatVector<T>>(
      pool, nulls, length, values, std::vector<BufferPtr>());
  flatVector->setNullCount(nullCount);
  return flatVector;
}

template <typename DataType, typename RequestedType>
void ByteRleColumnReader<DataType, RequestedType>::next(
    uint64_t numValues,
    VectorPtr& result,
    const uint64_t* incomingNulls) {
  auto flatVector = resetIfWrongFlatVectorType<RequestedType>(result);
  BufferPtr values;
  if (flatVector) {
    values = flatVector->mutableValues(numValues);
  }

  BufferPtr nulls = readNulls(numValues, result, incomingNulls);
  const auto* nullsPtr = nulls ? nulls->as<uint64_t>() : nullptr;
  uint64_t nullCount = nullsPtr ? bits::countNulls(nullsPtr, 0, numValues) : 0;

  if (flatVector) {
    detail::resetIfNotWritable(result, values);
  }
  if (!values) {
    values = AlignedBuffer::allocate<RequestedType>(numValues, &memoryPool_);
  }
  values->setSize(BaseVector::byteSize<RequestedType>(numValues));

  if (result) {
    result->setSize(numValues);
    result->setNullCount(nullCount);
  } else {
    result = makeFlatVector<RequestedType>(
        &memoryPool_, nulls, nullCount, numValues, values);
  }

  // Since the byte rle places the output in a char* instead of long*,
  // we cheat here and use the long* and then expand it in a second pass.
  auto valuesPtr = values->asMutable<RequestedType>();
  rle->next(reinterpret_cast<char*>(valuesPtr), numValues, nullsPtr);

  // Handle upcast
  if constexpr (
      !std::is_same_v<DataType, RequestedType> &&
      (std::is_same_v<DataType, bool> ||
       sizeof(DataType) < sizeof(RequestedType))) {
    expandBytes<DataType>(valuesPtr, numValues);
  }
}

namespace {

template <class IntDecoderT, typename T>
struct TemplatedReadHelper;

template <class IntDecoderT>
struct TemplatedReadHelper<IntDecoderT, int16_t> {
  static void nextValues(
      IntDecoderT& decoder,
      int16_t* data,
      uint64_t numValues,
      const uint64_t* nulls) {
    decoder.nextShorts(data, numValues, nulls);
  }
};

template <class IntDecoderT>
struct TemplatedReadHelper<IntDecoderT, int32_t> {
  static void nextValues(
      IntDecoderT& decoder,
      int32_t* data,
      uint64_t numValues,
      const uint64_t* nulls) {
    decoder.nextInts(data, numValues, nulls);
  }
};

template <class IntDecoderT>
struct TemplatedReadHelper<IntDecoderT, int64_t> {
  static void nextValues(
      IntDecoderT& decoder,
      int64_t* data,
      uint64_t numValues,
      const uint64_t* nulls) {
    decoder.next(data, numValues, nulls);
  }
};

template <class IntDecoderT, typename T>
void nextValues(
    IntDecoderT& decoder,
    T* data,
    uint64_t numValues,
    const uint64_t* nulls) {
  TemplatedReadHelper<IntDecoderT, T>::nextValues(
      decoder, data, numValues, nulls);
}
} // namespace

template <class ReqT>
class IntegerDirectColumnReader : public ColumnReader {
 private:
  std::unique_ptr<dwio::common::IntDecoder</*isSigned*/ true>> ints;

 public:
  IntegerDirectColumnReader(
      std::shared_ptr<const dwio::common::TypeWithId> nodeType,
      StripeStreams& stripe,
      uint32_t numBytes,
      FlatMapContext flatMapContext = FlatMapContext::nonFlatMapContext());
  ~IntegerDirectColumnReader() override = default;

  uint64_t skip(uint64_t numValues) override;

  void next(uint64_t numValues, VectorPtr& result, const uint64_t* nulls)
      override;
};

template <class ReqT>
IntegerDirectColumnReader<ReqT>::IntegerDirectColumnReader(
    std::shared_ptr<const dwio::common::TypeWithId> nodeType,
    StripeStreams& stripe,
    uint32_t numBytes,
    FlatMapContext flatMapContext)
    : ColumnReader(std::move(nodeType), stripe, std::move(flatMapContext)) {
  EncodingKey encodingKey{nodeType_->id, flatMapContext_.sequence};
  auto data = encodingKey.forKind(proto::Stream_Kind_DATA);
  bool dataVInts = stripe.getUseVInts(data);
  if (stripe.format() == DwrfFormat::kDwrf) {
    ints = createDirectDecoder</*isSigned*/ true>(
        stripe.getStream(data, true), dataVInts, numBytes);
  } else {
    auto encoding = stripe.getEncoding(encodingKey);
    RleVersion vers = convertRleVersion(encoding.kind());
    ints = createRleDecoder</*isSigned*/ true>(
        stripe.getStream(data, true), vers, memoryPool_, dataVInts, numBytes);
  }
}

template <class ReqT>
uint64_t IntegerDirectColumnReader<ReqT>::skip(uint64_t numValues) {
  numValues = ColumnReader::skip(numValues);
  ints->skip(numValues);
  return numValues;
}

template <class ReqT>
void IntegerDirectColumnReader<ReqT>::next(
    uint64_t numValues,
    VectorPtr& result,
    const uint64_t* incomingNulls) {
  auto flatVector = resetIfWrongFlatVectorType<ReqT>(result);
  BufferPtr values;
  if (flatVector) {
    values = flatVector->mutableValues(numValues);
  }

  BufferPtr nulls = readNulls(numValues, result, incomingNulls);
  const auto* nullsPtr = nulls ? nulls->as<uint64_t>() : nullptr;
  uint64_t nullCount = nullsPtr ? bits::countNulls(nullsPtr, 0, numValues) : 0;

  if (flatVector) {
    detail::resetIfNotWritable(result, values);
  }
  if (!values) {
    values = AlignedBuffer::allocate<ReqT>(numValues, &memoryPool_);
  }

  if (result) {
    result->setSize(numValues);
    result->setNullCount(nullCount);
  } else {
    result =
        makeFlatVector<ReqT>(&memoryPool_, nulls, nullCount, numValues, values);
  }

  nextValues(*ints, values->asMutable<ReqT>(), numValues, nullsPtr);
}

template <class ReqT>
class IntegerDictionaryColumnReader : public ColumnReader {
 private:
  BufferPtr dictionary;
  BufferPtr inDictionary;
  std::unique_ptr<ByteRleDecoder> inDictionaryReader;
  std::unique_ptr<dwio::common::IntDecoder</* isSigned = */ false>> dataReader;
  uint64_t dictionarySize;
  std::function<BufferPtr()> dictInit;
  bool initialized_{false};

  BufferPtr allocateValues(uint64_t numValues, VectorPtr& result);

 public:
  IntegerDictionaryColumnReader(
      std::shared_ptr<const dwio::common::TypeWithId> nodeType,
      StripeStreams& stripe,
      uint32_t numBytes,
      FlatMapContext flatMapContext = FlatMapContext::nonFlatMapContext());
  ~IntegerDictionaryColumnReader() override = default;

  uint64_t skip(uint64_t numValues) override;

  void next(uint64_t numValues, VectorPtr& result, const uint64_t* nulls)
      override;

 private:
  template <typename T>
  void FOLLY_ALWAYS_INLINE
  setOutput(T* data, uint64_t index, const int64_t* dict, const char* inDict) {
    if (!inDict || bits::isBitSet(inDict, index)) {
      // data[index] is signed, but index to the dictionary should be unsigned.
      // So cast data[index] to unsigned
      auto val = static_cast<typename std::make_unsigned<T>::type>(data[index]);
      DWIO_ENSURE_LT(
          val,
          dictionarySize,
          "Index to dictionary (",
          val,
          ") is larger than dictionary size (",
          dictionarySize,
          ")");
      data[index] = static_cast<T>(dict[val]);
    }
  }

  template <typename T>
  void populateOutput(
      const int64_t* dict,
      T* data,
      uint64_t numValues,
      const uint64_t* nulls,
      const char* inDict) {
    if (nulls) {
      for (uint64_t i = 0; i < numValues; ++i) {
        if (!bits::isBitNull(nulls, i)) {
          setOutput(data, i, dict, inDict);
        }
      }
    } else {
      for (uint64_t i = 0; i < numValues; ++i) {
        setOutput(data, i, dict, inDict);
      }
    }
  }

  void ensureInitialized();
};

template <class ReqT>
IntegerDictionaryColumnReader<ReqT>::IntegerDictionaryColumnReader(
    std::shared_ptr<const dwio::common::TypeWithId> nodeType,
    StripeStreams& stripe,
    uint32_t numBytes,
    FlatMapContext flatMapContext)
    : ColumnReader(std::move(nodeType), stripe, std::move(flatMapContext)) {
  EncodingKey encodingKey{nodeType_->id, flatMapContext_.sequence};
  auto encoding = stripe.getEncoding(encodingKey);
  dictionarySize = encoding.dictionarysize();

  RleVersion vers = convertRleVersion(encoding.kind());
  auto data = encodingKey.forKind(proto::Stream_Kind_DATA);
  bool dataVInts = stripe.getUseVInts(data);
  dataReader = createRleDecoder</* isSigned = */ false>(
      stripe.getStream(data, true), vers, memoryPool_, dataVInts, numBytes);

  // make a lazy dictionary initializer
  dictInit = stripe.getIntDictionaryInitializerForNode(encodingKey, numBytes);

  auto inDictStream = stripe.getStream(
      encodingKey.forKind(proto::Stream_Kind_IN_DICTIONARY), false);
  if (inDictStream) {
    inDictionaryReader =
        createBooleanRleDecoder(std::move(inDictStream), encodingKey);
  }
}

template <class ReqT>
uint64_t IntegerDictionaryColumnReader<ReqT>::skip(uint64_t numValues) {
  numValues = ColumnReader::skip(numValues);
  dataReader->skip(numValues);
  if (inDictionaryReader) {
    inDictionaryReader->skip(numValues);
  }
  return numValues;
}

template <class ReqT>
void IntegerDictionaryColumnReader<ReqT>::next(
    uint64_t numValues,
    VectorPtr& result,
    const uint64_t* incomingNulls) {
  auto flatVector = resetIfWrongFlatVectorType<ReqT>(result);
  BufferPtr values;
  if (result) {
    values = flatVector->mutableValues(numValues);
  }

  BufferPtr nulls = readNulls(numValues, result, incomingNulls);
  const auto* nullsPtr = nulls ? nulls->as<uint64_t>() : nullptr;
  uint64_t nullCount = nullsPtr ? bits::countNulls(nullsPtr, 0, numValues) : 0;

  if (flatVector) {
    detail::resetIfNotWritable(result, values);
  }
  if (!values) {
    values = AlignedBuffer::allocate<ReqT>(numValues, &memoryPool_);
  }

  if (result) {
    result->setSize(numValues);
    result->setNullCount(nullCount);
  } else {
    result =
        makeFlatVector<ReqT>(&memoryPool_, nulls, nullCount, numValues, values);
  }

  // read the stream of booleans indicating whether a given data entry
  // is an offset or a literal value.
  const char* inDict = nullptr;
  if (inDictionaryReader) {
    detail::ensureCapacity<bool>(inDictionary, numValues, &memoryPool_);
    inDictionaryReader->next(
        inDictionary->asMutable<char>(), numValues, nullsPtr);
    inDict = inDictionary->as<char>();
  }

  // lazy load dictionary only when it's needed
  ensureInitialized();

  auto dict = dictionary->as<int64_t>();
  auto* valuesPtr = values->asMutable<ReqT>();
  nextValues(*dataReader, valuesPtr, numValues, nullsPtr);
  populateOutput(dict, valuesPtr, numValues, nullsPtr, inDict);
}

template <class ReqT>
void IntegerDictionaryColumnReader<ReqT>::ensureInitialized() {
  if (LIKELY(initialized_)) {
    return;
  }

  dictionary = dictInit();
  initialized_ = true;
}

class TimestampColumnReader : public ColumnReader {
 private:
  std::unique_ptr<dwio::common::IntDecoder</*isSigned*/ true>> seconds;
  std::unique_ptr<dwio::common::IntDecoder</*isSigned*/ false>> nano;

  BufferPtr secondsBuffer_;
  BufferPtr nanosBuffer_;

 public:
  TimestampColumnReader(
      std::shared_ptr<const dwio::common::TypeWithId> nodeType,
      StripeStreams& stripe,
      FlatMapContext flatMapContext);
  ~TimestampColumnReader() override = default;

  uint64_t skip(uint64_t numValues) override;

  void next(uint64_t numValues, VectorPtr& result, const uint64_t* nulls)
      override;
};

TimestampColumnReader::TimestampColumnReader(
    std::shared_ptr<const dwio::common::TypeWithId> nodeType,
    StripeStreams& stripe,
    FlatMapContext flatMapContext)
    : ColumnReader(std::move(nodeType), stripe, std::move(flatMapContext)) {
  EncodingKey encodingKey{nodeType_->id, flatMapContext_.sequence};
  RleVersion vers = convertRleVersion(stripe.getEncoding(encodingKey).kind());
  auto data = encodingKey.forKind(proto::Stream_Kind_DATA);
  bool vints = stripe.getUseVInts(data);
  seconds = createRleDecoder</*isSigned*/ true>(
      stripe.getStream(data, true),
      vers,
      memoryPool_,
      vints,
      dwio::common::LONG_BYTE_SIZE);
  auto nanoData = encodingKey.forKind(proto::Stream_Kind_NANO_DATA);
  bool nanoVInts = stripe.getUseVInts(nanoData);
  nano = createRleDecoder</*isSigned*/ false>(
      stripe.getStream(nanoData, true),
      vers,
      memoryPool_,
      nanoVInts,
      dwio::common::LONG_BYTE_SIZE);
}

uint64_t TimestampColumnReader::skip(uint64_t numValues) {
  numValues = ColumnReader::skip(numValues);
  seconds->skip(numValues);
  nano->skip(numValues);
  return numValues;
}

void TimestampColumnReader::next(
    uint64_t numValues,
    VectorPtr& result,
    const uint64_t* incomingNulls) {
  auto flatVector = resetIfWrongFlatVectorType<Timestamp>(result);
  BufferPtr values;
  if (flatVector) {
    values = flatVector->mutableValues(numValues);
  }

  BufferPtr nulls = readNulls(numValues, result, incomingNulls);
  const auto* nullsPtr = nulls ? nulls->as<uint64_t>() : nullptr;
  uint64_t nullCount = nullsPtr ? bits::countNulls(nullsPtr, 0, numValues) : 0;

  if (flatVector) {
    detail::resetIfNotWritable(result, values);
  }
  if (!values) {
    values = AlignedBuffer::allocate<Timestamp>(numValues, &memoryPool_);
  }

  if (result) {
    result->setSize(numValues);
    result->setNullCount(nullCount);
  } else {
    result = makeFlatVector<Timestamp>(
        &memoryPool_, nulls, nullCount, numValues, values);
  }

  detail::ensureCapacity<int64_t>(secondsBuffer_, numValues, &memoryPool_);
  detail::ensureCapacity<uint64_t>(nanosBuffer_, numValues, &memoryPool_);
  auto secondsData = secondsBuffer_->asMutable<int64_t>();
  auto nanosData = nanosBuffer_->asMutable<uint64_t>();
  seconds->next(secondsData, numValues, nullsPtr);
  nano->next(reinterpret_cast<int64_t*>(nanosData), numValues, nullsPtr);
  auto* valuesPtr = values->asMutable<Timestamp>();
  detail::fillTimestamps(
      valuesPtr, nullsPtr, secondsData, nanosData, numValues);
}

template <class T>
struct FpColumnReaderTraits {};

template <>
struct FpColumnReaderTraits<float> {
  using IntegerType = int32_t;
};

template <>
struct FpColumnReaderTraits<double> {
  using IntegerType = int64_t;
};

template <class DataT, class ReqT>
class FloatingPointColumnReader : public ColumnReader {
 public:
  FloatingPointColumnReader(
      std::shared_ptr<const dwio::common::TypeWithId> nodeType,
      StripeStreams& stripe,
      FlatMapContext flatMapContext);
  ~FloatingPointColumnReader() override = default;

  uint64_t skip(uint64_t numValues) override;

  void next(uint64_t numValues, VectorPtr& result, const uint64_t* nulls)
      override;

 private:
  std::unique_ptr<dwio::common::SeekableInputStream> inputStream;
  const char* bufferPointer;
  const char* bufferEnd;

  void readStreamIfRequired() {
    if (UNLIKELY(bufferPointer == bufferEnd)) {
      int32_t length;
      DWIO_ENSURE(
          inputStream->Next(
              reinterpret_cast<const void**>(&bufferPointer), &length),
          "bad read in FloatingPointColumnReader::next()");
      bufferEnd = bufferPointer + length;
    }
  }

  unsigned char readByte() {
    readStreamIfRequired();
    return static_cast<unsigned char>(*(bufferPointer++));
  }

  DataT readValue() {
    if (folly::kIsLittleEndian && bufferEnd - bufferPointer >= sizeof(DataT)) {
      const DataT* result = reinterpret_cast<const DataT*>(bufferPointer);
      bufferPointer += sizeof(DataT);
      return *result;
    }

    using IntegerType = typename FpColumnReaderTraits<DataT>::IntegerType;
    IntegerType bits = 0;
    for (IntegerType i = 0; i < sizeof(IntegerType); i++) {
      bits |= static_cast<IntegerType>(readByte()) << (i * 8);
    }
    DataT* result = reinterpret_cast<DataT*>(&bits);
    return *result;
  }
};

template <class DataT, class ReqT>
FloatingPointColumnReader<DataT, ReqT>::FloatingPointColumnReader(
    std::shared_ptr<const dwio::common::TypeWithId> nodeType,
    StripeStreams& stripe,
    FlatMapContext flatMapContext)
    : ColumnReader(std::move(nodeType), stripe, std::move(flatMapContext)),
      inputStream(stripe.getStream(
          EncodingKey{nodeType_->id, flatMapContext_.sequence}.forKind(
              proto::Stream_Kind_DATA),
          true)),
      bufferPointer(nullptr),
      bufferEnd(nullptr) {
  // PASS
}

template <class DataT, class ReqT>
uint64_t FloatingPointColumnReader<DataT, ReqT>::skip(uint64_t numValues) {
  numValues = ColumnReader::skip(numValues);
  auto remaining = static_cast<size_t>(bufferEnd - bufferPointer);
  auto toSkip = sizeof(DataT) * numValues;
  if (remaining >= toSkip) {
    bufferPointer += toSkip;
  } else {
    inputStream->Skip(static_cast<int32_t>(toSkip - remaining));
    bufferEnd = nullptr;
    bufferPointer = nullptr;
  }

  return numValues;
}

template <class DataT, class ReqT>
void FloatingPointColumnReader<DataT, ReqT>::next(
    uint64_t numValues,
    VectorPtr& result,
    const uint64_t* incomingNulls) {
  auto flatVector = resetIfWrongFlatVectorType<ReqT>(result);
  BufferPtr values;
  if (flatVector) {
    values = flatVector->mutableValues(numValues);
  }

  BufferPtr nulls = readNulls(numValues, result, incomingNulls);
  const auto* nullsPtr = nulls ? nulls->as<uint64_t>() : nullptr;
  uint64_t nullCount = nullsPtr ? bits::countNulls(nullsPtr, 0, numValues) : 0;

  if (flatVector) {
    detail::resetIfNotWritable(result, values);
  }
  if (!values) {
    values = AlignedBuffer::allocate<ReqT>(numValues, &memoryPool_);
  }

  if (result) {
    result->setSize(numValues);
    result->setNullCount(nullCount);
  } else {
    result =
        makeFlatVector<ReqT>(&memoryPool_, nulls, nullCount, numValues, values);
  }

  auto* valuesPtr = values->asMutable<ReqT>();
  if (nulls) {
    for (size_t i = 0; i < numValues; ++i) {
      if (!bits::isBitNull(nullsPtr, i)) {
        valuesPtr[i] = readValue();
      }
    }
  } else {
    if (folly::kIsLittleEndian && sizeof(DataT) == sizeof(ReqT)) {
      int32_t indexToWrite = 0;
      while (indexToWrite != numValues) {
        readStreamIfRequired();
        size_t validElements = (bufferEnd - bufferPointer) / sizeof(DataT);
        size_t elementsToCopy = std::min(
            static_cast<uint64_t>(validElements), numValues - indexToWrite);
        size_t bytesToCopy = (elementsToCopy * sizeof(DataT));
        std::copy(
            bufferPointer,
            bufferPointer + bytesToCopy,
            reinterpret_cast<char*>(valuesPtr) +
                (indexToWrite * sizeof(DataT)));
        bufferPointer += bytesToCopy;
        indexToWrite += elementsToCopy;

        // If a value crosses the boundary we can be in a state where
        // index != numValues and bufferPointer != bufferEnd. In this
        // state reading a single value causes a read of the stream in order
        // to decode the next value after which we can resume the fast copy.
        if ((indexToWrite != numValues) && (bufferPointer != bufferEnd)) {
          valuesPtr[indexToWrite++] = readValue();
        }
      }
    } else {
      for (size_t i = 0; i < numValues; ++i) {
        valuesPtr[i] = readValue();
      }
    }
  }
}

class StringDictionaryColumnReader : public ColumnReader {
 private:
  void loadStrideDictionary();

  BufferPtr dictionaryBlob;
  BufferPtr dictionaryOffset;
  BufferPtr inDict;
  BufferPtr strideDict;
  BufferPtr strideDictOffset;
  BufferPtr indices_;
  std::unique_ptr<dwio::common::IntDecoder</*isSigned*/ false>> dictIndex;
  std::unique_ptr<ByteRleDecoder> inDictionaryReader;
  std::unique_ptr<dwio::common::SeekableInputStream> strideDictStream;
  std::unique_ptr<dwio::common::IntDecoder</*isSigned*/ false>>
      strideDictLengthDecoder;

  FlatVectorPtr<StringView> combinedDictionaryValues_;
  FlatVectorPtr<StringView> dictionaryValues_;

  uint64_t dictionaryCount;
  uint64_t strideDictCount;
  int64_t lastStrideIndex;
  size_t positionOffset;
  size_t strideDictSizeOffset;

  std::unique_ptr<dwio::common::SeekableInputStream> indexStream_;
  std::unique_ptr<proto::RowIndex> rowIndex_;
  const StrideIndexProvider& provider;

  // lazy load the dictionary
  std::unique_ptr<dwio::common::IntDecoder</*isSigned*/ false>> lengthDecoder;
  std::unique_ptr<dwio::common::SeekableInputStream> blobStream;
  const bool returnFlatVector_;
  bool initialized_{false};

  BufferPtr loadDictionary(
      uint64_t count,
      dwio::common::SeekableInputStream& data,
      IntDecoder</*isSigned*/ false>& lengthDecoder,
      BufferPtr& offsets);

  bool FOLLY_ALWAYS_INLINE setOutput(
      uint64_t index,
      int64_t dictIndex,
      const char* dict,
      const int64_t* dictOffsets,
      const char* strideDict,
      const int64_t* strideDictOffsets,
      const char* inDict,
      const char*& outputStarts,
      int64_t& outputLengths) const;

  void readDictionaryVector(
      uint64_t numValues,
      VectorPtr& result,
      const uint64_t* nulls);

  void
  readFlatVector(uint64_t numValues, VectorPtr& result, const uint64_t* nulls);

  void ensureInitialized();

 public:
  StringDictionaryColumnReader(
      std::shared_ptr<const dwio::common::TypeWithId> nodeType,
      StripeStreams& stripe,
      FlatMapContext flatMapContext = FlatMapContext::nonFlatMapContext());
  ~StringDictionaryColumnReader() override = default;

  uint64_t skip(uint64_t numValues) override;

  void next(uint64_t numValues, VectorPtr& result, const uint64_t* nulls)
      override;
};

StringDictionaryColumnReader::StringDictionaryColumnReader(
    std::shared_ptr<const dwio::common::TypeWithId> nodeType,
    StripeStreams& stripe,
    FlatMapContext flatMapContext)
    : ColumnReader(std::move(nodeType), stripe, std::move(flatMapContext)),
      lastStrideIndex(-1),
      provider(stripe.getStrideIndexProvider()),
      returnFlatVector_(stripe.getRowReaderOptions().getReturnFlatVector()) {
  EncodingKey encodingKey{nodeType_->id, flatMapContext_.sequence};
  RleVersion rleVersion =
      convertRleVersion(stripe.getEncoding(encodingKey).kind());
  dictionaryCount = stripe.getEncoding(encodingKey).dictionarysize();

  const auto dataId = encodingKey.forKind(proto::Stream_Kind_DATA);
  bool dictVInts = stripe.getUseVInts(dataId);
  dictIndex = createRleDecoder</*isSigned*/ false>(
      stripe.getStream(dataId, true),
      rleVersion,
      memoryPool_,
      dictVInts,
      dwio::common::INT_BYTE_SIZE);

  const auto lenId = encodingKey.forKind(proto::Stream_Kind_LENGTH);
  bool lenVInts = stripe.getUseVInts(lenId);
  lengthDecoder = createRleDecoder</*isSigned*/ false>(
      stripe.getStream(lenId, false),
      rleVersion,
      memoryPool_,
      lenVInts,
      dwio::common::INT_BYTE_SIZE);

  blobStream = stripe.getStream(
      encodingKey.forKind(proto::Stream_Kind_DICTIONARY_DATA), false);

  // handle in dictionary stream
  std::unique_ptr<dwio::common::SeekableInputStream> inDictStream =
      stripe.getStream(
          encodingKey.forKind(proto::Stream_Kind_IN_DICTIONARY), false);
  if (inDictStream) {
    inDictionaryReader =
        createBooleanRleDecoder(std::move(inDictStream), encodingKey);

    // stride dictionary only exists if in dictionary exists
    strideDictStream = stripe.getStream(
        encodingKey.forKind(proto::Stream_Kind_STRIDE_DICTIONARY), true);
    DWIO_ENSURE_NOT_NULL(strideDictStream, "Stride dictionary is missing");

    indexStream_ = stripe.getStream(
        encodingKey.forKind(proto::Stream_Kind_ROW_INDEX), true);
    DWIO_ENSURE_NOT_NULL(indexStream_, "String index is missing");

    const auto strideDictLenId =
        encodingKey.forKind(proto::Stream_Kind_STRIDE_DICTIONARY_LENGTH);
    bool strideLenVInt = stripe.getUseVInts(strideDictLenId);
    strideDictLengthDecoder = createRleDecoder</*isSigned*/ false>(
        stripe.getStream(strideDictLenId, true),
        rleVersion,
        memoryPool_,
        strideLenVInt,
        dwio::common::INT_BYTE_SIZE);
  }
}

uint64_t StringDictionaryColumnReader::skip(uint64_t numValues) {
  numValues = ColumnReader::skip(numValues);
  dictIndex->skip(numValues);
  if (inDictionaryReader) {
    inDictionaryReader->skip(numValues);
  }
  return numValues;
}

BufferPtr StringDictionaryColumnReader::loadDictionary(
    uint64_t count,
    dwio::common::SeekableInputStream& data,
    IntDecoder</*isSigned*/ false>& lengthDecoder,
    BufferPtr& offsets) {
  // read lengths from length reader
  auto* offsetsPtr = offsets->asMutable<int64_t>();
  offsetsPtr[0] = 0;
  lengthDecoder.next(offsetsPtr + 1, count, nullptr);

  // set up array that keeps offset of start positions of individual entries
  // in the dictionary
  for (uint64_t i = 1; i < count + 1; ++i) {
    offsetsPtr[i] += offsetsPtr[i - 1];
  }

  // read bytes from underlying string
  int64_t blobSize = offsetsPtr[count];
  BufferPtr dictionary = AlignedBuffer::allocate<char>(blobSize, &memoryPool_);
  data.readFully(dictionary->asMutable<char>(), blobSize);
  return dictionary;
}

void StringDictionaryColumnReader::loadStrideDictionary() {
  auto nextStride = provider.getStrideIndex();
  if (nextStride == lastStrideIndex) {
    return;
  }

  // get stride dictionary size and load it if needed
  auto& positions = rowIndex_->entry(nextStride).positions();
  strideDictCount = positions.Get(strideDictSizeOffset);
  if (strideDictCount > 0) {
    // seek stride dictionary related streams
    std::vector<uint64_t> pos(
        positions.begin() + positionOffset, positions.end());
    dwio::common::PositionProvider pp(pos);
    strideDictStream->seekToPosition(pp);
    strideDictLengthDecoder->seekToRowGroup(pp);

    detail::ensureCapacity<int64_t>(
        strideDictOffset, strideDictCount + 1, &memoryPool_);
    strideDict = loadDictionary(
        strideDictCount,
        *strideDictStream,
        *strideDictLengthDecoder,
        strideDictOffset);
  } else {
    strideDict.reset();
  }

  lastStrideIndex = nextStride;

  dictionaryValues_.reset();
  combinedDictionaryValues_.reset();
}

bool /* FOLLY_ALWAYS_INLINE */ StringDictionaryColumnReader::setOutput(
    uint64_t index,
    int64_t dictIndex,
    const char* dict,
    const int64_t* dictOffsets,
    const char* strideDict,
    const int64_t* strideDictOffsets,
    const char* inDict,
    const char*& outputStarts,
    int64_t& outputLengths) const {
  const char* data;
  const int64_t* offsets;
  uint64_t dictCount;
  bool hasStrideDict = false;
  if (!inDict || bits::isBitSet(inDict, index)) {
    data = dict;
    offsets = dictOffsets;
    dictCount = dictionaryCount;
  } else {
    DWIO_ENSURE_NOT_NULL(strideDict);
    DWIO_ENSURE_NOT_NULL(strideDictOffsets);
    data = strideDict;
    offsets = strideDictOffsets;
    dictCount = strideDictCount;
    hasStrideDict = true;
  }
  DWIO_ENSURE_LT(
      dictIndex,
      dictCount,
      "Index to dictionary (",
      dictIndex,
      ") is larger than dictionary size (",
      dictCount,
      ")");
  outputStarts = data + offsets[dictIndex];
  outputLengths = offsets[dictIndex + 1] - offsets[dictIndex];
  return hasStrideDict;
}

void StringDictionaryColumnReader::next(
    uint64_t numValues,
    VectorPtr& result,
    const uint64_t* incomingNulls) {
  // lazy loading dictionary data when first hit
  ensureInitialized();

  const char* strideDictBlob = nullptr;
  if (inDictionaryReader) {
    loadStrideDictionary();
    if (strideDict) {
      DWIO_ENSURE_NOT_NULL(strideDictOffset);

      // It's possible strideDictBlob is nullptr when stride dictionary only
      // contains empty string. In that case, we need to make sure
      // strideDictBlob point to some valid address, and the last entry of
      // strideDictOffset have value 0.
      strideDictBlob = strideDict->as<char>();
      if (!strideDictBlob) {
        strideDictBlob = EMPTY_DICT.data();
        DWIO_ENSURE_EQ(strideDictOffset->as<int64_t>()[strideDictCount], 0);
      }
    }
  }

  if (returnFlatVector_) {
    readFlatVector(numValues, result, incomingNulls);
  } else {
    readDictionaryVector(numValues, result, incomingNulls);
  }
}

void StringDictionaryColumnReader::readDictionaryVector(
    uint64_t numValues,
    VectorPtr& result,
    const uint64_t* incomingNulls) {
  auto dictVector =
      detail::resetIfWrongVectorType<DictionaryVector<StringView>>(result);
  BufferPtr indices;
  if (dictVector) {
    indices = dictVector->mutableIndices(numValues);
  }

  BufferPtr nulls = readNulls(numValues, result, incomingNulls);
  const auto* nullsPtr = nulls ? nulls->as<uint64_t>() : nullptr;
  uint64_t nullCount = nullsPtr ? bits::countNulls(nullsPtr, 0, numValues) : 0;

  if (result) {
    detail::resetIfNotWritable(result, indices);
  }
  if (!indices) {
    indices = AlignedBuffer::allocate<vector_size_t>(numValues, &memoryPool_);
  }

  auto indicesPtr = indices->asMutable<vector_size_t>();
  dictIndex->nextInts(indicesPtr, numValues, nullsPtr);
  indices->setSize(numValues * sizeof(vector_size_t));

  bool hasStrideDict = false;

  // load inDictionary
  const char* inDictPtr = nullptr;
  if (inDictionaryReader) {
    detail::ensureCapacity<bool>(inDict, numValues, &memoryPool_);
    inDictionaryReader->next(inDict->asMutable<char>(), numValues, nullsPtr);
    inDictPtr = inDict->as<char>();
  }

  if (nulls) {
    for (uint64_t i = 0; i < numValues; ++i) {
      if (!bits::isBitNull(nullsPtr, i)) {
        if (!inDictPtr || bits::isBitSet(inDictPtr, i)) {
          // points to an entry in rowgroup dictionary
        } else {
          // points to an entry in stride dictionary
          indicesPtr[i] += dictionaryCount;
          hasStrideDict = true;
        }
      }
    }
  } else {
    for (uint64_t i = 0; i < numValues; ++i) {
      if (!inDictPtr || bits::isBitSet(inDictPtr, i)) {
        // points to an entry in rowgroup dictionary
      } else {
        // points to an entry in stride dictionary
        indicesPtr[i] += dictionaryCount;
        hasStrideDict = true;
      }
    }
  }

  VectorPtr dictionaryValues;
  const auto* dictionaryBlobPtr = dictionaryBlob->as<char>();
  const auto* dictionaryOffsetsPtr = dictionaryOffset->as<int64_t>();
  if (hasStrideDict) {
    if (!combinedDictionaryValues_) {
      // TODO Reuse memory
      BufferPtr values = AlignedBuffer::allocate<StringView>(
          dictionaryCount + strideDictCount, &memoryPool_);
      auto* valuesPtr = values->asMutable<StringView>();
      for (size_t i = 0; i < dictionaryCount; i++) {
        valuesPtr[i] = StringView(
            dictionaryBlobPtr + dictionaryOffsetsPtr[i],
            dictionaryOffsetsPtr[i + 1] - dictionaryOffsetsPtr[i]);
      }

      const auto* strideDictPtr = strideDict->as<char>();
      const auto* strideDictOffsetPtr = strideDictOffset->as<int64_t>();
      for (size_t i = 0; i < strideDictCount; i++) {
        valuesPtr[dictionaryCount + i] = StringView(
            strideDictPtr + strideDictOffsetPtr[i],
            strideDictOffsetPtr[i + 1] - strideDictOffsetPtr[i]);
      }

      combinedDictionaryValues_ = std::make_shared<FlatVector<StringView>>(
          &memoryPool_,
          nodeType_->type,
          BufferPtr(nullptr), // TODO nulls
          dictionaryCount + strideDictCount /*length*/,
          values,
          std::vector<BufferPtr>{dictionaryBlob, strideDict});
    }

    dictionaryValues = combinedDictionaryValues_;
  } else {
    if (!dictionaryValues_) {
      // TODO Reuse memory
      BufferPtr values =
          AlignedBuffer::allocate<StringView>(dictionaryCount, &memoryPool_);
      auto* valuesPtr = values->asMutable<StringView>();
      for (size_t i = 0; i < dictionaryCount; i++) {
        valuesPtr[i] = StringView(
            dictionaryBlobPtr + dictionaryOffsetsPtr[i],
            dictionaryOffsetsPtr[i + 1] - dictionaryOffsetsPtr[i]);
      }

      dictionaryValues_ = std::make_shared<FlatVector<StringView>>(
          &memoryPool_,
          nodeType_->type,
          BufferPtr(nullptr), // TODO nulls
          dictionaryCount /*length*/,
          values,
          std::vector<BufferPtr>{dictionaryBlob});
    }
    dictionaryValues = dictionaryValues_;
  }

  if (result) {
    result->setSize(numValues);
    result->setNullCount(nullCount);
    result->as<DictionaryVector<StringView>>()->setDictionaryValues(
        dictionaryValues);
  } else {
    result = std::make_shared<DictionaryVector<StringView>>(
        &memoryPool_, nulls, numValues, dictionaryValues, indices);
    result->setNullCount(nullCount);
  }
}

void StringDictionaryColumnReader::readFlatVector(
    uint64_t numValues,
    VectorPtr& result,
    const uint64_t* incomingNulls) {
  auto flatVector = resetIfWrongFlatVectorType<StringView>(result);
  BufferPtr data;
  if (flatVector) {
    data = flatVector->mutableValues(numValues);
  }

  BufferPtr nulls = readNulls(numValues, result, incomingNulls);
  const auto* nullsPtr = nulls ? nulls->as<uint64_t>() : nullptr;
  uint64_t nullCount = nullsPtr ? bits::countNulls(nullsPtr, 0, numValues) : 0;

  if (result) {
    detail::resetIfNotWritable(result, data);
  }
  if (!data) {
    data = AlignedBuffer::allocate<StringView>(numValues, &memoryPool_);
  }

  // load inDictionary
  const char* inDictPtr = nullptr;
  if (inDictionaryReader) {
    detail::ensureCapacity<bool>(inDict, numValues, &memoryPool_);
    inDictionaryReader->next(inDict->asMutable<char>(), numValues, nullsPtr);
    inDictPtr = inDict->as<char>();
  }
  auto dataPtr = data->asMutable<StringView>();

  // read indices
  if (!indices_ || indices_->capacity() < numValues * sizeof(int64_t)) {
    indices_ = AlignedBuffer::allocate<int64_t>(numValues, &memoryPool_);
  }
  auto indices = indices_->asMutable<int64_t>();
  dictIndex->next(indices, numValues, nullsPtr);

  const char* strideDictPtr = nullptr;
  int64_t* strideDictOffsetPtr = nullptr;
  if (strideDict) {
    strideDictPtr = strideDict->as<char>();
    strideDictOffsetPtr = strideDictOffset->asMutable<int64_t>();
  }
  auto* dictionaryBlobPtr = dictionaryBlob->as<char>();
  auto* dictionaryOffsetsPtr = dictionaryOffset->asMutable<int64_t>();
  bool hasStrideDict = false;
  const char* strData;
  int64_t strLen;
  if (nulls) {
    for (uint64_t i = 0; i < numValues; ++i) {
      if (!bits::isBitNull(nullsPtr, i)) {
        hasStrideDict = setOutput(
                            i,
                            indices[i],
                            dictionaryBlobPtr,
                            dictionaryOffsetsPtr,
                            strideDictPtr,
                            strideDictOffsetPtr,
                            inDictPtr,
                            strData,
                            strLen) ||
            hasStrideDict;
        dataPtr[i] = StringView{strData, static_cast<uint32_t>(strLen)};
      }
    }
  } else {
    for (uint64_t i = 0; i < numValues; ++i) {
      hasStrideDict = setOutput(
                          i,
                          indices[i],
                          dictionaryBlobPtr,
                          dictionaryOffsetsPtr,
                          strideDictPtr,
                          strideDictOffsetPtr,
                          inDictPtr,
                          strData,
                          strLen) ||
          hasStrideDict;
      dataPtr[i] = StringView{strData, static_cast<uint32_t>(strLen)};
    }
  }
  std::vector<BufferPtr> stringBuffers = {dictionaryBlob};
  if (hasStrideDict) {
    stringBuffers.emplace_back(strideDict);
  }
  if (result) {
    result->setSize(numValues);
    result->setNullCount(nullCount);
    flatVector->setStringBuffers(stringBuffers);
  } else {
    result = std::make_shared<FlatVector<StringView>>(
        &memoryPool_,
        nulls,
        numValues,
        data,
        std::vector<BufferPtr>{stringBuffers});
    result->setNullCount(nullCount);
  }
}

void StringDictionaryColumnReader::ensureInitialized() {
  if (LIKELY(initialized_)) {
    return;
  }

  detail::ensureCapacity<int64_t>(
      dictionaryOffset, dictionaryCount + 1, &memoryPool_);
  dictionaryBlob = loadDictionary(
      dictionaryCount, *blobStream, *lengthDecoder, dictionaryOffset);
  dictionaryValues_.reset();
  combinedDictionaryValues_.reset();

  // handle in dictionary stream
  if (inDictionaryReader) {
    // load stride dictionary offsets
    rowIndex_ = ProtoUtils::readProto<proto::RowIndex>(std::move(indexStream_));
    auto indexStartOffset = flatMapContext_.inMapDecoder
        ? flatMapContext_.inMapDecoder->loadIndices(0)
        : 0;
    positionOffset = notNullDecoder_
        ? notNullDecoder_->loadIndices(indexStartOffset)
        : indexStartOffset;
    size_t offset = strideDictStream->positionSize() + positionOffset;
    strideDictSizeOffset = strideDictLengthDecoder->loadIndices(offset);
  }
  initialized_ = true;
}

class StringDirectColumnReader : public ColumnReader {
 private:
  std::unique_ptr<dwio::common::IntDecoder</*isSigned*/ false>> length;
  std::unique_ptr<dwio::common::SeekableInputStream> blobStream;

  /**
   * Compute the total length of the values.
   * @param lengths the array of lengths
   * @param nulls the array of null flags
   * @param numValues the lengths of the arrays
   * @return the total number of bytes for the non-null values
   */
  size_t computeSize(
      const int64_t* lengths,
      const uint64_t* nulls,
      uint64_t numValues);

 public:
  StringDirectColumnReader(
      std::shared_ptr<const dwio::common::TypeWithId> nodeType,
      StripeStreams& stripe,
      FlatMapContext flatMapContext);
  ~StringDirectColumnReader() override = default;

  uint64_t skip(uint64_t numValues) override;

  void next(uint64_t numValues, VectorPtr& result, const uint64_t* nulls)
      override;
};

StringDirectColumnReader::StringDirectColumnReader(
    std::shared_ptr<const dwio::common::TypeWithId> nodeType,
    StripeStreams& stripe,
    FlatMapContext flatMapContext)
    : ColumnReader(std::move(nodeType), stripe, std::move(flatMapContext)) {
  EncodingKey encodingKey{nodeType_->id, flatMapContext_.sequence};
  RleVersion rleVersion =
      convertRleVersion(stripe.getEncoding(encodingKey).kind());
  auto lenId = encodingKey.forKind(proto::Stream_Kind_LENGTH);
  bool lenVInts = stripe.getUseVInts(lenId);
  length = createRleDecoder</*isSigned*/ false>(
      stripe.getStream(lenId, true),
      rleVersion,
      memoryPool_,
      lenVInts,
      dwio::common::INT_BYTE_SIZE);
  blobStream =
      stripe.getStream(encodingKey.forKind(proto::Stream_Kind_DATA), true);
}

uint64_t StringDirectColumnReader::skip(uint64_t numValues) {
  numValues = ColumnReader::skip(numValues);
  std::array<int64_t, BUFFER_SIZE> buffer;
  uint64_t done = 0;
  size_t totalBytes = 0;
  // read the lengths, so we know haw many bytes to skip
  while (done < numValues) {
    uint64_t step = std::min(BUFFER_SIZE, numValues - done);
    length->next(buffer.data(), step, nullptr);
    totalBytes += computeSize(buffer.data(), nullptr, step);
    done += step;
  }
  blobStream->Skip(static_cast<int32_t>(totalBytes));
  return numValues;
}

size_t StringDirectColumnReader::computeSize(
    const int64_t* lengths,
    const uint64_t* nulls,
    uint64_t numValues) {
  size_t totalLength = 0;
  if (nulls) {
    for (size_t i = 0; i < numValues; ++i) {
      if (!bits::isBitNull(nulls, i)) {
        totalLength += static_cast<size_t>(lengths[i]);
      }
    }
  } else {
    for (size_t i = 0; i < numValues; ++i) {
      totalLength += static_cast<size_t>(lengths[i]);
    }
  }
  return totalLength;
}

void StringDirectColumnReader::next(
    uint64_t numValues,
    VectorPtr& result,
    const uint64_t* incomingNulls) {
  auto flatVector = resetIfWrongFlatVectorType<StringView>(result);
  BufferPtr values;
  if (flatVector) {
    values = flatVector->mutableValues(numValues);
  }

  BufferPtr nulls = readNulls(numValues, result, incomingNulls);
  const auto* nullsPtr = nulls ? nulls->as<uint64_t>() : nullptr;
  uint64_t nullCount = nullsPtr ? bits::countNulls(nullsPtr, 0, numValues) : 0;

  if (flatVector) {
    detail::resetIfNotWritable(result, values);
  }
  if (!values) {
    values = AlignedBuffer::allocate<StringView>(numValues, &memoryPool_);
  }

  // TODO Reuse memory
  // read the length vector
  BufferPtr lengths = AlignedBuffer::allocate<int64_t>(numValues, &memoryPool_);
  length->next(lengths->asMutable<int64_t>(), numValues, nullsPtr);

  // figure out the total length of data we need fom the blob stream
  const auto* lengthsPtr = lengths->as<int64_t>();
  const size_t totalLength = computeSize(lengthsPtr, nullsPtr, numValues);

  // TODO Reuse memory
  // Load data from the blob stream into our buffer until we have enough
  // to get the rest directly out of the stream's buffer.
  BufferPtr data = AlignedBuffer::allocate<char>(totalLength, &memoryPool_);
  blobStream->readFully(data->asMutable<char>(), totalLength);

  auto* valuesPtr = values->asMutable<StringView>();
  const auto* dataPtr = data->as<char>();
  // Set up the start pointers for the ones that will come out of the buffer.
  uint64_t usedBytes = 0;
  if (nulls) {
    for (uint64_t i = 0; i < numValues; ++i) {
      if (!bits::isBitNull(nullsPtr, i)) {
        valuesPtr[i] = StringView(dataPtr + usedBytes, lengthsPtr[i]);
        usedBytes += lengthsPtr[i];
      }
    }
  } else {
    for (uint64_t i = 0; i < numValues; ++i) {
      valuesPtr[i] = StringView(dataPtr + usedBytes, lengthsPtr[i]);
      usedBytes += lengthsPtr[i];
    }
  }

  if (result) {
    result->setSize(numValues);
    result->setNullCount(nullCount);
    flatVector->setStringBuffers(std::vector<BufferPtr>{data});
  } else {
    result = std::make_shared<FlatVector<StringView>>(
        &memoryPool_,
        nodeType_->type,
        nulls,
        numValues,
        values,
        std::vector<BufferPtr>{data});
    result->setNullCount(nullCount);
  }
}

class StructColumnReader : public ColumnReader {
 private:
  const std::shared_ptr<const dwio::common::TypeWithId> requestedType_;
  std::vector<std::unique_ptr<ColumnReader>> children_;

 public:
  StructColumnReader(
      const std::shared_ptr<const dwio::common::TypeWithId>& requestedType,
      const std::shared_ptr<const dwio::common::TypeWithId>& dataType,
      StripeStreams& stripe,
      FlatMapContext flatMapContext);
  ~StructColumnReader() override = default;

  uint64_t skip(uint64_t numValues) override;

  void next(uint64_t numValues, VectorPtr& result, const uint64_t* nulls)
      override;
};

// From reading side - all sequences are by default 0
// except it's turned into a sequence level filtering
// Sequence level fitlering to be added in the future.
// This comment applied to all below compound types (struct, list, map)
// that consumes current column projection which is to be refactored
StructColumnReader::StructColumnReader(
    const std::shared_ptr<const dwio::common::TypeWithId>& requestedType,
    const std::shared_ptr<const dwio::common::TypeWithId>& dataType,
    StripeStreams& stripe,
    FlatMapContext flatMapContext)
    : ColumnReader(dataType, stripe, std::move(flatMapContext)),
      requestedType_{requestedType} {
  DWIO_ENSURE_EQ(nodeType_->id, dataType->id, "working on the same node");
  EncodingKey encodingKey{nodeType_->id, flatMapContext_.sequence};
  auto encoding = static_cast<int64_t>(stripe.getEncoding(encodingKey).kind());
  DWIO_ENSURE_EQ(
      encoding,
      proto::ColumnEncoding_Kind_DIRECT,
      "Unknown encoding for StructColumnReader");

  // count the number of selected sub-columns
  const auto& cs = stripe.getColumnSelector();
  auto project = stripe.getRowReaderOptions().getProjectSelectedType();
  for (uint64_t i = 0; i < requestedType_->size(); ++i) {
    auto& child = requestedType_->childAt(i);

    // if the requested field is not in file, we either return null reader
    // or constant reader based on its expression
    if (i >= nodeType_->size()) {
      children_.push_back(
          std::make_unique<NullColumnReader>(stripe, child->type));
    } else if (cs.shouldReadNode(child->id)) {
      children_.push_back(ColumnReader::build(
          child,
          nodeType_->childAt(i),
          stripe,
          FlatMapContext{flatMapContext_.sequence, nullptr}));
    } else if (!project) {
      children_.emplace_back();
    }
  }
}

uint64_t StructColumnReader::skip(uint64_t numValues) {
  numValues = ColumnReader::skip(numValues);
  for (auto& ptr : children_) {
    if (ptr) {
      ptr->skip(numValues);
    }
  }
  return numValues;
}

void StructColumnReader::next(
    uint64_t numValues,
    VectorPtr& result,
    const uint64_t* incomingNulls) {
  auto rowVector = detail::resetIfWrongVectorType<RowVector>(result);
  std::vector<VectorPtr> childrenVectors;
  if (rowVector) {
    // Track children vectors in a local variable because readNulls may reset
    // the parent vector.
    childrenVectors = rowVector->children();
    DWIO_ENSURE_GE(childrenVectors.size(), children_.size());
  }

  BufferPtr nulls = readNulls(numValues, result, incomingNulls);
  const auto nullsPtr = nulls ? nulls->as<uint64_t>() : nullptr;
  uint64_t nullCount = nullsPtr ? bits::countNulls(nullsPtr, 0, numValues) : 0;

  std::vector<VectorPtr>* childrenVectorsPtr = nullptr;
  if (result) {
    // Parent vector still exist, so there is no need to double reference
    // children vectors.
    childrenVectorsPtr = &rowVector->children();
    childrenVectors.clear();
  } else {
    childrenVectors.resize(children_.size());
    childrenVectorsPtr = &childrenVectors;
  }

  for (uint64_t i = 0; i < children_.size(); ++i) {
    auto& reader = children_[i];
    if (reader) {
      reader->next(numValues, (*childrenVectorsPtr)[i], nullsPtr);
    }
  }

  if (result) {
    result->setSize(numValues);
    result->setNullCount(nullCount);
  } else {
    // When read-string-as-row flag is on, string readers produce ROW(BIGINT,
    // BIGINT) type instead of VARCHAR or VARBINARY. In these cases,
    // requestedType_->type is not the right type of the final struct.
    std::vector<TypePtr> types;
    types.reserve(childrenVectorsPtr->size());
    for (auto i = 0; i < childrenVectorsPtr->size(); i++) {
      const auto& child = (*childrenVectorsPtr)[i];
      if (child) {
        types.emplace_back(child->type());
      } else {
        types.emplace_back(requestedType_->type->childAt(i));
      }
    }

    result = std::make_shared<RowVector>(
        &memoryPool_,
        ROW(std::move(types)),
        nulls,
        numValues,
        std::move(childrenVectors),
        nullCount);
  }
}

class ListColumnReader : public ColumnReader {
 private:
  std::unique_ptr<ColumnReader> child;
  std::unique_ptr<dwio::common::IntDecoder</*isSigned*/ false>> length;
  const std::shared_ptr<const dwio::common::TypeWithId> requestedType_;

 public:
  ListColumnReader(
      const std::shared_ptr<const dwio::common::TypeWithId>& requestedType,
      const std::shared_ptr<const dwio::common::TypeWithId>& dataType,
      StripeStreams& stripe,
      FlatMapContext flatMapContext);
  ~ListColumnReader() override = default;

  uint64_t skip(uint64_t numValues) override;

  void next(uint64_t numValues, VectorPtr& result, const uint64_t* nulls)
      override;
};

ListColumnReader::ListColumnReader(
    const std::shared_ptr<const dwio::common::TypeWithId>& requestedType,
    const std::shared_ptr<const dwio::common::TypeWithId>& dataType,
    StripeStreams& stripe,
    FlatMapContext flatMapContext)
    : ColumnReader(dataType, stripe, std::move(flatMapContext)),
      requestedType_{requestedType} {
  DWIO_ENSURE_EQ(nodeType_->id, dataType->id, "working on the same node");
  EncodingKey encodingKey{nodeType_->id, flatMapContext_.sequence};
  // count the number of selected sub-columns
  RleVersion vers = convertRleVersion(stripe.getEncoding(encodingKey).kind());

  auto lenId = encodingKey.forKind(proto::Stream_Kind_LENGTH);
  bool vints = stripe.getUseVInts(lenId);
  length = createRleDecoder</*isSigned*/ false>(
      stripe.getStream(lenId, true),
      vers,
      memoryPool_,
      vints,
      dwio::common::INT_BYTE_SIZE);

  const auto& cs = stripe.getColumnSelector();
  auto& childType = requestedType_->childAt(0);
  if (cs.shouldReadNode(childType->id)) {
    child = ColumnReader::build(
        childType,
        nodeType_->childAt(0),
        stripe,
        FlatMapContext{flatMapContext_.sequence, nullptr});
  }
}

uint64_t skipLengths(uint64_t numValues, IntDecoder<false>& length) {
  std::array<int64_t, BUFFER_SIZE> buffer;
  uint64_t childrenElements = 0;
  uint64_t lengthsRead = 0;
  while (lengthsRead < numValues) {
    uint64_t chunk = std::min(numValues - lengthsRead, BUFFER_SIZE);
    length.next(buffer.data(), chunk, nullptr);
    for (size_t i = 0; i < chunk; ++i) {
      childrenElements += static_cast<uint64_t>(buffer[i]);
    }
    lengthsRead += chunk;
  }
  return childrenElements;
}

uint64_t ListColumnReader::skip(uint64_t numValues) {
  numValues = ColumnReader::skip(numValues);
  if (child) {
    child->skip(skipLengths(numValues, *length));
  } else {
    length->skip(numValues);
  }
  return numValues;
}

void ListColumnReader::next(
    uint64_t numValues,
    VectorPtr& result,
    const uint64_t* incomingNulls) {
  auto resultArray = detail::resetIfWrongVectorType<ArrayVector>(result);
  VectorPtr elements;
  BufferPtr offsets;
  BufferPtr lengths;
  if (resultArray) {
    elements = resultArray->elements();
    offsets = resultArray->mutableOffsets(numValues);
    lengths = resultArray->mutableSizes(numValues);
  }

  BufferPtr nulls = readNulls(numValues, result, incomingNulls);
  const auto* nullsPtr = nulls ? nulls->as<uint64_t>() : nullptr;
  uint64_t nullCount = nullsPtr ? bits::countNulls(nullsPtr, 0, numValues) : 0;

  if (resultArray) {
    detail::resetIfNotWritable(result, offsets, lengths);
  }

  if (!offsets) {
    offsets = AlignedBuffer::allocate<vector_size_t>(numValues, &memoryPool_);
  }
  if (!lengths) {
    lengths = AlignedBuffer::allocate<vector_size_t>(numValues, &memoryPool_);
  }

  // Hack. Cast vector_size_t to signed so integer reader can handle it. This
  // cast is safe because length is unsigned 4 byte integer. We should instead
  // fix integer reader (similar to integer writer) so it can take unsigned
  // directly
  auto rawLengths = lengths->asMutable<std::make_signed_t<vector_size_t>>();
  length->next(rawLengths, numValues, nullsPtr);

  auto* offsetsPtr = offsets->asMutable<vector_size_t>();

  uint64_t totalChildren = 0;
  if (nulls) {
    for (size_t i = 0; i < numValues; ++i) {
      if (!bits::isBitNull(nullsPtr, i)) {
        offsetsPtr[i] = totalChildren;
        totalChildren += rawLengths[i];
      } else {
        offsetsPtr[i] = totalChildren;
        rawLengths[i] = 0;
      }
    }
  } else {
    for (size_t i = 0; i < numValues; ++i) {
      offsetsPtr[i] = totalChildren;
      totalChildren += rawLengths[i];
    }
  }

  if (result) {
    result->setSize(numValues);
    result->setNullCount(nullCount);
  } else {
    // When read-string-as-row flag is on, string readers produce ROW(BIGINT,
    // BIGINT) type instead of VARCHAR or VARBINARY. In these cases,
    // requestedType_->type is not the right type of the final vector.
    auto arrayType =
        elements != nullptr ? ARRAY(elements->type()) : requestedType_->type;
    result = std::make_shared<ArrayVector>(
        &memoryPool_,
        arrayType,
        nulls,
        numValues,
        offsets,
        lengths,
        elements,
        nullCount);
  }
  // reset elements to avoid it being double referenced.
  elements.reset();

  bool hasChildren = (child && totalChildren > 0);
  if (hasChildren) {
    child->next(totalChildren, result->as<ArrayVector>()->elements());
  }
}

class MapColumnReader : public ColumnReader {
 private:
  std::unique_ptr<ColumnReader> keyReader;
  std::unique_ptr<ColumnReader> elementReader;
  std::unique_ptr<dwio::common::IntDecoder</*isSigned*/ false>> length;
  const std::shared_ptr<const dwio::common::TypeWithId> requestedType_;

 public:
  MapColumnReader(
      const std::shared_ptr<const dwio::common::TypeWithId>& requestedType,
      const std::shared_ptr<const dwio::common::TypeWithId>& dataType,
      StripeStreams& stripe,
      FlatMapContext flatMapContext);
  ~MapColumnReader() override = default;

  uint64_t skip(uint64_t numValues) override;

  void next(uint64_t numValues, VectorPtr& result, const uint64_t* nulls)
      override;
};

MapColumnReader::MapColumnReader(
    const std::shared_ptr<const dwio::common::TypeWithId>& requestedType,
    const std::shared_ptr<const dwio::common::TypeWithId>& dataType,
    StripeStreams& stripe,
    FlatMapContext flatMapContext)
    : ColumnReader(dataType, stripe, std::move(flatMapContext)),
      requestedType_{requestedType} {
  DWIO_ENSURE_EQ(nodeType_->id, dataType->id, "working on the same node");
  EncodingKey encodingKey{nodeType_->id, flatMapContext_.sequence};
  // Determine if the key and/or value columns are selected
  RleVersion vers = convertRleVersion(stripe.getEncoding(encodingKey).kind());

  auto lenId = encodingKey.forKind(proto::Stream_Kind_LENGTH);
  bool vints = stripe.getUseVInts(lenId);
  length = createRleDecoder</*isSigned*/ false>(
      stripe.getStream(lenId, true),
      vers,
      memoryPool_,
      vints,
      dwio::common::INT_BYTE_SIZE);

  const auto& cs = stripe.getColumnSelector();
  auto& keyType = requestedType_->childAt(0);
  if (cs.shouldReadNode(keyType->id)) {
    keyReader = ColumnReader::build(
        keyType,
        nodeType_->childAt(0),
        stripe,
        FlatMapContext{flatMapContext_.sequence, nullptr});
  }

  auto& valueType = requestedType_->childAt(1);
  if (cs.shouldReadNode(valueType->id)) {
    elementReader = ColumnReader::build(
        valueType,
        nodeType_->childAt(1),
        stripe,
        FlatMapContext{flatMapContext_.sequence, nullptr});
  }

  VLOG(1) << "[Map] Initialized map column reader for node " << nodeType_->id;
}

uint64_t MapColumnReader::skip(uint64_t numValues) {
  numValues = ColumnReader::skip(numValues);
  if (keyReader || elementReader) {
    auto childrenElements = skipLengths(numValues, *length);
    if (keyReader) {
      keyReader->skip(childrenElements);
    }
    if (elementReader) {
      elementReader->skip(childrenElements);
    }
  } else {
    length->skip(numValues);
  }
  return numValues;
}

void MapColumnReader::next(
    uint64_t numValues,
    VectorPtr& result,
    const uint64_t* incomingNulls) {
  auto resultMap = detail::resetIfWrongVectorType<MapVector>(result);
  VectorPtr keys;
  VectorPtr values;
  BufferPtr offsets;
  BufferPtr lengths;
  if (result) {
    keys = resultMap->mapKeys();
    values = resultMap->mapValues();
    offsets = resultMap->mutableOffsets(numValues);
    lengths = resultMap->mutableSizes(numValues);
  }

  BufferPtr nulls = readNulls(numValues, result, incomingNulls);
  const auto* nullsPtr = nulls ? nulls->as<uint64_t>() : nullptr;
  uint64_t nullCount = nullsPtr ? bits::countNulls(nullsPtr, 0, numValues) : 0;

  if (resultMap) {
    detail::resetIfNotWritable(result, offsets, lengths);
  }

  if (!offsets) {
    offsets = AlignedBuffer::allocate<vector_size_t>(numValues, &memoryPool_);
  }
  if (!lengths) {
    lengths = AlignedBuffer::allocate<vector_size_t>(numValues, &memoryPool_);
  }

  // Hack. Cast vector_size_t to signed so integer reader can handle it. This
  // cast is safe because length is unsigned 4 byte integer. We should instead
  // fix integer reader (similar to integer writer) so it can take unsigned
  // directly
  auto rawLengths = lengths->asMutable<std::make_signed_t<vector_size_t>>();
  length->next(rawLengths, numValues, nullsPtr);

  auto* offsetsPtr = offsets->asMutable<vector_size_t>();

  uint32_t totalChildren = 0;
  if (nulls) {
    for (size_t i = 0; i < numValues; ++i) {
      if (!bits::isBitNull(nullsPtr, i)) {
        offsetsPtr[i] = totalChildren;
        totalChildren += rawLengths[i];
      } else {
        offsetsPtr[i] = totalChildren;
        rawLengths[i] = 0;
      }
    }
  } else {
    for (size_t i = 0; i < numValues; ++i) {
      offsetsPtr[i] = totalChildren;
      totalChildren += rawLengths[i];
    }
  }

  if (result) {
    result->setSize(numValues);
    result->setNullCount(nullCount);
  } else {
    // When read-string-as-row flag is on, string readers produce ROW(BIGINT,
    // BIGINT) type instead of VARCHAR or VARBINARY. In these cases,
    // requestedType_->type is not the right type of the final vector.
    auto mapType = (keys == nullptr || values == nullptr)
        ? requestedType_->type
        : MAP(keys->type(), values->type());
    result = std::make_shared<MapVector>(
        &memoryPool_,
        mapType,
        nulls,
        numValues,
        offsets,
        lengths,
        keys,
        values,
        nullCount);
  }
  // reset keys/values to avoid them being double referenced.
  keys.reset();
  values.reset();

  resultMap = result->as<MapVector>();
  if (keyReader && totalChildren > 0) {
    keyReader->next(totalChildren, resultMap->mapKeys());
  }
  if (elementReader && totalChildren > 0) {
    elementReader->next(totalChildren, resultMap->mapValues());
  }
}

template <typename DataT>
struct RleDecoderFactory {};

template <>
struct RleDecoderFactory<bool> {
  static std::function<std::unique_ptr<ByteRleDecoder>(
      std::unique_ptr<dwio::common::SeekableInputStream>,
      const EncodingKey&)>
  get() {
    return createBooleanRleDecoder;
  }
};

template <>
struct RleDecoderFactory<int8_t> {
  static std::function<std::unique_ptr<ByteRleDecoder>(
      std::unique_ptr<dwio::common::SeekableInputStream>,
      const EncodingKey&)>
  get() {
    return createByteRleDecoder;
  }
};

template <typename DataT>
std::unique_ptr<ColumnReader> buildByteRleColumnReader(
    const std::shared_ptr<const dwio::common::TypeWithId>& nodeType,
    TypeKind requestedKind,
    StripeStreams& stripe,
    FlatMapContext flatMapContext) {
  switch (requestedKind) {
    case TypeKind::BOOLEAN:
      return std::make_unique<ByteRleColumnReader<DataT, bool>>(
          nodeType,
          stripe,
          RleDecoderFactory<DataT>::get(),
          std::move(flatMapContext));
    case TypeKind::TINYINT:
      return std::make_unique<ByteRleColumnReader<DataT, int8_t>>(
          nodeType,
          stripe,
          RleDecoderFactory<DataT>::get(),
          std::move(flatMapContext));
    case TypeKind::SMALLINT:
      return std::make_unique<ByteRleColumnReader<DataT, int16_t>>(
          nodeType,
          stripe,
          RleDecoderFactory<DataT>::get(),
          std::move(flatMapContext));
    case TypeKind::INTEGER:
      return std::make_unique<ByteRleColumnReader<DataT, int32_t>>(
          nodeType,
          stripe,
          RleDecoderFactory<DataT>::get(),
          std::move(flatMapContext));
    case TypeKind::BIGINT:
      return std::make_unique<ByteRleColumnReader<DataT, int64_t>>(
          nodeType,
          stripe,
          RleDecoderFactory<DataT>::get(),
          std::move(flatMapContext));
    default:
      DWIO_RAISE(
          fmt::format("Unsupported upcast to typekind: {}", requestedKind));
  }
}

template <template <class> class IntegerColumnReaderT>
std::unique_ptr<ColumnReader> buildTypedIntegerColumnReader(
    const std::shared_ptr<const dwio::common::TypeWithId>& nodeType,
    TypeKind requestedKind,
    FlatMapContext flatMapContext,
    StripeStreams& stripe,
    uint32_t numBytes) {
  // The assumption here is that most downcasting cases won't ever be reached,
  // and would be caught in build method earlier.
  switch (requestedKind) {
    case TypeKind::INTEGER:
      return std::make_unique<IntegerColumnReaderT<int32_t>>(
          nodeType, stripe, numBytes, std::move(flatMapContext));
    case TypeKind::BIGINT:
      return std::make_unique<IntegerColumnReaderT<int64_t>>(
          nodeType, stripe, numBytes, std::move(flatMapContext));
    case TypeKind::SMALLINT:
      return std::make_unique<IntegerColumnReaderT<int16_t>>(
          nodeType, stripe, numBytes, std::move(flatMapContext));
    default:
      DWIO_RAISE(fmt::format(
          "Unsupported requested integral type: {}", requestedKind));
  }
}

std::unique_ptr<ColumnReader> buildIntegerReader(
    const std::shared_ptr<const dwio::common::TypeWithId>& nodeType,
    TypeKind requestedKind,
    uint32_t numBytes,
    FlatMapContext flatMapContext,
    StripeStreams& stripe) {
  EncodingKey ek{nodeType->id, flatMapContext.sequence};
  switch (static_cast<int64_t>(stripe.getEncoding(ek).kind())) {
    case proto::ColumnEncoding_Kind_DICTIONARY:
    case proto::ColumnEncoding_Kind_DICTIONARY_V2:
      return buildTypedIntegerColumnReader<IntegerDictionaryColumnReader>(
          nodeType, requestedKind, std::move(flatMapContext), stripe, numBytes);
    case proto::ColumnEncoding_Kind_DIRECT:
    case proto::ColumnEncoding_Kind_DIRECT_V2:
      return buildTypedIntegerColumnReader<IntegerDirectColumnReader>(
          nodeType, requestedKind, std::move(flatMapContext), stripe, numBytes);
    default:
      DWIO_RAISE("buildReader unhandled string encoding");
  }
}

std::unique_ptr<ColumnReader> ColumnReader::build(
    const std::shared_ptr<const dwio::common::TypeWithId>& requestedType,
    const std::shared_ptr<const dwio::common::TypeWithId>& dataType,
    StripeStreams& stripe,
    FlatMapContext flatMapContext) {
  CompatChecker::check(*dataType->type, *requestedType->type);
  EncodingKey ek{dataType->id, flatMapContext.sequence};
  switch (dataType->type->kind()) {
    case TypeKind::INTEGER:
      return buildIntegerReader(
          dataType,
          requestedType->type->kind(),
          dwio::common::INT_BYTE_SIZE,
          std::move(flatMapContext),
          stripe);
    case TypeKind::BIGINT:
      return buildIntegerReader(
          dataType,
          requestedType->type->kind(),
          dwio::common::LONG_BYTE_SIZE,
          std::move(flatMapContext),
          stripe);
    case TypeKind::SMALLINT:
      return buildIntegerReader(
          dataType,
          requestedType->type->kind(),
          dwio::common::SHORT_BYTE_SIZE,
          std::move(flatMapContext),
          stripe);
    case TypeKind::VARBINARY:
    case TypeKind::VARCHAR:
      switch (static_cast<int64_t>(stripe.getEncoding(ek).kind())) {
        case proto::ColumnEncoding_Kind_DICTIONARY:
        case proto::ColumnEncoding_Kind_DICTIONARY_V2:
          return std::make_unique<StringDictionaryColumnReader>(
              dataType, stripe, std::move(flatMapContext));
        case proto::ColumnEncoding_Kind_DIRECT:
        case proto::ColumnEncoding_Kind_DIRECT_V2:
          return std::make_unique<StringDirectColumnReader>(
              dataType, stripe, std::move(flatMapContext));
        default:
          DWIO_RAISE("buildReader unhandled string encoding");
      }
    case TypeKind::BOOLEAN:
      return buildByteRleColumnReader<bool>(
          dataType,
          requestedType->type->kind(),
          stripe,
          std::move(flatMapContext));
    case TypeKind::TINYINT:
      return buildByteRleColumnReader<int8_t>(
          dataType,
          requestedType->type->kind(),
          stripe,
          std::move(flatMapContext));
    case TypeKind::ARRAY:
      return std::make_unique<ListColumnReader>(
          requestedType, dataType, stripe, std::move(flatMapContext));
    case TypeKind::MAP:
      if (stripe.getEncoding(ek).kind() ==
          proto::ColumnEncoding_Kind_MAP_FLAT) {
        return FlatMapColumnReaderFactory::create(
            requestedType, dataType, stripe, std::move(flatMapContext));
      }
      return std::make_unique<MapColumnReader>(
          requestedType, dataType, stripe, std::move(flatMapContext));
    case TypeKind::ROW:
      return std::make_unique<StructColumnReader>(
          requestedType, dataType, stripe, std::move(flatMapContext));
    case TypeKind::REAL:
      if (requestedType->type->kind() == TypeKind::REAL) {
        return std::make_unique<FloatingPointColumnReader<float, float>>(
            dataType, stripe, std::move(flatMapContext));
      } else {
        return std::make_unique<FloatingPointColumnReader<float, double>>(
            dataType, stripe, std::move(flatMapContext));
      }
    case TypeKind::DOUBLE:
      return std::make_unique<FloatingPointColumnReader<double, double>>(
          dataType, stripe, std::move(flatMapContext));
    case TypeKind::TIMESTAMP:
      return std::make_unique<TimestampColumnReader>(
          dataType, stripe, std::move(flatMapContext));
    default:
      DWIO_RAISE("buildReader unhandled type");
  }
}

// static
ColumnReaderFactory* ColumnReaderFactory::baseFactory() {
  static auto instance = std::make_unique<ColumnReaderFactory>();
  return instance.get();
}

} // namespace facebook::velox::dwrf

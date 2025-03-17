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

#include "velox/serializers/VectorStream.h"

#include "velox/functions/prestosql/types/IPAddressType.h"
#include "velox/functions/prestosql/types/IPPrefixType.h"
#include "velox/functions/prestosql/types/UuidType.h"
#include "velox/serializers/PrestoSerializerSerializationUtils.h"

namespace facebook::velox::serializer::presto::detail {
namespace {
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
} // namespace

VectorStream::VectorStream(
    const TypePtr& type,
    std::optional<VectorEncoding::Simple> encoding,
    std::optional<VectorPtr> vector,
    StreamArena* streamArena,
    int32_t initialNumRows,
    const PrestoVectorSerde::PrestoOptions& opts)
    : type_(type),
      streamArena_(streamArena),
      isLongDecimal_(type_->isLongDecimal()),
      isUuid_(isUuidType(type_)),
      isIpAddress_(isIPAddressType(type_)),
      isIpPrefix_(isIPPrefixType(type_)),
      opts_(opts),
      encoding_(getEncoding(encoding, vector)),
      nulls_(streamArena, true, true),
      lengths_(streamArena),
      values_(streamArena),
      children_(memory::StlAllocator<VectorStream>(*streamArena->pool())) {
  if (initialNumRows == 0) {
    initializeHeader(typeToEncodingName(type), *streamArena);
    if (type_->size() > 0 && !isIpPrefix_) {
      hasLengths_ = true;
      children_.reserve(type_->size());
      for (int32_t i = 0; i < type_->size(); ++i) {
        children_.emplace_back(
            type_->childAt(i),
            std::nullopt,
            getChildAt(vector, i),
            streamArena_,
            initialNumRows,
            opts_);
      }

      // The first element in the offsets in the wire format is always 0 for
      // nested types. Set upon construction/reset in case empty (no append
      // calls will be made).
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
        children_.emplace_back(
            type_,
            std::nullopt,
            std::nullopt,
            streamArena,
            initialNumRows,
            opts);
        return;
      }
      case VectorEncoding::Simple::DICTIONARY: {
        // For fix width types that are smaller than int32_t (the type for
        // indexes into the dictionary) dictionary encoding increases the
        // size, so we should flatten it.
        if (!preserveEncodings() && type->isFixedWidth() &&
            type->cppSizeInBytes() <= sizeof(int32_t)) {
          encoding_ = std::nullopt;
          break;
        }

        initializeHeader(kDictionary, *streamArena);
        values_.startWrite(initialNumRows * 4);
        isDictionaryStream_ = true;
        children_.emplace_back(
            type_,
            std::nullopt,
            std::nullopt,
            streamArena,
            initialNumRows,
            opts);
        return;
      }
      default:
        break;
    }
  }

  initializeFlatStream(vector, initialNumRows);
}

void VectorStream::appendNulls(
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

void VectorStream::flattenStream(
    const VectorPtr& vector,
    int32_t initialNumRows) {
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

void VectorStream::flush(OutputStream* out) {
  out->write(reinterpret_cast<char*>(header_.buffer), header_.size);

  if (encoding_.has_value()) {
    switch (encoding_.value()) {
      case VectorEncoding::Simple::CONSTANT: {
        writeInt32(out, nonNullCount_);
        children_[0].flush(out);
        return;
      }
      case VectorEncoding::Simple::DICTIONARY: {
        writeInt32(out, nonNullCount_);
        children_[0].flush(out);
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
      if (isIPPrefixType(type_)) {
        writeInt32(out, nullCount_ + nonNullCount_);
        lengths_.flush(out);
        flushNulls(out);
        writeInt32(out, values_.size());
        values_.flush(out);
        break;
      }

      if (opts_.nullsFirst) {
        writeInt32(out, nullCount_ + nonNullCount_);
        flushNulls(out);
      }

      writeInt32(out, children_.size());
      for (auto& child : children_) {
        child.flush(out);
      }
      if (!opts_.nullsFirst) {
        writeInt32(out, nullCount_ + nonNullCount_);
        lengths_.flush(out);
        flushNulls(out);
      }
      return;

    case TypeKind::ARRAY:
      children_[0].flush(out);
      writeInt32(out, nullCount_ + nonNullCount_);
      lengths_.flush(out);
      flushNulls(out);
      return;

    case TypeKind::MAP: {
      children_[0].flush(out);
      children_[1].flush(out);
      // hash table size. -1 means not included in serialization.
      writeInt32(out, -1);
      writeInt32(out, nullCount_ + nonNullCount_);

      lengths_.flush(out);
      flushNulls(out);
      return;
    }

    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
    case TypeKind::OPAQUE:
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

void VectorStream::flushNulls(OutputStream* out) {
  if (!nullCount_) {
    char zero = 0;
    out->write(&zero, 1);
  } else {
    char one = 1;
    out->write(&one, 1);
    nulls_.flush(out);
  }
}

void VectorStream::clear() {
  encoding_ = std::nullopt;
  initializeHeader(typeToEncodingName(type_), *streamArena_);
  nonNullCount_ = 0;
  nullCount_ = 0;
  totalLength_ = 0;
  if (hasLengths_) {
    lengths_.startWrite(lengths_.size());
    if ((type_->kind() == TypeKind::ROW && !isIpPrefix_) ||
        type_->kind() == TypeKind::ARRAY || type_->kind() == TypeKind::MAP) {
      // The first element in the offsets in the wire format is always 0 for
      // nested types. Set upon construction/reset in case empty (no append
      // calls will be made).
      lengths_.appendOne<int32_t>(0);
    }
  }
  nulls_.startWrite(nulls_.size());
  values_.startWrite(values_.size());
  for (auto& child : children_) {
    child.clear();
  }
}

size_t VectorStream::serializedSize() {
  CountingOutputStream out;
  flush(&out);
  return out.size();
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

template <>
void VectorStream::append(folly::Range<const int128_t*> values) {
  for (auto& value : values) {
    int128_t val = value;
    if (isLongDecimal_) {
      val = toJavaDecimalValue(value);
    } else if (isUuid_) {
      val = toJavaUuidValue(value);
    } else if (isIpAddress_) {
      val = reverseIpAddressByteOrder(value);
    }
    values_.append<int128_t>(folly::Range(&val, 1));
  }
}

void VectorStream::initializeFlatStream(
    std::optional<VectorPtr> vector,
    vector_size_t initialNumRows) {
  initializeHeader(typeToEncodingName(type_), *streamArena_);
  nulls_.startWrite(0);

  switch (type_->kind()) {
    case TypeKind::ROW:
      [[fallthrough]];
    case TypeKind::ARRAY:
      [[fallthrough]];
    case TypeKind::MAP:
      // Velox represents ipprefix as a row, but we need
      // to serialize the data type as varbinary to be compatible with Java
      if (isIpPrefix_) {
        hasLengths_ = true;
        lengths_.startWrite(0);
        if (values_.ranges().empty()) {
          values_.startWrite(0);
        }
        break;
      }
      hasLengths_ = true;
      children_.reserve(type_->size());
      for (int32_t i = 0; i < type_->size(); ++i) {
        children_.emplace_back(
            type_->childAt(i),
            std::nullopt,
            getChildAt(vector, i),
            streamArena_,
            initialNumRows,
            opts_);
      }
      // The first element in the offsets in the wire format is always 0 for
      // nested types. Set upon construction/reset in case empty (no append
      // calls will be made).
      lengths_.startWrite(sizeof(vector_size_t));
      lengths_.appendOne<int32_t>(0);
      break;
    case TypeKind::OPAQUE:
      [[fallthrough]];
    case TypeKind::VARCHAR:
      [[fallthrough]];
    case TypeKind::VARBINARY:
      hasLengths_ = true;
      lengths_.startWrite(0);
      if (values_.ranges().empty()) {
        values_.startWrite(0);
      }
      break;
    default:
      if (values_.ranges().empty()) {
        values_.startWrite(0);
      }
      break;
  }
}
} // namespace facebook::velox::serializer::presto::detail

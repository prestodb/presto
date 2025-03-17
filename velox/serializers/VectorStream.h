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

#include "velox/common/memory/StreamArena.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/vector/BaseVector.h"

namespace facebook::velox::serializer::presto::detail {
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
      const PrestoVectorSerde::PrestoOptions& opts);

  void flattenStream(const VectorPtr& vector, int32_t initialNumRows);

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
    // Allocations from stream arena must be aligned size.
    static constexpr uint32_t kHeaderSize = 64;
    VELOX_CHECK_GE(kHeaderSize, name.size() + sizeof(int32_t));
    streamArena.newRange(kHeaderSize, nullptr, &header_);
    if (header_.size < kHeaderSize) {
      // StreamArena::newRange() does not guarantee the size. If returned range
      // does not have enough space, allocate a new one. The second arena
      // allocation will allocate a new slab which must have enough size.
      streamArena.newRange(kHeaderSize, nullptr, &header_);
      VELOX_CHECK_EQ(header_.size, kHeaderSize);
    }
    header_.size = name.size() + sizeof(int32_t);
    folly::storeUnaligned<int32_t>(header_.buffer, name.size());
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
      int32_t numNonNull);

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

  bool preserveEncodings() const {
    return opts_.preserveEncodings;
  }

  VectorStream* childAt(int32_t index) {
    return &children_[index];
  }

  ByteOutputStream& values() {
    return values_;
  }

  auto& nulls() {
    return nulls_;
  }

  // Returns the size to flush to OutputStream before calling `flush`.
  size_t serializedSize();

  // Writes out the accumulated contents. Does not change the state.
  void flush(OutputStream* out);

  void flushNulls(OutputStream* out);

  bool isLongDecimal() const {
    return isLongDecimal_;
  }

  bool isUuid() const {
    return isUuid_;
  }

  bool isIpAddress() const {
    return isIpAddress_;
  }

  bool isIpPrefix() const {
    return isIpPrefix_;
  }

  void clear();

 private:
  void initializeFlatStream(
      std::optional<VectorPtr> vector,
      vector_size_t initialNumRows);

  const TypePtr type_;
  StreamArena* const streamArena_;
  const bool isLongDecimal_;
  const bool isUuid_;
  const bool isIpAddress_;
  const bool isIpPrefix_;
  const PrestoVectorSerde::PrestoOptions opts_;
  std::optional<VectorEncoding::Simple> encoding_;
  int32_t nonNullCount_{0};
  int32_t nullCount_{0};
  int32_t totalLength_{0};
  bool hasLengths_{false};
  ByteRange header_;
  ByteOutputStream nulls_;
  ByteOutputStream lengths_;
  ByteOutputStream values_;
  std::vector<VectorStream, memory::StlAllocator<VectorStream>> children_;
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
void VectorStream::append(folly::Range<const Timestamp*> values);

template <>
void VectorStream::append(folly::Range<const bool*> values);

template <>
void VectorStream::append(folly::Range<const int128_t*> values);
} // namespace facebook::velox::serializer::presto::detail

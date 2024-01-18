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

#include <folly/io/IOBuf.h>
#include "velox/common/base/Scratch.h"
#include "velox/common/memory/StreamArena.h"
#include "velox/type/Type.h"

#include <folly/io/IOBuf.h>
#include <memory>

namespace facebook::velox {

struct ByteRange {
  /// Start of buffer. Not owned.
  uint8_t* buffer;

  /// Number of bytes or bits starting at 'buffer'.
  int32_t size;

  /// Index of next byte/bit to be read/written in 'buffer'.
  int32_t position;

  std::string toString() const;
};

class OutputStreamListener {
 public:
  virtual void onWrite(const char* /* s */, std::streamsize /* count */) {}
  virtual ~OutputStreamListener() = default;
};

class OutputStream {
 public:
  explicit OutputStream(OutputStreamListener* listener = nullptr)
      : listener_(listener) {}

  virtual ~OutputStream() = default;

  virtual void write(const char* s, std::streamsize count) = 0;

  virtual std::streampos tellp() const = 0;

  virtual void seekp(std::streampos pos) = 0;

  OutputStreamListener* listener() const {
    return listener_;
  }

 protected:
  OutputStreamListener* listener_;
};

class OStreamOutputStream : public OutputStream {
 public:
  explicit OStreamOutputStream(
      std::ostream* out,
      OutputStreamListener* listener = nullptr)
      : OutputStream(listener), out_(out) {}

  void write(const char* s, std::streamsize count) override {
    out_->write(s, count);
    if (listener_) {
      listener_->onWrite(s, count);
    }
  }

  std::streampos tellp() const override {
    return out_->tellp();
  }

  void seekp(std::streampos pos) override {
    out_->seekp(pos);
  }

 private:
  std::ostream* out_;
};

/// Read-only stream over one or more byte buffers.
class ByteInputStream {
 protected:
  /// TODO Remove after refactoring SpillInput.
  ByteInputStream() {}

 public:
  explicit ByteInputStream(std::vector<ByteRange> ranges)
      : ranges_{std::move(ranges)} {
    VELOX_CHECK(!ranges_.empty());
    current_ = &ranges_[0];
  }

  /// Disable copy constructor.
  ByteInputStream(const ByteInputStream&) = delete;

  /// Disable copy assignment operator.
  ByteInputStream& operator=(const ByteInputStream& other) = delete;

  /// Enable move constructor.
  ByteInputStream(ByteInputStream&& other) noexcept
      : ranges_{std::move(other.ranges_)}, current_{other.current_} {}

  /// Enable move assignment operator.
  ByteInputStream& operator=(ByteInputStream&& other) noexcept {
    if (this != &other) {
      ranges_ = std::move(other.ranges_);
      current_ = other.current_;
      other.current_ = nullptr;
    }
    return *this;
  }

  /// TODO Remove after refactoring SpillInput.
  virtual ~ByteInputStream() = default;

  /// Returns total number of bytes available in the stream.
  size_t size() const;

  /// Returns true if all input has been read.
  ///
  /// TODO: Remove 'virtual' after refactoring SpillInput.
  virtual bool atEnd() const;

  /// Returns current position (number of bytes from the start) in the stream.
  std::streampos tellp() const;

  /// Moves current position to specified one.
  void seekp(std::streampos pos);

  /// Returns the remaining size left from current reading position.
  size_t remainingSize() const;

  std::string toString() const;

  uint8_t readByte();

  void readBytes(uint8_t* bytes, int32_t size);

  template <typename T>
  T read() {
    if (current_->position + sizeof(T) <= current_->size) {
      current_->position += sizeof(T);
      return *reinterpret_cast<const T*>(
          current_->buffer + current_->position - sizeof(T));
    }
    // The number straddles two buffers. We read byte by byte and make
    // a little-endian uint64_t. The bytes can be cast to any integer
    // or floating point type since the wire format has the machine byte order.
    static_assert(sizeof(T) <= sizeof(uint64_t));
    uint64_t value = 0;
    for (int32_t i = 0; i < sizeof(T); ++i) {
      value |= static_cast<uint64_t>(readByte()) << (i * 8);
    }
    return *reinterpret_cast<const T*>(&value);
  }

  template <typename Char>
  void readBytes(Char* data, int32_t size) {
    readBytes(reinterpret_cast<uint8_t*>(data), size);
  }

  /// Returns a view over the read buffer for up to 'size' next
  /// bytes. The size of the value may be less if the current byte
  /// range ends within 'size' bytes from the current position.  The
  /// size will be 0 if at end.
  std::string_view nextView(int32_t size);

  void skip(int32_t size);

 protected:
  /// Sets 'current_' to point to the next range of input.  // The
  /// input is consecutive ByteRanges in 'ranges_' for the base class
  /// but any view over external buffers can be made by specialization.
  ///
  /// TODO: Remove 'virtual' after refactoring SpillInput.
  virtual void next(bool throwIfPastEnd = true);

  // TODO: Remove  after refactoring SpillInput.
  const std::vector<ByteRange>& ranges() const {
    return ranges_;
  }

  // TODO: Remove  after refactoring SpillInput.
  void setRange(ByteRange range) {
    ranges_.resize(1);
    ranges_[0] = range;
    current_ = ranges_.data();
  }

 private:
  std::vector<ByteRange> ranges_;

  // Pointer to the current element of 'ranges_'.
  ByteRange* current_{nullptr};
};

/// Stream over a chain of ByteRanges. Provides read, write and
/// comparison for equality between stream contents and memory. Used
/// for streams in repartitioning or for complex variable length data
/// in hash tables. The stream is seekable and supports overwriting of
/// previous content, for example, writing a message body and then
/// seeking back to start to write a length header.
class ByteOutputStream {
 public:
  /// For output.
  ByteOutputStream(
      StreamArena* arena,
      bool isBits = false,
      bool isReverseBitOrder = false)
      : arena_(arena), isBits_(isBits), isReverseBitOrder_(isReverseBitOrder) {}

  ByteOutputStream(const ByteOutputStream& other) = delete;

  void operator=(const ByteOutputStream& other) = delete;

  /// Sets 'this' to range over 'range'. If this is for purposes of writing,
  /// lastWrittenPosition specifies the end of any pre-existing content in
  /// 'range'.
  void setRange(ByteRange range, int32_t lastWrittenPosition) {
    ranges_.resize(1);
    ranges_[0] = range;
    current_ = ranges_.data();
    VELOX_CHECK_GE(ranges_.back().size, lastWrittenPosition);
    lastRangeEnd_ = lastWrittenPosition;
  }

  const std::vector<ByteRange>& ranges() const {
    return ranges_;
  }

  void startWrite(int32_t initialSize) {
    extend(initialSize);
  }

  void seek(int32_t range, int32_t position) {
    current_ = &ranges_[range];
    current_->position = position;
  }

  std::streampos tellp() const;

  void seekp(std::streampos position);

  /// Returns the size written into ranges_. This is the sum of the
  /// capacities of non-last ranges + the greatest write position of
  /// the last range.
  size_t size() const;

  int32_t lastRangeEnd() const {
    updateEnd();
    return lastRangeEnd_;
  }

  template <typename T>
  void append(folly::Range<const T*> values) {
    if (current_->position + sizeof(T) * values.size() > current_->size) {
      appendStringView(std::string_view(
          reinterpret_cast<const char*>(&values[0]),
          values.size() * sizeof(T)));
      return;
    }

    auto* target = reinterpret_cast<T*>(current_->buffer + current_->position);
    const auto* end = target + values.size();
    auto* valuePtr = &values[0];
    while (target != end) {
      *target = *valuePtr;
      ++target;
      ++valuePtr;
    }
    current_->position += sizeof(T) * values.size();
  }

  void appendBool(bool value, int32_t count);

  // A fast path for appending bits into pre-cleared buffers after first extend.
  inline void
  appendBitsFresh(const uint64_t* bits, int32_t begin, int32_t end) {
    const auto position = current_->position;
    if (begin == 0 && end <= 56) {
      const auto available = current_->size - position;
      // There must be 8 bytes writable. If available is 56, there are 7, so >.
      if (available > 56) {
        const auto offset = position & 7;
        uint64_t* buffer =
            reinterpret_cast<uint64_t*>(current_->buffer + (position >> 3));
        const auto mask = bits::lowMask(offset);
        *buffer = (*buffer & mask) | (bits[0] << offset);
        current_->position += end;
        return;
      }
    }
    appendBits(bits, begin, end);
  }

  // Writes 'bits' from bit positions begin..end to the current position of
  // 'this'. Extends 'this' if writing past end.
  void appendBits(const uint64_t* bits, int32_t begin, int32_t end);

  void appendStringView(StringView value);

  void appendStringView(std::string_view value);

  template <typename T>
  void appendOne(const T& value) {
    append(folly::Range(&value, 1));
  }

  void flush(OutputStream* stream);

  /// Returns the next byte that would be written to by a write. This
  /// is used after an append to release the remainder of the reserved
  /// space.
  char* writePosition();

  int32_t testingAllocatedBytes() const {
    return allocatedBytes_;
  }

  /// Returns a ByteInputStream to range over the current content of 'this'. The
  /// result is valid as long as 'this' is live and not changed.
  ByteInputStream inputStream() const;

  std::string toString() const;

 private:
  // Returns a range of 'size' items of T. If there is no contiguous space in
  // 'this', uses 'scratch' to make a temp block that is appended to 'this' in
  template <typename T>
  T* getAppendWindow(int32_t size, ScratchPtr<T>& scratchPtr) {
    const int32_t bytes = sizeof(T) * size;
    if (!current_) {
      extend(bytes);
    }
    auto available = current_->size - current_->position;
    if (available >= bytes) {
      current_->position += bytes;
      return reinterpret_cast<T*>(
          current_->buffer + current_->position - bytes);
    }
    // If the tail is not large enough, make  temp of the right size
    // in scratch. Extend the stream so that there is guaranteed space to copy
    // the scratch to the stream. This copy takes place in destruction of
    // AppendWindow and must not allocate so that it is noexcept.
    ensureSpace(bytes);
    return scratchPtr.get(size);
  }

  void extend(int32_t bytes);

  // Calls extend() enough times to make sure 'bytes' bytes can be
  // appended without new allocation. Does not change the append
  // position.
  void ensureSpace(int32_t bytes);

  int32_t newRangeSize(int32_t bytes) const;

  void updateEnd() const {
    if (!ranges_.empty() && current_ == &ranges_.back() &&
        current_->position > lastRangeEnd_) {
      lastRangeEnd_ = current_->position;
    }
  }

  StreamArena* const arena_{nullptr};

  // Indicates that position in ranges_ is in bits, not bytes.
  const bool isBits_;

  const bool isReverseBitOrder_;

  // True if the bit order in ranges_ has been inverted. Presto requires
  // reverse bit order.
  bool isReversed_ = false;

  std::vector<ByteRange> ranges_;
  // The total number of bytes allocated from 'arena_' in 'ranges_'.
  int64_t allocatedBytes_{0};

  // Pointer to the current element of 'ranges_'.
  ByteRange* current_{nullptr};

  // Number of bits/bytes that have been written in the last element
  // of 'ranges_'. In a write situation, all non-last ranges are full
  // and the last may be partly full. The position in the last range
  // is not necessarily the the end if there has been a seek.
  mutable int32_t lastRangeEnd_{0};

  template <typename T>
  friend class AppendWindow;
};

/// A scoped wrapper that provides 'size' T's of writable space in 'stream'.
/// Normally gives an address into 'stream's buffer but can use 'scratch' to
/// make a contiguous piece if stream does not have a suitable run.
template <typename T>
class AppendWindow {
 public:
  AppendWindow(ByteOutputStream& stream, Scratch& scratch)
      : stream_(stream), scratchPtr_(scratch) {}

  ~AppendWindow() noexcept {
    if (scratchPtr_.size()) {
      try {
        stream_.appendStringView(std::string_view(
            reinterpret_cast<const char*>(scratchPtr_.get()),
            scratchPtr_.size() * sizeof(T)));
      } catch (const std::exception& e) {
        // This is impossible because construction ensures there is space for
        // the bytes in the stream.
        LOG(FATAL) << "throw from AppendWindo append: " << e.what();
      }
    }
  }

  T* get(int32_t size) {
    return stream_.getAppendWindow(size, scratchPtr_);
  }

 private:
  ByteOutputStream& stream_;
  ScratchPtr<T> scratchPtr_;
};

template <>
inline Timestamp ByteInputStream::read<Timestamp>() {
  Timestamp value;
  readBytes(reinterpret_cast<uint8_t*>(&value), sizeof(value));
  return value;
}

template <>
inline int128_t ByteInputStream::read<int128_t>() {
  int128_t value;
  readBytes(reinterpret_cast<uint8_t*>(&value), sizeof(value));
  return value;
}

class IOBufOutputStream : public OutputStream {
 public:
  explicit IOBufOutputStream(
      memory::MemoryPool& pool,
      OutputStreamListener* listener = nullptr,
      int32_t initialSize = memory::AllocationTraits::kPageSize)
      : OutputStream(listener),
        arena_(std::make_shared<StreamArena>(&pool)),
        out_(std::make_unique<ByteOutputStream>(arena_.get())) {
    out_->startWrite(initialSize);
  }

  void write(const char* s, std::streamsize count) override {
    out_->appendStringView(std::string_view(s, count));
    if (listener_) {
      listener_->onWrite(s, count);
    }
  }

  std::streampos tellp() const override;

  void seekp(std::streampos pos) override;

  /// 'releaseFn' is executed on iobuf destruction if not null.
  std::unique_ptr<folly::IOBuf> getIOBuf(
      const std::function<void()>& releaseFn = nullptr);

 private:
  std::shared_ptr<StreamArena> arena_;
  std::unique_ptr<ByteOutputStream> out_;
};

} // namespace facebook::velox

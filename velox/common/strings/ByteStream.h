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
/**
 * Interfaces for efficient stream-like I/O.
 */

#ifndef COMMON_STRINGS_BYTESTREAM_H_
#define COMMON_STRINGS_BYTESTREAM_H_

#include <limits>
#include <streambuf>
#include <string>

#include <glog/logging.h>

#include <folly/FBString.h>
#include <folly/Range.h>

namespace facebook {
namespace strings {

const size_t kSizeMax = std::numeric_limits<size_t>::max();

/**
 * Interface for writing bytes to an underlying data sink (such as a string
 * buffer, network socket, or file).
 *
 * This interface only requires that the data be available in the underlying
 * sink when you call finish() or when the ByteSink object is destroyed
 * (assuming no errors), but implementations may provide stronger guarantees
 * (such as writing the data through immediately following an append* call, or
 * providing a flush() method).
 *
 * The error handling interface is purposefully simple, and implementations are
 * encouraged to provide error handling appropriate to the specific
 * implementation.  Importantly, append* may continue to be called in the
 * presence of an error (and will return 0), so that users of ByteSink don't
 * have to call bad() after every append.
 */
class ByteSink {
 public:
  virtual ~ByteSink() {}

  /**
   * Check the error status of this ByteSink.  Once bad() returns true, it
   * will continue to return true until the ByteSink is destroyed, or until
   * reset using an implementation-defined mechanism.  No guarantees are made
   * by this interface about any data appended to the sink if bad() returns
   * true, even data appended before bad() returned true.
   */
  virtual bool bad() const {
    return false;
  }

  /**
   * Has the sink hit its end?  Some sinks may write to an underlying stream
   * of a fixed or limited size, or for some other reason only consume a
   * fixed amount of data.  Note that, due to buffering, some of the data
   * appended on previous append() calls might not have actually been written
   * to the underlying destination.
   */
  virtual bool eof() const {
    return false;
  }

  /**
   * Append the given string to this ByteSink.  The caller may release the
   * memory associated with the string as soon as append() returns.
   *
   * append() returns the number of bytes consumed from the string before
   * the sink hits the end or becomes bad().
   *
   * Note that, if append() returns less than str.size(), the sink must
   * have reached its end (so eof() || bad() is true, and subsequent calls to
   * append() will return 0).  In particular, this may not be used for
   * non-blocking behavior.
   */
  virtual size_t append(folly::StringPiece str) = 0;
  size_t append(const void* data, size_t size) {
    return append(folly::StringPiece(static_cast<const char*>(data), size));
  }

  /**
   * Append the given string to this ByteSink.  The string must remain
   * allocated (and unchanged) until the ByteSink is destroyed.
   */
  virtual size_t appendAllocated(folly::StringPiece str) {
    return append(str);
  }

  /**
   * Convenience function that appends the bitwise representation of count
   * objects starting at address obj.  The usual caveats about endianness,
   * padding apply.
   */
  template <class T>
  size_t appendBitwise(const T* obj, size_t count) {
    const size_t sz = count * sizeof(T);
    return append(folly::StringPiece(reinterpret_cast<const char*>(obj), sz));
  }

  /**
   * Convenience function that appends the bitwise representation of a
   * type.  The usual caveats about endianness, alignment, padding apply.
   */
  template <class T>
  size_t appendBitwise(const T& obj) {
    return appendBitwise(&obj, 1);
  }

  /**
   * Indicate end of input; calling append* after finish() is an error.
   * All data must be flushed to the underlying destination before finish()
   * returns.  Calling finish() is not absolutely necessary: the ByteSink
   * destructor will ensure that the data is flushed, but finish() allows
   * you to test bad() after flushing the data.
   */
  virtual void finish() {}
};

class ByteSinkBuffer : public std::basic_streambuf<char> {
 public:
  using super = std::basic_streambuf<char>;
  using int_type = super::int_type;
  using traits = std::char_traits<char>;

  static constexpr const int_type kEOF = traits::eof();
  static constexpr const size_t kPutAreaSize = 1UL << 10;

  explicit ByteSinkBuffer(ByteSink& sink) : sink_(sink) {
    setp(&putArea_[0], &putArea_[kPutAreaSize]);
  }

  ~ByteSinkBuffer() override {
    sync();
  }

  /**
   * Purge all buffered stuff to underlying sink.
   * Per streambuf's interface:
   * "Returns unspecified value not equal to Traits::eof() on success,
   * Traits::eof() on failure.".  The return value's type is `int_type`,
   * just an integer bigger than `char`, which can accommodate all `char`
   * values plus one extra, special "eof" value.
   *
   * The actual purging out to the sink is done in our `flush()` method.
   * EOF is returned by `flush` if the underlying
   * sink is not accepting any more writes, or is `bad()`.
   * Otherwise we continue normally, with a new buffer, containing just the
   * given character.
   */
  int_type overflow(int_type ch = kEOF) override;

 protected:
  int32_t sync() override {
    flush();
    return 0;
  }

 private:
  int_type flush() {
    return 0;
  }

  char putArea_[kPutAreaSize];
  ByteSink& sink_;
};

/**
 * Simple ByteSink that appends to a string.
 */
template <class S>
class SByteSink : public ByteSink {
 public:
  explicit SByteSink(S* str) : str_(str) {}

  size_t append(folly::StringPiece s) override {
    str_->append(s.start(), s.size());
    return s.size();
  }

 private:
  S* str_;
};

typedef SByteSink<std::string> StringByteSink;
typedef SByteSink<folly::fbstring> FBStringByteSink;

/**
 * Interface for reading bytes from an underlying data source (such as a
 * string buffer, network socket, or file).
 *
 * This interface controls the memory allocations, and will return pointers
 * to memory that it manages.  The chunk sizes are also under control of the
 * ByteSink; this allows very efficient (zero-copy) operations.
 *
 * The error handling interface is purposefully simple, and implementations
 * are encouraged to provide error handling appropriate to the specific
 * implementation.  Once bad() returns true, all calls to next() will return
 * false until the ByteSource is destroyed (or until it is reset using an
 * implementation-defined mechanism).
 */
class ByteSource {
 public:
  virtual ~ByteSource() {}

  /**
   * Check the error status of this ByteSource.  Once bad() returns true, it
   * will continue to return true (and all calls to next() will return false)
   * until the ByteSource is destroyed, or until reset using an implementation-
   * defined mechanism.
   */
  virtual bool bad() const = 0;

  /**
   * Return the next chunk of data to read from the ByteSource, if available.
   * If there is more data to read from the ByteSource, set *chunk to the next
   * available chunk (chunk->size() > 0) and return true.  Otherwise (on
   * end-of-stream or error), return false.
   *
   * The memory pointed-to by chunk will be available until the next call to
   * next() or until the ByteSource object is destroyed, whichever comes first,
   * and no later.
   *
   * Note that normal end-of-stream is not an error condition; in that case,
   * next() will return false, but bad() will also return false.  On error,
   * next() returns false, and bad() returns true.
   */
  virtual bool next(folly::StringPiece* chunk) = 0;

  /**
   * Push back the last numBytes returned by the last next() call, so
   * they will be included in the next call to next().
   * REQUIRES:
   * - next() was the last method called, and it returned true
   * - numBytes <= chunk->size() for the last chunk returned
   */
  virtual void backUp(size_t numBytes) = 0;

  /**
   * Convenience method to append this ByteSource to a ByteSink.
   * Returns number of bytes appended.
   *
   * On error, the sink will be bad() or at eof(), and appendTo returns the
   * number of bytes that were successfully appended.
   */
  virtual size_t appendTo(ByteSink* sink, size_t maxBytes);

  /**
   * Convenience method to append at most maxBytes bytes from this ByteSource
   * to a string.  Returns number of bytes appended.
   */
  virtual size_t appendToString(
      std::string* out,
      size_t maxBytes = std::numeric_limits<size_t>::max()) {
    StringByteSink sink(out);
    return appendTo(&sink, maxBytes);
  }

  /**
   * Reads from this ByteSource until 'delim' is seen, and appends it to *out.
   * The delimiter is consumed, and added to the output (but will be absent
   * when the file ends with an incomplete line).
   *
   * readUntil returns false (and sets out to an empty string) only if the end
   * of the ByteSource was encountered before any characters were read.
   */
  bool readUntil(char delim, std::string* out);

  ssize_t read(void* buf, size_t count);
};

class ByteSourceBuffer : public std::basic_streambuf<char> {
 public:
  using super = std::basic_streambuf<char>;
  using int_type = super::int_type;
  using traits = std::char_traits<char>;

  static constexpr const int_type kEOF = traits::eof();
  static constexpr const size_t kGetAreaSize = 1UL << 10;

  explicit ByteSourceBuffer(ByteSource& source) : source_(source) {
    reset();
  }

  ~ByteSourceBuffer() override {}

 protected:
  int32_t underflow() override;

  void reset() {
    // initially empty buffer: begin, current, end all the same ptr.
    setg(getArea_, getArea_, getArea_);
  }

 private:
  char getArea_[kGetAreaSize];
  ByteSource& source_;
};

/**
 * Simple ByteSource that reads from a string, returning at most maxBytes
 * bytes at a time.
 */
class StringByteSource : public ByteSource {
 public:
  explicit StringByteSource(
      const folly::StringPiece& str,
      size_t maxBytes = kSizeMax)
      : str_(str),
        offset_(0),
        maxBytes_(maxBytes == kSizeMax ? str_.size() : maxBytes) {}
  ~StringByteSource() override {}

  bool bad() const override {
    return false;
  }
  bool next(folly::StringPiece* chunk) override {
    if (offset_ == str_.size()) {
      return false;
    }
    size_t len = std::min(str_.size() - offset_, maxBytes_);
    chunk->reset(str_.start() + offset_, len);
    offset_ += len;
    return true;
  }
  void backUp(size_t numBytes) override {
    CHECK_LE(numBytes, maxBytes_);
    CHECK_GE(offset_, numBytes);
    offset_ -= numBytes;
  }

 private:
  folly::StringPiece str_;
  size_t offset_;
  size_t maxBytes_;
};

} // namespace strings
} // namespace facebook

#endif /* __COMMON_STRINGS_BYTESTREAM_H_ */

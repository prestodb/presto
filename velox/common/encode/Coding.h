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
//
// Miscellaneous number encoding/decoding routines
// - Varint coding
// - ZigZag coding

#ifndef FACEBOOK_COMMON_ENCODE_CODING_H_
#define FACEBOOK_COMMON_ENCODE_CODING_H_

#include <folly/GroupVarint.h>
#include <folly/Likely.h>
#include <algorithm>
#include <utility>
#include "velox/common/encode/UInt128.h"
#include "velox/common/strings/ByteStream.h"

namespace facebook {

// Variable-length integer encoding, using a little-endian, base-128
// representation.
// The MSb is set on all bytes except the last.
class Varint {
 public:
  // Max size for a k-bit value: 1 + floor(k/7) bytes
  enum {
    kMaxSize32 = 5,
    kMaxSize64 = 10,
    kMaxSize128 = 19,
  };

  // Make <a,b> contain (a<<bBits) + b, if b has less than n bits
  // (that is, b < 2^n)
  // This is a useful transformation for encoding a 64-bit value together
  // with a small value using fewer bytes than encoding a and b separately.
  static UInt128 shift(uint64_t a, uint64_t b, uint32_t bBits) {
    return (UInt128(a) << bBits) | b;
  }
  static std::pair<uint64_t, uint64_t> unshift(UInt128 val, uint32_t bBits) {
    std::pair<uint64_t, uint64_t> p;
    p.second = val.lo() & ((static_cast<uint64_t>(1) << bBits) - 1);
    val >>= bBits;
    p.first = val.lo();
    return p;
  }

  // Determine the number of bytes needed to represent "val".
  // 32-bit values need at most 5 bytes.
  // 64-bit values need at most 10 bytes.
  static int32_t size(uint64_t val) {
    int32_t s = 1;
    while (val >= 128) {
      ++s;
      val >>= 7;
    }
    return s;
  }

  // Determine the number of bytes needed to represent "val".
  // 32-bit values need at most 5 bytes.
  // 64-bit values need at most 10 bytes.
  // 128-bit values need at most 19 bytes.
  static int32_t size128(UInt128 val) {
    int32_t s = 1;
    while (val.hi() != 0 || val.lo() >= 128) {
      ++s;
      val >>= 7;
    }
    return s;
  }

  // Encode "val" into the buffer pointed to by *dest, and advance *dest.
  // No bounds checking is done.
  static void encode(uint64_t val, char** dest) {
    char* p = *dest;
    while (val >= 128) {
      *p++ = 0x80 | (static_cast<char>(val) & 0x7f);
      val >>= 7;
    }
    *p++ = static_cast<char>(val);
    *dest = p;
  }
  static void encode128(UInt128 val, char** dest) {
    char* p = *dest;
    while (val.hi() != 0 || val.lo() >= 128) {
      *p++ = 0x80 | (static_cast<char>(val.lo()) & 0x7f);
      val >>= 7;
    }
    *p++ = static_cast<char>(val.lo());
    *dest = p;
  }

  // Encode "val" into the byte sink pointed-to by "sink".
  static void encodeToByteSink(uint64_t val, strings::ByteSink* sink) {
    char buf[kMaxSize64];
    char* p = buf;
    encode(val, &p);
    sink->append(folly::StringPiece(buf, p - buf));
  }
  static void encode128ToByteSink(UInt128 val, strings::ByteSink* sink) {
    char buf[kMaxSize128];
    char* p = buf;
    encode128(val, &p);
    sink->append(folly::StringPiece(buf, p - buf));
  }

  // Returns true if decode can be called without causing a CHECK failure.
  // The pointers are not adjusted at all
  static bool canDecode(folly::StringPiece src) {
    src = src.subpiece(0, kMaxSize64);
    return std::any_of(
        src.begin(), src.end(), [](char v) { return ~v & 0x80; });
  }

  // Decode a value from the buffer pointed to by *src, and advance *src.
  // Assumes that *src has at least max_size bytes available.
  static uint64_t decode(const char** src, int64_t max_size) {
    if (max_size >= kMaxSize64) { // Fast Path
      uint64_t x;
      int64_t b;
      const signed char* p = reinterpret_cast<const signed char*>(*src);
      do {
        // clang-format off
        b = *p++; x  = (b & 0x7f)      ; if (b >= 0) break;
        b = *p++; x |= (b & 0x7f) <<  7; if (b >= 0) break;
        b = *p++; x |= (b & 0x7f) << 14; if (b >= 0) break;
        b = *p++; x |= (b & 0x7f) << 21; if (b >= 0) break;
        b = *p++; x |= (b & 0x7f) << 28; if (b >= 0) break;
        b = *p++; x |= (b & 0x7f) << 35; if (b >= 0) break;
        b = *p++; x |= (b & 0x7f) << 42; if (b >= 0) break;
        b = *p++; x |= (b & 0x7f) << 49; if (b >= 0) break;
        b = *p++; x |= (b & 0x7f) << 56; if (b >= 0) break;
        b = *p++; x |= static_cast<uint64_t>(b & 0x7f) << 63; if (b >= 0) break;
        // clang-format on
        CHECK(false); // varints > 64bits == data corruption
      } while (false);
      *src = reinterpret_cast<const char*>(p);
      return x;
    }
    const char* p = *src;
    uint64_t val = 0;
    int32_t shift = 0;
    while (*p & 0x80) {
      CHECK_GT(max_size, 1); // We must have room for the last byte, too
      --max_size;
      val |= static_cast<uint64_t>(*p++ & 0x7f) << shift;
      shift += 7;
    }
    val |= static_cast<uint64_t>(*p++) << shift;
    *src = p;
    return val;
  }
  static UInt128 decode128(const char** src, int64_t max_size) {
    if (max_size > kMaxSize128) {
      // Varint-encoded numbers are at most 19 bytes, and we want to catch
      // the case where we have more than 18 consecutive bytes with bit 7 set
      // (which would be an error)
      max_size = kMaxSize128;
    }
    const char* p = *src;
    UInt128 val = 0;
    int32_t shift = 0;
    while (*p & 0x80) {
      CHECK_GT(max_size, 1); // We must have room for the last byte, too
      --max_size;
      val |= UInt128(*p++ & 0x7f) << shift;
      shift += 7;
    }
    val |= UInt128(*p++) << shift;
    *src = p;
    return val;
  }

  // Decode a value from a StringPiece, and advance the StringPiece.
  static uint64_t decode(folly::StringPiece* data) {
    const char* p = data->start();
    uint64_t val = decode(&p, data->size());
    data->advance(p - data->start());
    return val;
  }
  static UInt128 decode128(folly::StringPiece* data) {
    const char* p = data->start();
    UInt128 val = decode128(&p, data->size());
    data->advance(p - data->start());
    return val;
  }

  // Decode a value from a ByteSource, and advance the ByteSource.
  static uint64_t decodeFromByteSource(strings::ByteSource* src) {
    uint64_t val = 0;
    int32_t shift = 0;
    int32_t max_size = kMaxSize64;
    folly::StringPiece chunk;
    int32_t remaining = 0;
    const char* p = nullptr;
    for (;;) {
      if (remaining == 0) {
        CHECK(src->next(&chunk));
        p = chunk.start();
        remaining = chunk.size();
        DCHECK_GT(remaining, 0);
      }
      --remaining;
      if (*p & 0x80) {
        CHECK_GT(max_size, 1); // We must have room for the last byte, too
        --max_size;
        val |= static_cast<uint64_t>(*p++ & 0x7f) << shift;
        shift += 7;
      } else {
        val |= static_cast<uint64_t>(*p++) << shift;
        break;
      }
    }
    if (remaining) {
      src->backUp(remaining);
    }
    return val;
  }
  static UInt128 decode128FromByteSource(strings::ByteSource* src) {
    UInt128 val = 0;
    int32_t shift = 0;
    int32_t max_size = kMaxSize128;
    folly::StringPiece chunk;
    int32_t remaining = 0;
    const char* p = nullptr;
    for (;;) {
      if (remaining == 0) {
        CHECK(src->next(&chunk));
        p = chunk.start();
        remaining = chunk.size();
        DCHECK_GT(remaining, 0);
      }
      --remaining;
      if (*p & 0x80) {
        CHECK_GT(max_size, 1); // We must have room for the last byte, too
        --max_size;
        val |= UInt128(*p++ & 0x7f) << shift;
        shift += 7;
      } else {
        val |= UInt128(*p++) << shift;
        break;
      }
    }
    if (remaining) {
      src->backUp(remaining);
    }
    return val;
  }
};

// Zig-zag encoding that maps signed integers with a small absolute value
// to unsigned integers with a small (positive) value.
// if x >= 0, ZigZag::encode(x) == 2*x
// if x <  0, ZigZag::encode(x) == -2*x + 1
class ZigZag {
 public:
  static uint64_t encode(int64_t val) {
    // Bit-twiddling magic stolen from the Google protocol buffer document;
    // val >> 63 is an arithmetic shift because val is signed
    return (static_cast<uint64_t>(val) << 1) ^ (val >> 63);
  }

  static int64_t decode(uint64_t val) {
    return static_cast<int64_t>((val >> 1) ^ -(val & 1));
  }
};

namespace internal {
class ByteSinkAppender {
 public:
  /* implicit */ ByteSinkAppender(strings::ByteSink* out) : out_(out) {}
  void operator()(folly::StringPiece sp) {
    out_->append(sp.data(), sp.size());
  }

 private:
  strings::ByteSink* out_;
};
} // namespace internal

// Import GroupVarint encoding / decoding code from folly

typedef folly::GroupVarint32 GroupVarint32;
typedef folly::GroupVarint64 GroupVarint64;

typedef folly::GroupVarintEncoder<uint32_t, internal::ByteSinkAppender>
    GroupVarint32Encoder;
typedef folly::GroupVarintEncoder<uint64_t, internal::ByteSinkAppender>
    GroupVarint64Encoder;

typedef folly::GroupVarintDecoder<uint32_t> GroupVarint32Decoder;
typedef folly::GroupVarintDecoder<uint64_t> GroupVarint64Decoder;

} // namespace facebook

#endif /* FACEBOOK_COMMON_ENCODE_CODING_H_ */

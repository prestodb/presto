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
#include "velox/common/encode/Base64.h"

#include <folly/Portability.h>
#include <folly/container/Foreach.h>
#include <folly/io/Cursor.h>
#include <stdint.h>

namespace facebook::velox::encoding {

constexpr const Base64::Charset kBase64Charset = {
    'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
    'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
    'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
    'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '+', '/'};
constexpr const Base64::Charset kBase64UrlCharset = {
    'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
    'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
    'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
    'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '-', '_'};

constexpr const Base64::ReverseIndex kBase64ReverseIndexTable = {
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 62,  255,
    255, 255, 63,  52,  53,  54,  55,  56,  57,  58,  59,  60,  61,  255, 255,
    255, 255, 255, 255, 255, 0,   1,   2,   3,   4,   5,   6,   7,   8,   9,
    10,  11,  12,  13,  14,  15,  16,  17,  18,  19,  20,  21,  22,  23,  24,
    25,  255, 255, 255, 255, 255, 255, 26,  27,  28,  29,  30,  31,  32,  33,
    34,  35,  36,  37,  38,  39,  40,  41,  42,  43,  44,  45,  46,  47,  48,
    49,  50,  51,  255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255};
constexpr const Base64::ReverseIndex kBase64UrlReverseIndexTable = {
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 62,  255,
    62,  255, 63,  52,  53,  54,  55,  56,  57,  58,  59,  60,  61,  255, 255,
    255, 255, 255, 255, 255, 0,   1,   2,   3,   4,   5,   6,   7,   8,   9,
    10,  11,  12,  13,  14,  15,  16,  17,  18,  19,  20,  21,  22,  23,  24,
    25,  255, 255, 255, 255, 63,  255, 26,  27,  28,  29,  30,  31,  32,  33,
    34,  35,  36,  37,  38,  39,  40,  41,  42,  43,  44,  45,  46,  47,  48,
    49,  50,  51,  255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255};

constexpr bool checkForwardIndex(
    uint8_t idx,
    const Base64::Charset& charset,
    const Base64::ReverseIndex& table) {
  return (table[static_cast<uint8_t>(charset[idx])] == idx) &&
      (idx > 0 ? checkForwardIndex(idx - 1, charset, table) : true);
}
// Verify that for every entry in kBase64Charset, the corresponding entry
// in kBase64ReverseIndexTable is correct.
static_assert(
    checkForwardIndex(
        sizeof(kBase64Charset) - 1,
        kBase64Charset,
        kBase64ReverseIndexTable),
    "kBase64Charset has incorrect entries");
// Verify that for every entry in kBase64UrlCharset, the corresponding entry
// in kBase64UrlReverseIndexTable is correct.
static_assert(
    checkForwardIndex(
        sizeof(kBase64UrlCharset) - 1,
        kBase64UrlCharset,
        kBase64UrlReverseIndexTable),
    "kBase64UrlCharset has incorrect entries");
// Similar to strchr(), but for null-terminated const strings.
// Another difference is that we do not consider "\0" to be present in the
// string.
// Returns true if "str" contains the character c.
constexpr bool constCharsetContains(
    const Base64::Charset& charset,
    uint8_t idx,
    const char c) {
  return idx < charset.size() &&
      ((charset[idx] == c) || constCharsetContains(charset, idx + 1, c));
}
constexpr bool checkReverseIndex(
    uint8_t idx,
    const Base64::Charset& charset,
    const Base64::ReverseIndex& table) {
  return (table[idx] == 255
              ? !constCharsetContains(charset, 0, static_cast<char>(idx))
              : (charset[table[idx]] == idx)) &&
      (idx > 0 ? checkReverseIndex(idx - 1, charset, table) : true);
}
// Verify that for every entry in kBase64ReverseIndexTable, the corresponding
// entry in kBase64Charset is correct.
static_assert(
    checkReverseIndex(
        sizeof(kBase64ReverseIndexTable) - 1,
        kBase64Charset,
        kBase64ReverseIndexTable),
    "kBase64ReverseIndexTable has incorrect entries.");
// Verify that for every entry in kBase64ReverseIndexTable, the corresponding
// entry in kBase64Charset is correct.
// We can't run this check as the URL version has two duplicate entries so that
// the url decoder can handle url encodings and default encodings
// static_assert(
//     checkReverseIndex(
//         sizeof(kBase64UrlReverseIndexTable) - 1,
//         kBase64UrlCharset,
//         kBase64UrlReverseIndexTable),
//     "kBase64UrlReverseIndexTable has incorrect entries.");

template <class T>
/*  static */ std::string
Base64::encodeImpl(const T& data, const Charset& charset, bool include_pad) {
  size_t outlen = calculateEncodedSize(data.size(), include_pad);

  std::string out;
  out.resize(outlen);

  encodeImpl(data, charset, include_pad, out.data());
  return out;
}

// static
size_t Base64::calculateEncodedSize(size_t size, bool withPadding) {
  if (size == 0) {
    return 0;
  }

  // Calculate the output size assuming that we are including padding.
  size_t encodedSize = ((size + 2) / 3) * 4;
  if (!withPadding) {
    // If the padding was not requested, subtract the padding bytes.
    encodedSize -= (3 - (size % 3)) % 3;
  }
  return encodedSize;
}

// static
void Base64::encode(const char* data, size_t len, char* output) {
  encodeImpl(folly::StringPiece(data, len), kBase64Charset, true, output);
}

template <class T>
/* static */ void Base64::encodeImpl(
    const T& data,
    const Charset& charset,
    bool include_pad,
    char* out) {
  auto len = data.size();
  if (len == 0) {
    return;
  }

  auto wp = out;
  auto it = data.begin();

  // For each group of 3 bytes (24 bits) in the input, split that into
  // 4 groups of 6 bits and encode that using the supplied charset lookup
  for (; len > 2; len -= 3) {
    uint32_t curr = uint8_t(*it++) << 16;
    curr |= uint8_t(*it++) << 8;
    curr |= uint8_t(*it++);

    *wp++ = charset[(curr >> 18) & 0x3f];
    *wp++ = charset[(curr >> 12) & 0x3f];
    *wp++ = charset[(curr >> 6) & 0x3f];
    *wp++ = charset[curr & 0x3f];
  }

  if (len > 0) {
    // We have either 1 or 2 input bytes left.  Encode this similar to the
    // above (assuming 0 for all other bytes).  Optionally append the '='
    // character if it is requested.
    uint32_t curr = uint8_t(*it++) << 16;
    *wp++ = charset[(curr >> 18) & 0x3f];
    if (len > 1) {
      curr |= uint8_t(*it) << 8;
      *wp++ = charset[(curr >> 12) & 0x3f];
      *wp++ = charset[(curr >> 6) & 0x3f];
      if (include_pad) {
        *wp = kBase64Pad;
      }
    } else {
      *wp++ = charset[(curr >> 12) & 0x3f];
      if (include_pad) {
        *wp++ = kBase64Pad;
        *wp = kBase64Pad;
      }
    }
  }
}

std::string Base64::encode(folly::StringPiece text) {
  return encodeImpl(text, kBase64Charset, true);
}

std::string Base64::encode(const char* data, size_t len) {
  return encode(folly::StringPiece(data, len));
}

namespace {

/**
 * this is a quick and dirty iterator implementation for an IOBuf so that the
 * template that uses iterators can work on IOBuf chains.  It only implements
 * postfix increment because that is all the algorithm needs, and it is a noop
 * since the read<>() function already incremented the cursor.
 */
class IOBufWrapper {
 private:
  class Iterator {
   public:
    explicit Iterator(const folly::IOBuf* data) : cs_(data) {}

    Iterator& operator++(int32_t) {
      // This is a noop since reading from the Cursor has already moved the
      // position
      return *this;
    }

    uint8_t operator*() {
      // This will read _and_ increment
      return cs_.read<uint8_t>();
    }

   private:
    folly::io::Cursor cs_;
  };

 public:
  explicit IOBufWrapper(const folly::IOBuf* data) : data_(data) {}

  size_t size() const {
    return data_->computeChainDataLength();
  }

  Iterator begin() const {
    return Iterator(data_);
  }

 private:
  const folly::IOBuf* data_;
};

} // namespace

std::string Base64::encode(const folly::IOBuf* data) {
  return encodeImpl(IOBufWrapper(data), kBase64Charset, true);
}

void Base64::encodeAppend(folly::StringPiece text, std::string& out) {
  size_t outlen = calculateEncodedSize(text.size(), true);

  size_t initialLen = out.size();
  out.resize(initialLen + outlen);
  encodeImpl(text, kBase64Charset, true, out.data() + initialLen);
}

std::string Base64::decode(folly::StringPiece encoded) {
  std::string output;
  Base64::decode(std::make_pair(encoded.data(), encoded.size()), output);
  return output;
}

void Base64::decode(
    const std::pair<const char*, int32_t>& payload,
    std::string& output) {
  size_t out_len = payload.second / 4 * 3;
  output.resize(out_len, '\0');
  out_len = Base64::decode(payload.first, payload.second, &output[0], out_len);
  output.resize(out_len);
}

// static
void Base64::decode(const char* data, size_t size, char* output) {
  size_t out_len = size / 4 * 3;
  Base64::decode(data, size, output, out_len);
}

uint8_t Base64::Base64ReverseLookup(
    char p,
    const Base64::ReverseIndex& reverse_lookup) {
  auto curr = reverse_lookup[(uint8_t)p];
  if (curr >= 0x40) {
    throw Base64Exception(
        "Base64::decode() - invalid input string: invalid characters");
  }

  return curr;
}

size_t
Base64::decode(const char* src, size_t src_len, char* dst, size_t dst_len) {
  return decodeImpl(src, src_len, dst, dst_len, kBase64ReverseIndexTable, true);
}

// static
size_t
Base64::calculateDecodedSize(const char* data, size_t& size, bool withPadding) {
  if (size == 0) {
    return 0;
  }

  auto needed = (size / 4) * 3;
  if (withPadding) {
    // If the pad characters are included then the source string must be a
    // multiple of 4 and we can query the end of the string to see how much
    // padding exists.
    if (size % 4 != 0) {
      throw Base64Exception(
          "Base64::decode() - invalid input string: "
          "string length is not multiple of 4.");
    }

    auto padding = countPadding(data, size);
    size -= padding;
    return needed - padding;
  }

  // If padding doesn't exist we need to calculate it from the size - if the
  // size % 4 is 0 then we have an even multiple 3 byte chunks in the result
  // if it is 2 then we need 1 more byte in the output.  If it is 3 then we
  // need 2 more bytes in the output.  It should never be 1.
  auto extra = size % 4;
  if (extra) {
    if (extra == 1) {
      throw Base64Exception(
          "Base64::decode() - invalid input string: "
          "string length cannot be 1 more than a multiple of 4.");
    }
    return needed + extra - 1;
  }

  // Just because we don't need the pad, doesn't mean it is not there.  The
  // URL decoder should be able to handle the original encoding.
  auto padding = countPadding(data, size);
  size -= padding;
  return needed - padding;
}

size_t Base64::decodeImpl(
    const char* src,
    size_t src_len,
    char* dst,
    size_t dst_len,
    const Base64::ReverseIndex& reverse_lookup,
    bool include_pad) {
  if (!src_len) {
    return 0;
  }

  auto needed = calculateDecodedSize(src, src_len, include_pad);
  if (dst_len < needed) {
    throw Base64Exception(
        "Base64::decode() - invalid output string: "
        "output string is too small.");
  }

  // Handle full groups of 4 characters
  for (; src_len > 4; src_len -= 4, src += 4, dst += 3) {
    // Each character of the 4 encode 6 bits of the original, grab each with
    // the appropriate shifts to rebuild the original and then split that back
    // into the original 8 bit bytes.
    uint32_t last = (Base64ReverseLookup(src[0], reverse_lookup) << 18) |
        (Base64ReverseLookup(src[1], reverse_lookup) << 12) |
        (Base64ReverseLookup(src[2], reverse_lookup) << 6) |
        Base64ReverseLookup(src[3], reverse_lookup);
    dst[0] = (last >> 16) & 0xff;
    dst[1] = (last >> 8) & 0xff;
    dst[2] = last & 0xff;
  }

  // Handle the last 2-4 characters.  This is similar to the above, but the
  // last 2 characters may or may not exist.
  DCHECK(src_len >= 2);
  uint32_t last = (Base64ReverseLookup(src[0], reverse_lookup) << 18) |
      (Base64ReverseLookup(src[1], reverse_lookup) << 12);
  dst[0] = (last >> 16) & 0xff;
  if (src_len > 2) {
    last |= Base64ReverseLookup(src[2], reverse_lookup) << 6;
    dst[1] = (last >> 8) & 0xff;
    if (src_len > 3) {
      last |= Base64ReverseLookup(src[3], reverse_lookup);
      dst[2] = last & 0xff;
    }
  }

  return needed;
}

std::string Base64::encode_url(folly::StringPiece text) {
  return encodeImpl(text, kBase64UrlCharset, false);
}

std::string Base64::encode_url(const char* data, size_t len) {
  return encode_url(folly::StringPiece(data, len));
}

std::string Base64::encode_url(const folly::IOBuf* data) {
  return encodeImpl(IOBufWrapper(data), kBase64UrlCharset, false);
}

std::string Base64::decode_url(folly::StringPiece encoded) {
  std::string output;
  Base64::decode_url(std::make_pair(encoded.data(), encoded.size()), output);
  return output;
}

void Base64::decode_url(
    const std::pair<const char*, int32_t>& payload,
    std::string& output) {
  size_t out_len = (payload.second + 3) / 4 * 3;
  output.resize(out_len, '\0');
  out_len = Base64::decodeImpl(
      payload.first,
      payload.second,
      &output[0],
      out_len,
      kBase64UrlReverseIndexTable,
      false);
  output.resize(out_len);
}

// // uint32_t Base64::base64_encode_string(
// //     const std::string& str,
// //     std::string& out) {
// //   uint8_t b[4];
// //   uint32_t result = 0;

// //   out.clear();
// //   const uint8_t* bytes = (const uint8_t*)str.data();
// //   uint32_t len = str.length();
// //   out.reserve((len + 2) / 3 * 4);
// //   while (len >= 3) {
// //     apache::thrift::protocol::base64_encode(bytes, 3, b);
// //     out.append(1, b[0]);
// //     out.append(1, b[1]);
// //     out.append(1, b[2]);
// //     out.append(1, b[3]);
// //     result += 4;
// //     bytes += 3;
// //     len -= 3;
// //   }
// //   if (len > 0) {
// //     apache::thrift::protocol::base64_encode((const uint8_t*)bytes, len,
// b);
// //     for (int32_t j = 0; j < len + 1; j++) {
// //       out.append(1, b[j]);
// //     }
// //     for (int32_t j = 0; j < 3 - len; j++) {
// //       out.append(1, '=');
// //     }
// //     result += len + 1;
// //   }
// //   return result;
// // }

} // namespace facebook::velox::encoding

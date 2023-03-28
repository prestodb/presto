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

#include <exception>
#include <map>
#include <string>

#include <folly/Range.h>
#include <folly/io/IOBuf.h>

namespace facebook::velox::encoding {

class Base64Exception : public std::exception {
 public:
  explicit Base64Exception(const char* msg) : msg_(msg) {}
  const char* what() const noexcept override {
    return msg_;
  }

 protected:
  const char* msg_;
};

class Base64 {
 public:
  using Charset = std::array<char, 64>;
  using ReverseIndex = std::array<uint8_t, 256>;

  static std::string encode(const char* data, size_t len);
  static std::string encode(folly::StringPiece text);
  static std::string encode(const folly::IOBuf* text);

  /// Returns encoded size for the input of the specified size.
  static size_t calculateEncodedSize(size_t size, bool withPadding = true);

  /// Encodes the specified number of characters from the 'data' and writes the
  /// result to the 'output'. The output must have enough space, e.g. as
  /// returned by the calculateEncodedSize().
  static void encode(const char* data, size_t size, char* output);

  // Appends the encoded text to out.
  static void encodeAppend(folly::StringPiece text, std::string& out);

  static std::string decode(folly::StringPiece encoded);

  /// Returns decoded size for the specified input. Adjusts the 'size' to
  /// subtract the length of the padding, if exists.
  static size_t
  calculateDecodedSize(const char* data, size_t& size, bool withPadding = true);

  /// Decodes the specified number of characters from the 'data' and writes the
  /// result to the 'output'. The output must have enough space, e.g. as
  /// returned by the calculateDecodedSize().
  static void decode(const char* data, size_t size, char* output);

  static void decode(
      const std::pair<const char*, int32_t>& payload,
      std::string& outp);

  /// Encodes the specified number of characters from the 'data' and writes the
  /// result to the 'output'. The output must have enough space, e.g. as
  /// returned by the calculateEncodedSize().
  static void encodeUrl(const char* data, size_t size, char* output);

  // compatible with www's Base64URL::encode/decode
  // TODO rename encode_url/decode_url to encodeUrl/encodeUrl.
  static std::string encode_url(const char* data, size_t len);
  static std::string encode_url(const folly::IOBuf* data);
  static std::string encode_url(folly::StringPiece text);
  static void decode_url(
      const std::pair<const char*, int32_t>& payload,
      std::string& output);
  static std::string decode_url(folly::StringPiece text);

  /*
   * Take str(base64 encoded string) and writes the decoded string to out.
   */
  static void base64_decode_string(const std::string& str, std::string& out);

  static size_t
  decode(const char* src, size_t src_len, char* dst, size_t dst_len);

 private:
  constexpr static char kBase64Pad = '=';
  static inline size_t countPadding(const char* src, size_t len) {
    DCHECK_GE(len, 2);
    return src[len - 1] != kBase64Pad ? 0 : src[len - 2] != kBase64Pad ? 1 : 2;
  }

  static uint8_t Base64ReverseLookup(char p, const ReverseIndex& table);

  template <class T>
  static std::string
  encodeImpl(const T& data, const Charset& charset, bool include_pad);

  template <class T>
  static void encodeImpl(
      const T& data,
      const Charset& charset,
      bool include_pad,
      char* out);

  static size_t decodeImpl(
      const char* src,
      size_t src_len,
      char* dst,
      size_t dst_len,
      const ReverseIndex& table,
      bool include_pad);
};

} // namespace facebook::velox::encoding

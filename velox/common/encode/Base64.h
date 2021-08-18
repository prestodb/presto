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
#ifndef FACEBOOK_COMMON_ENCODE_BASE64_H_
#define FACEBOOK_COMMON_ENCODE_BASE64_H_

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
  static std::string decode(folly::StringPiece encoded);

  // Appends the encoded text to out.
  static void encodeAppend(folly::StringPiece text, std::string& out);

  static void decode(
      const std::pair<const char*, int32_t>& payload,
      std::string& outp);

  // compatible with www's Base64URL::encode/decode
  static std::string encode_url(const char* data, size_t len);
  static std::string encode_url(const folly::IOBuf* data);
  static std::string encode_url(folly::StringPiece text);
  static void decode_url(
      const std::pair<const char*, int32_t>& payload,
      std::string& output);
  static std::string decode_url(folly::StringPiece text);

  /*
   * Take str and writes the encoded string to out.
   */
  static uint32_t base64_encode_string(
      const std::string& str,
      std::string& out);

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

  static size_t
  calcDecodedSize(const char* src, size_t& src_len, bool include_pad);
  static uint8_t Base64ReverseLookup(char p, const ReverseIndex& table);

  template <class T>
  static std::string
  encodeImpl(const T& data, const Charset& charset, bool include_pad);

  template <class T>
  static void encodeImpl(
      const T& data,
      const Charset& charset,
      bool include_pad,
      std::string& out);

  static size_t decodeImpl(
      const char* src,
      size_t src_len,
      char* dst,
      size_t dst_len,
      const ReverseIndex& table,
      bool include_pad);
};

} // namespace facebook::velox::encoding

#endif // FACEBOOK_COMMON_ENCODE_BASE64_H_

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

#include <folly/hash/Checksum.h>
#define XXH_INLINE_ALL
#include <xxhash.h>

#include "folly/ssl/OpenSSLHash.h"
#include "velox/common/base/BitUtil.h"
#include "velox/common/encode/Base64.h"
#include "velox/external/md5/md5.h"
#include "velox/functions/Udf.h"
#include "velox/functions/lib/ToHex.h"
#include "velox/functions/lib/string/StringImpl.h"

namespace facebook::velox::functions {

/// crc32(varbinary) → bigint
/// Return an int64_t checksum calculated using the crc32 method in zlib.
template <typename T>
struct CRC32Function {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE
  void call(out_type<int64_t>& result, const arg_type<Varchar>& input) {
    result = static_cast<int64_t>(folly::crc32_type(
        reinterpret_cast<const unsigned char*>(input.data()), input.size()));
  }
};

/// xxhash64(varbinary) → varbinary
/// Return an 8-byte binary to hash64 of input (varbinary such as string)
template <typename T>
struct XxHash64Function {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE
  void call(out_type<Varbinary>& result, const arg_type<Varbinary>& input) {
    // Seed is set to 0.
    int64_t hash = folly::Endian::swap64(XXH64(input.data(), input.size(), 0));
    static const auto kLen = sizeof(int64_t);

    // Resizing output and copy
    result.resize(kLen);
    std::memcpy(result.data(), &hash, kLen);
  }
};

/// md5(varbinary) → varbinary
template <typename T>
struct Md5Function {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  template <typename TTo, typename TFrom>
  FOLLY_ALWAYS_INLINE void call(TTo& result, const TFrom& input) {
    static const auto kByteLength = 16;
    result.resize(kByteLength);
    crypto::MD5Context md5Context;
    md5Context.Add((const uint8_t*)input.data(), input.size());
    md5Context.Finish((uint8_t*)result.data());
  }
};

/// sha1(varbinary) -> varbinary
template <typename T>
struct Sha1Function {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE
  void call(out_type<Varbinary>& result, const arg_type<Varbinary>& input) {
    result.resize(20);
    folly::ssl::OpenSSLHash::sha1(
        folly::MutableByteRange((uint8_t*)result.data(), result.size()),
        folly::ByteRange((const uint8_t*)input.data(), input.size()));
  }
};

/// sha256(varbinary) -> varbinary
template <typename T>
struct Sha256Function {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  template <typename TTo, typename TFrom>
  FOLLY_ALWAYS_INLINE void call(TTo& result, const TFrom& input) {
    result.resize(32);
    folly::ssl::OpenSSLHash::sha256(
        folly::MutableByteRange((uint8_t*)result.data(), result.size()),
        folly::ByteRange((const uint8_t*)input.data(), input.size()));
  }
};

/// sha512(varbinary) -> varbinary
template <typename T>
struct Sha512Function {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  template <typename TTo, typename TFrom>
  FOLLY_ALWAYS_INLINE void call(TTo& result, const TFrom& input) {
    result.resize(64);
    folly::ssl::OpenSSLHash::sha512(
        folly::MutableByteRange((uint8_t*)result.data(), result.size()),
        folly::ByteRange((const uint8_t*)input.data(), input.size()));
  }
};

/// spooky_hash_v2_32(varbinary) -> varbinary
template <typename T>
struct SpookyHashV232Function {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE
  void call(out_type<Varbinary>& result, const arg_type<Varbinary>& input) {
    // Swap bytes with folly::Endian::swap32 similar to the Java implementation,
    // Velox and SpookyHash only support little-endian platforms.
    uint32_t hash = folly::Endian::swap32(
        folly::hash::SpookyHashV2::Hash32(input.data(), input.size(), 0));
    static const auto kHashLength = sizeof(int32_t);
    result.resize(kHashLength);
    std::memcpy(result.data(), &hash, kHashLength);
  }
};

/// spooky_hash_v2_64(varbinary) -> varbinary
template <typename T>
struct SpookyHashV264Function {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE
  void call(out_type<Varbinary>& result, const arg_type<Varbinary>& input) {
    // Swap bytes with folly::Endian::swap64 similar to the Java implementation,
    // Velox and SpookyHash only support little-endian platforms.
    uint64_t hash = folly::Endian::swap64(
        folly::hash::SpookyHashV2::Hash64(input.data(), input.size(), 0));
    static const auto kHashLength = sizeof(int64_t);
    result.resize(kHashLength);
    std::memcpy(result.data(), &hash, kHashLength);
  }
};

/// hmac_sha1(varbinary) -> varbinary
template <typename T>
struct HmacSha1Function {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  template <typename TOutput, typename TInput>
  FOLLY_ALWAYS_INLINE void
  call(TOutput& result, const TInput& data, const TInput& key) {
    result.resize(20);
    folly::ssl::OpenSSLHash::hmac_sha1(
        folly::MutableByteRange((uint8_t*)result.data(), result.size()),
        folly::ByteRange((const uint8_t*)key.data(), key.size()),
        folly::ByteRange((const uint8_t*)data.data(), data.size()));
  }
};

/// hmac_sha256(varbinary) -> varbinary
template <typename T>
struct HmacSha256Function {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  template <typename TTo, typename TFrom>
  FOLLY_ALWAYS_INLINE void
  call(TTo& result, const TFrom& data, const TFrom& key) {
    result.resize(32);
    folly::ssl::OpenSSLHash::hmac_sha256(
        folly::MutableByteRange((uint8_t*)result.data(), result.size()),
        folly::ByteRange((const uint8_t*)key.data(), key.size()),
        folly::ByteRange((const uint8_t*)data.data(), data.size()));
  }
};

/// hmac_sha512(varbinary) -> varbinary
template <typename T>
struct HmacSha512Function {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  template <typename TTo, typename TFrom>
  FOLLY_ALWAYS_INLINE void
  call(TTo& result, const TFrom& data, const TFrom& key) {
    result.resize(64);
    folly::ssl::OpenSSLHash::hmac_sha512(
        folly::MutableByteRange((uint8_t*)result.data(), result.size()),
        folly::ByteRange((const uint8_t*)key.data(), key.size()),
        folly::ByteRange((const uint8_t*)data.data(), data.size()));
  }
};

/// hmac_md5(varbinary) -> varbinary
template <typename T>
struct HmacMd5Function {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varbinary>& result,
      const arg_type<Varbinary>& data,
      const arg_type<Varbinary>& key) {
    result.resize(16);
    folly::ssl::OpenSSLHash::hmac(
        folly::MutableByteRange((uint8_t*)result.data(), result.size()),
        EVP_md5(),
        folly::ByteRange((const uint8_t*)key.data(), key.size()),
        folly::ByteRange((const uint8_t*)data.data(), data.size()));
  }
};

template <typename T>
struct ToHexFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varbinary>& result,
      const arg_type<Varchar>& input) {
    ToHexUtil::toHex(input, result);
  }
};

FOLLY_ALWAYS_INLINE static uint8_t fromHex(char c) {
  if (c >= '0' && c <= '9') {
    return c - '0';
  }

  if (c >= 'A' && c <= 'F') {
    return 10 + c - 'A';
  }

  if (c >= 'a' && c <= 'f') {
    return 10 + c - 'a';
  }

  VELOX_USER_FAIL("Invalid hex character: {}", c);
}

template <typename T>
struct FromHexFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Varbinary>& input) {
    VELOX_USER_CHECK_EQ(
        input.size() % 2,
        0,
        "Invalid input length for from_hex(): {}",
        input.size());

    const auto resultSize = input.size() / 2;
    result.resize(resultSize);

    const char* inputBuffer = input.data();
    char* resultBuffer = result.data();

    for (auto i = 0; i < resultSize; ++i) {
      resultBuffer[i] =
          (fromHex(inputBuffer[i * 2]) << 4) | fromHex(inputBuffer[i * 2 + 1]);
    }
  }
};

template <typename T>
struct ToBase64Function {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Varbinary>& input) {
    result.resize(encoding::Base64::calculateEncodedSize(input.size()));
    encoding::Base64::encode(input.data(), input.size(), result.data());
  }
};

template <typename T>
struct FromBase64Function {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE void call(
      out_type<Varbinary>& result,
      const arg_type<Varchar>& input) {
    try {
      auto inputSize = input.size();
      result.resize(
          encoding::Base64::calculateDecodedSize(input.data(), inputSize));
      encoding::Base64::decode(
          input.data(), inputSize, result.data(), result.size());
    } catch (const encoding::Base64Exception& e) {
      VELOX_USER_FAIL(e.what());
    }
  }
};

template <typename T>
struct FromBase64UrlFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE void call(
      out_type<Varbinary>& result,
      const arg_type<Varchar>& input) {
    auto inputSize = input.size();
    result.resize(
        encoding::Base64::calculateDecodedSize(input.data(), inputSize));
    encoding::Base64::decodeUrl(
        input.data(), inputSize, result.data(), result.size());
  }
};

template <typename T>
struct ToBase64UrlFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Varbinary>& input) {
    result.resize(encoding::Base64::calculateEncodedSize(input.size()));
    encoding::Base64::encodeUrl(input.data(), input.size(), result.data());
  }
};

template <typename T>
struct FromBigEndian32 {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE
  void call(out_type<int32_t>& result, const arg_type<Varbinary>& input) {
    static constexpr auto kTypeLength = sizeof(int32_t);
    VELOX_USER_CHECK_EQ(input.size(), kTypeLength, "Expected 4-byte input");
    memcpy(&result, input.data(), kTypeLength);
    result = folly::Endian::big(result);
  }
};

template <typename T>
struct ToBigEndian32 {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE
  void call(out_type<Varbinary>& result, const arg_type<int32_t>& input) {
    static constexpr auto kTypeLength = sizeof(int32_t);
    auto value = folly::Endian::big(input);
    result.setNoCopy(
        StringView(reinterpret_cast<const char*>(&value), kTypeLength));
  }
};

template <typename T>
struct FromBigEndian64 {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE
  void call(out_type<int64_t>& result, const arg_type<Varbinary>& input) {
    static constexpr auto kTypeLength = sizeof(int64_t);
    VELOX_USER_CHECK_EQ(input.size(), kTypeLength, "Expected 8-byte input");
    memcpy(&result, input.data(), kTypeLength);
    result = folly::Endian::big(result);
  }
};

template <typename T>
struct ToBigEndian64 {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE
  void call(out_type<Varbinary>& result, const arg_type<int64_t>& input) {
    static constexpr auto kTypeLength = sizeof(int64_t);
    auto value = folly::Endian::big(input);
    result.setNoCopy(
        StringView(reinterpret_cast<const char*>(&value), kTypeLength));
  }
};

template <typename T>
struct ToIEEE754Bits64 {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varbinary>& result,
      const arg_type<double>& input) {
    static constexpr auto kTypeLength = sizeof(int64_t);
    auto value = folly::Endian::big(input);
    result.setNoCopy(
        StringView(reinterpret_cast<const char*>(&value), kTypeLength));
  }
};

template <typename T>
struct FromIEEE754Bits64 {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<double>& result,
      const arg_type<Varbinary>& input) {
    static constexpr auto kTypeLength = sizeof(int64_t);
    VELOX_USER_CHECK_EQ(input.size(), kTypeLength, "Expected 8-byte input");
    memcpy(&result, input.data(), kTypeLength);
    result = folly::Endian::big(result);
  }
};

template <typename T>
struct ToIEEE754Bits32 {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varbinary>& result,
      const arg_type<float>& input) {
    static constexpr auto kTypeLength = sizeof(int32_t);
    auto value = folly::Endian::big(input);
    result.setNoCopy(
        StringView(reinterpret_cast<const char*>(&value), kTypeLength));
  }
};

template <typename T>
struct FromIEEE754Bits32 {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<float>& result,
      const arg_type<Varbinary>& input) {
    static constexpr auto kTypeLength = sizeof(int32_t);
    VELOX_USER_CHECK_EQ(
        input.size(),
        kTypeLength,
        "Input floating-point value must be exactly 4 bytes long");
    memcpy(&result, input.data(), kTypeLength);
    result = folly::Endian::big(result);
  }
};

/// lpad(binary, size, padbinary) -> varbinary
///     Left pads input to size characters with padding.  If size is
///     less than the length of input, the result is truncated to size
///     characters.  size must not be negative and padding must be non-empty.
/// rpad(binary, size, padbinary) -> varbinary
///     Right pads input to size characters with padding.  If size is
///     less than the length of input, the result is truncated to size
///     characters.  size must not be negative and padding must be non-empty.
template <typename T, bool lpad>
struct PadFunctionVarbinaryBase {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varbinary>& result,
      const arg_type<Varbinary>& binary,
      const arg_type<int64_t>& size,
      const arg_type<Varbinary>& padbinary) {
    stringImpl::pad<lpad, false /*isAscii*/>(result, binary, size, padbinary);
  }
};

template <typename T>
struct LPadVarbinaryFunction : public PadFunctionVarbinaryBase<T, true> {};

template <typename T>
struct RPadVarbinaryFunction : public PadFunctionVarbinaryBase<T, false> {};

} // namespace facebook::velox::functions

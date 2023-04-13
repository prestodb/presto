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
#include "velox/common/encode/Base64.h"
#include "velox/external/md5/md5.h"
#include "velox/functions/Udf.h"

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

FOLLY_ALWAYS_INLINE unsigned char toHex(unsigned char c) {
  return c < 10 ? (c + '0') : (c + 'A' - 10);
}

template <typename T>
struct ToHexFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varbinary>& result,
      const arg_type<Varchar>& input) {
    static const char* const kHexTable =
        "000102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F"
        "202122232425262728292A2B2C2D2E2F303132333435363738393A3B3C3D3E3F"
        "404142434445464748494A4B4C4D4E4F505152535455565758595A5B5C5D5E5F"
        "606162636465666768696A6B6C6D6E6F707172737475767778797A7B7C7D7E7F"
        "808182838485868788898A8B8C8D8E8F909192939495969798999A9B9C9D9E9F"
        "A0A1A2A3A4A5A6A7A8A9AAABACADAEAFB0B1B2B3B4B5B6B7B8B9BABBBCBDBEBF"
        "C0C1C2C3C4C5C6C7C8C9CACBCCCDCECFD0D1D2D3D4D5D6D7D8D9DADBDCDDDEDF"
        "E0E1E2E3E4E5E6E7E8E9EAEBECEDEEEFF0F1F2F3F4F5F6F7F8F9FAFBFCFDFEFF";

    const auto inputSize = input.size();
    result.resize(inputSize * 2);

    const unsigned char* inputBuffer =
        reinterpret_cast<const unsigned char*>(input.data());
    char* resultBuffer = result.data();

    for (auto i = 0; i < inputSize; ++i) {
      resultBuffer[i * 2] = kHexTable[inputBuffer[i] * 2];
      resultBuffer[i * 2 + 1] = kHexTable[inputBuffer[i] * 2 + 1];
    }
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
      encoding::Base64::decode(input.data(), input.size(), result.data());
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
    auto inputData = input.data();
    auto inputSize = input.size();
    const bool hasPad =
        (*(input.end()) == encoding::Base64::kBase64Pad) ? true : false;
    result.resize(
        encoding::Base64::calculateDecodedSize(inputData, inputSize, hasPad));
    encoding::Base64::decodeUrl(
        inputData, inputSize, result.data(), result.size(), hasPad);
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

} // namespace facebook::velox::functions

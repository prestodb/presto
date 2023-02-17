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

#include "velox/expression/VectorFunction.h"
#include "velox/functions/Macros.h"
#include "velox/functions/UDFOutputString.h"
#include "velox/functions/lib/string/StringImpl.h"

namespace facebook::velox::functions::sparksql {

template <typename T>
struct AsciiFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(int32_t& result, const arg_type<Varchar>& s) {
    result = s.empty() ? 0 : s.data()[0];
    return true;
  }
};

template <typename T>
struct ChrFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(out_type<Varchar>& result, int64_t ord) {
    if (ord < 0) {
      result.resize(0);
    } else {
      result.resize(1);
      *result.data() = ord;
    }
    return true;
  }
};

template <typename T>
struct Md5Function {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  template <typename TTo, typename TFrom>
  FOLLY_ALWAYS_INLINE bool call(TTo& result, const TFrom& input) {
    stringImpl::md5_radix(result, input, 16);
    return true;
  }
};

std::vector<std::shared_ptr<exec::FunctionSignature>> instrSignatures();

std::shared_ptr<exec::VectorFunction> makeInstr(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs);

std::vector<std::shared_ptr<exec::FunctionSignature>> lengthSignatures();

std::shared_ptr<exec::VectorFunction> makeLength(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs);

/// Expands each char of the digest data to two chars,
/// representing the hex value of each digest char, in order.
/// Note: digestSize must be one-half of outputSize.
void encodeDigestToBase16(uint8_t* output, int digestSize);

/// sha1 function
/// sha1(varbinary) -> string
/// Calculate SHA-1 digest and convert the result to a hex string.
/// Returns SHA-1 digest as a 40-character hex string.
template <typename T>
struct Sha1HexStringFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE
  void call(out_type<Varchar>& result, const arg_type<Varbinary>& input) {
    static const int kSha1Length = 20;
    result.resize(kSha1Length * 2);
    folly::ssl::OpenSSLHash::sha1(
        folly::MutableByteRange((uint8_t*)result.data(), kSha1Length),
        folly::ByteRange((const uint8_t*)input.data(), input.size()));
    encodeDigestToBase16((uint8_t*)result.data(), kSha1Length);
  }
};

/// contains function
/// contains(string, string) -> bool
/// Searches the second argument in the first one.
/// Returns true if it is found
template <typename T>
struct ContainsFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      out_type<bool>& result,
      const arg_type<Varchar>& str,
      const arg_type<Varchar>& pattern) {
    result = std::string_view(str).find(std::string_view(pattern)) !=
        std::string_view::npos;
    return true;
  }
};

/// startsWith function
/// startsWith(string, string) -> bool
/// Returns true if the first string starts with the second string
template <typename T>
struct StartsWithFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      out_type<bool>& result,
      const arg_type<Varchar>& str,
      const arg_type<Varchar>& pattern) {
    auto str1 = std::string_view(str);
    auto str2 = std::string_view(pattern);
    // TODO: Once C++20 supported we may want to replace this with
    // string_view::starts_with

    if (str2.length() > str1.length()) {
      result = false;
    } else {
      result = str1.substr(0, str2.length()) == str2;
      ;
    }
    return true;
  }
};

/// endsWith function
/// endsWith(string, string) -> bool
/// Returns true if the first string ends with the second string
template <typename T>
struct EndsWithFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      out_type<bool>& result,
      const arg_type<Varchar>& str,
      const arg_type<Varchar>& pattern) {
    auto str1 = std::string_view(str);
    auto str2 = std::string_view(pattern);
    // TODO Once C++20 supported we may want to replace this with
    // string_view::ends_with
    if (str2.length() > str1.length()) {
      result = false;
    } else {
      result =
          str1.substr(str1.length() - str2.length(), str2.length()) == str2;
    }
    return true;
  }
};

} // namespace facebook::velox::functions::sparksql

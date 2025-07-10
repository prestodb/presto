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

#include <memory>
#include <string>
#include <string_view>
#include <variant>

// Single file containing all the types and enums in nimble, as well as some
// templates for mapping between those types and C++ types.
//
// A note on types: For all of our enum classes, we assume the number of
// types will stay small, so that each one is representable by a single byte.
// Don't violate this!

namespace facebook::wave::nimble {

using VariantType = std::variant<
    int8_t,
    uint8_t,
    int16_t,
    uint16_t,
    int32_t,
    uint32_t,
    int64_t,
    uint64_t,
    float,
    double,
    bool,
    std::string>;

template <typename T>
class Variant {
 public:
  static void set(VariantType& target, T source) {
    target = source;
  }

  static T get(VariantType& source) {
    return std::get<T>(source);
  }
};

template <>
void Variant<std::string_view>::set(
    VariantType& target,
    std::string_view source);

template <>
std::string_view Variant<std::string_view>::get(VariantType& source);

enum class EncodingType {
  // Native encoding for numerics, simple packed chars with offsets for strings,
  // bitpacked for bools. All data types supported.
  Trivial = 0,
  // Run length encoded data. The runs lengths are bit packed, and the run
  // values are encoded like the trivial encoding. All data types supported.
  RLE = 1,
  // Data with the uniques encoded in a dictionary and the indices into that
  // dictionary. All data types except bools supported.
  Dictionary = 2,
  // Stores integer types packed into a fixed number of bits (namely, the
  // smallest required to represent the largest element). Currently only
  // works with non-negative values, we may add ZigZag encoding later.
  FixedBitWidth = 3,
  // Stores nullable data using a 'sentinel' value to represent nulls in a
  // single non-nullable encoding.
  Sentinel = 4,
  // Stores nullable data by wrapping one subencoding representing the non-nulls
  // with another subencoding marking which rows are null.
  Nullable = 5,
  // Stores indices to set (or unset) bits. Useful for storing sparse data, such
  // as when only a few rows in a encoding are non-null.
  SparseBool = 6,
  // Stores integer types via varint encoding. Currently only
  // works with non-negative values, we may add ZigZag encoding later.
  Varint = 7,
  // Stores integer types with a delta encoding. Currently only supports
  // positive deltas.
  Delta = 8,
  // Stores constant (i.e. only 1 unique value) data.
  Constant = 9,
  // Stores 'mainly constant' data, i.e. treats one particular value as special,
  // using a bool child vector to store whether each row is that special value,
  // and stores the non-special values as a separate encoding.
  MainlyConstant = 10,
};
std::string toString(EncodingType encodingType);
std::ostream& operator<<(std::ostream& out, EncodingType encodingType);

enum class DataType : uint8_t {
  Undefined = 0,
  Int8 = 1,
  Uint8 = 2,
  Int16 = 3,
  Uint16 = 4,
  Int32 = 5,
  Uint32 = 6,
  Int64 = 7,
  Uint64 = 8,
  Float = 9,
  Double = 10,
  Bool = 11,
  String = 12,
};

std::string toString(DataType dataType);
std::ostream& operator<<(std::ostream& out, DataType dataType);

// General string compression. Make sure values here match those in the footer
// specification
enum class CompressionType : uint8_t {
  Uncompressed = 0,
  // Zstd doesn't require us to externally store level or any other info.
  Zstd = 1,
  MetaInternal = 2,
};

std::string toString(CompressionType compressionType);
std::ostream& operator<<(std::ostream& out, CompressionType compressionType);

enum class ChecksumType : uint8_t { XXH3_64 = 0 };

std::string toString(ChecksumType type);

// A CompresionType and any type-specific configuration params.
struct CompressionParams {
  CompressionType type;

  // For zstd.
  int zstdLevel = 1;
};

// Parameters controlling the search for the optimal encoding on a data set.
struct OptimalSearchParams {
  // Whether recursive structures are allowed. E.g. a encoding may use another
  // encoding as a subencoding, and may use the encoding factory to find the
  // best subencoding. We must terminate the recursion at some depth. With the
  // default of 1 allowed recursion the top level encoding may use recursive
  // encodings, but its subencodings may not.
  int allowedRecursions = 1;

  // Whether to log debug info during the search (such as estimated sizes of
  // each encoding considered, etc.);
  bool logSearch = false;

  // Helps align log messages when log_search=true to help distinguish
  // subencoding log messages from higher-level ones.
  int logDepth = 0;

  // Entropy encodings, such as HuffmanEncoding, can be much more compact than
  // others, but are quite a bit slower. However, compared to applying a general
  // string compression on top of another encoding they are relatively fast.
  // In general the entropy encodings will also be smaller than GSC on top of
  // a non-entropy encoding.
  bool enableEntropyEncodings = true;

  // For some dimension encodings that will frequently be grouped by, we may
  // want to force dictionary enabled encodings so grouping by that can will be
  // fast.
  bool requireDictionaryEnabled = false;
};

template <typename T>
struct TypeTraits {};

template <>
struct TypeTraits<int8_t> {
  using physicalType = uint8_t;
  using sumType = int64_t;
  static constexpr DataType dataType = DataType::Int8;
};

template <>
struct TypeTraits<uint8_t> {
  using physicalType = uint8_t;
  using sumType = uint64_t;
  static constexpr DataType dataType = DataType::Uint8;
};

template <>
struct TypeTraits<int16_t> {
  using physicalType = uint16_t;
  using sumType = int64_t;
  static constexpr DataType dataType = DataType::Int16;
};

template <>
struct TypeTraits<uint16_t> {
  using physicalType = uint16_t;
  using sumType = uint64_t;
  static constexpr DataType dataType = DataType::Uint16;
};

template <>
struct TypeTraits<int32_t> {
  using physicalType = uint32_t;
  using sumType = int64_t;
  static constexpr DataType dataType = DataType::Int32;
};

template <>
struct TypeTraits<uint32_t> {
  using physicalType = uint32_t;
  using sumType = uint64_t;
  static constexpr DataType dataType = DataType::Uint32;
};

template <>
struct TypeTraits<int64_t> {
  using physicalType = uint64_t;
  using sumType = int64_t;
  static constexpr DataType dataType = DataType::Int64;
};

template <>
struct TypeTraits<uint64_t> {
  using physicalType = uint64_t;
  using sumType = uint64_t;
  static constexpr DataType dataType = DataType::Uint64;
};

template <>
struct TypeTraits<float> {
  using physicalType = uint32_t;
  using sumType = double;
  static constexpr DataType dataType = DataType::Float;
};

template <>
struct TypeTraits<double> {
  using physicalType = uint64_t;
  using sumType = double;
  static constexpr DataType dataType = DataType::Double;
};

template <>
struct TypeTraits<bool> {
  using physicalType = bool;
  static constexpr DataType dataType = DataType::Bool;
};

template <>
struct TypeTraits<std::string> {
  using physicalType = std::string;
  static constexpr DataType dataType = DataType::String;
};

template <>
struct TypeTraits<std::string_view> {
  using physicalType = std::string_view;
  static constexpr DataType dataType = DataType::String;
};

template <typename T>
constexpr bool isFourByteIntegralType() {
  return std::is_same_v<T, int32_t> || std::is_same_v<T, uint32_t>;
}

template <typename T>
constexpr bool isSignedIntegralType() {
  return std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t> ||
      std::is_same_v<T, int8_t> || std::is_same_v<T, int16_t>;
}

template <typename T>
constexpr bool isUnsignedIntegralType() {
  return std::is_same_v<T, uint32_t> || std::is_same_v<T, uint64_t> ||
      std::is_same_v<T, uint8_t> || std::is_same_v<T, uint16_t>;
}

template <typename T>
constexpr bool isIntegralType() {
  return isSignedIntegralType<T>() || isUnsignedIntegralType<T>();
}

template <typename T>
constexpr bool isFloatingPointType() {
  return std::is_same_v<T, float> || std::is_same_v<T, double>;
}

template <typename T>
constexpr bool isNumericType() {
  return isIntegralType<T>() || isFloatingPointType<T>();
}

template <typename T>
constexpr bool isStringType() {
  return std::is_same_v<T, std::string_view> || std::is_same_v<T, std::string>;
}

template <typename T>
constexpr bool isBoolType() {
  return std::is_same_v<T, bool>;
}

} // namespace facebook::wave::nimble

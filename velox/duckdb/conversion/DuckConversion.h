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

#include "velox/type/Type.h"

#include <duckdb.hpp> // @manual

namespace facebook::velox {
class Variant;
}

namespace facebook::velox::duckdb {

/// Converts Velox type to DuckDB type.
::duckdb::LogicalType fromVeloxType(const TypePtr& type);

/// Converts DuckDB type to Velox type.
TypePtr toVeloxType(
    ::duckdb::LogicalType type,
    bool fileColumnNamesReadAsLowerCase = false);

static ::duckdb::timestamp_t veloxTimestampToDuckDB(
    const Timestamp& timestamp) {
  auto micros = timestamp.getSeconds() * 1000000 + timestamp.getNanos() / 1000;
  return ::duckdb::Timestamp::FromEpochMicroSeconds(micros);
}

static Timestamp duckdbTimestampToVelox(
    const ::duckdb::timestamp_t& timestamp) {
  auto micros = ::duckdb::Timestamp::GetEpochMicroSeconds(timestamp);

  auto seconds = micros / 1'000'000;
  auto nanoSeconds = (micros % 1'000'000) * 1'000;

  // Make sure nanoseconds are >= 0 even if timestamp represents time before
  // epoch.
  if (nanoSeconds < 0) {
    seconds--;
    nanoSeconds += 1'000'000'000;
  }

  return Timestamp(seconds, nanoSeconds);
}

// Converts a duckDB Value (class that holds an arbitrary data type) into
// Velox Variant.
Variant duckValueToVariant(const ::duckdb::Value& val);

// Converts duckDB decimal Value into appropriate decimal Variant.
// The duckdb::Value::GetValue() call for decimal type returns a double value.
// To avoid this, this method uses the duckdb::Value::GetUnsafeValue<int>()
// method.
// @param val duckdb decimal value.
// @return decimal Variant.
Variant decimalVariant(const ::duckdb::Value& val);

// value conversion routines
template <class T>
struct DuckNumericConversion {
  typedef T DUCK_TYPE;
  typedef T VELOX_TYPE;

  static T toDuck(const T& input, ::duckdb::Vector& /* unused */) {
    return input;
  }
  static T toVelox(const T& input) {
    return input;
  }
};

struct DuckHugeintConversion {
  typedef ::duckdb::hugeint_t DUCK_TYPE;
  typedef double VELOX_TYPE;

  static ::duckdb::hugeint_t toDuck(
      const double& input,
      ::duckdb::Vector& /* unused */) {
    return ::duckdb::Hugeint::Convert<double>(input);
  }
  static double toVelox(const ::duckdb::hugeint_t& input) {
    return ::duckdb::Hugeint::Cast<double>(input);
  }
};

struct DuckStringConversion {
  typedef ::duckdb::string_t DUCK_TYPE;
  typedef StringView VELOX_TYPE;

  static ::duckdb::string_t toDuck(
      const StringView& input,
      ::duckdb::Vector& result) {
    return ::duckdb::StringVector::AddString(
        result, input.data(), input.size());
  }
  static StringView toVelox(const ::duckdb::string_t& input) {
    return StringView(input.GetDataUnsafe(), input.GetSize());
  }
};

struct DuckBlobConversion {
  typedef ::duckdb::string_t DUCK_TYPE;
  typedef StringView VELOX_TYPE;

  static ::duckdb::string_t toDuck(
      const StringView& input,
      ::duckdb::Vector& result) {
    return ::duckdb::StringVector::AddStringOrBlob(
        result, input.data(), input.size());
  }
  static StringView toVelox(const ::duckdb::string_t& input) {
    return StringView(input.GetDataUnsafe(), input.GetSize());
  }
};

struct DuckInt16DecimalConversion {
  typedef int16_t DUCK_TYPE;
  typedef int64_t VELOX_TYPE;

  static int16_t toDuck(const int64_t& input, ::duckdb::Vector& /* unused */) {
    return input;
  }

  static int64_t toVelox(const int16_t input) {
    return static_cast<int64_t>(input);
  }
};

struct DuckInt32DecimalConversion {
  typedef int32_t DUCK_TYPE;
  typedef int64_t VELOX_TYPE;

  static int32_t toDuck(const int64_t& input, ::duckdb::Vector& /* unused */) {
    return input;
  }

  static int64_t toVelox(const int32_t input) {
    return static_cast<int64_t>(input);
  }
};

struct DuckInt64DecimalConversion {
  typedef int64_t DUCK_TYPE;
  typedef int64_t VELOX_TYPE;

  static int64_t toDuck(const int64_t& input, ::duckdb::Vector& /* unused */) {
    return input;
  }

  static int64_t toVelox(const int64_t input) {
    return input;
  }
};

struct DuckLongDecimalConversion {
  typedef ::duckdb::hugeint_t DUCK_TYPE;
  typedef int128_t VELOX_TYPE;

  static ::duckdb::hugeint_t toDuck(
      const int128_t& input,
      ::duckdb::Vector& /* unused */) {
    ::duckdb::hugeint_t duckValue;
    duckValue.upper = (input >> 64);
    duckValue.lower = input;
    return duckValue;
  }

  static int128_t toVelox(const ::duckdb::hugeint_t input) {
    return HugeInt::build(input.upper, input.lower);
  }
};

struct DuckTimestampConversion {
  typedef ::duckdb::timestamp_t DUCK_TYPE;
  typedef Timestamp VELOX_TYPE;

  static ::duckdb::timestamp_t toDuck(
      const Timestamp& input,
      ::duckdb::Vector& /* unused */) {
    return veloxTimestampToDuckDB(input);
  }
  static Timestamp toVelox(const ::duckdb::timestamp_t& input) {
    return duckdbTimestampToVelox(input);
  }
};

struct DuckDateConversion {
  typedef ::duckdb::date_t DUCK_TYPE;
  typedef int32_t VELOX_TYPE;

  static ::duckdb::date_t toDuck(
      const int32_t& input,
      ::duckdb::Vector& /* unused */) {
    return ::duckdb::Date::EpochDaysToDate(input);
  }
  static int32_t toVelox(const ::duckdb::date_t& input) {
    return ::duckdb::Date::EpochDays(input);
  }
};

/// Returns CREATE TABLE <tableName>(<schema>) DuckDB SQL.
std::string makeCreateTableSql(
    const std::string& tableName,
    const RowType& rowType);

} // namespace facebook::velox::duckdb

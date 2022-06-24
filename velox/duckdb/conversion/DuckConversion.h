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

#include "velox/external/duckdb/duckdb.hpp"
#include "velox/type/Type.h"

namespace facebook::velox {
class variant;
}

namespace facebook::velox::duckdb {

::duckdb::LogicalType fromVeloxType(TypeKind kind);
bool duckdbTypeIsSupported(::duckdb::LogicalType type);
TypePtr toVeloxType(::duckdb::LogicalType type);

static ::duckdb::timestamp_t veloxTimestampToDuckDB(
    const Timestamp& timestamp) {
  auto micros = timestamp.getSeconds() * 1000000 + timestamp.getNanos() / 1000;
  return ::duckdb::Timestamp::FromEpochMicroSeconds(micros);
}

static Timestamp duckdbTimestampToVelox(
    const ::duckdb::timestamp_t& timestamp) {
  auto micros = ::duckdb::Timestamp::GetEpochMicroSeconds(timestamp);
  return Timestamp(micros / 1000000, (micros % 1000000) * 1000);
}

// Converts a duckDB Value (class that holds an arbitraty data type) into a
// VELOX's variant.
variant duckValueToVariant(const ::duckdb::Value& val);

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

struct DuckInt16DecimalConversion {
  typedef int16_t DUCK_TYPE;
  typedef ShortDecimal VELOX_TYPE;

  static int16_t toDuck(
      const ShortDecimal& input,
      ::duckdb::Vector& /* unused */) {
    return input.unscaledValue();
  }

  static ShortDecimal toVelox(const int16_t input) {
    return ShortDecimal(static_cast<int64_t>(input));
  }
};

struct DuckInt32DecimalConversion {
  typedef int32_t DUCK_TYPE;
  typedef ShortDecimal VELOX_TYPE;

  static int32_t toDuck(
      const ShortDecimal& input,
      ::duckdb::Vector& /* unused */) {
    return input.unscaledValue();
  }

  static ShortDecimal toVelox(const int32_t input) {
    return ShortDecimal(static_cast<int64_t>(input));
  }
};

struct DuckInt64DecimalConversion {
  typedef int64_t DUCK_TYPE;
  typedef ShortDecimal VELOX_TYPE;

  static int64_t toDuck(
      const ShortDecimal& input,
      ::duckdb::Vector& /* unused */) {
    return input.unscaledValue();
  }

  static ShortDecimal toVelox(const int64_t input) {
    return ShortDecimal(input);
  }
};

struct DuckLongDecimalConversion {
  typedef ::duckdb::hugeint_t DUCK_TYPE;
  typedef LongDecimal VELOX_TYPE;

  static ::duckdb::hugeint_t toDuck(
      const LongDecimal& input,
      ::duckdb::Vector& /* unused */) {
    ::duckdb::hugeint_t duckValue;
    duckValue.upper = (input.unscaledValue() >> 64);
    duckValue.lower = input.unscaledValue();
    return duckValue;
  }

  static LongDecimal toVelox(const ::duckdb::hugeint_t input) {
    return LongDecimal(buildInt128(input.upper, input.lower));
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
  typedef Date VELOX_TYPE;

  static ::duckdb::date_t toDuck(
      const Date& input,
      ::duckdb::Vector& /* unused */) {
    return ::duckdb::Date::EpochDaysToDate(input.days());
  }
  static Date toVelox(const ::duckdb::date_t& input) {
    return Date(::duckdb::Date::EpochDays(input));
  }
};

} // namespace facebook::velox::duckdb

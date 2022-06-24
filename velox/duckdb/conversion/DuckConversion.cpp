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
#include "velox/duckdb/conversion/DuckConversion.h"
#include "velox/type/Variant.h"

namespace facebook::velox::duckdb {
using ::duckdb::DataChunk;
using ::duckdb::Date;
using ::duckdb::LogicalType;
using ::duckdb::LogicalTypeId;
using ::duckdb::Value;
using ::duckdb::Vector;
using ::duckdb::VectorOperations;
using ::duckdb::VectorType;

using ::duckdb::data_ptr_t;
using ::duckdb::date_t;
using ::duckdb::dtime_t;
using ::duckdb::string_t;
using ::duckdb::timestamp_t;

//! Type mapping for velox -> DuckDB conversions
LogicalType fromVeloxType(TypeKind kind) {
  switch (kind) {
    case TypeKind::BOOLEAN:
      return LogicalType::BOOLEAN;
    case TypeKind::TINYINT:
      return LogicalType::TINYINT;
    case TypeKind::SMALLINT:
      return LogicalType::SMALLINT;
    case TypeKind::INTEGER:
      return LogicalType::INTEGER;
    case TypeKind::BIGINT:
      return LogicalType::BIGINT;
    case TypeKind::REAL:
      return LogicalType::FLOAT;
    case TypeKind::DOUBLE:
      return LogicalType::DOUBLE;
    case TypeKind::VARCHAR:
      return LogicalType::VARCHAR;
    case TypeKind::TIMESTAMP:
      return LogicalType::TIMESTAMP;
    default:
      throw std::runtime_error(
          "unsupported type for velox -> DuckDB conversion: " +
          mapTypeKindToName(kind));
  }
}

//! Whether or not a type is supported for velox <-> DuckDB conversion; note
//! that this is more restrictive than toVeloxType We only support types that
//! have a 1:1 mapping between DuckDB and velox here
bool duckdbTypeIsSupported(LogicalType type) {
  switch (type.id()) {
    case LogicalTypeId::BOOLEAN:
    case LogicalTypeId::TINYINT:
    case LogicalTypeId::SMALLINT:
    case LogicalTypeId::INTEGER:
    case LogicalTypeId::BIGINT:
    case LogicalTypeId::FLOAT:
    case LogicalTypeId::DOUBLE:
    case LogicalTypeId::VARCHAR:
    case LogicalTypeId::TIMESTAMP:
    case LogicalTypeId::DATE:
      return true;
    default:
      return false;
  }
}

//! Type mapping for DuckDB -> velox conversions, we support more types here
TypePtr toVeloxType(LogicalType type) {
  switch (type.id()) {
    case LogicalTypeId::BOOLEAN:
      return BOOLEAN();
    case LogicalTypeId::TINYINT:
      return TINYINT();
    case LogicalTypeId::SMALLINT:
      return SMALLINT();
    case LogicalTypeId::INTEGER:
      return INTEGER();
    case LogicalTypeId::BIGINT:
      return BIGINT();
    case LogicalTypeId::FLOAT:
      return REAL();
    case LogicalTypeId::DECIMAL:
      uint8_t width;
      uint8_t scale;
      type.GetDecimalProperties(width, scale);
      return DECIMAL(width, scale);
    case LogicalTypeId::HUGEINT:
    case LogicalTypeId::DOUBLE:
      return DOUBLE();
    case LogicalTypeId::VARCHAR:
      return VARCHAR();
    case LogicalTypeId::DATE:
      return DATE();
    case LogicalTypeId::TIMESTAMP:
      return TIMESTAMP();
    case LogicalTypeId::BLOB:
      return VARBINARY();
    default:
      throw std::runtime_error(
          "unsupported type for duckdb -> velox conversion: " +
          type.ToString());
  }
}

variant duckValueToVariant(const Value& val) {
  switch (val.type().id()) {
    case LogicalTypeId::SQLNULL:
      return variant(TypeKind::UNKNOWN);
    case LogicalTypeId::BOOLEAN:
      return variant(val.GetValue<bool>());
    case LogicalTypeId::TINYINT:
      return variant(val.GetValue<int8_t>());
    case LogicalTypeId::SMALLINT:
      return variant(val.GetValue<int16_t>());
    case LogicalTypeId::INTEGER:
      return variant(val.GetValue<int32_t>());
    case LogicalTypeId::BIGINT:
      return variant(val.GetValue<int64_t>());
    case LogicalTypeId::FLOAT:
      return variant(val.GetValue<float>());
    case LogicalTypeId::DOUBLE:
      return variant(val.GetValue<double>());
    case LogicalTypeId::DECIMAL:
      return variant(val.GetValue<double>());
    case LogicalTypeId::VARCHAR:
      return variant(val.GetValue<std::string>());
    case LogicalTypeId::BLOB:
      return variant::binary(val.GetValue<std::string>());
    default:
      throw std::runtime_error(
          "unsupported type for duckdb value -> velox  variant conversion: " +
          val.type().ToString());
  }
}

} // namespace facebook::velox::duckdb

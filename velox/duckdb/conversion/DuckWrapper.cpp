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
#include "velox/duckdb/conversion/DuckWrapper.h"
#include "velox/duckdb/conversion/DuckConversion.h"
#include "velox/external/duckdb/duckdb.hpp"
#include "velox/external/duckdb/tpch/include/tpch-extension.hpp"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::duckdb {
using ::duckdb::Connection;
using ::duckdb::DataChunk;
using ::duckdb::DuckDB;
using ::duckdb::Hugeint;
using ::duckdb::hugeint_t;
using ::duckdb::LogicalTypeId;
using ::duckdb::PhysicalType;
using ::duckdb::QueryResult;

DuckDBWrapper::DuckDBWrapper(core::ExecCtx* context, const char* path)
    : context_(context) {
  db_ = std::make_unique<DuckDB>(path);
  connection_ = std::make_unique<Connection>(*db_);
  db_->LoadExtension<::duckdb::TPCHExtension>();
}

DuckDBWrapper::~DuckDBWrapper() {}

std::unique_ptr<DuckResult> DuckDBWrapper::execute(const std::string& query) {
  auto duckResult = connection_->Query(query);
  return std::make_unique<DuckResult>(context_, move(duckResult));
}

void DuckDBWrapper::print(const std::string& query) {
  auto result = connection_->Query(query);
  result->Print();
}

DuckResult::DuckResult(
    core::ExecCtx* context,
    std::unique_ptr<QueryResult> queryResult)
    : context_(context), queryResult_(std::move(queryResult)) {
  auto columnCount = queryResult_->types.size();

  std::vector<std::string> names;
  std::vector<TypePtr> types;
  names.reserve(columnCount);
  types.reserve(columnCount);
  for (auto i = 0; i < columnCount; i++) {
    types.push_back(getType(i));
    names.push_back(getName(i));
  }
  type_ = std::make_shared<RowType>(move(names), move(types));
}

DuckResult::~DuckResult() {}

bool DuckResult::success() {
  return queryResult_->success;
}

std::string DuckResult::errorMessage() {
  return queryResult_->error;
}

RowVectorPtr DuckResult::getVector() {
  auto rowType = getType();
  std::vector<VectorPtr> outputColumns;
  outputColumns.reserve(columnCount());
  for (auto i = 0; i < columnCount(); i++) {
    outputColumns.push_back(getVector(i));
  }

  return std::make_shared<RowVector>(
      context_->pool(),
      rowType,
      BufferPtr(nullptr),
      currentChunk_->size(),
      outputColumns,
      folly::none);
}

TypePtr DuckResult::getType(size_t columnIdx) {
  assert(columnIdx < queryResult_->types.size());
  return toVeloxType(queryResult_->types[columnIdx]);
}

std::string DuckResult::getName(size_t columnIdx) {
  assert(columnIdx < queryResult_->names.size());
  return queryResult_->names[columnIdx];
}

template <class OP>
VectorPtr convert(
    ::duckdb::Vector& duckVector,
    const TypePtr& veloxType,
    size_t size,
    memory::MemoryPool* pool) {
  auto vectorType = duckVector.GetVectorType();
  switch (vectorType) {
    case ::duckdb::VectorType::FLAT_VECTOR: {
      auto& duckValidity = ::duckdb::FlatVector::Validity(duckVector);
      auto* duckData =
          ::duckdb::FlatVector::GetData<typename OP::DUCK_TYPE>(duckVector);

      // TODO Figure out how to perform a zero-copy conversion.
      auto result = BaseVector::create(veloxType, size, pool);
      auto flatResult = result->as<FlatVector<typename OP::VELOX_TYPE>>();

      // Some DuckDB vectors have different internal layout and cannot be
      // trivially copied.
      if (duckVector.GetType() == LogicalTypeId::HUGEINT ||
          duckVector.GetType() == LogicalTypeId::TIMESTAMP ||
          duckVector.GetType() == LogicalTypeId::DATE ||
          duckVector.GetType() == LogicalTypeId::VARCHAR) {
        for (auto i = 0; i < size; i++) {
          if (duckValidity.RowIsValid(i)) {
            flatResult->set(i, OP::toVelox(duckData[i]));
          }
        }
      } else {
        auto rawValues = flatResult->mutableRawValues();
        memcpy(rawValues, duckData, size * sizeof(typename OP::VELOX_TYPE));
      }

      if (!duckValidity.AllValid()) {
        auto rawNulls = flatResult->mutableRawNulls();
        memcpy(rawNulls, duckValidity.GetData(), bits::nbytes(size));
      }
      return result;
    }
    case ::duckdb::VectorType::DICTIONARY_VECTOR: {
      auto& child = ::duckdb::DictionaryVector::Child(duckVector);
      auto& selection = ::duckdb::DictionaryVector::SelVector(duckVector);

      // DuckDB vectors doesn't tell what their size is. We are going to use max
      // index + 1 instead as the vector is guaranteed to be at least that
      // large.
      vector_size_t maxIndex = 0;
      for (auto i = 0; i < size; i++) {
        maxIndex = std::max(maxIndex, (vector_size_t)selection.get_index(i));
      }
      auto base = convert<OP>(child, veloxType, maxIndex + 1, pool);

      auto indices = AlignedBuffer::allocate<vector_size_t>(size, pool);
      memcpy(
          indices->asMutable<vector_size_t>(),
          selection.data(),
          size * sizeof(vector_size_t));

      return BaseVector::wrapInDictionary(
          BufferPtr(nullptr), indices, size, base);
    }
    default:
      VELOX_UNSUPPORTED(
          "Unsupported DuckDB vector encoding: {}",
          ::duckdb::VectorTypeToString(vectorType));
  }
}

struct NumericCastToDouble {
  template <class T>
  static double operation(T input) {
    return double(input);
  }
};

template <>
double NumericCastToDouble::operation(hugeint_t input) {
  return Hugeint::Cast<double>(input);
}

template <class T>
VectorPtr convertDecimalToDouble(
    ::duckdb::Vector& duckVector,
    size_t size,
    const TypePtr& veloxType,
    memory::MemoryPool* pool,
    uint8_t scale) {
  auto* duckData = ::duckdb::FlatVector::GetData<T>(duckVector);
  auto& duckValidity = ::duckdb::FlatVector::Validity(duckVector);

  auto result = BaseVector::create(veloxType, size, pool);
  auto flatResult = result->as<FlatVector<double>>();
  double dividend = std::pow(10, scale);
  for (auto i = 0; i < size; i++) {
    if (duckValidity.RowIsValid(i)) {
      double converted =
          NumericCastToDouble::template operation<T>(duckData[i]) / dividend;
      flatResult->set(i, converted);
    } else {
      result->setNull(i, true);
    }
  }
  return result;
}

VectorPtr toVeloxVector(
    int32_t size,
    ::duckdb::Vector& duckVector,
    const TypePtr& veloxType,
    memory::MemoryPool* pool) {
  auto type = duckVector.GetType();
  switch (type.id()) {
    case LogicalTypeId::BOOLEAN:
      return convert<DuckNumericConversion<bool>>(
          duckVector, veloxType, size, pool);
    case LogicalTypeId::TINYINT:
      return convert<DuckNumericConversion<int8_t>>(
          duckVector, veloxType, size, pool);
    case LogicalTypeId::SMALLINT:
      return convert<DuckNumericConversion<int16_t>>(
          duckVector, veloxType, size, pool);
    case LogicalTypeId::INTEGER:
      return convert<DuckNumericConversion<int32_t>>(
          duckVector, veloxType, size, pool);
    case LogicalTypeId::BIGINT:
      return convert<DuckNumericConversion<int64_t>>(
          duckVector, veloxType, size, pool);
    case LogicalTypeId::HUGEINT:
      return convert<DuckHugeintConversion>(duckVector, veloxType, size, pool);
    case LogicalTypeId::FLOAT:
      return convert<DuckNumericConversion<float>>(
          duckVector, veloxType, size, pool);
    case LogicalTypeId::DOUBLE:
      return convert<DuckNumericConversion<double>>(
          duckVector, veloxType, size, pool);
    case LogicalTypeId::DECIMAL: {
      uint8_t width;
      uint8_t scale;
      type.GetDecimalProperties(width, scale);
      switch (type.InternalType()) {
        case PhysicalType::INT16:
          return convertDecimalToDouble<int16_t>(
              duckVector, size, veloxType, pool, scale);
        case PhysicalType::INT32:
          return convertDecimalToDouble<int32_t>(
              duckVector, size, veloxType, pool, scale);
        case PhysicalType::INT64:
          return convertDecimalToDouble<int64_t>(
              duckVector, size, veloxType, pool, scale);
        case PhysicalType::INT128:
          return convertDecimalToDouble<hugeint_t>(
              duckVector, size, veloxType, pool, scale);
        default:
          throw std::runtime_error(
              "unrecognized internal type for decimal (this shouldn't happen");
      }
    }
    case LogicalTypeId::VARCHAR:
      return convert<DuckStringConversion>(duckVector, veloxType, size, pool);
    case LogicalTypeId::DATE:
      return convert<DuckDateConversion>(duckVector, veloxType, size, pool);
    case LogicalTypeId::TIMESTAMP:
      return convert<DuckTimestampConversion>(
          duckVector, veloxType, size, pool);
    default:
      throw std::runtime_error(
          "Unsupported vector type for conversion: " + type.ToString());
  }
}

VectorPtr DuckResult::getVector(size_t columnIdx) {
  VELOX_CHECK_LT(columnIdx, columnCount());
  VELOX_CHECK(
      currentChunk_,
      "no chunk available: did you call next() and did it return true?");
  auto& duckVector = currentChunk_->data[columnIdx];
  auto resultType = getType(columnIdx);
  return toVeloxVector(
      currentChunk_->size(), duckVector, resultType, context_->pool());
}

bool DuckResult::next() {
  currentChunk_ = queryResult_->Fetch();
  if (!currentChunk_) {
    return false;
  }
  currentChunk_->Normalify();
  return currentChunk_->size() > 0;
}

} // namespace facebook::velox::duckdb

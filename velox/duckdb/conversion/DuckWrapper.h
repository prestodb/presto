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
#include "velox/core/QueryCtx.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"

namespace duckdb {
class DuckDB;
class Connection;
class DataChunk;
class QueryResult;
class Vector;
} // namespace duckdb

namespace facebook::velox::duckdb {

class DuckResult;

class DuckDBWrapper {
 public:
  //! Opens a DuckDB database object with an optional path to the physical
  //! database (default: in-memory only)
  explicit DuckDBWrapper(core::ExecCtx* context, const char* path = nullptr);
  ~DuckDBWrapper();

  //! Execute a SQL query in the loaded database, and return the result as a
  //! DuckResult
  std::unique_ptr<DuckResult> execute(const std::string& query);

  //! Execute a SQL query in the loaded database and print the result to stdout
  void print(const std::string& query);

 private:
  core::ExecCtx* context_;
  std::unique_ptr<::duckdb::DuckDB> db_;
  std::unique_ptr<::duckdb::Connection> connection_;
};

class DuckResult {
 public:
  DuckResult(
      core::ExecCtx* context,
      std::unique_ptr<::duckdb::QueryResult> query_result);
  ~DuckResult();

 public:
  //! Returns true if the query succeeded, or false if it failed
  bool success();

  //! Returns the error message in case the query failed
  std::string errorMessage();

  //! Returns the number of columns in the result
  size_t columnCount() {
    return type_ ? type_->size() : 0;
  }

  //! Get the type of the result as a row
  std::shared_ptr<const RowType> getType() {
    return type_;
  }

  //! Gets a vector of the query result; returns nullptr if the result has no
  //! data (i.e. when next() has returned false, or when next() has not been
  //! called yet)
  RowVectorPtr getVector();

  //! Fetches the next chunk from the result set; returns true if there is more
  //! data, or false if finished Next needs to be called before any data is
  //! fetched from the result
  bool next();

 private:
  core::ExecCtx* context_;
  std::shared_ptr<const RowType> type_;

  std::unique_ptr<::duckdb::QueryResult> queryResult_;
  std::unique_ptr<::duckdb::DataChunk> currentChunk_;

 private:
  //! Returns the type of the column at the specified column index; index should
  //! be in range [0, column_count)
  TypePtr getType(size_t columnIdx);

  //! Returns the name of the column at the specified column index; index should
  //! be in range [0, column_count)
  std::string getName(size_t columnIdx);

  //! Fetch a single vector of the result; index should
  //! be in range [0, column_count)
  VectorPtr getVector(size_t columnIdx);
};

VectorPtr toVeloxVector(
    int32_t size,
    ::duckdb::Vector& duckVector,
    const TypePtr& veloxType,
    memory::MemoryPool* pool);

} // namespace facebook::velox::duckdb

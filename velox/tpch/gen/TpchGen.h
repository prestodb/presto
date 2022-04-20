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

#include "velox/common/memory/Memory.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::tpch {

/// This file uses TPC-H DBGEN to generate data encoded using Velox Vectors.
///
/// The basic input for the API is the TPC-H table name (the Table enum), the
/// TPC-H scale factor, the maximum batch size, and the offset. The common usage
/// is to make successive calls to this API advancing the offset parameter,
/// until all records were read. Clients might also assign different slices of
/// the range "[0, getRowCount(Table, scaleFactor)[" to different threads in
/// order to generate datasets in parallel.
///
/// If not enough records are available given a particular scale factor and
/// offset, less than maxRows records might be returned.
///
/// Data is always returned in a RowVector.

enum class Table {
  TBL_PART,
  TBL_SUPPLIER,
  TBL_PARTSUP,
  TBL_CUSTOMER,
  TBL_ORDERS,
  TBL_LINEITEM,
  TBL_NATION,
  TBL_REGION,
};

/// Returns the row count for a particular TPC-H table given a scale factor, as
/// defined in the spec available at:
///
///  https://www.tpc.org/tpch/
constexpr size_t getRowCount(Table table, size_t scaleFactor);

/// Returns a row vector containing at most `maxRows` rows of the "orders"
/// table, starting at `offset`, and given the scale factor. The row vector
/// returned has the following schema:
///
///  o_orderkey: BIGINT
///  o_custkey: BIGINT
///  o_orderstatus: VARCHAR
///  o_totalprice: DOUBLE
///  o_orderdate: VARCHAR
///  o_orderpriority: VARCHAR
///  o_clerk: VARCHAR
///  o_shippriority: INTEGER
///  o_comment: VARCHAR
///
RowVectorPtr genTpchOrders(
    size_t maxRows = 10000,
    size_t offset = 0,
    size_t scaleFactor = 1,
    memory::MemoryPool* pool =
        &velox::memory::getProcessDefaultMemoryManager().getRoot());

/// Returns a row vector containing at most `maxRows` rows of the "nation"
/// table, starting at `offset`, and given the scale factor. The row vector
/// returned has the following schema:
///
///  n_nationkey: BIGINT
///  n_name: VARCHAR
///  n_regionkey: BIGINT
///  n_comment: VARCHAR
///
RowVectorPtr genTpchNation(
    size_t maxRows = 10000,
    size_t offset = 0,
    size_t scaleFactor = 1,
    memory::MemoryPool* pool =
        &velox::memory::getProcessDefaultMemoryManager().getRoot());

} // namespace facebook::velox::tpch

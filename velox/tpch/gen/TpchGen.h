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

enum class Table : uint8_t {
  TBL_PART,
  TBL_SUPPLIER,
  TBL_PARTSUPP,
  TBL_CUSTOMER,
  TBL_ORDERS,
  TBL_LINEITEM,
  TBL_NATION,
  TBL_REGION,
};

/// Returns table name as a string.
std::string_view toTableName(Table table);

/// Returns the row count for a particular TPC-H table given a scale factor, as
/// defined in the spec available at:
///
///  https://www.tpc.org/tpch/
size_t getRowCount(Table table, size_t scaleFactor);

/// Returns the schema (RowType) for a particular TPC-H table.
RowTypePtr getTableSchema(Table table);

/// Returns the type of a particular table:column pair. Throws if `columnName`
/// does not exist in `table`.
TypePtr resolveTpchColumn(Table table, const std::string& columnName);

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

/// NOTE: This function's parameters have different semantic from the function
/// above. Dbgen does not provide deterministic random access to lineitem
/// generated rows (like it does for all other tables), because the number of
/// lineitems in an order is chosen at random (between 1 and 7, 4 on average).
//
/// In order to make this function reproducible and deterministic, the
/// parameters (maxRows and offset) refer to orders, not lineitems, and thus the
/// number of linteitem returned rows will be on average 4 * maxOrderRows.
///
/// Returns a row vector containing on average `maxOrderRows * 4` (from
/// `maxOrderRows` to `maxOrderRows * 7`) rows of the "lineitem" table. The
/// offset is controlled based on the orders table, starting at `ordersOffset`,
/// and given the scale factor. The row vector returned has the following
/// schema:
///
///  l_orderkey: BIGINT
///  l_partkey: BIGINT
///  l_suppkey: BIGINT
///  l_linenumber: INTEGER
///  l_quantity: DOUBLE
///  l_extendedprice: DOUBLE
///  l_discount: DOUBLE
///  l_tax: DOUBLE
///  l_returnflag: VARCHAR
///  l_linestatus: VARCHAR
///  l_shipdate: VARCHAR
///  l_commitdate: VARCHAR
///  l_receiptdate: VARCHAR
///  l_shipinstruct: VARCHAR
///  l_shipmode: VARCHAR
///  l_comment: VARCHAR
///
RowVectorPtr genTpchLineItem(
    size_t maxOrdersRows = 10000,
    size_t ordersOffset = 0,
    size_t scaleFactor = 1,
    memory::MemoryPool* pool =
        &velox::memory::getProcessDefaultMemoryManager().getRoot());

/// Returns a row vector containing at most `maxRows` rows of the "part"
/// table, starting at `offset`, and given the scale factor. The row vector
/// returned has the following schema:
///
///  p_partkey: BIGINT
///  p_name: VARCHAR
///  p_mfgr: VARCHAR
///  p_brand: VARCHAR
///  p_type: VARCHAR
///  p_size: INTEGER
///  p_container: VARCHAR
///  p_retailprice: DOUBLE
///  p_comment: VARCHAR
///
RowVectorPtr genTpchPart(
    size_t maxRows = 10000,
    size_t offset = 0,
    size_t scaleFactor = 1,
    memory::MemoryPool* pool =
        &velox::memory::getProcessDefaultMemoryManager().getRoot());

/// Returns a row vector containing at most `maxRows` rows of the "supplier"
/// table, starting at `offset`, and given the scale factor. The row vector
/// returned has the following schema:
///
///  s_suppkey: BIGINT
///  s_name: VARCHAR
///  s_address: VARCHAR
///  s_nationkey: BIGINT
///  s_phone: VARCHAR
///  s_acctbal: DOUBLE
///  s_comment: VARCHAR
///
RowVectorPtr genTpchSupplier(
    size_t maxRows = 10000,
    size_t offset = 0,
    size_t scaleFactor = 1,
    memory::MemoryPool* pool =
        &velox::memory::getProcessDefaultMemoryManager().getRoot());

/// Returns a row vector containing at most `maxRows` rows of the "partsupp"
/// table, starting at `offset`, and given the scale factor. The row vector
/// returned has the following schema:
///
///  ps_partkey: BIGINT
///  ps_suppkey: BIGINT
///  ps_availqty: INTEGER
///  ps_supplycost: DOUBLE
///  ps_comment: VARCHAR
///
RowVectorPtr genTpchPartSupp(
    size_t maxRows = 10000,
    size_t offset = 0,
    size_t scaleFactor = 1,
    memory::MemoryPool* pool =
        &velox::memory::getProcessDefaultMemoryManager().getRoot());

/// Returns a row vector containing at most `maxRows` rows of the "customer"
/// table, starting at `offset`, and given the scale factor. The row vector
/// returned has the following schema:
///
///  c_custkey: BIGINT
///  c_name: VARCHAR
///  c_addressname: VARCHAR
///  c_nationkey: BIGINT
///  c_phone: VARCHAR
///  c_acctbal: DOUBLE
///  c_mktsegment: VARCHAR
///  c_comment: VARCHAR
///
RowVectorPtr genTpchCustomer(
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

/// Returns a row vector containing at most `maxRows` rows of the "region"
/// table, starting at `offset`, and given the scale factor. The row vector
/// returned has the following schema:
///
///  r_regionkey: BIGINT
///  r_name: VARCHAR
///  r_comment: VARCHAR
///
RowVectorPtr genTpchRegion(
    size_t maxRows = 10000,
    size_t offset = 0,
    size_t scaleFactor = 1,
    memory::MemoryPool* pool =
        &velox::memory::getProcessDefaultMemoryManager().getRoot());

} // namespace facebook::velox::tpch

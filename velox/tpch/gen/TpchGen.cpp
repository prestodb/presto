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

#include "velox/tpch/gen/TpchGen.h"
#include "velox/external/duckdb/tpch/dbgen/include/dbgen/dbgen_gunk.hpp"
#include "velox/external/duckdb/tpch/dbgen/include/dbgen/dss.h"
#include "velox/external/duckdb/tpch/dbgen/include/dbgen/dsstypes.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::tpch {

namespace {

// The cardinality of the LINEITEM table is not a strict multiple of SF since
// the number of lineitems in an order is chosen at random with an average of
// four. This function contains the row count for all authorized scale factors
// (as described by the TPC-H spec), and approximates the remaining.
constexpr size_t getLineItemRowCount(size_t scaleFactor) {
  switch (scaleFactor) {
    case 1:
      return 6'001'215;
    case 10:
      return 59'986'052;
    case 30:
      return 179'998'372;
    case 100:
      return 600'037'902;
    case 300:
      return 1'799'989'091;
    case 1'000:
      return 5'999'989'709;
    case 3'000:
      return 18'000'048'306;
    case 10'000:
      return 59'999'994'267;
    case 30'000:
      return 179'999'978'268;
    case 100'000:
      return 599'999'969'200;
    default:
      break;
  }
  return 6'000'000 * scaleFactor;
}

} // namespace

constexpr size_t getRowCount(Table table, size_t scaleFactor) {
  switch (table) {
    case Table::TBL_PART:
      return 200'000 * scaleFactor;
    case Table::TBL_SUPPLIER:
      return 10'000 * scaleFactor;
    case Table::TBL_PARTSUP:
      return 800'000 * scaleFactor;
    case Table::TBL_CUSTOMER:
      return 150'000 * scaleFactor;
    case Table::TBL_ORDERS:
      return 1'500'000 * scaleFactor;
    case Table::TBL_NATION:
      return 25;
    case Table::TBL_REGION:
      return 5;
    case Table::TBL_LINEITEM:
      return getLineItemRowCount(scaleFactor);
  }
  return 0; // make gcc happy.
}

RowVectorPtr genTpchNation(
    size_t maxRows,
    size_t offset,
    size_t scaleFactor,
    memory::MemoryPool* pool) {
  size_t rowCount = getRowCount(Table::TBL_NATION, scaleFactor);
  size_t vectorSize = std::min(rowCount - offset, maxRows);

  // Create schema and allocate vectors.
  static TypePtr nationRowType =
      ROW({"n_nationkey", "n_name", "n_regionkey", "n_comment"},
          {BIGINT(), VARCHAR(), BIGINT(), VARCHAR()});
  std::vector<VectorPtr> children = {
      BaseVector::create(BIGINT(), vectorSize, pool),
      BaseVector::create(VARCHAR(), vectorSize, pool),
      BaseVector::create(BIGINT(), vectorSize, pool),
      BaseVector::create(VARCHAR(), vectorSize, pool),
  };

  auto nationKeyVector = children[0]->asFlatVector<int64_t>();
  auto nameVector = children[1]->asFlatVector<StringView>();
  auto regionKeyVector = children[2]->asFlatVector<int64_t>();
  auto commentVector = children[3]->asFlatVector<StringView>();

  // load_dists()/cleanup_dists() need to be called to ensure the global
  // variables required by dbgen are populated.
  load_dists();
  code_t code;

  // Dbgen generates the dataset one row at a time, so we need to transpose it
  // into a columnar format.
  for (size_t i = 0; i < vectorSize; ++i) {
    row_start(NATION);
    mk_nation(i + offset + 1, &code);
    nationKeyVector->set(i, code.code);
    nameVector->set(i, StringView(code.text, strlen(code.text)));
    regionKeyVector->set(i, code.join);
    commentVector->set(i, StringView(code.comment, code.clen));
    row_stop_h(NATION);
  }
  cleanup_dists();

  return std::make_shared<RowVector>(
      pool, nationRowType, BufferPtr(nullptr), vectorSize, std::move(children));
}

} // namespace facebook::velox::tpch

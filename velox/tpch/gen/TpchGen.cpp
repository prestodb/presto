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
#include "velox/tpch/gen/DBGenIterator.h"
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

size_t getVectorSize(size_t rowCount, size_t maxRows, size_t offset) {
  if (offset >= rowCount) {
    return 0;
  }
  return std::min(rowCount - offset, maxRows);
}

std::vector<VectorPtr> allocateVectors(
    const RowTypePtr& type,
    size_t vectorSize,
    memory::MemoryPool* pool) {
  std::vector<VectorPtr> vectors;
  vectors.reserve(type->size());

  for (const auto& childType : type->children()) {
    vectors.emplace_back(BaseVector::create(childType, vectorSize, pool));
  }
  return vectors;
}

double decimalToDouble(int64_t value) {
  return (double)value * 0.01;
}

} // namespace

std::string_view toTableName(Table table) {
  switch (table) {
    case Table::TBL_PART:
      return "part";
    case Table::TBL_SUPPLIER:
      return "supplier";
    case Table::TBL_PARTSUPP:
      return "partsupp";
    case Table::TBL_CUSTOMER:
      return "customer";
    case Table::TBL_ORDERS:
      return "orders";
    case Table::TBL_LINEITEM:
      return "lineitem";
    case Table::TBL_NATION:
      return "nation";
    case Table::TBL_REGION:
      return "region";
  }
  return ""; // make gcc happy.
}

Table fromTableName(std::string_view tableName) {
  static std::unordered_map<std::string_view, Table> map{
      {"part", Table::TBL_PART},
      {"supplier", Table::TBL_SUPPLIER},
      {"partsupp", Table::TBL_PARTSUPP},
      {"customer", Table::TBL_CUSTOMER},
      {"orders", Table::TBL_ORDERS},
      {"lineitem", Table::TBL_LINEITEM},
      {"nation", Table::TBL_NATION},
      {"region", Table::TBL_REGION},
  };

  auto it = map.find(tableName);
  if (it != map.end()) {
    return it->second;
  }
  throw std::invalid_argument(
      fmt::format("Invalid TPC-H table name: '{}'", tableName));
}

size_t getRowCount(Table table, size_t scaleFactor) {
  switch (table) {
    case Table::TBL_PART:
      return 200'000 * scaleFactor;
    case Table::TBL_SUPPLIER:
      return 10'000 * scaleFactor;
    case Table::TBL_PARTSUPP:
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

RowTypePtr getTableSchema(Table table) {
  switch (table) {
    case Table::TBL_PART: {
      static RowTypePtr type = ROW(
          {
              "p_partkey",
              "p_name",
              "p_mfgr",
              "p_brand",
              "p_type",
              "p_size",
              "p_container",
              "p_retailprice",
              "p_comment",
          },
          {
              BIGINT(),
              VARCHAR(),
              VARCHAR(),
              VARCHAR(),
              VARCHAR(),
              INTEGER(),
              VARCHAR(),
              DOUBLE(),
              VARCHAR(),
          });
      return type;
    }

    case Table::TBL_SUPPLIER: {
      static RowTypePtr type = ROW(
          {
              "s_suppkey",
              "s_name",
              "s_address",
              "s_nationkey",
              "s_phone",
              "s_acctbal",
              "s_comment",
          },
          {
              BIGINT(),
              VARCHAR(),
              VARCHAR(),
              BIGINT(),
              VARCHAR(),
              DOUBLE(),
              VARCHAR(),
          });
      return type;
    }

    case Table::TBL_PARTSUPP: {
      static RowTypePtr type = ROW(
          {
              "ps_partkey",
              "ps_suppkey",
              "ps_availqty",
              "ps_supplycost",
              "ps_comment",
          },
          {
              BIGINT(),
              BIGINT(),
              INTEGER(),
              DOUBLE(),
              VARCHAR(),
          });
      return type;
    }

    case Table::TBL_CUSTOMER: {
      static RowTypePtr type = ROW(
          {
              "c_custkey",
              "c_name",
              "c_address",
              "c_nationkey",
              "c_phone",
              "c_acctbal",
              "c_mktsegment",
              "c_comment",
          },
          {
              BIGINT(),
              VARCHAR(),
              VARCHAR(),
              BIGINT(),
              VARCHAR(),
              DOUBLE(),
              VARCHAR(),
              VARCHAR(),
          });
      return type;
    }

    case Table::TBL_ORDERS: {
      static RowTypePtr type = ROW(
          {
              "o_orderkey",
              "o_custkey",
              "o_orderstatus",
              "o_totalprice",
              "o_orderdate",
              "o_orderpriority",
              "o_clerk",
              "o_shippriority",
              "o_comment",
          },
          {
              BIGINT(),
              BIGINT(),
              VARCHAR(),
              DOUBLE(),
              VARCHAR(),
              VARCHAR(),
              VARCHAR(),
              INTEGER(),
              VARCHAR(),
          });
      return type;
    }

    case Table::TBL_LINEITEM: {
      static RowTypePtr type = ROW(
          {
              "l_orderkey",
              "l_partkey",
              "l_suppkey",
              "l_linenumber",
              "l_quantity",
              "l_extendedprice",
              "l_discount",
              "l_tax",
              "l_returnflag",
              "l_linestatus",
              "l_shipdate",
              "l_commitdate",
              "l_receiptdate",
              "l_shipinstruct",
              "l_shipmode",
              "l_comment",
          },
          {
              BIGINT(),
              BIGINT(),
              BIGINT(),
              INTEGER(),
              DOUBLE(),
              DOUBLE(),
              DOUBLE(),
              DOUBLE(),
              VARCHAR(),
              VARCHAR(),
              VARCHAR(),
              VARCHAR(),
              VARCHAR(),
              VARCHAR(),
              VARCHAR(),
              VARCHAR(),
          });
      return type;
    }

    case Table::TBL_NATION: {
      static RowTypePtr type = ROW(
          {
              "n_nationkey",
              "n_name",
              "n_regionkey",
              "n_comment",
          },
          {
              BIGINT(),
              VARCHAR(),
              BIGINT(),
              VARCHAR(),
          });
      return type;
    }
    case Table::TBL_REGION: {
      static RowTypePtr type = ROW(
          {
              "r_regionkey",
              "r_name",
              "r_comment",
          },
          {
              BIGINT(),
              VARCHAR(),
              VARCHAR(),
          });
      return type;
    }
  }
  return nullptr; // make gcc happy.
}

TypePtr resolveTpchColumn(Table table, const std::string& columnName) {
  return getTableSchema(table)->findChild(columnName);
}

RowVectorPtr genTpchOrders(
    size_t maxRows,
    size_t offset,
    size_t scaleFactor,
    memory::MemoryPool* pool) {
  // Create schema and allocate vectors.
  auto ordersRowType = getTableSchema(Table::TBL_ORDERS);
  size_t vectorSize = getVectorSize(
      getRowCount(Table::TBL_ORDERS, scaleFactor), maxRows, offset);
  auto children = allocateVectors(ordersRowType, vectorSize, pool);

  auto orderKeyVector = children[0]->asFlatVector<int64_t>();
  auto custKeyVector = children[1]->asFlatVector<int64_t>();
  auto orderStatusVector = children[2]->asFlatVector<StringView>();
  auto totalPriceVector = children[3]->asFlatVector<double>();
  auto orderDateVector = children[4]->asFlatVector<StringView>();
  auto orderPriorityVector = children[5]->asFlatVector<StringView>();
  auto clerkVector = children[6]->asFlatVector<StringView>();
  auto shipPriorityVector = children[7]->asFlatVector<int32_t>();
  auto commentVector = children[8]->asFlatVector<StringView>();

  auto dbgenIt = DBGenIterator::create(scaleFactor);
  dbgenIt.initOrder(offset);
  order_t order;

  // Dbgen generates the dataset one row at a time, so we need to transpose it
  // into a columnar format.
  for (size_t i = 0; i < vectorSize; ++i) {
    dbgenIt.genOrder(i + offset + 1, order);

    orderKeyVector->set(i, order.okey);
    custKeyVector->set(i, order.custkey);
    orderStatusVector->set(i, StringView(&order.orderstatus, 1));
    totalPriceVector->set(i, decimalToDouble(order.totalprice));
    orderDateVector->set(i, StringView(order.odate, strlen(order.odate)));
    orderPriorityVector->set(
        i, StringView(order.opriority, strlen(order.opriority)));
    clerkVector->set(i, StringView(order.clerk, strlen(order.clerk)));
    shipPriorityVector->set(i, order.spriority);
    commentVector->set(i, StringView(order.comment, order.clen));
  }
  return std::make_shared<RowVector>(
      pool, ordersRowType, BufferPtr(nullptr), vectorSize, std::move(children));
}

RowVectorPtr genTpchLineItem(
    size_t maxOrderRows,
    size_t ordersOffset,
    size_t scaleFactor,
    memory::MemoryPool* pool) {
  // We control the buffer size based on the orders table, then allocate the
  // underlying buffer using the worst case (orderVectorSize * 7).
  size_t orderVectorSize = getVectorSize(
      getRowCount(Table::TBL_ORDERS, scaleFactor), maxOrderRows, ordersOffset);
  size_t lineItemUpperBound = orderVectorSize * 7;

  // Create schema and allocate vectors.
  auto lineItemRowType = getTableSchema(Table::TBL_LINEITEM);
  auto children = allocateVectors(lineItemRowType, lineItemUpperBound, pool);

  auto orderKeyVector = children[0]->asFlatVector<int64_t>();
  auto partKeyVector = children[1]->asFlatVector<int64_t>();
  auto suppKeyVector = children[2]->asFlatVector<int64_t>();
  auto lineNumberVector = children[3]->asFlatVector<int32_t>();

  auto quantityVector = children[4]->asFlatVector<double>();
  auto extendedPriceVector = children[5]->asFlatVector<double>();
  auto discountVector = children[6]->asFlatVector<double>();
  auto taxVector = children[7]->asFlatVector<double>();

  auto returnFlagVector = children[8]->asFlatVector<StringView>();
  auto lineStatusVector = children[9]->asFlatVector<StringView>();
  auto shipDateVector = children[10]->asFlatVector<StringView>();
  auto commitDateVector = children[11]->asFlatVector<StringView>();
  auto receiptDateVector = children[12]->asFlatVector<StringView>();
  auto shipInstructVector = children[13]->asFlatVector<StringView>();
  auto shipModeVector = children[14]->asFlatVector<StringView>();
  auto commentVector = children[15]->asFlatVector<StringView>();

  auto dbgenIt = DBGenIterator::create(scaleFactor);
  dbgenIt.initOrder(ordersOffset);
  order_t order;

  // Dbgen can't generate lineItem one row at a time; instead, it generates
  // orders with a random number of lineitems associated. So we treat offset
  // and maxRows as being in terms of orders (to make it deterministic), and
  // return a RowVector with a variable number of rows.
  size_t lineItemCount = 0;

  for (size_t i = 0; i < orderVectorSize; ++i) {
    dbgenIt.genOrder(i + ordersOffset + 1, order);

    for (size_t l = 0; l < order.lines; ++l) {
      const auto& line = order.l[l];
      orderKeyVector->set(lineItemCount + l, line.okey);
      partKeyVector->set(lineItemCount + l, line.partkey);
      suppKeyVector->set(lineItemCount + l, line.suppkey);

      lineNumberVector->set(lineItemCount + l, line.lcnt);

      quantityVector->set(lineItemCount + l, decimalToDouble(line.quantity));
      extendedPriceVector->set(lineItemCount + l, decimalToDouble(line.eprice));
      discountVector->set(lineItemCount + l, decimalToDouble(line.discount));
      taxVector->set(lineItemCount + l, decimalToDouble(line.tax));

      returnFlagVector->set(lineItemCount + l, StringView(line.rflag, 1));
      lineStatusVector->set(lineItemCount + l, StringView(line.lstatus, 1));

      shipDateVector->set(
          lineItemCount + l, StringView(line.sdate, strlen(line.sdate)));
      commitDateVector->set(
          lineItemCount + l, StringView(line.cdate, strlen(line.cdate)));
      receiptDateVector->set(
          lineItemCount + l, StringView(line.rdate, strlen(line.rdate)));

      shipInstructVector->set(
          lineItemCount + l,
          StringView(line.shipinstruct, strlen(line.shipinstruct)));
      shipModeVector->set(
          lineItemCount + l, StringView(line.shipmode, strlen(line.shipmode)));
      commentVector->set(
          lineItemCount + l, StringView(line.comment, strlen(line.comment)));
    }
    lineItemCount += order.lines;
  }

  // Resize to shrink the buffers - since we allocated based on the upper bound.
  for (auto& child : children) {
    child->resize(lineItemCount);
  }
  return std::make_shared<RowVector>(
      pool,
      lineItemRowType,
      BufferPtr(nullptr),
      lineItemCount,
      std::move(children));
}

RowVectorPtr genTpchPart(
    size_t maxRows,
    size_t offset,
    size_t scaleFactor,
    memory::MemoryPool* pool) {
  // Create schema and allocate vectors.
  auto partRowType = getTableSchema(Table::TBL_PART);
  size_t vectorSize =
      getVectorSize(getRowCount(Table::TBL_PART, scaleFactor), maxRows, offset);
  auto children = allocateVectors(partRowType, vectorSize, pool);

  auto partKeyVector = children[0]->asFlatVector<int64_t>();
  auto nameVector = children[1]->asFlatVector<StringView>();
  auto mfgrVector = children[2]->asFlatVector<StringView>();
  auto brandVector = children[3]->asFlatVector<StringView>();
  auto typeVector = children[4]->asFlatVector<StringView>();
  auto sizeVector = children[5]->asFlatVector<int32_t>();
  auto containerVector = children[6]->asFlatVector<StringView>();
  auto retailPriceVector = children[7]->asFlatVector<double>();
  auto commentVector = children[8]->asFlatVector<StringView>();

  auto dbgenIt = DBGenIterator::create(scaleFactor);
  dbgenIt.initPart(offset);
  part_t part;

  // Dbgen generates the dataset one row at a time, so we need to transpose it
  // into a columnar format.
  for (size_t i = 0; i < vectorSize; ++i) {
    dbgenIt.genPart(i + offset + 1, part);

    partKeyVector->set(i, part.partkey);
    nameVector->set(i, StringView(part.name, strlen(part.name)));
    mfgrVector->set(i, StringView(part.mfgr, strlen(part.mfgr)));
    brandVector->set(i, StringView(part.brand, strlen(part.brand)));
    typeVector->set(i, StringView(part.type, part.tlen));
    sizeVector->set(i, part.size);
    containerVector->set(i, StringView(part.container, strlen(part.container)));
    retailPriceVector->set(i, decimalToDouble(part.retailprice));
    commentVector->set(i, StringView(part.comment, part.clen));
  }
  return std::make_shared<RowVector>(
      pool, partRowType, BufferPtr(nullptr), vectorSize, std::move(children));
}

RowVectorPtr genTpchSupplier(
    size_t maxRows,
    size_t offset,
    size_t scaleFactor,
    memory::MemoryPool* pool) {
  // Create schema and allocate vectors.
  auto supplierRowType = getTableSchema(Table::TBL_SUPPLIER);
  size_t vectorSize = getVectorSize(
      getRowCount(Table::TBL_SUPPLIER, scaleFactor), maxRows, offset);
  auto children = allocateVectors(supplierRowType, vectorSize, pool);

  auto suppKeyVector = children[0]->asFlatVector<int64_t>();
  auto nameVector = children[1]->asFlatVector<StringView>();
  auto addressVector = children[2]->asFlatVector<StringView>();
  auto nationKeyVector = children[3]->asFlatVector<int64_t>();
  auto phoneVector = children[4]->asFlatVector<StringView>();
  auto acctbalVector = children[5]->asFlatVector<double>();
  auto commentVector = children[6]->asFlatVector<StringView>();

  auto dbgenIt = DBGenIterator::create(scaleFactor);
  dbgenIt.initSupplier(offset);
  supplier_t supp;

  // Dbgen generates the dataset one row at a time, so we need to transpose it
  // into a columnar format.
  for (size_t i = 0; i < vectorSize; ++i) {
    dbgenIt.genSupplier(i + offset + 1, supp);

    suppKeyVector->set(i, supp.suppkey);
    nameVector->set(i, StringView(supp.name, strlen(supp.name)));
    addressVector->set(i, StringView(supp.address, supp.alen));
    nationKeyVector->set(i, supp.nation_code);
    phoneVector->set(i, StringView(supp.phone, strlen(supp.phone)));
    acctbalVector->set(i, decimalToDouble(supp.acctbal));
    commentVector->set(i, StringView(supp.comment, supp.clen));
  }
  return std::make_shared<RowVector>(
      pool,
      supplierRowType,
      BufferPtr(nullptr),
      vectorSize,
      std::move(children));
}

RowVectorPtr genTpchPartSupp(
    size_t maxRows,
    size_t offset,
    size_t scaleFactor,
    memory::MemoryPool* pool) {
  // Create schema and allocate vectors.
  auto partSuppRowType = getTableSchema(Table::TBL_PARTSUPP);
  size_t vectorSize = getVectorSize(
      getRowCount(Table::TBL_PARTSUPP, scaleFactor), maxRows, offset);
  auto children = allocateVectors(partSuppRowType, vectorSize, pool);

  auto partKeyVector = children[0]->asFlatVector<int64_t>();
  auto suppKeyVector = children[1]->asFlatVector<int64_t>();
  auto availQtyVector = children[2]->asFlatVector<int32_t>();
  auto supplyCostVector = children[3]->asFlatVector<double>();
  auto commentVector = children[4]->asFlatVector<StringView>();

  auto dbgenIt = DBGenIterator::create(scaleFactor);
  part_t part;

  // The iteration logic is a bit more complicated as partsupp records are
  // generated using mk_part(), which returns a vector of 4 (SUPP_PER_PART)
  // partsupp record at a time. So we need to align the user's requested window
  // (maxRows, offset), with the 4-at-a-time record window provided by DBGEN.
  size_t partIdx = offset / SUPP_PER_PART;
  size_t partSuppIdx = offset % SUPP_PER_PART;
  size_t partSuppCount = 0;

  dbgenIt.initPart(partIdx);

  do {
    dbgenIt.genPart(partIdx + 1, part);

    while ((partSuppIdx < SUPP_PER_PART) && (partSuppCount < vectorSize)) {
      const auto& partSupp = part.s[partSuppIdx];

      partKeyVector->set(partSuppCount, partSupp.partkey);
      suppKeyVector->set(partSuppCount, partSupp.suppkey);
      availQtyVector->set(partSuppCount, partSupp.qty);
      supplyCostVector->set(partSuppCount, decimalToDouble(partSupp.scost));
      commentVector->set(
          partSuppCount, StringView(partSupp.comment, partSupp.clen));

      ++partSuppIdx;
      ++partSuppCount;
    }
    partSuppIdx = 0;
    ++partIdx;

  } while (partSuppCount < vectorSize);

  VELOX_CHECK_EQ(partSuppCount, vectorSize);
  return std::make_shared<RowVector>(
      pool,
      partSuppRowType,
      BufferPtr(nullptr),
      vectorSize,
      std::move(children));
}

RowVectorPtr genTpchCustomer(
    size_t maxRows,
    size_t offset,
    size_t scaleFactor,
    memory::MemoryPool* pool) {
  // Create schema and allocate vectors.
  auto customerRowType = getTableSchema(Table::TBL_CUSTOMER);
  size_t vectorSize = getVectorSize(
      getRowCount(Table::TBL_CUSTOMER, scaleFactor), maxRows, offset);
  auto children = allocateVectors(customerRowType, vectorSize, pool);

  auto custKeyVector = children[0]->asFlatVector<int64_t>();
  auto nameVector = children[1]->asFlatVector<StringView>();
  auto addressVector = children[2]->asFlatVector<StringView>();
  auto nationKeyVector = children[3]->asFlatVector<int64_t>();
  auto phoneVector = children[4]->asFlatVector<StringView>();
  auto acctBalVector = children[5]->asFlatVector<double>();
  auto mktSegmentVector = children[6]->asFlatVector<StringView>();
  auto commentVector = children[7]->asFlatVector<StringView>();

  auto dbgenIt = DBGenIterator::create(scaleFactor);
  dbgenIt.initCustomer(offset);
  customer_t cust;

  // Dbgen generates the dataset one row at a time, so we need to transpose it
  // into a columnar format.
  for (size_t i = 0; i < vectorSize; ++i) {
    dbgenIt.genCustomer(i + offset + 1, cust);

    custKeyVector->set(i, cust.custkey);
    nameVector->set(i, StringView(cust.name, strlen(cust.name)));
    addressVector->set(i, StringView(cust.address, cust.alen));
    nationKeyVector->set(i, cust.nation_code);
    phoneVector->set(i, StringView(cust.phone, strlen(cust.phone)));
    acctBalVector->set(i, decimalToDouble(cust.acctbal));
    mktSegmentVector->set(
        i, StringView(cust.mktsegment, strlen(cust.mktsegment)));
    commentVector->set(i, StringView(cust.comment, cust.clen));
  }
  return std::make_shared<RowVector>(
      pool,
      customerRowType,
      BufferPtr(nullptr),
      vectorSize,
      std::move(children));
}

RowVectorPtr genTpchNation(
    size_t maxRows,
    size_t offset,
    size_t scaleFactor,
    memory::MemoryPool* pool) {
  // Create schema and allocate vectors.
  auto nationRowType = getTableSchema(Table::TBL_NATION);
  size_t vectorSize = getVectorSize(
      getRowCount(Table::TBL_NATION, scaleFactor), maxRows, offset);
  auto children = allocateVectors(nationRowType, vectorSize, pool);

  auto nationKeyVector = children[0]->asFlatVector<int64_t>();
  auto nameVector = children[1]->asFlatVector<StringView>();
  auto regionKeyVector = children[2]->asFlatVector<int64_t>();
  auto commentVector = children[3]->asFlatVector<StringView>();

  auto dbgenIt = DBGenIterator::create(scaleFactor);
  dbgenIt.initNation(offset);
  code_t code;

  // Dbgen generates the dataset one row at a time, so we need to transpose it
  // into a columnar format.
  for (size_t i = 0; i < vectorSize; ++i) {
    dbgenIt.genNation(i + offset + 1, code);

    nationKeyVector->set(i, code.code);
    nameVector->set(i, StringView(code.text, strlen(code.text)));
    regionKeyVector->set(i, code.join);
    commentVector->set(i, StringView(code.comment, code.clen));
  }
  return std::make_shared<RowVector>(
      pool, nationRowType, BufferPtr(nullptr), vectorSize, std::move(children));
}

RowVectorPtr genTpchRegion(
    size_t maxRows,
    size_t offset,
    size_t scaleFactor,
    memory::MemoryPool* pool) {
  // Create schema and allocate vectors.
  auto regionRowType = getTableSchema(Table::TBL_REGION);
  size_t vectorSize = getVectorSize(
      getRowCount(Table::TBL_REGION, scaleFactor), maxRows, offset);
  auto children = allocateVectors(regionRowType, vectorSize, pool);

  auto regionKeyVector = children[0]->asFlatVector<int64_t>();
  auto nameVector = children[1]->asFlatVector<StringView>();
  auto commentVector = children[2]->asFlatVector<StringView>();

  auto dbgenIt = DBGenIterator::create(scaleFactor);
  dbgenIt.initRegion(offset);
  code_t code;

  // Dbgen generates the dataset one row at a time, so we need to transpose it
  // into a columnar format.
  for (size_t i = 0; i < vectorSize; ++i) {
    dbgenIt.genRegion(i + offset + 1, code);

    regionKeyVector->set(i, code.code);
    nameVector->set(i, StringView(code.text, strlen(code.text)));
    commentVector->set(i, StringView(code.comment, code.clen));
  }
  return std::make_shared<RowVector>(
      pool, regionRowType, BufferPtr(nullptr), vectorSize, std::move(children));
}

} // namespace facebook::velox::tpch

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

#include "velox/tpch/gen/DBGenIterator.h"

#include <folly/Singleton.h>
#include "velox/common/base/Exceptions.h"
#include "velox/external/duckdb/tpch/dbgen/include/dbgen/dbgen_gunk.hpp"

namespace facebook::velox::tpch {

namespace {

// DBGenBackend is a singleton that controls access to the DBGEN C functions,
// and ensures that the required structures are properly initialized and
// destructed.
//
// Only acquire instances of this class using folly::Singleton.
class DBGenBackend {
 public:
  DBGenBackend() {
    // load_dists()/cleanup_dists() need to be called to ensure the global
    // structures required by dbgen are populated.
    DBGenContext dbgenCtx;
    load_dists(
        10 * 1024 * 1024, &dbgenCtx); // 10 MB buffer size for text generation.
  }
  ~DBGenBackend() {
    cleanup_dists();
  }
};

// Make the object above a singleton.
static folly::Singleton<DBGenBackend> DBGenBackendSingleton;

} // namespace

DBGenIterator::DBGenIterator(double scaleFactor) {
  auto dbgenBackend = DBGenBackendSingleton.try_get();
  VELOX_CHECK_NOT_NULL(dbgenBackend, "Unable to initialize dbgen's dbgunk.");
  if (scaleFactor < MIN_SCALE && scaleFactor > 0) {
    dbgenCtx_.scale_factor = 1;
  } else {
    dbgenCtx_.scale_factor = static_cast<long>(scaleFactor);
  }
}

void DBGenIterator::initNation(size_t offset) {
  sd_nation(NATION, offset, &dbgenCtx_);
}

void DBGenIterator::initRegion(size_t offset) {
  sd_region(REGION, offset, &dbgenCtx_);
}

void DBGenIterator::initOrder(size_t offset) {
  sd_order(ORDER, offset, &dbgenCtx_);
  sd_line(LINE, offset, &dbgenCtx_);
}

void DBGenIterator::initSupplier(size_t offset) {
  sd_supp(SUPP, offset, &dbgenCtx_);
}

void DBGenIterator::initPart(size_t offset) {
  sd_part(PART, offset, &dbgenCtx_);
  sd_psupp(PSUPP, offset, &dbgenCtx_);
}

void DBGenIterator::initCustomer(size_t offset) {
  sd_cust(CUST, offset, &dbgenCtx_);
}

void DBGenIterator::genNation(size_t index, code_t& code) {
  row_start(NATION, &dbgenCtx_);
  mk_nation(index, &code, &dbgenCtx_);
  row_stop_h(NATION, &dbgenCtx_);
}

void DBGenIterator::genRegion(size_t index, code_t& code) {
  row_start(REGION, &dbgenCtx_);
  mk_region(index, &code, &dbgenCtx_);
  row_stop_h(REGION, &dbgenCtx_);
}

void DBGenIterator::genOrder(size_t index, order_t& order) {
  row_start(ORDER, &dbgenCtx_);
  mk_order(index, &order, &dbgenCtx_, /*update-num=*/0);
  row_stop_h(ORDER, &dbgenCtx_);
}

void DBGenIterator::genSupplier(size_t index, supplier_t& supplier) {
  row_start(SUPP, &dbgenCtx_);
  mk_supp(index, &supplier, &dbgenCtx_);
  row_stop_h(SUPP, &dbgenCtx_);
}

void DBGenIterator::genPart(size_t index, part_t& part) {
  row_start(PART, &dbgenCtx_);
  mk_part(index, &part, &dbgenCtx_);
  row_stop_h(PART, &dbgenCtx_);
}

void DBGenIterator::genCustomer(size_t index, customer_t& customer) {
  row_start(CUST, &dbgenCtx_);
  mk_cust(index, &customer, &dbgenCtx_);
  row_stop_h(CUST, &dbgenCtx_);
}

} // namespace facebook::velox::tpch

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

#define DECLARER
#include "presto_cpp/main/connectors/tpcds/DSDGenIterator.h"
#include "address.h"
#include "build_support.h"
#include "config.h"
#include "dist.h"
#include "genrand.h"
#include "init.h"
#include "iostream"
#include "parallel.h"
#include "params.h"
#include "porting.h"
#include "scaling.h"
#include "tdefs.h"

namespace facebook::velox::tpcds {

void InitializeDSDgen(
    double scale,
    vector_size_t parallel,
    vector_size_t child,
    DSDGenContext& dsdGenContext) {
  dsdGenContext.Reset();
  resetCountCount();

  std::string scaleStr = std::to_string(scale);
  set_str("SCALE", scaleStr.c_str(), dsdGenContext);
  std::string parallelStr = std::to_string(parallel);
  set_str("PARALLEL", parallelStr.c_str(), dsdGenContext);
  std::string childStr = std::to_string(child);
  set_str("CHILD", childStr.c_str(), dsdGenContext);

  init_rand(dsdGenContext); // no random numbers without this
}

std::string getQuery(int query) {
  if (query <= 0 || query > TPCDS_QUERIES_COUNT) {
    throw std::exception();
  }
  return TPCDS_QUERIES[query - 1];
}

DSDGenIterator::DSDGenIterator(
    double scaleFactor,
    vector_size_t parallel,
    vector_size_t child) {
  table_defs.resize(DBGEN_VERSION); // there are 24 TPC-DS tables
  VELOX_CHECK_GE(scaleFactor, 0, "Tpch scale factor must be non-negative");
  dsdgenCtx_.scaleFactor = scaleFactor;
  InitializeDSDgen(scaleFactor, parallel, child, dsdgenCtx_);
}

void DSDGenIterator::initializeTable(
    std::vector<VectorPtr> children,
    int table_id) {
  auto tdef = getSimpleTdefsByNumber(table_id, dsdgenCtx_);
  tpcds_table_def table_def;
  table_def.name = tdef->name;
  table_def.fl_child = tdef->flags & FL_CHILD ? 1 : 0;
  table_def.fl_small = tdef->flags & FL_SMALL ? 1 : 0;
  table_def.first_column = tdef->nFirstColumn;
  table_def.children = children;
  table_def.dsdGenContext = &dsdgenCtx_;
  table_defs[table_id] = std::make_unique<tpcds_table_def>(table_def);
}

std::vector<std::unique_ptr<tpcds_table_def>>& DSDGenIterator::getTableDefs() {
  return table_defs;
};

tpcds_builder_func DSDGenIterator::GetTDefFunctionByNumber(int table_id) {
  auto table_funcs = getTdefFunctionsByNumber(table_id);
  return table_funcs->builder;
}

void DSDGenIterator::initTableOffset(int32_t table_id, size_t offset) {
  row_skip(table_id, offset, dsdgenCtx_);
}
void DSDGenIterator::genRow(int32_t table_id, size_t index) {
  auto builder_func = GetTDefFunctionByNumber(table_id);
  builder_func((void*)&table_defs, index, dsdgenCtx_);
  row_stop(table_id, dsdgenCtx_);
}

int64_t DSDGenIterator::getRowCount(int32_t table) {
  return get_rowcount(table, dsdgenCtx_);
}

} // namespace facebook::velox::tpcds

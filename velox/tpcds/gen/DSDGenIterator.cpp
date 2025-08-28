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

#include "velox/tpcds/gen/DSDGenIterator.h"
#include "velox/common/base/Exceptions.h"
#include "velox/tpcds/gen/dsdgen/include/dist.h"
#include "velox/tpcds/gen/dsdgen/include/genrand.h"
#include "velox/tpcds/gen/dsdgen/include/parallel.h"
#include "velox/tpcds/gen/dsdgen/include/params.h"
#include "velox/tpcds/gen/dsdgen/include/scaling.h"
#include "velox/tpcds/gen/dsdgen/include/tdefs.h"

using namespace facebook::velox;

namespace facebook::velox::tpcds {

void initializeDSDgen(
    double scale,
    int32_t parallel,
    int32_t child,
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

DSDGenIterator::DSDGenIterator(
    double scaleFactor,
    int32_t parallel,
    int32_t child) {
  tableDefs_.resize(DBGEN_VERSION); // there are 24 TPC-DS tables
  VELOX_CHECK_GT(scaleFactor, 0.0, "Tpcds scale factor must be non-negative");
  initializeDSDgen(scaleFactor, parallel, child, dsdgenCtx_);
}

void DSDGenIterator::initializeTable(
    const std::vector<VectorPtr>& children,
    int table_id) {
  auto tdef = getSimpleTdefsByNumber(table_id, dsdgenCtx_);
  facebook::velox::tpcds::TpcdsTableDef table_def;
  table_def.name = tdef->name;
  table_def.fl_child = tdef->flags & FL_CHILD ? 1 : 0;
  table_def.fl_small = tdef->flags & FL_SMALL ? 1 : 0;
  table_def.first_column = tdef->nFirstColumn;
  table_def.children = children;
  table_def.dsdGenContext = &dsdgenCtx_;
  tableDefs_[table_id] = std::make_unique<TpcdsTableDef>(table_def);
}

std::vector<std::unique_ptr<TpcdsTableDef>>& DSDGenIterator::getTableDefs() {
  return tableDefs_;
};

tpcds_builder_func DSDGenIterator::getTDefFunctionByNumber(int table_id) {
  auto table_funcs = getTdefFunctionsByNumber(table_id);
  return table_funcs->builder;
}

void DSDGenIterator::initTableOffset(int32_t table_id, size_t offset) {
  row_skip(table_id, offset, dsdgenCtx_);
}

void DSDGenIterator::genRow(int32_t table_id, size_t index) {
  auto builder_func = getTDefFunctionByNumber(table_id);
  builder_func((void*)&tableDefs_, index, dsdgenCtx_);
  row_stop(table_id, dsdgenCtx_);
}

int64_t DSDGenIterator::getRowCount(int32_t table) {
  return get_rowcount(table, dsdgenCtx_);
}

} // namespace facebook::velox::tpcds

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

#include "velox/tpcds/gen/utils/append_info-c.h"

namespace facebook::velox::tpcds {

typedef int64_t ds_key_t;

typedef int (*tpcds_builder_func)(void*, ds_key_t, DSDGenContext& dsdgenCtx);

void initializeDSDgen(
    double scale,
    int32_t parallel,
    int32_t child,
    DSDGenContext& dsdGenContext);

/// This class exposes a thread-safe and reproducible iterator over TPC-DS
/// synthetically generated data, backed by DSDGEN.
class DSDGenIterator {
 public:
  explicit DSDGenIterator(double scaleFactor, int32_t parallel, int32_t child);

  /// Initializes the table definition and the table schema.
  void initializeTable(const std::vector<VectorPtr>& children, int table);

  /// Returns a vector of all the table definitions.
  std::vector<std::unique_ptr<TpcdsTableDef>>& getTableDefs();

  // Before generating records using the gen*() functions below, call the
  // initTableOffset(int32_t table_id, size_t offset) function to correctly
  // initialize the seed given the offset to be generated.
  // table_id corresponds to the table that needs to be generated.
  // offset represents the row from which data for tables will be generated
  // using the gen*() functions. offset lets generate data for a segment of the
  // table and should be less than the table's row count.
  void initTableOffset(int32_t table_id, size_t offset);

  /// Generate different types of records.
  // table_id corresponds to the table that is to be generated and row is the
  // row to be generated.
  void genRow(int32_t table_id, size_t row);

  /// Gets the row count for a table.
  ds_key_t getRowCount(int32_t table_id);

  // Gets the metadata for a table, which hold information about the mk_*()
  // functions responsible for generating the data.
  tpcds_builder_func getTDefFunctionByNumber(int table_id);

 protected:
  DSDGenContext dsdgenCtx_;
  std::vector<std::unique_ptr<TpcdsTableDef>> tableDefs_;
};

} // namespace facebook::velox::tpcds

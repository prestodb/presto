/*
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

#include <presto_cpp/main/connectors/tpcds/dsdgen/include/dsdgen-c/dist.h>
#include <presto_cpp/main/connectors/tpcds/dsdgen/include/tpcds_constants.hpp>
#include <presto_cpp/main/connectors/tpcds/include/append_info-c.hpp>

namespace facebook::velox::tpcds {

typedef int64_t ds_key_t;

typedef int (*tpcds_builder_func)(void*, ds_key_t, DSDGenContext& dsdgenCtx);

void InitializeDSDgen(
    double scale,
    vector_size_t parallel,
    vector_size_t child,
    DSDGenContext& dsdGenContext);

std::string getQuery(int query);

/// This class exposes a thread-safe and reproducible iterator over TPC-DS
/// synthetically generated data, backed by DSDGEN.
class DSDGenIterator {
 public:
  explicit DSDGenIterator(
      double scaleFactor,
      vector_size_t parallel,
      vector_size_t child);

  void initializeTable(std::vector<VectorPtr> children, int table);

  std::vector<std::unique_ptr<tpcds_table_def>>& getTableDefs();

  // Before generating records using the gen*() functions below, call the
  // appropriate init*() function to correctly initialize the seed given the
  // offset to be generated.
  void initTableOffset(int32_t table_id, size_t offset);

  // Generate different types of records.
  void genRow(int32_t table_id, size_t index);

  ds_key_t getRowCount(int32_t table_id);

  tpcds_builder_func GetTDefFunctionByNumber(int table_id);

 protected:
  DSDGenContext dsdgenCtx_;
  std::vector<std::unique_ptr<tpcds_table_def>> table_defs;
};

} // namespace facebook::velox::tpcds

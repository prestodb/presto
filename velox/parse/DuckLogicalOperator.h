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

// This file contains the definitions of the logical plan nodes extracted from
// the duckdb-internal.hpp file. It is not possible to include
// duckdb-internal.hpp directly because it contains an incompatible / outdated
// copy of fmt/format.h.

/*
Copyright 2018-2022 Stichting DuckDB Foundation

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

#include "velox/external/duckdb/duckdb.hpp"

namespace duckdb {

//! LogicalDummyScan represents a dummy scan returning a single row
class LogicalDummyScan : public LogicalOperator {
 public:
  explicit LogicalDummyScan(idx_t table_index)
      : LogicalOperator(LogicalOperatorType::LOGICAL_DUMMY_SCAN),
        table_index(table_index) {}

  idx_t table_index;

 public:
  vector<ColumnBinding> GetColumnBindings() override {
    return {ColumnBinding(table_index, 0)};
  }

  idx_t EstimateCardinality(ClientContext& context) override {
    return 1;
  }

 protected:
  void ResolveTypes() override {
    if (types.size() == 0) {
      types.emplace_back(LogicalType::INTEGER);
    }
  }
};

//! LogicalGet represents a scan operation from a data source
class LogicalGet : public LogicalOperator {
 public:
  LogicalGet(
      idx_t table_index,
      TableFunction function,
      unique_ptr<FunctionData> bind_data,
      vector<LogicalType> returned_types,
      vector<string> returned_names);

  //! The table index in the current bind context
  idx_t table_index;
  //! The function that is called
  TableFunction function;
  //! The bind data of the function
  unique_ptr<FunctionData> bind_data;
  //! The types of ALL columns that can be returned by the table function
  vector<LogicalType> returned_types;
  //! The names of ALL columns that can be returned by the table function
  vector<string> names;
  //! Bound column IDs
  vector<column_t> column_ids;
  //! Filters pushed down for table scan
  TableFilterSet table_filters;

  string GetName() const override;
  string ParamsToString() const override;
  //! Returns the underlying table that is being scanned, or nullptr if there is
  //! none
  TableCatalogEntry* GetTable() const;

 public:
  vector<ColumnBinding> GetColumnBindings() override;

  idx_t EstimateCardinality(ClientContext& context) override;

 protected:
  void ResolveTypes() override;
};

//! LogicalFilter represents a filter operation (e.g. WHERE or HAVING clause)
class LogicalFilter : public LogicalOperator {
 public:
  explicit LogicalFilter(unique_ptr<Expression> expression);
  LogicalFilter();

  vector<idx_t> projection_map;

 public:
  vector<ColumnBinding> GetColumnBindings() override;

  bool SplitPredicates() {
    return SplitPredicates(expressions);
  }
  //! Splits up the predicates of the LogicalFilter into a set of predicates
  //! separated by AND Returns whether or not any splits were made
  static bool SplitPredicates(vector<unique_ptr<Expression>>& expressions);

 protected:
  void ResolveTypes() override;
};

//! LogicalProjection represents the projection list in a SELECT clause
class LogicalProjection : public LogicalOperator {
 public:
  LogicalProjection(
      idx_t table_index,
      vector<unique_ptr<Expression>> select_list);

  idx_t table_index;

 public:
  vector<ColumnBinding> GetColumnBindings() override;

 protected:
  void ResolveTypes() override;
};

using GroupingSet = set<idx_t>;

//! LogicalAggregate represents an aggregate operation with (optional) GROUP BY
//! operator.
class LogicalAggregate : public LogicalOperator {
 public:
  LogicalAggregate(
      idx_t group_index,
      idx_t aggregate_index,
      vector<unique_ptr<Expression>> select_list);

  //! The table index for the groups of the LogicalAggregate
  idx_t group_index;
  //! The table index for the aggregates of the LogicalAggregate
  idx_t aggregate_index;
  //! The table index for the GROUPING function calls of the LogicalAggregate
  idx_t groupings_index;
  //! The set of groups (optional).
  vector<unique_ptr<Expression>> groups;
  //! The set of grouping sets (optional).
  vector<GroupingSet> grouping_sets;
  //! The list of grouping function calls (optional)
  vector<vector<idx_t>> grouping_functions;
  //! Group statistics (optional)
  vector<unique_ptr<BaseStatistics>> group_stats;

 public:
  string ParamsToString() const override;

  vector<ColumnBinding> GetColumnBindings() override;

 protected:
  void ResolveTypes() override;
};

//! LogicalCrossProduct represents a cross product between two relations
class LogicalCrossProduct : public LogicalOperator {
 public:
  LogicalCrossProduct();

 public:
  vector<ColumnBinding> GetColumnBindings() override;

 protected:
  void ResolveTypes() override;
};
} // namespace duckdb

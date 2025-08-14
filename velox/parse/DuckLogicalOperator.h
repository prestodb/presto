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

#include <duckdb.hpp> // @manual

/// Extract from DuckDB headers describing resolved logical
/// plans. This is a verbatic extract and naming conventions are those
/// of DuckDB.
namespace duckdb {

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

//! LogicalDistinct filters duplicate entries from its child operator
class LogicalDistinct : public LogicalOperator {
 public:
  static constexpr const LogicalOperatorType TYPE =
      LogicalOperatorType::LOGICAL_DISTINCT;

 public:
  explicit LogicalDistinct(DistinctType distinct_type);
  explicit LogicalDistinct(
      vector<unique_ptr<Expression>> targets,
      DistinctType distinct_type);

  //! Whether or not this is a DISTINCT or DISTINCT ON
  DistinctType distinct_type;
  //! The set of distinct targets
  vector<unique_ptr<Expression>> distinct_targets;
  //! The order by modifier (optional, only for distinct on)
  unique_ptr<BoundOrderModifier> order_by;

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

/// LogicalLimit represents a LIMIT clause
class LogicalLimit : public LogicalOperator {
 public:
  LogicalLimit(
      int64_t limit_val,
      int64_t offset_val,
      unique_ptr<Expression> limit,
      unique_ptr<Expression> offset);

  /// Limit and offset values in case they are constants, used in optimizations.
  int64_t limit_val;
  int64_t offset_val;
  /// The maximum amount of elements to emit
  unique_ptr<Expression> limit;
  /// The offset from the start to begin emitting elements
  unique_ptr<Expression> offset;

 public:
  vector<ColumnBinding> GetColumnBindings() override;

  idx_t EstimateCardinality(ClientContext& context) override;

 protected:
  void ResolveTypes() override;
};

class LogicalOrder : public LogicalOperator {
 public:
  static constexpr const LogicalOperatorType TYPE =
      LogicalOperatorType::LOGICAL_ORDER_BY;

 public:
  explicit LogicalOrder(vector<BoundOrderByNode> orders)
      : LogicalOperator(LogicalOperatorType::LOGICAL_ORDER_BY),
        orders(std::move(orders)) {}

  vector<BoundOrderByNode> orders;
  vector<idx_t> projections;

 public:
  vector<ColumnBinding> GetColumnBindings() override {
    auto child_bindings = children[0]->GetColumnBindings();
    if (projections.empty()) {
      return child_bindings;
    }

    vector<ColumnBinding> result;
    for (auto& col_idx : projections) {
      result.push_back(child_bindings[col_idx]);
    }
    return result;
  }

  void Serialize(FieldWriter& writer) const override;
  static unique_ptr<LogicalOperator> Deserialize(
      LogicalDeserializationState& state,
      FieldReader& reader);

  string ParamsToString() const override {
    string result = "ORDERS:\n";
    for (idx_t i = 0; i < orders.size(); i++) {
      if (i > 0) {
        result += "\n";
      }
      result += orders[i].expression->GetName();
    }
    return result;
  }

 protected:
  void ResolveTypes() override {
    const auto child_types = children[0]->types;
    if (projections.empty()) {
      types = child_types;
    } else {
      for (auto& col_idx : projections) {
        types.push_back(child_types[col_idx]);
      }
    }
  }
};

//! LogicalJoin represents a join between two relations
class LogicalJoin : public LogicalOperator {
 public:
  explicit LogicalJoin(
      JoinType type,
      LogicalOperatorType logical_type = LogicalOperatorType::LOGICAL_JOIN);

  // Gets the set of table references that are reachable from this node
  static void GetTableReferences(
      LogicalOperator& op,
      unordered_set<idx_t>& bindings);

  static void GetExpressionBindings(
      Expression& expr,
      unordered_set<idx_t>& bindings);

  /// The type of the join (INNER, OUTER, etc...)
  JoinType join_type;
  /// Table index used to refer to the MARK column (in case of a MARK join)
  idx_t mark_index;
  /// The columns of the LHS that are output by the join
  vector<idx_t> left_projection_map;
  /// The columns of the RHS that are output by the join
  vector<idx_t> right_projection_map;
  /// Join Keys statistics (optional)
  vector<unique_ptr<BaseStatistics>> join_stats;

 public:
  vector<ColumnBinding> GetColumnBindings() override;

 protected:
  void ResolveTypes() override;
};

//! JoinCondition represents a left-right comparison join condition
struct JoinCondition {
 public:
  JoinCondition() {}

  //! Turns the JoinCondition into an expression; note that this destroys the
  //! JoinCondition as the expression inherits the left/right expressions
  static unique_ptr<Expression> CreateExpression(JoinCondition cond);
  static unique_ptr<Expression> CreateExpression(
      vector<JoinCondition> conditions);

 public:
  unique_ptr<Expression> left;
  unique_ptr<Expression> right;
  ExpressionType comparison;
};

//! LogicalComparisonJoin represents a join that involves comparisons between
//! the LHS and RHS
class LogicalComparisonJoin : public LogicalJoin {
 public:
  explicit LogicalComparisonJoin(
      JoinType type,
      LogicalOperatorType logical_type =
          LogicalOperatorType::LOGICAL_COMPARISON_JOIN);

  //! The conditions of the join
  vector<JoinCondition> conditions;
  //! Used for duplicate-eliminated joins
  vector<LogicalType> delim_types;

 public:
  string ParamsToString() const override;

 public:
  static unique_ptr<LogicalOperator> CreateJoin(
      JoinType type,
      unique_ptr<LogicalOperator> left_child,
      unique_ptr<LogicalOperator> right_child,
      unordered_set<idx_t>& left_bindings,
      unordered_set<idx_t>& right_bindings,
      vector<unique_ptr<Expression>>& expressions);
};

//! LogicalDelimGet represents a duplicate eliminated scan belonging to a
//! DelimJoin
class LogicalDelimGet : public LogicalOperator {
 public:
  LogicalDelimGet(idx_t table_index, vector<LogicalType> types)
      : LogicalOperator(LogicalOperatorType::LOGICAL_DELIM_GET),
        table_index(table_index) {
    D_ASSERT(types.size() > 0);
    chunk_types = types;
  }

  //! The table index in the current bind context
  idx_t table_index;
  //! The types of the chunk
  vector<LogicalType> chunk_types;

 public:
  vector<ColumnBinding> GetColumnBindings() override {
    return GenerateColumnBindings(table_index, chunk_types.size());
  }

 protected:
  void ResolveTypes() override {
    // types are resolved in the constructor
    this->types = chunk_types;
  }
};

//! LogicalDelimJoin represents a special "duplicate eliminated" join. This join
//! type is only used for subquery flattening, and involves performing duplicate
//! elimination on the LEFT side which is then pushed into the RIGHT side.
class LogicalDelimJoin : public LogicalComparisonJoin {
 public:
  explicit LogicalDelimJoin(JoinType type)
      : LogicalComparisonJoin(type, LogicalOperatorType::LOGICAL_DELIM_JOIN) {}

  //! The set of columns that will be duplicate eliminated from the LHS and
  //! pushed into the RHS
  vector<unique_ptr<Expression>> duplicate_eliminated_columns;
};

//! LogicalAnyJoin represents a join with an arbitrary expression as
//! JoinCondition
class LogicalAnyJoin : public LogicalJoin {
 public:
  static constexpr const LogicalOperatorType TYPE =
      LogicalOperatorType::LOGICAL_ANY_JOIN;

 public:
  explicit LogicalAnyJoin(JoinType type);

  //! The JoinCondition on which this join is performed
  unique_ptr<Expression> condition;

 public:
  string ParamsToString() const override;
  void Serialize(FieldWriter& writer) const override;
  static unique_ptr<LogicalOperator> Deserialize(
      LogicalDeserializationState& state,
      FieldReader& reader);
};

} // namespace duckdb

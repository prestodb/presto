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

#include "velox/core/PlanNode.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/py/type/PyType.h"

namespace facebook::velox::py {

/// Called when the Python module is loaded/initialized to register Velox
/// resources like serde functions, type resolver, local filesystem and Presto
/// functions (scalar and aggregate).
void registerAllResources();

class PyVector;

/// Thin wrapper class used to expose Velox plan nodes to Python. It is only
/// used to maintain the internal Velox structure and expose basic string
/// serialization functionality.
class PyPlanNode {
 public:
  /// Creates a new PyPlanNode wrapper given a Velox plan node. Throws if
  /// planNode is nullptr.
  explicit PyPlanNode(core::PlanNodePtr planNode);

  const core::PlanNodePtr& planNode() const {
    return planNode_;
  }

  std::string id() const {
    return std::string{planNode_->id()};
  }

  std::string serialize() const {
    return folly::toJson(planNode_->serialize());
  }

  std::string name() const {
    return std::string(planNode_->name());
  }

  std::string toString(bool detailed = false, bool recursive = false) const {
    return planNode_->toString(detailed, recursive);
  }

 private:
  core::PlanNodePtr planNode_;
};

/// Wrapper class for PlanBuilder. It allows us to avoid exposing all details of
/// the class to users making it easier to use.
class PyPlanBuilder {
 public:
  /// Constructs a new PyPlanBuilder. If provided, the planNodeIdGenerator is
  /// used; otherwise a new one is created.
  PyPlanBuilder(
      const std::shared_ptr<core::PlanNodeIdGenerator>& generator = nullptr);

  // Returns the plan node at the head of the internal plan builder. If there is
  // no plan node (plan builder is empty), then std::nullopt will signal pybind
  // to return None to the Python client.
  std::optional<PyPlanNode> planNode() const {
    if (planBuilder_.planNode() != nullptr) {
      return PyPlanNode(planBuilder_.planNode());
    }
    return std::nullopt;
  }

  // Returns a new builder sharing the plan node id generator, such that the new
  // builder can safely be used to build other parts/pipelines of the same plan.
  PyPlanBuilder newBuilder() {
    DCHECK(planNodeIdGenerator_ != nullptr);
    return PyPlanBuilder{planNodeIdGenerator_};
  }

  /// Add table scan node with basic functionality. This API is Hive-connector
  /// specific.
  ///
  /// @param output The schema to be read from the file. Needs to be a RowType.
  /// @param aliases Aliases to apply, mapping from the desired output name to
  /// the input name in the file. If there are aliases, OutputSchema should be
  /// defined based on the aliased names. The Python dict should contain strings
  /// for keys and values.
  /// @param subfields Used for projecting subfields from containers (like map
  /// keys or struct fields), when one does not want to read the entire
  /// container. It's a dictionary mapping from column name (string) to the list
  /// of subitems to project from it. For now, a list of integers representing
  /// the subfields in a flatmap/struct.
  /// @param rowIndexColumnName If defined, create an output column with that
  /// name producing $row_ids. This name needs to be part of `output`.
  /// @param connectorId ID of the connector to use for this scan. By default
  /// the Python module will specify the default name used to register the
  /// Hive connector in the first place.
  ///
  /// Example (from Python API):
  ///
  /// tableScan(
  ///   output=ROW(
  ///     names=["row_number", "$row_group_id", "aliased_name"],
  ///     types=[BIGINT(), VARCHAR(), MAP(INTEGER(), VARCHAR())],
  ///   ),
  ///   subfields={"aliased_name": [1234, 5431, 65223]},
  ///   row_index="row_number",
  ///   aliases={
  ///     "aliased_name": "name_in_file",
  ///    },
  ///  )
  PyPlanBuilder& tableScan(
      const velox::py::PyType& output,
      const pybind11::dict& aliases,
      const pybind11::dict& subfields,
      const std::string& rowIndexColumnName,
      const std::string& connectorId);

  // Add the provided vectors straight into the operator tree.
  PyPlanBuilder& values(const std::vector<PyVector>& values);

  // Add a list of projections. Projections are specified as SQL expressions and
  // currently use DuckDB's parser semantics.
  PyPlanBuilder& project(const std::vector<std::string>& projections);

  // Add a filter (selection) to the plan. Filters are specified as SQL
  // expressions and currently use DuckDB's parser semantics.
  PyPlanBuilder& filter(const std::string& filter);

  // Add a single-stage aggregation given a set of group keys, and aggregations.
  // Aggregations are specified as SQL expressions and currently use DuckDB's
  // parser semantics.
  PyPlanBuilder& singleAggregation(
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& aggregations);

  // Add a merge join node to the plan. Assumes that both left and right
  // subtrees will produce input sorted in join key order.
  //
  // @param leftKeys Set of join keys from the left (current plan builder) plan
  // subtree.
  // @param leftKeys Set of join keys from the right plan subtree.
  // @param rightPlanSubtree Subtree to join to.
  // @param output List of column names to project in the output of the join.
  // @param filter An optional filter specified as a SQL expression to be
  // applied during the join.
  // @param joinType The type of join (kInner, kLeft, kRight, or kFull)
  PyPlanBuilder& mergeJoin(
      const std::vector<std::string>& leftKeys,
      const std::vector<std::string>& rightKeys,
      const PyPlanNode& rightPlanSubtree,
      const std::vector<std::string>& output,
      const std::string& filter,
      core::JoinType joinType);

  // TODO: Add other nodes.

 private:
  exec::test::PlanBuilder planBuilder_;
  std::shared_ptr<core::PlanNodeIdGenerator> planNodeIdGenerator_;
};

} // namespace facebook::velox::py

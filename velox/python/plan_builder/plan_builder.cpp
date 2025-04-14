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

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "velox/python/init/PyInit.h"
#include "velox/python/plan_builder/PyPlanBuilder.h"
#include "velox/python/type/PyType.h"
#include "velox/python/vector/PyVector.h"

namespace py = pybind11;

PYBIND11_MODULE(plan_builder, m) {
  using namespace facebook;

  velox::py::initializeVeloxMemory();
  velox::py::registerAllResources();

  // Need types to specify table scan schema output.
  py::module::import("pyvelox.type");

  // PlanNode should not be created from Python directly (no registered
  // constructor).
  py::class_<velox::py::PyPlanNode>(m, "PlanNode")
      .def(
          "__str__",
          [](const velox::py::PyPlanNode& planNode) {
            return planNode.toString(false, true);
          },
          py::doc(R"(
        Returns a short and recursive description of the plan.
      )"))
      .def("name", &velox::py::PyPlanNode::name, py::doc(R"(
        Returns the name of the current plan node.
      )"))
      .def("serialize", &velox::py::PyPlanNode::serialize, py::doc(R"(
        Returns a serialized string containing the plan specification.
      )"))
      .def(
          "to_string",
          [](const velox::py::PyPlanNode& planNode) {
            return planNode.toString(true, true);
          },
          py::doc(R"(
        Returns a detailed and recursive description of the plan.
      )"))
      .def("id", &velox::py::PyPlanNode::id, py::doc(R"(
        Returns the id of the current plan node.
      )"));

  m.def(
      "deserialize_plan",
      [](const std::string& serializedPlan) {
        return velox::py::PyPlanNode::deserialize(serializedPlan, nullptr);
      },
      py::doc(R"(
       Deserializes a serialized plan string.
  )"));

  // Join type enum for hash, merge and nested loop joins.
  py::enum_<velox::core::JoinType>(m, "JoinType")
      .value("INNER", velox::core::JoinType::kInner)
      .value("LEFT", velox::core::JoinType::kLeft)
      .value("RIGHT", velox::core::JoinType::kRight)
      .value("FULL", velox::core::JoinType::kFull);

  py::class_<velox::py::PyPlanBuilder>(m, "PlanBuilder", py::module_local())
      .def(py::init<>())
      .def("get_plan_node", &velox::py::PyPlanBuilder::planNode, py::doc(R"(
        Returns the current plan node.
      )"))
      .def("new_builder", &velox::py::PyPlanBuilder::newBuilder, py::doc(R"(
        Returns a new builder sharing the same plan node id generator,
        so that they can be safely reused to build different parts of the
        same plan.
      )"))
      .def(
          "table_scan",
          &velox::py::PyPlanBuilder::tableScan,
          py::arg("output_schema") = velox::py::PyType{},
          py::arg("aliases") = py::dict{},
          py::arg("subfields") = py::dict{},
          py::arg("filters") = std::vector<std::string>{},
          py::arg("remaining_filter") = "",
          py::arg("row_index") = "",
          py::arg("connector_id") = "hive",
          py::arg("input_files") = std::nullopt,
          py::doc(R"(
        Adds a table scan node to the plan.

        Args:
          output_schema: A RowType containing the schema to be projected out
                         of the scan.
          aliases: An optional map of aliases to apply, from the desired
                   output name to the name as defined in the file. If
                   there are aliases, `output` should be specified based
                   on the aliased name.
          subfields: Used to project individual items from columns instead
                     of reading entire containers. It maps from the column
                     name to a list of items to be projected out.
          filters: A list of SQL filters to be applied to the data as it is
                   decoded/read.
          remainingFilter: SQL expression for the additional conjunct. May
                           include multiple columns and SQL functions. The
                           remainingFilter is AND'ed with the other filters.
          row_index: If defined, creates an output column with this name
                     producing $row_ids. This name needs to be part of the
                     `output` as BIGINT.
          connector_id: ID of the connector to use for this scan.
          input_files: If defined, uses as the input files so that no splits
                      will need to be added later.
      )"))
      .def(
          "table_write",
          &velox::py::PyPlanBuilder::tableWrite,
          py::arg("output_file") = std::nullopt,
          py::arg("output_path") = std::nullopt,
          py::arg("connector_id") = "hive",
          py::arg("output_schema") = std::nullopt,
          py::doc(R"(
        Adds a table write node to the plan.

        Args:
          output_file: Name of the file to be written.
          output_path: The output path where output files will be written.
                       Specify this parameter instead of `outputFile` if the
                       task is supposed to write files in parallel using
                       multiple drivers. The actual file names in this path
                       will be automatically generated and returned as the
                       TableWriter output. Takes precedence over output_file.
          connector_id: ID of the connector to use for this scan.
          output_schema: An optional RowType containing the schema to be
                         written to the file. By default write the schema
                         produced by the operator upstream.
      )"))
      .def(
          "values",
          &velox::py::PyPlanBuilder::values,
          py::arg("values") = std::vector<velox::py::PyVector>{},
          py::doc(R"(
        Adds the specified vectors to the operator tree as input. All input
        vectors need to be RowVectors.
      )"))
      .def(
          "project",
          &velox::py::PyPlanBuilder::project,
          py::arg("projections") = std::vector<std::string>{},
          py::doc(R"(
        Adds a projection node, calculating expression specified in
        `projections`. Expressions are specified as SQL expressions.
      )"))
      .def(
          "filter",
          &velox::py::PyPlanBuilder::filter,
          py::arg("filter") = "",
          py::doc(R"(
        Adds a filter node. The filter expression is specified as a
        SQL expression.
      )"))
      .def(
          "aggregate",
          &velox::py::PyPlanBuilder::aggregate,
          py::arg("grouping_keys") = std::vector<std::string>{},
          py::arg("aggregations") = std::vector<std::string>{},
          py::doc(R"(
        Adds a single stage aggregation.

        Args:
          grouping_keys: List of columns to group by.
          aggregations: List of aggregate expressions.
      )"))
      .def(
          "order_by",
          &velox::py::PyPlanBuilder::orderBy,
          py::arg("keys"),
          py::arg("is_partial") = false,
          py::doc(R"(
        Sorts the input based on the values of sorting keys.

        Args:
          keys: List of columns to order by. The strings can be column names
                and optionally contain the sort orientation ("col" or
                "col DESC").
          is_partial: If this node is sorting partial query results (and hence
                      can run in parallel in multiple drivers), or final.
      )"))
      .def(
          "limit",
          &velox::py::PyPlanBuilder::limit,
          py::arg("count"),
          py::arg("offset") = 0,
          py::arg("is_partial") = false,
          py::doc(R"(
        Limit how many rows from the input to produce as output.

        Args:
          count: How many rows to produce, at most.
          offset: Hoy many rows from the beggining of the input to skip.
          is_partial: If this is restricting partial results and hence
                      can be applied once per driver, or if it's applied
                      to the query output.
      )"))
      .def(
          "hash_join",
          &velox::py::PyPlanBuilder::hashJoin,
          py::arg("left_keys"),
          py::arg("right_keys"),
          py::arg("build_plan_node"),
          py::arg("output") = std::vector<std::string>{},
          py::arg("filter") = "",
          py::arg("join_type") = velox::core::JoinType::kInner,
          py::doc(R"(
        Adds a hash join node. Uses the build_plan_node subtree to build the
        hash table, and the current subtree as the probe side.

        Args:
          left_keys: List of keys from the left table (probe).
          right_keys: List of keys from the right table (build).
          build_plan_node: The plan node defined the subplan to join with.
          output: List of columns to be projected out of the join.
          filter: Optional join filter expression.
          join_type: Join type (inner, left, right, full, etc).
      )"))
      .def(
          "merge_join",
          &velox::py::PyPlanBuilder::mergeJoin,
          py::arg("left_keys"),
          py::arg("right_keys"),
          py::arg("right_plan_node"),
          py::arg("output") = std::vector<std::string>{},
          py::arg("filter") = "",
          py::arg("join_type") = velox::core::JoinType::kInner,
          py::doc(R"(
        Adds a merge join node. Merge join requires that left and right sides
        and sorted based on the join keys.

        Args:
          left_keys: List of keys from the left table.
          right_keys: List of keys from the right table.
          right_plan_node: The plan node defined the subplan to join with.
          output: List of columns to be projected out of the join.
          filter: Optional join filter expression.
          join_type: Join type (inner, left, right, full, etc).
      )"))
      .def(
          "index_lookup_join",
          &velox::py::PyPlanBuilder::indexLookupJoin,
          py::arg("left_keys"),
          py::arg("right_keys"),
          py::arg("index_plan_node"),
          py::arg("output") = std::vector<std::string>{},
          py::arg("join_type") = velox::core::JoinType::kInner,
          py::doc(R"(
        Adds an index lookup join node. It requires the index_plan_node
        subtree to be composed of a single table scan on a connector with
        indexed access support.

        Args:
          left_keys: List of keys from the left table.
          right_keys: List of keys from the right table.
          index_plan_node: The subtree containing the lookup table scan.
          output: List of columns to be projected out of the join.
          join_type: Join type (inner, left, right, full, etc).
      )"))
      .def(
          "sorted_merge",
          &velox::py::PyPlanBuilder::sortedMerge,
          py::arg("keys"),
          py::arg("sources"),
          py::doc(R"(
        Takes N sorted `source` subtrees and merges them into a sorted output.
        Assumes that all sources are sorted on `keys`.

        Args:
          keys: The sorting keys.
          sources: The list of sources to merge.
      )"))
      .def(
          "tpch_gen",
          &velox::py::PyPlanBuilder::tpchGen,
          py::arg("table_name"),
          py::arg("columns") = std::vector<std::string>{},
          py::arg("scale_factor") = 1,
          py::arg("num_parts") = 1,
          py::arg("connector_id") = "tpch",
          py::doc(R"(
        Generates TPC-H data on the fly using dbgen. Note that generating data
        on the fly is not terribly efficient, so for performance evaluation one
        should generate data using this node, write it to output storage files,
        (Parquet, ORC, or similar), then benchmark a query plan that reads
        those files.

        Args:
          table_name: The TPC-H table name to generate data for.
          columns: The columns from `table_name` to generate data for. If
                   empty (the default), generate data for all columns.
          scale_factor: TPC-H scale factor to use - controls the amount of
                        data generated.
          num_parts: How many splits to generate. This controls the parallelism
                     and the number of output files to be generated.
          connector_id: ID of the connector to use for this scan.
      )"));
}

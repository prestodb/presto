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

#include "velox/py/lib/PyInit.h"
#include "velox/py/plan_builder/PyPlanBuilder.h"
#include "velox/py/type/PyType.h"
#include "velox/py/vector/PyVector.h"

namespace py = pybind11;

PYBIND11_MODULE(plan_builder, m) {
  using namespace facebook;

  velox::py::initializeVeloxMemory();
  velox::py::registerAllResources();

  // Need types to specify table scan schema output.
  py::module::import("velox.py.type");

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
          py::arg("output") = velox::py::PyType{},
          py::arg("aliases") = py::dict{},
          py::arg("subfields") = py::dict{},
          py::arg("row_index") = "",
          py::arg("connector_id") = "prism",
          py::doc(R"(
        Adds a table scan node to the plan.

        Args:
          output: A RowType containing the schema to be projected out
                  of the scan.
          aliases: An optional map of aliases to apply, from the desired
                   output name to the name as defined in the file. If
                   there are aliases, `output` should be specified based
                   on the aliased name.
          subfields: Used to project individual items from columns instead
                     of reading entire containers. It maps from the column
                     name to a list of items to be projected out.
          row_index: If defined, creates an output column with this name
                     producing $row_ids. This name needs to be part of the
                     `output` as BIGINT.
          connector_id: ID of the connector to use for this scan.
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
          "singleAggregation",
          &velox::py::PyPlanBuilder::singleAggregation,
          py::arg("grouping_keys") = std::vector<std::string>{},
          py::arg("aggregations") = std::vector<std::string>{},
          py::doc(R"(
        Adds a single stage aggregation.

        Args:
          grouping_keys: List of columns to group by.
          aggregations: List of aggregate expressions.
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
        Adds a merge join node.

        Args:
          left_keys: List of keys from the left table.
          right_keys: List of keys from the right table.
          right_plan_node: The plan node defined the subplan to join with.
          output: List of columns to be projected out of the join.
          filter: Optional join filter expression.
      )"));
}

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

#include "velox/py/plan_builder/PyPlanBuilder.h"

#include <folly/executors/GlobalExecutor.h>
#include <pybind11/stl.h>
#include "velox/connectors/hive/TableHandle.h"
#include "velox/core/PlanNode.h"
#include "velox/dwio/dwrf/RegisterDwrfReader.h"
#include "velox/dwio/dwrf/RegisterDwrfWriter.h"
#include "velox/dwio/dwrf/writer/Writer.h"
#include "velox/exec/Cursor.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"
#include "velox/py/vector/PyVector.h"

namespace facebook::velox::py {

namespace py = pybind11;

folly::once_flag registerOnceFlag;

void registerAllResourcesOnce() {
  velox::filesystems::registerLocalFileSystem();

  velox::dwrf::registerDwrfWriterFactory();
  velox::dwrf::registerDwrfReaderFactory();

  velox::parse::registerTypeResolver();

  velox::core::PlanNode::registerSerDe();
  velox::Type::registerSerDe();
  velox::common::Filter::registerSerDe();
  velox::connector::hive::LocationHandle::registerSerDe();
  velox::connector::hive::HiveSortingColumn::registerSerDe();
  velox::connector::hive::HiveBucketProperty::registerSerDe();
  velox::connector::hive::HiveTableHandle::registerSerDe();
  velox::connector::hive::HiveColumnHandle::registerSerDe();
  velox::connector::hive::HiveInsertTableHandle::registerSerDe();
  velox::core::ITypedExpr::registerSerDe();

  // Register functions.
  // TODO: We should move this to a separate module so that clients could
  // register only when needed.
  velox::functions::prestosql::registerAllScalarFunctions();
  velox::aggregate::prestosql::registerAllAggregateFunctions();
}

void registerAllResources() {
  folly::call_once(registerOnceFlag, registerAllResourcesOnce);
}

PyPlanNode::PyPlanNode(core::PlanNodePtr planNode)
    : planNode_(std::move(planNode)) {
  if (planNode_ == nullptr) {
    throw std::runtime_error("Velox plan node cannot be nullptr.");
  }
}

PyPlanBuilder::PyPlanBuilder(
    const std::shared_ptr<core::PlanNodeIdGenerator>& generator)
    : planNodeIdGenerator_(
          generator ? generator
                    : std::make_shared<core::PlanNodeIdGenerator>()) {
  auto rootPool = memory::memoryManager()->addRootPool();
  auto leafPool = rootPool->addLeafChild("py_plan_builder_pool");
  planBuilder_ = exec::test::PlanBuilder(planNodeIdGenerator_, leafPool.get());
}

PyPlanBuilder& PyPlanBuilder::tableScan(
    const velox::py::PyType& output,
    const py::dict& aliases,
    const py::dict& subfields,
    const std::string& rowIndexColumnName,
    const std::string& connectorId) {
  using namespace connector::hive;
  exec::test::PlanBuilder::TableScanBuilder builder(planBuilder_);

  // Try to convert the output type.
  auto outputRowSchema = asRowType(output.type());
  if (outputRowSchema == nullptr) {
    throw std::runtime_error("Output schema must be a ROW().");
  }

  // If there are any aliases, convert to std container and add to the builder.
  std::unordered_map<std::string, std::string> aliasMap;

  if (!aliases.empty()) {
    for (const auto& item : aliases) {
      if (!py::isinstance<py::str>(item.first) ||
          !py::isinstance<py::str>(item.second)) {
        throw std::runtime_error(
            "Keys and values from aliases map need to be strings.");
      }
      aliasMap[item.first.cast<std::string>()] =
          item.second.cast<std::string>();
    }
  }

  // If there are subfields, create the appropriate structures and add to the
  // scan.
  if (!subfields.empty() || !rowIndexColumnName.empty()) {
    std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
        assignments;

    for (size_t i = 0; i < outputRowSchema->size(); ++i) {
      auto name = outputRowSchema->nameOf(i);
      auto type = outputRowSchema->childAt(i);
      std::vector<common::Subfield> requiredSubfields;

      py::object key = py::cast(name);

      // Check if the column was aliased by the user.
      auto it = aliasMap.find(name);
      auto hiveName = it == aliasMap.end() ? name : it->second;

      if (subfields.contains(key)) {
        py::handle value = subfields[key];
        if (!py::isinstance<py::list>(value)) {
          throw std::runtime_error(
              "Subfield map value should be a list of integers.");
        }

        auto values = value.cast<std::vector<int64_t>>();

        // TODO: Assume for now they are fields in a flap map.
        for (const auto& subfield : values) {
          requiredSubfields.emplace_back(
              fmt::format("{}[{}]", hiveName, subfield));
        }
      }

      auto columnType = (name == rowIndexColumnName)
          ? HiveColumnHandle::ColumnType::kRowIndex
          : HiveColumnHandle::ColumnType::kRegular;

      auto columnHandle = std::make_shared<HiveColumnHandle>(
          hiveName, columnType, type, type, std::move(requiredSubfields));
      assignments.insert({name, columnHandle});
    }
    builder.assignments(std::move(assignments));
  }

  builder.outputType(outputRowSchema)
      .columnAliases(std::move(aliasMap))
      .connectorId(connectorId)
      .endTableScan();
  return *this;
}

PyPlanBuilder& PyPlanBuilder::values(const std::vector<PyVector>& values) {
  std::vector<RowVectorPtr> vectors;
  vectors.reserve(values.size());

  for (const auto& pyVector : values) {
    if (auto rowVector =
            std::dynamic_pointer_cast<RowVector>(pyVector.vector())) {
      vectors.emplace_back(rowVector);
    } else {
      throw std::runtime_error("Values node only takes RowVectors.");
    }
  }

  planBuilder_.values(vectors);
  return *this;
}

PyPlanBuilder& PyPlanBuilder::project(
    const std::vector<std::string>& projections) {
  planBuilder_.project(projections);
  return *this;
}

PyPlanBuilder& PyPlanBuilder::filter(const std::string& filter) {
  planBuilder_.filter(filter);
  return *this;
}

PyPlanBuilder& PyPlanBuilder::singleAggregation(
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& aggregations) {
  planBuilder_.singleAggregation(groupingKeys, aggregations);
  return *this;
}

PyPlanBuilder& PyPlanBuilder::mergeJoin(
    const std::vector<std::string>& leftKeys,
    const std::vector<std::string>& rightKeys,
    const PyPlanNode& rightPlanSubtree,
    const std::vector<std::string>& output,
    const std::string& filter,
    core::JoinType joinType) {
  planBuilder_.mergeJoin(
      leftKeys,
      rightKeys,
      rightPlanSubtree.planNode(),
      filter,
      output,
      joinType);
  return *this;
}

} // namespace facebook::velox::py

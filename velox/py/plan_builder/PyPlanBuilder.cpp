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
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/connectors/tpch/TpchConnectorSplit.h"
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
#include "velox/tpch/gen/TpchGen.h"

namespace facebook::velox::py {

namespace py = pybind11;

folly::once_flag registerOnceFlag;

void registerAllResourcesOnce() {
  velox::filesystems::registerLocalFileSystem();

  velox::dwrf::registerDwrfWriterFactory();
  velox::dwrf::registerDwrfReaderFactory();

  velox::dwio::common::LocalFileSink::registerFactory();

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

PyPlanNode::PyPlanNode(
    core::PlanNodePtr planNode,
    const TScanFilesPtr& scanFiles)
    : planNode_(std::move(planNode)), scanFiles_(scanFiles) {
  if (planNode_ == nullptr) {
    throw std::runtime_error("Velox plan node cannot be nullptr.");
  }
}

PyPlanBuilder::PyPlanBuilder(
    const std::shared_ptr<core::PlanNodeIdGenerator>& generator,
    const TScanFilesPtr& scanFiles)
    : planNodeIdGenerator_(
          generator ? generator
                    : std::make_shared<core::PlanNodeIdGenerator>()),
      scanFiles_(scanFiles ? scanFiles : std::make_shared<TScanFiles>()) {
  auto rootPool = memory::memoryManager()->addRootPool();
  auto leafPool = rootPool->addLeafChild("py_plan_builder_pool");
  planBuilder_ = exec::test::PlanBuilder(planNodeIdGenerator_, leafPool.get());
}

std::optional<PyPlanNode> PyPlanBuilder::planNode() const {
  if (planBuilder_.planNode() != nullptr) {
    return PyPlanNode(planBuilder_.planNode(), scanFiles_);
  }
  return std::nullopt;
}

PyPlanBuilder& PyPlanBuilder::tableWrite(
    const PyFile& outputFile,
    const std::string& connectorId,
    const std::optional<PyType>& outputSchema) {
  exec::test::PlanBuilder::TableWriterBuilder builder(planBuilder_);

  // Try to convert the output type.
  RowTypePtr outputRowSchema;

  if (outputSchema != std::nullopt) {
    outputRowSchema = asRowType(outputSchema->type());

    if (outputRowSchema == nullptr) {
      throw std::runtime_error("Output schema must be a ROW().");
    }
    builder.outputType(outputRowSchema);
  }

  builder.outputFileName(outputFile.filePath())
      .fileFormat(outputFile.fileFormat())
      .connectorId(connectorId)
      .endTableWriter();
  return *this;
}

PyPlanBuilder& PyPlanBuilder::tableScan(
    const PyType& outputSchema,
    const py::dict& aliases,
    const py::dict& subfields,
    const std::string& rowIndexColumnName,
    const std::string& connectorId,
    const std::optional<std::vector<PyFile>>& inputFiles) {
  using namespace connector::hive;
  exec::test::PlanBuilder::TableScanBuilder builder(planBuilder_);

  // Try to convert the output type.
  auto outputRowSchema = asRowType(outputSchema.type());
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

  // Store the id of the scan and the respective splits.
  std::vector<std::shared_ptr<connector::ConnectorSplit>> splits;
  if (inputFiles.has_value()) {
    for (const auto& inputFile : *inputFiles) {
      splits.push_back(std::make_shared<connector::hive::HiveConnectorSplit>(
          connectorId, inputFile.filePath(), inputFile.fileFormat()));
    }
  }

  (*scanFiles_)[planBuilder_.planNode()->id()] = std::move(splits);
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

PyPlanBuilder& PyPlanBuilder::aggregate(
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& aggregations) {
  planBuilder_.singleAggregation(groupingKeys, aggregations);
  return *this;
}

PyPlanBuilder& PyPlanBuilder::orderBy(
    const std::vector<std::string>& keys,
    bool isPartial) {
  planBuilder_.orderBy(keys, isPartial);
  return *this;
}

PyPlanBuilder&
PyPlanBuilder::limit(int64_t count, int64_t offset, bool isPartial) {
  planBuilder_.limit(offset, count, isPartial);
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

PyPlanBuilder& PyPlanBuilder::sortedMerge(
    const std::vector<std::string>& keys,
    const std::vector<std::optional<PyPlanNode>>& pySources) {
  std::vector<core::PlanNodePtr> sources;
  sources.reserve(pySources.size());

  for (const auto& pySource : pySources) {
    if (pySource.has_value()) {
      sources.push_back(pySource->planNode());
    }
  }

  planBuilder_.localMerge(keys, std::move(sources));
  return *this;
}

PyPlanBuilder& PyPlanBuilder::tpchGen(
    const std::string& tableName,
    const std::vector<std::string>& columns,
    double scaleFactor,
    size_t numParts,
    const std::string& connectorId) {
  // If `columns` is empty, get all columns from `table`.
  auto table = tpch::fromTableName(tableName);
  planBuilder_.tpchTableScan(
      table,
      columns.empty() ? tpch::getTableSchema(table)->names() : columns,
      scaleFactor,
      connectorId);

  // Generate one split per part.
  std::vector<std::shared_ptr<connector::ConnectorSplit>> splits;
  for (size_t i = 0; i < numParts; ++i) {
    splits.push_back(std::make_shared<connector::tpch::TpchConnectorSplit>(
        connectorId, numParts, i));
  }

  (*scanFiles_)[planBuilder_.planNode()->id()] = std::move(splits);
  return *this;
}

} // namespace facebook::velox::py

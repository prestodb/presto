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

#include "velox/python/plan_builder/PyPlanBuilder.h"

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
#include "velox/python/vector/PyVector.h"
#include "velox/tpch/gen/TpchGen.h"

namespace facebook::velox::py {

namespace py = pybind11;

PyPlanNode::PyPlanNode(
    core::PlanNodePtr planNode,
    const std::shared_ptr<PyPlanContext>& planContext)
    : planNode_(std::move(planNode)), planContext_(planContext) {
  if (planNode_ == nullptr) {
    throw std::runtime_error("Velox plan node cannot be nullptr.");
  }
}

PyPlanBuilder::PyPlanBuilder(const std::shared_ptr<PyPlanContext>& planContext)
    : planContext_(
          planContext ? planContext : std::make_shared<PyPlanContext>()) {
  rootPool_ = memory::memoryManager()->addRootPool();
  leafPool_ = rootPool_->addLeafChild("py_plan_builder_pool");
  planBuilder_ = exec::test::PlanBuilder(
      planContext_->planNodeIdGenerator, leafPool_.get());
}

std::optional<PyPlanNode> PyPlanBuilder::planNode() const {
  if (planBuilder_.planNode() != nullptr) {
    return PyPlanNode(planBuilder_.planNode(), planContext_);
  }
  return std::nullopt;
}

PyPlanBuilder& PyPlanBuilder::tableWrite(
    const std::optional<PyFile>& outputFile,
    const std::optional<PyFile>& outputPath,
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

  if (!outputFile && !outputPath) {
    throw std::runtime_error(
        "Either outputFile or outputPath need to be specified.");
  }

  if (outputFile) {
    builder.outputFileName(outputFile->filePath())
        .fileFormat(outputFile->fileFormat());
  }

  // outputPath takes precedence and overwrites outputFile if both are
  // specified.
  if (outputPath) {
    builder.outputDirectoryPath(outputPath->filePath())
        .fileFormat(outputPath->fileFormat());
  }

  builder.connectorId(connectorId).endTableWriter();
  return *this;
}

PyPlanBuilder& PyPlanBuilder::tableScan(
    const PyType& outputSchema,
    const py::dict& aliases,
    const py::dict& subfields,
    const std::vector<std::string>& filters,
    const std::string& remainingFilter,
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

  // If there are filters to push down.
  if (!filters.empty()) {
    builder.subfieldFilters(filters);
  }

  // If there are remaining filters to push down.
  if (!remainingFilter.empty()) {
    builder.remainingFilter(remainingFilter);
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

  planContext_->scanFiles[planBuilder_.planNode()->id()] = std::move(splits);
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

PyPlanBuilder& PyPlanBuilder::hashJoin(
    const std::vector<std::string>& leftKeys,
    const std::vector<std::string>& rightKeys,
    const PyPlanNode& buildPlanSubtree,
    const std::vector<std::string>& output,
    const std::string& filter,
    core::JoinType joinType) {
  planBuilder_.hashJoin(
      leftKeys,
      rightKeys,
      buildPlanSubtree.planNode(),
      filter,
      output,
      joinType);
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

PyPlanBuilder& PyPlanBuilder::indexLookupJoin(
    const std::vector<std::string>& leftKeys,
    const std::vector<std::string>& rightKeys,
    const PyPlanNode& indexPlanSubtree,
    const std::vector<std::string>& output,
    core::JoinType joinType) {
  if (const auto tableScanNode =
          std::dynamic_pointer_cast<const core::TableScanNode>(
              indexPlanSubtree.planNode())) {
    planBuilder_.indexLookupJoin(
        leftKeys, rightKeys, tableScanNode, {}, output, joinType);
  } else {
    throw std::runtime_error(
        "Index Loop Join subtree must be a single TableScanNode.");
  }
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

  planContext_->scanFiles[planBuilder_.planNode()->id()] = std::move(splits);

  // If the user is specifying multiple parts, it's likely because they want to
  // write this exact number of files. Saving this as a config.
  planContext_->queryConfigs[core::QueryConfig::kTaskWriterCount] =
      std::to_string(numParts);
  return *this;
}

} // namespace facebook::velox::py

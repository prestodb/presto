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

#include "velox/exec/tests/utils/IndexLookupJoinTestBase.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

namespace fecebook::velox::exec::test {
using namespace facebook::velox::test;

namespace {
std::vector<std::string> appendMatchColumn(
    const std::vector<std::string> columns) {
  std::vector<std::string> resultColumns;
  resultColumns.reserve(columns.size() + 1);
  for (const auto& column : columns) {
    resultColumns.push_back(column);
  }
  resultColumns.push_back("__match__");
  return resultColumns;
}
} // namespace

RowTypePtr IndexLookupJoinTestBase::concat(
    const RowTypePtr& a,
    const RowTypePtr& b) {
  std::vector<std::string> names = a->names();
  std::vector<TypePtr> types = a->children();
  names.insert(names.end(), b->names().begin(), b->names().end());
  types.insert(types.end(), b->children().begin(), b->children().end());
  return ROW(std::move(names), std::move(types));
}

int IndexLookupJoinTestBase::getNumRows(const std::vector<int>& cardinalities) {
  int numRows{1};
  for (const auto& cardinality : cardinalities) {
    numRows *= cardinality;
  }
  return numRows;
}

std::vector<RowVectorPtr> IndexLookupJoinTestBase::generateProbeInput(
    size_t numBatches,
    size_t batchSize,
    size_t numDuplicateProbeRows,
    SequenceTableData& tableData,
    std::shared_ptr<memory::MemoryPool>& pool,
    const std::vector<std::string>& probeJoinKeys,
    bool hasNullKeys,
    const std::vector<std::string>& inColumns,
    const std::vector<std::pair<std::string, std::string>>& betweenColumns,
    std::optional<int> equalMatchPct,
    std::optional<int> inMatchPct,
    std::optional<int> betweenMatchPct) {
  VELOX_CHECK_LE(
      probeJoinKeys.size() + betweenColumns.size() + inColumns.size(),
      keyType_->size());
  std::vector<RowVectorPtr> probeInputs;
  probeInputs.reserve(numBatches);
  VectorFuzzer::Options opts;
  opts.vectorSize = batchSize * numDuplicateProbeRows;
  opts.allowSlice = false;
  // TODO: add nullable handling later.
  opts.nullRatio = hasNullKeys ? 0.1 : 0.0;
  VectorFuzzer fuzzer(opts, pool.get());
  for (int i = 0; i < numBatches; ++i) {
    probeInputs.push_back(fuzzer.fuzzInputRow(probeType_));
    // NOTE: index connector doesn't expect in condition column rray elements to
    // be null.
    if ((!inMatchPct.has_value() || tableData.keyData->size() == 0) &&
        hasNullKeys) {
      for (int i = 0; i < probeType_->size(); ++i) {
        const auto columnType = probeType_->childAt(i);
        if (columnType->isArray()) {
          opts.nullRatio = 0.0;
          VectorFuzzer vectorFuzzer(opts, pool.get());
          probeInputs.back()->childAt(i) = vectorFuzzer.fuzz(columnType);
          VELOX_CHECK(!probeInputs.back()->childAt(i)->mayHaveNulls());
          VELOX_CHECK_EQ(
              probeInputs.back()->childAt(i)->size(),
              probeInputs.back()->size());
        }
      }
    }
  }

  if (tableData.keyData->size() == 0) {
    return probeInputs;
  }

  const auto numTableRows = tableData.keyData->size();
  std::vector<FlatVectorPtr<int64_t>> tableKeyVectors;
  for (int i = 0; i < probeJoinKeys.size(); ++i) {
    auto keyVector = tableData.keyData->childAt(i);
    keyVector->loadedVector();
    BaseVector::flattenVector(keyVector);
    tableKeyVectors.push_back(
        std::dynamic_pointer_cast<FlatVector<int64_t>>(keyVector));
  }

  if (equalMatchPct.has_value()) {
    VELOX_CHECK_GE(equalMatchPct.value(), 0);
    VELOX_CHECK_LE(equalMatchPct.value(), 100);
    for (int i = 0; i < numBatches; ++i) {
      std::vector<FlatVectorPtr<int64_t>> probeKeyVectors;
      for (int j = 0; j < probeJoinKeys.size(); ++j) {
        probeKeyVectors.push_back(BaseVector::create<FlatVector<int64_t>>(
            probeType_->findChild(probeJoinKeys[j]),
            probeInputs[i]->size(),
            pool.get()));
      }
      for (int row = 0; row < probeInputs[i]->size();
           row += numDuplicateProbeRows) {
        const auto hit =
            (folly::Random::rand32(rng_) % 100) < equalMatchPct.value();
        if (hit) {
          const auto matchKeyRow = folly::Random::rand32(numTableRows, rng_);
          for (int j = 0; j < probeJoinKeys.size(); ++j) {
            for (int k = 0; k < numDuplicateProbeRows; ++k) {
              if (probeKeyVectors[j]->isNullAt(row + k)) {
                continue;
              }
              probeKeyVectors[j]->set(
                  row + k, tableKeyVectors[j]->valueAt(matchKeyRow));
            }
          }
        } else {
          for (int j = 0; j < probeJoinKeys.size(); ++j) {
            const auto randomValue = folly::Random::rand32(rng_) % 4096;
            for (int k = 0; k < numDuplicateProbeRows; ++k) {
              if (probeKeyVectors[j]->isNullAt(row + k)) {
                continue;
              }
              probeKeyVectors[j]->set(
                  row + k, tableData.maxKeys[j] + 1 + randomValue);
            }
          }
        }
      }
      for (int j = 0; j < probeJoinKeys.size(); ++j) {
        probeInputs[i]->childAt(j) = probeKeyVectors[j];
      }
    }
  }

  if (inMatchPct.has_value()) {
    VELOX_CHECK(!inColumns.empty());
    VELOX_CHECK_GE(inMatchPct.value(), 0);
    VELOX_CHECK_LE(inMatchPct.value(), 100);
    for (int i = 0; i < inColumns.size(); ++i) {
      const auto inColumnName = inColumns[i];
      const auto inColumnChannel = probeType_->getChildIdx(inColumnName);
      auto inColumnType = std::dynamic_pointer_cast<const ArrayType>(
          probeType_->childAt(inColumnChannel));
      VELOX_CHECK_NOT_NULL(inColumnType);
      const auto tableKeyChannel = probeJoinKeys.size() + i;
      VELOX_CHECK(keyType_->childAt(tableKeyChannel)
                      ->equivalent(*inColumnType->elementType()));
      const auto minValue = !inMatchPct.has_value()
          ? tableData.minKeys[tableKeyChannel] - 1
          : tableData.minKeys[tableKeyChannel];
      const auto maxValue = !inMatchPct.has_value()
          ? minValue
          : tableData.minKeys[tableKeyChannel] +
              (tableData.maxKeys[tableKeyChannel] -
               tableData.minKeys[tableKeyChannel]) *
                  inMatchPct.value() / 100;
      for (int i = 0; i < numBatches; ++i) {
        probeInputs[i]->childAt(inColumnChannel) = makeArrayVector<int64_t>(
            probeInputs[i]->size(),
            [&](auto row) -> vector_size_t { return maxValue - minValue + 1; },
            [&](auto /*unused*/, auto index) { return minValue + index; },
            [&](auto row) {
              return probeInputs[i]->childAt(inColumnChannel)->isNullAt(row);
            });
      }
    }
  }

  if (betweenMatchPct.has_value()) {
    VELOX_CHECK(!betweenColumns.empty());
    VELOX_CHECK_GE(betweenMatchPct.value(), 0);
    VELOX_CHECK_LE(betweenMatchPct.value(), 100);
    for (int i = 0; i < betweenColumns.size(); ++i) {
      const auto tableKeyChannel = probeJoinKeys.size() + i;
      const auto betweenColumn = betweenColumns[i];
      const auto lowerBoundColumn = betweenColumn.first;
      std::optional<int32_t> lowerBoundChannel;
      if (!lowerBoundColumn.empty()) {
        lowerBoundChannel = probeType_->getChildIdx(lowerBoundColumn);
        VELOX_CHECK(probeType_->childAt(lowerBoundChannel.value())
                        ->equivalent(*keyType_->childAt(tableKeyChannel)));
      }
      const auto upperBoundColumn = betweenColumn.first;
      std::optional<int32_t> upperBoundChannel;
      if (!upperBoundColumn.empty()) {
        upperBoundChannel = probeType_->getChildIdx(upperBoundColumn);
        VELOX_CHECK(probeType_->childAt(upperBoundChannel.value())
                        ->equivalent(*keyType_->childAt(tableKeyChannel)));
      }
      for (int i = 0; i < numBatches; ++i) {
        if (lowerBoundChannel.has_value()) {
          probeInputs[i]->childAt(lowerBoundChannel.value()) =
              makeFlatVector<int64_t>(
                  probeInputs[i]->size(),
                  [&](auto /*unused*/) {
                    return tableData.minKeys[tableKeyChannel];
                  },
                  [&](auto row) {
                    return probeInputs[i]
                        ->childAt(lowerBoundChannel.value())
                        ->isNullAt(row);
                  });
        }
        const auto upperBoundColumn = betweenColumn.second;
        if (upperBoundChannel.has_value()) {
          probeInputs[i]->childAt(upperBoundChannel.value()) =
              makeFlatVector<int64_t>(
                  probeInputs[i]->size(),
                  [&](auto /*unused*/) -> int64_t {
                    if (betweenMatchPct.value() == 0) {
                      return tableData.minKeys[tableKeyChannel] - 1;
                    } else {
                      return tableData.minKeys[tableKeyChannel] +
                          (tableData.maxKeys[tableKeyChannel] -
                           tableData.minKeys[tableKeyChannel]) *
                          betweenMatchPct.value() / 100;
                    }
                  },
                  [&](auto row) {
                    return probeInputs[i]
                        ->childAt(upperBoundChannel.value())
                        ->isNullAt(row);
                  });
        }
      }
    }
  }
  return probeInputs;
}

PlanNodePtr IndexLookupJoinTestBase::makeLookupPlan(
    const std::shared_ptr<PlanNodeIdGenerator>& planNodeIdGenerator,
    TableScanNodePtr indexScanNode,
    const std::vector<RowVectorPtr>& probeVectors,
    const std::vector<std::string>& leftKeys,
    const std::vector<std::string>& rightKeys,
    const std::vector<std::string>& joinConditions,
    bool includeMatchColumn,
    JoinType joinType,
    const std::vector<std::string>& outputColumns,
    PlanNodeId& joinNodeId) {
  VELOX_CHECK_EQ(leftKeys.size(), rightKeys.size());
  VELOX_CHECK_LE(leftKeys.size(), keyType_->size());
  return PlanBuilder(planNodeIdGenerator, pool_.get())
      .values(probeVectors)
      .startIndexLookupJoin()
      .leftKeys(leftKeys)
      .rightKeys(rightKeys)
      .indexSource(indexScanNode)
      .joinConditions(joinConditions)
      .includeMatchColumn(includeMatchColumn)
      .outputLayout(
          includeMatchColumn ? appendMatchColumn(outputColumns) : outputColumns)
      .joinType(joinType)
      .endIndexLookupJoin()
      .capturePlanNodeId(joinNodeId)
      .planNode();
}

PlanNodePtr IndexLookupJoinTestBase::makeLookupPlan(
    const std::shared_ptr<PlanNodeIdGenerator>& planNodeIdGenerator,
    TableScanNodePtr indexScanNode,
    const std::vector<std::string>& leftKeys,
    const std::vector<std::string>& rightKeys,
    const std::vector<std::string>& joinConditions,
    bool includeMatchColumn,
    JoinType joinType,
    const std::vector<std::string>& outputColumns) {
  VELOX_CHECK_EQ(leftKeys.size(), rightKeys.size());
  VELOX_CHECK_LE(leftKeys.size(), keyType_->size());
  return PlanBuilder(planNodeIdGenerator, pool_.get())
      .startTableScan()
      .outputType(probeType_)
      .endTableScan()
      .captureScanNodeId(probeScanNodeId_)
      .startIndexLookupJoin()
      .leftKeys(leftKeys)
      .rightKeys(rightKeys)
      .indexSource(indexScanNode)
      .joinConditions(joinConditions)
      .includeMatchColumn(includeMatchColumn)
      .outputLayout(
          includeMatchColumn ? appendMatchColumn(outputColumns) : outputColumns)
      .joinType(joinType)
      .endIndexLookupJoin()
      .capturePlanNodeId(joinNodeId_)
      .planNode();
}

void IndexLookupJoinTestBase::createDuckDbTable(
    const std::string& tableName,
    const std::vector<RowVectorPtr>& data) {
  // Change each column with prefix 'c' to simplify the duckdb table
  // column naming.
  std::vector<std::string> columnNames;
  columnNames.reserve(data[0]->type()->size());
  for (int i = 0; i < data[0]->type()->size(); ++i) {
    columnNames.push_back(fmt::format("c{}", i));
  }
  std::vector<RowVectorPtr> duckDbInputs;
  duckDbInputs.reserve(data.size());
  for (const auto& dataVector : data) {
    duckDbInputs.emplace_back(
        makeRowVector(columnNames, dataVector->children()));
  }
  duckDbQueryRunner_.createTable(tableName, duckDbInputs);
}

TableScanNodePtr IndexLookupJoinTestBase::makeIndexScanNode(
    const std::shared_ptr<PlanNodeIdGenerator>& planNodeIdGenerator,
    const connector::ConnectorTableHandlePtr& indexTableHandle,
    const RowTypePtr& outputType,
    const connector::ColumnHandleMap& assignments) {
  auto planBuilder = PlanBuilder(planNodeIdGenerator, pool_.get());
  auto indexTableScan = std::dynamic_pointer_cast<const TableScanNode>(
      PlanBuilder::TableScanBuilder(planBuilder)
          .tableHandle(indexTableHandle)
          .outputType(outputType)
          .assignments(assignments)
          .endTableScan()
          .capturePlanNodeId(indexScanNodeId_)
          .planNode());
  VELOX_CHECK_NOT_NULL(indexTableScan);
  return indexTableScan;
}

void IndexLookupJoinTestBase::generateIndexTableData(
    const std::vector<int>& keyCardinalities,
    SequenceTableData& tableData,
    std::shared_ptr<memory::MemoryPool>& pool) {
  VELOX_CHECK_EQ(keyCardinalities.size(), keyType_->size());
  const auto numRows = getNumRows(keyCardinalities);
  VectorFuzzer::Options opts;
  opts.vectorSize = numRows;
  opts.nullRatio = 0.0;
  opts.allowSlice = false;
  VectorFuzzer fuzzer(opts, pool.get());

  tableData.keyData = fuzzer.fuzzInputFlatRow(keyType_);
  tableData.valueData = fuzzer.fuzzInputFlatRow(valueType_);

  VELOX_CHECK_EQ(numRows, tableData.keyData->size());
  tableData.maxKeys.resize(keyType_->size());
  tableData.minKeys.resize(keyType_->size());
  // Set the key column vector to the same value to easy testing with
  // specified match ratio.
  for (int i = keyType_->size() - 1, numRepeats = 1; i >= 0;
       numRepeats *= keyCardinalities[i--]) {
    int64_t minKey = std::numeric_limits<int64_t>::max();
    int64_t maxKey = std::numeric_limits<int64_t>::min();
    int numKeys = keyCardinalities[i];
    tableData.keyData->childAt(i) =
        makeFlatVector<int64_t>(tableData.keyData->size(), [&](auto row) {
          const int64_t keyValue = 1 + (row / numRepeats) % numKeys;
          minKey = std::min(minKey, keyValue);
          maxKey = std::max(maxKey, keyValue);
          return keyValue;
        });
    tableData.minKeys[i] = minKey;
    tableData.maxKeys[i] = maxKey;
  }

  std::vector<VectorPtr> tableColumns;
  VELOX_CHECK_EQ(tableType_->size(), keyType_->size() + valueType_->size());
  tableColumns.reserve(tableType_->size());
  for (auto i = 0; i < keyType_->size(); ++i) {
    tableColumns.push_back(tableData.keyData->childAt(i));
  }
  for (auto i = 0; i < valueType_->size(); ++i) {
    tableColumns.push_back(tableData.valueData->childAt(i));
  }
  tableData.tableData = makeRowVector(tableType_->names(), tableColumns);
}

RowTypePtr IndexLookupJoinTestBase::makeScanOutputType(
    std::vector<std::string> outputNames) {
  std::vector<TypePtr> types;
  for (int i = 0; i < outputNames.size(); ++i) {
    if (valueType_->getChildIdxIfExists(outputNames[i]).has_value()) {
      types.push_back(valueType_->findChild(outputNames[i]));
      continue;
    }
    types.push_back(keyType_->findChild(outputNames[i]));
  }
  return ROW(std::move(outputNames), std::move(types));
}

bool IndexLookupJoinTestBase::isFilter(const std::string& conditionSql) const {
  const auto inputType = concat(keyType_, probeType_);
  return PlanBuilder::parseIndexJoinCondition(
             conditionSql, inputType, pool_.get())
      ->isFilter();
}

std::shared_ptr<Task> IndexLookupJoinTestBase::runLookupQuery(
    const PlanNodePtr& plan,
    int numPrefetchBatches,
    const std::string& duckDbVefifySql) {
  return AssertQueryBuilder(duckDbQueryRunner_)
      .plan(plan)
      .config(
          QueryConfig::kIndexLookupJoinMaxPrefetchBatches,
          std::to_string(numPrefetchBatches))
      .assertResults(duckDbVefifySql);
}

std::shared_ptr<Task> IndexLookupJoinTestBase::runLookupQuery(
    const PlanNodePtr& plan,
    const std::vector<std::shared_ptr<TempFilePath>>& probeFiles,
    bool serialExecution,
    bool barrierExecution,
    int maxOutputRows,
    int numPrefetchBatches,
    const std::string& duckDbVefifySql) {
  return AssertQueryBuilder(duckDbQueryRunner_)
      .plan(plan)
      .splits(probeScanNodeId_, makeHiveConnectorSplits(probeFiles))
      .serialExecution(serialExecution)
      .barrierExecution(barrierExecution)
      .config(QueryConfig::kMaxOutputBatchRows, std::to_string(maxOutputRows))
      .config(
          QueryConfig::kIndexLookupJoinMaxPrefetchBatches,
          std::to_string(numPrefetchBatches))
      .assertResults(duckDbVefifySql);
}

void IndexLookupJoinTestBase::verifyResultWithMatchColumn(
    const PlanNodePtr& planWithoutMatchColumn,
    const PlanNodeId& probeScanNodeIdWithoutMatchColumn,
    const PlanNodePtr& planWithMatchColumn,
    const PlanNodeId& probeScanNodeIdWithMatchColumn,
    const std::vector<std::shared_ptr<TempFilePath>>& probeFiles) {
  VectorPtr expectedResult = AssertQueryBuilder(duckDbQueryRunner_)
                                 .plan(planWithoutMatchColumn)
                                 .splits(
                                     probeScanNodeIdWithoutMatchColumn,
                                     makeHiveConnectorSplits(probeFiles))
                                 .copyResults(pool());
  BaseVector::flattenVector(expectedResult);

  VectorPtr resultWithMatchColumn = AssertQueryBuilder(duckDbQueryRunner_)
                                        .plan(planWithMatchColumn)
                                        .splits(
                                            probeScanNodeIdWithMatchColumn,
                                            makeHiveConnectorSplits(probeFiles))
                                        .copyResults(pool());
  BaseVector::flattenVector(resultWithMatchColumn);
  auto rowResultWithMatchMatchColumn =
      std::dynamic_pointer_cast<RowVector>(resultWithMatchColumn);
  const auto resultWithMatchColumnType =
      std::dynamic_pointer_cast<const RowType>(
          rowResultWithMatchMatchColumn->type());
  std::vector<VectorPtr> childVectors;
  std::unordered_set<std::string> lookupColumnNameSet(
      valueType_->names().begin(), valueType_->names().end());
  std::vector<VectorPtr> lookupColumnVectors;
  for (int i = 0; i < rowResultWithMatchMatchColumn->childrenSize() - 1; ++i) {
    childVectors.push_back(rowResultWithMatchMatchColumn->childAt(i));
    if (lookupColumnNameSet.contains(resultWithMatchColumnType->nameOf(i))) {
      lookupColumnVectors.push_back(rowResultWithMatchMatchColumn->childAt(i));
    }
  }
  auto resultWithoutMatchColumn = makeRowVector(childVectors);
  assertEqualVectors(expectedResult, resultWithoutMatchColumn);
  // Verify the match column if it is expected.
  const auto matchColumn =
      rowResultWithMatchMatchColumn
          ->childAt(rowResultWithMatchMatchColumn->childrenSize() - 1)
          ->asFlatVector<bool>();
  for (int i = 0; i < resultWithMatchColumn->size(); ++i) {
    const bool match = matchColumn->valueAt(i);
    if (match) {
      for (const auto& lookupColumnVector : lookupColumnVectors) {
        ASSERT_FALSE(lookupColumnVector->isNullAt(i));
      }
    } else {
      for (const auto& lookupColumnVector : lookupColumnVectors) {
        ASSERT_TRUE(lookupColumnVector->isNullAt(i));
      }
    }
  }
}

std::vector<std::shared_ptr<TempFilePath>>
IndexLookupJoinTestBase::createProbeFiles(
    const std::vector<RowVectorPtr>& probeVectors) {
  std::vector<std::shared_ptr<TempFilePath>> probeFiles;
  probeFiles.reserve(probeVectors.size());
  for (auto i = 0; i < probeVectors.size(); ++i) {
    probeFiles.push_back(TempFilePath::create());
  }
  writeToFiles(toFilePaths(probeFiles), probeVectors);
  return probeFiles;
}
} // namespace fecebook::velox::exec::test

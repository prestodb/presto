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
#include "velox/exec/tests/utils/PlanBuilder.h"

namespace fecebook::velox::exec::test {

facebook::velox::RowTypePtr IndexLookupJoinTestBase::concat(
    const facebook::velox::RowTypePtr& a,
    const facebook::velox::RowTypePtr& b) {
  std::vector<std::string> names = a->names();
  std::vector<facebook::velox::TypePtr> types = a->children();
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

std::vector<facebook::velox::RowVectorPtr>
IndexLookupJoinTestBase::generateProbeInput(
    size_t numBatches,
    size_t batchSize,
    size_t numDuplicateProbeRows,
    SequenceTableData& tableData,
    std::shared_ptr<facebook::velox::memory::MemoryPool>& pool,
    const std::vector<std::string>& probeJoinKeys,
    const std::vector<std::string> inColumns,
    const std::vector<std::pair<std::string, std::string>>& betweenColumns,
    std::optional<int> equalMatchPct,
    std::optional<int> inMatchPct,
    std::optional<int> betweenMatchPct) {
  VELOX_CHECK_LE(
      probeJoinKeys.size() + betweenColumns.size() + inColumns.size(),
      keyType_->size());
  std::vector<facebook::velox::RowVectorPtr> probeInputs;
  probeInputs.reserve(numBatches);
  facebook::velox::VectorFuzzer::Options opts;
  opts.vectorSize = batchSize * numDuplicateProbeRows;
  opts.allowSlice = false;
  // TODO: add nullable handling later.
  opts.nullRatio = 0.0;
  facebook::velox::VectorFuzzer fuzzer(opts, pool.get());
  for (int i = 0; i < numBatches; ++i) {
    probeInputs.push_back(fuzzer.fuzzInputRow(probeType_));
  }

  if (tableData.keyData->size() == 0) {
    return probeInputs;
  }

  const auto numTableRows = tableData.keyData->size();
  std::vector<facebook::velox::FlatVectorPtr<int64_t>> tableKeyVectors;
  for (int i = 0; i < probeJoinKeys.size(); ++i) {
    auto keyVector = tableData.keyData->childAt(i);
    keyVector->loadedVector();
    facebook::velox::BaseVector::flattenVector(keyVector);
    tableKeyVectors.push_back(
        std::dynamic_pointer_cast<facebook::velox::FlatVector<int64_t>>(
            keyVector));
  }

  if (equalMatchPct.has_value()) {
    VELOX_CHECK_GE(equalMatchPct.value(), 0);
    VELOX_CHECK_LE(equalMatchPct.value(), 100);
    for (int i = 0, totalRows = 0; i < numBatches; ++i) {
      std::vector<facebook::velox::FlatVectorPtr<int64_t>> probeKeyVectors;
      for (int j = 0; j < probeJoinKeys.size(); ++j) {
        probeKeyVectors.push_back(facebook::velox::BaseVector::create<
                                  facebook::velox::FlatVector<int64_t>>(
            probeType_->findChild(probeJoinKeys[j]),
            probeInputs[i]->size(),
            pool.get()));
      }
      for (int row = 0; row < probeInputs[i]->size();
           row += numDuplicateProbeRows, totalRows += numDuplicateProbeRows) {
        if ((totalRows / numDuplicateProbeRows) % 100 < equalMatchPct.value()) {
          const auto matchKeyRow = folly::Random::rand64(numTableRows);
          for (int j = 0; j < probeJoinKeys.size(); ++j) {
            for (int k = 0; k < numDuplicateProbeRows; ++k) {
              probeKeyVectors[j]->set(
                  row + k, tableKeyVectors[j]->valueAt(matchKeyRow));
            }
          }
        } else {
          for (int j = 0; j < probeJoinKeys.size(); ++j) {
            const auto randomValue = folly::Random::rand32() % 4096;
            for (int k = 0; k < numDuplicateProbeRows; ++k) {
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
      auto inColumnType =
          std::dynamic_pointer_cast<const facebook::velox::ArrayType>(
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
            [&](auto row) -> facebook::velox::vector_size_t {
              return maxValue - minValue + 1;
            },
            [&](auto /*unused*/, auto index) { return minValue + index; });
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
                  probeInputs[i]->size(), [&](auto /*unused*/) {
                    return tableData.minKeys[tableKeyChannel];
                  });
        }
        const auto upperBoundColumn = betweenColumn.second;
        if (upperBoundChannel.has_value()) {
          probeInputs[i]->childAt(upperBoundChannel.value()) =
              makeFlatVector<int64_t>(
                  probeInputs[i]->size(), [&](auto /*unused*/) -> int64_t {
                    if (betweenMatchPct.value() == 0) {
                      return tableData.minKeys[tableKeyChannel] - 1;
                    } else {
                      return tableData.minKeys[tableKeyChannel] +
                          (tableData.maxKeys[tableKeyChannel] -
                           tableData.minKeys[tableKeyChannel]) *
                          betweenMatchPct.value() / 100;
                    }
                  });
        }
      }
    }
  }
  return probeInputs;
}

facebook::velox::core::PlanNodePtr IndexLookupJoinTestBase::makeLookupPlan(
    const std::shared_ptr<facebook::velox::core::PlanNodeIdGenerator>&
        planNodeIdGenerator,
    facebook::velox::core::TableScanNodePtr indexScanNode,
    const std::vector<facebook::velox::RowVectorPtr>& probeVectors,
    const std::vector<std::string>& leftKeys,
    const std::vector<std::string>& rightKeys,
    const std::vector<std::string>& joinConditions,
    facebook::velox::core::JoinType joinType,
    const std::vector<std::string>& outputColumns,
    facebook::velox::core::PlanNodeId& joinNodeId) {
  VELOX_CHECK_EQ(leftKeys.size(), rightKeys.size());
  VELOX_CHECK_LE(leftKeys.size(), keyType_->size());
  return facebook::velox::exec::test::PlanBuilder(
             planNodeIdGenerator, pool_.get())
      .values(probeVectors)
      .indexLookupJoin(
          leftKeys,
          rightKeys,
          indexScanNode,
          joinConditions,
          outputColumns,
          joinType)
      .capturePlanNodeId(joinNodeId)
      .planNode();
}

void IndexLookupJoinTestBase::createDuckDbTable(
    const std::string& tableName,
    const std::vector<facebook::velox::RowVectorPtr>& data) {
  // Change each column with prefix 'c' to simplify the duckdb table
  // column naming.
  std::vector<std::string> columnNames;
  columnNames.reserve(data[0]->type()->size());
  for (int i = 0; i < data[0]->type()->size(); ++i) {
    columnNames.push_back(fmt::format("c{}", i));
  }
  std::vector<facebook::velox::RowVectorPtr> duckDbInputs;
  duckDbInputs.reserve(data.size());
  for (const auto& dataVector : data) {
    duckDbInputs.emplace_back(
        makeRowVector(columnNames, dataVector->children()));
  }
  duckDbQueryRunner_.createTable(tableName, duckDbInputs);
}

facebook::velox::core::TableScanNodePtr
IndexLookupJoinTestBase::makeIndexScanNode(
    const std::shared_ptr<facebook::velox::core::PlanNodeIdGenerator>&
        planNodeIdGenerator,
    const std::shared_ptr<facebook::velox::connector::ConnectorTableHandle>
        indexTableHandle,
    const facebook::velox::RowTypePtr& outputType,
    facebook::velox::core::PlanNodeId& scanNodeId,
    std::unordered_map<
        std::string,
        std::shared_ptr<facebook::velox::connector::ColumnHandle>>&
        assignments) {
  auto planBuilder = facebook::velox::exec::test::PlanBuilder(
      planNodeIdGenerator, pool_.get());
  auto indexTableScan =
      std::dynamic_pointer_cast<const facebook::velox::core::TableScanNode>(
          facebook::velox::exec::test::PlanBuilder::TableScanBuilder(
              planBuilder)
              .tableHandle(indexTableHandle)
              .outputType(outputType)
              .assignments(assignments)
              .endTableScan()
              .capturePlanNodeId(scanNodeId)
              .planNode());
  VELOX_CHECK_NOT_NULL(indexTableScan);
  return indexTableScan;
}

void IndexLookupJoinTestBase::generateIndexTableData(
    const std::vector<int>& keyCardinalities,
    SequenceTableData& tableData,
    std::shared_ptr<facebook::velox::memory::MemoryPool>& pool) {
  VELOX_CHECK_EQ(keyCardinalities.size(), keyType_->size());
  const auto numRows = getNumRows(keyCardinalities);
  facebook::velox::VectorFuzzer::Options opts;
  opts.vectorSize = numRows;
  opts.nullRatio = 0.0;
  opts.allowSlice = false;
  facebook::velox::VectorFuzzer fuzzer(opts, pool.get());

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

  std::vector<facebook::velox::VectorPtr> tableColumns;
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

facebook::velox::RowTypePtr IndexLookupJoinTestBase::makeScanOutputType(
    std::vector<std::string> outputNames) {
  std::vector<facebook::velox::TypePtr> types;
  for (int i = 0; i < outputNames.size(); ++i) {
    if (valueType_->getChildIdxIfExists(outputNames[i]).has_value()) {
      types.push_back(valueType_->findChild(outputNames[i]));
      continue;
    }
    types.push_back(keyType_->findChild(outputNames[i]));
  }
  return facebook::velox::ROW(std::move(outputNames), std::move(types));
}

bool IndexLookupJoinTestBase::isFilter(const std::string& conditionSql) const {
  const auto inputType = concat(keyType_, probeType_);
  return facebook::velox::exec::test::PlanBuilder::parseIndexJoinCondition(
             conditionSql, inputType, pool_.get())
      ->isFilter();
}

std::shared_ptr<facebook::velox::exec::Task>
IndexLookupJoinTestBase::runLookupQuery(
    const facebook::velox::core::PlanNodePtr& plan,
    int numPrefetchBatches,
    const std::string& duckDbVefifySql) {
  return facebook::velox::exec::test::AssertQueryBuilder(duckDbQueryRunner_)
      .plan(plan)
      .config(
          facebook::velox::core::QueryConfig::
              kIndexLookupJoinMaxPrefetchBatches,
          std::to_string(numPrefetchBatches))
      .assertResults(duckDbVefifySql);
}
} // namespace fecebook::velox::exec::test

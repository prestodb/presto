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

#include "velox/exec/tests/utils/LocalRunnerTestBase.h"
namespace facebook::velox::exec::test {

void LocalRunnerTestBase::SetUp() {
  HiveConnectorTestBase::SetUp();
  exec::ExchangeSource::factories().clear();
  exec::ExchangeSource::registerFactory(createLocalExchangeSource);
  ensureTestData();
}

std::shared_ptr<core::QueryCtx> LocalRunnerTestBase::makeQueryCtx(
    const std::string& queryId,
    memory::MemoryPool* rootPool) {
  auto config = config_;
  auto hiveConfig = hiveConfig_;
  std::unordered_map<std::string, std::shared_ptr<config::ConfigBase>>
      connectorConfigs;
  auto copy = hiveConfig_;
  connectorConfigs[kHiveConnectorId] =
      std::make_shared<config::ConfigBase>(std::move(copy));

  return core::QueryCtx::create(
      schemaExecutor_.get(),
      core::QueryConfig(config),
      std::move(connectorConfigs),
      cache::AsyncDataCache::getInstance(),
      rootPool->shared_from_this(),
      nullptr,
      queryId);
}

void LocalRunnerTestBase::ensureTestData() {
  if (!files_) {
    makeTables(testTables_, files_);
  }
  makeSchema();
  splitSourceFactory_ =
      std::make_shared<runner::LocalSplitSourceFactory>(schema_, 2);
}

void LocalRunnerTestBase::makeSchema() {
  auto schemaQueryCtx = makeQueryCtx("schema", rootPool_.get());
  common::SpillConfig spillConfig;
  common::PrefixSortConfig prefixSortConfig(100, 130);
  auto leafPool = schemaQueryCtx->pool()->addLeafChild("schemaReader");
  auto connectorQueryCtx = std::make_shared<connector::ConnectorQueryCtx>(
      leafPool.get(),
      schemaQueryCtx->pool(),
      schemaQueryCtx->connectorSessionProperties(kHiveConnectorId),
      &spillConfig,
      prefixSortConfig,
      std::make_unique<exec::SimpleExpressionEvaluator>(
          schemaQueryCtx.get(), schemaPool_.get()),
      schemaQueryCtx->cache(),
      "scan_for_schema",
      "schema",
      "N/a",
      0,
      schemaQueryCtx->queryConfig().sessionTimezone());
  auto connector = connector::getConnector(kHiveConnectorId);
  schema_ = std::make_shared<runner::LocalSchema>(
      files_->getPath(),
      dwio::common::FileFormat::DWRF,
      reinterpret_cast<velox::connector::hive::HiveConnector*>(connector.get()),
      connectorQueryCtx);
}

void LocalRunnerTestBase::makeTables(
    std::vector<TableSpec> specs,
    std::shared_ptr<TempDirectoryPath>& directory) {
  directory = exec::test::TempDirectoryPath::create();
  for (auto& spec : specs) {
    auto tablePath = fmt::format("{}/{}", directory->getPath(), spec.name);
    auto fs = filesystems::getFileSystem(tablePath, {});
    fs->mkdir(tablePath);
    for (auto i = 0; i < spec.numFiles; ++i) {
      auto vectors = HiveConnectorTestBase::makeVectors(
          spec.columns, spec.numVectorsPerFile, spec.rowsPerVector);
      if (spec.customizeData) {
        for (auto& vector : vectors) {
          spec.customizeData(vector);
        }
      }
      writeToFile(fmt::format("{}/f{}", tablePath, i), vectors);
    }
  }
}

} // namespace facebook::velox::exec::test

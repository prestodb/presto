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

#include "velox/experimental/cudf/connectors/hive/CudfHiveConnector.h"
#include "velox/experimental/cudf/exec/ToCudf.h"
#include "velox/experimental/cudf/exec/VeloxCudfInterop.h"
#include "velox/experimental/cudf/tests/utils/CudfHiveConnectorTestBase.h"

#include "velox/common/base/Exceptions.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/file/tests/FaultyFileSystem.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/dwio/common/FileSink.h"
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/exec/Driver.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"

#include <cudf/io/parquet.hpp>
#include <cudf/io/types.hpp>
#include <cudf/table/table.hpp>
#include <cudf/table/table_view.hpp>

#include <functional>
#include <string>

namespace facebook::velox::cudf_velox::exec::test {

namespace {

void fillColumnNames(
    cudf::io::table_input_metadata& tableMeta,
    const std::string& prefix) {
  // Fill unnamed columns' names in cudf table_meta
  std::function<void(cudf::io::column_in_metadata&, std::string)>
      addDefaultName =
          [&](cudf::io::column_in_metadata& colMeta, std::string defaultName) {
            if (colMeta.get_name().empty()) {
              colMeta.set_name(defaultName);
            }
            for (int32_t i = 0; i < colMeta.num_children(); ++i) {
              addDefaultName(
                  colMeta.child(i), fmt::format("{}_{}", defaultName, i));
            }
          };
  for (int32_t i = 0; i < tableMeta.column_metadata.size(); ++i) {
    addDefaultName(tableMeta.column_metadata[i], prefix + std::to_string(i));
  }
}

} // namespace

using facebook::velox::connector::hive::HiveConnectorFactory;

CudfHiveConnectorTestBase::CudfHiveConnectorTestBase() {
  filesystems::registerLocalFileSystem();
  tests::utils::registerFaultyFileSystem();
}

void CudfHiveConnectorTestBase::SetUp() {
  OperatorTestBase::SetUp();

  // Register cudf to enable the CudfDatasource creation from CudfHiveConnector
  facebook::velox::cudf_velox::registerCudf();

  // Register Hive connector
  facebook::velox::cudf_velox::connector::hive::CudfHiveConnectorFactory
      factory;
  auto hiveConnector = factory.newConnector(
      kCudfHiveConnectorId,
      std::make_shared<facebook::velox::config::ConfigBase>(
          std::unordered_map<std::string, std::string>()),
      ioExecutor_.get());
  facebook::velox::connector::registerConnector(hiveConnector);
  dwio::common::registerFileSinks();
}

void CudfHiveConnectorTestBase::TearDown() {
  // Make sure all pending loads are finished or cancelled before unregister
  // connector.
  ioExecutor_.reset();
  facebook::velox::connector::unregisterConnector(kCudfHiveConnectorId);
  facebook::velox::connector::unregisterConnectorFactory(
      HiveConnectorFactory::kHiveConnectorName);
  facebook::velox::cudf_velox::unregisterCudf();
  OperatorTestBase::TearDown();
}

void CudfHiveConnectorTestBase::resetCudfHiveConnector(
    const std::shared_ptr<const facebook::velox::config::ConfigBase>& config) {
  facebook::velox::connector::unregisterConnector(kCudfHiveConnectorId);

  facebook::velox::cudf_velox::connector::hive::CudfHiveConnectorFactory
      factory;
  auto hiveConnector =
      factory.newConnector(kCudfHiveConnectorId, config, ioExecutor_.get());
  facebook::velox::connector::registerConnector(hiveConnector);
}

std::vector<RowVectorPtr> CudfHiveConnectorTestBase::makeVectors(
    const RowTypePtr& rowType,
    int32_t numVectors,
    int32_t rowsPerVector) {
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < numVectors; ++i) {
    auto vector = std::dynamic_pointer_cast<RowVector>(
        velox::test::BatchMaker::createBatch(rowType, rowsPerVector, *pool_));
    vectors.push_back(vector);
  }
  return vectors;
}

std::shared_ptr<facebook::velox::exec::Task>
CudfHiveConnectorTestBase::assertQuery(
    const core::PlanNodePtr& plan,
    const std::vector<
        std::shared_ptr<facebook::velox::exec::test::TempFilePath>>& filePaths,
    const std::string& duckDbSql) {
  return OperatorTestBase::assertQuery(
      plan, makeCudfHiveConnectorSplits(filePaths), duckDbSql);
}

std::shared_ptr<facebook::velox::exec::Task>
CudfHiveConnectorTestBase::assertQuery(
    const facebook::velox::core::PlanNodePtr& plan,
    const std::vector<
        std::shared_ptr<facebook::velox::connector::ConnectorSplit>>& splits,
    const std::string& duckDbSql,
    const int32_t numPrefetchSplit) {
  return facebook::velox::exec::test::AssertQueryBuilder(
             plan, duckDbQueryRunner_)
      .config(
          facebook::velox::core::QueryConfig::kMaxSplitPreloadPerDriver,
          std::to_string(numPrefetchSplit))
      .splits(splits)
      .assertResults(duckDbSql);
}

std::vector<std::shared_ptr<facebook::velox::exec::test::TempFilePath>>
CudfHiveConnectorTestBase::makeFilePaths(int count) {
  std::vector<std::shared_ptr<facebook::velox::exec::test::TempFilePath>>
      filePaths;
  filePaths.reserve(count);
  for (auto i = 0; i < count; ++i) {
    filePaths.emplace_back(facebook::velox::exec::test::TempFilePath::create());
  }
  return filePaths;
}

void CudfHiveConnectorTestBase::writeToFile(
    const std::string& filePath,
    const std::vector<RowVectorPtr>& vectors,
    std::string prefix) {
  // Convert all RowVectorPtrs to cudf tables
  std::vector<std::unique_ptr<cudf::table>> cudfTables;
  cudfTables.reserve(vectors.size());
  for (const auto& vector : vectors) {
    VELOX_CHECK_NOT_NULL(vector);
    if (vector->size()) {
      auto stream = cudf::get_default_stream();
      auto cudfTable = with_arrow::toCudfTable(vector, vector->pool(), stream);
      stream.synchronize();
      cudfTables.emplace_back(std::move(cudfTable));
    }
  }
  // Make sure cudfTables has at least one table
  if (cudfTables.empty()) {
    VELOX_CHECK(not cudfTables.empty());
    return;
  }

  // Create a sink and writer
  auto const sinkInfo = cudf::io::sink_info(filePath);
  auto tableInputMetadata =
      cudf::io::table_input_metadata(cudfTables[0]->view());
  fillColumnNames(tableInputMetadata, prefix);
  auto options = cudf::io::chunked_parquet_writer_options::builder(sinkInfo)
                     .metadata(tableInputMetadata)
                     .build();
  cudf::io::chunked_parquet_writer writer(options);

  // Write all table chunks
  for (const auto& table : cudfTables) {
    writer.write(table->view());
  }

  // Close the writer
  writer.close();
}

void CudfHiveConnectorTestBase::writeToFile(
    const std::string& filePath,
    RowVectorPtr vector,
    std::string prefix) {
  auto const sinkInfo = cudf::io::sink_info(filePath);
  VELOX_CHECK_NOT_NULL(vector);
  auto stream = cudf::get_default_stream();
  auto cudfTable = with_arrow::toCudfTable(vector, vector->pool(), stream);
  stream.synchronize();
  auto tableInputMetadata = cudf::io::table_input_metadata(cudfTable->view());
  fillColumnNames(tableInputMetadata, prefix);
  auto options =
      cudf::io::parquet_writer_options::builder(sinkInfo, cudfTable->view())
          .metadata(tableInputMetadata)
          .build();
  cudf::io::write_parquet(options);
}

std::vector<std::shared_ptr<facebook::velox::connector::ConnectorSplit>>
CudfHiveConnectorTestBase::makeCudfHiveConnectorSplits(
    const std::vector<
        std::shared_ptr<facebook::velox::exec::test::TempFilePath>>&
        filePaths) {
  std::vector<std::shared_ptr<facebook::velox::connector::ConnectorSplit>>
      splits;
  for (const auto& filePath : filePaths) {
    splits.push_back(makeCudfHiveConnectorSplit(filePath->getPath()));
  }
  return splits;
}

std::vector<
    std::shared_ptr<facebook::velox::connector::hive::HiveConnectorSplit>>
CudfHiveConnectorTestBase::makeCudfHiveConnectorSplits(
    const std::string& filePath,
    uint32_t splitCount) {
  auto file =
      filesystems::getFileSystem(filePath, nullptr)->openFileForRead(filePath);
  const int64_t fileSize = file->size();
  // Take the upper bound.
  const int64_t splitSize = std::ceil((fileSize) / splitCount);
  std::vector<
      std::shared_ptr<facebook::velox::connector::hive::HiveConnectorSplit>>
      splits;
  // Add all the splits.
  for (int i = 0; i < splitCount; i++) {
    auto split =
        facebook::velox::connector::hive::HiveConnectorSplitBuilder(filePath)
            .connectorId(kCudfHiveConnectorId)
            .fileFormat(facebook::velox::dwio::common::FileFormat::PARQUET)
            .build();
    splits.push_back(std::move(split));
  }
  return splits;
}

std::shared_ptr<facebook::velox::connector::hive::HiveConnectorSplit>
CudfHiveConnectorTestBase::makeCudfHiveConnectorSplit(
    const std::string& filePath,
    int64_t splitWeight) {
  return facebook::velox::connector::hive::HiveConnectorSplitBuilder(filePath)
      .connectorId(kCudfHiveConnectorId)
      .fileFormat(facebook::velox::dwio::common::FileFormat::PARQUET)
      .splitWeight(splitWeight)
      .build();
}

// static
std::shared_ptr<connector::hive::CudfHiveInsertTableHandle>
CudfHiveConnectorTestBase::makeCudfHiveInsertTableHandle(
    const std::vector<std::string>& tableColumnNames,
    const std::vector<TypePtr>& tableColumnTypes,
    std::shared_ptr<connector::hive::LocationHandle> locationHandle,
    const std::optional<common::CompressionKind> compressionKind,
    const std::unordered_map<std::string, std::string>& serdeParameters,
    const std::shared_ptr<dwio::common::WriterOptions>& writerOptions) {
  std::vector<std::shared_ptr<const connector::hive::CudfHiveColumnHandle>>
      columnHandles;

  for (int i = 0; i < tableColumnNames.size(); ++i) {
    columnHandles.push_back(
        std::make_shared<connector::hive::CudfHiveColumnHandle>(
            tableColumnNames.at(i),
            tableColumnTypes.at(i),
            cudf::data_type{veloxToCudfTypeId(tableColumnTypes.at(i))}));
  }

  return std::make_shared<connector::hive::CudfHiveInsertTableHandle>(
      columnHandles,
      locationHandle,
      compressionKind,
      serdeParameters,
      writerOptions);
}

} // namespace facebook::velox::cudf_velox::exec::test

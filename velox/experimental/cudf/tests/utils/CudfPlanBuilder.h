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

#include "velox/experimental/cudf/connectors/hive/CudfHiveDataSink.h"
#include "velox/experimental/cudf/tests/utils/CudfHiveConnectorTestBase.h"

#include "velox/dwio/common/Options.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

#include <string>

namespace facebook::velox::cudf_velox::exec::test {

using namespace facebook::velox;
using namespace facebook::velox::core;
using namespace facebook::velox::exec;
using namespace facebook::velox::common;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::common::test;
using namespace facebook::velox::common::testutil;
using namespace facebook::velox::dwio::common;

// Adds a TableWriter node to write all input columns into a CudfHive table.
std::function<PlanNodePtr(std::string, PlanNodePtr)> addCudfTableWriter(
    const RowTypePtr& inputColumns,
    const std::vector<std::string>& tableColumnNames,
    const std::optional<core::ColumnStatsSpec>& columnStatsSpec,
    const std::shared_ptr<core::InsertTableHandle>& insertHandle,
    facebook::velox::connector::CommitStrategy commitStrategy =
        facebook::velox::connector::CommitStrategy::kNoCommit);

/// Adds a TableWriteNode to write all input columns into an un-partitioned
/// un-bucketed CudfHive table without compression.
///
/// @param outputDirectoryPath Path to a directory to write data to.
/// @param fileFormat File format to use for the written data.
/// @param columnStatsSpec ColumnStatsSpec for column statistics collection
/// during write.
/// @param polymorphic options object to be passed to the writer.
/// write, supported aggregation types vary for different column types.
/// @param outputFileName Optional file name of the output. If specified
/// (non-empty), use it instead of generating the file name in Velox. Should
/// only be specified in non-bucketing write.
/// For example:
/// Boolean: count, countIf.
/// NumericType/Date/Timestamp: min, max, approx_distinct, count.
/// Varchar: count, approx_distinct, sum_data_size_for_stats,
/// max_data_size_for_stats.
std::function<PlanNodePtr(std::string, PlanNodePtr)> cudfTableWrite(
    const std::string& outputDirectoryPath,
    const dwio::common::FileFormat fileFormat =
        dwio::common::FileFormat::PARQUET,
    const std::optional<core::ColumnStatsSpec>& columnStatsSpec = std::nullopt,
    const std::shared_ptr<dwio::common::WriterOptions>& options = nullptr,
    const std::string& outputFileName = "");

/// Adds a TableWriteNode to write all input columns into CudfHive
/// table with compression.
///
/// @param outputDirectoryPath Path to a directory to write data to.
/// @param fileFormat File format to use for the written data.
/// @param columnStatsSpec ColumnStatsSpec for column statistics collection
/// during write.
/// @param connectorId Name used to register the connector.
/// @param serdeParameters Additional parameters passed to the writer.
/// @param Option objects passed to the writer.
/// @param outputFileName Optional file name of the output. If specified
/// (non-empty), use it instead of generating the file name in Velox. Should
/// only be specified in non-bucketing write.
/// @param compressionKind Compression scheme to use for writing the
/// output data files.
/// @param schema Output schema to be passed to the writer. By default use the
/// output of the previous operator.
std::function<PlanNodePtr(std::string, PlanNodePtr)> cudfTableWrite(
    const std::string& outputDirectoryPath,
    const dwio::common::FileFormat fileFormat,
    const std::optional<core::ColumnStatsSpec>& columnStatsSpec,
    const std::string_view& connectorId = kCudfHiveConnectorId,
    const std::unordered_map<std::string, std::string>& serdeParameters = {},
    const std::shared_ptr<dwio::common::WriterOptions>& options = nullptr,
    const std::string& outputFileName = "",
    const common::CompressionKind compression =
        common::CompressionKind::CompressionKind_NONE,
    const RowTypePtr& schema = nullptr);

} // namespace facebook::velox::cudf_velox::exec::test

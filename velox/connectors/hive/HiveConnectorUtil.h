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

#pragma once
#include <folly/Executor.h>
#include <folly/container/F14Map.h>

#include "velox/connectors/Connector.h"
#include "velox/connectors/hive/FileHandle.h"
#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/Reader.h"

namespace facebook::velox::connector::hive {

class HiveColumnHandle;
class HiveTableHandle;
class HiveConfig;
struct HiveConnectorSplit;

using SubfieldFilters =
    std::unordered_map<common::Subfield, std::unique_ptr<common::Filter>>;

constexpr const char* kPath = "$path";
constexpr const char* kBucket = "$bucket";

const std::string& getColumnName(const common::Subfield& subfield);

void checkColumnNameLowerCase(const std::shared_ptr<const Type>& type);

void checkColumnNameLowerCase(const SubfieldFilters& filters);

void checkColumnNameLowerCase(const core::TypedExprPtr& typeExpr);

std::shared_ptr<common::ScanSpec> makeScanSpec(
    const RowTypePtr& rowType,
    const folly::F14FastMap<std::string, std::vector<const common::Subfield*>>&
        outputSubfields,
    const SubfieldFilters& filters,
    const RowTypePtr& dataColumns,
    const std::unordered_map<std::string, std::shared_ptr<HiveColumnHandle>>&
        partitionKeys,
    memory::MemoryPool* pool);

void configureReaderOptions(
    dwio::common::ReaderOptions& readerOptions,
    const std::shared_ptr<HiveConfig>& config,
    const Config* sessionProperties,
    const std::shared_ptr<HiveTableHandle>& hiveTableHandle,
    const std::shared_ptr<HiveConnectorSplit>& hiveSplit);

void configureReaderOptions(
    dwio::common::ReaderOptions& readerOptions,
    const std::shared_ptr<HiveConfig>& hiveConfig,
    const Config* sessionProperties,
    const RowTypePtr& fileSchema,
    const std::shared_ptr<HiveConnectorSplit>& hiveSplit,
    const std::unordered_map<std::string, std::string>& tableParameters = {});

void configureRowReaderOptions(
    dwio::common::RowReaderOptions& rowReaderOptions,
    const std::unordered_map<std::string, std::string>& tableParameters,
    std::shared_ptr<common::ScanSpec> scanSpec,
    std::shared_ptr<common::MetadataFilter> metadataFilter,
    const RowTypePtr& rowType,
    std::shared_ptr<HiveConnectorSplit> hiveSplit);

bool applyPartitionFilter(
    TypeKind kind,
    const std::string& partitionValue,
    common::Filter* filter);

bool testFilters(
    common::ScanSpec* scanSpec,
    dwio::common::Reader* reader,
    const std::string& filePath,
    const std::unordered_map<std::string, std::optional<std::string>>&
        partitionKey,
    std::unordered_map<std::string, std::shared_ptr<HiveColumnHandle>>*
        partitionKeysHandle);

std::unique_ptr<dwio::common::BufferedInput> createBufferedInput(
    const FileHandle& fileHandle,
    const dwio::common::ReaderOptions& readerOpts,
    const ConnectorQueryCtx* connectorQueryCtx,
    std::shared_ptr<io::IoStatistics> ioStats,
    folly::Executor* executor);

} // namespace facebook::velox::connector::hive

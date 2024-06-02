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

#include "velox/exec/Split.h"

namespace facebook::velox::exec::test {
const std::string kHiveConnectorId = "test-hive";

/// Write the vector to the path.
void writeToFile(
    const std::string& path,
    const VectorPtr& vector,
    memory::MemoryPool* pool);

/// Write vectors into the path, one file per vector, and create spilts from
/// those files.
std::vector<Split> makeSplits(
    const std::vector<RowVectorPtr>& inputs,
    const std::string& path,
    const std::shared_ptr<memory::MemoryPool>& writerPool);

/// Create a split from an exsiting file.
Split makeSplit(const std::string& filePath);

/// Create a connector split from an exsiting file.
std::shared_ptr<connector::ConnectorSplit> makeConnectorSplit(
    const std::string& filePath);

/// Create column names with the pattern '${prefix}${i}'.
std::vector<std::string> makeNames(const std::string& prefix, size_t n);

/// Returns whether type is supported in TableScan. Empty Row type and Unknown
/// type are not supported.
bool isTableScanSupported(const TypePtr& type);

/// Concat tow RowTypes.
RowTypePtr concat(const RowTypePtr& a, const RowTypePtr& b);

/// Skip queries that use Timestamp, Varbinary, and IntervalDayTime types.
/// DuckDB doesn't support nanosecond precision for timestamps or casting from
/// Bigint to Interval.
///
/// TODO Investigate mismatches reported when comparing Varbinary.
bool containsUnsupportedTypes(const TypePtr& type);
} // namespace facebook::velox::exec::test

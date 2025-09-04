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

#include "velox/core/PlanNode.h"
#include "velox/exec/Split.h"
#include "velox/exec/fuzzer/ReferenceQueryRunner.h"
#include "velox/exec/tests/utils/QueryAssertions.h"

namespace facebook::velox::exec::test {
const std::string kHiveConnectorId = "test-hive";

struct SortingKeyAndOrder {
  const std::string key_;
  const core::SortOrder sortOrder_;

  SortingKeyAndOrder(std::string key, core::SortOrder sortOrder)
      : key_(std::move(key)), sortOrder_(std::move(sortOrder)) {}
};

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

/// Create splits with schema information from a directory.
/// For example under directory /table1:
/// /table1/p0=0/p1=0/00000_file1
/// /table1/p0=0/p1=1/00000_file1
/// It would return splits:
/// split1 with partition keys (p0, 0), (p1, 0)
/// split2 with partition keys (p0, 0), (p1, 1)
std::vector<Split> makeSplits(const std::string& directory);

/// Create a split from an exsiting file.
Split makeSplit(
    const std::string& filePath,
    const std::unordered_map<std::string, std::optional<std::string>>&
        partitionKeys = {},
    std::optional<int32_t> tableBucketNumber = std::nullopt);

/// Create a connector split from an exsiting file.
std::shared_ptr<connector::ConnectorSplit> makeConnectorSplit(
    const std::string& filePath,
    const std::unordered_map<std::string, std::optional<std::string>>&
        partitionKeys = {},
    std::optional<int32_t> tableBucketNumber = std::nullopt);

/// Create column names with the pattern '${prefix}${i}'.
std::vector<std::string> makeNames(const std::string& prefix, size_t n);

/// Create a batch consists of single all-null BIGINT column with as many rows
/// as original input. Used when the query doesn't need to read any columns, but
/// it needs to see a specific number of rows. This way we will be able to
/// create a temporary test table with the necessary number of rows.
RowVectorPtr makeNullRows(
    const std::vector<velox::RowVectorPtr>& input,
    const std::string& colName,
    memory::MemoryPool* pool);

/// Returns whether type is supported in TableScan. Empty Row type and Unknown
/// type are not supported.
bool isTableScanSupported(const TypePtr& type);

/// Concat two RowTypes.
RowTypePtr concat(const RowTypePtr& a, const RowTypePtr& b);

/// Skip queries that use Timestamp, Varbinary, and IntervalDayTime types.
/// DuckDB doesn't support nanosecond precision for timestamps or casting from
/// Bigint to Interval.
///
/// TODO Investigate mismatches reported when comparing Varbinary.
bool containsUnsupportedTypes(const TypePtr& type);

/// Determines whether the signature has an argument that contains typeName.
/// typeName should be in lower case.
bool usesInputTypeName(
    const exec::FunctionSignature& signature,
    const std::string& typeName);

/// Determines whether the signature has an argument or return type that
/// contains typeName. typeName should be in lower case.
bool usesTypeName(
    const exec::FunctionSignature& signature,
    const std::string& typeName);

// First resolves typeSignature. Then, the resolved type is a RowType or
// contains RowTypes with empty field names, adds default names to these fields
// in the RowTypes.
TypePtr sanitizeTryResolveType(
    const exec::TypeSignature& typeSignature,
    const std::unordered_map<std::string, SignatureVariable>& variables,
    const std::unordered_map<std::string, TypePtr>& resolvedTypeVariables);

TypePtr sanitizeTryResolveType(
    const exec::TypeSignature& typeSignature,
    const std::unordered_map<std::string, SignatureVariable>& variables,
    const std::unordered_map<std::string, TypePtr>& typeVariablesBindings,
    std::unordered_map<std::string, int>& integerVariablesBindings,
    const std::unordered_map<std::string, LongEnumParameter>&
        longEnumParameterVariablesBindings,
    const std::unordered_map<std::string, VarcharEnumParameter>&
        varcharEnumParameterVariablesBindings);

// Invoked to set up memory system with arbitration.
void setupMemory(
    int64_t allocatorCapacity,
    int64_t arbitratorCapacity,
    bool enableGlobalArbitration = true);

/// Registers hive connector with configs. It should be called in the
/// constructor of fuzzers that test plans with TableScan or uses
/// PrestoQueryRunner that writes data to a local file.
void registerHiveConnector(
    const std::unordered_map<std::string, std::string>& hiveConfigs);

// Returns a PrestoQueryRunner instance if prestoUrl is non-empty. Otherwise,
// returns a DuckQueryRunner instance and set disabled aggregation functions
// properly.
std::unique_ptr<ReferenceQueryRunner> setupReferenceQueryRunner(
    memory::MemoryPool* aggregatePool,
    const std::string& prestoUrl,
    const std::string& runnerName,
    const uint32_t& reqTimeoutMs);

// Logs the input vectors if verbose logging is turned on.
void logVectors(const std::vector<RowVectorPtr>& vectors);

// Converts 'plan' into an SQL query and runs in the reference DB.
// Result is returned as a MaterializedRowMultiset with the
// ReferenceQueryErrorCode::kSuccess if successful, or an std::nullopt with a
// ReferenceQueryErrorCode if the query fails.
std::pair<std::optional<MaterializedRowMultiset>, ReferenceQueryErrorCode>
computeReferenceResults(
    const core::PlanNodePtr& plan,
    ReferenceQueryRunner* referenceQueryRunner);

// Similar to computeReferenceResults(), but returns the result as a
// std::vector<RowVectorPtr>. This API throws if referenceQueryRunner doesn't
// support returning results as a vector.
std::pair<std::optional<std::vector<RowVectorPtr>>, ReferenceQueryErrorCode>
computeReferenceResultsAsVector(
    const core::PlanNodePtr& plan,
    ReferenceQueryRunner* referenceQueryRunner);

} // namespace facebook::velox::exec::test

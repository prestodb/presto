/*
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

#include "presto_cpp/main/tvf/spi/DescriptorArgument.h"
#include "presto_cpp/main/tvf/spi/ReturnTypeSpecification.h"
#include "presto_cpp/main/tvf/spi/ScalarArgument.h"
#include "presto_cpp/main/tvf/spi/TableArgument.h"
#include "presto_cpp/main/tvf/spi/TableFunctionAnalysis.h"
#include "presto_cpp/main/tvf/spi/TableFunctionResult.h"

#include "velox/common/memory/HashStringAllocator.h"
#include "velox/core/QueryConfig.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::presto::tvf {

/// The TableFunctionDataProcessor is responsible for processing the input data
/// of a single partition of a Table Function with Table Arguments.
class TableFunctionDataProcessor {
 public:
  explicit TableFunctionDataProcessor(
      const std::string& name,
      velox::memory::MemoryPool* pool,
      velox::HashStringAllocator* stringAllocator)
      : name_(name), pool_(pool), stringAllocator_(stringAllocator) {}

  virtual ~TableFunctionDataProcessor() = default;

  velox::memory::MemoryPool* pool() const {
    return pool_;
  }

  const velox::HashStringAllocator* stringAllocator() const {
    return stringAllocator_;
  }

  const std::string name() const {
    return name_;
  }

  /// This method processes a partition of input data of the TableFunction
  /// It is called multiple times until the partition is fully processed.
  /// @param input : Is a vector of RowVectors one for each input TableArgument
  /// of the function.
  /// The rows in the RowVector are ordered according to the
  /// corresponding argument specification of the Table function.
  /// Each RowVector for a table argument consists of the columns requested
  /// during analysis (see TableFunctionAnalysis::getRequiredColumns()).
  /// If any of the sources is fully processed, then the RowVectorPtr contains
  /// nullptr for that source.
  /// If all sources are fully processed, the argument is a vector of nullptrs.
  /// @return {@link TableFunctionProcessorState} includes the processor's state
  /// and optionally a portion of result.
  /// After the returned state is {@code FINISHED}, the method will not be
  /// called again. So its important that the function cleans up any state
  /// before it returns
  /// {@code FINISHED}.
  virtual std::shared_ptr<TableFunctionResult> apply(
      const std::vector<velox::RowVectorPtr>& input) {
    VELOX_NYI(" TableFunction::apply() for input vector is not implemented");
  }

 protected:
  const std::string name_;
  velox::memory::MemoryPool* pool_;
  velox::HashStringAllocator* const stringAllocator_;
};

/// Processes a single split for a table function. Each
/// {@code TableFunctionSplitProcessor} instance is associated with exactly one
/// split and is responsible for processing that split to completion.
/// The {@link #apply(ConnectorSplit)} method is called repeatedly until the
/// processor returns {@link TableFunctionProcessorState.Finished},
/// at which point the split is considered fully processed.
class TableFunctionSplitProcessor {
 public:
  explicit TableFunctionSplitProcessor(
      const std::string& name,
      velox::memory::MemoryPool* pool,
      velox::HashStringAllocator* stringAllocator)
      : pool_(pool), stringAllocator_(stringAllocator) {}

  virtual ~TableFunctionSplitProcessor() = default;

  velox::memory::MemoryPool* pool() const {
    return pool_;
  }

  const velox::HashStringAllocator* stringAllocator() const {
    return stringAllocator_;
  }

  const std::string name() const {
    return name_;
  }

  /// This method processes a split. It is called multiple times until the
  /// whole output for the split is produced.
  /// @param split a {@link ConnectorSplit} representing a subtask.
  /// @return {@link TableFunctionProcessorState} including the processor's
  /// state and optionally a portion of result.
  /// After the returned state is {@code FINISHED}, the method will not be
  /// called again.
  /// So its important that the function cleans up any state before it returns
  /// {@code FINISHED}.
  virtual std::shared_ptr<TableFunctionResult> apply(
      const TableSplitHandlePtr& split) {
    VELOX_NYI(" TableFunction::apply() for split is not implemented");
  }

 protected:
  const std::string name_;
  velox::memory::MemoryPool* pool_;
  velox::HashStringAllocator* const stringAllocator_;
};

class TableFunction {
 public:
  explicit TableFunction() {};

  virtual ~TableFunction() = default;

  static std::unique_ptr<TableFunctionAnalysis> analyze(
      const std::string& name,
      const std::unordered_map<std::string, std::shared_ptr<Argument>>& args);

  static std::unique_ptr<TableFunctionDataProcessor> createDataProcessor(
      const std::string& name,
      const TableFunctionHandlePtr& handle,
      velox::memory::MemoryPool* pool,
      velox::HashStringAllocator* stringAllocator,
      const velox::core::QueryConfig& config);

  static std::unique_ptr<TableFunctionSplitProcessor> createSplitProcessor(
      const std::string& name,
      const TableFunctionHandlePtr& handle,
      velox::memory::MemoryPool* pool,
      velox::HashStringAllocator* stringAllocator,
      const velox::core::QueryConfig& config);

  static std::vector<TableSplitHandlePtr> getSplits(
      const std::string& name,
      const TableFunctionHandlePtr& handle);

  static std::unique_ptr<TableFunctionDataProcessor> defaultCreateDataProcessor(
      const TableFunctionHandlePtr& /* handle */,
      velox::memory::MemoryPool* /* pool */,
      velox::HashStringAllocator* /* stringAllocator */,
      const velox::core::QueryConfig& /* config */) {
    VELOX_NYI("TableFunction::createDataProcessor is not implemented");
  }

  static std::unique_ptr<TableFunctionSplitProcessor>
  defaultCreateSplitProcessor(
      const TableFunctionHandlePtr& /* handle */,
      velox::memory::MemoryPool* /* pool */,
      velox::HashStringAllocator* /* stringAllocator */,
      const velox::core::QueryConfig& /* config */) {
    VELOX_NYI("TableFunction::createSplitProcessor is not implemented");
  }

  static std::vector<TableSplitHandlePtr> defaultGetSplits(
      const TableFunctionHandlePtr& /* handle */) {
    VELOX_NYI("TableFunction::getSplits is not implemented");
  }
};

using TableFunctionAnalyzer =
    std::function<std::unique_ptr<TableFunctionAnalysis>(
        const std::unordered_map<std::string, std::shared_ptr<Argument>>&
            args)>;

using TableFunctionDataProcessorFactory =
    std::function<std::unique_ptr<TableFunctionDataProcessor>(
        const TableFunctionHandlePtr& handle,
        velox::memory::MemoryPool* pool,
        velox::HashStringAllocator* stringAllocator,
        const velox::core::QueryConfig& config)>;

using TableFunctionSplitProcessorFactory =
    std::function<std::unique_ptr<TableFunctionSplitProcessor>(
        const TableFunctionHandlePtr& handle,
        velox::memory::MemoryPool* pool,
        velox::HashStringAllocator* stringAllocator,
        const velox::core::QueryConfig& config)>;

using TableFunctionSplitGenerator =
    std::function<std::vector<TableSplitHandlePtr>(
        const TableFunctionHandlePtr& handle)>;

/// Register a Table function with the specified name, arguments and return
/// spec.
/// Registering a function with the same name a second time overrides the
/// first registration.
/// If the TableFunction is a Generic table function whose return type is
/// determined by the input types of its input tables, then it needs to provide
/// an analyze function that will be invoked during SQL query analysis
/// to determine the return type from the input tables arguments.
/// Each Table Function has either a TableFunctionDataProcessor or a
/// TableFunctionSplitProcessor, depending on whether the table function
/// processes input tables or is a leaf operation working on scalar
/// and descriptor arguments.
/// If the Table Function is a leaf operation, then it must provide a
/// getSplits() implementation that divides the work into splits, and a
/// TableFunctionSplitProcessor will ßbe created to process each split.
bool registerTableFunction(
    const std::string& name,
    TableArgumentSpecList argumentsSpec,
    ReturnSpecPtr returnSpec,
    TableFunctionAnalyzer analyzer,
    TableFunctionDataProcessorFactory dataProcessorFactory =
        TableFunction::defaultCreateDataProcessor,
    TableFunctionSplitProcessorFactory splitProcessorFactory =
        TableFunction::defaultCreateSplitProcessor,
    TableFunctionSplitGenerator splitGenerator =
        TableFunction::defaultGetSplits);

ReturnSpecPtr getTableFunctionReturnType(const std::string& name);

TableArgumentSpecList getTableFunctionArgumentSpecs(const std::string& name);

struct TableFunctionEntry {
  TableArgumentSpecList argumentsSpec;
  ReturnSpecPtr returnSpec;
  TableFunctionAnalyzer analyzer;
  TableFunctionDataProcessorFactory dataProcessorFactory;
  TableFunctionSplitProcessorFactory splitProcessorFactory;
  TableFunctionSplitGenerator splitGenerator;
};
using TableFunctionMap = std::unordered_map<std::string, TableFunctionEntry>;

/// Returns a map of all Table function names to their registrations.
TableFunctionMap& tableFunctions();
} // namespace facebook::presto::tvf

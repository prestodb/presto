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

#include "presto_cpp/main/tvf/spi/Descriptor.h"
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

  virtual std::shared_ptr<TableFunctionResult> apply(
      const std::vector<velox::RowVectorPtr>& input) {
    VELOX_NYI(" TableFunction::apply() for input vector is not implemented");
  }

 protected:
  const std::string name_;
  velox::memory::MemoryPool* pool_;
  velox::HashStringAllocator* const stringAllocator_;
};

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
  explicit TableFunction(){};

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

  static std::unique_ptr<TableFunctionDataProcessor> defaultCreateDataProcessor(
      const TableFunctionHandlePtr& /* handle */,
      velox::memory::MemoryPool* /* pool */,
      velox::HashStringAllocator* /* stringAllocator */,
      const velox::core::QueryConfig& /* config */) {
    VELOX_NYI("TableFunction::createDataProcessor is not implemented");
  }

  static std::unique_ptr<TableFunctionSplitProcessor> createSplitProcessor(
      const std::string& name,
      const TableFunctionHandlePtr& handle,
      velox::memory::MemoryPool* pool,
      velox::HashStringAllocator* stringAllocator,
      const velox::core::QueryConfig& config);

  static std::unique_ptr<TableFunctionSplitProcessor>
  defaultCreateSplitProcessor(
      const TableFunctionHandlePtr& /* handle */,
      velox::memory::MemoryPool* /* pool */,
      velox::HashStringAllocator* /* stringAllocator */,
      const velox::core::QueryConfig& /* config */) {
    VELOX_NYI("TableFunction::createSplitProcessor is not implemented");
  }

  static std::vector<TableSplitHandlePtr> getSplits(
      const std::string& name,
      const TableFunctionHandlePtr& handle);

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

struct TableFunctionEntry {
  TableArgumentSpecList argumentsSpec;
  ReturnSpecPtr returnSpec;
  TableFunctionAnalyzer analyzer;
  TableFunctionDataProcessorFactory dataProcessorFactory;
  TableFunctionSplitProcessorFactory splitProcessorFactory;
  TableFunctionSplitGenerator splitGenerator;
};

/// Register a Table function with the specified name.
/// Registering a function with the same name a second time overrides the first
/// registration.
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

using TableFunctionMap = std::unordered_map<std::string, TableFunctionEntry>;

/// Returns a map of all Table function names to their registrations.
TableFunctionMap& tableFunctions();
} // namespace facebook::presto::tvf

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

using TableArgumentSpecList =
    std::unordered_set<std::shared_ptr<ArgumentSpecification>>;

class TableFunction {
 public:
  explicit TableFunction(
      velox::memory::MemoryPool* pool,
      velox::HashStringAllocator* stringAllocator)
      : pool_(pool), stringAllocator_(stringAllocator) {}

  virtual ~TableFunction() = default;

  velox::memory::MemoryPool* pool() const {
    return pool_;
  }

  const velox::HashStringAllocator* stringAllocator() const {
    return stringAllocator_;
  }

  static std::unique_ptr<TableFunction> create(
      const std::string& name,
      const std::shared_ptr<const TableFunctionHandle>& handle,
      velox::memory::MemoryPool* pool,
      velox::HashStringAllocator* stringAllocator,
      const velox::core::QueryConfig& config);

  static std::unique_ptr<TableFunctionAnalysis> analyze(
      const std::string& name,
      const std::unordered_map<std::string, std::shared_ptr<Argument>>& args);

  virtual std::shared_ptr<TableFunctionResult> apply(
      const std::vector<velox::RowVectorPtr>& input) {
    VELOX_NYI(" TableFunction::apply() for input vector is not implemented");
  }

  virtual std::shared_ptr<TableFunctionResult> apply(
      const TableSplitHandlePtr& split) {
    VELOX_NYI(" TableFunction::apply() for split is not implemented");
  }

 protected:
  velox::memory::MemoryPool* pool_;
  velox::HashStringAllocator* const stringAllocator_;
};

/// Information from the Table operator that is useful for the function logic.
/// @param args  Vector of the input arguments to the function. These could be
/// constants or positions of the input argument column in the input row of the
/// operator. These indices are used to access data from the WindowPartition
/// object.
/// @param resultType  Type of the result of the function.
using TableFunctionFactory = std::function<std::unique_ptr<TableFunction>(
    const std::shared_ptr<const TableFunctionHandle>& handle,
    velox::memory::MemoryPool* pool,
    velox::HashStringAllocator* stringAllocator,
    const velox::core::QueryConfig& config)>;

using TableFunctionAnalyzer =
    std::function<std::unique_ptr<TableFunctionAnalysis>(
        const std::unordered_map<std::string, std::shared_ptr<Argument>>&
            args)>;

/// Register a Table function with the specified name.
/// Registering a function with the same name a second time overrides the first
/// registration.
bool registerTableFunction(
    const std::string& name,
    TableArgumentSpecList argumentsSpec,
    ReturnSpecPtr returnSpec,
    TableFunctionAnalyzer analyzer,
    TableFunctionFactory factory);

struct TableFunctionEntry {
  TableArgumentSpecList argumentsSpec;
  ReturnSpecPtr returnSpec;
  TableFunctionAnalyzer analyzer;
  TableFunctionFactory factory;
};

// Returning a pointer since it can be dynamic cast.
ReturnSpecPtr getTableFunctionReturnType(const std::string& name);

TableArgumentSpecList getTableFunctionArgumentSpecs(const std::string& name);

using TableFunctionMap = std::unordered_map<std::string, TableFunctionEntry>;

/// Returns a map of all Table function names to their registrations.
TableFunctionMap& tableFunctions();
} // namespace facebook::presto::tvf

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

#include "presto_cpp/main/tvf/spi/TableFunction.h"

#include "velox/expression/FunctionSignature.h"

namespace facebook::presto::tvf {

using namespace facebook::velox;

TableFunctionMap& tableFunctions() {
  static TableFunctionMap functions;
  return functions;
}

namespace {
std::optional<const TableFunctionEntry*> getTableFunctionEntry(
    const std::string& name) {
  auto& functionsMap = tableFunctions();
  auto it = functionsMap.find(name);
  if (it != functionsMap.end()) {
    return &it->second;
  }

  return std::nullopt;
}
} // namespace

bool registerTableFunction(
    const std::string& name,
    TableArgumentSpecList argumentsSpec,
    ReturnSpecPtr returnSpec,
    TableFunctionAnalyzer analyzer,
    TableFunctionDataProcessorFactory dataProcessorfactory,
    TableFunctionSplitProcessorFactory splitProcessorfactory,
    TableFunctionSplitGenerator splitGenerator) {
  auto sanitizedName = exec::sanitizeName(name);
  tableFunctions().insert(
      {sanitizedName,
       {std::move(argumentsSpec),
        std::move(returnSpec),
        std::move(analyzer),
        std::move(dataProcessorfactory),
        std::move(splitProcessorfactory),
        std::move(splitGenerator)}});
  return true;
}

ReturnSpecPtr getTableFunctionReturnType(const std::string& name) {
  const auto sanitizedName = exec::sanitizeName(name);
  if (auto func = getTableFunctionEntry(sanitizedName)) {
    return func.value()->returnSpec;
  } else {
    VELOX_USER_FAIL("ReturnTypeSpecification not found for function: {}", name);
  }
}

TableArgumentSpecList getTableFunctionArgumentSpecs(const std::string& name) {
  const auto sanitizedName = exec::sanitizeName(name);
  if (auto func = getTableFunctionEntry(sanitizedName)) {
    return func.value()->argumentsSpec;
  } else {
    VELOX_USER_FAIL("Arguments Specification not found for function: {}", name);
  }
}

std::unique_ptr<TableFunctionAnalysis> TableFunction::analyze(
    const std::string& name,
    const std::unordered_map<std::string, std::shared_ptr<Argument>>& args) {
  // Lookup the function in the new registry first.
  if (auto func = getTableFunctionEntry(name)) {
    return func.value()->analyzer(args);
  }

  VELOX_USER_FAIL("Table function not registered: {}", name);
}

std::unique_ptr<TableFunctionDataProcessor> TableFunction::createDataProcessor(
    const std::string& name,
    const std::shared_ptr<const TableFunctionHandle>& handle,
    memory::MemoryPool* pool,
    HashStringAllocator* stringAllocator,
    const core::QueryConfig& config) {
  // Lookup the function in the new registry first.
  if (auto func = getTableFunctionEntry(name)) {
    return func.value()->dataProcessorFactory(
        handle, pool, stringAllocator, config);
  }

  VELOX_USER_FAIL("Table function not registered: {}", name);
}

std::unique_ptr<TableFunctionSplitProcessor>
TableFunction::createSplitProcessor(
    const std::string& name,
    const std::shared_ptr<const TableFunctionHandle>& handle,
    memory::MemoryPool* pool,
    HashStringAllocator* stringAllocator,
    const core::QueryConfig& config) {
  // Lookup the function in the new registry first.
  if (auto func = getTableFunctionEntry(name)) {
    return func.value()->splitProcessorFactory(
        handle, pool, stringAllocator, config);
  }

  VELOX_USER_FAIL("Table function not registered: {}", name);
}

std::vector<TableSplitHandlePtr> TableFunction::getSplits(
    const std::string& name,
    const TableFunctionHandlePtr& handle) {
  // Lookup the function in the new registry first.
  if (auto func = getTableFunctionEntry(name)) {
    return func.value()->splitGenerator(handle);
  }

  VELOX_USER_FAIL("Table function not registered: {}", name);
}

} // namespace facebook::presto::tvf

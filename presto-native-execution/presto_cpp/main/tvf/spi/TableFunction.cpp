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

folly::Synchronized<TableFunctionMap>& tableFunctions() {
  static folly::Synchronized<TableFunctionMap> functions;
  return functions;
}

namespace {
/// Returns a copy of the registration of 'name', or std::nullopt if the
/// function is not registered. A copy is returned as a registration can be
/// replaced by a subsequent registration of the same name.
std::optional<TableFunctionEntry> getTableFunctionEntry(
    const std::string& name) {
  return tableFunctions().withRLock(
      [&](const auto& functionsMap) -> std::optional<TableFunctionEntry> {
        auto it = functionsMap.find(name);
        if (it != functionsMap.end()) {
          return it->second;
        }

        return std::nullopt;
      });
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
  tableFunctions().withWLock([&](auto& functionsMap) {
    functionsMap.insert_or_assign(
        sanitizedName,
        TableFunctionEntry{
            std::move(argumentsSpec),
            std::move(returnSpec),
            std::move(analyzer),
            std::move(dataProcessorfactory),
            std::move(splitProcessorfactory),
            std::move(splitGenerator)});
  });
  return true;
}

ReturnSpecPtr getTableFunctionReturnType(const std::string& name) {
  const auto sanitizedName = exec::sanitizeName(name);
  if (auto func = getTableFunctionEntry(sanitizedName)) {
    return func->returnSpec;
  } else {
    VELOX_USER_FAIL("ReturnTypeSpecification not found for function: {}", name);
  }
}

TableArgumentSpecList getTableFunctionArgumentSpecs(const std::string& name) {
  const auto sanitizedName = exec::sanitizeName(name);
  if (auto func = getTableFunctionEntry(sanitizedName)) {
    return func->argumentsSpec;
  } else {
    VELOX_USER_FAIL("Arguments Specification not found for function: {}", name);
  }
}

std::unique_ptr<TableFunctionAnalysis> TableFunction::analyze(
    const std::string& name,
    const std::unordered_map<std::string, std::shared_ptr<Argument>>& args) {
  if (auto func = getTableFunctionEntry(name)) {
    return func->analyzer(args);
  }

  VELOX_USER_FAIL("Table function not registered: {}", name);
}

std::unique_ptr<TableFunctionDataProcessor> TableFunction::createDataProcessor(
    const std::string& name,
    const std::shared_ptr<const TableFunctionHandle>& handle,
    memory::MemoryPool* pool,
    HashStringAllocator* stringAllocator,
    const core::QueryConfig& config) {
  if (auto func = getTableFunctionEntry(name)) {
    return func->dataProcessorFactory(handle, pool, stringAllocator, config);
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
    return func->splitProcessorFactory(handle, pool, stringAllocator, config);
  }

  VELOX_USER_FAIL("Table function not registered: {}", name);
}

std::vector<TableSplitHandlePtr> TableFunction::getSplits(
    const std::string& name,
    const TableFunctionHandlePtr& handle) {
  // Lookup the function in the new registry first.
  if (auto func = getTableFunctionEntry(name)) {
    return func->splitGenerator(handle);
  }

  VELOX_USER_FAIL("Table function not registered: {}", name);
}

} // namespace facebook::presto::tvf

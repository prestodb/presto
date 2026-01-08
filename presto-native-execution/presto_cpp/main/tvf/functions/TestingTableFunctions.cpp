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

#include "presto_cpp/main/tvf/functions/TestingTableFunctions.h"

#include "velox/vector/BaseVector.h"
#include "velox/vector/ConstantVector.h"

using namespace facebook::velox;

namespace facebook::presto::tvf {

std::unique_ptr<SimpleTableFunctionAnalysis> SimpleTableFunction::analyze(
    const std::unordered_map<std::string, std::shared_ptr<Argument>>& args) {
  std::vector<std::string> returnNames;
  std::vector<TypePtr> returnTypes;

  const auto arg = std::dynamic_pointer_cast<ScalarArgument>(args.at("COLUMN"));
  const auto val = arg->value()->as<ConstantVector<StringView>>()->valueAt(0);

  returnNames.push_back(val);
  returnTypes.push_back(BOOLEAN());

  auto analysis = std::make_unique<SimpleTableFunctionAnalysis>();
  analysis->tableFunctionHandle_ =
      std::make_shared<SimpleTableFunctionHandle>();
  analysis->returnType_ =
      std::make_shared<Descriptor>(returnNames, returnTypes);
  return analysis;
}

void registerSimpleTableFunction(const std::string& name) {
  TableArgumentSpecList argSpecs;
  argSpecs.insert(
      std::make_shared<ScalarArgumentSpecification>("COLUMN", VARCHAR(), true));
  argSpecs.insert(
      std::make_shared<ScalarArgumentSpecification>(
          "IGNORED", BIGINT(), false));

  registerTableFunction(
      name,
      argSpecs,
      std::make_shared<GenericTableReturnType>(),
      SimpleTableFunction::analyze);
}

std::shared_ptr<TableFunctionResult> IdentityDataProcessor::apply(
    const std::vector<velox::RowVectorPtr>& input) {
  auto inputTable = input.at(0);
  auto numRows = inputTable->size();
  if (numRows == 0) {
    return std::make_shared<TableFunctionResult>(
        TableFunctionResult::TableFunctionState::kFinished);
  }
  return std::make_shared<TableFunctionResult>(true, std::move(inputTable));
}

std::unique_ptr<IdentityFunctionAnalysis> IdentityFunction::analyze(
    const std::unordered_map<std::string, std::shared_ptr<Argument>>& args) {
  auto input = std::dynamic_pointer_cast<TableArgument>(args.at("INPUT"));
  std::vector<std::string> returnNames = input->rowType()->names();
  std::vector<TypePtr> returnTypes;
  std::vector<column_index_t> requiredColsList;
  for (size_t i = 0; i < returnNames.size(); i++) {
    returnTypes.push_back(input->rowType()->childAt(i));
    requiredColsList.push_back(i);
  }
  RequiredColumnsMap requiredColumns;
  requiredColumns.emplace("INPUT", requiredColsList);

  auto analysis = std::make_unique<IdentityFunctionAnalysis>();
  analysis->tableFunctionHandle_ = std::make_shared<IdentityFunctionHandle>();
  analysis->returnType_ =
      std::make_shared<Descriptor>(returnNames, returnTypes);
  analysis->requiredColumns_ = requiredColumns;
  return analysis;
}

void registerIdentityFunction(const std::string& name) {
  TableArgumentSpecList argSpecs;
  argSpecs.insert(
      std::make_shared<TableArgumentSpecification>(
          "INPUT", true, false, false));
  registerTableFunction(
      name,
      argSpecs,
      std::make_shared<GenericTableReturnType>(),
      IdentityFunction::analyze,
      [](const TableFunctionHandlePtr& handle,
         memory::MemoryPool* pool,
         HashStringAllocator* stringAllocator,
         const velox::core::QueryConfig& config)
          -> std::unique_ptr<TableFunctionDataProcessor> {
        return std::make_unique<IdentityDataProcessor>(
            dynamic_cast<const IdentityFunctionHandle*>(handle.get()), pool);
      });
}

std::shared_ptr<TableFunctionResult> RepeatFunctionDataProcessor::apply(
    const std::vector<velox::RowVectorPtr>& input) {
  auto inputTable = input.at(0);
  auto numRows = inputTable->size();
  if (numRows == 0) {
    return std::make_shared<TableFunctionResult>(
        TableFunctionResult::TableFunctionState::kFinished);
  }

  RowVectorPtr outputTable =
      RowVector::createEmpty(inputTable->rowType(), pool());
  auto count = handle_->count();
  outputTable->resize(numRows * count);
  for (int i = 0; i < count; i++) {
    outputTable->copy(inputTable.get(), i * numRows, 0, numRows);
  }

  return std::make_shared<TableFunctionResult>(true, std::move(outputTable));
}

std::unique_ptr<RepeatFunctionAnalysis> RepeatFunction::analyze(
    const std::unordered_map<std::string, std::shared_ptr<Argument>>& args) {
  auto input = std::dynamic_pointer_cast<TableArgument>(args.at("INPUT"));

  auto countArg = args.at("COUNT");
  auto countPtr = std::dynamic_pointer_cast<ScalarArgument>(countArg);

  auto count = countPtr->value()->as<ConstantVector<int64_t>>()->valueAt(0);

  std::vector<std::string> returnNames = input->rowType()->names();
  std::vector<TypePtr> returnTypes;
  std::vector<column_index_t> requiredColsList;
  for (size_t i = 0; i < returnNames.size(); i++) {
    returnTypes.push_back(input->rowType()->childAt(i));
    requiredColsList.push_back(i);
  }
  RequiredColumnsMap requiredColumns;
  requiredColumns.emplace("INPUT", requiredColsList);
  auto analysis = std::make_unique<RepeatFunctionAnalysis>();
  analysis->tableFunctionHandle_ =
      std::make_shared<RepeatFunctionHandle>(count);
  analysis->returnType_ =
      std::make_shared<Descriptor>(returnNames, returnTypes);
  analysis->requiredColumns_ = requiredColumns;
  return analysis;
}

void registerRepeatFunction(const std::string& name) {
  TableArgumentSpecList argSpecs;
  argSpecs.insert(
      std::make_shared<TableArgumentSpecification>(
          "INPUT", true, false, false));
  argSpecs.insert(
      std::make_shared<ScalarArgumentSpecification>("COUNT", BIGINT(), true));
  registerTableFunction(
      name,
      argSpecs,
      std::make_shared<GenericTableReturnType>(),
      RepeatFunction::analyze,
      [](const TableFunctionHandlePtr& handle,
         memory::MemoryPool* pool,
         HashStringAllocator* stringAllocator,
         const velox::core::QueryConfig& config)
          -> std::unique_ptr<TableFunctionDataProcessor> {
        return std::make_unique<RepeatFunctionDataProcessor>(
            dynamic_cast<const RepeatFunctionHandle*>(handle.get()), pool);
      });
}

} // namespace facebook::presto::tvf

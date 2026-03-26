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

#include "TestingTableFunctions.h"

#include "velox/vector/BaseVector.h"
#include "velox/vector/ConstantVector.h"
#include "velox/vector/FlatVector.h"

using namespace facebook::velox;

namespace facebook::presto::tvf {

// Operator-based SimpleTableFunction implementation
std::unique_ptr<SimpleTableFunctionAnalysis> SimpleTableFunction::analyze(
    const std::unordered_map<std::string, std::shared_ptr<Argument>>& args) {
  std::vector<std::string> returnNames;
  std::vector<TypePtr> returnTypes;

  const auto arg = std::dynamic_pointer_cast<ScalarArgument>(args.at("COLUMN"));
  const auto val = arg->value()->as<ConstantVector<StringView>>()->valueAt(0);

  returnNames.push_back(std::string(val));
  returnTypes.push_back(BOOLEAN());

  auto analysis = std::make_unique<SimpleTableFunctionAnalysis>();
  analysis->tableFunctionHandle_ =
      std::make_shared<SimpleTableFunctionHandle>(std::string(val));
  analysis->returnType_ =
      std::make_shared<Descriptor>(returnNames, returnTypes);
  return analysis;
}

void registerSimpleTableFunction(const std::string& name) {
  TableArgumentSpecList argSpecs;
  argSpecs.push_back(
      std::make_shared<ScalarArgumentSpecification>("COLUMN", VARCHAR(), false, "col"));
  argSpecs.push_back(
      std::make_shared<ScalarArgumentSpecification>(
          "IGNORED", BIGINT(), false, "0"));

  registerTableFunction(
      name,
      argSpecs,
      std::make_shared<GenericTableReturnTypeSpecification>(),
      SimpleTableFunction::analyze,
      TableFunction::defaultCreateDataProcessor,
      TableFunction::defaultCreateSplitProcessor,
      SimpleTableFunction::getSplits);
}

std::shared_ptr<TableFunctionResult> IdentityDataProcessor::apply(
    const std::vector<velox::RowVectorPtr>& input) {
  auto inputTable = input.at(0);
  if (!inputTable || inputTable->size() == 0) {
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
  requiredColumns.push_back({"INPUT", requiredColsList});

  auto analysis = std::make_unique<IdentityFunctionAnalysis>();
  analysis->tableFunctionHandle_ = std::make_shared<IdentityFunctionHandle>();
  analysis->returnType_ =
      std::make_shared<Descriptor>(returnNames, returnTypes);
  analysis->requiredColumns_ = requiredColumns;
  return analysis;
}

void registerIdentityFunction(const std::string& name) {
  TableArgumentSpecList argSpecs;
  argSpecs.push_back(
      std::make_shared<TableArgumentSpecification>(
          "INPUT", false, false, false));
  registerTableFunction(
      name,
      argSpecs,
      std::make_shared<GenericTableReturnTypeSpecification>(),
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
  if (!inputTable || inputTable->size() == 0) {
    return std::make_shared<TableFunctionResult>(
        TableFunctionResult::TableFunctionState::kFinished);
  }

  auto numRows = inputTable->size();
  auto count = handle_->count();
  auto totalOutputRows = numRows * count;

  // Create index column - repeat each row index 'count' times
  auto indexColumn = BaseVector::create<FlatVector<int32_t>>(INTEGER(), totalOutputRows, pool());
  auto* rawIndices = indexColumn->mutableRawValues();

  for (int64_t round = 0; round < count; round++) {
    for (int64_t i = 0; i < numRows; i++) {
      rawIndices[round * numRows + i] = i;
    }
  }

  // Create output RowVector
  auto rowType = ROW({INTEGER()});
  std::vector<VectorPtr> children = {indexColumn};
  RowVectorPtr outputTable = std::make_shared<RowVector>(
      pool(), rowType, BufferPtr(nullptr), totalOutputRows, std::move(children));

  return std::make_shared<TableFunctionResult>(true, std::move(outputTable));
}

std::unique_ptr<RepeatFunctionAnalysis> RepeatFunction::analyze(
    const std::unordered_map<std::string, std::shared_ptr<Argument>>& args) {
  auto input = std::dynamic_pointer_cast<TableArgument>(args.at("INPUT"));

  auto countArg = args.at("COUNT");
  auto countPtr = std::dynamic_pointer_cast<ScalarArgument>(countArg);

  auto count = countPtr->value()->as<ConstantVector<int64_t>>()->valueAt(0);

  // For ONLY_PASS_THROUGH, per spec, function must require at least one column (index 0)
  RequiredColumnsMap requiredColumns{{"INPUT", {0}}};

  auto analysis = std::make_unique<RepeatFunctionAnalysis>();
  analysis->tableFunctionHandle_ =
      std::make_shared<RepeatFunctionHandle>(count);
  analysis->requiredColumns_ = requiredColumns;
  return analysis;
}

void registerRepeatFunction(const std::string& name) {
  TableArgumentSpecList argSpecs;
  argSpecs.push_back(
      std::make_shared<TableArgumentSpecification>(
          "INPUT", false, false, true));
  argSpecs.push_back(
      std::make_shared<ScalarArgumentSpecification>("COUNT", BIGINT(), false, "2"));
  registerTableFunction(
      name,
      argSpecs,
      std::make_shared<OnlyPassThroughReturnTypeSpecification>(),
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

std::shared_ptr<TableFunctionResult> IdentityPassThroughFunctionDataProcessor::apply(
    const std::vector<velox::RowVectorPtr>& input) {
  auto inputTable = input.at(0);
  if (!inputTable || inputTable->size() == 0) {
    return std::make_shared<TableFunctionResult>(
        TableFunctionResult::TableFunctionState::kFinished);
  }
  
  // Handle null input table (happens when partition has 0 rows)
  auto numRows = inputTable ? inputTable->size() : 0;

  // Create index column with identity mapping (0, 1, 2, ...)
  auto indexColumn = BaseVector::create<FlatVector<int32_t>>(INTEGER(), numRows, pool());

  if (numRows > 0) {
    auto* rawIndices = indexColumn->mutableRawValues();
    std::iota(rawIndices, rawIndices + numRows, 0);
  }

  // Create output RowVector
  auto rowType = ROW({INTEGER()});
  std::vector<VectorPtr> children = {indexColumn};
  RowVectorPtr outputTable = std::make_shared<RowVector>(
      pool(), rowType, BufferPtr(nullptr), numRows, std::move(children));

  return std::make_shared<TableFunctionResult>(true, std::move(outputTable));
}

std::unique_ptr<IdentityPassThroughFunctionAnalysis> IdentityPassThroughFunction::analyze(
    const std::unordered_map<std::string, std::shared_ptr<Argument>>& args) {
  auto input = std::dynamic_pointer_cast<TableArgument>(args.at("INPUT"));

  // Per spec, function must require at least one column (index 0)
  RequiredColumnsMap requiredColumns{{"INPUT", {0}}};

  auto analysis = std::make_unique<IdentityPassThroughFunctionAnalysis>();
  analysis->tableFunctionHandle_ = std::make_shared<IdentityPassThroughFunctionHandle>();
  analysis->requiredColumns_ = requiredColumns;
  return analysis;
}

void registerIdentityPassThroughFunction(const std::string& name) {
  TableArgumentSpecList argSpecs;
  argSpecs.push_back(
      std::make_shared<TableArgumentSpecification>(
          "INPUT", false, false, true));
  registerTableFunction(
      name,
      argSpecs,
      std::make_shared<OnlyPassThroughReturnTypeSpecification>(),
      IdentityPassThroughFunction::analyze,
      [](const TableFunctionHandlePtr& handle,
         memory::MemoryPool* pool,
         HashStringAllocator* stringAllocator,
         const velox::core::QueryConfig& config)
          -> std::unique_ptr<TableFunctionDataProcessor> {
        return std::make_unique<IdentityPassThroughFunctionDataProcessor>(
            dynamic_cast<const IdentityPassThroughFunctionHandle*>(handle.get()), pool);
      });
}

std::shared_ptr<TableFunctionResult> EmptyOutputFunctionDataProcessor::apply(
    const std::vector<velox::RowVectorPtr>& input) {
  auto inputTable = input.at(0);
  if (!inputTable || inputTable->size() == 0) {
    return std::make_shared<TableFunctionResult>(
        TableFunctionResult::TableFunctionState::kFinished);
  }
  
  // Return empty output (0 rows, 1 BOOLEAN column) for each input page
  // This matches the Java implementation which returns an empty Page
  auto rowType = ROW({BOOLEAN()});
  std::vector<VectorPtr> children = {
      BaseVector::create<FlatVector<bool>>(BOOLEAN(), 0, pool())};
  RowVectorPtr outputTable = std::make_shared<RowVector>(
      pool(), rowType, BufferPtr(nullptr), 0, std::move(children));

  return std::make_shared<TableFunctionResult>(true, std::move(outputTable));
}

std::unique_ptr<EmptyOutputFunctionAnalysis> EmptyOutputFunction::analyze(
    const std::unordered_map<std::string, std::shared_ptr<Argument>>& args) {
  auto input = std::dynamic_pointer_cast<TableArgument>(args.at("INPUT"));

  // Require all input columns (per Java implementation)
  std::vector<column_index_t> requiredColsList;
  for (size_t i = 0; i < input->rowType()->size(); i++) {
    requiredColsList.push_back(i);
  }
  RequiredColumnsMap requiredColumns;
  requiredColumns.push_back({"INPUT", requiredColsList});

  auto analysis = std::make_unique<EmptyOutputFunctionAnalysis>();
  analysis->tableFunctionHandle_ = std::make_shared<EmptyOutputFunctionHandle>();
  analysis->requiredColumns_ = requiredColumns;
  return analysis;
}

void registerEmptyOutputFunction(const std::string& name) {
  TableArgumentSpecList argSpecs;
  argSpecs.push_back(
      std::make_shared<TableArgumentSpecification>(
          "INPUT", false, false, false));  // rowSemantics=false, pruneWhenEmpty=false (keepWhenEmpty), passThroughColumns=false
  
  // Create descriptor for the return type: single BOOLEAN column named "column"
  std::vector<std::string> returnNames = {"column"};
  std::vector<TypePtr> returnTypes = {BOOLEAN()};
  auto descriptor = std::make_shared<Descriptor>(returnNames, returnTypes);
  
  registerTableFunction(
      name,
      argSpecs,
      std::make_shared<DescribedTableReturnTypeSpecification>(descriptor),
      EmptyOutputFunction::analyze,
      [](const TableFunctionHandlePtr& handle,
         memory::MemoryPool* pool,
         HashStringAllocator* stringAllocator,
         const velox::core::QueryConfig& config)
          -> std::unique_ptr<TableFunctionDataProcessor> {
        return std::make_unique<EmptyOutputFunctionDataProcessor>(
            dynamic_cast<const EmptyOutputFunctionHandle*>(handle.get()), pool);
      });
}

std::shared_ptr<TableFunctionResult> EmptyOutputWithPassThroughFunctionDataProcessor::apply(
    const std::vector<velox::RowVectorPtr>& input) {
  auto inputTable = input.at(0);
  
  // Handle null input (empty partition) or when all the input is seen.
  if (!inputTable || inputTable->size() == 0) {
    return std::make_shared<TableFunctionResult>(
        TableFunctionResult::TableFunctionState::kFinished);
  }
  
  // Return empty output with 2 columns: 1 proper BOOLEAN + 1 pass-through BIGINT index
  // This matches Java which returns a Page with 2 blocks, even though the return type
  // specification only declares 1 proper column. The second column is the pass-through index.
  auto rowType = ROW({BOOLEAN(), BIGINT()});
  std::vector<VectorPtr> children = {
      BaseVector::create<FlatVector<bool>>(BOOLEAN(), 0, pool()),
      BaseVector::create<FlatVector<int64_t>>(BIGINT(), 0, pool())};
  RowVectorPtr outputTable = std::make_shared<RowVector>(
      pool(), rowType, BufferPtr(nullptr), 0, std::move(children));

  return std::make_shared<TableFunctionResult>(true, std::move(outputTable));
}

std::unique_ptr<EmptyOutputWithPassThroughFunctionAnalysis> EmptyOutputWithPassThroughFunction::analyze(
    const std::unordered_map<std::string, std::shared_ptr<Argument>>& args) {
  auto input = std::dynamic_pointer_cast<TableArgument>(args.at("INPUT"));

  // Require all input columns (per Java implementation)
  std::vector<column_index_t> requiredColsList;
  for (size_t i = 0; i < input->rowType()->size(); i++) {
    requiredColsList.push_back(i);
  }
  RequiredColumnsMap requiredColumns;
  requiredColumns.push_back({"INPUT", requiredColsList});

  // Return type is specified in DescribedTableReturnTypeSpecification at registration
  // Do not set returnType_ here to avoid ambiguity
  // The framework will automatically add pass-through columns.
  auto analysis = std::make_unique<EmptyOutputWithPassThroughFunctionAnalysis>();
  analysis->tableFunctionHandle_ = std::make_shared<EmptyOutputWithPassThroughFunctionHandle>();
  analysis->requiredColumns_ = requiredColumns;
  return analysis;
}

void registerEmptyOutputWithPassThroughFunction(const std::string& name) {
  TableArgumentSpecList argSpecs;
  argSpecs.push_back(
      std::make_shared<TableArgumentSpecification>(
          "INPUT", false, false, true));  // rowSemantics=false, pruneWhenEmpty=false (keepWhenEmpty), passThroughColumns=true
  
  // Create descriptor for the return type: single BOOLEAN column named "column"
  // The framework will automatically add pass-through columns
  std::vector<std::string> returnNames = {"column"};
  std::vector<TypePtr> returnTypes = {BOOLEAN()};
  auto descriptor = std::make_shared<Descriptor>(returnNames, returnTypes);
  
  registerTableFunction(
      name,
      argSpecs,
      std::make_shared<DescribedTableReturnTypeSpecification>(descriptor),
      EmptyOutputWithPassThroughFunction::analyze,
      [](const TableFunctionHandlePtr& handle,
         memory::MemoryPool* pool,
         HashStringAllocator* stringAllocator,
         const velox::core::QueryConfig& config)
          -> std::unique_ptr<TableFunctionDataProcessor> {
        return std::make_unique<EmptyOutputWithPassThroughFunctionDataProcessor>(
            dynamic_cast<const EmptyOutputWithPassThroughFunctionHandle*>(handle.get()), pool);
      });
}

// EmptySourceFunction implementation
std::shared_ptr<TableFunctionResult> EmptySourceFunctionSplitProcessor::apply(
    const TableSplitHandlePtr& split) {
  // Return kFinished state to indicate we're done (no rows to produce)
  // This matches the Java implementation which returns empty results
  return std::make_shared<TableFunctionResult>(
      TableFunctionResult::TableFunctionState::kFinished);
}

std::vector<TableSplitHandlePtr> EmptySourceFunction::getSplits(
    const TableFunctionHandlePtr& handle) {
  // Return empty vector - no splits means no rows
  return std::vector<TableSplitHandlePtr>();
}

std::unique_ptr<EmptySourceFunctionAnalysis> EmptySourceFunction::analyze(
    const std::unordered_map<std::string, std::shared_ptr<Argument>>& args) {
  // No input arguments for source function
  
  // Return type is specified in DescribedTableReturnTypeSpecification at registration
  // Do not set returnType_ here to avoid ambiguity
  auto analysis = std::make_unique<EmptySourceFunctionAnalysis>();
  analysis->tableFunctionHandle_ = std::make_shared<EmptySourceFunctionHandle>();
  // No required columns since there are no input arguments
  return analysis;
}

void registerEmptySourceFunction(const std::string& name) {
  // Source functions have no table arguments
  TableArgumentSpecList argSpecs;
  
  // Create descriptor for the return type: single BOOLEAN column named "column"
  std::vector<std::string> returnNames = {"column"};
  std::vector<TypePtr> returnTypes = {BOOLEAN()};
  auto descriptor = std::make_shared<Descriptor>(returnNames, returnTypes);
  
  registerTableFunction(
      name,
      argSpecs,
      std::make_shared<DescribedTableReturnTypeSpecification>(descriptor),
      EmptySourceFunction::analyze,
      TableFunction::defaultCreateDataProcessor,  // No data processor
      [](const TableFunctionHandlePtr& handle,
         memory::MemoryPool* pool,
         HashStringAllocator* stringAllocator,
         const velox::core::QueryConfig& config)
          -> std::unique_ptr<TableFunctionSplitProcessor> {
        return std::make_unique<EmptySourceFunctionSplitProcessor>(
            dynamic_cast<const EmptySourceFunctionHandle*>(handle.get()), pool);
      },
      EmptySourceFunction::getSplits);  // Add getSplits function
}

// ConstantFunction implementation
std::shared_ptr<TableFunctionResult> ConstantFunctionSplitProcessor::apply(
    const TableSplitHandlePtr& split) {
  bool usedData = false;

  // NOTE: The C++ framework passes the split on every apply() call until kFinished is returned.
  // This differs from Java where the split is passed once and subsequent calls receive null.
  // We use initialized_ flag to ensure we only process the split data once, preventing
  // state variables (fullPagesCount_, processedPages_, reminder_) from resetting on each call.
  if (split != nullptr && !initialized_) {
    auto constantSplit = dynamic_cast<const ConstantFunctionSplitHandle*>(split.get());
    int32_t count = constantSplit->count();
    fullPagesCount_ = count / PAGE_SIZE;
    reminder_ = count % PAGE_SIZE;
    initialized_ = true;
    if (fullPagesCount_ > 0) {
      auto flatVector = BaseVector::create<FlatVector<int32_t>>(INTEGER(), PAGE_SIZE, pool());
      
      if (!handle_->value().has_value()) {
        // Set all values to null
        for (int32_t i = 0; i < PAGE_SIZE; i++) {
          flatVector->setNull(i, true);
        }
      } else {
        // Set all values to the constant
        auto* rawValues = flatVector->mutableRawValues();
        int32_t value = static_cast<int32_t>(handle_->value().value());
        for (int32_t i = 0; i < PAGE_SIZE; i++) {
          rawValues[i] = value;
        }
      }
      block_ = flatVector;
    } else {
      auto flatVector = BaseVector::create<FlatVector<int32_t>>(INTEGER(), reminder_, pool());
      
      if (!handle_->value().has_value()) {
        // Set all values to null
        for (int32_t i = 0; i < reminder_; i++) {
          flatVector->setNull(i, true);
        }
      } else {
        // Set all values to the constant
        auto* rawValues = flatVector->mutableRawValues();
        int32_t value = static_cast<int32_t>(handle_->value().value());
        for (int32_t i = 0; i < reminder_; i++) {
          rawValues[i] = value;
        }
      }
      block_ = flatVector;
    }
    usedData = true;
  }

  if (processedPages_ < fullPagesCount_) {
    processedPages_++;
    auto rowType = ROW({INTEGER()});
    std::vector<VectorPtr> children = {block_};
    RowVectorPtr outputTable = std::make_shared<RowVector>(
        pool(), rowType, BufferPtr(nullptr), PAGE_SIZE, std::move(children));
    
    if (usedData) {
      return std::make_shared<TableFunctionResult>(true, std::move(outputTable));
    }
    return std::make_shared<TableFunctionResult>(false, std::move(outputTable));
  }

  if (reminder_ > 0) {
    // If fullPagesCount_ > 0, we need to slice the PAGE_SIZE block to get the reminder
    // If fullPagesCount_ == 0, the block is already reminder_ size, so use it directly
    VectorPtr outputVector;
    if (fullPagesCount_ > 0) {
      outputVector = block_->slice(0, reminder_);
    } else {
      outputVector = block_;
    }
    
    auto rowType = ROW({INTEGER()});
    std::vector<VectorPtr> children = {outputVector};
    RowVectorPtr outputTable = std::make_shared<RowVector>(
        pool(), rowType, BufferPtr(nullptr), reminder_, std::move(children));
    reminder_ = 0;
    
    if (usedData) {
      return std::make_shared<TableFunctionResult>(true, std::move(outputTable));
    }
    return std::make_shared<TableFunctionResult>(false, std::move(outputTable));
  }

  return std::make_shared<TableFunctionResult>(
      TableFunctionResult::TableFunctionState::kFinished);
}

std::vector<TableSplitHandlePtr> ConstantFunction::getSplits(
    const TableFunctionHandlePtr& handle) {
  auto constantHandle = dynamic_cast<const ConstantFunctionHandle*>(handle.get());
  constexpr int32_t DEFAULT_SPLIT_SIZE = 5500;
  
  std::vector<TableSplitHandlePtr> splits;
  int32_t count = constantHandle->count();
  
  for (int32_t i = 0; i < count / DEFAULT_SPLIT_SIZE; i++) {
    splits.push_back(std::make_shared<ConstantFunctionSplitHandle>(DEFAULT_SPLIT_SIZE));
  }
  
  int32_t remainingSize = count % DEFAULT_SPLIT_SIZE;
  if (remainingSize > 0) {
    splits.push_back(std::make_shared<ConstantFunctionSplitHandle>(remainingSize));
  }
  
  return splits;
}

std::unique_ptr<ConstantFunctionAnalysis> ConstantFunction::analyze(
    const std::unordered_map<std::string, std::shared_ptr<Argument>>& args) {
  // Get VALUE argument (can be null)
  auto valueArg = std::dynamic_pointer_cast<ScalarArgument>(args.at("VALUE"));
  std::optional<int32_t> value;
  
  // Check if the value vector exists and is not null at position 0
  // INTEGER type in Presto maps to int32_t in Velox
  auto valueVector = valueArg->value();
  if (valueVector) {
    auto constantVec = valueVector->as<ConstantVector<int32_t>>();
    if (constantVec && !constantVec->isNullAt(0)) {
      value = constantVec->valueAt(0);
    }
  }
  
  // Get N argument (count) - also INTEGER type (int32_t)
  auto countArg = std::dynamic_pointer_cast<ScalarArgument>(args.at("N"));
  auto countVector = countArg->value();
  VELOX_CHECK_NOT_NULL(countVector, "count value for function constant() is null");
  int32_t count = countVector->as<ConstantVector<int32_t>>()->valueAt(0);
  
  VELOX_CHECK_GT(count, 0, "count value for function constant() must be positive");
  
  auto analysis = std::make_unique<ConstantFunctionAnalysis>();
  analysis->tableFunctionHandle_ = std::make_shared<ConstantFunctionHandle>(value, count);
  return analysis;
}

void registerConstantFunction(const std::string& name) {
  // Source functions have no table arguments
  TableArgumentSpecList argSpecs;
  argSpecs.push_back(
      std::make_shared<ScalarArgumentSpecification>("VALUE", INTEGER(), false));
  argSpecs.push_back(
      std::make_shared<ScalarArgumentSpecification>("N", INTEGER(), false, "1"));
  
  // Create descriptor for the return type: single INTEGER column named "constant_column"
  std::vector<std::string> returnNames = {"constant_column"};
  std::vector<TypePtr> returnTypes = {INTEGER()};
  auto descriptor = std::make_shared<Descriptor>(returnNames, returnTypes);
  
  registerTableFunction(
      name,
      argSpecs,
      std::make_shared<DescribedTableReturnTypeSpecification>(descriptor),
      ConstantFunction::analyze,
      TableFunction::defaultCreateDataProcessor,  // No data processor
      [](const TableFunctionHandlePtr& handle,
         memory::MemoryPool* pool,
         HashStringAllocator* stringAllocator,
         const velox::core::QueryConfig& config)
          -> std::unique_ptr<TableFunctionSplitProcessor> {
        return std::make_unique<ConstantFunctionSplitProcessor>(
            dynamic_cast<const ConstantFunctionHandle*>(handle.get()), pool);
      },
      ConstantFunction::getSplits);
}

// TestSingleInputFunction implementation
TestSingleInputFunctionDataProcessor::TestSingleInputFunctionDataProcessor(
    const TestSingleInputFunctionHandle* handle,
    memory::MemoryPool* pool)
    : TableFunctionDataProcessor("test_single_input", pool, nullptr),
      handle_(handle) {
  // Pre-build the result Page once (matching Java behavior)
  // The Java implementation creates the result Page once in the processor provider
  // and reuses it for every input
  auto boolColumn = BaseVector::create<FlatVector<bool>>(BOOLEAN(), 1, pool);
  auto* rawBools = boolColumn->mutableRawValues();
  rawBools[0] = true;
  
  auto rowType = ROW({BOOLEAN()});
  std::vector<VectorPtr> children = {boolColumn};
  result_ = std::make_shared<RowVector>(
      pool, rowType, BufferPtr(nullptr), 1, std::move(children));
}

std::shared_ptr<TableFunctionResult> TestSingleInputFunctionDataProcessor::apply(
    const std::vector<velox::RowVectorPtr>& input) {
  auto inputTable = input.at(0);
  
  // Handle null input - return FINISHED
  if (!inputTable) {
    return std::make_shared<TableFunctionResult>(
        TableFunctionResult::TableFunctionState::kFinished);
  }
  
  // Return the pre-built result (same Page for every input)
  // This matches the Java behavior where the same Page is reused
  return std::make_shared<TableFunctionResult>(true, result_);
}

std::unique_ptr<TestSingleInputFunctionAnalysis> TestSingleInputFunction::analyze(
    const std::unordered_map<std::string, std::shared_ptr<Argument>>& args) {
  auto input = std::dynamic_pointer_cast<TableArgument>(args.at("INPUT"));
  
  // Require all input columns (per Java implementation)
  std::vector<column_index_t> requiredColsList;
  for (size_t i = 0; i < input->rowType()->size(); i++) {
    requiredColsList.push_back(i);
  }
  RequiredColumnsMap requiredColumns;
  requiredColumns.push_back({"INPUT", requiredColsList});
  
  auto analysis = std::make_unique<TestSingleInputFunctionAnalysis>();
  analysis->tableFunctionHandle_ = std::make_shared<TestSingleInputFunctionHandle>();
  analysis->requiredColumns_ = requiredColumns;
  return analysis;
}

void registerTestSingleInputFunction(const std::string& name) {
  TableArgumentSpecList argSpecs;
  argSpecs.push_back(
      std::make_shared<TableArgumentSpecification>(
          "INPUT", true, true, false));  // rowSemantics=true, pruneWhenEmpty=true, passThroughColumns=false
  
  // Create descriptor for the return type: single BOOLEAN column named "boolean_result"
  std::vector<std::string> returnNames = {"boolean_result"};
  std::vector<TypePtr> returnTypes = {BOOLEAN()};
  auto descriptor = std::make_shared<Descriptor>(returnNames, returnTypes);
  
  registerTableFunction(
      name,
      argSpecs,
      std::make_shared<DescribedTableReturnTypeSpecification>(descriptor),
      TestSingleInputFunction::analyze,
      [](const TableFunctionHandlePtr& handle,
         memory::MemoryPool* pool,
         HashStringAllocator* stringAllocator,
         const velox::core::QueryConfig& config)
          -> std::unique_ptr<TableFunctionDataProcessor> {
        return std::make_unique<TestSingleInputFunctionDataProcessor>(
            dynamic_cast<const TestSingleInputFunctionHandle*>(handle.get()), pool);
      });
}

// PassThroughInputFunction implementation
std::shared_ptr<TableFunctionResult> PassThroughInputFunctionDataProcessor::apply(
    const std::vector<velox::RowVectorPtr>& input) {
  if (finished_) {
    return std::make_shared<TableFunctionResult>(
        TableFunctionResult::TableFunctionState::kFinished);
  }
  
  // If input is null, we're done processing - produce final result
  // Only check for null pointers, NOT empty vectors (0 rows just means no data in this batch)
  bool input0Done = (input[0] == nullptr);
  bool input1Done = (input.size() < 2 || input[1] == nullptr);
  
  if (input.empty() || (input0Done && input1Done)) {
    finished_ = true;
    
    // Create proper columns: input_1_present and input_2_present
    auto input1PresentColumn = BaseVector::create<FlatVector<bool>>(BOOLEAN(), 1, pool());
    auto input2PresentColumn = BaseVector::create<FlatVector<bool>>(BOOLEAN(), 1, pool());
    input1PresentColumn->set(0, input1Present_);
    input2PresentColumn->set(0, input2Present_);
    
    // Create pass-through index columns (must be INTEGER/int32_t for Velox)
    auto input1PassThroughColumn = BaseVector::create<FlatVector<int32_t>>(INTEGER(), 1, pool());
    auto input2PassThroughColumn = BaseVector::create<FlatVector<int32_t>>(INTEGER(), 1, pool());
    
    if (input1Present_) {
      input1PassThroughColumn->set(0, input1EndIndex_ - 1);
    } else {
      input1PassThroughColumn->setNull(0, true);
    }
    
    if (input2Present_) {
      input2PassThroughColumn->set(0, input2EndIndex_ - 1);
    } else {
      input2PassThroughColumn->setNull(0, true);
    }
    
    // Create output with 4 columns: 2 proper + 2 pass-through indices
    auto rowType = ROW({BOOLEAN(), BOOLEAN(), INTEGER(), INTEGER()});
    std::vector<VectorPtr> children = {
        input1PresentColumn,
        input2PresentColumn,
        input1PassThroughColumn,
        input2PassThroughColumn};
    RowVectorPtr outputTable = std::make_shared<RowVector>(
        pool(), rowType, BufferPtr(nullptr), 1, std::move(children));

    return std::make_shared<TableFunctionResult>(false, std::move(outputTable));
  }
  
  auto countNonNullRows = [](const RowVectorPtr& vec) -> int {
    if (!vec || vec->size() == 0) return 0;
    auto firstCol = vec->childAt(0);
    int count = 0;
    for (int i = 0; i < vec->size(); i++) {
      if (!firstCol->isNullAt(i)) {
        count++;
      }
    }
    return count;
  };
  
  if (input[0] != nullptr && input[0]->size() > 0) {
    input1Present_ = true;
    input1EndIndex_ += countNonNullRows(input[0]);
  }
  
  if (input.size() > 1 && input[1] != nullptr && input[1]->size() > 0) {
    input2Present_ = true;
    input2EndIndex_ += countNonNullRows(input[1]);
  }

  return std::make_shared<TableFunctionResult>(true, nullptr);
}

std::unique_ptr<PassThroughInputFunctionAnalysis> PassThroughInputFunction::analyze(
    const std::unordered_map<std::string, std::shared_ptr<Argument>>& args) {
  RequiredColumnsMap requiredColumns;
  
  for (const auto& inputName : {"INPUT_1", "INPUT_2"}) {
    auto it = args.find(inputName);
    if (it != args.end()) {
      auto tableArg = std::dynamic_pointer_cast<TableArgument>(it->second);
      if (tableArg) {
        requiredColumns.push_back({inputName, {0}});
      }
    }
  }
  
  auto analysis = std::make_unique<PassThroughInputFunctionAnalysis>();
  analysis->tableFunctionHandle_ = std::make_shared<PassThroughInputFunctionHandle>();
  analysis->requiredColumns_ = requiredColumns;
  return analysis;
}

void registerPassThroughInputFunction(const std::string& name) {
  TableArgumentSpecList argSpecs;
  argSpecs.push_back(
      std::make_shared<TableArgumentSpecification>(
          "INPUT_1", false, false, true));
  argSpecs.push_back(
      std::make_shared<TableArgumentSpecification>(
          "INPUT_2", false, false, true));
  
  std::vector<std::string> returnNames = {"input_1_present", "input_2_present"};
  std::vector<TypePtr> returnTypes = {BOOLEAN(), BOOLEAN()};
  auto descriptor = std::make_shared<Descriptor>(returnNames, returnTypes);
  
  registerTableFunction(
      name,
      argSpecs,
      std::make_shared<DescribedTableReturnTypeSpecification>(descriptor),
      PassThroughInputFunction::analyze,
      [](const TableFunctionHandlePtr& handle,
         memory::MemoryPool* pool,
         HashStringAllocator* stringAllocator,
         const velox::core::QueryConfig& config)
          -> std::unique_ptr<TableFunctionDataProcessor> {
        return std::make_unique<PassThroughInputFunctionDataProcessor>(
            dynamic_cast<const PassThroughInputFunctionHandle*>(handle.get()), pool);
      });
}

TestInputsFunctionDataProcessor::TestInputsFunctionDataProcessor(
    const TestInputsFunctionHandle* handle,
    memory::MemoryPool* pool)
    : TableFunctionDataProcessor("test_inputs", pool, nullptr),
      handle_(handle) {
  auto boolColumn = BaseVector::create<FlatVector<bool>>(BOOLEAN(), 1, pool);
  boolColumn->mutableRawValues()[0] = true;
  
  result_ = std::make_shared<RowVector>(
      pool, ROW({BOOLEAN()}), BufferPtr(nullptr), 1,
      std::vector<VectorPtr>{boolColumn});
}

std::shared_ptr<TableFunctionResult> TestInputsFunctionDataProcessor::apply(
    const std::vector<velox::RowVectorPtr>& input) {
  auto inputTable = input.at(0);
  if (inputTable == nullptr) {
    //bool allNull = std::all_of(input.begin(), input.end(),
    //    [](const auto& vec) { return vec == nullptr; });
    return std::make_shared<TableFunctionResult>(
        TableFunctionResult::TableFunctionState::kFinished);
  }
  
  if (inputTable->size() == 0) {
    return std::make_shared<TableFunctionResult>(
        TableFunctionResult::TableFunctionState::kFinished);
  }
  
  return std::make_shared<TableFunctionResult>(true, result_);
}

std::unique_ptr<TestInputsFunctionAnalysis> TestInputsFunction::analyze(
    const std::unordered_map<std::string, std::shared_ptr<Argument>>& args) {
  RequiredColumnsMap requiredColumns;
  
  for (const auto& inputName : {"INPUT_1", "INPUT_2", "INPUT_3", "INPUT_4"}) {
    auto it = args.find(inputName);
    if (it != args.end()) {
      auto tableArg = std::dynamic_pointer_cast<TableArgument>(it->second);
      if (tableArg) {
        std::vector<column_index_t> requiredColsList;
        for (size_t i = 0; i < tableArg->rowType()->size(); i++) {
          requiredColsList.push_back(i);
        }
        requiredColumns.push_back({inputName, requiredColsList});
      }
    }
  }
  
  auto analysis = std::make_unique<TestInputsFunctionAnalysis>();
  analysis->tableFunctionHandle_ = std::make_shared<TestInputsFunctionHandle>();
  analysis->requiredColumns_ = requiredColumns;
  return analysis;
}

void registerTestInputsFunction(const std::string& name) {
  TableArgumentSpecList argSpecs;
  argSpecs.push_back(
      std::make_shared<TableArgumentSpecification>(
          "INPUT_1", true, true, false));
  argSpecs.push_back(
      std::make_shared<TableArgumentSpecification>(
          "INPUT_2", false, false, false));
  argSpecs.push_back(
      std::make_shared<TableArgumentSpecification>(
          "INPUT_3", false, false, false));
  argSpecs.push_back(
      std::make_shared<TableArgumentSpecification>(
          "INPUT_4", false, false, false));
  
  auto descriptor = std::make_shared<Descriptor>(
      std::vector<std::string>{"boolean_result"},
      std::vector<TypePtr>{BOOLEAN()});
  
  registerTableFunction(
      name,
      argSpecs,
      std::make_shared<DescribedTableReturnTypeSpecification>(descriptor),
      TestInputsFunction::analyze,
      [](const TableFunctionHandlePtr& handle,
         memory::MemoryPool* pool,
         HashStringAllocator* stringAllocator,
         const velox::core::QueryConfig& config)
          -> std::unique_ptr<TableFunctionDataProcessor> {
        return std::make_unique<TestInputsFunctionDataProcessor>(
            dynamic_cast<const TestInputsFunctionHandle*>(handle.get()), pool);
      });
}

} // namespace facebook::presto::tvf


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
#include "velox/common/base/Exceptions.h"

namespace facebook::presto::tvf {

using namespace facebook::velox;

namespace {

class ExcludeColumnsHandle : public TableFunctionHandle {
 public:
  ExcludeColumnsHandle(){};

  std::string_view name() const override {
    return "ExcludeColumnsHandle";
  }

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = fmt::format("{}", name());
    return obj;
  }

  static std::shared_ptr<ExcludeColumnsHandle> create(
      const folly::dynamic& obj,
      void* context) {
    return std::make_shared<ExcludeColumnsHandle>();
  }

  static void registerSerDe() {
    auto& registry = velox::DeserializationWithContextRegistryForSharedPtr();
    registry.Register("ExcludeColumnsHandle", ExcludeColumnsHandle::create);
  }

 private:
};

class ExcludeColumnsAnalysis : public TableFunctionAnalysis {
 public:
  explicit ExcludeColumnsAnalysis() : TableFunctionAnalysis() {}
};

static const std::string TABLE_ARGUMENT_NAME = "INPUT";
static const std::string DESCRIPTOR_ARGUMENT_NAME = "COLUMNS";

class ExcludeColumnsDataProcessor : public TableFunctionDataProcessor {
 public:
  explicit ExcludeColumnsDataProcessor(
      const ExcludeColumnsHandle* handle,
      memory::MemoryPool* pool)
      : TableFunctionDataProcessor("exclude_columns", pool, nullptr),
        handle_(handle) {}

  std::shared_ptr<TableFunctionResult> apply(
      const std::vector<RowVectorPtr>& input) override {
    auto inputTable = input.at(0);
    auto numRows = inputTable->size();
    if (numRows == 0) {
      return std::make_shared<TableFunctionResult>(
          TableFunctionResult::TableFunctionState::kFinished);
    }

    // Get a projection of non-excluded columns from inputTable.
    return std::make_shared<TableFunctionResult>(true, std::move(inputTable));
  }

 private:
  const ExcludeColumnsHandle* handle_;
};

class ExcludeColumns : public TableFunction {
 public:
  static std::unique_ptr<ExcludeColumnsAnalysis> analyze(
      const std::unordered_map<std::string, std::shared_ptr<Argument>>& args) {
    VELOX_CHECK_GT(
        args.count(DESCRIPTOR_ARGUMENT_NAME), 0, "COLUMNS arg not found");
    VELOX_CHECK_GT(args.count(TABLE_ARGUMENT_NAME), 0, "INPUT arg not found");

    auto excludedColumnsArg = args.at(DESCRIPTOR_ARGUMENT_NAME);
    VELOX_CHECK(excludedColumnsArg, "COLUMNS arg is NULL");
    auto excludedColumnsDesc =
        std::dynamic_pointer_cast<Descriptor>(excludedColumnsArg);
    VELOX_CHECK(excludedColumnsDesc, "COLUMNS arg not a descriptor");

    auto inputArg = args.at(TABLE_ARGUMENT_NAME);
    VELOX_CHECK(inputArg, "INPUT arg is NULL");
    auto inputTableArg = std::dynamic_pointer_cast<TableArgument>(inputArg);
    VELOX_CHECK(inputTableArg, "INPUT arg not a table");

    // Validate that each excluded column is found in the input table
    // and remove it.
    auto inputColumns = inputTableArg->rowType()->names();
    std::unordered_set<std::string> inputColumnsSet;
    for (const auto& col : inputColumns) {
      inputColumnsSet.insert(col);
    }
    for (const auto& col : excludedColumnsDesc->names()) {
      VELOX_CHECK_GT(
          inputColumnsSet.count(col), 0, "COLUMN {} not found in INPUT", col);
    }
    std::unordered_set<std::string> excludeColumnsSet;
    for (const auto& col : excludedColumnsDesc->names()) {
      excludeColumnsSet.insert(col);
    }

    std::vector<std::string> returnNames;
    std::vector<TypePtr> returnTypes;
    std::unordered_map<std::string, std::vector<column_index_t>>
        requiredColumns;
    requiredColumns.reserve(1);
    std::vector<column_index_t> requiredColsList;
    for (column_index_t i = 0; i < inputColumns.size(); i++) {
      if (excludeColumnsSet.count(inputColumns.at(i)) == 0) {
        // This column is not in the exclude_columns list and so is returned in
        // the output.
        returnNames.push_back(inputColumns.at(i));
        returnTypes.push_back(inputTableArg->rowType()->childAt(i));
        requiredColsList.push_back(i);
      }
    }
    requiredColumns.insert({TABLE_ARGUMENT_NAME, requiredColsList});
    auto analysis = std::make_unique<ExcludeColumnsAnalysis>();
    analysis->tableFunctionHandle_ = std::make_shared<ExcludeColumnsHandle>();
    analysis->returnType_ =
        std::make_shared<Descriptor>(returnNames, returnTypes);
    analysis->requiredColumns_ = requiredColumns;
    return std::move(analysis);
  }

  velox::RowTypePtr returnType_;
  const SelectivityVector inputSelections_;
};
} // namespace

void registerExcludeColumns(const std::string& name) {
  TableArgumentSpecList argSpecs;
  argSpecs.insert(
      std::make_shared<TableArgumentSpecification>(TABLE_ARGUMENT_NAME, true, true, false));
  argSpecs.insert(std::make_shared<DescriptorArgumentSpecification>(
      DESCRIPTOR_ARGUMENT_NAME, Descriptor({"columns"}), true));
  registerTableFunction(
      name,
      argSpecs,
      std::make_shared<GenericTableReturnType>(),
      ExcludeColumns::analyze,
      [](const TableFunctionHandlePtr& handle,
         memory::MemoryPool* pool,
         HashStringAllocator* /*stringAllocator*/,
         const core::QueryConfig& /*queryConfig*/)
          -> std::unique_ptr<TableFunctionDataProcessor> {
  auto excludeHandle = dynamic_cast<const ExcludeColumnsHandle*>(handle.get());
    return std::make_unique<ExcludeColumnsDataProcessor>(excludeHandle, pool);
  });
  ExcludeColumnsHandle::registerSerDe();
}

} // namespace facebook::presto::tvf

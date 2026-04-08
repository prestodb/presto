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
  ExcludeColumnsHandle() {};

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
    if (!inputTable || inputTable->size() == 0) {
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
    if (!excludedColumnsArg) {
      VELOX_USER_FAIL("COLUMNS descriptor is null");
    }
    auto excludedColumnsDescArg =
        std::dynamic_pointer_cast<DescriptorArgument>(excludedColumnsArg);
    if (!excludedColumnsDescArg) {
      VELOX_USER_FAIL("COLUMNS descriptor is null");
    }
    auto excludedColumnsDesc = excludedColumnsDescArg->descriptor();
    // Check if descriptor has types (not allowed)
    if (!excludedColumnsDesc.types().empty()) {
      VELOX_USER_FAIL("COLUMNS descriptor contains types");
    }

    auto inputArg = args.at(TABLE_ARGUMENT_NAME);
    VELOX_CHECK(inputArg, "INPUT arg is NULL");
    auto inputTableArg = std::dynamic_pointer_cast<TableArgument>(inputArg);
    VELOX_CHECK(inputTableArg, "INPUT arg not a table");

    // Validate that each excluded column is found in the input table
    // and collect non-existent columns for error reporting
    auto inputColumns = inputTableArg->rowType()->names();
    std::unordered_set<std::string> inputColumnsSet;
    for (const auto& col : inputColumns) {
      inputColumnsSet.insert(col);
    }

    std::vector<std::string> nonExistentColumns;
    for (const auto& col : excludedColumnsDesc.names()) {
      if (inputColumnsSet.count(col) == 0) {
        nonExistentColumns.push_back(col);
      }
    }

    VELOX_USER_CHECK(
        nonExistentColumns.empty(),
        "Excluded columns: [{}] not present in the table",
        folly::join(", ", nonExistentColumns));

    std::unordered_set<std::string> excludeColumnsSet;
    for (const auto& col : excludedColumnsDesc.names()) {
      excludeColumnsSet.insert(col);
    }

    std::vector<std::string> returnNames;
    std::vector<TypePtr> returnTypes;
    RequiredColumnsMap requiredColumns;
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

    // Check if all columns are excluded
    VELOX_USER_CHECK(!returnNames.empty(), "All columns are excluded");

    requiredColumns.push_back({TABLE_ARGUMENT_NAME, requiredColsList});
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
  argSpecs.push_back(
      std::make_shared<TableArgumentSpecification>(
          TABLE_ARGUMENT_NAME, true, true, false));
  std::vector<std::string> excludeColumnNames = {"columns"};
  auto excludeColumnsNamesDesc =
      std::make_shared<Descriptor>(excludeColumnNames);
  auto excludeColumnsArg =
      std::make_shared<DescriptorArgument>(excludeColumnsNamesDesc);
  argSpecs.push_back(
      std::make_shared<DescriptorArgumentSpecification>(
          DESCRIPTOR_ARGUMENT_NAME, excludeColumnsArg, true));
  registerTableFunction(
      name,
      argSpecs,
      std::make_shared<GenericTableReturnTypeSpecification>(),
      ExcludeColumns::analyze,
      [](const TableFunctionHandlePtr& handle,
         memory::MemoryPool* pool,
         HashStringAllocator* /*stringAllocator*/,
         const core::QueryConfig& /*queryConfig*/)
          -> std::unique_ptr<TableFunctionDataProcessor> {
        auto excludeHandle =
            dynamic_cast<const ExcludeColumnsHandle*>(handle.get());
        return std::make_unique<ExcludeColumnsDataProcessor>(
            excludeHandle, pool);
      });
  ExcludeColumnsHandle::registerSerDe();
}

} // namespace facebook::presto::tvf

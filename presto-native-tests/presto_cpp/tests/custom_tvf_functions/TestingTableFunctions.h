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

#include "presto_cpp/main/tvf/spi/TableFunction.h"

using namespace facebook::velox;

namespace facebook::presto::tvf {

// Operator-based version of SimpleTableFunction
class SimpleTableFunctionHandle : public TableFunctionHandle {
 public:
  explicit SimpleTableFunctionHandle(const std::string& columnName)
      : columnName_(columnName) {}

  std::string_view name() const override {
    return "SimpleTableFunctionHandle";
  }

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = fmt::format("{}", name());
    obj["columnName"] = columnName_;
    return obj;
  }

  static std::shared_ptr<SimpleTableFunctionHandle> create(
      const folly::dynamic& obj,
      void* context) {
    return std::make_shared<SimpleTableFunctionHandle>(
        obj["columnName"].asString());
  }

  static void registerSerDe() {
    auto& registry = velox::DeserializationWithContextRegistryForSharedPtr();
    registry.Register("SimpleTableFunctionHandle", create);
  }

  const std::string& columnName() const {
    return columnName_;
  }

 private:
  std::string columnName_;
};

class SimpleTableFunctionAnalysis : public TableFunctionAnalysis {};

class SimpleTableFunction final : public TableFunction {
 public:
  static std::unique_ptr<SimpleTableFunctionAnalysis> analyze(
      const std::unordered_map<std::string, std::shared_ptr<Argument>>& args);

  static std::vector<TableSplitHandlePtr> getSplits(
      const TableFunctionHandlePtr& handle) {
    // Return empty vector - no splits means no rows
    return std::vector<TableSplitHandlePtr>();
  }
};

void registerSimpleTableFunction(const std::string& name);

class IdentityFunctionHandle : public TableFunctionHandle {
 public:
  std::string_view name() const override {
    return "IdentityFunctionHandle";
  };

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = fmt::format("{}", name());
    return obj;
  };

  static std::shared_ptr<IdentityFunctionHandle> create(
      const folly::dynamic& obj,
      void* context) {
    return std::shared_ptr<IdentityFunctionHandle>();
  };

  static void registerSerDe() {
    auto& registry = velox::DeserializationWithContextRegistryForSharedPtr();
    registry.Register("IdentityFunctionHandle", create);
  }
};

class IdentityFunctionAnalysis : public TableFunctionAnalysis {};

class IdentityDataProcessor : public TableFunctionDataProcessor {
 public:
  explicit IdentityDataProcessor(
      const IdentityFunctionHandle* handle,
      memory::MemoryPool* pool)
      : TableFunctionDataProcessor("identity", pool, nullptr),
        handle_(handle) {}

  std::shared_ptr<TableFunctionResult> apply(
      const std::vector<velox::RowVectorPtr>& input) override;

 private:
  const IdentityFunctionHandle* handle_;
};

class IdentityFunction final : public TableFunction {
 public:
  static std::unique_ptr<IdentityFunctionAnalysis> analyze(
      const std::unordered_map<std::string, std::shared_ptr<Argument>>& args);
};

void registerIdentityFunction(const std::string& name);

class RepeatFunctionHandle : public TableFunctionHandle {
 public:
  explicit RepeatFunctionHandle(int64_t count) : count_(count) {}

  std::string_view name() const override {
    return "RepeatFunctionHandle";
  };

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = fmt::format("{}", name());
    obj["count"] = count_;
    return obj;
  };

  static std::shared_ptr<RepeatFunctionHandle> create(
      const folly::dynamic& obj,
      void* context) {
    return std::make_shared<RepeatFunctionHandle>(obj["count"].asInt());
  };

  static void registerSerDe() {
    auto& registry = velox::DeserializationWithContextRegistryForSharedPtr();
    registry.Register("RepeatFunctionHandle", create);
  }

  int64_t count() const {
    return count_;
  }

 private:
  int64_t count_;
};

class RepeatFunctionDataProcessor : public TableFunctionDataProcessor {
 public:
  RepeatFunctionDataProcessor(
      const RepeatFunctionHandle* handle,
      velox::memory::MemoryPool* pool)
      : TableFunctionDataProcessor("repeat", pool, nullptr), handle_(handle) {}

  std::shared_ptr<TableFunctionResult> apply(
      const std::vector<velox::RowVectorPtr>& input) override;

 private:
  const RepeatFunctionHandle* handle_;
};

class RepeatFunctionAnalysis : public TableFunctionAnalysis {};

class RepeatFunction final : public TableFunction {
 public:
  static std::unique_ptr<RepeatFunctionAnalysis> analyze(
      const std::unordered_map<std::string, std::shared_ptr<Argument>>& args);
};

void registerRepeatFunction(const std::string& name);

class IdentityPassThroughFunctionHandle : public TableFunctionHandle {
 public:
  std::string_view name() const override {
    return "IdentityPassThroughFunctionHandle";
  };

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = fmt::format("{}", name());
    return obj;
  };

  static std::shared_ptr<IdentityPassThroughFunctionHandle> create(
      const folly::dynamic& obj,
      void* context) {
    return std::shared_ptr<IdentityPassThroughFunctionHandle>();
  };

  static void registerSerDe() {
    auto& registry = velox::DeserializationWithContextRegistryForSharedPtr();
    registry.Register("IdentityPassThroughFunctionHandle", create);
  }
};

class IdentityPassThroughFunctionAnalysis : public TableFunctionAnalysis {};

class IdentityPassThroughFunctionDataProcessor : public TableFunctionDataProcessor {
 public:
  explicit IdentityPassThroughFunctionDataProcessor(
      const IdentityPassThroughFunctionHandle* handle,
      memory::MemoryPool* pool)
      : TableFunctionDataProcessor("identity_pass_through", pool, nullptr),
        handle_(handle) {}

  std::shared_ptr<TableFunctionResult> apply(
      const std::vector<velox::RowVectorPtr>& input) override;

 private:
  const IdentityPassThroughFunctionHandle* handle_;
};

class IdentityPassThroughFunction final : public TableFunction {
 public:
  static std::unique_ptr<IdentityPassThroughFunctionAnalysis> analyze(
      const std::unordered_map<std::string, std::shared_ptr<Argument>>& args);
};

void registerIdentityPassThroughFunction(const std::string& name);

class EmptyOutputFunctionHandle : public TableFunctionHandle {
 public:
  std::string_view name() const override {
    return "EmptyOutputFunctionHandle";
  };

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = fmt::format("{}", name());
    return obj;
  };

  static std::shared_ptr<EmptyOutputFunctionHandle> create(
      const folly::dynamic& obj,
      void* context) {
    return std::shared_ptr<EmptyOutputFunctionHandle>();
  };

  static void registerSerDe() {
    auto& registry = velox::DeserializationWithContextRegistryForSharedPtr();
    registry.Register("EmptyOutputFunctionHandle", create);
  }
};

class EmptyOutputFunctionAnalysis : public TableFunctionAnalysis {};

class EmptyOutputFunctionDataProcessor : public TableFunctionDataProcessor {
 public:
  explicit EmptyOutputFunctionDataProcessor(
      const EmptyOutputFunctionHandle* handle,
      memory::MemoryPool* pool)
      : TableFunctionDataProcessor("empty_output", pool, nullptr),
        handle_(handle) {}

  std::shared_ptr<TableFunctionResult> apply(
      const std::vector<velox::RowVectorPtr>& input) override;

 private:
  const EmptyOutputFunctionHandle* handle_;
};

class EmptyOutputFunction final : public TableFunction {
 public:
  static std::unique_ptr<EmptyOutputFunctionAnalysis> analyze(
      const std::unordered_map<std::string, std::shared_ptr<Argument>>& args);
};

void registerEmptyOutputFunction(const std::string& name);

class EmptyOutputWithPassThroughFunctionHandle : public TableFunctionHandle {
 public:
  std::string_view name() const override {
    return "EmptyOutputWithPassThroughFunctionHandle";
  };

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = fmt::format("{}", name());
    return obj;
  };

  static std::shared_ptr<EmptyOutputWithPassThroughFunctionHandle> create(
      const folly::dynamic& obj,
      void* context) {
    return std::shared_ptr<EmptyOutputWithPassThroughFunctionHandle>();
  };

  static void registerSerDe() {
    auto& registry = velox::DeserializationWithContextRegistryForSharedPtr();
    registry.Register("EmptyOutputWithPassThroughFunctionHandle", create);
  }
};

class EmptyOutputWithPassThroughFunctionAnalysis : public TableFunctionAnalysis {};

class EmptyOutputWithPassThroughFunctionDataProcessor : public TableFunctionDataProcessor {
 public:
  explicit EmptyOutputWithPassThroughFunctionDataProcessor(
      const EmptyOutputWithPassThroughFunctionHandle* handle,
      memory::MemoryPool* pool)
      : TableFunctionDataProcessor("empty_output_with_pass_through", pool, nullptr),
        handle_(handle) {}

  std::shared_ptr<TableFunctionResult> apply(
      const std::vector<velox::RowVectorPtr>& input) override;

 private:
  const EmptyOutputWithPassThroughFunctionHandle* handle_;
};

class EmptyOutputWithPassThroughFunction final : public TableFunction {
 public:
  static std::unique_ptr<EmptyOutputWithPassThroughFunctionAnalysis> analyze(
      const std::unordered_map<std::string, std::shared_ptr<Argument>>& args);
};

// EmptySourceFunction - A source function that takes no input and returns empty pages
// This is a split-based function (uses TableFunctionSplitProcessor)
class EmptySourceFunctionHandle : public TableFunctionHandle {
 public:
  std::string_view name() const override {
    return "EmptySourceFunctionHandle";
  };

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = fmt::format("{}", name());
    return obj;
  };

  static std::shared_ptr<EmptySourceFunctionHandle> create(
      const folly::dynamic& obj,
      void* context) {
    return std::shared_ptr<EmptySourceFunctionHandle>();
  };

  static void registerSerDe() {
    auto& registry = velox::DeserializationWithContextRegistryForSharedPtr();
    registry.Register("EmptySourceFunctionHandle", create);
  }
};

class EmptySourceFunctionSplitHandle : public TableSplitHandle {
 public:
  std::string_view name() const override {
    return "EmptySourceFunctionSplitHandle";
  }

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = fmt::format("{}", name());
    return obj;
  }

  static std::shared_ptr<EmptySourceFunctionSplitHandle> create(
      const folly::dynamic& obj,
      void* context) {
    return std::make_shared<EmptySourceFunctionSplitHandle>();
  }

  static void registerSerDe() {
    auto& registry = velox::DeserializationWithContextRegistryForSharedPtr();
    registry.Register("EmptySourceFunctionSplitHandle", create);
  }
};

class EmptySourceFunctionAnalysis : public TableFunctionAnalysis {};

class EmptySourceFunctionSplitProcessor : public TableFunctionSplitProcessor {
 public:
  explicit EmptySourceFunctionSplitProcessor(
      const EmptySourceFunctionHandle* handle,
      memory::MemoryPool* pool)
      : TableFunctionSplitProcessor("empty_source", pool, nullptr),
        handle_(handle) {}

  std::shared_ptr<TableFunctionResult> apply(
      const TableSplitHandlePtr& split) override;

 private:
  const EmptySourceFunctionHandle* handle_;
};

class EmptySourceFunction final : public TableFunction {
 public:
  static std::unique_ptr<EmptySourceFunctionAnalysis> analyze(
      const std::unordered_map<std::string, std::shared_ptr<Argument>>& args);
  
  static std::vector<TableSplitHandlePtr> getSplits(
      const TableFunctionHandlePtr& handle);
};

void registerEmptySourceFunction(const std::string& name);

void registerEmptyOutputWithPassThroughFunction(const std::string& name);

// ConstantFunction - A source function that generates constant values
// This is a split-based function (uses TableFunctionSplitProcessor)
class ConstantFunctionHandle : public TableFunctionHandle {
 public:
  ConstantFunctionHandle(std::optional<int32_t> value, int32_t count)
      : value_(value), count_(count) {}

  std::string_view name() const override {
    return "ConstantFunctionHandle";
  };

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = fmt::format("{}", name());
    if (value_.has_value()) {
      obj["value"] = value_.value();
    } else {
      obj["value"] = nullptr;
    }
    obj["count"] = count_;
    return obj;
  };

  static std::shared_ptr<ConstantFunctionHandle> create(
      const folly::dynamic& obj,
      void* context) {
    std::optional<int32_t> value;
    if (!obj["value"].isNull()) {
      value = obj["value"].asInt();
    }
    return std::make_shared<ConstantFunctionHandle>(value, obj["count"].asInt());
  };

  static void registerSerDe() {
    auto& registry = velox::DeserializationWithContextRegistryForSharedPtr();
    registry.Register("ConstantFunctionHandle", create);
  }

  std::optional<int32_t> value() const {
    return value_;
  }

  int32_t count() const {
    return count_;
  }

 private:
  std::optional<int32_t> value_;
  int32_t count_;
};

class ConstantFunctionSplitHandle : public TableSplitHandle {
 public:
  explicit ConstantFunctionSplitHandle(int32_t count) : count_(count) {}

  std::string_view name() const override {
    return "ConstantFunctionSplitHandle";
  }

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = fmt::format("{}", name());
    obj["count"] = count_;
    return obj;
  }

  static std::shared_ptr<ConstantFunctionSplitHandle> create(
      const folly::dynamic& obj,
      void* context) {
    return std::make_shared<ConstantFunctionSplitHandle>(obj["count"].asInt());
  }

  static void registerSerDe() {
    auto& registry = velox::DeserializationWithContextRegistryForSharedPtr();
    registry.Register("ConstantFunctionSplitHandle", create);
  }

  int32_t count() const {
    return count_;
  }

 private:
  int32_t count_;
};

class ConstantFunctionAnalysis : public TableFunctionAnalysis {};

class ConstantFunctionSplitProcessor : public TableFunctionSplitProcessor {
 public:
  ConstantFunctionSplitProcessor(
      const ConstantFunctionHandle* handle,
      memory::MemoryPool* pool)
      : TableFunctionSplitProcessor("constant", pool, nullptr),
        handle_(handle),
        fullPagesCount_(0),
        processedPages_(0),
        reminder_(0),
        initialized_(false) {}

  std::shared_ptr<TableFunctionResult> apply(
      const TableSplitHandlePtr& split) override;

 private:
  static constexpr int32_t PAGE_SIZE = 1000;
  const ConstantFunctionHandle* handle_;
  int32_t fullPagesCount_;
  int32_t processedPages_;
  int32_t reminder_;
  VectorPtr block_;
  bool initialized_;
};

class ConstantFunction final : public TableFunction {
 public:
  static std::unique_ptr<ConstantFunctionAnalysis> analyze(
      const std::unordered_map<std::string, std::shared_ptr<Argument>>& args);
  
  static std::vector<TableSplitHandlePtr> getSplits(
      const TableFunctionHandlePtr& handle);
};

void registerConstantFunction(const std::string& name);

// TestSingleInputFunction - A row semantics function that takes a table input
// and returns a single boolean column with value true for each input row
class TestSingleInputFunctionHandle : public TableFunctionHandle {
 public:
  std::string_view name() const override {
    return "TestSingleInputFunctionHandle";
  };

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = fmt::format("{}", name());
    return obj;
  };

  static std::shared_ptr<TestSingleInputFunctionHandle> create(
      const folly::dynamic& obj,
      void* context) {
    return std::shared_ptr<TestSingleInputFunctionHandle>();
  };

  static void registerSerDe() {
    auto& registry = velox::DeserializationWithContextRegistryForSharedPtr();
    registry.Register("TestSingleInputFunctionHandle", create);
  }
};

class TestSingleInputFunctionAnalysis : public TableFunctionAnalysis {};

class TestSingleInputFunctionDataProcessor : public TableFunctionDataProcessor {
 public:
  explicit TestSingleInputFunctionDataProcessor(
      const TestSingleInputFunctionHandle* handle,
      memory::MemoryPool* pool);

  std::shared_ptr<TableFunctionResult> apply(
      const std::vector<velox::RowVectorPtr>& input) override;

 private:
  const TestSingleInputFunctionHandle* handle_;
  RowVectorPtr result_;  // Pre-built result page with single 'true' value
};

class TestSingleInputFunction final : public TableFunction {
 public:
  static std::unique_ptr<TestSingleInputFunctionAnalysis> analyze(
      const std::unordered_map<std::string, std::shared_ptr<Argument>>& args);
};

void registerTestSingleInputFunction(const std::string& name);

// PassThroughInputFunction - A function with two table inputs that have pass-through columns
// Returns boolean flags indicating whether each input was present, plus pass-through indices
class PassThroughInputFunctionHandle : public TableFunctionHandle {
 public:
  std::string_view name() const override {
    return "PassThroughInputFunctionHandle";
  };

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = fmt::format("{}", name());
    return obj;
  };

  static std::shared_ptr<PassThroughInputFunctionHandle> create(
      const folly::dynamic& obj,
      void* context) {
    return std::shared_ptr<PassThroughInputFunctionHandle>();
  };

  static void registerSerDe() {
    auto& registry = velox::DeserializationWithContextRegistryForSharedPtr();
    registry.Register("PassThroughInputFunctionHandle", create);
  }
};

class PassThroughInputFunctionAnalysis : public TableFunctionAnalysis {};

class PassThroughInputFunctionDataProcessor : public TableFunctionDataProcessor {
 public:
  explicit PassThroughInputFunctionDataProcessor(
      const PassThroughInputFunctionHandle* handle,
      memory::MemoryPool* pool)
      : TableFunctionDataProcessor("pass_through", pool, nullptr),
        handle_(handle),
        input1Present_(false),
        input2Present_(false),
        input1EndIndex_(0),
        input2EndIndex_(0),
        finished_(false) {}

  std::shared_ptr<TableFunctionResult> apply(
      const std::vector<velox::RowVectorPtr>& input) override;

 private:
  const PassThroughInputFunctionHandle* handle_;
  bool input1Present_;
  bool input2Present_;
  int64_t input1EndIndex_;
  int64_t input2EndIndex_;
  bool finished_;
};

class PassThroughInputFunction final : public TableFunction {
 public:
  static std::unique_ptr<PassThroughInputFunctionAnalysis> analyze(
      const std::unordered_map<std::string, std::shared_ptr<Argument>>& args);
};

void registerPassThroughInputFunction(const std::string& name);

// TestInputsFunction - A function with four table inputs to test input partitioning
// INPUT_1 has row semantics (pruneWhenEmpty=true), INPUT_2, INPUT_3, INPUT_4 have set semantics (keepWhenEmpty=true)
// Returns a single boolean column with value true for each partition tuple processed
class TestInputsFunctionHandle : public TableFunctionHandle {
 public:
  std::string_view name() const override {
    return "TestInputsFunctionHandle";
  };

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = fmt::format("{}", name());
    return obj;
  };

  static std::shared_ptr<TestInputsFunctionHandle> create(
      const folly::dynamic& obj,
      void* context) {
    return std::shared_ptr<TestInputsFunctionHandle>();
  };

  static void registerSerDe() {
    auto& registry = velox::DeserializationWithContextRegistryForSharedPtr();
    registry.Register("TestInputsFunctionHandle", create);
  }
};

class TestInputsFunctionAnalysis : public TableFunctionAnalysis {};

class TestInputsFunctionDataProcessor : public TableFunctionDataProcessor {
 public:
  explicit TestInputsFunctionDataProcessor(
      const TestInputsFunctionHandle* handle,
      memory::MemoryPool* pool);

  std::shared_ptr<TableFunctionResult> apply(
      const std::vector<velox::RowVectorPtr>& input) override;

 private:
  const TestInputsFunctionHandle* handle_;
  RowVectorPtr result_;  // Pre-built result page with single 'true' value
};

class TestInputsFunction final : public TableFunction {
 public:
  static std::unique_ptr<TestInputsFunctionAnalysis> analyze(
      const std::unordered_map<std::string, std::shared_ptr<Argument>>& args);
};

void registerTestInputsFunction(const std::string& name);

} // namespace facebook::presto::tvf


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

#include "presto_cpp/main/tvf/spi/TableFunction.h"
#include "velox/common/base/Exceptions.h"
#include "velox/vector/FlatVector.h"

using namespace facebook::velox;

namespace facebook::presto::tvf {

namespace {

static const std::string START_ARGUMENT_NAME = "START";
static const std::string STOP_ARGUMENT_NAME = "STOP";
static const std::string STEP_ARGUMENT_NAME = "STEP";

class SequenceHandle : public TableFunctionHandle {
 public:
  SequenceHandle(int64_t start, int64_t stop, int64_t step)
      : start_(start), stop_(stop), step_(step){};

  std::string_view name() const override {
    return "SequenceHandle";
  }

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = fmt::format("{}", name());
    obj["start"] = start_;
    obj["stop"] = stop_;
    obj["step"] = step_;
    return obj;
  }

  static std::shared_ptr<SequenceHandle> create(
      const folly::dynamic& obj,
      void* context) {
    return std::make_shared<SequenceHandle>(
        obj["start"].asInt(), obj["stop"].asInt(), obj["step"].asInt());
  }

  static void registerSerDe() {
    auto& registry = velox::DeserializationWithContextRegistryForSharedPtr();
    registry.Register("SequenceHandle", SequenceHandle::create);
  }

 private:
  int64_t start_;
  int64_t stop_;
  int64_t step_;
};

class SequenceSplitHandle : public TableSplitHandle {
 public:
  SequenceSplitHandle(int64_t start, int64_t stop)
      : start_(start), stop_(stop){};

  std::string_view name() const override {
    return "SequenceSplitHandle";
  }

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = fmt::format("{}", name());
    obj["start"] = start_;
    obj["stop"] = stop_;
    return obj;
  }

  static std::shared_ptr<SequenceSplitHandle> create(
      const folly::dynamic& obj,
      void* context) {
    return std::make_shared<SequenceSplitHandle>(
        obj["start"].asInt(), obj["stop"].asInt());
  }

  static void registerSerDe() {
    auto& registry = velox::DeserializationWithContextRegistryForSharedPtr();
    registry.Register("SequenceSplitHandle", SequenceSplitHandle::create);
  }

 private:
  int64_t start_;
  int64_t stop_;
};

class SequenceAnalysis : public TableFunctionAnalysis {
 public:
  explicit SequenceAnalysis() : TableFunctionAnalysis() {}
};

class Sequence : public TableFunction {
 public:
  explicit Sequence(velox::memory::MemoryPool* pool)
      : TableFunction(pool, nullptr) {}

  static std::unique_ptr<TableFunctionAnalysis> analyze(
      const std::unordered_map<std::string, std::shared_ptr<Argument>>& args,
      const velox::core::QueryConfig& /*queryConfig*/) {
    VELOX_CHECK_GT(args.count(START_ARGUMENT_NAME), 0, "START arg not found");
    VELOX_CHECK_GT(args.count(STOP_ARGUMENT_NAME), 0, "STOP arg not found");

    auto startArg = args.at(START_ARGUMENT_NAME);
    VELOX_CHECK(startArg, "START arg is NULL");
    auto startPtr = std::dynamic_pointer_cast<ScalarArgument>(startArg);
    VELOX_CHECK(startPtr, "START arg is not a scalar");
    auto startVal =
        startPtr->value()->template as<ConstantVector<int64_t>>()->valueAt(0);

    auto stopArg = args.at(STOP_ARGUMENT_NAME);
    VELOX_CHECK(stopArg, "STOP arg is NULL");
    auto stopPtr = std::dynamic_pointer_cast<ScalarArgument>(stopArg);
    VELOX_CHECK(stopPtr, "STOP arg is not a scalar");
    auto stopVal =
        stopPtr->value()->template as<ConstantVector<int64_t>>()->valueAt(0);

    auto stepArg = args.at(STEP_ARGUMENT_NAME);
    VELOX_CHECK(stepArg, "STEP arg is NULL");
    auto stepPtr = std::dynamic_pointer_cast<ScalarArgument>(stepArg);
    VELOX_CHECK(stepPtr, "STEP arg is not a scalar");
    auto stepVal =
        stepPtr->value()->template as<ConstantVector<int64_t>>()->valueAt(0);

    auto handle = std::make_shared<SequenceHandle>(startVal, stopVal, stepVal);
    auto analysis = std::make_unique<SequenceAnalysis>();
    analysis->tableFunctionHandle_ = handle;
    return analysis;
  }

  std::shared_ptr<TableFunctionResult> apply(
      const std::shared_ptr<const TableSplitHandle>& split) override {
    auto sequenceSplit =
        std::dynamic_pointer_cast<const SequenceSplitHandle>(split);
    VELOX_CHECK(sequenceSplit, "Split was not a SequenceSplitHandle");

    // Figure the number of rows for the sequence number, and split into
    // blocks to only return x at a time.
    return std::make_shared<TableFunctionResult>(
        TableFunctionResult::TableFunctionState::kFinished);
  }
};
} // namespace

void registerSequence(const std::string& name) {
  TableArgumentSpecList argSpecs;
  argSpecs.insert(std::make_shared<ScalarArgumentSpecification>(
      START_ARGUMENT_NAME, BIGINT(), true));
  argSpecs.insert(std::make_shared<ScalarArgumentSpecification>(
      STOP_ARGUMENT_NAME, BIGINT(), true));
  // TODO : Figure how to make this an optional argument.
  argSpecs.insert(std::make_shared<ScalarArgumentSpecification>(
      STEP_ARGUMENT_NAME, BIGINT(), true));

  std::vector<std::string> names = {"sequential_number"};
  std::vector<TypePtr> types = {BIGINT()};
  auto returnType = std::make_shared<Descriptor>(names, types);

  registerTableFunction(
      name,
      argSpecs,
      std::make_shared<DescribedTableReturnType>(returnType),
      Sequence::analyze,
      [](const std::shared_ptr<const TableFunctionHandle>& handle,
         velox::memory::MemoryPool* pool,
         velox::HashStringAllocator* /*stringAllocator*/,
         const velox::core::QueryConfig& /*queryConfig*/)
          -> std::unique_ptr<TableFunction> {
        return std::make_unique<Sequence>(pool);
      });
  SequenceHandle::registerSerDe();
  SequenceSplitHandle::registerSerDe();
}

} // namespace facebook::presto::tvf

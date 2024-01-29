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

#include "velox/common/memory/Memory.h"
#include "velox/exec/Operator.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/vector/BaseVector.h"

// Test utilities, for convenience and conciseness.
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/vector/tests/utils/VectorMaker.h"

#include <folly/init/Init.h>
#include <algorithm>

using namespace facebook::velox;

// This file contains a step-by-step example of a custom user-defined operator
// using the operator extensibility API. It defines a new trivial
// "DuplicateRowOperator" that simply duplicates every input row in the output.
// It does so by wrapping inputs in a dictionary vector to increase its
// cardinality in a zero-copy manner.

// In order to define a new operator, four steps required:
//
// #1. Define a new custom plan node.
// #2. Define the new operator, extending the base exec::Operator class.
// #3. Define the plan translation logic (create operator based on plan node).
// #4. Register the plan translator.

// First, let's define a custom plan node:
class DuplicateRowNode : public core::PlanNode {
 public:
  // Our example takes a single source.
  DuplicateRowNode(const core::PlanNodeId& id, core::PlanNodePtr source)
      : PlanNode(id), sources_{std::move(source)} {}

  // Output is always the exact same as the single input source.
  const RowTypePtr& outputType() const override {
    return sources_.front()->outputType();
  }

  const std::vector<core::PlanNodePtr>& sources() const override {
    return sources_;
  }

  std::string_view name() const override {
    return "duplicate row";
  }

 private:
  // One can add details about the plan node and its metadata in a textual
  // format.
  void addDetails(std::stringstream& /* stream */) const override {}

  std::vector<core::PlanNodePtr> sources_;
};

// Second, let's define the operator. Here's where the main logic lives.
class DuplicateRowOperator : public exec::Operator {
 public:
  // The operator takes the plan node defined above plus additional env
  // parameters for execution.
  DuplicateRowOperator(
      int32_t operatorId,
      exec::DriverCtx* driverCtx,
      const std::shared_ptr<const DuplicateRowNode>& planNode)
      : Operator(
            driverCtx,
            nullptr,
            operatorId,
            planNode->id(),
            "DuplicateRow") {}

  // Called every time there's input available. We just save it in the `input_`
  // member defined in the base class, and process it on `getOutput()`.
  void addInput(RowVectorPtr input) override {
    input_ = input;
  }

  bool needsInput() const override {
    return !noMoreInput_;
  }

  // Called every time your operator needs to produce data. It processes the
  // input saved in `input_`, wraps it in a dictionary, and returns a new
  // RowVector.
  RowVectorPtr getOutput() override {
    if (!input_) {
      return nullptr;
    }

    // We move `input_` to signal the input has been processed.
    auto input = std::move(input_);

    // Creates dictionary indices with twice the size, and set duplicated
    // consecutive indices (0, 0, 1, 1, 2, 2, ...). This allows us to increase
    // the cardinality and produce output without touching the actual data. The
    // assumption is that the data could be complex/nested data types which
    // could be expensive to copy.
    const size_t outputSize = input->size() * 2;
    BufferPtr indices = AlignedBuffer::allocate<int32_t>(outputSize, pool());
    auto rawIndices = indices->asMutable<int32_t>();

    for (size_t i = 0; i < outputSize; ++i) {
      rawIndices[i] = i / 2;
    }

    // Wrap each column in a dictionary, create a new row vector, and return.
    std::vector<VectorPtr> outputChildren;
    outputChildren.reserve(input->childrenSize());

    for (const auto& child : input->children()) {
      outputChildren.push_back(BaseVector::wrapInDictionary(
          BufferPtr(), indices, outputSize, child));
    }
    return std::make_shared<RowVector>(
        pool(),
        input->type(),
        BufferPtr(),
        outputSize,
        std::move(outputChildren));
  }

  // This simple operator is never blocked.
  exec::BlockingReason isBlocked(ContinueFuture* future) override {
    return exec::BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return noMoreInput_;
  }
};

// Third, we need to define a plan translation logic to convert our custom plan
// node into our custom operator. Check `velox/exec/LocalPlanner.cpp` for more
// details.
class DuplicateRowTranslator : public exec::Operator::PlanNodeTranslator {
  std::unique_ptr<exec::Operator>
  toOperator(exec::DriverCtx* ctx, int32_t id, const core::PlanNodePtr& node) {
    if (auto dupRowNode =
            std::dynamic_pointer_cast<const DuplicateRowNode>(node)) {
      return std::make_unique<DuplicateRowOperator>(id, ctx, dupRowNode);
    }
    return nullptr;
  }
};

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};

  // Fourth, we register the custom plan translator. We're now ready to use our
  // operator in a query plan.
  exec::Operator::registerOperator(std::make_unique<DuplicateRowTranslator>());

  // From now on we will create a query plan and input dataset, execute it, an
  // assert that the output results contain the dataset properly duplicated.

  // Create a new memory pool to in this example.
  memory::MemoryManager::initialize({});
  auto pool = memory::memoryManager()->addLeafPool();

  // VectorMaker is a test utility that helps you build vectors. Shouldn't be
  // used in production.
  test::VectorMaker maker{pool.get()};

  // Create an input dataset containing two unnamed columns (INTEGER and
  // VARCHAR), and 5 records:
  auto inputRowVector = maker.rowVector({
      maker.flatVector({0, 1, 2, 3, 4}),
      maker.flatVector({"a", "b", "c", "d", "e"}),
  });

  // Create a query plan containing a ValuesNode (to let you pump input datasets
  // directly into the operator chain), and our custom plan node.
  auto plan = exec::test::PlanBuilder()
                  .values({inputRowVector})
                  .addNode([](std::string id, core::PlanNodePtr input) {
                    return std::make_shared<DuplicateRowNode>(id, input);
                  })
                  .planNode();

  // Create the expected results to ensure our plan produces the correct
  // results.
  auto expectedResults = maker.rowVector({
      maker.flatVector({0, 0, 1, 1, 2, 2, 3, 3, 4, 4}),
      maker.flatVector({"a", "a", "b", "b", "c", "c", "d", "d", "e", "e"}),
  });

  // Execute the plan above and assert it produces the results in
  // `expectedResults`.
  exec::test::AssertQueryBuilder(plan).assertResults(expectedResults);

  return 0;
}

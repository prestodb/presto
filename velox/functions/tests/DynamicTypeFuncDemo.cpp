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
#include "velox/expression/Expr.h"
#include "velox/expression/VectorFunction.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::functions {

using namespace facebook::velox::exec;

// Demo function showing how to use typeVariable("T") for dynamic type binding
// Signature: T -> T, where T can be any type determined at runtime
//
// This demonstrates Velox's powerful type system that allows functions to:
// 1. Accept arguments of any type (using type variable T)
// 2. Automatically bind the type variable to the actual input type at runtime
// 3. Ensure type consistency between input and output
class DynamicTypeFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    // Prepare the output vector with the same type as input (T -> T)
    BaseVector::ensureWritable(rows, outputType, context.pool(), result);

    // This demo expects exactly one argument
    VELOX_CHECK_EQ(args.size(), 1);

    // For demonstration purposes, we expect the input to be a row type
    // In a real implementation, this function could handle any type T
    auto* input = args[0]->as<RowVector>();
    VELOX_CHECK_NOT_NULL(input);

    // Demo validation: check that all row fields are integers
    // This shows how you can inspect the structure of dynamic types at runtime
    for (auto i = 0; i < input->childrenSize(); ++i) {
      VELOX_CHECK(input->childAt(i)->type()->isInteger());
    }

    // In a real function, you would:
    // 1. Process the input data according to your function's logic
    // 2. Populate the result vector with the computed values
    // 3. Handle null values appropriately
    //
    // For this demo, we just validate the input structure
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // This signature demonstrates the power of Velox's type variable system:
    //
    // .typeVariable("T") - Declares a generic type variable named "T"
    //   - T can be bound to ANY Velox type at runtime (INTEGER, VARCHAR, ROW,
    //   ARRAY, etc.)
    //   - The same T must be used consistently throughout the signature
    //
    // .returnType("T") - Output type matches the input type
    // .argumentType("T") - Input type can be any type, bound to T
    //
    // At runtime, Velox will:
    // 1. Examine the actual argument types passed to the function
    // 2. Bind T to the concrete type (e.g., if input is ROW(INTEGER, VARCHAR),
    // then T = ROW(INTEGER, VARCHAR))
    // 3. Validate that the function can handle this type binding
    // 4. Ensure type safety throughout execution
    return {exec::FunctionSignatureBuilder()
                .typeVariable("T") // Declare type variable T
                .returnType("T") // Return type = T (same as input)
                .argumentType("T") // First argument type = T
                .build()};
  }
};

namespace {
void registerDynamicTypeFunction() {
  exec::registerVectorFunction(
      "dynamic_row_udf",
      DynamicTypeFunction::signatures(),
      std::make_unique<DynamicTypeFunction>());
}
} // namespace

void runDynamicTypeTest(
    const VectorPtr& elementVector,
    size_t elementSize,
    size_t numElements,
    memory::MemoryPool* pool) {
  std::vector<VectorPtr> elementVectors;
  for (auto i = 0; i < numElements; ++i) {
    elementVectors.push_back(elementVector);
  }
  std::vector<std::string> names;
  names.reserve(numElements);
  std::vector<TypePtr> types;
  types.reserve(numElements);
  for (int i = 0; i < numElements; ++i) {
    names.push_back("c" + std::to_string(i));
    types.push_back(elementVector->type());
  }
  const auto expectedOutputType = ROW(std::move(names), std::move(types));

  auto rowVector = std::make_shared<RowVector>(
      pool, expectedOutputType, nullptr, elementSize, elementVectors);
  auto inputVector = std::make_shared<RowVector>(
      pool,
      ROW({"c0"}, {rowVector->type()}),
      nullptr,
      elementSize,
      std::vector<VectorPtr>{rowVector});

  // Prepare for function call
  std::shared_ptr<core::QueryCtx> queryCtx{core::QueryCtx::create()};
  core::ExecCtx execCtx{pool, queryCtx.get()};
  SelectivityVector rows(elementSize);
  auto untypedExpr = parse::parseExpr("dynamic_row_udf(c0)", {});
  auto parsedExpr =
      core::Expressions::inferTypes(untypedExpr, inputVector->type(), pool);
  exec::ExprSet exprSet({parsedExpr}, &execCtx);
  exec::EvalCtx evalCtx(&execCtx, &exprSet, inputVector.get());
  std::vector<VectorPtr> result{1};
  exprSet.eval(rows, evalCtx, result);
  VELOX_CHECK_EQ(result[0]->size(), elementSize);
  VELOX_CHECK_EQ(result[0]->type(), expectedOutputType);
}

void runDynamicTypeDemo() {
  // Initialize Velox
  memory::MemoryManager::initialize(memory::MemoryManagerOptions{});
  registerDynamicTypeFunction();
  parse::registerTypeResolver();
  auto pool = memory::MemoryManager::getInstance()->addLeafPool();

  const auto elementSize{3};
  auto elementVector = std::dynamic_pointer_cast<FlatVector<int32_t>>(
      BaseVector::create(INTEGER(), elementSize, pool.get()));
  for (auto i = 0; i < elementSize; ++i) {
    elementVector->set(i, i);
  }

  runDynamicTypeTest(elementVector, elementSize, 1, pool.get());
  runDynamicTypeTest(elementVector, elementSize, 2, pool.get());
  runDynamicTypeTest(elementVector, elementSize, 3, pool.get());
}
} // namespace facebook::velox::functions

int main(int argc, char** argv) {
  facebook::velox::functions::runDynamicTypeDemo();
  return 0;
}

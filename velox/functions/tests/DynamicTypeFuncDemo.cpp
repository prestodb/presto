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
#include "folly/Conv.h"
#include "velox/common/memory/Memory.h"
#include "velox/exec/tests/utils/QueryAssertions.h"
#include "velox/expression/Expr.h"
#include "velox/expression/VectorFunction.h"
#include "velox/expression/VectorReaders.h"
#include "velox/expression/VectorWriters.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/tests/utils/VectorMaker.h"

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
  elementVectors.reserve(numElements);
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

// Initialize Velox environment
std::shared_ptr<memory::MemoryPool> initializeVelox() {
  memory::MemoryManager::initialize(memory::MemoryManager::Options{});
  registerDynamicTypeFunction();
  parse::registerTypeResolver();
  return memory::MemoryManager::getInstance()->addLeafPool();
}

// Create original row vector with array columns
VectorPtr createOriginalRowVector(
    memory::MemoryPool* pool,
    int numRows,
    int numCols,
    int arraySize) {
  velox::test::VectorMaker vectorMaker{pool};
  std::vector<VectorPtr> arrayVectors;

  for (int col = 0; col < numCols; ++col) {
    auto arrayVector = vectorMaker.arrayVector<int64_t>(
        numRows,
        [arraySize](auto row) { return arraySize; },
        [col](auto row, auto index) {
          return row * 100 + col * 10 + index + 1;
        });
    arrayVectors.push_back(arrayVector);

    // Log each arrayVector's toString
    LOG(INFO) << "Created Column " << col << " (ArrayVector):";
    LOG(INFO) << arrayVector->toString(0, numRows);
  }

  auto rowVector =
      vectorMaker.rowVector({"col0", "col1", "col2"}, arrayVectors);

  return rowVector;
}

// Demonstrate VectorReader<DynamicRow> usage
VectorPtr readRowVector(
    const VectorPtr& originalRowVector,
    memory::MemoryPool* pool) {
  DecodedVector decoded;
  decoded.decode(*originalRowVector);
  exec::VectorReader<DynamicRow> reader(&decoded);

  velox::test::VectorMaker vectorMaker{pool};
  std::vector<VectorPtr> newArrayVectors;

  auto numRows = originalRowVector->size();
  auto numCols = originalRowVector->as<RowVector>()->childrenSize();

  for (int col = 0; col < numCols; ++col) {
    std::vector<std::vector<int64_t>> columnData;

    for (int row = 0; row < numRows; ++row) {
      auto dynamicRowView = reader[row];
      auto columnView = dynamicRowView.at(col);

      if (auto arrayView = columnView->tryCastTo<Array<int64_t>>()) {
        std::vector<int64_t> rowData;
        for (size_t j = 0; j < arrayView->size(); ++j) {
          auto value = (*arrayView)[j];
          rowData.push_back(value.value());
        }
        columnData.push_back(rowData);

        std::string arrayContent = "[";
        for (size_t j = 0; j < rowData.size(); ++j) {
          if (j > 0) {
            arrayContent += ", ";
          }
          arrayContent += folly::to<std::string>(rowData[j]);
        }
        arrayContent += "]";
        LOG(INFO) << "Read from Column " << col << " Row " << row << ": "
                  << arrayContent;
      }
    }

    auto newArrayVector = vectorMaker.arrayVector<int64_t>(columnData);
    newArrayVectors.push_back(newArrayVector);
  }

  auto newRowVector =
      vectorMaker.rowVector({"col0", "col1", "col2"}, newArrayVectors);

  LOG(INFO) << "Created new rowVector from VectorReader<DynamicRow> data:";
  LOG(INFO) << "New RowVector toString: " << newRowVector->toString(0, numRows);

  return newRowVector;
}

// Demonstrate VectorWriter<DynamicRow> usage - creates new output vector
VectorPtr writeRowVector(
    const VectorPtr& referenceRowVector,
    memory::MemoryPool* pool) {
  LOG(INFO)
      << "=== Using VectorWriter<DynamicRow> to create new output vector ===";

  auto numRows = referenceRowVector->size();
  auto numCols = referenceRowVector->as<RowVector>()->childrenSize();

  // Get array size from the first array in the reference vector
  auto firstArrayCol =
      referenceRowVector->as<RowVector>()->childAt(0)->as<ArrayVector>();
  auto arraySize = firstArrayCol->sizeAt(0);

  // Create a new output vector with the same structure as reference
  auto outputRowType = referenceRowVector->type();

  // Create new output vector
  VectorPtr outputVector;
  SelectivityVector rows(numRows);
  BaseVector::ensureWritable(rows, outputRowType, pool, outputVector);

  // Set up VectorWriter to write to the new output vector
  exec::VectorWriter<DynamicRow> dynamicWriter;
  dynamicWriter.init(*outputVector->as<RowVector>());

  // Write data using VectorWriter<DynamicRow>
  for (int row = 0; row < numRows; ++row) {
    dynamicWriter.setOffset(row);
    auto& dynamicRowWriter = dynamicWriter.current();

    // Write each column (array) for this row
    for (int col = 0; col < numCols; ++col) {
      auto& arrayWriter = dynamicRowWriter.get_writer_at(col);

      if (auto typedArrayWriter = arrayWriter.tryCastTo<Array<int64_t>>()) {
        typedArrayWriter->resize(arraySize);
        for (int i = 0; i < arraySize; ++i) {
          (*typedArrayWriter)[i] = row * 100 + col * 10 + i + 1;
        }
      }
    }

    dynamicWriter.commit();
  }
  dynamicWriter.finish();

  LOG(INFO) << "Created new rowVector using VectorWriter<DynamicRow>:";
  LOG(INFO) << "Output RowVector toString: "
            << outputVector->toString(0, numRows);

  return outputVector;
}

void runDynamicTypeDemo() {
  // Step 1: Initialize Velox environment
  auto pool = initializeVelox();

  // Step 2: Create original row vector with array columns
  const int numRows = 3;
  const int numCols = 3;
  const int arraySize = 4;
  auto originalRowVector =
      createOriginalRowVector(pool.get(), numRows, numCols, arraySize);

  // Step 3: Demonstrate VectorReader<DynamicRow> usage
  auto readerRowVector = readRowVector(originalRowVector, pool.get());

  // Step 4: Demonstrate VectorWriter<DynamicRow> usage
  auto writerRowVector = writeRowVector(originalRowVector, pool.get());

  // Step 5: Verify that VectorReader output and VectorWriter output matches
  // original
  std::vector<RowVectorPtr> readerVecs = {
      std::dynamic_pointer_cast<RowVector>(readerRowVector)};
  std::vector<RowVectorPtr> writerVecs = {
      std::dynamic_pointer_cast<RowVector>(writerRowVector)};
  std::vector<RowVectorPtr> originalVecs = {
      std::dynamic_pointer_cast<RowVector>(originalRowVector)};

  exec::test::assertEqualResults(readerVecs, writerVecs);
  exec::test::assertEqualResults(originalVecs, writerVecs);
}

} // namespace facebook::velox::functions

int main(int argc, char** argv) {
  facebook::velox::functions::runDynamicTypeDemo();
  return 0;
}

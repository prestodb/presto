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

#include "velox/exec/tests/PrestoQueryRunnerIntermediateTypeTransformTestBase.h"

namespace facebook::velox::exec::test {

void PrestoQueryRunnerIntermediateTypeTransformTestBase::test(
    const VectorPtr& vector) {
  const auto colName = "col";
  const auto input =
      makeRowVector({colName}, {transformIntermediateTypes(vector)});

  auto expr = getProjectionsToIntermediateTypes(
      vector->type(),
      std::make_shared<core::FieldAccessExpr>(
          colName,
          std::nullopt,
          std::vector<core::ExprPtr>{std::make_shared<core::InputExpr>()}),
      colName);

  core::PlanNodePtr plan =
      PlanBuilder().values({input}).projectExpressions({expr}).planNode();

  AssertQueryBuilder(plan).assertResults(makeRowVector({colName}, {vector}));
}

void PrestoQueryRunnerIntermediateTypeTransformTestBase::testDictionary(
    const VectorPtr& base) {
  // Wrap in a dictionary without nulls.
  auto dict = BaseVector::wrapInDictionary(
      nullptr, makeIndicesInReverse(100), 100, base);
  test(dict);
  // Wrap in a dictionary with some nulls.
  dict = BaseVector::wrapInDictionary(
      makeNulls(100, [](vector_size_t row) { return row % 10 == 0; }),
      makeIndicesInReverse(100),
      100,
      base);
  test(dict);
  // Wrap in a dictionary with all nulls.
  dict = BaseVector::wrapInDictionary(
      makeNulls(100, [](vector_size_t) { return true; }),
      makeIndicesInReverse(100),
      100,
      base);
  test(dict);
}

void PrestoQueryRunnerIntermediateTypeTransformTestBase::testConstant(
    const VectorPtr& base) {
  // Test a non-null constant.
  test(BaseVector::wrapInConstant(100, 0, base));
  // Test a null constant.
  test(BaseVector::createNullConstant(ARRAY(base->type()), 100, pool_.get()));
}

void PrestoQueryRunnerIntermediateTypeTransformTestBase::test(
    const TypePtr& type) {
  // No nulls.
  auto& opts = fuzzer_.getMutableOptions();
  opts.nullRatio = 0;
  auto vector = fuzzer_.fuzzFlat(type, kVectorSize);
  test(vector);
  // All nulls.
  opts.nullRatio = 1;
  vector = fuzzer_.fuzzFlat(type, kVectorSize);
  test(vector);
  // Some nulls.
  opts.nullRatio = 0.1;
  vector = fuzzer_.fuzzFlat(type, kVectorSize);
  test(vector);

  testDictionary(vector);
  testConstant(vector);
}

void PrestoQueryRunnerIntermediateTypeTransformTestBase::testArray(
    const TypePtr& type) {
  // Test array vector no nulls.
  auto& opts = fuzzer_.getMutableOptions();
  opts.nullRatio = 0;
  auto array = fuzzer_.fuzzArray(type, kVectorSize);
  test(array);

  // Test array vector all nulls.
  opts.nullRatio = 1;
  array = fuzzer_.fuzzArray(type, kVectorSize);
  test(array);

  // Test array vector some nulls.
  opts.nullRatio = 0.1;
  array = fuzzer_.fuzzArray(type, kVectorSize);
  test(array);

  testDictionary(array);
  testConstant(array);
}

void PrestoQueryRunnerIntermediateTypeTransformTestBase::testMap(
    const TypePtr& type) {
  // Test map vector no nulls.
  auto& opts = fuzzer_.getMutableOptions();
  opts.nullRatio = 0.0;
  auto map = fuzzer_.fuzzMap(type, type, kVectorSize);
  test(map);

  // Test map vector all nulls.
  opts.nullRatio = 1;
  map = fuzzer_.fuzzMap(type, type, kVectorSize);
  test(map);

  // Test map vector some nulls.
  opts.nullRatio = 0.1;
  map = fuzzer_.fuzzMap(type, type, kVectorSize);
  test(map);

  testDictionary(map);
  testConstant(map);
}

void PrestoQueryRunnerIntermediateTypeTransformTestBase::testRow(
    const TypePtr& type) {
  const auto size = 100;
  auto& opts = fuzzer_.getMutableOptions();
  opts.nullRatio = 0;
  auto field1 = fuzzer_.fuzz(type, size);
  opts.nullRatio = 0.1;
  auto field2 = fuzzer_.fuzz(type, size);
  opts.nullRatio = 1;
  auto field3 = fuzzer_.fuzz(type, size);
  const auto rowType =
      ROW({"c1", "c2", "c3"}, {field1->type(), field2->type(), field3->type()});
  // Test map vector no nulls.
  test(vectorMaker_.rowVector({field1, field2, field3}));

  // Test row vector some nulls.
  test(std::make_shared<RowVector>(
      pool_.get(),
      rowType,
      makeNulls(size, [](vector_size_t row) { return row % 10 == 0; }),
      size,
      std::vector<VectorPtr>{field1, field2, field3}));

  // Test row vector all nulls.
  test(std::make_shared<RowVector>(
      pool_.get(),
      rowType,
      makeNulls(size, [](vector_size_t) { return true; }),
      size,
      std::vector<VectorPtr>{field1, field2, field3}));

  const auto base = vectorMaker_.rowVector({field1, field2, field3});
  testDictionary(base);
  testConstant(base);
}

void PrestoQueryRunnerIntermediateTypeTransformTestBase::testArray(
    const VectorPtr& vector) {
  test(fuzzer_.fuzzArray(vector, vector->size()));
}

void PrestoQueryRunnerIntermediateTypeTransformTestBase::testMap(
    const VectorPtr& keys,
    const VectorPtr& values) {
  VELOX_DCHECK_EQ(keys->size(), values->size());
  test(fuzzer_.fuzzMap(keys, values, keys->size()));
}

void PrestoQueryRunnerIntermediateTypeTransformTestBase::testRow(
    std::vector<VectorPtr>&& vectors,
    std::vector<std::string> names) {
  auto vector_size = vectors.size();
  VELOX_DCHECK_EQ(vector_size, names.size());
  test(fuzzer_.fuzzRow(std::move(vectors), names, vector_size));
}

} // namespace facebook::velox::exec::test

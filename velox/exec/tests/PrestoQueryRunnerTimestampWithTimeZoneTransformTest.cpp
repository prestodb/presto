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

#include "velox/exec/fuzzer/PrestoQueryRunnerIntermediateTypeTransforms.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "velox/functions/prestosql/types/fuzzer_utils/TimestampWithTimeZoneInputGenerator.h"

namespace facebook::velox::exec::test {
namespace {
class PrestoQueryRunnerTimestampWithTimeZoneTransformTest
    : public functions::test::FunctionBaseTest {
 public:
  VectorPtr fuzzTimestampWithTimeZone(
      const size_t seed,
      const double nullRatio,
      const vector_size_t size) {
    auto generator =
        fuzzer::TimestampWithTimeZoneInputGenerator(seed, nullRatio);
    std::vector<std::optional<int64_t>> values;
    for (int i = 0; i < size; i++) {
      auto value = generator.generate();
      if (value.hasValue()) {
        values.emplace_back(value.value<TypeKind::BIGINT>());
      } else {
        values.emplace_back(std::nullopt);
      }
    }

    return makeNullableFlatVector(values, TIMESTAMP_WITH_TIME_ZONE());
  }

  void test(const VectorPtr& vector) {
    const auto colName = "col";
    const auto input =
        makeRowVector({colName}, {transformIntermediateOnlyType(vector)});

    auto expr = getIntermediateOnlyTypeProjectionExpr(
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

  void testDictionary(const VectorPtr& base) {
    // Wrap in a dictionary without nulls.
    test(BaseVector::wrapInDictionary(
        nullptr, makeIndicesInReverse(100), 100, base));
    // Wrap in a dictionary with some nulls.
    test(BaseVector::wrapInDictionary(
        makeNulls(100, [](vector_size_t row) { return row % 10 == 0; }),
        makeIndicesInReverse(100),
        100,
        base));
    // Wrap in a dictionary with all nulls.
    test(BaseVector::wrapInDictionary(
        makeNulls(100, [](vector_size_t) { return true; }),
        makeIndicesInReverse(100),
        100,
        base));
  }

  void testConstant(const VectorPtr& base) {
    // Test a non-null constant.
    test(BaseVector::wrapInConstant(100, 0, base));
    // Test a null constant.
    test(BaseVector::createNullConstant(
        ARRAY(TIMESTAMP_WITH_TIME_ZONE()), 100, pool_.get()));
  }
};

TEST_F(
    PrestoQueryRunnerTimestampWithTimeZoneTransformTest,
    isIntermediateOnlyType) {
  ASSERT_FALSE(isIntermediateOnlyType(BOOLEAN()));
  ASSERT_FALSE(isIntermediateOnlyType(BIGINT()));
  ASSERT_FALSE(isIntermediateOnlyType(VARCHAR()));
  ASSERT_TRUE(isIntermediateOnlyType(TIMESTAMP_WITH_TIME_ZONE()));

  ASSERT_FALSE(isIntermediateOnlyType(ARRAY(BOOLEAN())));
  ASSERT_FALSE(isIntermediateOnlyType(ARRAY(BIGINT())));
  ASSERT_FALSE(isIntermediateOnlyType(ARRAY(VARCHAR())));
  ASSERT_TRUE(isIntermediateOnlyType(ARRAY(TIMESTAMP_WITH_TIME_ZONE())));

  ASSERT_FALSE(isIntermediateOnlyType(MAP(BOOLEAN(), BIGINT())));
  ASSERT_FALSE(isIntermediateOnlyType(MAP(TIMESTAMP(), VARCHAR())));
  ASSERT_TRUE(
      isIntermediateOnlyType(MAP(TIMESTAMP_WITH_TIME_ZONE(), SMALLINT())));
  ASSERT_TRUE(
      isIntermediateOnlyType(MAP(VARBINARY(), TIMESTAMP_WITH_TIME_ZONE())));

  ASSERT_FALSE(isIntermediateOnlyType(ROW({BOOLEAN(), BIGINT()})));
  ASSERT_FALSE(
      isIntermediateOnlyType(ROW({TINYINT(), VARCHAR(), ARRAY(VARBINARY())})));
  ASSERT_TRUE(
      isIntermediateOnlyType(ROW({TIMESTAMP_WITH_TIME_ZONE(), SMALLINT()})));
  ASSERT_TRUE(isIntermediateOnlyType(
      ROW({BOOLEAN(), ARRAY(TIMESTAMP_WITH_TIME_ZONE())})));
  ASSERT_TRUE(isIntermediateOnlyType(ROW(
      {SMALLINT(),
       TIMESTAMP(),
       ARRAY(ROW({MAP(VARCHAR(), TIMESTAMP_WITH_TIME_ZONE())}))})));
}

TEST_F(
    PrestoQueryRunnerTimestampWithTimeZoneTransformTest,
    transformIntermediateOnlyTypeTimestampWithTimeZone) {
  auto seed = 0;
  // No nulls.
  test(fuzzTimestampWithTimeZone(seed++, 0, 100));
  // Some nulls.
  test(fuzzTimestampWithTimeZone(seed++, 0.1, 100));
  // All nulls.
  test(fuzzTimestampWithTimeZone(seed++, 1, 100));

  auto base = fuzzTimestampWithTimeZone(seed++, 0, 100);
  testDictionary(base);
  testConstant(base);
}

TEST_F(
    PrestoQueryRunnerTimestampWithTimeZoneTransformTest,
    transformIntermediateOnlyTypeTimestampWithTimeZoneArray) {
  auto elements = fuzzTimestampWithTimeZone(0, 0.1, 1000);
  auto size = 100;
  std::vector<vector_size_t> offsets(size + 1);
  for (int i = 0; i < size + 1; i++) {
    offsets.push_back(i * 10);
  }
  // Test array vector no nulls.
  test(vectorMaker_.arrayVector(offsets, elements));

  std::vector<vector_size_t> nulls(size / 10);
  for (int i = 0; i < size / 10; i++) {
    nulls[i] = i * 10;
  }
  // Test array vector some nulls.
  test(vectorMaker_.arrayVector(offsets, elements, nulls));

  nulls.clear();
  for (int i = 0; i < size; i++) {
    nulls.push_back(i);
  }
  // Test array vector all nulls.
  test(vectorMaker_.arrayVector(offsets, elements, nulls));

  const auto base = vectorMaker_.arrayVector(offsets, elements);
  testDictionary(base);
  testConstant(base);
}

TEST_F(
    PrestoQueryRunnerTimestampWithTimeZoneTransformTest,
    transformIntermediateOnlyTypeTimestampWithTimeZoneMap) {
  auto keys = fuzzTimestampWithTimeZone(0, 0, 1000);
  auto values = fuzzTimestampWithTimeZone(1, 0.1, 1000);
  auto size = 100;
  std::vector<vector_size_t> offsets(size + 1);
  for (int i = 0; i < size + 1; i++) {
    offsets.push_back(i * 10);
  }
  // Test map vector no nulls.
  test(vectorMaker_.mapVector(offsets, keys, values));

  std::vector<vector_size_t> nulls(size / 10);
  for (int i = 0; i < size / 10; i++) {
    nulls[i] = i * 10;
  }
  // Test map vector some nulls.
  test(vectorMaker_.mapVector(offsets, keys, values, nulls));

  nulls.clear();
  for (int i = 0; i < size; i++) {
    nulls.push_back(i);
  }
  // Test map vector all nulls.
  test(vectorMaker_.mapVector(offsets, keys, values, nulls));

  const auto base = vectorMaker_.mapVector(offsets, keys, values);
  testDictionary(base);
  testConstant(base);
}

TEST_F(
    PrestoQueryRunnerTimestampWithTimeZoneTransformTest,
    transformIntermediateOnlyTypeTimestampWithTimeZoneRow) {
  const auto size = 100;
  auto field1 = fuzzTimestampWithTimeZone(0, 0, size);
  auto field2 = fuzzTimestampWithTimeZone(1, 0.1, size);
  auto field3 = fuzzTimestampWithTimeZone(2, 1, size);
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
} // namespace
} // namespace facebook::velox::exec::test

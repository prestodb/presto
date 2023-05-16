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

#include "velox/functions/sparksql/MightContain.h"
#include "velox/common/base/BloomFilter.h"
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"

namespace facebook::velox::functions::sparksql::test {
namespace {

class MightContainTest : public SparkFunctionBaseTest {
 protected:
  void testMightContain(
      const std::optional<std::string>& serialized,
      const VectorPtr& value,
      const VectorPtr& expected) {
    // Not using `evaluate()` because the bloom filter binary cannot be parsed
    // by DuckDB parser.
    auto selected = SelectivityVector(value->size());
    std::vector<core::TypedExprPtr> args;
    if (serialized.has_value()) {
      args.push_back(std::make_shared<core::ConstantTypedExpr>(
          VARBINARY(), variant::binary(serialized.value())));
    } else {
      args.push_back(std::make_shared<core::ConstantTypedExpr>(
          VARBINARY(), variant::null(TypeKind::VARBINARY)));
    }
    args.push_back(
        std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "c0"));
    auto expr = exec::ExprSet(
        {std::make_shared<core::CallTypedExpr>(
            BOOLEAN(), args, "might_contain")},
        &execCtx_);
    auto data = makeRowVector({value});
    exec::EvalCtx evalCtx(&execCtx_, &expr, data.get());
    std::vector<VectorPtr> results(1);
    expr.eval(selected, evalCtx, results);
    velox::test::assertEqualVectors(expected, results[0]);
  }

  std::string getSerializedBloomFilter(int32_t kSize) {
    BloomFilter bloomFilter;
    bloomFilter.reset(kSize);
    for (auto i = 0; i < kSize; ++i) {
      bloomFilter.insert(folly::hasher<int64_t>()(i));
    }
    std::string data;
    data.resize(bloomFilter.serializedSize());
    bloomFilter.serialize(data.data());
    return data;
  }
};

TEST_F(MightContainTest, basic) {
  constexpr int32_t kSize = 10;
  auto serialized = getSerializedBloomFilter(kSize);
  auto value =
      makeFlatVector<int64_t>(kSize, [](vector_size_t row) { return row; });
  auto expectedContain = makeConstant(true, kSize);
  testMightContain(serialized, value, expectedContain);

  auto valueNotContain = makeFlatVector<int64_t>(
      kSize, [](vector_size_t row) { return row + 123451; });
  auto expectedNotContain = makeConstant(false, kSize);
  testMightContain(serialized, valueNotContain, expectedNotContain);

  auto values = makeNullableFlatVector<int64_t>(
      {1, 2, 3, 4, 5, std::nullopt, 123451, 23456, 4, 5});
  auto expected = makeNullableFlatVector<bool>(
      {true, true, true, true, true, std::nullopt, false, false, true, true});
  testMightContain(serialized, values, expected);
}

TEST_F(MightContainTest, nullBloomFilter) {
  auto value = makeFlatVector<int64_t>({2, 4});
  auto expected = makeNullConstant(TypeKind::BOOLEAN, value->size());
  testMightContain(std::nullopt, value, expected);
}
} // namespace
} // namespace facebook::velox::functions::sparksql::test

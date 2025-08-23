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
#include "velox/vector/VariantToVector.h"
#include <glog/logging.h>
#include <gtest/gtest.h>
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/type/Variant.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox {
namespace {

class VariantToVectorTest : public testing::Test, public test::VectorTestBase {
 public:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void testNull(const TypePtr& type) const {
    auto vector = variantToVector(type, Variant::null(type->kind()), pool());

    EXPECT_TRUE(vector->isConstantEncoding());
    EXPECT_EQ(vector->type()->toString(), type->toString());
    EXPECT_TRUE(vector->isNullAt(0));

    auto copy = vector->variantAt(0);
    EXPECT_TRUE(copy.isNull());
    EXPECT_TRUE(copy.isTypeCompatible(type));
  }

  void testValue(
      const TypePtr& type,
      const Variant& value,
      const VectorPtr& expected) {
    auto vector = variantToVector(type, value, pool());

    EXPECT_TRUE(vector->isConstantEncoding());
    EXPECT_EQ(vector->type()->toString(), type->toString());
    EXPECT_TRUE(expected->equalValueAt(vector.get(), 0, 0))
        << "Expected: " << expected->toString(0)
        << ", but got: " << vector->toString(0);

    auto copy = vector->variantAt(0);
    EXPECT_FALSE(copy.isNull());
    EXPECT_TRUE(copy.isTypeCompatible(type));
    EXPECT_EQ(copy, value);
  }
};

TEST_F(VariantToVectorTest, integer) {
  testNull(INTEGER());
  testValue(
      INTEGER(),
      Variant::create<int32_t>(123),
      makeFlatVector(std::vector<int32_t>{123}));
}

TEST_F(VariantToVectorTest, decimal) {
  auto type = DECIMAL(20, 3);
  testNull(type);

  std::vector<int128_t> data = {1000123};
  VectorPtr expected = makeFlatVector<int128_t>(data, type);
  testValue(type, Variant(data[0]), expected);

  std::vector<int128_t> arrayData = {1000123, 1000456, 10000789};

  std::vector<Variant> arrayInputData = {
      Variant(arrayData[0]), Variant(arrayData[1]), Variant(arrayData[2])};
  Variant arrayInput = Variant::array(arrayInputData);

  VELOX_ASSERT_THROW(
      variantToVector(ARRAY(type), arrayInput, pool()),
      "Type not supported: DECIMAL(20, 3)");
}

TEST_F(VariantToVectorTest, timestamp) {
  auto type = TIMESTAMP();
  auto data = util::fromTimestampString(
                  "2022-06-27 00:00:00", util::TimestampParseMode::kPrestoCast)
                  .value();

  testNull(type);
  testValue(type, Variant(data), makeFlatVector<Timestamp>({data}));
}

TEST_F(VariantToVectorTest, varchar) {
  auto type = VARCHAR();
  testNull(type);
  testValue(type, Variant("hello"), makeFlatVector<StringView>({"hello"}));
  testValue(
      type,
      Variant("non_inline_string"),
      makeFlatVector<StringView>({"non_inline_string"}));
}

TEST_F(VariantToVectorTest, array) {
  auto type = ARRAY(INTEGER());
  testNull(type);

  testValue(
      type,
      Variant::array({1, 2, 3, 4}),
      makeArrayVectorFromJson<int32_t>({"[1, 2, 3, 4]"}));

  // Empty array.
  testValue(type, Variant::array({}), makeArrayVectorFromJson<int32_t>({"[]"}));

  // Array with null elements.
  testValue(
      type,
      Variant::array({1, 2, Variant::null(TypeKind::INTEGER), 4}),
      makeArrayVectorFromJson<int32_t>({"[1, 2, null, 4]"}));
}

TEST_F(VariantToVectorTest, nestedContainers) {
  auto type = MAP(DOUBLE(), ARRAY(INTEGER()));
  testNull(type);

  auto keys = makeFlatVector<double>({1.0, 2.0});
  std::vector<std::vector<int32_t>> data = {{1, 2, 3, 4}, {2, 3, 4, 5}};
  auto values = makeArrayVector(data);
  auto expected = makeMapVector({0}, keys, values);

  auto value = Variant::map(
      {{1.0, Variant::array({1, 2, 3, 4})},
       {2.0, Variant::array({2, 3, 4, 5})}});

  testValue(type, value, expected);
}

TEST_F(VariantToVectorTest, map) {
  auto type = MAP(INTEGER(), DOUBLE());
  testNull(type);

  auto expected = makeMapVectorFromJson<int32_t, double>(
      {"{1: 1.0, 2: 2.0, 3: 3.0, 4: 4.0}"});

  auto value = Variant::map({
      {1, 1.0},
      {2, 2.0},
      {3, 3.0},
      {4, 4.0},
  });
  testValue(type, value, expected);
}

TEST_F(VariantToVectorTest, row) {
  auto type = ROW({"a", "b"}, {INTEGER(), DOUBLE()});
  testNull(type);

  auto expected = makeRowVector({
      makeFlatVector(std::vector<int32_t>{1}),
      makeFlatVector(std::vector<double>{1.0}),
  });

  testValue(type, Variant::row({1, 1.0}), expected);
}

TEST_F(VariantToVectorTest, rowOfComplexTypes) {
  auto type = ROW({"a", "b"}, {ARRAY(INTEGER()), MAP(INTEGER(), DOUBLE())});
  testNull(type);

  auto expected = makeRowVector(
      {makeArrayVectorFromJson<int32_t>({"[1, 2, 3, 4]"}),
       makeMapVectorFromJson<int32_t, double>(
           {"{1: 1.0, 2: 2.0, 3: 3.0, 4: 4.0}"})});

  auto row = Variant::row({
      Variant::array({1, 2, 3, 4}),
      Variant::map({{1, 1.0}, {2, 2.0}, {3, 3.0}, {4, 4.0}}),
  });

  testValue(type, row, expected);
}

struct OpaqueValue {
  int value;
  explicit OpaqueValue(int v) : value{v} {}
};

TEST_F(VariantToVectorTest, opaque) {
  auto type = OPAQUE<OpaqueValue>();
  testNull(type);

  auto data = std::make_shared<OpaqueValue>(1);
  auto expected = makeFlatVector<std::shared_ptr<void>>({data});

  testValue(type, Variant::opaque(data), expected);
}

} // namespace
} // namespace facebook::velox

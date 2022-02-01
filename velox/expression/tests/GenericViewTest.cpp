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
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <optional>

#include "velox/common/base/Exceptions.h"
#include "velox/expression/VectorUdfTypeSystem.h"
#include "velox/functions/prestosql/tests/FunctionBaseTest.h"

namespace facebook::velox {
namespace {
DecodedVector* decode(DecodedVector& decoder, const BaseVector& vector) {
  SelectivityVector rows(vector.size());
  decoder.decode(vector, rows);
  return &decoder;
}

class GenericViewTest : public functions::test::FunctionBaseTest {
 protected:
  using array_data_t =
      std::vector<std::optional<std::vector<std::optional<int64_t>>>>;

  array_data_t arrayData1 = {
      {{}},
      {{std::nullopt}},
      {{std::nullopt, 1}},
      {{std::nullopt, std::nullopt, std::nullopt}},
      {{0, 1, 2, 4}},
      {{99, 98}},
      {{101, std::nullopt}},
      {{10001, 12345676, std::nullopt}}, // after this index different
      {{std::nullopt, 1}},
      {{std::nullopt, 2}},
      {{std::nullopt, 3, std::nullopt}},
      {{0, 1, 2, 4}},
      {{99, 100}},
      {{101, std::nullopt, 22}},
      {{10001, 12345676, std::nullopt, 101}},
  };

  array_data_t arrayData2 = {
      {{}},
      {{std::nullopt}},
      {{std::nullopt, 1}},
      {{std::nullopt, std::nullopt, std::nullopt}},
      {{0, 1, 2, 4}},
      {{99, 98}},
      {{101, std::nullopt}},
      {{10001, 12345676, std::nullopt, 1}}, // after this index different
      {{2, 1}},
      {{std::nullopt, 3}},
      {{std::nullopt, 3, std::nullopt, 1}},
      {{0, 1, 2, 4}},
      {{99, 100, 12}},
      {{1011, std::nullopt, 22}},
      {{10001, 1, std::nullopt, 101}},
  };

  template <typename DataT>
  void testEqual(
      const VectorPtr& vector1,
      const VectorPtr& vector2,
      const DataT& data1,
      const DataT& data2) {
    DecodedVector decoded1;
    DecodedVector decoded2;
    exec::VectorReader<Generic> reader1(decode(decoded1, *vector1));
    exec::VectorReader<Generic> reader2(decode(decoded2, *vector2));

    for (auto i = 0; i < vector1->size(); i++) {
      ASSERT_EQ(data1[i].has_value(), reader1.isSet(i));
      ASSERT_EQ(data2[i].has_value(), reader2.isSet(i));
      if (data1[i].has_value() && data2[i].has_value()) {
        ASSERT_EQ(
            data1[i].value() == data2[i].value(), reader1[i] == reader2[i]);
      }
    }
  }

  void testHash(const VectorPtr& vector) {
    DecodedVector decoded;
    exec::VectorReader<Generic> reader(decode(decoded, *vector));
    for (auto i = 0; i < vector->size(); i++) {
      if (reader.isSet(i)) {
        ASSERT_EQ(
            std::hash<exec::GenericView>{}(reader[i]), vector->hashValueAt(i));
        ASSERT_EQ(reader[i].hash(), vector->hashValueAt(i));
      }
    }
  }
};

TEST_F(GenericViewTest, testInt) {
  std::vector<std::optional<int64_t>> data1 = {
      1, 2, std::nullopt, 1, std::nullopt, 5, 6};
  std::vector<std::optional<int64_t>> data2 = {
      2, 2, 1, std::nullopt, std::nullopt, 5, 7};

  auto vector1 = vectorMaker_.flatVectorNullable<int64_t>(data1);
  auto vector2 = vectorMaker_.flatVectorNullable<int64_t>(data2);
  testEqual(vector1, vector2, data1, data2);
  testHash(vector1);
}

// Test reader<Generic> where generic elements are arrays<ints>
TEST_F(GenericViewTest, testArrayOfInt) {
  auto vector1 = vectorMaker_.arrayVectorNullable(arrayData1);
  auto vector2 = vectorMaker_.arrayVectorNullable(arrayData2);
  testEqual(vector1, vector2, arrayData1, arrayData2);
  testHash(vector1);
}

// Test reader<Array<Generic>> where generic elements are ints.
TEST_F(GenericViewTest, testArrayOfGeneric) {
  auto vector1 = vectorMaker_.arrayVectorNullable(arrayData1);
  auto vector2 = vectorMaker_.arrayVectorNullable(arrayData2);

  DecodedVector decoded1;
  DecodedVector decoded2;
  exec::VectorReader<Array<Generic>> reader1(decode(decoded1, *vector1));
  exec::VectorReader<Array<Generic>> reader2(decode(decoded2, *vector2));

  // Reader will return std::vector<std::optional<Generic>> like object.
  for (auto i = 0; i < vector1->size(); i++) {
    auto arrayView1 = reader1[i];
    auto arrayView2 = reader2[i];

    // Test comparing generics nested in ArrayView.
    for (auto j = 0; j < arrayView1.size(); j++) {
      auto generic1 = arrayView1[j];

      for (auto k = 0; k < arrayView2.size(); k++) {
        auto generic2 = arrayView2[k];

        ASSERT_EQ(
            generic1 == generic2,
            arrayData1[i].value()[j] == arrayData2[i].value()[k]);
      }
    }
  }
}

} // namespace
} // namespace facebook::velox

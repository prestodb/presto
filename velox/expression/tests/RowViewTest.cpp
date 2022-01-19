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

#include "gtest/gtest.h"
#include "velox/expression/VectorUdfTypeSystem.h"
#include "velox/functions/prestosql/tests/FunctionBaseTest.h"

namespace {

using namespace facebook::velox;

DecodedVector* decode(DecodedVector& decoder, const BaseVector& vector) {
  SelectivityVector rows(vector.size());
  decoder.decode(vector, rows);
  return &decoder;
}

class RowViewTest : public functions::test::FunctionBaseTest {
 protected:
  VectorPtr makeTestRowVector() {
    std::vector<int32_t> data1;
    std::vector<float> data2;
    auto size = 10;
    for (auto i = 0; i < size; ++i) {
      data1.push_back(i);
      data2.push_back(size - i);
    };
    return makeRowVector(
        {makeFlatVector(data1), makeFlatVector(data2)},
        [](auto i) { return i % 2 == 0; });
  }
};

TEST_F(RowViewTest, basic) {
  auto rowVector = makeTestRowVector();
  DecodedVector decoded;
  exec::VectorReader<Row<int32_t, float>> reader(decode(decoded, *rowVector));

  for (auto i = 0; i < rowVector->size(); ++i) {
    auto isSet = reader.isSet(i);
    ASSERT_EQ(isSet, i % 2 == 1);
    if (isSet) {
      auto&& r = reader[i];
      ASSERT_EQ(*r.at<0>(), i);
      ASSERT_EQ(*r.at<1>(), rowVector->size() - i);
    }
  }
}

TEST_F(RowViewTest, encoded) {
  auto rowVector = makeTestRowVector();
  // Wrap in dictionary.
  auto vectorSize = rowVector->size();
  BufferPtr indices =
      AlignedBuffer::allocate<vector_size_t>(vectorSize, pool_.get());
  auto rawIndices = indices->asMutable<vector_size_t>();
  // Assign indices such that array is reversed.
  for (size_t i = 0; i < vectorSize; ++i) {
    rawIndices[i] = vectorSize - 1 - i;
  }
  rowVector = BaseVector::wrapInDictionary(
      BufferPtr(nullptr), indices, vectorSize, rowVector);

  DecodedVector decoded;
  exec::VectorReader<Row<int32_t, float>> reader(decode(decoded, *rowVector));

  for (auto i = 0; i < rowVector->size(); ++i) {
    auto isSet = reader.isSet(i);
    ASSERT_EQ(isSet, i % 2 == 0);
    if (isSet) {
      auto&& r = reader[i];
      ASSERT_EQ(*r.at<0>(), rowVector->size() - i - 1);
      ASSERT_EQ(*r.at<1>(), i + 1);
    }
  }
}

TEST_F(RowViewTest, get) {
  auto rowVector = makeTestRowVector();
  DecodedVector decoded;
  exec::VectorReader<Row<int32_t, float>> reader(decode(decoded, *rowVector));

  for (auto i = 0; i < rowVector->size(); ++i) {
    auto isSet = reader.isSet(i);
    ASSERT_EQ(isSet, i % 2 == 1);
    if (isSet) {
      auto&& r = reader[i];
      ASSERT_EQ(*exec::get<0>(r), i);
      ASSERT_EQ(*exec::get<1>(r), rowVector->size() - i);
    }
  }
}

} // namespace

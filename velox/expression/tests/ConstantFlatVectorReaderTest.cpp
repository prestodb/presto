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

#include "velox/expression/VectorUdfTypeSystem.h"
#include "velox/functions/prestosql/tests/FunctionBaseTest.h"

namespace facebook::velox::exec {

class ConstantFlatVectorReaderTest : public functions::test::FunctionBaseTest {
};

TEST_F(ConstantFlatVectorReaderTest, flatContainsNoNulls) {
  auto vector =
      makeFlatVector<int32_t>(10, [](vector_size_t row) { return row * 2; });

  ConstantFlatVectorReader<int32_t> reader(vector.get());

  ASSERT_FALSE(reader.mayHaveNulls());
  ASSERT_FALSE(reader.mayHaveNullsRecursive());

  for (vector_size_t i = 0; i < vector->size(); i++) {
    ASSERT_EQ(i * 2, reader[i]);
    ASSERT_EQ(i * 2, reader.readNullFree(i));
    ASSERT_TRUE(reader.isSet(i));
    ASSERT_FALSE(reader.containsNull(i));
    ASSERT_FALSE(reader.containsNull(0, i));
    ASSERT_FALSE(reader.containsNull(i, vector->size()));
  }
}

TEST_F(ConstantFlatVectorReaderTest, flatContainsNulls) {
  auto vector = makeFlatVector<int32_t>(
      10,
      [](vector_size_t row) { return row * 2; },
      [](vector_size_t row) { return row % 5 == 2; });

  ConstantFlatVectorReader<int32_t> reader(vector.get());

  ASSERT_TRUE(reader.mayHaveNulls());
  ASSERT_TRUE(reader.mayHaveNullsRecursive());

  for (vector_size_t i = 0; i < vector->size(); i++) {
    if (i % 5 == 2) {
      ASSERT_FALSE(reader.isSet(i));
      ASSERT_TRUE(reader.containsNull(i));
    } else {
      ASSERT_EQ(i * 2, reader[i]);
      ASSERT_EQ(i * 2, reader.readNullFree(i));
      ASSERT_TRUE(reader.isSet(i));
      ASSERT_FALSE(reader.containsNull(i));
    }

    if (i > 2) {
      ASSERT_TRUE(reader.containsNull(0, i));
    } else {
      ASSERT_FALSE(reader.containsNull(0, i));
    }

    if (i <= 7) {
      ASSERT_TRUE(reader.containsNull(i, vector->size()));
    } else {
      ASSERT_FALSE(reader.containsNull(i, vector->size()));
    }
  }
}

TEST_F(ConstantFlatVectorReaderTest, constant) {
  auto vector = makeConstant<int32_t>(5, 10);

  ConstantFlatVectorReader<int32_t> reader(
      dynamic_cast<ConstantVector<int32_t>*>(vector.get()));

  ASSERT_FALSE(reader.mayHaveNulls());
  ASSERT_FALSE(reader.mayHaveNullsRecursive());

  for (vector_size_t i = 0; i < vector->size(); i++) {
    ASSERT_EQ(5, reader[i]);
    ASSERT_EQ(5, reader.readNullFree(i));
    ASSERT_TRUE(reader.isSet(i));
    ASSERT_FALSE(reader.containsNull(i));
    ASSERT_FALSE(reader.containsNull(0, i));
    ASSERT_FALSE(reader.containsNull(i, vector->size()));
  }
}

TEST_F(ConstantFlatVectorReaderTest, constantNull) {
  auto vector = makeConstant<int32_t>(std::nullopt, 10);

  ConstantFlatVectorReader<int32_t> reader(
      dynamic_cast<ConstantVector<int32_t>*>(vector.get()));

  ASSERT_TRUE(reader.mayHaveNulls());
  ASSERT_TRUE(reader.mayHaveNullsRecursive());

  for (vector_size_t i = 0; i < vector->size(); i++) {
    ASSERT_FALSE(reader.isSet(i));
    ASSERT_TRUE(reader.containsNull(i));
    if (i > 0) {
      ASSERT_TRUE(reader.containsNull(0, i));
    }
    ASSERT_TRUE(reader.containsNull(i, vector->size()));
  }
}
} // namespace facebook::velox::exec

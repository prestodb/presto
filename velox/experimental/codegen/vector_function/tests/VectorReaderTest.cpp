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

#include <folly/Random.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include "velox/experimental/codegen/vector_function/GeneratedVectorFunction-inl.h" // NOLINT (CLANGTIDY  )
#include "velox/experimental/codegen/vector_function/StringTypes.h"

namespace facebook::velox::codegen {
TEST(VectorReader, ReadDoublesVectors) {
  const size_t vectorSize = 1000;
  auto inRowType = ROW({"columnA", "columnB"}, {DOUBLE(), DOUBLE()});
  auto outRowType = ROW({"expr1", "expr2"}, {DOUBLE(), DOUBLE()});

  auto pool = memory::getDefaultMemoryPool();
  auto inRowVector = BaseVector::create(inRowType, vectorSize, pool.get());
  auto outRowVector = BaseVector::create(outRowType, vectorSize, pool.get());

  VectorPtr& in1 = inRowVector->as<RowVector>()->childAt(0);

  SelectivityVector selectivityVector(vectorSize);
  selectivityVector.setAll();
  in1->resize(vectorSize);
  in1->addNulls(nullptr, selectivityVector);
  VectorReader<DoubleType, OutputReaderConfig<false, false>> writer(in1);
  VectorReader<DoubleType, InputReaderConfig<false, false>> reader(in1);

  for (size_t row = 0; row < vectorSize; row++) {
    writer[row] = (double)row;
  }

  for (size_t row = 0; row < vectorSize; row++) {
    ASSERT_DOUBLE_EQ((double)row, *reader[row]);
  }

  for (size_t row = 0; row < vectorSize; row++) {
    ASSERT_DOUBLE_EQ(*reader[row], in1->asFlatVector<double>()->valueAt(row));
  }
}

TEST(VectorReader, ReadBoolVectors) {
  // TODO: Move those to test class
  auto pool = memory::getDefaultMemoryPool();
  const size_t vectorSize = 1000;

  auto inRowType = ROW({"columnA", "columnB"}, {BOOLEAN(), BOOLEAN()});
  auto outRowType = ROW({"expr1", "expr2"}, {BOOLEAN(), BOOLEAN()});

  auto inRowVector = BaseVector::create(inRowType, vectorSize, pool.get());
  auto outRowVector = BaseVector::create(outRowType, vectorSize, pool.get());

  VectorPtr& inputVector = inRowVector->as<RowVector>()->childAt(0);
  inputVector->resize(vectorSize);
  VectorReader<BooleanType, InputReaderConfig<false, false>> reader(
      inputVector);
  VectorReader<BooleanType, OutputReaderConfig<false, false>> writer(
      inputVector);

  for (size_t row = 0; row < vectorSize; row++) {
    writer[row] = row % 2 == 0;
  }

  // Check that writing of values to the reader was success
  for (size_t row = 0; row < vectorSize; row++) {
    ASSERT_DOUBLE_EQ((row % 2 == 0), *reader[row]);
    ASSERT_DOUBLE_EQ(
        (row % 2 == 0), inputVector->asFlatVector<bool>()->valueAt(row));
  }

  // Write a null at even indices
  for (size_t row = 0; row < vectorSize; row++) {
    if (row % 2) {
      writer[row] = std::nullopt;
    }
  }

  for (size_t row = 0; row < vectorSize; row++) {
    ASSERT_EQ(inputVector->asFlatVector<bool>()->isNullAt(row), row % 2);
  }
}
} // namespace facebook::velox::codegen

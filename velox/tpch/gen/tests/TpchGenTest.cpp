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

#include "velox/tpch/gen/TpchGen.h"
#include "velox/type/StringView.h"
#include "velox/vector/FlatVector.h"

namespace {

using namespace facebook::velox;
using namespace facebook::velox::tpch;

TEST(TpchGenTestNation, default) {
  auto rowVector = genTpchNation();
  ASSERT_NE(rowVector, nullptr);
  EXPECT_EQ(4, rowVector->childrenSize());
  EXPECT_EQ(25, rowVector->size());

  auto nationKey = rowVector->childAt(0)->asFlatVector<int64_t>();
  auto nationName = rowVector->childAt(1)->asFlatVector<StringView>();

  EXPECT_EQ(0, nationKey->valueAt(0));
  EXPECT_EQ("ALGERIA"_sv, nationName->valueAt(0));

  // Ensure we won't crash while accessing any of the columns.
  LOG(INFO) << rowVector->toString(0);

  EXPECT_EQ(24, nationKey->valueAt(24));
  EXPECT_EQ("UNITED STATES"_sv, nationName->valueAt(24));
  LOG(INFO) << rowVector->toString(24);
}

// Ensure scale factor doesn't affect Nation table.
TEST(TpchGenTestNation, scaleFactor) {
  auto rowVector = genTpchNation(10000, 0, 1000);
  ASSERT_NE(rowVector, nullptr);

  EXPECT_EQ(4, rowVector->childrenSize());
  EXPECT_EQ(25, rowVector->size());
}

TEST(TpchGenTestNation, smallBatch) {
  auto rowVector = genTpchNation(10);
  ASSERT_NE(rowVector, nullptr);

  EXPECT_EQ(4, rowVector->childrenSize());
  EXPECT_EQ(10, rowVector->size());

  auto nationKey = rowVector->childAt(0)->asFlatVector<int64_t>();
  EXPECT_EQ(0, nationKey->valueAt(0));
  EXPECT_EQ(9, nationKey->valueAt(9));
}

TEST(TpchGenTestNation, smallBatchWithOffset) {
  auto rowVector = genTpchNation(10, 5);
  ASSERT_NE(rowVector, nullptr);

  EXPECT_EQ(4, rowVector->childrenSize());
  EXPECT_EQ(10, rowVector->size());

  auto nationKey = rowVector->childAt(0)->asFlatVector<int64_t>();
  EXPECT_EQ(5, nationKey->valueAt(0));
  EXPECT_EQ(14, nationKey->valueAt(9));
}

TEST(TpchGenTestNation, smallBatchPastEnd) {
  auto rowVector = genTpchNation(10, 20);
  ASSERT_NE(rowVector, nullptr);

  EXPECT_EQ(4, rowVector->childrenSize());
  EXPECT_EQ(5, rowVector->size());

  auto nationKey = rowVector->childAt(0)->asFlatVector<int64_t>();
  EXPECT_EQ(20, nationKey->valueAt(0));
  EXPECT_EQ(24, nationKey->valueAt(4));
}

} // namespace

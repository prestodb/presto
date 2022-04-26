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

#include <folly/init/Init.h>
#include "gtest/gtest.h"

#include "velox/tpch/gen/TpchGen.h"
#include "velox/type/StringView.h"
#include "velox/vector/FlatVector.h"

namespace {

using namespace facebook::velox;
using namespace facebook::velox::tpch;

// Nation tests.

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
  auto rowVector = genTpchNation(10'000, 0, 1'000);
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

TEST(TpchGenTestNation, reproducible) {
  auto rowVector1 = genTpchNation();
  auto rowVector2 = genTpchNation();
  auto rowVector3 = genTpchNation();

  for (size_t i = 0; i < rowVector1->size(); ++i) {
    ASSERT_TRUE(rowVector1->equalValueAt(rowVector2.get(), i, i));
    ASSERT_TRUE(rowVector1->equalValueAt(rowVector3.get(), i, i));
  }

  // Ensure it's also reproducible if we add an offset.
  auto rowVector4 = genTpchNation(100, 10);
  auto rowVector5 = genTpchNation(100, 10);

  for (size_t i = 0; i < rowVector4->size(); ++i) {
    ASSERT_TRUE(rowVector4->equalValueAt(rowVector5.get(), i, i));
  }
}

// Orders tests.

TEST(TpchGenTestOrders, batches) {
  auto rowVector1 = genTpchOrders(10'000);

  EXPECT_EQ(9, rowVector1->childrenSize());
  EXPECT_EQ(10'000, rowVector1->size());

  auto orderKey = rowVector1->childAt(0)->asFlatVector<int64_t>();
  auto orderDate = rowVector1->childAt(4)->asFlatVector<StringView>();

  EXPECT_EQ(1, orderKey->valueAt(0));
  EXPECT_EQ("1996-01-02"_sv, orderDate->valueAt(0));
  LOG(INFO) << rowVector1->toString(0);

  EXPECT_EQ(40'000, orderKey->valueAt(9999));
  EXPECT_EQ("1995-01-30"_sv, orderDate->valueAt(9999));
  LOG(INFO) << rowVector1->toString(9999);

  // Get second batch.
  auto rowVector2 = genTpchOrders(10'000, 10'000);

  EXPECT_EQ(9, rowVector2->childrenSize());
  EXPECT_EQ(10'000, rowVector2->size());

  orderKey = rowVector2->childAt(0)->asFlatVector<int64_t>();
  orderDate = rowVector2->childAt(4)->asFlatVector<StringView>();

  EXPECT_EQ(40001, orderKey->valueAt(0));
  EXPECT_EQ("1996-01-02"_sv, orderDate->valueAt(0));
  LOG(INFO) << rowVector2->toString(0);

  EXPECT_EQ(80000, orderKey->valueAt(9999));
  EXPECT_EQ("1995-01-30"_sv, orderDate->valueAt(9999));
  LOG(INFO) << rowVector2->toString(9999);
}

TEST(TpchGenTestOrders, lastBatch) {
  // Ask for 200 but there are only 100 left.
  auto rowVector = genTpchOrders(200, 1'499'900);
  EXPECT_EQ(100, rowVector->size());

  // Ensure we get 200 on a larger scale factor.
  rowVector = genTpchOrders(200, 1'499'900, 2);
  EXPECT_EQ(200, rowVector->size());
}

TEST(TpchGenTestOrders, reproducible) {
  {
    auto rowVector1 = genTpchOrders(1000);
    auto rowVector2 = genTpchOrders(1000);
    auto rowVector3 = genTpchOrders(1000);

    for (size_t i = 0; i < rowVector1->size(); ++i) {
      ASSERT_TRUE(rowVector1->equalValueAt(rowVector2.get(), i, i));
      ASSERT_TRUE(rowVector1->equalValueAt(rowVector3.get(), i, i));
    }
  }

  // Ensure it's also reproducible if we add an offset.
  {
    auto rowVector1 = genTpchOrders(1000, 2000);
    auto rowVector2 = genTpchOrders(1000, 2000);
    auto rowVector3 = genTpchOrders(1000, 2000);

    for (size_t i = 0; i < rowVector1->size(); ++i) {
      ASSERT_TRUE(rowVector1->equalValueAt(rowVector2.get(), i, i));
      ASSERT_TRUE(rowVector1->equalValueAt(rowVector3.get(), i, i));
    }
  }

  // Ensure that if the offsets are different, records will be different.
  {
    auto rowVector1 = genTpchOrders(1000, 2000);
    auto rowVector2 = genTpchOrders(1000, 2001);

    for (size_t i = 0; i < rowVector2->size(); ++i) {
      ASSERT_FALSE(rowVector1->equalValueAt(rowVector2.get(), i, i));
    }
  }
}

} // namespace

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}

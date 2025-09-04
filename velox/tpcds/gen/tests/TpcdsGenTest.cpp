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

#include "velox/common/memory/Memory.h"
#include "velox/tpcds/gen/TpcdsGen.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

namespace {

using namespace facebook::velox;

class TpcdsGenTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    pool_ = memory::memoryManager()->addLeafPool("TpcdsGenTest");
  }

  std::shared_ptr<memory::MemoryPool> pool_;
};

TEST_F(TpcdsGenTest, batches) {
  auto rowVector1 =
      genTpcdsData(tpcds::Table::TBL_ITEM, 5000, 0, pool_.get(), 1, 1, 1);

  EXPECT_EQ(22, rowVector1->childrenSize());
  EXPECT_EQ(5000, rowVector1->size());

  auto itemKey = rowVector1->childAt(0)->asFlatVector<int64_t>();
  auto itemId = rowVector1->childAt(1)->asFlatVector<StringView>();
  auto itemStartDate = rowVector1->childAt(2)->asFlatVector<int32_t>();

  EXPECT_EQ(1, itemKey->valueAt(0));
  EXPECT_EQ("AAAAAAAABAAAAAAA", itemId->valueAt(0));
  EXPECT_EQ("1997-10-27", DATE()->toString(itemStartDate->valueAt(0)));
  LOG(INFO) << rowVector1->toString(0);

  EXPECT_EQ(4001, itemKey->valueAt(4000));
  EXPECT_EQ("AAAAAAAAAKPAAAAA", itemId->valueAt(4000));
  EXPECT_EQ("1999-10-28", DATE()->toString(itemStartDate->valueAt(4000)));
  LOG(INFO) << rowVector1->toString(4000);

  // Get second batch.
  auto rowVector2 =
      genTpcdsData(tpcds::Table::TBL_ITEM, 5000, 5000, pool_.get(), 1, 1, 1);

  EXPECT_EQ(22, rowVector2->childrenSize());
  EXPECT_EQ(5000, rowVector2->size());

  itemKey = rowVector2->childAt(0)->asFlatVector<int64_t>();
  itemId = rowVector2->childAt(1)->asFlatVector<StringView>();
  itemStartDate = rowVector2->childAt(2)->asFlatVector<int32_t>();

  EXPECT_EQ(5001, itemKey->valueAt(0));
  EXPECT_EQ("AAAAAAAAIIDBAAAA", itemId->valueAt(0));
  EXPECT_EQ("2000-10-27", DATE()->toString(itemStartDate->valueAt(0)));
  LOG(INFO) << rowVector2->toString(0);

  EXPECT_EQ(9001, itemKey->valueAt(4000));
  EXPECT_EQ("AAAAAAAAJCDCAAAA", itemId->valueAt(4000));
  EXPECT_EQ("1997-10-27", DATE()->toString(itemStartDate->valueAt(4000)));
  LOG(INFO) << rowVector2->toString(4000);
}

TEST_F(TpcdsGenTest, lastBatch) {
  // Ask for 10000 but there are only 5000 left.
  auto rowVector = genTpcdsData(
      tpcds::Table::TBL_CUSTOMER_ADDRESS, 10000, 45000, pool_.get(), 1, 1, 1);
  EXPECT_EQ(5000, rowVector->size());

  // Ensure we get 10000 on a larger scale factor.
  rowVector = genTpcdsData(
      tpcds::Table::TBL_CUSTOMER_ADDRESS, 10000, 45000, pool_.get(), 10, 1, 1);
  EXPECT_EQ(10000, rowVector->size());
}

TEST_F(TpcdsGenTest, reproducible) {
  // Ensure data generated is reproducible.
  {
    auto rowVector1 = genTpcdsData(
        tpcds::Table::TBL_CUSTOMER_ADDRESS, 5000, 0, pool_.get(), 1, 1, 1);
    auto rowVector2 = genTpcdsData(
        tpcds::Table::TBL_CUSTOMER_ADDRESS, 5000, 0, pool_.get(), 1, 1, 1);
    auto rowVector3 = genTpcdsData(
        tpcds::Table::TBL_CUSTOMER_ADDRESS, 5000, 0, pool_.get(), 1, 1, 1);

    for (size_t i = 0; i < rowVector1->size(); ++i) {
      ASSERT_TRUE(rowVector1->equalValueAt(rowVector2.get(), i, i));
      ASSERT_TRUE(rowVector1->equalValueAt(rowVector3.get(), i, i));
    }
  }

  // Ensure it's also reproducible if we add an offset.
  {
    auto rowVector1 = genTpcdsData(
        tpcds::Table::TBL_CUSTOMER_ADDRESS, 5000, 1000, pool_.get(), 1, 1, 1);
    auto rowVector2 = genTpcdsData(
        tpcds::Table::TBL_CUSTOMER_ADDRESS, 5000, 1000, pool_.get(), 1, 1, 1);
    auto rowVector3 = genTpcdsData(
        tpcds::Table::TBL_CUSTOMER_ADDRESS, 5000, 1000, pool_.get(), 1, 1, 1);

    for (size_t i = 0; i < rowVector1->size(); ++i) {
      ASSERT_TRUE(rowVector1->equalValueAt(rowVector2.get(), i, i));
      ASSERT_TRUE(rowVector1->equalValueAt(rowVector3.get(), i, i));
    }
  }

  // Ensure that if the offsets are different, records will be different.
  {
    auto rowVector1 = genTpcdsData(
        tpcds::Table::TBL_CUSTOMER_ADDRESS, 5000, 0, pool_.get(), 1, 1, 1);
    auto rowVector2 = genTpcdsData(
        tpcds::Table::TBL_CUSTOMER_ADDRESS, 5000, 500, pool_.get(), 1, 1, 1);

    for (size_t i = 0; i < rowVector2->size(); ++i) {
      ASSERT_FALSE(rowVector1->equalValueAt(rowVector2.get(), i, i));
    }
  }
}

} // namespace

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init{&argc, &argv, false};
  return RUN_ALL_TESTS();
}

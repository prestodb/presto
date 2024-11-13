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
#include "velox/vector/FlatVector.h"

namespace {

using namespace facebook::velox;
using namespace facebook::velox::tpch;

// Nation tests.

class TpchGenTestNationTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    pool_ = memory::memoryManager()->addLeafPool("TpchGenTestNationTest");
  }

  std::shared_ptr<memory::MemoryPool> pool_;
};

TEST_F(TpchGenTestNationTest, default) {
  auto rowVector = genTpchNation(pool_.get());
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
TEST_F(TpchGenTestNationTest, scaleFactor) {
  auto rowVector = genTpchNation(pool_.get(), 10'000, 0, 1'000);
  ASSERT_NE(rowVector, nullptr);

  EXPECT_EQ(4, rowVector->childrenSize());
  EXPECT_EQ(25, rowVector->size());
}

TEST_F(TpchGenTestNationTest, smallBatch) {
  auto rowVector = genTpchNation(pool_.get(), 10);
  ASSERT_NE(rowVector, nullptr);

  EXPECT_EQ(4, rowVector->childrenSize());
  EXPECT_EQ(10, rowVector->size());

  auto nationKey = rowVector->childAt(0)->asFlatVector<int64_t>();
  EXPECT_EQ(0, nationKey->valueAt(0));
  EXPECT_EQ(9, nationKey->valueAt(9));
}

TEST_F(TpchGenTestNationTest, smallBatchWithOffset) {
  auto rowVector = genTpchNation(pool_.get(), 10, 5);
  ASSERT_NE(rowVector, nullptr);

  EXPECT_EQ(4, rowVector->childrenSize());
  EXPECT_EQ(10, rowVector->size());

  auto nationKey = rowVector->childAt(0)->asFlatVector<int64_t>();
  EXPECT_EQ(5, nationKey->valueAt(0));
  EXPECT_EQ(14, nationKey->valueAt(9));
}

TEST_F(TpchGenTestNationTest, smallBatchPastEnd) {
  auto rowVector = genTpchNation(pool_.get(), 10, 20);
  ASSERT_NE(rowVector, nullptr);

  EXPECT_EQ(4, rowVector->childrenSize());
  EXPECT_EQ(5, rowVector->size());

  auto nationKey = rowVector->childAt(0)->asFlatVector<int64_t>();
  EXPECT_EQ(20, nationKey->valueAt(0));
  EXPECT_EQ(24, nationKey->valueAt(4));
}

TEST_F(TpchGenTestNationTest, reproducible) {
  auto rowVector1 = genTpchNation(pool_.get());
  auto rowVector2 = genTpchNation(pool_.get());
  auto rowVector3 = genTpchNation(pool_.get());

  for (size_t i = 0; i < rowVector1->size(); ++i) {
    ASSERT_TRUE(rowVector1->equalValueAt(rowVector2.get(), i, i));
    ASSERT_TRUE(rowVector1->equalValueAt(rowVector3.get(), i, i));
  }

  // Ensure it's also reproducible if we add an offset.
  auto rowVector4 = genTpchNation(pool_.get(), 100, 10);
  auto rowVector5 = genTpchNation(pool_.get(), 100, 10);

  for (size_t i = 0; i < rowVector4->size(); ++i) {
    ASSERT_TRUE(rowVector4->equalValueAt(rowVector5.get(), i, i));
  }

  // Ensure it's also reproducible if we generate batches starting in
  // different offsets.
  auto rowVector6 = genTpchNation(pool_.get(), 100, 0);
  auto rowVector7 = genTpchNation(pool_.get(), 90, 10);

  for (size_t i = 0; i < rowVector7->size(); ++i) {
    ASSERT_TRUE(rowVector7->equalValueAt(rowVector6.get(), i, i + 10));
  }
}

// Region.
class TpchGenTestRegionTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    pool_ = memory::memoryManager()->addLeafPool("TpchGenTestRegionTest");
  }

  std::shared_ptr<memory::MemoryPool> pool_;
};

TEST_F(TpchGenTestRegionTest, batches) {
  auto rowVector1 = genTpchRegion(pool_.get());

  EXPECT_EQ(3, rowVector1->childrenSize());
  EXPECT_EQ(5, rowVector1->size());

  auto regionKey = rowVector1->childAt(0)->asFlatVector<int64_t>();
  auto regionName = rowVector1->childAt(1)->asFlatVector<StringView>();

  EXPECT_EQ(0, regionKey->valueAt(0));
  EXPECT_EQ("AFRICA"_sv, regionName->valueAt(0));
  LOG(INFO) << rowVector1->toString(0);

  EXPECT_EQ(4, regionKey->valueAt(4));
  EXPECT_EQ("MIDDLE EAST"_sv, regionName->valueAt(4));
  LOG(INFO) << rowVector1->toString(4);
}

TEST_F(TpchGenTestRegionTest, lastBatch) {
  // Ask for 100 regions but there are only 5.
  auto rowVector = genTpchRegion(pool_.get(), 100);
  EXPECT_EQ(5, rowVector->size());

  // Scale factor doens't affect it.
  rowVector = genTpchRegion(pool_.get(), 100, 0, 2);
  EXPECT_EQ(5, rowVector->size());

  // Zero records if we go beyond the end.
  rowVector = genTpchRegion(pool_.get(), 1'000, 200'000);
  EXPECT_EQ(0, rowVector->size());
}

TEST_F(TpchGenTestRegionTest, reproducible) {
  auto rowVector1 = genTpchRegion(pool_.get(), 100);
  auto rowVector2 = genTpchRegion(pool_.get(), 100);
  auto rowVector3 = genTpchRegion(pool_.get(), 100);

  ASSERT_EQ(5, rowVector1->size());

  for (size_t i = 0; i < rowVector1->size(); ++i) {
    ASSERT_TRUE(rowVector1->equalValueAt(rowVector2.get(), i, i));
    ASSERT_TRUE(rowVector1->equalValueAt(rowVector3.get(), i, i));
  }

  auto rowVector4 = genTpchRegion(pool_.get(), 100, 0);
  auto rowVector5 = genTpchRegion(pool_.get(), 98, 2);

  for (size_t i = 0; i < rowVector5->size(); ++i) {
    ASSERT_TRUE(rowVector5->equalValueAt(rowVector4.get(), i, i + 2));
  }
}

// Orders tests.
class TpchGenTestOrdersTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    pool_ = memory::memoryManager()->addLeafPool("TpchGenTestOrdersTest");
  }

  std::shared_ptr<memory::MemoryPool> pool_;
};

TEST_F(TpchGenTestOrdersTest, batches) {
  auto rowVector1 = genTpchOrders(pool_.get(), 10'000);

  EXPECT_EQ(9, rowVector1->childrenSize());
  EXPECT_EQ(10'000, rowVector1->size());

  auto orderKey = rowVector1->childAt(0)->asFlatVector<int64_t>();
  auto orderTotalPrice = rowVector1->childAt(3)->asFlatVector<double>();
  auto orderDate = rowVector1->childAt(4)->asFlatVector<int32_t>();

  EXPECT_EQ(1, orderKey->valueAt(0));
  EXPECT_EQ(173665.47, orderTotalPrice->valueAt(0));
  EXPECT_EQ("1996-01-02", DATE()->toString(orderDate->valueAt(0)));
  LOG(INFO) << rowVector1->toString(0);

  EXPECT_EQ(40'000, orderKey->valueAt(9999));
  EXPECT_EQ(87784.83, orderTotalPrice->valueAt(9999));
  EXPECT_EQ("1995-01-30", DATE()->toString(orderDate->valueAt(9999)));
  LOG(INFO) << rowVector1->toString(9999);

  // Get second batch.
  auto rowVector2 = genTpchOrders(pool_.get(), 10'000, 10'000);

  EXPECT_EQ(9, rowVector2->childrenSize());
  EXPECT_EQ(10'000, rowVector2->size());

  orderKey = rowVector2->childAt(0)->asFlatVector<int64_t>();
  orderTotalPrice = rowVector2->childAt(3)->asFlatVector<double>();
  orderDate = rowVector2->childAt(4)->asFlatVector<int32_t>();

  EXPECT_EQ(40001, orderKey->valueAt(0));
  EXPECT_EQ(100589.02, orderTotalPrice->valueAt(0));
  EXPECT_EQ("1995-02-25", DATE()->toString(orderDate->valueAt(0)));
  LOG(INFO) << rowVector2->toString(0);

  EXPECT_EQ(80000, orderKey->valueAt(9999));
  EXPECT_EQ(142775.84, orderTotalPrice->valueAt(9999));
  EXPECT_EQ("1995-12-15", DATE()->toString(orderDate->valueAt(9999)));
  LOG(INFO) << rowVector2->toString(9999);
}

TEST_F(TpchGenTestOrdersTest, lastBatch) {
  // Ask for 200 but there are only 100 left.
  auto rowVector = genTpchOrders(pool_.get(), 200, 1'499'900);
  EXPECT_EQ(100, rowVector->size());

  // Ensure we get 200 on a larger scale factor.
  rowVector = genTpchOrders(pool_.get(), 200, 1'499'900, 2);
  EXPECT_EQ(200, rowVector->size());
}

TEST_F(TpchGenTestOrdersTest, reproducible) {
  {
    auto rowVector1 = genTpchOrders(pool_.get(), 1000);
    auto rowVector2 = genTpchOrders(pool_.get(), 1000);
    auto rowVector3 = genTpchOrders(pool_.get(), 1000);

    for (size_t i = 0; i < rowVector1->size(); ++i) {
      ASSERT_TRUE(rowVector1->equalValueAt(rowVector2.get(), i, i));
      ASSERT_TRUE(rowVector1->equalValueAt(rowVector3.get(), i, i));
    }
  }

  // Ensure it's also reproducible if we add an offset.
  {
    auto rowVector1 = genTpchOrders(pool_.get(), 1000, 2000);
    auto rowVector2 = genTpchOrders(pool_.get(), 1000, 2000);
    auto rowVector3 = genTpchOrders(pool_.get(), 1000, 2000);

    for (size_t i = 0; i < rowVector1->size(); ++i) {
      ASSERT_TRUE(rowVector1->equalValueAt(rowVector2.get(), i, i));
      ASSERT_TRUE(rowVector1->equalValueAt(rowVector3.get(), i, i));
    }
  }

  // Ensure it's reproducible if we generate from different offsets.
  {
    auto rowVector1 = genTpchOrders(pool_.get(), 1000, 0);
    auto rowVector2 = genTpchOrders(pool_.get(), 990, 10);

    for (size_t i = 0; i < rowVector2->size(); ++i) {
      ASSERT_TRUE(rowVector2->equalValueAt(rowVector1.get(), i, i + 10));
    }
  }

  // Ensure that if the offsets are different, records will be different.
  {
    auto rowVector1 = genTpchOrders(pool_.get(), 1000, 2000);
    auto rowVector2 = genTpchOrders(pool_.get(), 1000, 2001);

    for (size_t i = 0; i < rowVector2->size(); ++i) {
      ASSERT_FALSE(rowVector1->equalValueAt(rowVector2.get(), i, i));
    }
  }
}

// Lineitem.
class TpchGenTestLineItemTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    pool_ = memory::memoryManager()->addLeafPool("TpchGenTestLineItemTest");
  }

  std::shared_ptr<memory::MemoryPool> pool_;
};

TEST_F(TpchGenTestLineItemTest, batches) {
  size_t ordersMaxSize = 100;
  auto rowVector1 = genTpchLineItem(pool_.get(), ordersMaxSize);

  // Always returns 16 columns, and number of lineItem rows varies from 1 to 7
  // per order.
  EXPECT_EQ(16, rowVector1->childrenSize());
  EXPECT_GE(rowVector1->size(), ordersMaxSize);
  EXPECT_LE(rowVector1->size(), ordersMaxSize * 7);

  auto orderKey = rowVector1->childAt(0)->asFlatVector<int64_t>();
  auto quantity = rowVector1->childAt(4)->asFlatVector<double>();
  auto shipDate = rowVector1->childAt(10)->asFlatVector<int32_t>();

  EXPECT_EQ(1, orderKey->valueAt(0));
  EXPECT_EQ(17, quantity->valueAt(0));
  EXPECT_EQ("1996-03-13", DATE()->toString(shipDate->valueAt(0)));
  LOG(INFO) << rowVector1->toString(0);

  vector_size_t lastRow = rowVector1->size() - 1;
  EXPECT_EQ(388, orderKey->valueAt(lastRow));
  EXPECT_EQ(40, quantity->valueAt(lastRow));
  EXPECT_EQ("1992-12-24", DATE()->toString(shipDate->valueAt(lastRow)));
  LOG(INFO) << rowVector1->toString(lastRow);

  // Get next batch.
  auto rowVector2 = genTpchLineItem(pool_.get(), ordersMaxSize, ordersMaxSize);

  EXPECT_EQ(16, rowVector2->childrenSize());
  EXPECT_GE(rowVector2->size(), ordersMaxSize);
  EXPECT_LE(rowVector2->size(), ordersMaxSize * 7);

  orderKey = rowVector2->childAt(0)->asFlatVector<int64_t>();
  shipDate = rowVector2->childAt(10)->asFlatVector<int32_t>();

  EXPECT_EQ(389, orderKey->valueAt(0));
  EXPECT_EQ("1994-04-13", DATE()->toString(shipDate->valueAt(0)));
  LOG(INFO) << rowVector2->toString(0);

  lastRow = rowVector2->size() - 1;
  EXPECT_EQ(800, orderKey->valueAt(lastRow));
  EXPECT_EQ("1998-07-23", DATE()->toString(shipDate->valueAt(lastRow)));
  LOG(INFO) << rowVector2->toString(lastRow);
}

TEST_F(TpchGenTestLineItemTest, lastBatch) {
  // Ask for 1000 lineItems but there are only 10 orders left.
  auto rowVector = genTpchLineItem(pool_.get(), 1000, 1'499'990);
  EXPECT_GE(rowVector->size(), 10);
  EXPECT_LE(rowVector->size(), 10 * 7);

  // Ensure we get 1000 orders on a larger scale factor.
  rowVector = genTpchLineItem(pool_.get(), 1000, 1'499'990, 2);
  EXPECT_GE(rowVector->size(), 1000);
  EXPECT_LE(rowVector->size(), 1000 * 7);
}

TEST_F(TpchGenTestLineItemTest, reproducible) {
  {
    auto rowVector1 = genTpchLineItem(pool_.get(), 1000);
    auto rowVector2 = genTpchLineItem(pool_.get(), 1000);
    auto rowVector3 = genTpchLineItem(pool_.get(), 1000);

    for (size_t i = 0; i < rowVector1->size(); ++i) {
      ASSERT_TRUE(rowVector1->equalValueAt(rowVector2.get(), i, i));
      ASSERT_TRUE(rowVector1->equalValueAt(rowVector3.get(), i, i));
    }
  }

  // Ensure it's also reproducible if we add an offset.
  {
    auto rowVector1 = genTpchLineItem(pool_.get(), 1000, 2000);
    auto rowVector2 = genTpchLineItem(pool_.get(), 1000, 2000);
    auto rowVector3 = genTpchLineItem(pool_.get(), 1000, 2000);

    for (size_t i = 0; i < rowVector1->size(); ++i) {
      ASSERT_TRUE(rowVector1->equalValueAt(rowVector2.get(), i, i));
      ASSERT_TRUE(rowVector1->equalValueAt(rowVector3.get(), i, i));
    }
  }

  // Ensure it's reproducible if we generate from different offsets.
  {
    auto rowVector1 = genTpchLineItem(pool_.get(), 1000);
    auto rowVector2 = genTpchLineItem(pool_.get(), 998, 2);

    // The offset for comparisons is 7, since the first generated order has 6
    // lineitems, and the second has 1.
    for (size_t i = 0; i < rowVector2->size(); ++i) {
      ASSERT_TRUE(rowVector2->equalValueAt(rowVector1.get(), i, i + 7));
    }
  }

  // Ensure that if the offsets are different, records will be different.
  {
    auto rowVector1 = genTpchLineItem(pool_.get(), 1000, 2000);
    auto rowVector2 = genTpchLineItem(pool_.get(), 1000, 2001);

    for (size_t i = 0; i < rowVector2->size(); ++i) {
      ASSERT_FALSE(rowVector1->equalValueAt(rowVector2.get(), i, i));
    }
  }
}

// Supplier.
class TpchGenTestSupplierTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    pool_ = memory::memoryManager()->addLeafPool("TpchGenTestSupplierTest");
  }

  std::shared_ptr<memory::MemoryPool> pool_;
};

TEST_F(TpchGenTestSupplierTest, batches) {
  auto rowVector1 = genTpchSupplier(pool_.get(), 1'000);

  EXPECT_EQ(7, rowVector1->childrenSize());
  EXPECT_EQ(1'000, rowVector1->size());

  auto suppKey = rowVector1->childAt(0)->asFlatVector<int64_t>();
  auto nationKey = rowVector1->childAt(3)->asFlatVector<int64_t>();
  auto phone = rowVector1->childAt(4)->asFlatVector<StringView>();

  EXPECT_EQ(1, suppKey->valueAt(0));
  EXPECT_EQ(17, nationKey->valueAt(0));
  EXPECT_EQ("27-918-335-1736"_sv, phone->valueAt(0));
  LOG(INFO) << rowVector1->toString(0);

  EXPECT_EQ(1'000, suppKey->valueAt(999));
  EXPECT_EQ(17, nationKey->valueAt(999));
  EXPECT_EQ("27-971-649-2792"_sv, phone->valueAt(999));
  LOG(INFO) << rowVector1->toString(999);

  // Get second batch.
  auto rowVector2 = genTpchSupplier(pool_.get(), 1'000, 1'000);

  EXPECT_EQ(7, rowVector2->childrenSize());
  EXPECT_EQ(1'000, rowVector2->size());

  suppKey = rowVector2->childAt(0)->asFlatVector<int64_t>();
  nationKey = rowVector2->childAt(3)->asFlatVector<int64_t>();
  phone = rowVector2->childAt(4)->asFlatVector<StringView>();

  EXPECT_EQ(1'001, suppKey->valueAt(0));
  EXPECT_EQ(9, nationKey->valueAt(0));
  EXPECT_EQ("19-393-671-5272"_sv, phone->valueAt(0));
  LOG(INFO) << rowVector2->toString(0);

  EXPECT_EQ(2'000, suppKey->valueAt(999));
  EXPECT_EQ(11, nationKey->valueAt(999));
  EXPECT_EQ("21-860-645-7227"_sv, phone->valueAt(999));
  LOG(INFO) << rowVector2->toString(999);
}

TEST_F(TpchGenTestSupplierTest, lastBatch) {
  // Ask for 10'000 suppliers but there are only 10 left.
  auto rowVector = genTpchSupplier(pool_.get(), 10'000, 9'990);
  EXPECT_EQ(10, rowVector->size());

  // Ensure we get 1000 suppliers on a larger scale factor.
  rowVector = genTpchSupplier(pool_.get(), 1'000, 9'990, 2);
  EXPECT_EQ(1'000, rowVector->size());

  // Zero records if we go beyond the end.
  rowVector = genTpchSupplier(pool_.get(), 1'000, 10'000);
  EXPECT_EQ(0, rowVector->size());
}

TEST_F(TpchGenTestSupplierTest, reproducible) {
  auto rowVector1 = genTpchSupplier(pool_.get(), 100);
  auto rowVector2 = genTpchSupplier(pool_.get(), 100);
  auto rowVector3 = genTpchSupplier(pool_.get(), 100);

  for (size_t i = 0; i < rowVector1->size(); ++i) {
    ASSERT_TRUE(rowVector1->equalValueAt(rowVector2.get(), i, i));
    ASSERT_TRUE(rowVector1->equalValueAt(rowVector3.get(), i, i));
  }

  // Ensure it's also reproducible if we add an offset.
  auto rowVector4 = genTpchSupplier(pool_.get(), 100, 10);
  auto rowVector5 = genTpchSupplier(pool_.get(), 100, 10);

  for (size_t i = 0; i < rowVector4->size(); ++i) {
    ASSERT_TRUE(rowVector4->equalValueAt(rowVector5.get(), i, i));
  }

  // Ensure it's also reproducible if we generate from different offsets.
  auto rowVector6 = genTpchSupplier(pool_.get(), 100, 0);
  auto rowVector7 = genTpchSupplier(pool_.get(), 90, 10);

  for (size_t i = 0; i < rowVector7->size(); ++i) {
    ASSERT_TRUE(rowVector7->equalValueAt(rowVector6.get(), i, i + 10));
  }
}

// Part.
class TpchGenTestPartTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    pool_ = memory::memoryManager()->addLeafPool("TpchGenTestPartTest");
  }

  std::shared_ptr<memory::MemoryPool> pool_;
};

TEST_F(TpchGenTestPartTest, batches) {
  auto rowVector1 = genTpchPart(pool_.get(), 1'000);

  EXPECT_EQ(9, rowVector1->childrenSize());
  EXPECT_EQ(1'000, rowVector1->size());

  auto partKey = rowVector1->childAt(0)->asFlatVector<int64_t>();
  auto mfgr = rowVector1->childAt(2)->asFlatVector<StringView>();

  EXPECT_EQ(1, partKey->valueAt(0));
  EXPECT_EQ("Manufacturer#1"_sv, mfgr->valueAt(0));

  EXPECT_EQ(1'000, partKey->valueAt(999));
  EXPECT_EQ("Manufacturer#2"_sv, mfgr->valueAt(999));
  LOG(INFO) << rowVector1->toString(999);

  // Get second batch.
  auto rowVector2 = genTpchPart(pool_.get(), 1'000, 1'000);

  EXPECT_EQ(9, rowVector2->childrenSize());
  EXPECT_EQ(1'000, rowVector2->size());

  partKey = rowVector2->childAt(0)->asFlatVector<int64_t>();
  mfgr = rowVector2->childAt(2)->asFlatVector<StringView>();

  EXPECT_EQ(1'001, partKey->valueAt(0));
  EXPECT_EQ("Manufacturer#5"_sv, mfgr->valueAt(0));
  LOG(INFO) << rowVector2->toString(0);

  EXPECT_EQ(2'000, partKey->valueAt(999));
  EXPECT_EQ("Manufacturer#1"_sv, mfgr->valueAt(999));
  LOG(INFO) << rowVector2->toString(999);
}

TEST_F(TpchGenTestPartTest, lastBatch) {
  // Ask for 10'000 parts but there are only 10 left.
  auto rowVector = genTpchPart(pool_.get(), 10'000, 199'990);
  EXPECT_EQ(10, rowVector->size());

  // Ensure we get 1000 parts on a larger scale factor.
  rowVector = genTpchPart(pool_.get(), 1'000, 199'990, 2);
  EXPECT_EQ(1'000, rowVector->size());

  // Zero records if we go beyond the end.
  rowVector = genTpchPart(pool_.get(), 1'000, 200'000);
  EXPECT_EQ(0, rowVector->size());
}

TEST_F(TpchGenTestPartTest, reproducible) {
  auto rowVector1 = genTpchPart(pool_.get(), 100);
  auto rowVector2 = genTpchPart(pool_.get(), 100);
  auto rowVector3 = genTpchPart(pool_.get(), 100);

  for (size_t i = 0; i < rowVector1->size(); ++i) {
    ASSERT_TRUE(rowVector1->equalValueAt(rowVector2.get(), i, i));
    ASSERT_TRUE(rowVector1->equalValueAt(rowVector3.get(), i, i));
  }

  // Ensure it's also reproducible if we add an offset.
  auto rowVector4 = genTpchPart(pool_.get(), 100, 10);
  auto rowVector5 = genTpchPart(pool_.get(), 100, 10);

  for (size_t i = 0; i < rowVector4->size(); ++i) {
    ASSERT_TRUE(rowVector4->equalValueAt(rowVector5.get(), i, i));
  }

  // Ensure it's also reproducible if we add different offsets.
  auto rowVector6 = genTpchPart(pool_.get(), 100, 0);
  auto rowVector7 = genTpchPart(pool_.get(), 90, 10);

  for (size_t i = 0; i < rowVector7->size(); ++i) {
    ASSERT_TRUE(rowVector7->equalValueAt(rowVector6.get(), i, i + 10));
  }
}

// PartSupp.
class TpchGenTestPartSuppTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    pool_ = memory::memoryManager()->addLeafPool("TpchGenTestPartSuppTest");
  }

  bool partSuppCheck(
      const RowVectorPtr& vector,
      size_t idx,
      std::pair<size_t, size_t> expected) {
    return (expected.first ==
            vector->childAt(0)->asFlatVector<int64_t>()->valueAt(idx)) &&
        (expected.second ==
         vector->childAt(1)->asFlatVector<int64_t>()->valueAt(idx));
  }

  std::shared_ptr<memory::MemoryPool> pool_;
};

TEST_F(TpchGenTestPartSuppTest, batches) {
  auto rowVector1 = genTpchPartSupp(pool_.get(), 1'000);

  EXPECT_EQ(5, rowVector1->childrenSize());
  EXPECT_EQ(1'000, rowVector1->size());

  EXPECT_TRUE(partSuppCheck(rowVector1, 0, {1, 2}));
  EXPECT_TRUE(partSuppCheck(rowVector1, 1, {1, 2502}));
  EXPECT_TRUE(partSuppCheck(rowVector1, 2, {1, 5002}));
  EXPECT_TRUE(partSuppCheck(rowVector1, 3, {1, 7502}));
  EXPECT_TRUE(partSuppCheck(rowVector1, 4, {2, 3}));
  EXPECT_TRUE(partSuppCheck(rowVector1, 5, {2, 2503}));

  // Get second batch.
  auto rowVector2 = genTpchPartSupp(pool_.get(), 1'000, 1'000);

  EXPECT_EQ(5, rowVector2->childrenSize());
  EXPECT_EQ(1'000, rowVector2->size());

  EXPECT_TRUE(partSuppCheck(rowVector2, 0, {251, 252}));
  EXPECT_TRUE(partSuppCheck(rowVector2, 1, {251, 2752}));
  EXPECT_TRUE(partSuppCheck(rowVector2, 2, {251, 5252}));
  EXPECT_TRUE(partSuppCheck(rowVector2, 3, {251, 7752}));
  EXPECT_TRUE(partSuppCheck(rowVector2, 4, {252, 253}));
  EXPECT_TRUE(partSuppCheck(rowVector2, 5, {252, 2753}));
}

// PartSupp records are generated based on mk_part, which generates 4 partsupp
// records at a time. This tests that the 4 record boundary is transparent and
// works as expected.
TEST_F(TpchGenTestPartSuppTest, misalignedBatches) {
  auto rowVector = genTpchPartSupp(pool_.get(), 5, 0);
  EXPECT_EQ(5, rowVector->size());

  EXPECT_TRUE(partSuppCheck(rowVector, 0, {1, 2}));
  EXPECT_TRUE(partSuppCheck(rowVector, 1, {1, 2502}));
  EXPECT_TRUE(partSuppCheck(rowVector, 2, {1, 5002}));
  EXPECT_TRUE(partSuppCheck(rowVector, 3, {1, 7502}));
  EXPECT_TRUE(partSuppCheck(rowVector, 4, {2, 3}));

  // Rotate.
  rowVector = genTpchPartSupp(pool_.get(), 5, 1);
  EXPECT_EQ(5, rowVector->size());

  EXPECT_TRUE(partSuppCheck(rowVector, 0, {1, 2502}));
  EXPECT_TRUE(partSuppCheck(rowVector, 1, {1, 5002}));
  EXPECT_TRUE(partSuppCheck(rowVector, 2, {1, 7502}));
  EXPECT_TRUE(partSuppCheck(rowVector, 3, {2, 3}));
  EXPECT_TRUE(partSuppCheck(rowVector, 4, {2, 2503}));

  // Rotate.
  rowVector = genTpchPartSupp(pool_.get(), 5, 2);
  EXPECT_EQ(5, rowVector->size());

  EXPECT_TRUE(partSuppCheck(rowVector, 0, {1, 5002}));
  EXPECT_TRUE(partSuppCheck(rowVector, 1, {1, 7502}));
  EXPECT_TRUE(partSuppCheck(rowVector, 2, {2, 3}));
  EXPECT_TRUE(partSuppCheck(rowVector, 3, {2, 2503}));
  EXPECT_TRUE(partSuppCheck(rowVector, 4, {2, 5003}));

  // Rotate.
  rowVector = genTpchPartSupp(pool_.get(), 5, 3);
  EXPECT_EQ(5, rowVector->size());

  EXPECT_TRUE(partSuppCheck(rowVector, 0, {1, 7502}));
  EXPECT_TRUE(partSuppCheck(rowVector, 1, {2, 3}));
  EXPECT_TRUE(partSuppCheck(rowVector, 2, {2, 2503}));
  EXPECT_TRUE(partSuppCheck(rowVector, 3, {2, 5003}));
  EXPECT_TRUE(partSuppCheck(rowVector, 4, {2, 7503}));

  // Rotate. We're aligned to the 4-record window again.
  rowVector = genTpchPartSupp(pool_.get(), 5, 4);
  EXPECT_EQ(5, rowVector->size());

  EXPECT_TRUE(partSuppCheck(rowVector, 0, {2, 3}));
  EXPECT_TRUE(partSuppCheck(rowVector, 1, {2, 2503}));
  EXPECT_TRUE(partSuppCheck(rowVector, 2, {2, 5003}));
  EXPECT_TRUE(partSuppCheck(rowVector, 3, {2, 7503}));
  EXPECT_TRUE(partSuppCheck(rowVector, 4, {3, 4}));
}

TEST_F(TpchGenTestPartSuppTest, lastBatch) {
  // Ask for 1'000 records but there are only 10 left.
  auto rowVector = genTpchPartSupp(pool_.get(), 1'000, 799'990);
  EXPECT_EQ(10, rowVector->size());

  // Ensure we get 1'000 records on a larger scale factor.
  rowVector = genTpchPartSupp(pool_.get(), 1'000, 799'990, 2);
  EXPECT_EQ(1'000, rowVector->size());

  // Zero records if we go beyond the end.
  rowVector = genTpchPartSupp(pool_.get(), 1'000, 800'000);
  EXPECT_EQ(0, rowVector->size());
}

TEST_F(TpchGenTestPartSuppTest, reproducible) {
  auto rowVector1 = genTpchPartSupp(pool_.get(), 100);
  auto rowVector2 = genTpchPartSupp(pool_.get(), 100);
  auto rowVector3 = genTpchPartSupp(pool_.get(), 100);
  EXPECT_EQ(100, rowVector1->size());

  for (size_t i = 0; i < rowVector1->size(); ++i) {
    ASSERT_TRUE(rowVector1->equalValueAt(rowVector2.get(), i, i));
    ASSERT_TRUE(rowVector1->equalValueAt(rowVector3.get(), i, i));
  }

  // Ensure it's also reproducible if we add an offset.
  auto rowVector4 = genTpchPartSupp(pool_.get(), 100, 10);
  auto rowVector5 = genTpchPartSupp(pool_.get(), 100, 10);
  EXPECT_EQ(100, rowVector4->size());

  for (size_t i = 0; i < rowVector4->size(); ++i) {
    ASSERT_TRUE(rowVector4->equalValueAt(rowVector5.get(), i, i));
  }

  // Ensure it's also reproducible if we add different offsets.
  auto rowVector6 = genTpchPartSupp(pool_.get(), 100, 0);
  auto rowVector7 = genTpchPartSupp(pool_.get(), 91, 9);

  for (size_t i = 0; i < rowVector7->size(); ++i) {
    ASSERT_TRUE(rowVector7->equalValueAt(rowVector6.get(), i, i + 9));
  }
}

// Customer.
class TpchGenTestCustomerTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    pool_ = memory::memoryManager()->addLeafPool("TpchGenTestCustomerTest");
  }

  std::shared_ptr<memory::MemoryPool> pool_;
};

TEST_F(TpchGenTestCustomerTest, batches) {
  auto rowVector1 = genTpchCustomer(pool_.get(), 1'000);

  EXPECT_EQ(8, rowVector1->childrenSize());
  EXPECT_EQ(1'000, rowVector1->size());

  auto custKey = rowVector1->childAt(0)->asFlatVector<int64_t>();
  auto mktSegment = rowVector1->childAt(6)->asFlatVector<StringView>();

  EXPECT_EQ(1, custKey->valueAt(0));
  EXPECT_EQ("BUILDING"_sv, mktSegment->valueAt(0));

  EXPECT_EQ(1'000, custKey->valueAt(999));
  EXPECT_EQ("BUILDING"_sv, mktSegment->valueAt(999));
  LOG(INFO) << rowVector1->toString(999);

  // Get second batch.
  auto rowVector2 = genTpchCustomer(pool_.get(), 1'000, 1'000);

  EXPECT_EQ(8, rowVector2->childrenSize());
  EXPECT_EQ(1'000, rowVector2->size());

  custKey = rowVector2->childAt(0)->asFlatVector<int64_t>();
  mktSegment = rowVector2->childAt(6)->asFlatVector<StringView>();

  EXPECT_EQ(1'001, custKey->valueAt(0));
  EXPECT_EQ("MACHINERY"_sv, mktSegment->valueAt(0));
  LOG(INFO) << rowVector2->toString(0);

  EXPECT_EQ(2'000, custKey->valueAt(999));
  EXPECT_EQ("AUTOMOBILE"_sv, mktSegment->valueAt(999));
  LOG(INFO) << rowVector2->toString(999);
}

TEST_F(TpchGenTestCustomerTest, lastBatch) {
  // Ask for 10'000 customers but there are only 10 left.
  auto rowVector = genTpchCustomer(pool_.get(), 10'000, 149'990);
  EXPECT_EQ(10, rowVector->size());

  // Ensure we get 1000 customers on a larger scale factor.
  rowVector = genTpchCustomer(pool_.get(), 1'000, 149'990, 2);
  EXPECT_EQ(1'000, rowVector->size());

  // Zero records if we go beyond the end.
  rowVector = genTpchCustomer(pool_.get(), 1'000, 200'000);
  EXPECT_EQ(0, rowVector->size());
}

TEST_F(TpchGenTestCustomerTest, reproducible) {
  auto rowVector1 = genTpchCustomer(pool_.get(), 100);
  auto rowVector2 = genTpchCustomer(pool_.get(), 100);
  auto rowVector3 = genTpchCustomer(pool_.get(), 100);

  for (size_t i = 0; i < rowVector1->size(); ++i) {
    ASSERT_TRUE(rowVector1->equalValueAt(rowVector2.get(), i, i));
    ASSERT_TRUE(rowVector1->equalValueAt(rowVector3.get(), i, i));
  }

  // Ensure it's also reproducible if we add an offset.
  auto rowVector4 = genTpchCustomer(pool_.get(), 100, 10);
  auto rowVector5 = genTpchCustomer(pool_.get(), 100, 10);

  for (size_t i = 0; i < rowVector4->size(); ++i) {
    ASSERT_TRUE(rowVector4->equalValueAt(rowVector5.get(), i, i));
  }

  // Ensure it's also reproducible if we add different offsets.
  auto rowVector6 = genTpchCustomer(pool_.get(), 100, 0);
  auto rowVector7 = genTpchCustomer(pool_.get(), 90, 10);

  for (size_t i = 0; i < rowVector7->size(); ++i) {
    ASSERT_TRUE(rowVector7->equalValueAt(rowVector6.get(), i, i + 10));
  }
}

} // namespace

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init{&argc, &argv, false};
  return RUN_ALL_TESTS();
}

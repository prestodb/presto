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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "velox/dwio/common/OnDemandUnitLoader.h"
#include "velox/dwio/common/UnitLoaderTools.h"
#include "velox/dwio/common/tests/utils/UnitLoaderTestTools.h"

using namespace ::testing;
using facebook::velox::dwio::common::LoadUnit;
using facebook::velox::dwio::common::OnDemandUnitLoaderFactory;
using facebook::velox::dwio::common::UnitLoader;
using facebook::velox::dwio::common::UnitLoaderFactory;
using facebook::velox::dwio::common::test::getUnitsLoadedWithFalse;
using facebook::velox::dwio::common::test::LoadUnitMock;
using facebook::velox::dwio::common::test::ReaderMock;

TEST(OnDemandUnitLoaderTests, LoadsCorrectlyWithReader) {
  size_t blockedOnIoCount = 0;
  OnDemandUnitLoaderFactory factory([&](auto) { ++blockedOnIoCount; });
  ReaderMock readerMock{{10, 20, 30}, {0, 0, 0}, factory};
  EXPECT_EQ(readerMock.unitsLoaded(), std::vector<bool>({false, false, false}));
  EXPECT_EQ(blockedOnIoCount, 0);

  EXPECT_TRUE(readerMock.read(3)); // Unit: 0, rows: 0-2, load(0)
  EXPECT_EQ(readerMock.unitsLoaded(), std::vector<bool>({true, false, false}));
  EXPECT_EQ(blockedOnIoCount, 1);

  EXPECT_TRUE(readerMock.read(3)); // Unit: 0, rows: 3-5
  EXPECT_EQ(readerMock.unitsLoaded(), std::vector<bool>({true, false, false}));
  EXPECT_EQ(blockedOnIoCount, 1);

  EXPECT_TRUE(readerMock.read(4)); // Unit: 0, rows: 6-9
  EXPECT_EQ(readerMock.unitsLoaded(), std::vector<bool>({true, false, false}));
  EXPECT_EQ(blockedOnIoCount, 1);

  EXPECT_TRUE(readerMock.read(14)); // Unit: 1, rows: 0-13, unload(0), load(1)
  EXPECT_EQ(readerMock.unitsLoaded(), std::vector<bool>({false, true, false}));
  EXPECT_EQ(blockedOnIoCount, 2);

  // will only read 5 rows, no more rows in unit 1
  EXPECT_TRUE(readerMock.read(10)); // Unit: 1, rows: 14-19
  EXPECT_EQ(readerMock.unitsLoaded(), std::vector<bool>({false, true, false}));
  EXPECT_EQ(blockedOnIoCount, 2);

  EXPECT_TRUE(readerMock.read(30)); // Unit: 2, rows: 0-29, unload(1), load(2)
  EXPECT_EQ(readerMock.unitsLoaded(), std::vector<bool>({false, false, true}));
  EXPECT_EQ(blockedOnIoCount, 3);

  EXPECT_FALSE(readerMock.read(30)); // No more data
  EXPECT_EQ(readerMock.unitsLoaded(), std::vector<bool>({false, false, true}));
  EXPECT_EQ(blockedOnIoCount, 3);
}

TEST(OnDemandUnitLoaderTests, LoadsCorrectlyWithNoCallback) {
  OnDemandUnitLoaderFactory factory(nullptr);
  ReaderMock readerMock{{10, 20, 30}, {0, 0, 0}, factory};
  EXPECT_EQ(readerMock.unitsLoaded(), std::vector<bool>({false, false, false}));

  EXPECT_TRUE(readerMock.read(3)); // Unit: 0, rows: 0-2, load(0)
  EXPECT_EQ(readerMock.unitsLoaded(), std::vector<bool>({true, false, false}));

  EXPECT_TRUE(readerMock.read(3)); // Unit: 0, rows: 3-5
  EXPECT_EQ(readerMock.unitsLoaded(), std::vector<bool>({true, false, false}));

  EXPECT_TRUE(readerMock.read(4)); // Unit: 0, rows: 6-9
  EXPECT_EQ(readerMock.unitsLoaded(), std::vector<bool>({true, false, false}));

  EXPECT_TRUE(readerMock.read(14)); // Unit: 1, rows: 0-13, unload(2), load(1)
  EXPECT_EQ(readerMock.unitsLoaded(), std::vector<bool>({false, true, false}));

  // will only read 5 rows, no more rows in unit 1
  EXPECT_TRUE(readerMock.read(10)); // Unit: 1, rows: 14-19
  EXPECT_EQ(readerMock.unitsLoaded(), std::vector<bool>({false, true, false}));

  EXPECT_TRUE(readerMock.read(30)); // Unit: 2, rows: 0-29, unload(1), load(2)
  EXPECT_EQ(readerMock.unitsLoaded(), std::vector<bool>({false, false, true}));

  EXPECT_FALSE(readerMock.read(30)); // No more data
  EXPECT_EQ(readerMock.unitsLoaded(), std::vector<bool>({false, false, true}));
}

TEST(OnDemandUnitLoaderTests, UnitOutOfRange) {
  OnDemandUnitLoaderFactory factory(nullptr);
  std::vector<std::atomic_bool> unitsLoaded(getUnitsLoadedWithFalse(1));
  std::vector<std::unique_ptr<LoadUnit>> units;
  units.push_back(std::make_unique<LoadUnitMock>(10, 0, unitsLoaded, 0));

  auto unitLoader = factory.create(std::move(units));
  unitLoader->getLoadedUnit(0);
  EXPECT_THAT(
      [&]() { unitLoader->getLoadedUnit(1); },
      Throws<facebook::velox::VeloxRuntimeError>(Property(
          &facebook::velox::VeloxRuntimeError::message,
          HasSubstr("Unit out of range"))));
}

TEST(OnDemandUnitLoaderTests, CanRequestUnitMultipleTimes) {
  OnDemandUnitLoaderFactory factory(nullptr);
  std::vector<std::atomic_bool> unitsLoaded(getUnitsLoadedWithFalse(1));
  std::vector<std::unique_ptr<LoadUnit>> units;
  units.push_back(std::make_unique<LoadUnitMock>(10, 0, unitsLoaded, 0));

  auto unitLoader = factory.create(std::move(units));
  unitLoader->getLoadedUnit(0);
  unitLoader->getLoadedUnit(0);
  unitLoader->getLoadedUnit(0);
}

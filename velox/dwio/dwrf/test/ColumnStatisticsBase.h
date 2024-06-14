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

#pragma once

#include <gtest/gtest.h>

#include "velox/dwio/common/Statistics.h"
#include "velox/dwio/dwrf/writer/StatisticsBuilder.h"

namespace facebook::velox::dwrf {
class ColumnStatisticsBase {
 public:
  ColumnStatisticsBase()
      : arena_(std::make_unique<google::protobuf::Arena>()) {}

  void testSize() {
    {
      StatisticsBuilder missingSize{options()};
      ASSERT_FALSE(missingSize.getSize().has_value());
      StatisticsBuilder hasSize{StatisticsBuilderOptions{
          /*stringLengthLimit=*/32, /*initialSize=*/10}};
      ASSERT_TRUE(hasSize.getSize().has_value());
      EXPECT_EQ(10, hasSize.getSize().value());

      hasSize.merge(missingSize, /*ignoreSize=*/true);
      EXPECT_FALSE(missingSize.getSize().has_value());
      ASSERT_TRUE(hasSize.getSize().has_value());
      EXPECT_EQ(10, hasSize.getSize().value());

      // Coercing to missing/invalid size when not ignoring by default.
      hasSize.merge(missingSize);
      EXPECT_FALSE(missingSize.getSize().has_value());
      EXPECT_FALSE(hasSize.getSize().has_value());
    }
    {
      StatisticsBuilder from{options()};
      ASSERT_FALSE(from.getSize().has_value());

      StatisticsBuilder to{options()};
      ASSERT_FALSE(to.getSize().has_value());
      to.incrementSize(64);
      ASSERT_FALSE(to.getSize().has_value());

      to.ensureSize();
      ASSERT_EQ(0, to.getSize().value());
      to.incrementSize(64);
      EXPECT_EQ(64, to.getSize().value());

      to.merge(from, /*ignoreSize=*/true);
      EXPECT_FALSE(from.getSize().has_value());
      EXPECT_EQ(64, to.getSize().value());

      to.merge(from);
      EXPECT_FALSE(from.getSize().has_value());
      EXPECT_FALSE(to.getSize().has_value());
    }
  }

  void testInteger() {
    IntegerStatisticsBuilder builder{options()};
    // empty builder should have all defaults
    EXPECT_EQ(std::numeric_limits<int64_t>::max(), builder.getMinimum());
    EXPECT_EQ(std::numeric_limits<int64_t>::min(), builder.getMaximum());
    EXPECT_EQ(0, builder.getSum());
    EXPECT_EQ(0, builder.getNumberOfValues());

    builder.addValues(3);
    builder.addValues(1);
    builder.addValues(5);

    // stats should be merged
    IntegerStatisticsBuilder target{options()};
    target.merge(*builder.build());
    auto stats = as<dwio::common::IntegerColumnStatistics>(target.build());
    EXPECT_EQ(3, stats->getNumberOfValues());
    EXPECT_EQ(1, stats->getMinimum());
    EXPECT_EQ(5, stats->getMaximum());
    EXPECT_EQ(9, stats->getSum());

    // stats should be merged again
    builder.addValues(0);
    builder.addValues(6);
    target.merge(*builder.build());
    stats = as<dwio::common::IntegerColumnStatistics>(target.build());
    EXPECT_EQ(8, stats->getNumberOfValues());
    EXPECT_EQ(0, stats->getMinimum());
    EXPECT_EQ(6, stats->getMaximum());
    EXPECT_EQ(24, stats->getSum());

    target.merge(*builder.build());
    stats = as<dwio::common::IntegerColumnStatistics>(target.build());
    EXPECT_EQ(13, stats->getNumberOfValues());
    EXPECT_EQ(0, stats->getMinimum());
    EXPECT_EQ(6, stats->getMaximum());
    EXPECT_EQ(39, stats->getSum());

    // add value
    target.addValues(100, 2);
    stats = as<dwio::common::IntegerColumnStatistics>(target.build());
    EXPECT_EQ(15, stats->getNumberOfValues());
    EXPECT_EQ(0, stats->getMinimum());
    EXPECT_EQ(100, stats->getMaximum());
    EXPECT_EQ(239, stats->getSum());

    // reset
    builder.reset();
    EXPECT_EQ(std::numeric_limits<int64_t>::max(), builder.getMinimum());
    EXPECT_EQ(std::numeric_limits<int64_t>::min(), builder.getMaximum());
    EXPECT_EQ(0, builder.getSum());
    EXPECT_EQ(0, builder.getNumberOfValues());
  }

  void testIntegerMissingStats(
      ColumnStatisticsWrapper columnStatisticsWrapper,
      void* protoPtr,
      DwrfFormat format) {
    IntegerStatisticsBuilder target{options()};
    target.addValues(1, 5);
    auto stats = as<dwio::common::IntegerColumnStatistics>(target.build());
    EXPECT_EQ(5, stats->getSum());

    // merge missing stats
    target.merge(
        *buildColumnStatisticsFromProto(columnStatisticsWrapper, context()));
    stats = as<dwio::common::IntegerColumnStatistics>(target.build());
    EXPECT_EQ(stats, nullptr);

    // merge again
    if (format == DwrfFormat::kDwrf) {
      auto intPtr = reinterpret_cast<proto::IntegerStatistics*>(protoPtr);
      intPtr->set_minimum(0);
      intPtr->set_maximum(1);
      intPtr->set_sum(100);
    } else {
      auto intPtr = reinterpret_cast<proto::orc::IntegerStatistics*>(protoPtr);
      intPtr->set_minimum(0);
      intPtr->set_maximum(1);
      intPtr->set_sum(100);
    }

    target.merge(
        *buildColumnStatisticsFromProto(columnStatisticsWrapper, context()));
    stats = as<dwio::common::IntegerColumnStatistics>(target.build());
    EXPECT_EQ(stats, nullptr);

    // add again
    target.addValues(2);
    stats = as<dwio::common::IntegerColumnStatistics>(target.build());
    EXPECT_EQ(stats, nullptr);
  }

  void testIntegerEmptyStats(
      ColumnStatisticsWrapper columnStatisticsWrapper,
      void* protoPtr,
      DwrfFormat format) {
    IntegerStatisticsBuilder target{options()};
    target.addValues(1, 5);
    auto stats = as<dwio::common::IntegerColumnStatistics>(target.build());
    EXPECT_EQ(5, stats->getSum());

    // merge empty stats
    target.merge(
        *buildColumnStatisticsFromProto(columnStatisticsWrapper, context()));
    stats = as<dwio::common::IntegerColumnStatistics>(target.build());
    EXPECT_EQ(5, stats->getSum());

    // merge again
    if (format == DwrfFormat::kDwrf) {
      auto columnPtr = reinterpret_cast<proto::ColumnStatistics*>(protoPtr);
      columnPtr->clear_numberofvalues();
    } else {
      auto columnPtr =
          reinterpret_cast<proto::orc::ColumnStatistics*>(protoPtr);
      columnPtr->clear_numberofvalues();
    }
    target.merge(
        *buildColumnStatisticsFromProto(columnStatisticsWrapper, context()));
    stats = as<dwio::common::IntegerColumnStatistics>(target.build());
    EXPECT_EQ(stats, nullptr);
  }

  void testIntegerOverflow() {
    IntegerStatisticsBuilder target{options()};
    auto testMinMax =
        [&](int64_t val1, int64_t val2, int64_t min, int64_t max) {
          target.reset();
          target.addValues(val1);
          auto stats =
              as<dwio::common::IntegerColumnStatistics>(target.build());
          EXPECT_EQ(val1, stats->getMaximum());
          EXPECT_EQ(val1, stats->getMinimum());
          EXPECT_EQ(val1, stats->getSum());

          target.addValues(val2);
          stats = as<dwio::common::IntegerColumnStatistics>(target.build());
          EXPECT_EQ(max, stats->getMaximum());
          EXPECT_EQ(min, stats->getMinimum());
          EXPECT_FALSE(stats->getSum().has_value());
        };

    testMinMax(
        std::numeric_limits<int64_t>::min(),
        -1,
        std::numeric_limits<int64_t>::min(),
        -1);
    testMinMax(
        std::numeric_limits<int64_t>::max(),
        1,
        1,
        std::numeric_limits<int64_t>::max());

    // make sure we also capture overflow that happened for adding multiple
    // items
    target.reset();
    target.addValues(std::numeric_limits<int64_t>::max() / 10, 11);
    auto stats = as<dwio::common::IntegerColumnStatistics>(target.build());
    EXPECT_EQ(11, stats->getNumberOfValues());
    EXPECT_EQ(stats->getMaximum().value(), stats->getMinimum().value());
    EXPECT_FALSE(stats->getSum().has_value());

    // merge overflow
    auto testMergeOverflow = [&](int64_t val1, int64_t val2) {
      target.reset();
      target.addValues(val1);
      IntegerStatisticsBuilder builder{options()};
      builder.addValues(val2);
      target.merge(builder);
      stats = as<dwio::common::IntegerColumnStatistics>(target.build());
      EXPECT_FALSE(stats->getSum().has_value());
    };
    testMergeOverflow(std::numeric_limits<int64_t>::min(), -1);
    testMergeOverflow(std::numeric_limits<int64_t>::max(), 1);
  }

  void testDoubles() {
    DoubleStatisticsBuilder builder{options()};
    // empty builder should have all defaults
    EXPECT_EQ(std::numeric_limits<double>::infinity(), builder.getMinimum());
    EXPECT_EQ(-std::numeric_limits<double>::infinity(), builder.getMaximum());
    EXPECT_EQ(0, builder.getSum());
    EXPECT_EQ(0, builder.getNumberOfValues());

    builder.addValues(3);
    builder.addValues(1);
    builder.addValues(5);

    // stats should be merged
    DoubleStatisticsBuilder target{options()};
    target.merge(*builder.build());
    auto stats = as<dwio::common::DoubleColumnStatistics>(target.build());
    EXPECT_EQ(3, stats->getNumberOfValues());
    EXPECT_EQ(1, stats->getMinimum());
    EXPECT_EQ(5, stats->getMaximum());
    EXPECT_EQ(9, stats->getSum());

    // stats should be merged again
    builder.addValues(0);
    builder.addValues(6);
    target.merge(*builder.build());
    stats = as<dwio::common::DoubleColumnStatistics>(target.build());
    EXPECT_EQ(8, stats->getNumberOfValues());
    EXPECT_EQ(0, stats->getMinimum());
    EXPECT_EQ(6, stats->getMaximum());
    EXPECT_EQ(24, stats->getSum());

    target.merge(*builder.build());
    stats = as<dwio::common::DoubleColumnStatistics>(target.build());
    EXPECT_EQ(13, stats->getNumberOfValues());
    EXPECT_EQ(0, stats->getMinimum());
    EXPECT_EQ(6, stats->getMaximum());
    EXPECT_EQ(39, stats->getSum());

    // add value
    target.addValues(100, 2);
    stats = as<dwio::common::DoubleColumnStatistics>(target.build());
    EXPECT_EQ(15, stats->getNumberOfValues());
    EXPECT_EQ(0, stats->getMinimum());
    EXPECT_EQ(100, stats->getMaximum());
    EXPECT_EQ(239, stats->getSum());

    // reset
    builder.reset();
    EXPECT_EQ(std::numeric_limits<double>::infinity(), builder.getMinimum());
    EXPECT_EQ(-std::numeric_limits<double>::infinity(), builder.getMaximum());
    EXPECT_EQ(0, builder.getSum());
    EXPECT_EQ(0, builder.getNumberOfValues());
  }

  void testDoubleMissingStats(
      ColumnStatisticsWrapper columnStatisticsWrapper,
      void* protoPtr,
      DwrfFormat format) {
    DoubleStatisticsBuilder target{options()};
    target.addValues(1, 5);
    auto stats = as<dwio::common::DoubleColumnStatistics>(target.build());
    EXPECT_EQ(5, stats->getSum());

    target.merge(
        *buildColumnStatisticsFromProto(columnStatisticsWrapper, context()));
    stats = as<dwio::common::DoubleColumnStatistics>(target.build());
    EXPECT_EQ(stats, nullptr);

    // merge again
    if (format == DwrfFormat::kDwrf) {
      auto doubleProto = reinterpret_cast<proto::DoubleStatistics*>(protoPtr);
      doubleProto->set_minimum(0);
      doubleProto->set_maximum(1);
      doubleProto->set_sum(100);
    } else {
      auto doubleProto =
          reinterpret_cast<proto::orc::DoubleStatistics*>(protoPtr);
      doubleProto->set_minimum(0);
      doubleProto->set_maximum(1);
      doubleProto->set_sum(100);
    }
    target.merge(
        *buildColumnStatisticsFromProto(columnStatisticsWrapper, context()));
    stats = as<dwio::common::DoubleColumnStatistics>(target.build());
    EXPECT_EQ(stats, nullptr);

    // add again
    target.addValues(2);
    stats = as<dwio::common::DoubleColumnStatistics>(target.build());
    EXPECT_EQ(stats, nullptr);
  }

  void testDoubleEmptyStats(
      ColumnStatisticsWrapper columnStatisticsWrapper,
      void* protoPtr,
      DwrfFormat format) {
    DoubleStatisticsBuilder target{options()};
    target.addValues(1, 5);
    auto stats = as<dwio::common::DoubleColumnStatistics>(target.build());
    EXPECT_EQ(5, stats->getSum());

    // merge empty stats
    target.merge(
        *buildColumnStatisticsFromProto(columnStatisticsWrapper, context()));
    stats = as<dwio::common::DoubleColumnStatistics>(target.build());
    EXPECT_EQ(5, stats->getSum());

    // merge again
    if (format == DwrfFormat::kDwrf) {
      auto columnPtr = reinterpret_cast<proto::ColumnStatistics*>(protoPtr);
      columnPtr->clear_numberofvalues();
    } else {
      auto columnPtr =
          reinterpret_cast<proto::orc::ColumnStatistics*>(protoPtr);
      columnPtr->clear_numberofvalues();
    }
    target.merge(
        *buildColumnStatisticsFromProto(columnStatisticsWrapper, context()));
    stats = as<dwio::common::DoubleColumnStatistics>(target.build());
    EXPECT_EQ(stats, nullptr);
  }

  void testDoubleNaN() {
    DoubleStatisticsBuilder target{options()};
    // test nan. Nan causes fallback to basic stats.
    target.addValues(std::numeric_limits<float>::quiet_NaN());
    auto stats = as<dwio::common::DoubleColumnStatistics>(target.build());
    EXPECT_EQ(stats, nullptr);

    target.reset();
    target.addValues(std::numeric_limits<double>::infinity());
    target.addValues(-std::numeric_limits<double>::infinity());
    stats = as<dwio::common::DoubleColumnStatistics>(target.build());
    EXPECT_EQ(stats->getMaximum(), std::numeric_limits<double>::infinity());
    EXPECT_EQ(stats->getMinimum(), -std::numeric_limits<double>::infinity());
    EXPECT_FALSE(stats->getSum().has_value());

    target.reset();
    DoubleStatisticsBuilder builder{options()};
    target.addValues(std::numeric_limits<double>::infinity());
    builder.addValues(-std::numeric_limits<double>::infinity());
    target.merge(*builder.build());
    stats = as<dwio::common::DoubleColumnStatistics>(target.build());
    EXPECT_EQ(stats->getMaximum(), std::numeric_limits<double>::infinity());
    EXPECT_EQ(stats->getMinimum(), -std::numeric_limits<double>::infinity());
    EXPECT_FALSE(stats->getSum().has_value());
  }

  void testString() {
    StringStatisticsBuilder builder{options()};
    // empty builder should have all defaults
    EXPECT_FALSE(builder.getMinimum().has_value());
    EXPECT_FALSE(builder.getMaximum().has_value());
    EXPECT_EQ(0, builder.getTotalLength());
    EXPECT_EQ(0, builder.getNumberOfValues());

    builder.addValues("xx");
    builder.addValues("bb");
    builder.addValues("yy");

    // stats should be merged
    StringStatisticsBuilder target{options()};
    target.merge(*builder.build());
    auto stats = as<dwio::common::StringColumnStatistics>(target.build());
    EXPECT_EQ(3, stats->getNumberOfValues());
    EXPECT_EQ("bb", stats->getMinimum());
    EXPECT_EQ("yy", stats->getMaximum());
    EXPECT_EQ(6, stats->getTotalLength());

    // stats should be merged again
    builder.addValues("aa");
    builder.addValues("zz");
    target.merge(*builder.build());
    stats = as<dwio::common::StringColumnStatistics>(target.build());
    EXPECT_EQ(8, stats->getNumberOfValues());
    EXPECT_EQ("aa", stats->getMinimum());
    EXPECT_EQ("zz", stats->getMaximum());
    EXPECT_EQ(16, stats->getTotalLength());

    target.merge(*builder.build());
    stats = as<dwio::common::StringColumnStatistics>(target.build());
    EXPECT_EQ(13, stats->getNumberOfValues());
    EXPECT_EQ("aa", stats->getMinimum());
    EXPECT_EQ("zz", stats->getMaximum());
    EXPECT_EQ(26, stats->getTotalLength());

    // add value
    target.addValues("zzz", 2);
    stats = as<dwio::common::StringColumnStatistics>(target.build());
    EXPECT_EQ(15, stats->getNumberOfValues());
    EXPECT_EQ("aa", stats->getMinimum());
    EXPECT_EQ("zzz", stats->getMaximum());
    EXPECT_EQ(32, stats->getTotalLength());

    // reset
    builder.reset();
    EXPECT_FALSE(builder.getMinimum().has_value());
    EXPECT_FALSE(builder.getMaximum().has_value());
    EXPECT_EQ(0, builder.getTotalLength());
    EXPECT_EQ(0, builder.getNumberOfValues());
  }

  void testStringMissingStats(
      ColumnStatisticsWrapper columnStatisticsWrapper,
      void* protoPtr,
      DwrfFormat format) {
    StringStatisticsBuilder target{options()};
    target.addValues("zz", 5);
    auto stats = as<dwio::common::StringColumnStatistics>(target.build());
    EXPECT_EQ(10, stats->getTotalLength());

    // merge missing stats
    target.merge(
        *buildColumnStatisticsFromProto(columnStatisticsWrapper, context()));
    stats = as<dwio::common::StringColumnStatistics>(target.build());
    EXPECT_EQ(stats, nullptr);

    // merge again
    if (format == DwrfFormat::kDwrf) {
      auto strProto = reinterpret_cast<proto::StringStatistics*>(protoPtr);
      strProto->set_minimum("aa");
      strProto->set_maximum("bb");
      strProto->set_sum(100);
    } else {
      auto strProto = reinterpret_cast<proto::orc::StringStatistics*>(protoPtr);
      strProto->set_minimum("aa");
      strProto->set_maximum("bb");
      strProto->set_sum(100);
    }

    target.merge(
        *buildColumnStatisticsFromProto(columnStatisticsWrapper, context()));
    stats = as<dwio::common::StringColumnStatistics>(target.build());
    EXPECT_EQ(stats, nullptr);

    // add again
    target.addValues("aa");
    stats = as<dwio::common::StringColumnStatistics>(target.build());
    EXPECT_EQ(stats, nullptr);
  }

  void testStringEmptyStats(
      ColumnStatisticsWrapper columnStatisticsWrapper,
      void* protoPtr,
      DwrfFormat format) {
    StringStatisticsBuilder target{options()};
    target.addValues("zz", 5);
    auto stats = as<dwio::common::StringColumnStatistics>(target.build());
    EXPECT_EQ(10, stats->getTotalLength());

    // merge empty stats
    target.merge(
        *buildColumnStatisticsFromProto(columnStatisticsWrapper, context()));
    stats = as<dwio::common::StringColumnStatistics>(target.build());
    EXPECT_EQ(10, stats->getTotalLength());

    // merge again
    if (format == DwrfFormat::kDwrf) {
      auto proto = reinterpret_cast<proto::ColumnStatistics*>(protoPtr);
      proto->clear_numberofvalues();
    } else {
      auto proto = reinterpret_cast<proto::orc::ColumnStatistics*>(protoPtr);
      proto->clear_numberofvalues();
    }

    target.merge(
        *buildColumnStatisticsFromProto(columnStatisticsWrapper, context()));
    stats = as<dwio::common::StringColumnStatistics>(target.build());
    EXPECT_EQ(stats, nullptr);
  }

  void testStringLengthThreshold() {
    StringStatisticsBuilder target{StatisticsBuilderOptions{2}};
    target.addValues("yyy");
    auto stats = as<dwio::common::StringColumnStatistics>(target.build());
    EXPECT_FALSE(stats->getMinimum().has_value());
    EXPECT_FALSE(stats->getMaximum().has_value());

    // merge empty stats
    target.addValues("aa");
    target.addValues("zz");
    stats = as<dwio::common::StringColumnStatistics>(target.build());
    EXPECT_EQ(stats->getMinimum(), "aa");
    EXPECT_EQ(stats->getMaximum(), "zz");
  }

  void testStringLengthOverflow(
      ColumnStatisticsWrapper columnStatisticsWrapper,
      void* protoPtr,
      DwrfFormat format) {
    // add value causing overflow
    StringStatisticsBuilder target{options()};

    if (format == DwrfFormat::kDwrf) {
      auto strProto = reinterpret_cast<proto::StringStatistics*>(protoPtr);
      strProto->set_sum(std::numeric_limits<int64_t>::max());
      strProto->set_minimum("foo");
    } else {
      auto strProto = reinterpret_cast<proto::orc::StringStatistics*>(protoPtr);
      strProto->set_sum(std::numeric_limits<int64_t>::max());
      strProto->set_minimum("foo");
    }
    target.merge(
        *buildColumnStatisticsFromProto(columnStatisticsWrapper, context()));
    EXPECT_TRUE(target.getTotalLength().has_value());
    auto stats = as<dwio::common::StringColumnStatistics>(target.build());
    EXPECT_TRUE(stats->getTotalLength().has_value());

    target.addValues("foo");
    EXPECT_TRUE(target.getTotalLength().has_value());
    stats = as<dwio::common::StringColumnStatistics>(target.build());
    EXPECT_FALSE(stats->getTotalLength().has_value());

    // merge causing overflow
    target.reset();
    target.merge(
        *buildColumnStatisticsFromProto(columnStatisticsWrapper, context()));
    target.merge(
        *buildColumnStatisticsFromProto(columnStatisticsWrapper, context()));
    EXPECT_TRUE(target.getTotalLength().has_value());
    stats = as<dwio::common::StringColumnStatistics>(target.build());
    EXPECT_FALSE(stats->getTotalLength().has_value());
  }

  void testBoolean() {
    BooleanStatisticsBuilder builder{options()};
    // empty builder should have all defaults
    EXPECT_EQ(0, builder.getTrueCount());
    EXPECT_EQ(0, builder.getNumberOfValues());

    builder.addValues(true, 2);

    // stats should be merged
    BooleanStatisticsBuilder target{options()};
    target.merge(*builder.build());
    auto stats = as<dwio::common::BooleanColumnStatistics>(target.build());
    EXPECT_EQ(2, stats->getNumberOfValues());
    EXPECT_EQ(2, stats->getTrueCount());

    // stats should be merged again
    target.merge(*builder.build());
    stats = as<dwio::common::BooleanColumnStatistics>(target.build());
    EXPECT_EQ(4, stats->getNumberOfValues());
    EXPECT_EQ(4, stats->getTrueCount());

    // add value
    target.addValues(false, 2);
    stats = as<dwio::common::BooleanColumnStatistics>(target.build());
    EXPECT_EQ(6, stats->getNumberOfValues());
    EXPECT_EQ(4, stats->getTrueCount());

    // reset
    builder.reset();
    EXPECT_EQ(0, builder.getTrueCount());
    EXPECT_EQ(0, builder.getNumberOfValues());
  }

  void testBooleanMissingStats(
      ColumnStatisticsWrapper columnStatisticsWrapper,
      void* protoPtr,
      DwrfFormat format) {
    BooleanStatisticsBuilder target{options()};
    target.addValues(true, 5);
    auto stats = as<dwio::common::BooleanColumnStatistics>(target.build());
    EXPECT_EQ(5, stats->getTrueCount());

    // merge missing stats
    target.merge(
        *buildColumnStatisticsFromProto(columnStatisticsWrapper, context()));
    stats = as<dwio::common::BooleanColumnStatistics>(target.build());
    EXPECT_EQ(stats, nullptr);

    // merge again
    if (format == DwrfFormat::kDwrf) {
      auto boolProto = reinterpret_cast<proto::BucketStatistics*>(protoPtr);
      boolProto->add_count(1);
    } else {
      auto boolProto =
          reinterpret_cast<proto::orc::BucketStatistics*>(protoPtr);
      boolProto->add_count(1);
    }

    target.merge(
        *buildColumnStatisticsFromProto(columnStatisticsWrapper, context()));
    stats = as<dwio::common::BooleanColumnStatistics>(target.build());
    EXPECT_EQ(stats, nullptr);

    // add again
    target.addValues(true);
    stats = as<dwio::common::BooleanColumnStatistics>(target.build());
    EXPECT_EQ(stats, nullptr);
  }

  void testBooleanEmptyStats(
      ColumnStatisticsWrapper columnStatisticsWrapper,
      void* protoPtr,
      DwrfFormat format) {
    BooleanStatisticsBuilder target{options()};
    target.addValues(true, 5);
    auto stats = as<dwio::common::BooleanColumnStatistics>(target.build());
    EXPECT_EQ(5, stats->getTrueCount());

    // merge empty stats
    target.merge(
        *buildColumnStatisticsFromProto(columnStatisticsWrapper, context()));
    stats = as<dwio::common::BooleanColumnStatistics>(target.build());
    EXPECT_EQ(5, stats->getTrueCount());

    // merge again
    if (format == DwrfFormat::kDwrf) {
      auto proto = reinterpret_cast<proto::ColumnStatistics*>(protoPtr);
      proto->clear_numberofvalues();
    } else {
      auto proto = reinterpret_cast<proto::orc::ColumnStatistics*>(protoPtr);
      proto->clear_numberofvalues();
    }

    target.merge(
        *buildColumnStatisticsFromProto(columnStatisticsWrapper, context()));
    stats = as<dwio::common::BooleanColumnStatistics>(target.build());
    EXPECT_EQ(stats, nullptr);
  }

  void testBasic() {
    StatisticsBuilder builder{options()};
    EXPECT_EQ(0, builder.getNumberOfValues());
    EXPECT_EQ(0, builder.getRawSize());
    EXPECT_FALSE(builder.hasNull().value());

    builder.increaseValueCount(5);
    builder.increaseRawSize(10);
    builder.setHasNull();

    // stats should be merged
    StatisticsBuilder target{options()};
    target.merge(*builder.build());
    auto stats = target.build();
    EXPECT_EQ(5, stats->getNumberOfValues());
    EXPECT_EQ(10, stats->getRawSize());
    EXPECT_TRUE(stats->hasNull().value());

    // stats should be merged again
    target.merge(*builder.build());
    stats = target.build();
    EXPECT_EQ(10, stats->getNumberOfValues());
    EXPECT_EQ(20, stats->getRawSize());
    EXPECT_TRUE(stats->hasNull().value());

    // add value
    target.increaseValueCount(1);
    target.increaseRawSize(2);
    stats = target.build();
    EXPECT_EQ(11, stats->getNumberOfValues());
    EXPECT_EQ(22, stats->getRawSize());
    EXPECT_TRUE(stats->hasNull().value());

    // reset
    builder.reset();
    EXPECT_EQ(0, builder.getNumberOfValues());
    EXPECT_EQ(0, builder.getRawSize());
    EXPECT_FALSE(builder.hasNull().value());
  }

  void testBasicMissingStats(ColumnStatisticsWrapper columnStatisticsWrapper) {
    StatisticsBuilder target{options()};
    target.increaseValueCount(5);
    target.increaseRawSize(10);
    auto stats = target.build();
    EXPECT_EQ(5, stats->getNumberOfValues());
    EXPECT_EQ(10, stats->getRawSize());
    EXPECT_FALSE(stats->hasNull().value());

    // merge missing stats
    target.merge(
        *buildColumnStatisticsFromProto(columnStatisticsWrapper, context()));
    stats = target.build();
    EXPECT_FALSE(stats->getNumberOfValues().has_value());
    EXPECT_FALSE(stats->getRawSize().has_value());
    EXPECT_FALSE(stats->hasNull().has_value());

    // add again
    target.increaseValueCount(5);
    target.increaseRawSize(10);
    target.setHasNull();
    stats = target.build();
    EXPECT_FALSE(stats->getNumberOfValues().has_value());
    EXPECT_FALSE(stats->getRawSize().has_value());
    EXPECT_TRUE(stats->hasNull().value());
  }

  void testBasicHasNull(
      ColumnStatisticsWrapper columnStatisticsWrapper,
      DwrfFormat format) {
    enum class State { kTrue = 0, kFalse, kMissing };
    auto test = [&](State to, State from, State expected) {
      StatisticsBuilder target{options()};
      if (to == State::kTrue) {
        target.setHasNull();
      } else if (to == State::kMissing) {
        // merge against unknown
        target.merge(*buildColumnStatisticsFromProto(
            columnStatisticsWrapper, context()));
      }

      if (format == DwrfFormat::kDwrf) {
        auto columnStatistics =
            google::protobuf::Arena::CreateMessage<proto::ColumnStatistics>(
                arena_.get());
        if (from == State::kFalse) {
          columnStatistics->set_hasnull(false);
        } else if (from == State::kTrue) {
          columnStatistics->set_hasnull(true);
        }
        target.merge(*buildColumnStatisticsFromProto(
            ColumnStatisticsWrapper(columnStatistics), context()));
      } else {
        auto columnStatistics = google::protobuf::Arena::CreateMessage<
            proto::orc::ColumnStatistics>(arena_.get());
        if (from == State::kFalse) {
          columnStatistics->set_hasnull(false);
        } else if (from == State::kTrue) {
          columnStatistics->set_hasnull(true);
        }
        target.merge(*buildColumnStatisticsFromProto(
            ColumnStatisticsWrapper(columnStatistics), context()));
      }
      auto stats = target.build();
      if (expected == State::kFalse) {
        EXPECT_FALSE(stats->hasNull().value());
      } else if (expected == State::kTrue) {
        EXPECT_TRUE(stats->hasNull().value());
      } else {
        EXPECT_FALSE(stats->hasNull().has_value());
      }
    };

    // true / any => true
    test(State::kTrue, State::kTrue, State::kTrue);
    test(State::kTrue, State::kFalse, State::kTrue);
    test(State::kTrue, State::kMissing, State::kTrue);
    // unknown / true => true
    // unknown / unknown or false => unknown
    test(State::kMissing, State::kTrue, State::kTrue);
    test(State::kMissing, State::kFalse, State::kMissing);
    test(State::kMissing, State::kMissing, State::kMissing);
    // false / unknown => unknown
    // false / false => false
    // false / true => true
    test(State::kFalse, State::kMissing, State::kMissing);
    test(State::kFalse, State::kFalse, State::kFalse);
    test(State::kFalse, State::kTrue, State::kTrue);
  }

  void testBinary() {
    BinaryStatisticsBuilder builder{options()};
    // empty builder should have all defaults
    EXPECT_EQ(0, builder.getTotalLength());
    EXPECT_EQ(0, builder.getNumberOfValues());

    builder.addValues(5, 2);

    // stats should be merged
    BinaryStatisticsBuilder target{options()};
    target.merge(*builder.build());
    auto stats = as<dwio::common::BinaryColumnStatistics>(target.build());
    EXPECT_EQ(2, stats->getNumberOfValues());
    EXPECT_EQ(10, stats->getTotalLength());

    // stats should be merged again
    target.merge(*builder.build());
    stats = as<dwio::common::BinaryColumnStatistics>(target.build());
    EXPECT_EQ(4, stats->getNumberOfValues());
    EXPECT_EQ(20, stats->getTotalLength());

    // add value
    target.addValues(10);
    stats = as<dwio::common::BinaryColumnStatistics>(target.build());
    EXPECT_EQ(5, stats->getNumberOfValues());
    EXPECT_EQ(30, stats->getTotalLength());

    // reset
    builder.reset();
    EXPECT_EQ(0, builder.getTotalLength());
    EXPECT_EQ(0, builder.getNumberOfValues());
  }

  void testBinaryMissingStats(
      ColumnStatisticsWrapper columnStatisticsWrapper,
      void* protoPtr,
      DwrfFormat format) {
    BinaryStatisticsBuilder target{options()};
    target.addValues(5);
    auto stats = as<dwio::common::BinaryColumnStatistics>(target.build());
    EXPECT_EQ(5, stats->getTotalLength());

    // merge missing stats
    target.merge(
        *buildColumnStatisticsFromProto(columnStatisticsWrapper, context()));
    stats = as<dwio::common::BinaryColumnStatistics>(target.build());
    EXPECT_EQ(stats, nullptr);

    // merge again
    if (format == DwrfFormat::kDwrf) {
      auto binProto = reinterpret_cast<proto::BinaryStatistics*>(protoPtr);
      binProto->set_sum(100);
    } else {
      auto binProto = reinterpret_cast<proto::orc::BinaryStatistics*>(protoPtr);
      binProto->set_sum(100);
    }

    target.merge(
        *buildColumnStatisticsFromProto(columnStatisticsWrapper, context()));
    stats = as<dwio::common::BinaryColumnStatistics>(target.build());
    EXPECT_EQ(stats, nullptr);

    // add again
    target.addValues(10);
    stats = as<dwio::common::BinaryColumnStatistics>(target.build());
    EXPECT_EQ(stats, nullptr);
  }

  void testBinaryEmptyStats(
      ColumnStatisticsWrapper columnStatisticsWrapper,
      void* protoPtr,
      DwrfFormat format) {
    BinaryStatisticsBuilder target{options()};
    target.addValues(5);
    auto stats = as<dwio::common::BinaryColumnStatistics>(target.build());
    EXPECT_EQ(5, stats->getTotalLength());

    // merge empty stats
    target.merge(
        *buildColumnStatisticsFromProto(columnStatisticsWrapper, context()));
    stats = as<dwio::common::BinaryColumnStatistics>(target.build());
    EXPECT_EQ(5, stats->getTotalLength());

    // merge again
    if (format == DwrfFormat::kDwrf) {
      auto proto = reinterpret_cast<proto::ColumnStatistics*>(protoPtr);
      proto->clear_numberofvalues();
    } else {
      auto proto = reinterpret_cast<proto::orc::ColumnStatistics*>(protoPtr);
      proto->clear_numberofvalues();
    }

    target.merge(
        *buildColumnStatisticsFromProto(columnStatisticsWrapper, context()));
    stats = as<dwio::common::BinaryColumnStatistics>(target.build());
    EXPECT_EQ(stats, nullptr);
  }

  void testBinaryLengthOverflow(
      ColumnStatisticsWrapper columnStatisticsWrapper,
      void* protoPtr,
      DwrfFormat format) {
    // add value causing overflow
    BinaryStatisticsBuilder target{options()};
    target.addValues(std::numeric_limits<int64_t>::max());
    auto stats = as<dwio::common::BinaryColumnStatistics>(target.build());
    EXPECT_NE(stats, nullptr);
    target.addValues(1);
    EXPECT_TRUE(target.getTotalLength().has_value());
    stats = as<dwio::common::BinaryColumnStatistics>(target.build());
    EXPECT_EQ(stats, nullptr);

    // merge causing overflow
    target.reset();
    target.addValues(std::numeric_limits<int64_t>::max());

    if (format == DwrfFormat::kDwrf) {
      auto binProto = reinterpret_cast<proto::BinaryStatistics*>(protoPtr);
      binProto->set_sum(1);
    } else {
      auto binProto = reinterpret_cast<proto::orc::BinaryStatistics*>(protoPtr);
      binProto->set_sum(1);
    }

    target.merge(
        *buildColumnStatisticsFromProto(columnStatisticsWrapper, context()));
    EXPECT_TRUE(target.getTotalLength().has_value());
    stats = as<dwio::common::BinaryColumnStatistics>(target.build());
    EXPECT_EQ(stats, nullptr);
  }

  void testInitialSize() {
    StatisticsBuilder target{options()};
    target.increaseValueCount(1);
    EXPECT_FALSE(target.getSize().has_value());
    auto stats = target.build();
    EXPECT_FALSE(stats->getSize().has_value());

    StatisticsBuilder target2{StatisticsBuilderOptions{16, 100U}};
    target2.increaseValueCount(1);
    EXPECT_EQ(target2.getSize().value(), 100);
    stats = target2.build();
    EXPECT_EQ(stats->getSize().value(), 100);
    target2.reset();
    EXPECT_EQ(target2.getSize().value(), 100);
    stats = target2.build();
    EXPECT_EQ(stats->getSize().value(), 100);
  }

 protected:
  StatisticsBuilderOptions options() {
    StatisticsBuilderOptions options{16};
    return options;
  }

  facebook::velox::dwrf::StatsContext context() {
    facebook::velox::dwrf::StatsContext context{WriterVersion_CURRENT};
    return context;
  }

  template <typename T, typename U>
  std::unique_ptr<T> as(std::unique_ptr<U>&& ptr) {
    auto p = ptr.release();
    if (auto cp = dynamic_cast<T*>(p)) {
      return std::unique_ptr<T>(cp);
    }
    delete p;
    return nullptr;
  }

 private:
  std::unique_ptr<google::protobuf::Arena> arena_;
};
} // namespace facebook::velox::dwrf

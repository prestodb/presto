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
#include <cmath>
#include "velox/dwio/dwrf/writer/StatisticsBuilder.h"

using namespace facebook::dwio::common;
using namespace facebook::velox::dwrf;

StatisticsBuilderOptions options{16};

StatsContext context{WriterVersion_CURRENT};
template <typename T, typename U>
std::unique_ptr<T> as(std::unique_ptr<U>&& ptr) {
  auto p = ptr.release();
  if (auto cp = dynamic_cast<T*>(p)) {
    return std::unique_ptr<T>(cp);
  }
  delete p;
  return nullptr;
}

TEST(StatisticsBuilder, integer) {
  IntegerStatisticsBuilder builder{options};
  // empty builder should have all defaults
  EXPECT_EQ(std::numeric_limits<int64_t>::max(), builder.getMinimum());
  EXPECT_EQ(std::numeric_limits<int64_t>::min(), builder.getMaximum());
  EXPECT_EQ(0, builder.getSum());
  EXPECT_EQ(0, builder.getNumberOfValues());

  builder.addValues(3);
  builder.addValues(1);
  builder.addValues(5);

  // stats should be merged
  IntegerStatisticsBuilder target{options};
  target.merge(*builder.build());
  auto stats = as<IntegerColumnStatistics>(target.build());
  EXPECT_EQ(3, stats->getNumberOfValues());
  EXPECT_EQ(1, stats->getMinimum());
  EXPECT_EQ(5, stats->getMaximum());
  EXPECT_EQ(9, stats->getSum());

  // stats should be merged again
  builder.addValues(0);
  builder.addValues(6);
  target.merge(*builder.build());
  stats = as<IntegerColumnStatistics>(target.build());
  EXPECT_EQ(8, stats->getNumberOfValues());
  EXPECT_EQ(0, stats->getMinimum());
  EXPECT_EQ(6, stats->getMaximum());
  EXPECT_EQ(24, stats->getSum());

  target.merge(*builder.build());
  stats = as<IntegerColumnStatistics>(target.build());
  EXPECT_EQ(13, stats->getNumberOfValues());
  EXPECT_EQ(0, stats->getMinimum());
  EXPECT_EQ(6, stats->getMaximum());
  EXPECT_EQ(39, stats->getSum());

  // add value
  target.addValues(100, 2);
  stats = as<IntegerColumnStatistics>(target.build());
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

TEST(StatisticsBuilder, integerMissingStats) {
  IntegerStatisticsBuilder target{options};
  target.addValues(1, 5);
  auto stats = as<IntegerColumnStatistics>(target.build());
  EXPECT_EQ(5, stats->getSum());

  // merge missing stats
  proto::ColumnStatistics proto;
  auto intProto = proto.mutable_intstatistics();
  target.merge(*ColumnStatistics::fromProto(proto, context));
  stats = as<IntegerColumnStatistics>(target.build());
  EXPECT_EQ(stats, nullptr);

  // merge again
  intProto->set_minimum(0);
  intProto->set_maximum(1);
  intProto->set_sum(100);
  target.merge(*ColumnStatistics::fromProto(proto, context));
  stats = as<IntegerColumnStatistics>(target.build());
  EXPECT_EQ(stats, nullptr);

  // add again
  target.addValues(2);
  stats = as<IntegerColumnStatistics>(target.build());
  EXPECT_EQ(stats, nullptr);
}

TEST(StatisticsBuilder, integerEmptyStats) {
  IntegerStatisticsBuilder target{options};
  target.addValues(1, 5);
  auto stats = as<IntegerColumnStatistics>(target.build());
  EXPECT_EQ(5, stats->getSum());

  // merge empty stats
  proto::ColumnStatistics proto;
  proto.set_numberofvalues(0);
  target.merge(*ColumnStatistics::fromProto(proto, context));
  stats = as<IntegerColumnStatistics>(target.build());
  EXPECT_EQ(5, stats->getSum());

  // merge again
  proto.clear_numberofvalues();
  target.merge(*ColumnStatistics::fromProto(proto, context));
  stats = as<IntegerColumnStatistics>(target.build());
  EXPECT_EQ(stats, nullptr);
}

TEST(StatisticsBuilder, integerOverflow) {
  IntegerStatisticsBuilder target{options};
  auto testMinMax = [&](int64_t val1, int64_t val2, int64_t min, int64_t max) {
    target.reset();
    target.addValues(val1);
    auto stats = as<IntegerColumnStatistics>(target.build());
    EXPECT_EQ(val1, stats->getMaximum());
    EXPECT_EQ(val1, stats->getMinimum());
    EXPECT_EQ(val1, stats->getSum());

    target.addValues(val2);
    stats = as<IntegerColumnStatistics>(target.build());
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

  // make sure we also capture overflow that happened for adding multiple items
  target.reset();
  target.addValues(std::numeric_limits<int64_t>::max() / 10, 11);
  auto stats = as<IntegerColumnStatistics>(target.build());
  EXPECT_EQ(11, stats->getNumberOfValues());
  EXPECT_EQ(stats->getMaximum().value(), stats->getMinimum().value());
  EXPECT_FALSE(stats->getSum().has_value());

  // merge overflow
  auto testMergeOverflow = [&](int64_t val1, int64_t val2) {
    target.reset();
    target.addValues(val1);
    IntegerStatisticsBuilder builder{options};
    builder.addValues(val2);
    target.merge(builder);
    stats = as<IntegerColumnStatistics>(target.build());
    EXPECT_FALSE(stats->getSum().has_value());
  };
  testMergeOverflow(std::numeric_limits<int64_t>::min(), -1);
  testMergeOverflow(std::numeric_limits<int64_t>::max(), 1);
}

TEST(StatisticsBuilder, doubles) {
  DoubleStatisticsBuilder builder{options};
  // empty builder should have all defaults
  EXPECT_EQ(std::numeric_limits<double>::infinity(), builder.getMinimum());
  EXPECT_EQ(-std::numeric_limits<double>::infinity(), builder.getMaximum());
  EXPECT_EQ(0, builder.getSum());
  EXPECT_EQ(0, builder.getNumberOfValues());

  builder.addValues(3);
  builder.addValues(1);
  builder.addValues(5);

  // stats should be merged
  DoubleStatisticsBuilder target{options};
  target.merge(*builder.build());
  auto stats = as<DoubleColumnStatistics>(target.build());
  EXPECT_EQ(3, stats->getNumberOfValues());
  EXPECT_EQ(1, stats->getMinimum());
  EXPECT_EQ(5, stats->getMaximum());
  EXPECT_EQ(9, stats->getSum());

  // stats should be merged again
  builder.addValues(0);
  builder.addValues(6);
  target.merge(*builder.build());
  stats = as<DoubleColumnStatistics>(target.build());
  EXPECT_EQ(8, stats->getNumberOfValues());
  EXPECT_EQ(0, stats->getMinimum());
  EXPECT_EQ(6, stats->getMaximum());
  EXPECT_EQ(24, stats->getSum());

  target.merge(*builder.build());
  stats = as<DoubleColumnStatistics>(target.build());
  EXPECT_EQ(13, stats->getNumberOfValues());
  EXPECT_EQ(0, stats->getMinimum());
  EXPECT_EQ(6, stats->getMaximum());
  EXPECT_EQ(39, stats->getSum());

  // add value
  target.addValues(100, 2);
  stats = as<DoubleColumnStatistics>(target.build());
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

TEST(StatisticsBuilder, doubleMissingStats) {
  DoubleStatisticsBuilder target{options};
  target.addValues(1, 5);
  auto stats = as<DoubleColumnStatistics>(target.build());
  EXPECT_EQ(5, stats->getSum());

  // merge missing stats
  proto::ColumnStatistics proto;
  auto doubleProto = proto.mutable_doublestatistics();
  target.merge(*ColumnStatistics::fromProto(proto, context));
  stats = as<DoubleColumnStatistics>(target.build());
  EXPECT_EQ(stats, nullptr);

  // merge again
  doubleProto->set_minimum(0);
  doubleProto->set_maximum(1);
  doubleProto->set_sum(100);
  target.merge(*ColumnStatistics::fromProto(proto, context));
  stats = as<DoubleColumnStatistics>(target.build());
  EXPECT_EQ(stats, nullptr);

  // add again
  target.addValues(2);
  stats = as<DoubleColumnStatistics>(target.build());
  EXPECT_EQ(stats, nullptr);
}

TEST(StatisticsBuilder, doubleEmptyStats) {
  DoubleStatisticsBuilder target{options};
  target.addValues(1, 5);
  auto stats = as<DoubleColumnStatistics>(target.build());
  EXPECT_EQ(5, stats->getSum());

  // merge empty stats
  proto::ColumnStatistics proto;
  proto.set_numberofvalues(0);
  target.merge(*ColumnStatistics::fromProto(proto, context));
  stats = as<DoubleColumnStatistics>(target.build());
  EXPECT_EQ(5, stats->getSum());

  // merge again
  proto.clear_numberofvalues();
  target.merge(*ColumnStatistics::fromProto(proto, context));
  stats = as<DoubleColumnStatistics>(target.build());
  EXPECT_EQ(stats, nullptr);
}

TEST(StatisticsBuilder, doubleNaN) {
  DoubleStatisticsBuilder target{options};
  // test nan. Nan causes fallback to basic stats.
  target.addValues(std::numeric_limits<float>::quiet_NaN());
  auto stats = as<DoubleColumnStatistics>(target.build());
  EXPECT_EQ(stats, nullptr);

  target.reset();
  target.addValues(std::numeric_limits<double>::infinity());
  target.addValues(-std::numeric_limits<double>::infinity());
  stats = as<DoubleColumnStatistics>(target.build());
  EXPECT_EQ(stats->getMaximum(), std::numeric_limits<double>::infinity());
  EXPECT_EQ(stats->getMinimum(), -std::numeric_limits<double>::infinity());
  EXPECT_FALSE(stats->getSum().has_value());

  target.reset();
  DoubleStatisticsBuilder builder{options};
  target.addValues(std::numeric_limits<double>::infinity());
  builder.addValues(-std::numeric_limits<double>::infinity());
  target.merge(*builder.build());
  stats = as<DoubleColumnStatistics>(target.build());
  EXPECT_EQ(stats->getMaximum(), std::numeric_limits<double>::infinity());
  EXPECT_EQ(stats->getMinimum(), -std::numeric_limits<double>::infinity());
  EXPECT_FALSE(stats->getSum().has_value());
}

TEST(StatisticsBuilder, string) {
  StringStatisticsBuilder builder{options};
  // empty builder should have all defaults
  EXPECT_FALSE(builder.getMinimum().has_value());
  EXPECT_FALSE(builder.getMaximum().has_value());
  EXPECT_EQ(0, builder.getTotalLength());
  EXPECT_EQ(0, builder.getNumberOfValues());

  builder.addValues("xx");
  builder.addValues("bb");
  builder.addValues("yy");

  // stats should be merged
  StringStatisticsBuilder target{options};
  target.merge(*builder.build());
  auto stats = as<StringColumnStatistics>(target.build());
  EXPECT_EQ(3, stats->getNumberOfValues());
  EXPECT_EQ("bb", stats->getMinimum());
  EXPECT_EQ("yy", stats->getMaximum());
  EXPECT_EQ(6, stats->getTotalLength());

  // stats should be merged again
  builder.addValues("aa");
  builder.addValues("zz");
  target.merge(*builder.build());
  stats = as<StringColumnStatistics>(target.build());
  EXPECT_EQ(8, stats->getNumberOfValues());
  EXPECT_EQ("aa", stats->getMinimum());
  EXPECT_EQ("zz", stats->getMaximum());
  EXPECT_EQ(16, stats->getTotalLength());

  target.merge(*builder.build());
  stats = as<StringColumnStatistics>(target.build());
  EXPECT_EQ(13, stats->getNumberOfValues());
  EXPECT_EQ("aa", stats->getMinimum());
  EXPECT_EQ("zz", stats->getMaximum());
  EXPECT_EQ(26, stats->getTotalLength());

  // add value
  target.addValues("zzz", 2);
  stats = as<StringColumnStatistics>(target.build());
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

TEST(StatisticsBuilder, stringMissingStats) {
  StringStatisticsBuilder target{options};
  target.addValues("zz", 5);
  auto stats = as<StringColumnStatistics>(target.build());
  EXPECT_EQ(10, stats->getTotalLength());

  // merge missing stats
  proto::ColumnStatistics proto;
  auto strProto = proto.mutable_stringstatistics();
  target.merge(*ColumnStatistics::fromProto(proto, context));
  stats = as<StringColumnStatistics>(target.build());
  EXPECT_EQ(stats, nullptr);

  // merge again
  strProto->set_minimum("aa");
  strProto->set_maximum("bb");
  strProto->set_sum(100);
  target.merge(*ColumnStatistics::fromProto(proto, context));
  stats = as<StringColumnStatistics>(target.build());
  EXPECT_EQ(stats, nullptr);

  // add again
  target.addValues("aa");
  stats = as<StringColumnStatistics>(target.build());
  EXPECT_EQ(stats, nullptr);
}

TEST(StatisticsBuilder, stringEmptyStats) {
  StringStatisticsBuilder target{options};
  target.addValues("zz", 5);
  auto stats = as<StringColumnStatistics>(target.build());
  EXPECT_EQ(10, stats->getTotalLength());

  // merge empty stats
  proto::ColumnStatistics proto;
  proto.set_numberofvalues(0);
  target.merge(*ColumnStatistics::fromProto(proto, context));
  stats = as<StringColumnStatistics>(target.build());
  EXPECT_EQ(10, stats->getTotalLength());

  // merge again
  proto.clear_numberofvalues();
  target.merge(*ColumnStatistics::fromProto(proto, context));
  stats = as<StringColumnStatistics>(target.build());
  EXPECT_EQ(stats, nullptr);
}

TEST(StatisticsBuilder, stringLengthThreshold) {
  StringStatisticsBuilder target{StatisticsBuilderOptions{2}};
  target.addValues("yyy");
  auto stats = as<StringColumnStatistics>(target.build());
  EXPECT_FALSE(stats->getMinimum().has_value());
  EXPECT_FALSE(stats->getMaximum().has_value());

  // merge empty stats
  target.addValues("aa");
  target.addValues("zz");
  stats = as<StringColumnStatistics>(target.build());
  EXPECT_EQ(stats->getMinimum(), "aa");
  EXPECT_EQ(stats->getMaximum(), "zz");
}

TEST(StatisticsBuilder, stringLengthOverflow) {
  // add value causing overflow
  StringStatisticsBuilder target{options};
  proto::ColumnStatistics proto;
  proto.set_numberofvalues(1);
  auto strProto = proto.mutable_stringstatistics();
  strProto->set_sum(std::numeric_limits<int64_t>::max());
  strProto->set_minimum("foo");
  target.merge(*ColumnStatistics::fromProto(proto, context));
  EXPECT_TRUE(target.getTotalLength().has_value());
  auto stats = as<StringColumnStatistics>(target.build());
  EXPECT_TRUE(stats->getTotalLength().has_value());

  target.addValues("foo");
  EXPECT_TRUE(target.getTotalLength().has_value());
  stats = as<StringColumnStatistics>(target.build());
  EXPECT_FALSE(stats->getTotalLength().has_value());

  // merge causing overflow
  target.reset();
  target.merge(*ColumnStatistics::fromProto(proto, context));
  target.merge(*ColumnStatistics::fromProto(proto, context));
  EXPECT_TRUE(target.getTotalLength().has_value());
  stats = as<StringColumnStatistics>(target.build());
  EXPECT_FALSE(stats->getTotalLength().has_value());
}

TEST(StatisticsBuilder, boolean) {
  BooleanStatisticsBuilder builder{options};
  // empty builder should have all defaults
  EXPECT_EQ(0, builder.getTrueCount());
  EXPECT_EQ(0, builder.getNumberOfValues());

  builder.addValues(true, 2);

  // stats should be merged
  BooleanStatisticsBuilder target{options};
  target.merge(*builder.build());
  auto stats = as<BooleanColumnStatistics>(target.build());
  EXPECT_EQ(2, stats->getNumberOfValues());
  EXPECT_EQ(2, stats->getTrueCount());

  // stats should be merged again
  target.merge(*builder.build());
  stats = as<BooleanColumnStatistics>(target.build());
  EXPECT_EQ(4, stats->getNumberOfValues());
  EXPECT_EQ(4, stats->getTrueCount());

  // add value
  target.addValues(false, 2);
  stats = as<BooleanColumnStatistics>(target.build());
  EXPECT_EQ(6, stats->getNumberOfValues());
  EXPECT_EQ(4, stats->getTrueCount());

  // reset
  builder.reset();
  EXPECT_EQ(0, builder.getTrueCount());
  EXPECT_EQ(0, builder.getNumberOfValues());
}

TEST(StatisticsBuilder, booleanMissingStats) {
  BooleanStatisticsBuilder target{options};
  target.addValues(true, 5);
  auto stats = as<BooleanColumnStatistics>(target.build());
  EXPECT_EQ(5, stats->getTrueCount());

  // merge missing stats
  proto::ColumnStatistics proto;
  auto boolProto = proto.mutable_bucketstatistics();
  target.merge(*ColumnStatistics::fromProto(proto, context));
  stats = as<BooleanColumnStatistics>(target.build());
  EXPECT_EQ(stats, nullptr);

  // merge again
  boolProto->add_count(1);
  target.merge(*ColumnStatistics::fromProto(proto, context));
  stats = as<BooleanColumnStatistics>(target.build());
  EXPECT_EQ(stats, nullptr);

  // add again
  target.addValues(true);
  stats = as<BooleanColumnStatistics>(target.build());
  EXPECT_EQ(stats, nullptr);
}

TEST(StatisticsBuilder, booleanEmptyStats) {
  BooleanStatisticsBuilder target{options};
  target.addValues(true, 5);
  auto stats = as<BooleanColumnStatistics>(target.build());
  EXPECT_EQ(5, stats->getTrueCount());

  // merge empty stats
  proto::ColumnStatistics proto;
  proto.set_numberofvalues(0);
  target.merge(*ColumnStatistics::fromProto(proto, context));
  stats = as<BooleanColumnStatistics>(target.build());
  EXPECT_EQ(5, stats->getTrueCount());

  // merge again
  proto.clear_numberofvalues();
  target.merge(*ColumnStatistics::fromProto(proto, context));
  stats = as<BooleanColumnStatistics>(target.build());
  EXPECT_EQ(stats, nullptr);
}

TEST(StatisticsBuilder, basic) {
  StatisticsBuilder builder{options};
  EXPECT_EQ(0, builder.getNumberOfValues());
  EXPECT_EQ(0, builder.getRawSize());
  EXPECT_FALSE(builder.hasNull().value());

  builder.increaseValueCount(5);
  builder.increaseRawSize(10);
  builder.setHasNull();

  // stats should be merged
  StatisticsBuilder target{options};
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

TEST(StatisticsBuilder, basicMissingStats) {
  StatisticsBuilder target{options};
  target.increaseValueCount(5);
  target.increaseRawSize(10);
  auto stats = target.build();
  EXPECT_EQ(5, stats->getNumberOfValues());
  EXPECT_EQ(10, stats->getRawSize());
  EXPECT_FALSE(stats->hasNull().value());

  // merge missing stats
  proto::ColumnStatistics proto;
  target.merge(*ColumnStatistics::fromProto(proto, context));
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

TEST(StatisticsBuilder, basicHasNull) {
  enum State { TRUE = 0, FALSE, MISSING };
  auto test = [](State to, State from, State expected) {
    StatisticsBuilder target{options};
    if (to == State::TRUE) {
      target.setHasNull();
    } else if (to == State::MISSING) {
      // merge against unknown
      proto::ColumnStatistics proto;
      target.merge(*ColumnStatistics::fromProto(proto, context));
    }

    proto::ColumnStatistics proto;
    if (from == State::FALSE) {
      proto.set_hasnull(false);
    } else if (from == State::TRUE) {
      proto.set_hasnull(true);
    }

    target.merge(*ColumnStatistics::fromProto(proto, context));
    auto stats = target.build();
    if (expected == State::FALSE) {
      EXPECT_FALSE(stats->hasNull().value());
    } else if (expected == State::TRUE) {
      EXPECT_TRUE(stats->hasNull().value());
    } else {
      EXPECT_FALSE(stats->hasNull().has_value());
    }
  };

  // true / any => true
  test(State::TRUE, State::TRUE, State::TRUE);
  test(State::TRUE, State::FALSE, State::TRUE);
  test(State::TRUE, State::MISSING, State::TRUE);
  // unknown / true => true
  // unknown / unknown or false => unknown
  test(State::MISSING, State::TRUE, State::TRUE);
  test(State::MISSING, State::FALSE, State::MISSING);
  test(State::MISSING, State::MISSING, State::MISSING);
  // false / unknown => unknown
  // false / false => false
  // false / true => true
  test(State::FALSE, State::MISSING, State::MISSING);
  test(State::FALSE, State::FALSE, State::FALSE);
  test(State::FALSE, State::TRUE, State::TRUE);
}

TEST(StatisticsBuilder, binary) {
  BinaryStatisticsBuilder builder{options};
  // empty builder should have all defaults
  EXPECT_EQ(0, builder.getTotalLength());
  EXPECT_EQ(0, builder.getNumberOfValues());

  builder.addValues(5, 2);

  // stats should be merged
  BinaryStatisticsBuilder target{options};
  target.merge(*builder.build());
  auto stats = as<BinaryColumnStatistics>(target.build());
  EXPECT_EQ(2, stats->getNumberOfValues());
  EXPECT_EQ(10, stats->getTotalLength());

  // stats should be merged again
  target.merge(*builder.build());
  stats = as<BinaryColumnStatistics>(target.build());
  EXPECT_EQ(4, stats->getNumberOfValues());
  EXPECT_EQ(20, stats->getTotalLength());

  // add value
  target.addValues(10);
  stats = as<BinaryColumnStatistics>(target.build());
  EXPECT_EQ(5, stats->getNumberOfValues());
  EXPECT_EQ(30, stats->getTotalLength());

  // reset
  builder.reset();
  EXPECT_EQ(0, builder.getTotalLength());
  EXPECT_EQ(0, builder.getNumberOfValues());
}

TEST(StatisticsBuilder, binaryMissingStats) {
  BinaryStatisticsBuilder target{options};
  target.addValues(5);
  auto stats = as<BinaryColumnStatistics>(target.build());
  EXPECT_EQ(5, stats->getTotalLength());

  // merge missing stats
  proto::ColumnStatistics proto;
  auto binProto = proto.mutable_binarystatistics();
  target.merge(*ColumnStatistics::fromProto(proto, context));
  stats = as<BinaryColumnStatistics>(target.build());
  EXPECT_EQ(stats, nullptr);

  // merge again
  binProto->set_sum(100);
  target.merge(*ColumnStatistics::fromProto(proto, context));
  stats = as<BinaryColumnStatistics>(target.build());
  EXPECT_EQ(stats, nullptr);

  // add again
  target.addValues(10);
  stats = as<BinaryColumnStatistics>(target.build());
  EXPECT_EQ(stats, nullptr);
}

TEST(StatisticsBuilder, binaryEmptyStats) {
  BinaryStatisticsBuilder target{options};
  target.addValues(5);
  auto stats = as<BinaryColumnStatistics>(target.build());
  EXPECT_EQ(5, stats->getTotalLength());

  // merge empty stats
  proto::ColumnStatistics proto;
  proto.set_numberofvalues(0);
  target.merge(*ColumnStatistics::fromProto(proto, context));
  stats = as<BinaryColumnStatistics>(target.build());
  EXPECT_EQ(5, stats->getTotalLength());

  // merge again
  proto.clear_numberofvalues();
  target.merge(*ColumnStatistics::fromProto(proto, context));
  stats = as<BinaryColumnStatistics>(target.build());
  EXPECT_EQ(stats, nullptr);
}

TEST(StatisticsBuilder, binaryLengthOverflow) {
  // add value causing overflow
  BinaryStatisticsBuilder target{options};
  target.addValues(std::numeric_limits<int64_t>::max());
  auto stats = as<BinaryColumnStatistics>(target.build());
  EXPECT_NE(stats, nullptr);
  target.addValues(1);
  EXPECT_TRUE(target.getTotalLength().has_value());
  stats = as<BinaryColumnStatistics>(target.build());
  EXPECT_EQ(stats, nullptr);

  // merge causing overflow
  target.reset();
  target.addValues(std::numeric_limits<int64_t>::max());
  proto::ColumnStatistics proto;
  auto binProto = proto.mutable_binarystatistics();
  binProto->set_sum(1);
  target.merge(*ColumnStatistics::fromProto(proto, context));
  EXPECT_TRUE(target.getTotalLength().has_value());
  stats = as<BinaryColumnStatistics>(target.build());
  EXPECT_EQ(stats, nullptr);
}

TEST(StatisticsBuilder, initialSize) {
  StatisticsBuilder target{options};
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

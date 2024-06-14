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

#include "velox/dwio/dwrf/test/ColumnStatisticsBase.h"

using namespace facebook::velox::dwrf;

class ColumnStatisticsTest : public ::testing::Test,
                             public ColumnStatisticsBase {};

TEST_F(ColumnStatisticsTest, size) {
  testSize();
}

TEST_F(ColumnStatisticsTest, integer) {
  testInteger();
}

TEST_F(ColumnStatisticsTest, integerMissingStats) {
  proto::orc::ColumnStatistics proto;
  auto columnStatisticsWrapper = ColumnStatisticsWrapper(&proto);
  auto intProto = proto.mutable_intstatistics();
  testIntegerMissingStats(columnStatisticsWrapper, intProto, DwrfFormat::kOrc);
}

TEST_F(ColumnStatisticsTest, integerEmptyStats) {
  proto::orc::ColumnStatistics proto;
  proto.set_numberofvalues(0);
  auto columnStatisticsWrapper = ColumnStatisticsWrapper(&proto);
  testIntegerEmptyStats(
      columnStatisticsWrapper, (void*)&proto, DwrfFormat::kOrc);
}

TEST_F(ColumnStatisticsTest, integerOverflow) {
  testIntegerOverflow();
}

TEST_F(ColumnStatisticsTest, doubles) {
  testDoubles();
}

TEST_F(ColumnStatisticsTest, doubleMissingStats) {
  proto::orc::ColumnStatistics proto;
  auto columnStatisticsWrapper = ColumnStatisticsWrapper(&proto);
  auto doubleProto = proto.mutable_doublestatistics();
  testDoubleMissingStats(
      columnStatisticsWrapper, doubleProto, DwrfFormat::kOrc);
}

TEST_F(ColumnStatisticsTest, doubleEmptyStats) {
  proto::orc::ColumnStatistics proto;
  proto.set_numberofvalues(0);
  auto columnStatisticsWrapper = ColumnStatisticsWrapper(&proto);
  testDoubleEmptyStats(
      columnStatisticsWrapper, (void*)&proto, DwrfFormat::kOrc);
}

TEST_F(ColumnStatisticsTest, doubleNaN) {
  testDoubleNaN();
}

TEST_F(ColumnStatisticsTest, string) {
  testString();
}

TEST_F(ColumnStatisticsTest, stringMissingStats) {
  proto::orc::ColumnStatistics proto;
  auto columnStatisticsWrapper = ColumnStatisticsWrapper(&proto);
  auto strProto = proto.mutable_stringstatistics();
  testStringMissingStats(columnStatisticsWrapper, strProto, DwrfFormat::kOrc);
}

TEST_F(ColumnStatisticsTest, stringEmptyStats) {
  proto::orc::ColumnStatistics proto;
  proto.set_numberofvalues(0);
  auto columnStatisticsWrapper = ColumnStatisticsWrapper(&proto);
  testStringEmptyStats(
      columnStatisticsWrapper, (void*)&proto, DwrfFormat::kOrc);
}

TEST_F(ColumnStatisticsTest, stringLengthThreshold) {
  testStringLengthThreshold();
}

TEST_F(ColumnStatisticsTest, stringLengthOverflow) {
  proto::orc::ColumnStatistics proto;
  proto.set_numberofvalues(1);
  auto strProto = proto.mutable_stringstatistics();
  auto columnStatisticsWrapper = ColumnStatisticsWrapper(&proto);
  testStringLengthOverflow(columnStatisticsWrapper, strProto, DwrfFormat::kOrc);
}

TEST_F(ColumnStatisticsTest, boolean) {
  testBoolean();
}

TEST_F(ColumnStatisticsTest, booleanMissingStats) {
  proto::orc::ColumnStatistics proto;
  auto boolProto = proto.mutable_bucketstatistics();
  auto columnStatisticsWrapper = ColumnStatisticsWrapper(&proto);
  testBooleanMissingStats(columnStatisticsWrapper, boolProto, DwrfFormat::kOrc);
}

TEST_F(ColumnStatisticsTest, booleanEmptyStats) {
  proto::orc::ColumnStatistics proto;
  proto.set_numberofvalues(0);
  auto columnStatisticsWrapper = ColumnStatisticsWrapper(&proto);
  testBooleanEmptyStats(
      columnStatisticsWrapper, (void*)&proto, DwrfFormat::kOrc);
}

TEST_F(ColumnStatisticsTest, basic) {
  testBasic();
}

TEST_F(ColumnStatisticsTest, basicMissingStats) {
  proto::orc::ColumnStatistics proto;
  auto columnStatisticsWrapper = ColumnStatisticsWrapper(&proto);
  testBasicMissingStats(columnStatisticsWrapper);
}

TEST_F(ColumnStatisticsTest, basicHasNull) {
  proto::orc::ColumnStatistics proto;
  auto columnStatisticsWrapper = ColumnStatisticsWrapper(&proto);
  testBasicHasNull(columnStatisticsWrapper, DwrfFormat::kOrc);
}

TEST_F(ColumnStatisticsTest, binary) {
  testBinary();
}

TEST_F(ColumnStatisticsTest, binaryMissingStats) {
  proto::orc::ColumnStatistics proto;
  auto binProto = proto.mutable_binarystatistics();
  auto columnStatisticsWrapper = ColumnStatisticsWrapper(&proto);
  testBinaryMissingStats(columnStatisticsWrapper, binProto, DwrfFormat::kOrc);
}

TEST_F(ColumnStatisticsTest, binaryEmptyStats) {
  proto::orc::ColumnStatistics proto;
  proto.set_numberofvalues(0);
  auto columnStatisticsWrapper = ColumnStatisticsWrapper(&proto);
  testBinaryEmptyStats(
      columnStatisticsWrapper, (void*)&proto, DwrfFormat::kOrc);
}

TEST_F(ColumnStatisticsTest, binaryLengthOverflow) {
  proto::orc::ColumnStatistics proto;
  auto binProto = proto.mutable_binarystatistics();
  auto columnStatisticsWrapper = ColumnStatisticsWrapper(&proto);
  testBinaryLengthOverflow(columnStatisticsWrapper, binProto, DwrfFormat::kOrc);
}

TEST_F(ColumnStatisticsTest, initialSize) {
  testInitialSize();
}

TEST(MapStatistics, orcUnsupportedMapStatistics) {
  proto::orc::ColumnStatistics proto;
  auto columnStatisticsWrapper = ColumnStatisticsWrapper(&proto);
  ASSERT_FALSE(columnStatisticsWrapper.hasMapStatistics());
  ASSERT_THROW(
      columnStatisticsWrapper.mapStatistics(),
      ::facebook::velox::VeloxRuntimeError);
}

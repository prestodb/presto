/*
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

#include "presto_cpp/main/thrift/ThriftIO.h"
#include <folly/io/IOBuf.h>
#include <gtest/gtest.h>
#include <thrift/lib/cpp2/protocol/BinaryProtocol.h>
#include "presto_cpp/main/thrift/ProtocolToThrift.h"
#include "presto_cpp/main/thrift/gen-cpp2/presto_native_types.h"

using namespace facebook::presto;

class ThriftIOTest : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}

  template <typename T>
  void testThriftRoundTrips(T& original) {
    // Test thriftWrite -> thriftRead round trip
    std::string serializedString = thriftWrite(original);
    EXPECT_FALSE(serializedString.empty());
    EXPECT_GT(serializedString.size(), 0);

    auto deserializedFromString = std::make_shared<T>();
    thriftRead(serializedString, deserializedFromString);
    EXPECT_EQ(original, *deserializedFromString);

    // Test thriftWriteIOBuf -> thriftRead round trip
    auto iobuf = thriftWriteIOBuf(original);
    EXPECT_TRUE(iobuf != nullptr);
    EXPECT_GT(iobuf->computeChainDataLength(), 0);

    std::string serializedFromIOBuf = iobuf->moveToFbString().toStdString();
    auto deserializedFromIOBuf = std::make_shared<T>();
    thriftRead(serializedFromIOBuf, deserializedFromIOBuf);
    EXPECT_EQ(original, *deserializedFromIOBuf);

    // Verify both serialization methods produce identical results
    EXPECT_EQ(serializedString, serializedFromIOBuf);
  }
};

TEST_F(ThriftIOTest, thriftHostAddressRoundTrip) {
  thrift::HostAddress original;
  original.hostPortString_ref() = "localhost:8080";

  testThriftRoundTrips(original);
}

TEST_F(ThriftIOTest, thriftStageIdRoundTrip) {
  thrift::StageId original;
  original.queryId_ref() = "test_query_123";
  original.id_ref() = 42;

  testThriftRoundTrips(original);
}

TEST_F(ThriftIOTest, thriftPlanNodeIdRoundTrip) {
  thrift::PlanNodeId original;
  original.id_ref() = "test_plan_node_123";

  testThriftRoundTrips(original);
}

TEST_F(ThriftIOTest, thriftEmptyStringRoundTrip) {
  thrift::HostAddress original;
  original.hostPortString_ref() = ""; // Empty string

  testThriftRoundTrips(original);
}

TEST_F(ThriftIOTest, thriftLargeDataRoundTrip) {
  thrift::StageId original;
  original.queryId_ref() = std::string(1000, 'x'); // Large string
  original.id_ref() = INT32_MAX;

  testThriftRoundTrips(original);
}

TEST_F(ThriftIOTest, thriftNegativeValuesRoundTrip) {
  thrift::StageId original;
  original.queryId_ref() = "negative_test";
  original.id_ref() = -12345;

  testThriftRoundTrips(original);
}

TEST_F(ThriftIOTest, thriftOutputBufferIdRoundTrip) {
  thrift::OutputBufferId original;
  original.id_ref() = 98765;

  testThriftRoundTrips(original);
}

TEST_F(ThriftIOTest, thriftZeroValuesRoundTrip) {
  thrift::OutputBufferId original;
  original.id_ref() = 0;

  testThriftRoundTrips(original);
}

TEST_F(ThriftIOTest, thriftTaskIdRoundTrip) {
  thrift::TaskId original;

  // Create nested StageExecutionId
  thrift::StageExecutionId stageExecutionId;
  thrift::StageId stageId;
  stageId.queryId_ref() = "complex_query_789";
  stageId.id_ref() = 555;
  stageExecutionId.stageId_ref() = stageId;
  stageExecutionId.id_ref() = 777;
  original.stageExecutionId_ref() = stageExecutionId;

  original.id_ref() = 999;
  original.attemptNumber_ref() = 3;

  testThriftRoundTrips(original);
}

TEST_F(ThriftIOTest, thriftStageExecutionIdRoundTrip) {
  thrift::StageExecutionId original;

  // Create nested StageId
  thrift::StageId stageId;
  stageId.queryId_ref() = "nested_stage_query";
  stageId.id_ref() = 111;
  original.stageId_ref() = stageId;

  original.id_ref() = 222;

  testThriftRoundTrips(original);
}

TEST_F(ThriftIOTest, thriftBroadcastFileFooterRoundTrip) {
  thrift::BroadcastFileFooter original;

  // Test with empty page sizes
  original.pageSizes_ref() = std::vector<int64_t>{};
  testThriftRoundTrips(original);

  // Test with single page size
  original.pageSizes_ref() = std::vector<int64_t>{1024};
  testThriftRoundTrips(original);

  // Test with multiple page sizes
  original.pageSizes_ref() = std::vector<int64_t>{1024, 2048, 4096, 8192};
  testThriftRoundTrips(original);

  // Test with large page sizes
  original.pageSizes_ref() = std::vector<int64_t>{
      1073741824, // 1GB
      2147483648, // 2GB
      268435456, // 256MB
      67108864 // 64MB
  };
  testThriftRoundTrips(original);

  // Test with zero and negative values (edge cases)
  original.pageSizes_ref() = std::vector<int64_t>{0, -1, 100, 0, 50};
  testThriftRoundTrips(original);
}

// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include <gtest/gtest.h>
#include "presto_cpp/main/thrift/ThriftUtils.h"
#include "presto_cpp/main/common/tests/test_json.h"

using namespace facebook;
using namespace facebook::presto::protocol;

class TaskStatusTest : public ::testing::Test {};

TEST_F(TaskStatusTest, lifeSpan) {
  std::string str = R"("Group1001")";

  json j = json::parse(str);
  Lifespan lifeSpan = j;
  cpp2::Lifespan thriftLifespan;
  toThrift(lifeSpan, thriftLifespan);

  ASSERT_EQ(thriftLifespan.grouped(), true);
  ASSERT_EQ(thriftLifespan.groupId(), 1001);
}

TEST_F(TaskStatusTest, errorCode) {
  std::string str = R"({
    "code": 1234,
    "name": "name",
    "type": "INTERNAL_ERROR",
    "retriable": false
  })";

  json j = json::parse(str);
  ErrorCode errorCode = j;
  cpp2::ErrorCode thriftErrorCode;
  toThrift(errorCode, thriftErrorCode);

  ASSERT_EQ(thriftErrorCode.code(), 1234);
  ASSERT_EQ(thriftErrorCode.name(), "name");
  ASSERT_EQ(thriftErrorCode.type(), cpp2::ErrorType::INTERNAL_ERROR);
  ASSERT_EQ(thriftErrorCode.retriable(), false);
}

TEST_F(TaskStatusTest, executionFailureInfoOptionalFieldsEmpty) {
  std::string str = R"({
    "type": "type",
    "message": "message",
    "suppressed": [],
    "stack": [],
    "errorLocation": {
        "lineNumber": 1,
        "columnNumber": 2
    },
    "errorCode": {
        "code": 1234,
        "name": "name",
        "type": "INTERNAL_ERROR",
        "retriable": false
    },
    "remoteHost": "localhost:8080",
    "errorCause": "EXCEEDS_BROADCAST_MEMORY_LIMIT"
  })";

  json j = json::parse(str);
  ExecutionFailureInfo executionFailureInfo = j;
  cpp2::ExecutionFailureInfo thriftExecutionFailureInfo;
  toThrift(executionFailureInfo, thriftExecutionFailureInfo);

  ASSERT_EQ(thriftExecutionFailureInfo.type(), "type");
  ASSERT_EQ(thriftExecutionFailureInfo.errorLocation()->columnNumber(), 2);
  ASSERT_EQ(thriftExecutionFailureInfo.remoteHost()->host(), "localhost");
  ASSERT_EQ(thriftExecutionFailureInfo.remoteHost()->port(), 8080);
  ASSERT_EQ(thriftExecutionFailureInfo.errorCode()->type(), cpp2::ErrorType::INTERNAL_ERROR);
  ASSERT_EQ(thriftExecutionFailureInfo.errorCode()->retriable(), false);
  ASSERT_EQ(thriftExecutionFailureInfo.errorCause(), cpp2::ErrorCause::EXCEEDS_BROADCAST_MEMORY_LIMIT);
  ASSERT_EQ(thriftExecutionFailureInfo.cause(), nullptr);
  ASSERT_EQ(thriftExecutionFailureInfo.suppressed()->size(), 0);
}

TEST_F(TaskStatusTest, executionFailureInfoOptionalFieldsNonempty) {
  std::string str = slurp(getDataPath("/github/presto-trunk/presto-native-execution/presto_cpp/main/thrift/tests/data/", "ExecutionFailureInfo.json"));

  json j = json::parse(str);
  ExecutionFailureInfo executionFailureInfo = j;
  cpp2::ExecutionFailureInfo thriftExecutionFailureInfo;
  toThrift(executionFailureInfo, thriftExecutionFailureInfo);

  ASSERT_EQ((*thriftExecutionFailureInfo.cause()).type(), "cause");
  ASSERT_EQ((*thriftExecutionFailureInfo.cause()).errorCause(), cpp2::ErrorCause::UNKNOWN);
  ASSERT_EQ((*thriftExecutionFailureInfo.cause()).errorCode()->type(), cpp2::ErrorType::INSUFFICIENT_RESOURCES);
  ASSERT_EQ((*thriftExecutionFailureInfo.cause()).errorCode()->retriable(), true);
  ASSERT_EQ((*thriftExecutionFailureInfo.suppressed())[0].type(), "suppressed1");
  ASSERT_EQ((*thriftExecutionFailureInfo.suppressed())[0].errorCause(), cpp2::ErrorCause::LOW_PARTITION_COUNT);
  ASSERT_EQ((*thriftExecutionFailureInfo.suppressed())[0].errorCode()->type(), cpp2::ErrorType::EXTERNAL);
  ASSERT_EQ((*thriftExecutionFailureInfo.suppressed())[1].type(), "suppressed2");
  ASSERT_EQ((*thriftExecutionFailureInfo.suppressed())[1].errorCause(), cpp2::ErrorCause::EXCEEDS_BROADCAST_MEMORY_LIMIT);
  ASSERT_EQ((*thriftExecutionFailureInfo.suppressed())[1].errorCode()->type(), cpp2::ErrorType::INTERNAL_ERROR);
}

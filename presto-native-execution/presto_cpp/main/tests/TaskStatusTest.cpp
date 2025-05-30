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

#include <gtest/gtest.h>
#include "presto_cpp/main/thrift/ProtocolToThrift.h"
#include "presto_cpp/main/common/tests/test_json.h"

using namespace facebook;
using namespace facebook::presto::protocol;

class TaskStatusTest : public ::testing::Test {};

TEST_F(TaskStatusTest, lifeSpan) {
  std::string str = R"("Group1001")";

  json j = json::parse(str);
  Lifespan lifeSpan = j;
  facebook::presto::thrift::Lifespan thriftLifespan;
  facebook::presto::thrift::toThrift(lifeSpan, thriftLifespan);

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
  facebook::presto::thrift::ErrorCode thriftErrorCode;
  facebook::presto::thrift::toThrift(errorCode, thriftErrorCode);

  ASSERT_EQ(thriftErrorCode.code(), 1234);
  ASSERT_EQ(thriftErrorCode.name(), "name");
  ASSERT_EQ(thriftErrorCode.type(), facebook::presto::thrift::ErrorType::INTERNAL_ERROR);
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
  facebook::presto::thrift::ExecutionFailureInfo thriftExecutionFailureInfo;
  facebook::presto::thrift::toThrift(executionFailureInfo, thriftExecutionFailureInfo);

  ASSERT_EQ(thriftExecutionFailureInfo.type(), "type");
  ASSERT_EQ(thriftExecutionFailureInfo.errorLocation()->columnNumber(), 2);
  ASSERT_EQ(thriftExecutionFailureInfo.remoteHost()->host(), "localhost");
  ASSERT_EQ(thriftExecutionFailureInfo.remoteHost()->port(), 8080);
  ASSERT_EQ(thriftExecutionFailureInfo.errorCode()->type(), facebook::presto::thrift::ErrorType::INTERNAL_ERROR);
  ASSERT_EQ(thriftExecutionFailureInfo.errorCode()->retriable(), false);
  ASSERT_EQ(thriftExecutionFailureInfo.errorCause(), facebook::presto::thrift::ErrorCause::EXCEEDS_BROADCAST_MEMORY_LIMIT);
  ASSERT_EQ(thriftExecutionFailureInfo.cause(), nullptr);
  ASSERT_EQ(thriftExecutionFailureInfo.suppressed()->size(), 0);
}

TEST_F(TaskStatusTest, executionFailureInfoOptionalFieldsNonempty) {
  std::string str = slurp(getDataPath("/github/presto-trunk/presto-native-execution/presto_cpp/main/tests/data/", "ExecutionFailureInfo.json"));

  json j = json::parse(str);
  ExecutionFailureInfo executionFailureInfo = j;
  facebook::presto::thrift::ExecutionFailureInfo thriftExecutionFailureInfo;
  facebook::presto::thrift::toThrift(executionFailureInfo, thriftExecutionFailureInfo);

  ASSERT_EQ((*thriftExecutionFailureInfo.cause()).type(), "cause");
  ASSERT_EQ((*thriftExecutionFailureInfo.cause()).errorCause(), facebook::presto::thrift::ErrorCause::UNKNOWN);
  ASSERT_EQ((*thriftExecutionFailureInfo.cause()).errorCode()->type(), facebook::presto::thrift::ErrorType::INSUFFICIENT_RESOURCES);
  ASSERT_EQ((*thriftExecutionFailureInfo.cause()).errorCode()->retriable(), true);
  ASSERT_EQ((*thriftExecutionFailureInfo.suppressed())[0].type(), "suppressed1");
  ASSERT_EQ((*thriftExecutionFailureInfo.suppressed())[0].errorCause(), facebook::presto::thrift::ErrorCause::LOW_PARTITION_COUNT);
  ASSERT_EQ((*thriftExecutionFailureInfo.suppressed())[0].errorCode()->type(), facebook::presto::thrift::ErrorType::EXTERNAL);
  ASSERT_EQ((*thriftExecutionFailureInfo.suppressed())[1].type(), "suppressed2");
  ASSERT_EQ((*thriftExecutionFailureInfo.suppressed())[1].errorCause(), facebook::presto::thrift::ErrorCause::EXCEEDS_BROADCAST_MEMORY_LIMIT);
  ASSERT_EQ((*thriftExecutionFailureInfo.suppressed())[1].errorCode()->type(), facebook::presto::thrift::ErrorType::INTERNAL_ERROR);
}

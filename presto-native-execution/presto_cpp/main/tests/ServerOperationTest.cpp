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
#include "presto_cpp/main/ServerOperation.h"
#include <gtest/gtest.h>
#include "velox/common/base/Exceptions.h"

DECLARE_bool(velox_memory_leak_check_enabled);

namespace facebook::presto {

class ServerOperationTest : public testing::Test {
  void SetUp() override {
    FLAGS_velox_memory_leak_check_enabled = true;
  }
};

TEST_F(ServerOperationTest, targetActionLookup) {
  {
    // Targets lookup verification
    EXPECT_EQ(
        ServerOperation::kTargetLookup.size(),
        ServerOperation::kReverseTargetLookup.size());
    const auto endReverseIt = ServerOperation::kReverseTargetLookup.end();
    for (auto lookupIt = ServerOperation::kTargetLookup.begin();
         lookupIt != ServerOperation::kTargetLookup.end();
         lookupIt++) {
      EXPECT_NE(
          ServerOperation::kReverseTargetLookup.find(lookupIt->second),
          endReverseIt);
    }
    const auto endLookupIt = ServerOperation::kTargetLookup.end();
    for (auto reverseIt = ServerOperation::kReverseTargetLookup.begin();
         reverseIt != ServerOperation::kReverseTargetLookup.end();
         reverseIt++) {
      EXPECT_NE(
          ServerOperation::kTargetLookup.find(reverseIt->second), endLookupIt);
    }
  }

  {
    // Actions lookup verification
    EXPECT_EQ(
        ServerOperation::kActionLookup.size(),
        ServerOperation::kReverseActionLookup.size());
    const auto endReverseIt = ServerOperation::kReverseActionLookup.end();
    for (auto lookupIt = ServerOperation::kActionLookup.begin();
         lookupIt != ServerOperation::kActionLookup.end();
         lookupIt++) {
      EXPECT_NE(
          ServerOperation::kReverseActionLookup.find(lookupIt->second),
          endReverseIt);
    }
    const auto endLookupIt = ServerOperation::kActionLookup.end();
    for (auto reverseIt = ServerOperation::kReverseActionLookup.begin();
         reverseIt != ServerOperation::kReverseActionLookup.end();
         reverseIt++) {
      EXPECT_NE(
          ServerOperation::kActionLookup.find(reverseIt->second), endLookupIt);
    }
  }
}

TEST_F(ServerOperationTest, stringEnumConversion) {
  for (auto lookupIt = ServerOperation::kTargetLookup.begin();
       lookupIt != ServerOperation::kTargetLookup.end();
       lookupIt++) {
    EXPECT_EQ(
        ServerOperation::targetFromString(lookupIt->first), lookupIt->second);
    EXPECT_EQ(ServerOperation::targetString(lookupIt->second), lookupIt->first);
  }
  EXPECT_THROW(
      ServerOperation::targetFromString("UNKNOWN_TARGET"),
      velox::VeloxUserError);

  for (auto lookupIt = ServerOperation::kActionLookup.begin();
       lookupIt != ServerOperation::kActionLookup.end();
       lookupIt++) {
    EXPECT_EQ(
        ServerOperation::actionFromString(lookupIt->first), lookupIt->second);
    EXPECT_EQ(ServerOperation::actionString(lookupIt->second), lookupIt->first);
  }
  EXPECT_THROW(
      ServerOperation::actionFromString("UNKNOWN_ACTION"),
      velox::VeloxUserError);
}

TEST_F(ServerOperationTest, buildServerOp) {
  ServerOperation op;
  op = buildServerOpFromHttpMsgPath("/v1/operation/connector/clearCache");
  EXPECT_EQ(ServerOperation::Target::kConnector, op.target);
  EXPECT_EQ(ServerOperation::Action::kClearCache, op.action);

  op = buildServerOpFromHttpMsgPath("/v1/operation/connector/getCacheStats");
  EXPECT_EQ(ServerOperation::Target::kConnector, op.target);
  EXPECT_EQ(ServerOperation::Action::kGetCacheStats, op.action);

  op = buildServerOpFromHttpMsgPath("/v1/operation/systemConfig/setProperty");
  EXPECT_EQ(ServerOperation::Target::kSystemConfig, op.target);
  EXPECT_EQ(ServerOperation::Action::kSetProperty, op.action);

  EXPECT_THROW(
      op = buildServerOpFromHttpMsgPath("/v1/operation/whatzit/setProperty"),
      velox::VeloxUserError);
}

} // namespace facebook::presto
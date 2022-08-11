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

#include "velox/common/base/tests/GTestUtils.h"

#include "velox/substrait/SubstraitParser.h"
#include "velox/substrait/TypeUtils.h"
#include "velox/substrait/VeloxToSubstraitType.h"

using namespace facebook::velox;
using namespace facebook::velox::substrait;

namespace facebook::velox::substrait::test {

class VeloxToSubstraitTypeTest : public ::testing::Test {
 protected:
  void testTypeConversion(const TypePtr& type) {
    SCOPED_TRACE(type->toString());

    google::protobuf::Arena arena;
    auto substraitType = typeConvertor_->toSubstraitType(arena, type);
    auto sameType =
        toVeloxType(substraitParser_->parseType(substraitType)->type);
    ASSERT_TRUE(sameType->kindEquals(type))
        << "Expected: " << type->toString()
        << ", but got: " << sameType->toString();
  }

  std::shared_ptr<VeloxToSubstraitTypeConvertor> typeConvertor_;

  std::shared_ptr<SubstraitParser> substraitParser_ =
      std::make_shared<SubstraitParser>();
};

TEST_F(VeloxToSubstraitTypeTest, basic) {
  testTypeConversion(BOOLEAN());

  testTypeConversion(TINYINT());
  testTypeConversion(SMALLINT());
  testTypeConversion(INTEGER());
  testTypeConversion(BIGINT());

  testTypeConversion(REAL());
  testTypeConversion(DOUBLE());

  testTypeConversion(VARCHAR());
  testTypeConversion(VARBINARY());

  // Array type is not supported yet.
  ASSERT_ANY_THROW(testTypeConversion(ARRAY(BIGINT())));

  // Map type is not supported yet.
  ASSERT_ANY_THROW(testTypeConversion(MAP(BIGINT(), DOUBLE())));

  testTypeConversion(ROW({"a", "b", "c"}, {BIGINT(), BOOLEAN(), VARCHAR()}));
  testTypeConversion(
      ROW({"a", "b", "c"},
          {BIGINT(), ROW({"x", "y"}, {BOOLEAN(), VARCHAR()}), REAL()}));
  ASSERT_ANY_THROW(testTypeConversion(ROW({}, {})));
}
} // namespace facebook::velox::substrait::test

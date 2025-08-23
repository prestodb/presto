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
#include "velox/functions/prestosql/types/QDigestType.h"
#include "velox/functions/prestosql/types/QDigestRegistration.h"
#include "velox/functions/prestosql/types/parser/TypeParser.h"
#include "velox/functions/prestosql/types/tests/TypeTestBase.h"

namespace facebook::velox::test {
namespace {

class QDigestTypeTest : public testing::Test, public TypeTestBase {
 public:
  QDigestTypeTest() {
    registerQDigestType();
  }
};

TEST_F(QDigestTypeTest, basic) {
  auto testType = [](const std::string& typeString,
                     const TypePtr& parameterType) {
    auto type = QDIGEST(parameterType);
    ASSERT_STREQ(type->name(), "QDIGEST");
    ASSERT_STREQ(type->kindName(), "VARBINARY");
    ASSERT_EQ(type->parameters().size(), 1);
    ASSERT_EQ(type->toString(), typeString);

    ASSERT_TRUE(hasType("QDIGEST"));
    ASSERT_EQ(*getType("QDIGEST", {TypeParameter(parameterType)}), *type);

    ASSERT_FALSE(type->isOrderable());
  };
  testType("QDIGEST(BIGINT)", BIGINT());
  testType("QDIGEST(REAL)", REAL());
  testType("QDIGEST(DOUBLE)", DOUBLE());
}

TEST_F(QDigestTypeTest, serde) {
  testTypeSerde(QDIGEST(BIGINT()));
  testTypeSerde(QDIGEST(REAL()));
  testTypeSerde(QDIGEST(DOUBLE()));
}

TEST_F(QDigestTypeTest, parse) {
  ASSERT_EQ(
      *facebook::velox::functions::prestosql::parseType("qdigest(bigint)"),
      *QDIGEST(BIGINT()));
  ASSERT_EQ(
      *facebook::velox::functions::prestosql::parseType("qdigest(real)"),
      *QDIGEST(REAL()));
  ASSERT_EQ(
      *facebook::velox::functions::prestosql::parseType("qdigest(double)"),
      *QDIGEST(DOUBLE()));
}

} // namespace
} // namespace facebook::velox::test

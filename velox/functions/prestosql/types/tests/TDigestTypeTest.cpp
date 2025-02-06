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
#include "velox/functions/prestosql/types/TDigestType.h"
#include "velox/functions/prestosql/types/tests/TypeTestBase.h"

namespace facebook::velox::test {

class TDigestTypeTest : public testing::Test, public TypeTestBase {
 public:
  TDigestTypeTest() {
    registerTDigestType();
  }
};

TEST_F(TDigestTypeTest, basic) {
  ASSERT_STREQ(TDIGEST()->name(), "TDIGEST");
  ASSERT_STREQ(TDIGEST()->kindName(), "VARBINARY");
  ASSERT_TRUE(TDIGEST()->parameters().empty());
  ASSERT_EQ(TDIGEST()->toString(), "TDIGEST");

  ASSERT_TRUE(hasType("TDIGEST"));
  ASSERT_EQ(*getType("TDIGEST", {}), *TDIGEST());
}

TEST_F(TDigestTypeTest, serde) {
  testTypeSerde(TDIGEST());
}
} // namespace facebook::velox::test

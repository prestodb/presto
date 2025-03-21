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

#include "velox/functions/prestosql/types/GeometryType.h"
#include "velox/functions/prestosql/types/GeometryRegistration.h"
#include "velox/functions/prestosql/types/tests/TypeTestBase.h"

namespace facebook::velox::test {

class GeometryTypeTest : public testing::Test, public TypeTestBase {
 public:
  GeometryTypeTest() {
    registerGeometryType();
  }
};

TEST_F(GeometryTypeTest, basic) {
  ASSERT_EQ(GEOMETRY()->name(), "GEOMETRY");
  ASSERT_EQ(GEOMETRY()->kindName(), "VARBINARY");
  ASSERT_TRUE(GEOMETRY()->parameters().empty());
  ASSERT_EQ(GEOMETRY()->toString(), "GEOMETRY");

  ASSERT_TRUE(hasType("GEOMETRY"));
  ASSERT_EQ(*getType("GEOMETRY", {}), *GEOMETRY());
}

TEST_F(GeometryTypeTest, serde) {
  testTypeSerde(GEOMETRY());
}
} // namespace facebook::velox::test

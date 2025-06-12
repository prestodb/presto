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

#include "velox/exec/fuzzer/PrestoQueryRunnerIntermediateTypeTransforms.h"
#include "velox/exec/tests/PrestoQueryRunnerIntermediateTypeTransformTestBase.h"
#include "velox/functions/prestosql/types/QDigestType.h"

namespace facebook::velox::exec::test {
namespace {

class PrestoQueryRunnerQDigestTransformTest
    : public PrestoQueryRunnerIntermediateTypeTransformTestBase {};

TEST_F(PrestoQueryRunnerQDigestTransformTest, isIntermediateOnlyType) {
  ASSERT_TRUE(isIntermediateOnlyType(QDIGEST(DOUBLE())));
  ASSERT_TRUE(isIntermediateOnlyType(ARRAY(QDIGEST(DOUBLE()))));
  ASSERT_TRUE(isIntermediateOnlyType(MAP(QDIGEST(DOUBLE()), SMALLINT())));
  ASSERT_TRUE(isIntermediateOnlyType(MAP(VARBINARY(), QDIGEST(DOUBLE()))));
  ASSERT_TRUE(isIntermediateOnlyType(ROW({QDIGEST(DOUBLE()), SMALLINT()})));
  ASSERT_TRUE(isIntermediateOnlyType(ROW(
      {SMALLINT(),
       TIMESTAMP(),
       ARRAY(ROW({MAP(VARCHAR(), QDIGEST(DOUBLE()))}))})));

  ASSERT_TRUE(isIntermediateOnlyType(QDIGEST(BIGINT())));
  ASSERT_TRUE(isIntermediateOnlyType(ARRAY(QDIGEST(BIGINT()))));
  ASSERT_TRUE(isIntermediateOnlyType(MAP(QDIGEST(BIGINT()), SMALLINT())));
  ASSERT_TRUE(isIntermediateOnlyType(MAP(VARBINARY(), QDIGEST(BIGINT()))));
  ASSERT_TRUE(isIntermediateOnlyType(ROW({QDIGEST(BIGINT()), SMALLINT()})));
  ASSERT_TRUE(isIntermediateOnlyType(ROW(
      {SMALLINT(),
       TIMESTAMP(),
       ARRAY(ROW({MAP(VARCHAR(), QDIGEST(BIGINT()))}))})));

  ASSERT_TRUE(isIntermediateOnlyType(QDIGEST(REAL())));
  ASSERT_TRUE(isIntermediateOnlyType(ARRAY(QDIGEST(REAL()))));
  ASSERT_TRUE(isIntermediateOnlyType(MAP(QDIGEST(REAL()), SMALLINT())));
  ASSERT_TRUE(isIntermediateOnlyType(MAP(VARBINARY(), QDIGEST(REAL()))));
  ASSERT_TRUE(isIntermediateOnlyType(ROW({QDIGEST(REAL()), SMALLINT()})));
  ASSERT_TRUE(isIntermediateOnlyType(ROW(
      {SMALLINT(),
       TIMESTAMP(),
       ARRAY(ROW({MAP(VARCHAR(), QDIGEST(REAL()))}))})));
}

TEST_F(PrestoQueryRunnerQDigestTransformTest, transform) {
  test(QDIGEST(DOUBLE()));
  test(QDIGEST(BIGINT()));
  test(QDIGEST(REAL()));
}

TEST_F(PrestoQueryRunnerQDigestTransformTest, transformArray) {
  testArray(QDIGEST(DOUBLE()));
  testArray(QDIGEST(BIGINT()));
  testArray(QDIGEST(REAL()));
}

TEST_F(PrestoQueryRunnerQDigestTransformTest, transformMap) {
  testMap(QDIGEST(DOUBLE()));
  testMap(QDIGEST(BIGINT()));
  testMap(QDIGEST(REAL()));
}

TEST_F(PrestoQueryRunnerQDigestTransformTest, transformRow) {
  testRow(QDIGEST(DOUBLE()));
  testRow(QDIGEST(BIGINT()));
  testRow(QDIGEST(REAL()));
}

} // namespace
} // namespace facebook::velox::exec::test

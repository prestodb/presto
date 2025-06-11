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
#include "velox/functions/prestosql/types/TDigestType.h"

namespace facebook::velox::exec::test {
namespace {

class PrestoQueryRunnerTDigestTransformTest
    : public PrestoQueryRunnerIntermediateTypeTransformTestBase {};

TEST_F(PrestoQueryRunnerTDigestTransformTest, isIntermediateOnlyType) {
  ASSERT_TRUE(isIntermediateOnlyType(TDIGEST(DOUBLE())));
  ASSERT_TRUE(isIntermediateOnlyType(ARRAY(TDIGEST(DOUBLE()))));
  ASSERT_TRUE(isIntermediateOnlyType(MAP(TDIGEST(DOUBLE()), SMALLINT())));
  ASSERT_TRUE(isIntermediateOnlyType(MAP(VARBINARY(), TDIGEST(DOUBLE()))));
  ASSERT_TRUE(isIntermediateOnlyType(ROW({TDIGEST(DOUBLE()), SMALLINT()})));
  ASSERT_TRUE(isIntermediateOnlyType(ROW(
      {SMALLINT(),
       TIMESTAMP(),
       ARRAY(ROW({MAP(VARCHAR(), TDIGEST(DOUBLE()))}))})));
}

TEST_F(PrestoQueryRunnerTDigestTransformTest, transform) {
  test(TDIGEST(DOUBLE()));
}

TEST_F(PrestoQueryRunnerTDigestTransformTest, transformArray) {
  testArray(TDIGEST(DOUBLE()));
}

TEST_F(PrestoQueryRunnerTDigestTransformTest, transformMap) {
  testMap(TDIGEST(DOUBLE()));
}

TEST_F(PrestoQueryRunnerTDigestTransformTest, transformRow) {
  testRow(TDIGEST(DOUBLE()));
}

} // namespace
} // namespace facebook::velox::exec::test

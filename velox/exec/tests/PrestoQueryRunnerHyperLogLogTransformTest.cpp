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
#include "velox/functions/prestosql/types/HyperLogLogType.h"

namespace facebook::velox::exec::test {
namespace {

class PrestoQueryRunnerHyperLogLogTransformTest
    : public PrestoQueryRunnerIntermediateTypeTransformTestBase {};

TEST_F(PrestoQueryRunnerHyperLogLogTransformTest, isIntermediateOnlyType) {
  ASSERT_TRUE(isIntermediateOnlyType(HYPERLOGLOG()));
  ASSERT_TRUE(isIntermediateOnlyType(ARRAY(HYPERLOGLOG())));
  ASSERT_TRUE(isIntermediateOnlyType(MAP(HYPERLOGLOG(), SMALLINT())));
  ASSERT_TRUE(isIntermediateOnlyType(MAP(VARBINARY(), HYPERLOGLOG())));
  ASSERT_TRUE(isIntermediateOnlyType(ROW({HYPERLOGLOG(), SMALLINT()})));
  ASSERT_TRUE(isIntermediateOnlyType(ROW(
      {SMALLINT(), TIMESTAMP(), ARRAY(ROW({MAP(VARCHAR(), HYPERLOGLOG())}))})));
}

TEST_F(PrestoQueryRunnerHyperLogLogTransformTest, transform) {
  test(HYPERLOGLOG());
}

TEST_F(PrestoQueryRunnerHyperLogLogTransformTest, transformArray) {
  testArray(HYPERLOGLOG());
}

TEST_F(PrestoQueryRunnerHyperLogLogTransformTest, transformMap) {
  testMap(HYPERLOGLOG());
}

TEST_F(PrestoQueryRunnerHyperLogLogTransformTest, transformRow) {
  testRow(HYPERLOGLOG());
}

} // namespace
} // namespace facebook::velox::exec::test

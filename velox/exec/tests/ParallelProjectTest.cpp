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

#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

namespace facebook::velox::exec {
namespace {

class ParallelProjectTest : public test::OperatorTestBase {
 protected:
  void SetUp() override {
    test::OperatorTestBase::SetUp();
  }
};

TEST_F(ParallelProjectTest, basic) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>(100, folly::identity),
      makeFlatVector<int32_t>(100, folly::identity),
  });

  createDuckDbTable({data});

  auto plan =
      test::PlanBuilder()
          .values({data})
          .parallelProject({{"c0 + 1", "c0 * 2"}, {"c1 + 10", "c1 * 3"}})
          .planNode();

  assertQuery(plan, "SELECT c0 + 1, c0 * 2, c1 + 10, c1 * 3 FROM tmp");
}

} // namespace
} // namespace facebook::velox::exec

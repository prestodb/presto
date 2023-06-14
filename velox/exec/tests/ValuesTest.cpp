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

#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

using namespace facebook::velox;
using exec::test::AssertQueryBuilder;

namespace facebook::velox::exec::test {

class ValuesTest : public OperatorTestBase {
 protected:
  // Sample row vectors.
  RowVectorPtr input_{makeRowVector({
      makeFlatVector<int32_t>({0, 1, 2, 3, 5}),
      makeFlatVector<StringView>({"a", "b", "c", "d", "e"}),
      makeFlatVector<float>({0.1, 2.3, 4.5, 6.7, 8.9}),
  })};

  RowVectorPtr input2_{makeRowVector({
      makeFlatVector<int32_t>({6, 7}),
      makeFlatVector<StringView>({"f", "g"}),
      makeFlatVector<float>({10.11, 12.13}),
  })};
};

TEST_F(ValuesTest, empty) {
  // Base case: no vectors.
  AssertQueryBuilder(PlanBuilder().values({}).planNode()).assertEmptyResults();

  // Empty input vector.
  auto emptyInput = makeRowVector({});
  AssertQueryBuilder(PlanBuilder().values({emptyInput}).planNode())
      .assertEmptyResults();

  // Many empty vectors as input.
  AssertQueryBuilder(
      PlanBuilder().values({emptyInput, emptyInput, emptyInput}).planNode())
      .assertEmptyResults();
}

TEST_F(ValuesTest, simple) {
  // Single vector in, single vector out.
  AssertQueryBuilder(PlanBuilder().values({input_}).planNode())
      .assertResults({input_});

  // 3 vectors in, 3 vectors out.
  AssertQueryBuilder(PlanBuilder().values({input_, input_, input_}).planNode())
      .assertResults({input_, input_, input_});

  // Combine two different vectors.
  AssertQueryBuilder(
      PlanBuilder().values({input_, input2_, input2_, input_}).planNode())
      .assertResults({input_, input2_, input2_, input_});
}

TEST_F(ValuesTest, valuesWithParallelism) {
  // Parallelism true but single thread.
  AssertQueryBuilder(PlanBuilder().values({input_}, true).planNode())
      .assertResults({input_});

  // 5 threads.
  AssertQueryBuilder(PlanBuilder().values({input_}, true).planNode())
      .maxDrivers(5)
      .assertResults({input_, input_, input_, input_, input_});
}

TEST_F(ValuesTest, valuesWithRepeat) {
  // Single vectors in with repeat, many vectors out.
  AssertQueryBuilder(PlanBuilder().values({input_}, false, 2).planNode())
      .assertResults(std::vector<RowVectorPtr>{input_, input_});

  AssertQueryBuilder(PlanBuilder().values({input_}, false, 7).planNode())
      .assertResults({input_, input_, input_, input_, input_, input_, input_});

  // Zero repeats.
  AssertQueryBuilder(PlanBuilder().values({}, false, 0).planNode())
      .assertEmptyResults();
  AssertQueryBuilder(PlanBuilder().values({input_}, false, 0).planNode())
      .assertEmptyResults();

  // Try repeating with 2 different row vectors.
  AssertQueryBuilder(
      PlanBuilder().values({input_, input2_}, false, 3).planNode())
      .assertResults({input_, input2_, input_, input2_, input_, input2_});
}

TEST_F(ValuesTest, valuesWithRepeatAndParallelism) {
  // Two threads repeating twice each buffer.
  AssertQueryBuilder(
      PlanBuilder().values({input_, input2_}, true, 2).planNode())
      .maxDrivers(2)
      .assertResults(
          {input_, input2_, input_, input2_, input_, input2_, input_, input2_});
}

} // namespace facebook::velox::exec::test

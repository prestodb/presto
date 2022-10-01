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
#include "velox/functions/prestosql/window/WindowFunctionsRegistration.h"

namespace facebook::velox::window::test {

class WindowTestBase : public exec::test::OperatorTestBase {
 protected:
  void SetUp() override {
    exec::test::OperatorTestBase::SetUp();
    velox::window::registerWindowFunctions();
  }

  std::vector<RowVectorPtr>
  makeVectors(const RowTypePtr& rowType, vector_size_t size, int numVectors);

  // This function tests SQL queries for the window function and
  // the specified overClauses with the input RowVectors.
  // Note : 'function' should be a full window function invocation string
  // including input parameters and open/close braces. e.g. rank(), ntile(5)
  void testWindowFunction(
      const std::vector<RowVectorPtr>& input,
      const std::string& function,
      const std::vector<std::string>& overClauses);

  // This function operates on input RowVectors that have 2 columns.
  // It verifies (for the windowFunction) SQL queries with varying over
  // clauses. The over clauses covers all combinations of partition by
  // and order by of the two input columns.
  // Note : 'windowFunction' should be a full window function invocation string
  // including input parameters and open/close braces. e.g. rank(), ntile(5)
  void testTwoColumnInput(
      const std::vector<RowVectorPtr>& input,
      const std::string& windowFunction);

 private:
  void testWindowFunction(
      const std::vector<RowVectorPtr>& input,
      const std::string& function,
      const std::string& overClause);
};
}; // namespace facebook::velox::window::test

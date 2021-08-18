/*
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

#include "velox/experimental/codegen/Codegen.h"
#include <gtest/gtest.h>
#include "velox/core/PlanNode.h"
#include "velox/experimental/codegen/CodegenExceptions.h"
#include "velox/experimental/codegen/tests/CodegenTestBase.h"
#include "velox/experimental/codegen/utils/timer/NestedScopedTimer.h"
#include "velox/type/Type.h"
namespace facebook::velox::codegen {
class CodegenTest : public CodegenTestBase {};

TEST_F(CodegenTest, simpleProjectionDefaultNull) {
  auto inputRowType = ROW({"a", "b"}, std::vector<TypePtr>{DOUBLE(), DOUBLE()});
  testExpressions<DoubleType, DoubleType>(
      {"a + b", "a - b"}, inputRowType, 10, 100);
};

TEST_F(CodegenTest, simpleProjectionNotDefaultNull) {
  auto inputRowType = ROW({"a", "b"}, std::vector<TypePtr>{DOUBLE(), DOUBLE()});
  testExpressions<DoubleType, DoubleType>({"a", "b"}, inputRowType, 10, 100);
  testExpressions<DoubleType, DoubleType>(
      {"if(a > b , a + b, a - b)", "if( a < b , a - b, a + b)"},
      inputRowType,
      10,
      100);
};

TEST_F(CodegenTest, compile) {
  auto codegenLogger = std::make_shared<codegen::DefaultLogger>("codegen test");
  Codegen codegen(codegenLogger);
  // check that the correct weak symbol is used
  EXPECT_THROW(
      codegen.initialize("dummy json string"), CodegenInitializationException);
};

TEST_F(CodegenTest, simpleProjectionWithConstantFields) {
  auto inputRowType = ROW({"a", "b"}, std::vector<TypePtr>{DOUBLE(), DOUBLE()});
  testExpressions<DoubleType>("a>b", {"2.0"}, inputRowType, 10, 100);
};

TEST_F(CodegenTest, DISABLED_simpleProjectionWithFilter) {
  auto inputRowType = ROW({"a", "b"}, std::vector<TypePtr>{DOUBLE(), DOUBLE()});
  testExpressions<DoubleType, DoubleType>(
      "a > b", {" a + b", "a - b"}, inputRowType, 10, 100);
  testExpressions<DoubleType>("NOT a IS NULL", {"a"}, inputRowType, 10, 100);
};

TEST_F(CodegenTest, reorderedInput) {
  std::shared_ptr<const RowType> inputRowType =
      ROW({"a", "b", "c", "d"},
          std::vector<TypePtr>{DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE()});

  testExpressions<DoubleType>({"if(d>c , b, a)"}, inputRowType, 10, 100);
};

TEST_F(CodegenTest, booleanProjection) {
  auto inputRowType = ROW(
      {"a", "b", "d"}, std::vector<TypePtr>{BOOLEAN(), BOOLEAN(), DOUBLE()});
  testExpressions<DoubleType>({"if(a and b, 1.1, 1.2)"}, inputRowType, 1, 10);
};

TEST_F(CodegenTest, stringInputProjection) {
  auto inputRowType = ROW({"a"}, std::vector<TypePtr>{VARCHAR()});
  testExpressions<BigintType>({"length(a)"}, inputRowType, 10, 100);
};

TEST_F(CodegenTest, stringInputProjection2) {
  auto inputRowType =
      ROW({"a", "b"}, std::vector<TypePtr>{VARCHAR(), VARCHAR()});
  testExpressions<BooleanType>({"length(a)>length(b)"}, inputRowType, 10, 100);
};

// T94073748
// TEST_F(CodegenTest, stringOutputProjectionInput) {
//   auto inputRowType =
//       ROW({"a", "b"}, std::vector<TypePtr>{VARCHAR(), VARCHAR()});
//   testExpressions<VarcharType, VarcharType>({"a", "b"}, inputRowType, 10,
//   100);
// };

TEST_F(CodegenTest, stringOutputProjectionUDF) {
  auto inputRowType =
      ROW({"a", "b"}, std::vector<TypePtr>{VARCHAR(), VARCHAR()});
  testExpressions<VarcharType, VarcharType, VarcharType>(
      {"upper(a)", "lower(b)", "concat(a,'aaa',b)"}, inputRowType, 1, 100);
};

TEST_F(CodegenTest, stringCompound) {
  auto inputRowType = ROW({"a"}, std::vector<TypePtr>{VARCHAR()});
  testExpressions<VarcharType>({"lower(upper(a))"}, inputRowType, 10, 100);
};

} // namespace facebook::velox::codegen

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

// perf event counter for the generated code

#include "velox/experimental/codegen/tests/CodegenTestBase.h"

namespace facebook::velox::codegen {
namespace {
class CodegenPerfTest : public CodegenTestBase {
 public:
  virtual void SetUp() override {
    init({"-DCODEGEN_PERF"});
  }
};
} // namespace

/*
defaultnull disabled
branch instructions: 3993869
branch misses: 207653
cpu clock: 13575605
task clock: 13570701
defaultnull enabled
branch instructions: 860547
branch misses: 15796
cpu clock: 10786259
task clock: 10781911
*/
TEST_F(CodegenPerfTest, DISABLED_simpleProjectionDefaultNull) {
  auto inputRowType = ROW({"a", "b"}, std::vector<TypePtr>{DOUBLE(), DOUBLE()});
  testExpressions<DoubleType, DoubleType>(
      {"a + b", "a - b"}, inputRowType, 1, 1000000);
};

} // namespace facebook::velox::codegen

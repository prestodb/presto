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
#include "presto_cpp/main/expression/RowExpressionOptimizer.h"
#include <folly/Uri.h>
#include <gtest/gtest.h>
#include "presto_cpp/main/common/tests/test_json.h"
#include "presto_cpp/main/http/tests/HttpTestBase.h"
#include "velox/expression/RegisterSpecialForm.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/VectorStream.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::presto;
using namespace facebook::velox;

namespace {
const std::string getDataPath(const std::string& fileName) {
  std::string currentPath = boost::filesystem::current_path().c_str();
  LOG(INFO) << "Current path: " << currentPath;
  LOG(INFO) << "File name: " << fileName;
  if (boost::algorithm::ends_with(currentPath, "fbcode")) {
    return currentPath +
        "/github/presto-trunk/presto-native-execution/presto_cpp/main/expression/tests/data/" +
        fileName;
  }

  // CLion runs the tests from cmake-build-release/ or cmake-build-debug/
  // directory. Hard-coded json files are not copied there and test fails with
  // file not found. Fixing the path so that we can trigger these tests from
  // CLion.
  boost::algorithm::replace_all(currentPath, "cmake-build-release/", "");
  boost::algorithm::replace_all(currentPath, "cmake-build-debug/", "");

  // The tests are run manually on the command line from inside _build/release
  // or _build/debug directory. Fixing the path for these test runs so the json
  // files are found.
  boost::algorithm::replace_all(currentPath, "_build/release/", "");
  boost::algorithm::replace_all(currentPath, "_build/debug/", "");

  return currentPath + "/data/" + fileName;
}
} // namespace

// RowExpressionOptimizerTest only tests for basic expression optimization. End
// to end tests for different types of expressions can be found in the file
// TestDelegatingExpressionOptimizer.java in presto-native-sidecar-plugin.
class RowExpressionOptimizerTest
    : public ::testing::Test,
      public facebook::velox::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    parse::registerTypeResolver();
    functions::prestosql::registerAllScalarFunctions("presto.default.");
    exec::registerFunctionCallToSpecialForms();
    rowExpressionOptimizer_ =
        std::make_unique<expression::RowExpressionOptimizer>();
  }

  void testFile(const std::string& testName) {
    auto input = slurp(getDataPath(fmt::format("{}Input.json", testName)));
    json::array_t inputExpressions = json::parse(input);
    proxygen::HTTPMessage httpMessage;
    httpMessage.getHeaders().set(
        "X-Presto-Time-Zone", "America/Bahia_Banderas");
    httpMessage.getHeaders().set("X-Presto-User", "test");
    httpMessage.getHeaders().set(
        "X-Presto-Expression-Optimizer-Level", "OPTIMIZED");
    auto result =
        rowExpressionOptimizer_->optimize(&httpMessage, inputExpressions);

    EXPECT_EQ(result.second, true);
    json resultExpressions = result.first;
    EXPECT_EQ(resultExpressions.is_array(), true);
    auto expected =
        slurp(getDataPath(fmt::format("{}Expected.json", testName)));
    json::array_t expectedExpressions = json::parse(expected);
    auto numExpressions = resultExpressions.size();
    EXPECT_EQ(numExpressions, expectedExpressions.size());
    for (auto i = 0; i < numExpressions; i++) {
      EXPECT_EQ(resultExpressions.at(i), expectedExpressions.at(i));
    }
  }

  std::unique_ptr<expression::RowExpressionOptimizer> rowExpressionOptimizer_;
};

TEST_F(RowExpressionOptimizerTest, simple) {
  // Files SimpleExpressions{Input|Expected}.json contain the input and expected
  // JSON representing the RowExpressions resulting from the following queries:
  // select 1 + 2;
  // select abs(-11) + ceil(cast(3.4 as double)) + floor(cast(5.6 as double));
  // select 2 between 1 and 3;
  // Simple expression evaluation with constant folding is verified here.
  testFile("SimpleExpressions");
}

TEST_F(RowExpressionOptimizerTest, specialFormRewrites) {
  // Files SpecialExpressions{Input|Expected}.json contain the input and
  // expected JSON representing the RowExpressions resulting from the following
  // queries:
  // select if(1 < 2, 2, 3);
  // select (1 < 2) and (2 < 3);
  // select (1 < 2) or (2 < 3);
  // Special form expression rewrites are verified here.
  testFile("SpecialForm");
}

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
#include <gtest/gtest.h>

#include "presto_cpp/main/common/tests/test_json.h"
#include "presto_cpp/main/common/Utils.h"
#include "presto_cpp/main/sidecar/function/CudfFunctionMetadata.h"
#include "presto_cpp/main/sidecar/function/tests/FunctionMetadataTestUtils.h"
#include "presto_cpp/main/types/tests/TestUtils.h"
#include "velox/experimental/cudf/exec/CudfHashAggregation.h"
#include "velox/experimental/cudf/expression/ExpressionEvaluator.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/functions/prestosql/window/WindowFunctionsRegistration.h"

using json = nlohmann::json;

static const std::string kPrestoDefaultPrefix = "presto.default.";
static const std::string kPrestoCudfPrefix = "presto.cudf.";

namespace facebook::presto::cudf::test {

using facebook::presto::cudf::cudfFunctionMetadataProvider;
using facebook::presto::test::utils::getDataPath;
using facebook::velox::aggregate::prestosql::registerAllAggregateFunctions;
using facebook::velox::cudf_velox::registerBuiltinFunctions;
using facebook::velox::cudf_velox::registerStepAwareBuiltinAggregationFunctions;
using facebook::velox::functions::prestosql::registerAllScalarFunctions;
using facebook::velox::window::prestosql::registerAllWindowFunctions;
using facebook::presto::test::function::testFunctionMetadata;

class CudfFunctionMetadataTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    // Register CUDF builtin functions with a prefix
    registerBuiltinFunctions(kPrestoCudfPrefix);
    // Register CUDF builtin aggregation functions
    registerStepAwareBuiltinAggregationFunctions(kPrestoCudfPrefix);
  }

  void SetUp() override {
    functionMetadata_ = cudfFunctionMetadataProvider().getFunctionsMetadata(
        std::nullopt);
  }

  json functionMetadata_;
};

// Tests for CUDF scalar functions
TEST_F(CudfFunctionMetadataTest, cudfCardinality) {
  // Test CUDF cardinality function
  testFunctionMetadata(
      functionMetadata_,
      "cardinality",
      "CudfCardinality.json",
  1);
}

// Tests for CUDF aggregate functions
TEST_F(CudfFunctionMetadataTest, cudfSum) {
  // Test CUDF sum aggregate function
  testFunctionMetadata(functionMetadata_, "sum", "CudfSum.json", 6);
}

// Test that metadata is returned as a JSON object
TEST_F(CudfFunctionMetadataTest, cudfMetadataStructure) {
  // The result should be a JSON object with function names as keys
  ASSERT_TRUE(functionMetadata_.is_object());
  ASSERT_FALSE(functionMetadata_.empty());

  // Verify that CUDF functions are present
  ASSERT_TRUE(functionMetadata_.contains("cardinality"));
  ASSERT_TRUE(functionMetadata_.contains("sum"));

  // Each function should have an array of signatures
  for (auto it = functionMetadata_.begin(); it != functionMetadata_.end();
       ++it) {
    ASSERT_TRUE(it.value().is_array()) << "Function: " << it.key();
    ASSERT_FALSE(it.value().empty()) << "Function: " << it.key();

    // Each signature should have the required fields
    for (const auto& signature : it.value()) {
      ASSERT_TRUE(signature.contains("outputType")) << "Function: " << it.key();
      ASSERT_TRUE(signature.contains("paramTypes")) << "Function: " << it.key();
      ASSERT_TRUE(signature.contains("schema")) << "Function: " << it.key();
      ASSERT_TRUE(signature.contains("functionKind"))
          << "Function: " << it.key();

      // Schema should be "cudf"
      EXPECT_EQ(signature["schema"], "cudf") << "Function: " << it.key();
    }
  }
}

TEST_F(CudfFunctionMetadataTest, cudfRegistrationHasCatalogSchemaPrefix) {
  for (const auto& [name, _] :
       facebook::velox::cudf_velox::getCudfFunctionSignatureMap()) {
    const auto parts = facebook::presto::util::getFunctionNameParts(name);
    ASSERT_EQ(parts.size(), 3);
    EXPECT_EQ(parts[0], "presto");
    EXPECT_EQ(parts[1], "cudf");
  }

  for (const auto& [name, _] :
       facebook::velox::cudf_velox::getCudfAggregationFunctionSignatureMap()) {
    const auto parts = facebook::presto::util::getFunctionNameParts(name);
    ASSERT_EQ(parts.size(), 3);
    EXPECT_EQ(parts[0], "presto");
    EXPECT_EQ(parts[1], "cudf");
  }
}

} // namespace facebook::presto::cudf::test

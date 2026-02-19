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
#include "presto_cpp/main/sidecar/function/NativeFunctionMetadata.h"
#include "presto_cpp/main/sidecar/function/tests/FunctionMetadataTestUtils.h"
#include "presto_cpp/main/types/tests/TestUtils.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/functions/prestosql/window/WindowFunctionsRegistration.h"

using namespace facebook::velox;
using namespace facebook::presto;
using facebook::presto::test::function::testFunctionMetadata;

using json = nlohmann::json;

static const std::string kPrestoDefaultPrefix = "presto.default.";

class FunctionMetadataTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    aggregate::prestosql::registerAllAggregateFunctions(kPrestoDefaultPrefix);
    window::prestosql::registerAllWindowFunctions(kPrestoDefaultPrefix);
    functions::prestosql::registerAllScalarFunctions(kPrestoDefaultPrefix);
  }

  void SetUp() override {
    functionMetadata_ = nativeFunctionMetadata().getFunctionsMetadata();
  }

  json functionMetadata_;
};

TEST_F(FunctionMetadataTest, approxMostFrequent) {
  testFunctionMetadata(
      functionMetadata_,
      "approx_most_frequent",
      "ApproxMostFrequent.json",
  7);
}

TEST_F(FunctionMetadataTest, arrayFrequency) {
  testFunctionMetadata(
      functionMetadata_,
      "array_frequency",
      "ArrayFrequency.json",
  10);
}

TEST_F(FunctionMetadataTest, combinations) {
  testFunctionMetadata(
      functionMetadata_,
      "combinations",
      "Combinations.json",
  11);
}

TEST_F(FunctionMetadataTest, covarSamp) {
  testFunctionMetadata(
      functionMetadata_,
      "covar_samp",
      "CovarSamp.json",
  2);
}

TEST_F(FunctionMetadataTest, elementAt) {
  testFunctionMetadata(
      functionMetadata_,
      "element_at",
      "ElementAt.json",
  3);
}

TEST_F(FunctionMetadataTest, greatest) {
  testFunctionMetadata(
      functionMetadata_,
      "greatest",
      "Greatest.json",
  15);
}

TEST_F(FunctionMetadataTest, lead) {
  testFunctionMetadata(
      functionMetadata_,
      "lead",
      "Lead.json",
  3);
}

TEST_F(FunctionMetadataTest, mod) {
  testFunctionMetadata(functionMetadata_, "mod", "Mod.json", 7);
}

TEST_F(FunctionMetadataTest, ntile) {
  testFunctionMetadata(functionMetadata_, "ntile", "Ntile.json", 1);
}

TEST_F(FunctionMetadataTest, setAgg) {
  testFunctionMetadata(functionMetadata_, "set_agg", "SetAgg.json", 1);
}

TEST_F(FunctionMetadataTest, stddevSamp) {
  testFunctionMetadata(
      functionMetadata_,
      "stddev_samp",
      "StddevSamp.json",
  5);
}

TEST_F(FunctionMetadataTest, transformKeys) {
  testFunctionMetadata(
      functionMetadata_,
      "transform_keys",
      "TransformKeys.json",
  1);
}

TEST_F(FunctionMetadataTest, variance) {
  testFunctionMetadata(functionMetadata_, "variance", "Variance.json", 5);
}

TEST_F(FunctionMetadataTest, catalog) {
  // Test with the "presto" catalog that is registered in SetUpTestSuite
  std::string catalog = "presto";
  auto metadata =
      nativeFunctionMetadata().getFunctionsMetadata(catalog);

  // The result should be a JSON object with function names as keys
  ASSERT_TRUE(metadata.is_object());
  ASSERT_FALSE(metadata.empty());

  // Verify that common functions are present
  ASSERT_TRUE(metadata.contains("abs"));
  ASSERT_TRUE(metadata.contains("mod"));

  // Each function should have an array of signatures
  for (auto it = metadata.begin(); it != metadata.end(); ++it) {
    ASSERT_TRUE(it.value().is_array()) << "Function: " << it.key();
    ASSERT_FALSE(it.value().empty()) << "Function: " << it.key();

    // Each signature should have the required fields
    for (const auto& signature : it.value()) {
      ASSERT_TRUE(signature.contains("outputType")) << "Function: " << it.key();
      ASSERT_TRUE(signature.contains("paramTypes")) << "Function: " << it.key();
      ASSERT_TRUE(signature.contains("schema")) << "Function: " << it.key();
      ASSERT_TRUE(signature.contains("functionKind"))
          << "Function: " << it.key();

      // Schema should be "default" since we registered with "presto.default."
      // prefix
      EXPECT_EQ(signature["schema"], "default") << "Function: " << it.key();
    }
  }
}

TEST_F(FunctionMetadataTest, nonExistentCatalog) {
  auto metadata =
      nativeFunctionMetadata().getFunctionsMetadata("nonexistent");

  // When no functions match, it returns a null JSON value or empty object
  // The default json() constructor creates a null value
  ASSERT_TRUE(metadata.is_null() || (metadata.is_object() && metadata.empty()));
}

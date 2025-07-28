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
#include "presto_cpp/main/types/FunctionMetadata.h"
#include "presto_cpp/main/types/tests/TestUtils.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/functions/prestosql/window/WindowFunctionsRegistration.h"

using namespace facebook::velox;
using namespace facebook::presto;

using json = nlohmann::json;

static const std::string kPrestoDefaultPrefix = "presto.default.";
static const std::string kDefaultSchema = "default";

class FunctionMetadataTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    aggregate::prestosql::registerAllAggregateFunctions(kPrestoDefaultPrefix);
    window::prestosql::registerAllWindowFunctions(kPrestoDefaultPrefix);
    functions::prestosql::registerAllScalarFunctions(kPrestoDefaultPrefix);
  }

  void SetUp() override {
    functionMetadata_ = getFunctionsMetadata();
  }

  void testFunction(
      const std::string& name,
      const std::string& expectedFile,
      size_t expectedSize) {
    json metadataList = functionMetadata_.at(name);
    EXPECT_EQ(metadataList.size(), expectedSize);
    std::string expectedStr = slurp(test::utils::getDataPath(expectedFile));
    auto expected = json::parse(expectedStr);
    auto comparator = [](const json& a, const json& b) {
      return (a["outputType"] < b["outputType"]);
    };

    json::array_t expectedList = expected[name];
    std::sort(expectedList.begin(), expectedList.end(), comparator);
    std::sort(metadataList.begin(), metadataList.end(), comparator);
    for (auto i = 0; i < expectedSize; i++) {
      EXPECT_EQ(expectedList[i], metadataList[i]) << "Position: " << i;
    }
  }

  json functionMetadata_;
};

TEST_F(FunctionMetadataTest, approxMostFrequent) {
  testFunction("approx_most_frequent", "ApproxMostFrequent.json", 7);
}

TEST_F(FunctionMetadataTest, arrayFrequency) {
  testFunction("array_frequency", "ArrayFrequency.json", 10);
}

TEST_F(FunctionMetadataTest, combinations) {
  testFunction("combinations", "Combinations.json", 11);
}

TEST_F(FunctionMetadataTest, covarSamp) {
  testFunction("covar_samp", "CovarSamp.json", 2);
}

TEST_F(FunctionMetadataTest, elementAt) {
  testFunction("element_at", "ElementAt.json", 3);
}

TEST_F(FunctionMetadataTest, greatest) {
  testFunction("greatest", "Greatest.json", 13);
}

TEST_F(FunctionMetadataTest, lead) {
  testFunction("lead", "Lead.json", 3);
}

TEST_F(FunctionMetadataTest, mod) {
  testFunction("mod", "Mod.json", 7);
}

TEST_F(FunctionMetadataTest, ntile) {
  testFunction("ntile", "Ntile.json", 1);
}

TEST_F(FunctionMetadataTest, setAgg) {
  testFunction("set_agg", "SetAgg.json", 1);
}

TEST_F(FunctionMetadataTest, stddevSamp) {
  testFunction("stddev_samp", "StddevSamp.json", 5);
}

TEST_F(FunctionMetadataTest, transformKeys) {
  testFunction("transform_keys", "TransformKeys.json", 1);
}

TEST_F(FunctionMetadataTest, variance) {
  testFunction("variance", "Variance.json", 5);
}

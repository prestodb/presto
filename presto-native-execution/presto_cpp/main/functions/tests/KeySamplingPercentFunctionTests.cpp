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
#include "presto_cpp/main/functions/registration/KeySamplingPercentFunctionRegistration.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

using namespace facebook::velox;
using namespace facebook::velox::functions::test;

namespace facebook::presto::functions::test {

class KeySamplingPercentFunctionTest : public velox::functions::test::FunctionBaseTest {
 public:
   void SetUp() override {
     facebook::presto::functions::registerKeySamplingPercentFunction("");
   }
 };

  TEST_F(KeySamplingPercentFunctionTest, keySamplingPercent) {
    const auto keySamplingPercent = [&](std::optional<std::string> string) {
      return evaluateOnce<double>("key_sampling_percent(c0)", string);
    };

    EXPECT_EQ(std::nullopt, keySamplingPercent(std::nullopt));
    EXPECT_EQ(0.56, keySamplingPercent("abc"));
    EXPECT_EQ(6.11561179120687E-153, keySamplingPercent("abcdefghskwkjadhwd"));
    EXPECT_EQ(2.393674127734674E-93, keySamplingPercent("001yxzuj"));
    EXPECT_EQ(0.48, keySamplingPercent("56wfythjhdhvgewuikwemn"));
    EXPECT_EQ(0.7520703125, keySamplingPercent("special_#@,$|%/^~?{}+-"));
    EXPECT_EQ(0.4, keySamplingPercent("     "));
    EXPECT_EQ(0.28, keySamplingPercent(""));
    EXPECT_EQ(
        4.143659858002825E-274, keySamplingPercent("Hello World from Velox!"));
  }
}
// namespace facebook::presto::functions::test
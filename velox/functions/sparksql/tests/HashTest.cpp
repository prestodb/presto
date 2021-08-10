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

#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"

#include <stdint.h>

#include "velox/functions/sparksql/Hash.h"

namespace facebook::velox::functions::sparksql::test {
namespace {

class HashTest : public SparkFunctionBaseTest {
 public:
  static void SetUpTestCase() {
    exec::registerStatefulVectorFunction("hash", hashSignatures(), makeHash);
  }

 protected:
  template <typename T>
  std::optional<int32_t> hash(std::optional<T> arg) {
    return evaluateOnce<int32_t>("hash(c0)", arg);
  }
};

TEST_F(HashTest, String) {
  EXPECT_EQ(hash<std::string>("Spark"), 228093765);
  EXPECT_EQ(hash<std::string>(""), 142593372);
  EXPECT_EQ(hash<std::string>("abcdefghijklmnopqrstuvwxyz"), -1990933474);
  // String that has a length that is a multiple of four.
  EXPECT_EQ(hash<std::string>("12345678"), 2036199019);
  EXPECT_EQ(hash<std::string>(std::nullopt), 42);
}

TEST_F(HashTest, Int64) {
  EXPECT_EQ(hash<int64_t>(0xcafecafedeadbeef), -256235155);
  EXPECT_EQ(hash<int64_t>(0xdeadbeefcafecafe), 673261790);
  EXPECT_EQ(hash<int64_t>(INT64_MAX), -1604625029);
  EXPECT_EQ(hash<int64_t>(INT64_MIN), -853646085);
  EXPECT_EQ(hash<int64_t>(1), -1712319331);
  EXPECT_EQ(hash<int64_t>(0), -1670924195);
  EXPECT_EQ(hash<int64_t>(-1), -939490007);
  EXPECT_EQ(hash<int64_t>(std::nullopt), 42);
}

TEST_F(HashTest, Int32) {
  EXPECT_EQ(hash<int32_t>(0xdeadbeef), 141248195);
  EXPECT_EQ(hash<int32_t>(0xcafecafe), 638354558);
  EXPECT_EQ(hash<int32_t>(1), -559580957);
  EXPECT_EQ(hash<int32_t>(0), 933211791);
  EXPECT_EQ(hash<int32_t>(-1), -1604776387);
  EXPECT_EQ(hash<int32_t>(std::nullopt), 42);
}

TEST_F(HashTest, Int16) {
  EXPECT_EQ(hash<int16_t>(1), -559580957);
  EXPECT_EQ(hash<int16_t>(0), 933211791);
  EXPECT_EQ(hash<int16_t>(-1), -1604776387);
  EXPECT_EQ(hash<int16_t>(std::nullopt), 42);
}

TEST_F(HashTest, Int8) {
  EXPECT_EQ(hash<int8_t>(1), -559580957);
  EXPECT_EQ(hash<int8_t>(0), 933211791);
  EXPECT_EQ(hash<int8_t>(-1), -1604776387);
  EXPECT_EQ(hash<int8_t>(std::nullopt), 42);
}

TEST_F(HashTest, Bool) {
  EXPECT_EQ(hash<bool>(false), 933211791);
  EXPECT_EQ(hash<bool>(true), -559580957);
  EXPECT_EQ(hash<bool>(std::nullopt), 42);
}

TEST_F(HashTest, StringInt32) {
  auto hash = [&](std::optional<std::string> a, std::optional<int32_t> b) {
    return evaluateOnce<int32_t>("hash(c0, c1)", a, b);
  };

  EXPECT_EQ(hash(std::nullopt, std::nullopt), 42);
  EXPECT_EQ(hash("", std::nullopt), 142593372);
  EXPECT_EQ(hash(std::nullopt, 0), 933211791);
  EXPECT_EQ(hash("", 0), 1143746540);
}

TEST_F(HashTest, Double) {
  using limits = std::numeric_limits<double>;

  EXPECT_EQ(hash<double>(std::nullopt), 42);
  EXPECT_EQ(hash<double>(-0.0), -1670924195);
  EXPECT_EQ(hash<double>(0), -1670924195);
  EXPECT_EQ(hash<double>(1), -460888942);
  EXPECT_EQ(hash<double>(limits::quiet_NaN()), -1281358385);
  EXPECT_EQ(hash<double>(limits::infinity()), 833680482);
  EXPECT_EQ(hash<double>(-limits::infinity()), 461104036);
}

TEST_F(HashTest, Float) {
  using limits = std::numeric_limits<float>;

  EXPECT_EQ(hash<float>(std::nullopt), 42);
  EXPECT_EQ(hash<float>(-0.0f), 933211791);
  EXPECT_EQ(hash<float>(0), 933211791);
  EXPECT_EQ(hash<float>(1), -466301895);
  EXPECT_EQ(hash<float>(limits::quiet_NaN()), -349261430);
  EXPECT_EQ(hash<float>(limits::infinity()), 2026854605);
  EXPECT_EQ(hash<float>(-limits::infinity()), 427440766);
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test

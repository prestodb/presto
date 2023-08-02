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

#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"

#include <stdint.h>

namespace facebook::velox::functions::sparksql::test {
namespace {
class XxHash64Test : public SparkFunctionBaseTest {
 protected:
  template <typename T>
  std::optional<int64_t> xxhash64(std::optional<T> arg) {
    return evaluateOnce<int64_t>("xxhash64(c0)", arg);
  }
};

// The expected result was obtained by running SELECT xxhash64("Spark") query
// using spark-sql CLI.
TEST_F(XxHash64Test, varchar) {
  EXPECT_EQ(xxhash64<std::string>("Spark"), -4294468057691064905);
  EXPECT_EQ(xxhash64<std::string>(""), -7444071767201028348);
  EXPECT_EQ(
      xxhash64<std::string>("abcdefghijklmnopqrstuvwxyz"),
      -3265757659154784300);
  // String that has a length that is a multiple of four.
  EXPECT_EQ(xxhash64<std::string>("12345678"), 6863040065134489090);
  EXPECT_EQ(
      xxhash64<std::string>("12345678djdejidecjjeijcneknceincne"),
      -633855189410948723);
  EXPECT_EQ(xxhash64<std::string>(std::nullopt), 42);
}

TEST_F(XxHash64Test, int64) {
  EXPECT_EQ(xxhash64<int64_t>(0xcafecafedeadbeef), -6259772178006417012);
  EXPECT_EQ(xxhash64<int64_t>(0xdeadbeefcafecafe), -1700188678616701932);
  EXPECT_EQ(xxhash64<int64_t>(INT64_MAX), -3246596055638297850);
  EXPECT_EQ(xxhash64<int64_t>(INT64_MIN), -8619748838626508300);
  EXPECT_EQ(xxhash64<int64_t>(1), -7001672635703045582);
  EXPECT_EQ(xxhash64<int64_t>(0), -5252525462095825812);
  EXPECT_EQ(xxhash64<int64_t>(-1), 3858142552250413010);
  EXPECT_EQ(xxhash64<int64_t>(std::nullopt), 42);
}

TEST_F(XxHash64Test, int32) {
  EXPECT_EQ(xxhash64<int32_t>(0xdeadbeef), -8041005359684616715);
  EXPECT_EQ(xxhash64<int32_t>(0xcafecafe), 3599843564351570672);
  EXPECT_EQ(xxhash64<int32_t>(1), -6698625589789238999);
  EXPECT_EQ(xxhash64<int32_t>(0), 3614696996920510707);
  EXPECT_EQ(xxhash64<int32_t>(-1), 2017008487422258757);
  EXPECT_EQ(xxhash64<int32_t>(std::nullopt), 42);
}

TEST_F(XxHash64Test, int16) {
  EXPECT_EQ(xxhash64<int16_t>(1), -6698625589789238999);
  EXPECT_EQ(xxhash64<int16_t>(0), 3614696996920510707);
  EXPECT_EQ(xxhash64<int16_t>(-1), 2017008487422258757);
  EXPECT_EQ(xxhash64<int16_t>(std::nullopt), 42);
}

TEST_F(XxHash64Test, int8) {
  EXPECT_EQ(xxhash64<int8_t>(1), -6698625589789238999);
  EXPECT_EQ(xxhash64<int8_t>(0), 3614696996920510707);
  EXPECT_EQ(xxhash64<int8_t>(-1), 2017008487422258757);
  EXPECT_EQ(xxhash64<int8_t>(std::nullopt), 42);
}

TEST_F(XxHash64Test, bool) {
  EXPECT_EQ(xxhash64<bool>(false), 3614696996920510707);
  EXPECT_EQ(xxhash64<bool>(true), -6698625589789238999);
  EXPECT_EQ(xxhash64<bool>(std::nullopt), 42);
}

TEST_F(XxHash64Test, varcharInt32) {
  auto xxhash64 = [&](std::optional<std::string> a, std::optional<int32_t> b) {
    return evaluateOnce<int64_t>("xxhash64(c0, c1)", a, b);
  };

  EXPECT_EQ(xxhash64(std::nullopt, std::nullopt), 42);
  EXPECT_EQ(xxhash64("", std::nullopt), -7444071767201028348);
  EXPECT_EQ(xxhash64(std::nullopt, 0), 3614696996920510707);
  EXPECT_EQ(xxhash64("", 0), 5333022629466737987);
}

TEST_F(XxHash64Test, double) {
  using limits = std::numeric_limits<double>;

  EXPECT_EQ(xxhash64<double>(std::nullopt), 42);
  EXPECT_EQ(xxhash64<double>(-0.0), -5252525462095825812);
  EXPECT_EQ(xxhash64<double>(0), -5252525462095825812);
  EXPECT_EQ(xxhash64<double>(1), -2162451265447482029);
  EXPECT_EQ(xxhash64<double>(limits::quiet_NaN()), -3127944061524951246);
  EXPECT_EQ(xxhash64<double>(limits::infinity()), 5810986238603807492);
  EXPECT_EQ(xxhash64<double>(-limits::infinity()), 5326262080505358431);
}

TEST_F(XxHash64Test, float) {
  using limits = std::numeric_limits<float>;

  EXPECT_EQ(xxhash64<float>(std::nullopt), 42);
  EXPECT_EQ(xxhash64<float>(-0.0f), 3614696996920510707);
  EXPECT_EQ(xxhash64<float>(0), 3614696996920510707);
  EXPECT_EQ(xxhash64<float>(1), 700633588856507837);
  EXPECT_EQ(xxhash64<float>(limits::quiet_NaN()), 2692338816207849720);
  EXPECT_EQ(xxhash64<float>(limits::infinity()), -5940311692336719973);
  EXPECT_EQ(xxhash64<float>(-limits::infinity()), -7580553461823983095);
}

TEST_F(XxHash64Test, hashSeed) {
  auto xxhash64WithSeed = [&](int64_t seed, const std::optional<int64_t>& arg) {
    return evaluateOnce<int64_t>(
        fmt::format("xxhash64_with_seed({}, c0)", seed), arg);
  };

  static const auto kIntMin = std::numeric_limits<int32_t>::min();
  static const auto kIntMax = std::numeric_limits<int32_t>::max();
  static const auto kLongMin = std::numeric_limits<int64_t>::min();
  static const auto kLongMax = std::numeric_limits<int64_t>::max();

  EXPECT_EQ(xxhash64WithSeed(0L, 42), -5379971487550586029);
  EXPECT_EQ(xxhash64WithSeed(kLongMin, 42), -6671470883434376173);
  EXPECT_EQ(xxhash64WithSeed(kLongMax, 42), 4605443450566835086);
  EXPECT_EQ(
      xxhash64WithSeed(static_cast<int64_t>(kIntMin) - 1L, 42),
      8765374525824963196);
  EXPECT_EQ(
      xxhash64WithSeed(static_cast<int64_t>(kIntMax) + 1L, 42),
      8810073187160811495);
}

// The expected result was obtained by running SELECT xxhash64("hello", "world")
// query using spark-sql CLI.
TEST_F(XxHash64Test, hashSeedVarcharArgs) {
  auto xxhash64WithSeed = [&](int64_t seed,
                              const std::optional<std::string>& arg1,
                              const std::optional<std::string>& arg2) {
    return evaluateOnce<int64_t>(
        fmt::format("xxhash64_with_seed({}, c0, c1)", seed), arg1, arg2);
  };

  EXPECT_EQ(xxhash64WithSeed(42L, "hello", "world"), 7824066149349576922);
  EXPECT_EQ(xxhash64WithSeed(42L, "hello", std::nullopt), -4367754540140381902);
  EXPECT_EQ(xxhash64WithSeed(42L, "hello", ""), -5179011742163812830);
  EXPECT_EQ(xxhash64WithSeed(0L, std::nullopt, "hello"), 2794345569481354659);
  EXPECT_EQ(xxhash64WithSeed(0L, "", "hello"), 1992633642622160295);
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test

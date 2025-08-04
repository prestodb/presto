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

#include "velox/functions/lib/QuantileDigest.h"

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/testutil/RandomSeed.h"
#include "velox/functions/lib/tests/QuantileDigestTestBase.h"

using namespace facebook::velox::functions::qdigest;

namespace facebook::velox::functions {

class QuantileDigestTest : public QuantileDigestTestBase {
 protected:
  memory::MemoryPool* pool() {
    return pool_.get();
  }

  HashStringAllocator* allocator() {
    return &allocator_;
  }

  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  std::string encodeBase64(std::string_view input) {
    return folly::base64Encode(input);
  }

  template <typename T>
  void testQuantiles(
      const std::vector<T>& values,
      const std::vector<double>& quantiles,
      const std::vector<T>& expected,
      const std::vector<double>& weight = {}) {
    ASSERT_EQ(quantiles.size(), expected.size());
    if (!weight.empty()) {
      ASSERT_EQ(values.size(), weight.size());
    }

    QuantileDigest<T> digest{StlAllocator<T>(allocator()), 0.99};
    for (auto i = 0; i < values.size(); ++i) {
      if (weight.empty()) {
        digest.add(values[i], 1.0);
      } else {
        digest.add(values[i], weight[i]);
      }
    }

    std::vector<T> results(quantiles.size());
    digest.estimateQuantiles(quantiles, results.data());
    for (auto i = 0; i < quantiles.size(); ++i) {
      EXPECT_EQ(results[i], expected[i]);
    }
  }

  template <typename T>
  void testHugeWeight() {
    constexpr int N = 10;
    constexpr double kAccuracy = 0.99;
    constexpr double kMaxValue = std::numeric_limits<double>::max();
    QuantileDigest<T> digest{StlAllocator<T>(allocator()), kAccuracy};
    for (auto i = 0; i < N; ++i) {
      digest.add(T(i), kMaxValue);
    }
  }

  template <typename T>
  void testQuantileAtValueWithData(
      const std::vector<T>& inputValues,
      const std::vector<std::pair<T, double>>& testCases,
      const std::vector<T>& outOfRangeValues) {
    constexpr double kAccuracy = 1.0E-2;
    constexpr double kTolerance = 1.0E-3;

    QuantileDigest<T> digest{StlAllocator<T>(allocator()), kAccuracy};
    for (const auto& value : inputValues) {
      digest.add(value, 1.0);
    }

    for (const auto& [testValue, expectedQuantile] : testCases) {
      EXPECT_NEAR(
          *digest.quantileAtValue(testValue), expectedQuantile, kTolerance);
    }

    for (const auto& outOfRangeValue : outOfRangeValues) {
      EXPECT_FALSE(digest.quantileAtValue(outOfRangeValue).has_value());
    }
  }

  template <typename T>
  void testQuantileAtValueWithDigest(
      const QuantileDigest<T>& digest,
      const std::vector<T>& testData,
      const std::vector<std::optional<double>>& expectedData,
      const double tokerance) {
    for (auto i = 0; i < testData.size(); ++i) {
      if (expectedData[i].has_value()) {
        EXPECT_NEAR(
            *digest.quantileAtValue(testData[i]),
            expectedData[i].value(),
            tokerance);
      } else {
        EXPECT_FALSE(digest.quantileAtValue(testData[i]).has_value());
      }
    }
  }

  // A helper function to get the expected quantile at value for
  // qdigest comes from a query "SELECT QDIGEST_AGG(CAST(c0 AS BIGINT), 1, 0.99)
  // FROM UNNEST(SEQUENCE(-5000, 4999)) AS t(c0)".
  template <typename T>
  std::vector<std::optional<double>> getExpectedQuantileAtValue() {
    // Expected quantile values for different data types at specific test points
    // Format: {int64_t_result, double_result, float_result} // test_value
    // std::nullopt indicates the value is out of digest range
    std::vector<std::vector<std::optional<double>>> expectedDatas{
        {std::nullopt, std::nullopt, std::nullopt}, // -5001 (out of range)
        {0, 0, std::nullopt}, // -5000 (min boundary)
        {0, 0, std::nullopt}, // -4996
        {0.0302, 0.0128, std::nullopt}, // -4990
        {0.0302, 0.0128, std::nullopt}, // -4980
        {0.0302, 0.0217, 0}, // -4850
        {0.2884, 0.2789, 0.2919}, // -2000
        {0.5, 0.4925, 0.5}, //     0 (median)
        {0.6718, 0.6492, 0.6567}, //  2000
        {0.7099, 0.6873, 0.6948}, //  2111
        {0.8017, 0.7791, 0.7866}, //  3111
        {0.9019, 0.8687, 0.8762}, //  4111
        {0.9645, 0.9313, 0.9388}, //  4879
        {std::nullopt, std::nullopt, std::nullopt}, //  4880 (out of range)
        {std::nullopt, std::nullopt, std::nullopt}, //  5000 (out of range)
    };

    std::vector<std::optional<double>> result;
    constexpr int typeIndex = std::is_same_v<T, int64_t> ? 0
        : std::is_same_v<T, double>                      ? 1
                                                         : 2;

    result.reserve(expectedDatas.size());
    for (const auto& row : expectedDatas) {
      result.push_back(row[typeIndex]);
    }
    return result;
  }

  template <typename T>
  void testQuantileAtValue() {
    constexpr double kAccuracy = 1.0E-2;
    constexpr double kTolerance = 1.0E-3;

    // Test: Histogram-equivalent test with values 0-9 (from Java test)
    {
      QuantileDigest<T> digest{StlAllocator<T>(allocator()), 1.0};
      for (int i = 0; i < 10; ++i) {
        digest.add(static_cast<T>(i), 1.0);
      }

      for (int i = 0; i <= 9; ++i) {
        auto q = digest.quantileAtValue(static_cast<T>(i));
        EXPECT_TRUE(q.has_value());
        EXPECT_NEAR(q.value(), i / 10.0, kTolerance);
      }

      // Values outside the range [0, 9] should return no value
      EXPECT_FALSE(digest.quantileAtValue(static_cast<T>(-1)).has_value());
      EXPECT_FALSE(digest.quantileAtValue(static_cast<T>(10)).has_value());
      EXPECT_FALSE(
          digest.quantileAtValue(std::numeric_limits<T>::max()).has_value());
    }

    // Test: Basic weighted test
    {
      QuantileDigest<T> digest{StlAllocator<T>(allocator()), kAccuracy};
      digest.add(static_cast<T>(1), 1.0);
      digest.add(static_cast<T>(2), 2.0);
      digest.add(static_cast<T>(3), 3.0);
      digest.add(static_cast<T>(4), 4.0);
      EXPECT_NEAR(*digest.quantileAtValue(static_cast<T>(1)), 0.0, kTolerance);
      EXPECT_NEAR(*digest.quantileAtValue(static_cast<T>(2)), 0.1, kTolerance);
      EXPECT_NEAR(*digest.quantileAtValue(static_cast<T>(3)), 0.3, kTolerance);
      EXPECT_NEAR(*digest.quantileAtValue(static_cast<T>(4)), 0.6, kTolerance);
    }

    // Test: Values from -4999 to 4999 to cover both negative and positive
    // QDigest
    {
      std::vector<T> inputValues;
      for (int64_t i = -4999; i <= 4999; i++) {
        inputValues.push_back(static_cast<T>(i));
      }

      std::vector<std::pair<T, double>> testCases = {
          {static_cast<T>(-4996), 0.0},
          {static_cast<T>(-4995), 0.0002},
          {static_cast<T>(-4994), 0.0002},
          {static_cast<T>(-4990), 0.0006},
          {static_cast<T>(-4980), 0.0016},
          {static_cast<T>(-2000), 0.2998},
          {static_cast<T>(0), 0.4999},
          {static_cast<T>(2000), 0.6999},
          {static_cast<T>(4980), 0.9979},
          {static_cast<T>(4990), 0.9989},
          {static_cast<T>(4994), 0.9993},
          {static_cast<T>(4995), 0.9994},
          {static_cast<T>(4996), 0.9995},
          {static_cast<T>(4997), 0.9996},
          {static_cast<T>(4997), 0.9998},
      };

      std::vector<T> outOfRangeValues = {
          static_cast<T>(-5000), static_cast<T>(5000)};

      testQuantileAtValueWithData(inputValues, testCases, outOfRangeValues);
    }

    // Additional floating point test with fractional values
    if constexpr (std::is_floating_point_v<T>) {
      std::vector<T> inputValues;
      for (int i = -4999; i <= 4999; i++) {
        inputValues.push_back(static_cast<T>(i + 0.1 * i));
      }

      std::vector<std::pair<T, double>> testCases = {
          {static_cast<T>(-5498), 0.0001},
          {static_cast<T>(-5493.99), 0.0005},
          {static_cast<T>(-5491.99), 0.0007},
          {static_cast<T>(-5488.99), 0.0010},
          {static_cast<T>(0.0), 0.4999},
          {static_cast<T>(100.001), 0.5090},
          {static_cast<T>(200.001), 0.5181},
          {static_cast<T>(1999.009), 0.6817},
          {static_cast<T>(4999.009), 0.9544},
          {static_cast<T>(5497.999), 0.9998},
      };

      std::vector<T> outOfRangeValues = {
          static_cast<T>(-5500), static_cast<T>(5500.0)};

      testQuantileAtValueWithData(inputValues, testCases, outOfRangeValues);
    }

    // Test with qdigest directly deserialized from FROM_BASE64 string
    {
      // Test the quantile at value for below data points
      std::vector<T> testData = {
          static_cast<T>(-5001),
          static_cast<T>(-5000),
          static_cast<T>(-4996),
          static_cast<T>(-4990),
          static_cast<T>(-4980),
          static_cast<T>(-4850),
          static_cast<T>(-2000),
          static_cast<T>(0),
          static_cast<T>(2000),
          static_cast<T>(2111),
          static_cast<T>(3111),
          static_cast<T>(4111),
          static_cast<T>(4879),
          static_cast<T>(4880),
          static_cast<T>(5000),
      };
      std::string serializeStr = []() {
        if constexpr (std::is_same_v<T, int64_t>) {
          return decodeBase64(
              "AK5H4XoUru8/AAAAAAAAAAAAAAAAAAAAAHjs////////hxMAAAAAAABUAAAAFAAAAAAA4HJAeOz//////38MAAAAAACAWUAa7v//////fxAAAAAAAABnQAHv//////9/IwAAAAAAwGNAGu7//////38nAAAAAAAAAAB47P//////fxAAAAAAAIBtQBTw//////9/EAAAAAAAAGFADPL//////38QAAAAAAAAb0AI8///////fyMAAAAAAAAAAAzy//////9/JwAAAAAAAHFAFPD//////38UAAAAAABgdEAg9P//////fxQAAAAAAKBzQAr2//////9/JwAAAAAAgGdAIPT//////38rAAAAAACAZEAU8P//////fxAAAAAAAMBvQAL4//////9/EAAAAAAAAHBAAPn//////38jAAAAAAAAAAAC+P//////fyUAAAAAAIBvQAL4//////9/EAAAAAAAgGlANP3//////38AAAAAAAAAAED+////////fx4AAAAAAABlQFb///////9/IgAAAAAAAHVABv7//////38nAAAAAADAckAO/P//////fysAAAAAACBxQAL4//////9/LwAAAAAAAAAAFPD//////38zAAAAAABAdEB47P//////fwgAAAAAAABNQEYAAAAAAACADAAAAAAAQF5AhwAAAAAAAIAfAAAAAAAAAAAAAAAAAAAAgAwAAAAAAABgQAABAAAAAACADAAAAAAAAGBAgAEAAAAAAIAfAAAAAAAAAAAAAQAAAAAAgCMAAAAAAAAAAAAAAAAAAACADAAAAAAAAGBAAAIAAAAAAIAMAAAAAAAAYECAAgAAAAAAgB8AAAAAAAAAAAACAAAAAACAIQAAAAAAQFtAAAIAAAAAAIAnAAAAAABgYkAAAAAAAAAAgAwAAAAAAABgQAAEAAAAAACADAAAAAAAAGBAgAQAAAAAAIAfAAAAAAAAAAAABAAAAAAAgAwAAAAAAEBZQJsFAAAAAACAIwAAAAAAQFBAAAQAAAAAAIAMAAAAAAAAYEAABgAAAAAAgAwAAAAAAABgQIAGAAAAAACAHwAAAAAAAAAAAAYAAAAAAIAMAAAAAABAV0CjBwAAAAAAgB4AAAAAAEBQQGIHAAAAAACAIwAAAAAAgFhAAAYAAAAAAIAnAAAAAACAVkAABAAAAAAAgCsAAAAAAAAAAAAAAAAAAACADAAAAAAAAGBAAAgAAAAAAIAMAAAAAAAAYECACAAAAAAAgB8AAAAAAAAAAAAIAAAAAACAIQAAAAAAwGJAAAgAAAAAAIAMAAAAAAAAYEAACgAAAAAAgAwAAAAAAABgQIAKAAAAAACAHwAAAAAAAAAAAAoAAAAAAIAQAAAAAADAYUByCwAAAAAAgCMAAAAAAIBcQAAKAAAAAACAJwAAAAAAAAAAAAgAAAAAAIAMAAAAAAAAYEAADAAAAAAAgAwAAAAAAABgQIAMAAAAAACAHwAAAAAAAAAAAAwAAAAAAIAhAAAAAADAYEAADAAAAAAAgAwAAAAAAABgQAAOAAAAAACADAAAAAAAAGBAgA4AAAAAAIAfAAAAAAAAAAAADgAAAAAAgAwAAAAAAIBfQIIPAAAAAACAIwAAAAAAQGBAAA4AAAAAAIAnAAAAAAAAAAAADAAAAAAAgCsAAAAAAIBeQAAIAAAAAACALwAAAAAAgFpAAAAAAAAAAIAMAAAAAACAXkCGEAAAAAAAgAwAAAAAAIBdQIoRAAAAAACAHgAAAAAAQGBACBEAAAAAAIAjAAAAAAAAAAAEEAAAAAAAgAwAAAAAAIBcQI4SAAAAAACAHgAAAAAAwGFAABIAAAAAAIAQAAAAAAAAYUAAEwAAAAAAgCMAAAAAAAAAAAASAAAAAACAJwAAAAAAQGFABBAAAAAAAIAzAAAAAABAVEAAAAAAAAAAgP8AAAAAAAAAAHjs//////9/");
        }
        if constexpr (std::is_same_v<T, double>) {
          return decodeBase64(
              "AK5H4XoUru8/AAAAAAAAAAAAAAAAAAAAAP//////d0y/AAAAAACHs0B0AAAAsAAAAAAAAGBA//////93TD+sAAAAAABAVkD//////wBNP6wAAAAAAABUQP//////gE0/vwAAAAAAwFRA//////8ATT/DAAAAAAAAAAD//////3dMP6wAAAAAAABUQP//////gE4/sAAAAAAAAGBA//////8ATz/DAAAAAAAAAAD//////wBOP8cAAAAAAABaQP//////d0w/sAAAAAAAAFhA//////8BUD+wAAAAAAAAUED//////wFRP8MAAAAAAAAAAP//////AVA/rAAAAAAAAFBA//////8BUj+wAAAAAAAAWED//////wFTP8MAAAAAAAAAAP//////AVI/xwAAAAAA4GBA//////8BUD+0AAAAAAAAYED//////wFUP7AAAAAAAMBfQP//////A1Y/rAAAAAAAAFBA//////8BVz/DAAAAAAAAAAD//////wNWP8cAAAAAAIBcQP//////AVQ/ywAAAAAAQFVA//////8BUD+0AAAAAAAAYED//////wFYP6wAAAAAAABQQP//////AVo/rAAAAAAAAFBA//////+BWz/DAAAAAAAAWUD//////wFaP8cAAAAAAABVQP//////AVg/tAAAAAAAoGJA//////9XXD+wAAAAAADAVkD//////wFfP8IAAAAAAMBcQP//////B14/xwAAAAAAAFZA//////9XXD/LAAAAAABAV0D//////wFYP88AAAAAAAAAAP//////AVA/0wAAAAAAgGJA//////93TD+0AAAAAAAAV0D//////wNkP7QAAAAAAIBbQP//////S2Y/xwAAAAAAAAAA//////8DZD/KAAAAAAAAYED//////0dgP7QAAAAAAABgQP//////A2g/xQAAAAAAwFZA//////8DaD+0AAAAAADAX0D//////wduP8YAAAAAAIBbQP//////T2w/ywAAAAAAAAAA//////8DaD/PAAAAAACAXUD//////0dgP7gAAAAAAABgQP//////B3A/uAAAAAAAAGBA//////8HeD+4AAAAAABAVkD//////z99P8sAAAAAAAAAAP//////B3g/zwAAAAAAAFdA//////8HcD/TAAAAAABgYED//////0dgP9cAAAAAAIBYQP//////d0w/vAAAAAAAAGBA//////8PiD/AAAAAAAAAYED//////x+QP9MAAAAAAIBcQP//////74A/xAAAAAAAAFhA//////8/oD/XAAAAAAAAAAD//////++AP98AAAAAAABZQP//////d0w/vAAAAAAAAExAAAAAAAAAacC4AAAAAAAAS0AAAAAAAKB0wLwAAAAAAEBdQAAAAAAAsHjAzwAAAAAAAFNAAAAAAACQcMDTAAAAAACAUkAAAAAAAOBgwLQAAAAAAABJQAAAAAAAcILAuAAAAAAAQFxAAAAAAAB4hMDLAAAAAAAAAAAAAAAAAGiAwLgAAAAAAABgQAAAAAAAAIjAuAAAAAAAAGBAAAAAAAAAjMDLAAAAAAAAAAAAAAAAAACIwM8AAAAAAAAAAAAAAAAAaIDAtAAAAAAAAGBAAAAAAAAAkMC0AAAAAAAAYEAAAAAAAACSwMcAAAAAAAAAAAAAAAAAAJDAtAAAAAAAQFlAAAAAAABslsDLAAAAAABAUEAAAAAAAACQwLQAAAAAAABgQAAAAAAAAJjAtAAAAAAAAGBAAAAAAAAAmsDHAAAAAAAAAAAAAAAAAACYwLQAAAAAAEBXQAAAAAAAjJ7AxgAAAAAAQFBAAAAAAACIncDLAAAAAACAWEAAAAAAAACYwM8AAAAAAAAAAAAAAAAAAJDA0wAAAAAAgFZAAAAAAABogMCwAAAAAAAAYEAAAAAAAACgwLAAAAAAAABgQAAAAAAAAKHAwwAAAAAAAAAAAAAAAAAAoMDFAAAAAADAYkAAAAAAAACgwLAAAAAAAABgQAAAAAAAAKTAsAAAAAAAAGBAAAAAAAAApcDDAAAAAAAAAAAAAAAAAACkwLQAAAAAAMBhQAAAAAAA5KbAxwAAAAAAgFxAAAAAAAAApMDLAAAAAAAAAAAAAAAAAACgwLAAAAAAAABgQAAAAAAAAKjAsAAAAAAAAGBAAAAAAAAAqcDDAAAAAAAAAAAAAAAAAACowMUAAAAAAMBgQAAAAAAAAKjAsAAAAAAAAGBAAAAAAAAArMCwAAAAAAAAYEAAAAAAAACtwMMAAAAAAAAAAAAAAAAAAKzAsAAAAAAAgF9AAAAAAAAEr8DHAAAAAABAYEAAAAAAAACswMsAAAAAAAAAAAAAAAAAAKjAzwAAAAAAgF5AAAAAAAAAoMCsAAAAAACAXkAAAAAAAIawwKwAAAAAAIBdQAAAAAAAirHAvgAAAAAAQGBAAAAAAAAIscDDAAAAAAAAAAAAAAAAAASwwKwAAAAAAIBcQAAAAAAAjrLAvgAAAAAAwGFAAAAAAAAAssCwAAAAAAAAYUAAAAAAAACzwMMAAAAAAAAAAAAAAAAAALLAxwAAAAAAQGFAAAAAAAAEsMDTAAAAAACAW0AAAAAAAACgwNcAAAAAAABUQAAAAAAAaIDA3wAAAAAAgFNAAAAAAACAUcD/AAAAAAAgYkD//////3dMPw==");
        }
        return decodeBase64(
            "AK5H4XoUru8/AAAAAAAAAAAAAAAAAAAAAP+/Y7r/////ADicRQAAAABuAAAAPAAAAAAA4GJA/wdouv///39OAAAAAAAAZUD/v2O6////fzwAAAAAAEBiQP8fcLr///9/PAAAAAAAIGNA/wd4uv///39PAAAAAACAYkD/H3C6////f1MAAAAAAMBbQP+/Y7r///9/QAAAAAAAwGJA/w+Auv///388AAAAAACAXED/75C6////f1MAAAAAAMBhQP8PgLr///9/PAAAAAAAAGBA/w+guv///388AAAAAACAX0D/L7C6////fzgAAAAAAABLQP+vvLr///9/TwAAAAAAAAAA/y+wuv///39TAAAAAABgYkD/D6C6////f1cAAAAAAMBaQP8PgLr///9/PAAAAAAAwFpA/1/Buv///388AAAAAAAAYED/D8i6////f08AAAAAAAAAAP9fwbr///9/UQAAAAAAwGRA/1/Buv///388AAAAAADAXUD/n+C6////fzwAAAAAAABgQP8P6Lr///9/TwAAAAAAAAAA/5/guv///39AAAAAAAAgY0D/f/a6////f1MAAAAAAMBZQP+f4Lr///9/VwAAAAAAAFVA/1/Buv///39bAAAAAACAVUD/D4C6////f18AAAAAAAAAAP+/Y7r///9/QAAAAAAAQFxA//8hu////39AAAAAAAAAYED/HzC7////f1MAAAAAAAAAAP//Ibv///9/VgAAAAAAQFZA//8Hu////39AAAAAAAAAYED/H0C7////f0AAAAAAAABgQP8fULv///9/UwAAAAAAAAAA/x9Au////39EAAAAAAAgYED//2+7////f1cAAAAAAMBfQP8fQLv///9/WwAAAAAAAAAA//8Hu////39EAAAAAABAUkD//627////f0QAAAAAAMBXQP9/6Lv///9/VgAAAAAAgGNA/3/Bu////39bAAAAAAAAAAD//4a7////f18AAAAAACBkQP//B7v///9/YwAAAAAAQGJA/79juv///39IAAAAAABAWED//0+8////f0wAAAAAAEBcQP//j7z///9/XwAAAAAAYGNA//8JvP///39QAAAAAAAAWED//wG9////f2MAAAAAAAAAAP//Cbz///9/ZQAAAAAAAD5A//8JvP///39rAAAAAAAAAAD/v2O6////f3kAAAAAAEBdQP+/Y7r///9/SAAAAAAAAExAAABIQwAAAIBEAAAAAAAAS0AAAKVDAAAAgEgAAAAAAEBdQACAxUMAAACAWwAAAAAAAFNAAICEQwAAAIBfAAAAAACAUkAAAAdDAAAAgEAAAAAAAABJQACAE0QAAACARAAAAAAAQFxAAMAjRAAAAIBXAAAAAAAAAAAAQANEAAAAgEQAAAAAAABgQAAAQEQAAACARAAAAAAAAGBAAABgRAAAAIBXAAAAAAAAAAAAAEBEAAAAgFsAAAAAAAAAAABAA0QAAACAQAAAAAAAAGBAAACARAAAAIBAAAAAAAAAYEAAAJBEAAAAgFMAAAAAAAAAAAAAgEQAAACAQAAAAAAAQFlAAGCzRAAAAIBXAAAAAABAUEAAAIBEAAAAgEAAAAAAAABgQAAAwEQAAACAQAAAAAAAAGBAAADQRAAAAIBTAAAAAAAAAAAAAMBEAAAAgEAAAAAAAEBXQABg9EQAAACAUgAAAAAAQFBAAEDsRAAAAIBXAAAAAACAWEAAAMBEAAAAgFsAAAAAAAAAAAAAgEQAAACAXwAAAAAAgFZAAEADRAAAAIA8AAAAAAAAYEAAAABFAAAAgDwAAAAAAABgQAAACEUAAACATwAAAAAAAAAAAAAARQAAAIBRAAAAAADAYkAAAABFAAAAgDwAAAAAAABgQAAAIEUAAACAPAAAAAAAAGBAAAAoRQAAAIBPAAAAAAAAAAAAACBFAAAAgEAAAAAAAMBhQAAgN0UAAACAUwAAAAAAgFxAAAAgRQAAAIBXAAAAAAAAAAAAAABFAAAAgDwAAAAAAABgQAAAQEUAAACAPAAAAAAAAGBAAABIRQAAAIBPAAAAAAAAAAAAAEBFAAAAgFEAAAAAAMBgQAAAQEUAAACAPAAAAAAAAGBAAABgRQAAAIA8AAAAAAAAYEAAAGhFAAAAgE8AAAAAAAAAAAAAYEUAAACAPAAAAAAAgF9AACB4RQAAAIBTAAAAAABAYEAAAGBFAAAAgFcAAAAAAAAAAAAAQEUAAACAWwAAAAAAgF5AAAAARQAAAIA4AAAAAACAXkAAMIRFAAAAgDgAAAAAAIBdQABQjEUAAACASgAAAAAAQGBAAECIRQAAAIBPAAAAAAAAAAAAIIBFAAAAgDgAAAAAAIBcQABwlEUAAACASgAAAAAAwGFAAACQRQAAAIA8AAAAAAAAYUAAAJhFAAAAgE8AAAAAAAAAAAAAkEUAAACAUwAAAAAAQGFAACCARQAAAIBfAAAAAACAW0AAAABFAAAAgGMAAAAAAABUQABAA0QAAACAawAAAAAAgFNAAACMQgAAAID/AAAAAACAUUD/v2O6////fw==");
      }();

      QuantileDigest<T> digest{
          StlAllocator<T>(allocator()), serializeStr.data()};
      const auto expectedData = getExpectedQuantileAtValue<T>();

      testQuantileAtValueWithDigest(digest, testData, expectedData, kTolerance);
    }

    // Test with empty digest
    {
      QuantileDigest<T> digest{StlAllocator<T>(allocator()), kAccuracy};
      EXPECT_FALSE(digest.quantileAtValue(static_cast<T>(1)).has_value());
    }

    // Test: Inverse relationship with estimateQuantile
    if constexpr (std::is_floating_point_v<T>) {
      QuantileDigest<T> digest{StlAllocator<T>(allocator()), kAccuracy};
      for (int i = -10000; i <= 10000; ++i) {
        digest.add(static_cast<T>(i), 1.0);
      }

      std::vector<double> quantiles = {
          0.01, 0.0011, 0.02, 0.25, 0.5, 0.75, 0.9, 0.98, 0.99, 0.999, 1.0};
      for (double q : quantiles) {
        T value = digest.estimateQuantile(q);
        auto inverseQ = digest.quantileAtValue(value);
        EXPECT_TRUE(inverseQ.has_value());
        EXPECT_NEAR(*inverseQ, q, kTolerance);
      }
    }

    // Test Inverse relationship with estimateQuantile with random weights
    if constexpr (std::is_floating_point_v<T>) {
      QuantileDigest<T> digest{StlAllocator<T>(allocator()), kAccuracy};
      std::default_random_engine gen(common::testutil::getRandomSeed(42));
      std::uniform_real_distribution<T> dist;
      for (int i = -10000; i <= 10000; ++i) {
        auto weight = dist(gen);
        digest.add(static_cast<T>(i), weight);
      }

      std::vector<double> quantiles = {
          0.01, 0.0011, 0.02, 0.25, 0.5, 0.75, 0.9, 0.98, 0.99, 0.999, 1.0};
      for (double q : quantiles) {
        T value = digest.estimateQuantile(q);
        auto inverseQ = digest.quantileAtValue(value);
        EXPECT_TRUE(inverseQ.has_value());
        EXPECT_NEAR(*inverseQ, q, kTolerance);
      }
    }
  }

  template <typename T>
  void testLargeInputSize() {
    constexpr int N = 1e5;
    constexpr double kAccuracy = 0.8;
    std::vector<double> values;
    QuantileDigest<T> digest{StlAllocator<T>(allocator()), kAccuracy};
    std::default_random_engine gen(common::testutil::getRandomSeed(42));
    std::uniform_real_distribution<T> dist{-10.0, 10.0};
    for (int i = 0; i < N; ++i) {
      auto v = dist(gen);
      digest.add(v, 1.0);
      values.push_back(v);
    }
    std::sort(std::begin(values), std::end(values));
    checkQuantiles<QuantileDigest<T>, false>(
        values, digest, 0.0, values.size() * kAccuracy);

    values.clear();
    QuantileDigest<T> digestWeighted{StlAllocator<T>(allocator()), 0.8};
    for (int i = 0; i < N; ++i) {
      auto v = dist(gen);
      digest.add(v, i % 7 + 1);
      values.insert(values.end(), i % 7 + 1, v);
    }
    std::sort(std::begin(values), std::end(values));
    checkQuantiles<QuantileDigest<T>, false>(
        values, digest, 0.0, values.size() * kAccuracy);
  }

  template <typename T>
  void testEquivalentMerge() {
    constexpr double kAccuracy = 0.5;
    QuantileDigest<T> digest1{allocator(), kAccuracy};

    std::default_random_engine gen(common::testutil::getRandomSeed(42));
    std::uniform_real_distribution<> dist;
    for (auto i = 0; i < 100; ++i) {
      auto v = T(dist(gen));
      auto w = (i + 2) % 3 + 1;
      digest1.add(v, w);
    }

    QuantileDigest<T> mergeResult{allocator(), kAccuracy};
    digest1.compress();
    mergeResult.testingMerge(digest1);

    QuantileDigest<T> mergeSerializedResult{allocator(), kAccuracy};
    std::string buf(digest1.serializedByteSize(), '\0');
    digest1.serialize(buf.data());
    mergeSerializedResult.mergeSerialized(buf.data());

    EXPECT_EQ(
        mergeResult.serializedByteSize(),
        mergeSerializedResult.serializedByteSize());
    std::string mergeResultBuf(mergeResult.serializedByteSize(), '\0');
    mergeResult.serialize(mergeResultBuf.data());
    std::string mergeSerializedResultBuf(
        mergeSerializedResult.serializedByteSize(), '\0');
    mergeSerializedResult.serialize(mergeSerializedResultBuf.data());
    EXPECT_EQ(mergeResultBuf, mergeSerializedResultBuf);
  }

  template <typename T>
  void testMergeEmpty(bool mergeSerialized) {
    constexpr double kAccuracy = 0.7;
    QuantileDigest<T> digest{StlAllocator<T>(allocator()), kAccuracy};
    QuantileDigest<T> digestEmpty{StlAllocator<T>(allocator()), kAccuracy};

    std::vector<double> values;
    std::default_random_engine gen(common::testutil::getRandomSeed(42));
    std::uniform_real_distribution<> dist;
    for (auto i = 0; i < 100; ++i) {
      auto v = T(dist(gen));
      auto w = (i + 3) % 7 + 1;
      digest.add(v, w);
      values.insert(values.end(), w, v);
    }
    digest.compress();
    std::string original(digest.serializedByteSize(), '\0');
    digest.serialize(original.data());

    if (mergeSerialized) {
      std::string buf(digestEmpty.serializedByteSize(), '\0');
      digestEmpty.serialize(buf.data());
      digest.mergeSerialized(buf.data());
    } else {
      digest.testingMerge(digestEmpty);
    }

    std::string result(digest.serializedByteSize(), '\0');
    digest.serialize(result.data());
    EXPECT_EQ(original.size(), result.size());
    EXPECT_EQ(original, result);
  }

  template <typename T>
  void testMerge(bool mergeSerialized) {
    constexpr double kAccuracy = 0.5;
    QuantileDigest<T> digest1{StlAllocator<T>(allocator()), kAccuracy};
    QuantileDigest<T> digest2{StlAllocator<T>(allocator()), kAccuracy};
    QuantileDigest<T> digestEmpty{StlAllocator<T>(allocator()), kAccuracy};

    std::vector<double> allValues;
    std::vector<double> digest1Values;
    std::default_random_engine gen(common::testutil::getRandomSeed(42));
    std::uniform_real_distribution<> dist;
    for (auto i = 0; i < 10000; ++i) {
      auto v = T(dist(gen));
      auto w = (i + 3) % 7 + 1;
      digest1.add(v, w);
      allValues.insert(allValues.end(), w, v);
      digest1Values.insert(digest1Values.end(), w, v);

      v = T(dist(gen));
      w = (i + 2) % 3 + 1;
      digest2.add(v, w);
      allValues.insert(allValues.end(), w, v);
    }
    if (mergeSerialized) {
      std::string buf(digest1.serializedByteSize(), '\0');
      digest1.serialize(buf.data());
      digestEmpty.mergeSerialized(buf.data());
    } else {
      digestEmpty.testingMerge(digest1);
    }
    std::sort(std::begin(digest1Values), std::end(digest1Values));
    checkQuantiles<QuantileDigest<T>, false>(
        digest1Values, digestEmpty, 0.0, digest1Values.size() * kAccuracy);

    if (mergeSerialized) {
      std::string buf(digest2.serializedByteSize(), '\0');
      digest2.serialize(buf.data());
      digest1.mergeSerialized(buf.data());
    } else {
      digest1.testingMerge(digest2);
    }
    std::sort(std::begin(allValues), std::end(allValues));
    checkQuantiles<QuantileDigest<T>, false>(
        allValues, digest1, 0.0, allValues.size() * kAccuracy);
  }

  template <typename T>
  void testMergeWithJava(bool mergeSerialized) {
    constexpr double kAccuracy = 0.99;
    QuantileDigest<T> digest{StlAllocator<T>(allocator()), 0.99};
    std::vector<double> values;
    for (auto i = 0; i < 10000; ++i) {
      digest.add(static_cast<T>(i), 1.0);
      values.push_back(T(i));
    }

    std::string data;
    if constexpr (std::is_same_v<T, double>) {
      // Presto Query: SELECT QDIGEST_AGG(CAST(c0 AS DOUBLE), 1, 0.99) FROM
      //               UNNEST(SEQUENCE(5000, 8000, 2)) AS t(c0)
      data = decodeBase64(
          "AK5H4XoUru8/AAAAAAAAAAAAAAAAAAAAAAAAAAAAiLNAAAAAAABAv0CSAAAApAAAAAAAADBAAAAAAADAs8CkAAAAAAAAMEAAAAAAAOCzwLcAAAAAAAAAAAAAAAAAwLPAugAAAAAAADBAAAAAAACIs8CkAAAAAAAAMEAAAAAAAAC0wKQAAAAAAAAwQAAAAAAAILTAtwAAAAAAAAAAAAAAAAAAtMCkAAAAAAAAMEAAAAAAAEC0wKQAAAAAAAAwQAAAAAAAYLTAtwAAAAAAAAAAAAAAAABAtMC7AAAAAAAAAAAAAAAAAAC0wKQAAAAAAAAwQAAAAAAAgLTApAAAAAAAADBAAAAAAACgtMC3AAAAAAAAAAAAAAAAAIC0wKQAAAAAAAAwQAAAAAAAwLTApAAAAAAAADBAAAAAAADgtMC3AAAAAAAAAAAAAAAAAMC0wLsAAAAAAAAAAAAAAAAAgLTAvwAAAAAAAAAAAAAAAAAAtMCkAAAAAAAAMEAAAAAAAAC1wKQAAAAAAAAwQAAAAAAAILXAtwAAAAAAAAAAAAAAAAAAtcCkAAAAAAAAMEAAAAAAAEC1wKQAAAAAAAAwQAAAAAAAYLXAtwAAAAAAAAAAAAAAAABAtcC7AAAAAAAAAAAAAAAAAAC1wKQAAAAAAAAwQAAAAAAAgLXApAAAAAAAADBAAAAAAACgtcC3AAAAAAAAAAAAAAAAAIC1wL8AAAAAAAAAAAAAAAAAALXAwwAAAAAAADBAAAAAAAAAtMCkAAAAAAAAMEAAAAAAAAC2wKQAAAAAAAAwQAAAAAAAILbAtwAAAAAAAAAAAAAAAAAAtsCkAAAAAAAAMEAAAAAAAEC2wKQAAAAAAAAwQAAAAAAAYLbAtwAAAAAAAAAAAAAAAABAtsC7AAAAAAAAAAAAAAAAAAC2wKQAAAAAAAAwQAAAAAAAgLbApAAAAAAAADBAAAAAAACgtsC3AAAAAAAAAAAAAAAAAIC2wL8AAAAAAAAwQAAAAAAAALbApAAAAAAAADBAAAAAAABAt8CkAAAAAAAAMEAAAAAAAGC3wLcAAAAAAAAAAAAAAAAAQLfApAAAAAAAADBAAAAAAACAt8CkAAAAAAAAMEAAAAAAAKC3wLcAAAAAAAAAAAAAAAAAgLfApAAAAAAAADBAAAAAAADAt8CkAAAAAAAAMEAAAAAAAOC3wLcAAAAAAAAAAAAAAAAAwLfAuwAAAAAAAAAAAAAAAACAt8C/AAAAAAAAMEAAAAAAAAC3wMMAAAAAAAA0QAAAAAAAALbAxwAAAAAAADRAAAAAAAAAtMDLAAAAAAAAMEAAAAAAAIizwKQAAAAAAAAwQAAAAAAAALjApAAAAAAAADBAAAAAAAAguMC3AAAAAAAAAAAAAAAAAAC4wKQAAAAAAAAwQAAAAAAAQLjApAAAAAAAADBAAAAAAABguMC3AAAAAAAAAAAAAAAAAEC4wLsAAAAAAAAAAAAAAAAAALjApAAAAAAAADBAAAAAAACAuMCkAAAAAAAAMEAAAAAAAKC4wLcAAAAAAAAAAAAAAAAAgLjAvwAAAAAAADtAAAAAAAAAuMCkAAAAAAAAMEAAAAAAAAC5wKQAAAAAAAAwQAAAAAAAILnAtwAAAAAAAAAAAAAAAAAAucC5AAAAAAAAMEAAAAAAAAC5wKQAAAAAAAAwQAAAAAAAgLnApAAAAAAAADBAAAAAAACgucC3AAAAAAAAAAAAAAAAAIC5wKQAAAAAAAAwQAAAAAAAwLnApAAAAAAAADBAAAAAAADgucC3AAAAAAAAAAAAAAAAAMC5wLsAAAAAAAAAAAAAAAAAgLnAvwAAAAAAADBAAAAAAAAAucDDAAAAAAAAAAAAAAAAAAC4wKQAAAAAAAAwQAAAAAAAALrApAAAAAAAAChAAAAAAAAousC3AAAAAAAAAAAAAAAAAAC6wKQAAAAAAAAwQAAAAAAAQLrApAAAAAAAADBAAAAAAABgusC3AAAAAAAAAAAAAAAAAEC6wLsAAAAAAAAAAAAAAAAAALrApAAAAAAAADBAAAAAAADAusCkAAAAAAAAMEAAAAAAAOC6wLcAAAAAAAAAAAAAAAAAwLrAvwAAAAAAAAAAAAAAAAAAusCkAAAAAAAAMEAAAAAAAAC7wKQAAAAAAAAwQAAAAAAAILvAtwAAAAAAAAAAAAAAAAAAu8CkAAAAAAAAMEAAAAAAAEC7wKQAAAAAAAAwQAAAAAAAYLvAtwAAAAAAAAAAAAAAAABAu8C7AAAAAAAAAAAAAAAAAAC7wKQAAAAAAAAwQAAAAAAAgLvApAAAAAAAADBAAAAAAACgu8C3AAAAAAAAAAAAAAAAAIC7wL8AAAAAAAAAAAAAAAAAALvAwwAAAAAAADhAAAAAAAAAusDHAAAAAAAAN0AAAAAAAAC4wKQAAAAAAAAwQAAAAAAAALzApAAAAAAAADBAAAAAAAAgvMC3AAAAAAAAAAAAAAAAAAC8wKQAAAAAAAAwQAAAAAAAQLzApAAAAAAAADBAAAAAAABgvMC3AAAAAAAAAAAAAAAAAEC8wLsAAAAAAAAAAAAAAAAAALzApAAAAAAAADBAAAAAAADAvMCkAAAAAAAAMEAAAAAAAOC8wLcAAAAAAAAAAAAAAAAAwLzAvwAAAAAAACxAAAAAAAAAvMCkAAAAAAAAMEAAAAAAAAC9wKQAAAAAAAAwQAAAAAAAIL3AtwAAAAAAAAAAAAAAAAAAvcCkAAAAAAAAMEAAAAAAAEC9wKQAAAAAAAAwQAAAAAAAYL3AtwAAAAAAAAAAAAAAAABAvcC7AAAAAAAAAAAAAAAAAAC9wKQAAAAAAAAwQAAAAAAAgL3ApAAAAAAAADBAAAAAAACgvcC3AAAAAAAAAAAAAAAAAIC9wLkAAAAAAAAuQAAAAAAAgL3AvwAAAAAAADFAAAAAAAAAvcDDAAAAAAAAAAAAAAAAAAC8wKQAAAAAAAAwQAAAAAAAAL7ApAAAAAAAADBAAAAAAAAgvsC3AAAAAAAAAAAAAAAAAAC+wKQAAAAAAAAwQAAAAAAAgL7ApAAAAAAAADBAAAAAAACgvsC3AAAAAAAAAAAAAAAAAIC+wKQAAAAAAAAwQAAAAAAAwL7ApAAAAAAAADBAAAAAAADgvsC3AAAAAAAAAAAAAAAAAMC+wLsAAAAAAAAAAAAAAAAAgL7AvwAAAAAAAAAAAAAAAAAAvsCkAAAAAAAAMEAAAAAAAAC/wKQAAAAAAAAwQAAAAAAAIL/AtwAAAAAAAAAAAAAAAAAAv8DDAAAAAAAAO0AAAAAAAAC+wMcAAAAAAAA4QAAAAAAAALzAywAAAAAAADFAAAAAAAAAuMDPAAAAAAAAKkAAAAAAAIizwA==");
    } else {
      // Presto Query: SELECT QDIGEST_AGG(CAST(c0 AS REAL), 1, 0.99)
      //               FROM UNNEST(SEQUENCE(5000, 8000, 2)) AS t(c0)
      data = decodeBase64(
          "AK5H4XoUru8/AAAAAAAAAAAAAAAAAAAAAABAnEUAAAAAAAD6RQAAAAAwAAAAOAAAAAAAAEhAAECcRQAAAIA4AAAAAAAASEAAAKBFAAAAgDgAAAAAAABGQAAApEUAAACASwAAAAAAAAAAAACgRQAAAIA0AAAAAAAAQEAAAKpFAAAAgDQAAAAAAABAQAAArkUAAACASwAAAAAAAD9AAACoRQAAAIBPAAAAAAAARkAAAKBFAAAAgDgAAAAAAABLQABgsEUAAACANAAAAAAAAD5AACC2RQAAAIBLAAAAAAAAAAAAYLBFAAAAgDQAAAAAAABAQAAAuEUAAACAOAAAAAAAgEVAAAC8RQAAAIBLAAAAAAAAAAAAALhFAAAAgE8AAAAAAABHQABgsEUAAACAUwAAAAAAADxAAACgRQAAAIBXAAAAAACAQUAAQJxFAAAAgDgAAAAAAABOQABAwEUAAACASQAAAAAAADxAAEDARQAAAIA4AAAAAAAATkAAQMxFAAAAgEoAAAAAAABGQAAAyEUAAACATwAAAAAAAEhAAEDARQAAAIA0AAAAAAAAQEAAANRFAAAAgDQAAAAAAABAQAAA1kUAAACARwAAAAAAAAAAAADURQAAAIBKAAAAAAAAPEAAwNBFAAAAgDgAAAAAAABOQABA2EUAAACANAAAAAAAADxAAEDeRQAAAIBLAAAAAAAAQkAAQNhFAAAAgE8AAAAAAAAAAADA0EUAAACAUwAAAAAAADxAAEDARQAAAIA0AAAAAAAAQEAAAORFAAAAgDQAAAAAAABAQAAA5kUAAACARwAAAAAAAAAAAADkRQAAAIA0AAAAAAAAPEAAQOpFAAAAgEYAAAAAAABCQAAA6EUAAACANAAAAAAAADRAAMDuRQAAAIBGAAAAAAAARkAAAOxFAAAAgEsAAAAAAAAAAAAA6EUAAACATwAAAAAAAAAAAEDhRQAAAIA4AAAAAAAATkAAQPBFAAAAgCgAAAAAAAAQQADA90UAAACARgAAAAAAAE5AAAD0RQAAAIBLAAAAAAAAAAAAQPBFAAAAgE0AAAAAAIBAQABA8EUAAACAUwAAAAAAAEhAAEDhRQAAAIBXAAAAAAAARkAAQMBFAAAAgFsAAAAAAAA9QABAnEUAAACA");
    }
    for (auto i = 5000; i <= 8000; i += 2) {
      values.push_back(T(i));
    }

    if (mergeSerialized) {
      digest.mergeSerialized(data.data());
    } else {
      QuantileDigest<T> digestJava{StlAllocator<T>(allocator()), data.data()};
      digest.testingMerge(digestJava);
    }
    std::sort(std::begin(values), std::end(values));
    checkQuantiles<QuantileDigest<T>, false>(
        values, digest, 0.0, values.size() * kAccuracy);
  }

  template <typename T>
  void testScale() {
    constexpr double kAccuracy = 0.75;
    QuantileDigest<T> digest{StlAllocator<T>(allocator()), kAccuracy};
    std::vector<double> values;
    for (auto i = 0; i < 10000; ++i) {
      digest.add(static_cast<T>(i), 1.0);
      values.push_back(T(i));
    }

    std::sort(std::begin(values), std::end(values));
    checkQuantiles<QuantileDigest<T>, false>(
        values, digest, 0.0, values.size() * kAccuracy);

    digest.scale(10.0);
    std::vector<double> scaledValues;
    for (const auto value : values) {
      scaledValues.insert(scaledValues.end(), 10, value);
    }
    checkQuantiles<QuantileDigest<T>, false>(
        scaledValues, digest, 0.0, scaledValues.size() * kAccuracy);
  }

 private:
  std::shared_ptr<memory::MemoryPool> rootPool_{
      memory::memoryManager()->addRootPool()};
  std::shared_ptr<memory::MemoryPool> pool_{rootPool_->addLeafChild("leaf")};
  HashStringAllocator allocator_{pool_.get()};
};

TEST_F(QuantileDigestTest, basic) {
  std::vector<double> quantiles{
      0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0};
  testQuantiles<int64_t>(
      {-5, -4, 4, -3, 3, -2, 2, -1, 1, 0},
      quantiles,
      {-5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 4});
  testQuantiles<double>(
      {-5.0, -4.0, 4.0, -3.0, 3.0, -2.0, 2.0, -1.0, 1.0, 0.0},
      quantiles,
      {-5.0, -4.0, -3.0, -2.0, -1.0, 0.0, 1.0, 2.0, 3.0, 4.0, 4.0});
  testQuantiles<float>(
      {-5.0, -4.0, 4.0, -3.0, 3.0, -2.0, 2.0, -1.0, 1.0, 0.0},
      quantiles,
      {-5.0, -4.0, -3.0, -2.0, -1.0, 0.0, 1.0, 2.0, 3.0, 4.0, 4.0});
}

TEST_F(QuantileDigestTest, weighted) {
  std::vector<double> quantiles{
      0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0};

  testQuantiles<int64_t>(
      {0, 2, 4, 5}, quantiles, {0, 0, 0, 2, 4, 4, 4, 4, 4, 5, 5}, {3, 1, 5, 1});
  testQuantiles<double>(
      {0.0, 2.0, 4.0, 5.0},
      quantiles,
      {0.0, 0.0, 0.0, 2.0, 4.0, 4.0, 4.0, 4.0, 4.0, 5.0, 5.0},
      {3, 1, 5, 1});
  testQuantiles<float>(
      {0.0, 2.0, 4.0, 5.0},
      quantiles,
      {0.0, 0.0, 0.0, 2.0, 4.0, 4.0, 4.0, 4.0, 4.0, 5.0, 5.0},
      {3, 1, 5, 1});
}

TEST_F(QuantileDigestTest, largeInputSize) {
  testLargeInputSize<double>();
  testLargeInputSize<float>();
}

TEST_F(QuantileDigestTest, checkQuantilesAfterSerDe) {
  QuantileDigest<int64_t> digestBigint{StlAllocator<int64_t>(allocator()), 0.8};
  QuantileDigest<double> digestDouble{StlAllocator<double>(allocator()), 0.8};
  for (auto i = 0; i < 100; ++i) {
    digestBigint.add(static_cast<int64_t>(i), 1.0);
    digestDouble.add(static_cast<double>(i), 1.0);
  }
  // Repeat SerDe three times to match Presto result of a partial ->
  // intermediate -> intermediate -> final aggregation plan.
  // Query: SELECT
  //          VALUES_AT_QUANTILES(
  //            QDIGEST_AGG(CAST(c0 AS BIGINT/DOUBLE), 1, 0.8),
  //            ARRAY[0.01, 0.1, 0.15, 0.3, 0.55, 0.7, 0.85, 0.9, 0.99]
  //        )
  //        FROM UNNEST(SEQUENCE(0, 99)) AS t(c0)
  for (auto i = 1; i <= 3; ++i) {
    std::string buf(1 + digestBigint.serializedByteSize(), '\0');
    digestBigint.serialize(buf.data());
    digestBigint =
        QuantileDigest<int64_t>{StlAllocator<int64_t>(allocator()), buf.data()};

    buf.resize(1 + digestDouble.serializedByteSize(), '\0');
    digestDouble.serialize(buf.data());
    digestDouble =
        QuantileDigest<double>{StlAllocator<double>(allocator()), buf.data()};
  }

  std::vector<double> quantiles{
      0.01, 0.1, 0.15, 0.3, 0.55, 0.7, 0.85, 0.9, 0.99};
  std::vector<int64_t> expectedBigint{0, 8, 8, 40, 63, 80, 95, 96, 99};
  std::vector<int64_t> resultsBigint(quantiles.size());
  digestBigint.estimateQuantiles(quantiles, resultsBigint.data());
  for (auto i = 0; i < quantiles.size(); ++i) {
    EXPECT_EQ(resultsBigint[i], expectedBigint[i]);
  }

  std::vector<double> expectedDouble{
      1.0, 10.0, 15.0, 30.0, 55.0, 70.0, 85.0, 90.0, 99.0};
  std::vector<double> resultsDouble(quantiles.size());
  digestDouble.estimateQuantiles(quantiles, resultsDouble.data());
  for (auto i = 0; i < quantiles.size(); ++i) {
    EXPECT_EQ(resultsDouble[i], expectedDouble[i]);
  }
}

TEST_F(QuantileDigestTest, serializedMatchJava) {
  // Test small input size.
  // Query: SELECT QDIGEST_AGG(CAST(c0 AS BIGINT/DOUBLE), 1, 0.99) FROM
  //        UNNEST(SEQUENCE(-5, 4)) AS t(c0)
  {
    QuantileDigest<int64_t> digestBigint{
        StlAllocator<int64_t>(allocator()), 0.99};
    QuantileDigest<double> digestDouble{
        StlAllocator<double>(allocator()), 0.99};
    QuantileDigest<float> digestReal{StlAllocator<float>(allocator()), 0.99};
    for (auto i = -5; i < 5; ++i) {
      digestBigint.add(static_cast<int64_t>(i), 1.0);
      digestDouble.add(static_cast<double>(i), 1.0);
      digestReal.add(static_cast<float>(i), 1.0);
    }
    std::string buf(1 + digestBigint.serializedByteSize(), '\0');
    auto length = digestBigint.serialize(buf.data());
    buf.resize(length);
    auto encodedBuf = encodeBase64(buf);
    ASSERT_EQ(
        encodedBuf,
        "AK5H4XoUru8/AAAAAAAAAAAAAAAAAAAAAPv/////////BAAAAAAAAAATAAAAAAAAAAAAAPA/+////////38AAAAAAAAA8D/8////////fwAAAAAAAADwP/3///////9/AwAAAAAAAAAA/P///////38AAAAAAAAA8D/+////////fwAAAAAAAADwP/////////9/AwAAAAAAAAAA/v///////38HAAAAAAAAAAD8////////fwsAAAAAAAAAAPv///////9/AAAAAAAAAPA/AAAAAAAAAIAAAAAAAAAA8D8BAAAAAAAAgAMAAAAAAAAAAAAAAAAAAACAAAAAAAAAAPA/AgAAAAAAAIAAAAAAAAAA8D8DAAAAAAAAgAMAAAAAAAAAAAIAAAAAAACABwAAAAAAAAAAAAAAAAAAAIAAAAAAAAAA8D8EAAAAAAAAgAsAAAAAAAAAAAAAAAAAAACA/wAAAAAAAAAA+////////38=");

    buf.clear();
    buf.resize(1 + digestDouble.serializedByteSize(), '\0');
    length = digestDouble.serialize(buf.data());
    buf.resize(length);
    encodedBuf = encodeBase64(buf);
    ASSERT_EQ(
        encodedBuf,
        "AK5H4XoUru8/AAAAAAAAAAAAAAAAAAAAAP///////+u/AAAAAAAAEEATAAAAAAAAAAAAAPA/////////6z8AAAAAAAAA8D/////////vP8sAAAAAAAAAAP///////+s/AAAAAAAAAPA/////////9z8AAAAAAAAA8D//////////P88AAAAAAAAAAP////////c/0wAAAAAAAAAA////////6z8AAAAAAAAA8D////////8PQPsAAAAAAAAAAP///////+s/AAAAAAAAAPA/AAAAAAAAAIAAAAAAAAAA8D8AAAAAAADwv/cAAAAAAAAAAAAAAAAAAACAAAAAAAAAAPA/AAAAAAAAAMAAAAAAAAAA8D8AAAAAAAAIwM8AAAAAAAAAAAAAAAAAAADAAAAAAAAAAPA/AAAAAAAAEMDTAAAAAAAAAAAAAAAAAAAAwPsAAAAAAAAAAAAAAAAAAACA/wAAAAAAAAAA////////6z8=");

    buf.clear();
    buf.resize(1 + digestReal.serializedByteSize(), '\0');
    length = digestReal.serialize(buf.data());
    buf.resize(length);
    encodedBuf = encodeBase64(buf);
    ASSERT_EQ(
        encodedBuf,
        "AK5H4XoUru8/AAAAAAAAAAAAAAAAAAAAAP//X7//////AACAQAAAAAATAAAAAAAAAAAAAPA///9fv////38AAAAAAAAA8D///3+/////f1cAAAAAAAAAAP//X7////9/AAAAAAAAAPA///+/v////38AAAAAAAAA8D////+/////f1sAAAAAAAAAAP//v7////9/XwAAAAAAAAAA//9fv////38AAAAAAAAA8D///3/A////f3sAAAAAAAAAAP//X7////9/AAAAAAAAAPA/AAAAAAAAAIAAAAAAAAAA8D8AAIA/AAAAgHcAAAAAAAAAAAAAAAAAAACAAAAAAAAAAPA/AAAAQAAAAIAAAAAAAAAA8D8AAEBAAAAAgFsAAAAAAAAAAAAAAEAAAACAAAAAAAAAAPA/AACAQAAAAIBfAAAAAAAAAAAAAABAAAAAgHsAAAAAAAAAAAAAAAAAAACA/wAAAAAAAAAA//9fv////38=");
  }

  // Test large input size.
  // Query: SELECT QDIGEST_AGG(CAST(c0 AS BIGINT), 1, 0.99) FROM
  //        UNNEST(SEQUENCE(-5000, 4999)) AS t(c0)
  {
    QuantileDigest<int64_t> digestBigint{
        StlAllocator<int64_t>(allocator()), 0.99};
    QuantileDigest<double> digestDouble{
        StlAllocator<double>(allocator()), 0.99};
    QuantileDigest<float> digestReal{StlAllocator<float>(allocator()), 0.99};
    for (auto i = -5000; i < 5000; ++i) {
      digestBigint.add(static_cast<int64_t>(i), 1.0);
      digestDouble.add(static_cast<double>(i), 1.0);
      digestReal.add(static_cast<float>(i), 1.0);
    }

    std::string bufBigint(1 + digestBigint.serializedByteSize(), '\0');
    std::string bufDouble(1 + digestDouble.serializedByteSize(), '\0');
    std::string bufReal(1 + digestReal.serializedByteSize(), '\0');
    auto length = digestBigint.serialize(bufBigint.data());
    bufBigint.resize(length);
    length = digestDouble.serialize(bufDouble.data());
    bufDouble.resize(length);
    length = digestReal.serialize(bufReal.data());
    bufReal.resize(length);
    for (auto i = 1; i <= 3; ++i) {
      digestBigint = QuantileDigest<int64_t>{
          StlAllocator<int64_t>(allocator()), bufBigint.data()};
      bufBigint.clear();
      bufBigint.resize(1 + digestBigint.serializedByteSize(), '\0');
      length = digestBigint.serialize(bufBigint.data());
      bufBigint.resize(length);

      digestDouble = QuantileDigest<double>{
          StlAllocator<double>(allocator()), bufDouble.data()};
      bufDouble.clear();
      bufDouble.resize(1 + digestDouble.serializedByteSize(), '\0');
      length = digestDouble.serialize(bufDouble.data());
      bufDouble.resize(length);

      digestReal = QuantileDigest<float>{
          StlAllocator<float>(allocator()), bufReal.data()};
      bufReal.clear();
      bufReal.resize(1 + digestReal.serializedByteSize(), '\0');
      length = digestReal.serialize(bufReal.data());
      bufReal.resize(length);
    }
    auto encodedBuf = encodeBase64(bufBigint);
    ASSERT_EQ(
        encodedBuf,
        "AK5H4XoUru8/AAAAAAAAAAAAAAAAAAAAAHjs////////hxMAAAAAAABUAAAAFAAAAAAA4HJAeOz//////38MAAAAAACAWUAa7v//////fxAAAAAAAABnQAHv//////9/IwAAAAAAwGNAGu7//////38nAAAAAAAAAAB47P//////fxAAAAAAAIBtQBTw//////9/EAAAAAAAAGFADPL//////38QAAAAAAAAb0AI8///////fyMAAAAAAAAAAAzy//////9/JwAAAAAAAHFAFPD//////38UAAAAAABgdEAg9P//////fxQAAAAAAKBzQAr2//////9/JwAAAAAAgGdAIPT//////38rAAAAAACAZEAU8P//////fxAAAAAAAMBvQAL4//////9/EAAAAAAAAHBAAPn//////38jAAAAAAAAAAAC+P//////fyUAAAAAAIBvQAL4//////9/EAAAAAAAgGlANP3//////38AAAAAAAAAAED+////////fx4AAAAAAABlQFb///////9/IgAAAAAAAHVABv7//////38nAAAAAADAckAO/P//////fysAAAAAACBxQAL4//////9/LwAAAAAAAAAAFPD//////38zAAAAAABAdEB47P//////fwgAAAAAAABNQEYAAAAAAACADAAAAAAAQF5AhwAAAAAAAIAfAAAAAAAAAAAAAAAAAAAAgAwAAAAAAABgQAABAAAAAACADAAAAAAAAGBAgAEAAAAAAIAfAAAAAAAAAAAAAQAAAAAAgCMAAAAAAAAAAAAAAAAAAACADAAAAAAAAGBAAAIAAAAAAIAMAAAAAAAAYECAAgAAAAAAgB8AAAAAAAAAAAACAAAAAACAIQAAAAAAQFtAAAIAAAAAAIAnAAAAAABgYkAAAAAAAAAAgAwAAAAAAABgQAAEAAAAAACADAAAAAAAAGBAgAQAAAAAAIAfAAAAAAAAAAAABAAAAAAAgAwAAAAAAEBZQJsFAAAAAACAIwAAAAAAQFBAAAQAAAAAAIAMAAAAAAAAYEAABgAAAAAAgAwAAAAAAABgQIAGAAAAAACAHwAAAAAAAAAAAAYAAAAAAIAMAAAAAABAV0CjBwAAAAAAgB4AAAAAAEBQQGIHAAAAAACAIwAAAAAAgFhAAAYAAAAAAIAnAAAAAACAVkAABAAAAAAAgCsAAAAAAAAAAAAAAAAAAACADAAAAAAAAGBAAAgAAAAAAIAMAAAAAAAAYECACAAAAAAAgB8AAAAAAAAAAAAIAAAAAACAIQAAAAAAwGJAAAgAAAAAAIAMAAAAAAAAYEAACgAAAAAAgAwAAAAAAABgQIAKAAAAAACAHwAAAAAAAAAAAAoAAAAAAIAQAAAAAADAYUByCwAAAAAAgCMAAAAAAIBcQAAKAAAAAACAJwAAAAAAAAAAAAgAAAAAAIAMAAAAAAAAYEAADAAAAAAAgAwAAAAAAABgQIAMAAAAAACAHwAAAAAAAAAAAAwAAAAAAIAhAAAAAADAYEAADAAAAAAAgAwAAAAAAABgQAAOAAAAAACADAAAAAAAAGBAgA4AAAAAAIAfAAAAAAAAAAAADgAAAAAAgAwAAAAAAIBfQIIPAAAAAACAIwAAAAAAQGBAAA4AAAAAAIAnAAAAAAAAAAAADAAAAAAAgCsAAAAAAIBeQAAIAAAAAACALwAAAAAAgFpAAAAAAAAAAIAMAAAAAACAXkCGEAAAAAAAgAwAAAAAAIBdQIoRAAAAAACAHgAAAAAAQGBACBEAAAAAAIAjAAAAAAAAAAAEEAAAAAAAgAwAAAAAAIBcQI4SAAAAAACAHgAAAAAAwGFAABIAAAAAAIAQAAAAAAAAYUAAEwAAAAAAgCMAAAAAAAAAAAASAAAAAACAJwAAAAAAQGFABBAAAAAAAIAzAAAAAABAVEAAAAAAAAAAgP8AAAAAAAAAAHjs//////9/");

    encodedBuf = encodeBase64(bufDouble);
    ASSERT_EQ(
        encodedBuf,
        "AK5H4XoUru8/AAAAAAAAAAAAAAAAAAAAAP//////d0y/AAAAAACHs0B0AAAAsAAAAAAAAGBA//////93TD+sAAAAAABAVkD//////wBNP6wAAAAAAABUQP//////gE0/vwAAAAAAwFRA//////8ATT/DAAAAAAAAAAD//////3dMP6wAAAAAAABUQP//////gE4/sAAAAAAAAGBA//////8ATz/DAAAAAAAAAAD//////wBOP8cAAAAAAABaQP//////d0w/sAAAAAAAAFhA//////8BUD+wAAAAAAAAUED//////wFRP8MAAAAAAAAAAP//////AVA/rAAAAAAAAFBA//////8BUj+wAAAAAAAAWED//////wFTP8MAAAAAAAAAAP//////AVI/xwAAAAAA4GBA//////8BUD+0AAAAAAAAYED//////wFUP7AAAAAAAMBfQP//////A1Y/rAAAAAAAAFBA//////8BVz/DAAAAAAAAAAD//////wNWP8cAAAAAAIBcQP//////AVQ/ywAAAAAAQFVA//////8BUD+0AAAAAAAAYED//////wFYP6wAAAAAAABQQP//////AVo/rAAAAAAAAFBA//////+BWz/DAAAAAAAAWUD//////wFaP8cAAAAAAABVQP//////AVg/tAAAAAAAoGJA//////9XXD+wAAAAAADAVkD//////wFfP8IAAAAAAMBcQP//////B14/xwAAAAAAAFZA//////9XXD/LAAAAAABAV0D//////wFYP88AAAAAAAAAAP//////AVA/0wAAAAAAgGJA//////93TD+0AAAAAAAAV0D//////wNkP7QAAAAAAIBbQP//////S2Y/xwAAAAAAAAAA//////8DZD/KAAAAAAAAYED//////0dgP7QAAAAAAABgQP//////A2g/xQAAAAAAwFZA//////8DaD+0AAAAAADAX0D//////wduP8YAAAAAAIBbQP//////T2w/ywAAAAAAAAAA//////8DaD/PAAAAAACAXUD//////0dgP7gAAAAAAABgQP//////B3A/uAAAAAAAAGBA//////8HeD+4AAAAAABAVkD//////z99P8sAAAAAAAAAAP//////B3g/zwAAAAAAAFdA//////8HcD/TAAAAAABgYED//////0dgP9cAAAAAAIBYQP//////d0w/vAAAAAAAAGBA//////8PiD/AAAAAAAAAYED//////x+QP9MAAAAAAIBcQP//////74A/xAAAAAAAAFhA//////8/oD/XAAAAAAAAAAD//////++AP98AAAAAAABZQP//////d0w/vAAAAAAAAExAAAAAAAAAacC4AAAAAAAAS0AAAAAAAKB0wLwAAAAAAEBdQAAAAAAAsHjAzwAAAAAAAFNAAAAAAACQcMDTAAAAAACAUkAAAAAAAOBgwLQAAAAAAABJQAAAAAAAcILAuAAAAAAAQFxAAAAAAAB4hMDLAAAAAAAAAAAAAAAAAGiAwLgAAAAAAABgQAAAAAAAAIjAuAAAAAAAAGBAAAAAAAAAjMDLAAAAAAAAAAAAAAAAAACIwM8AAAAAAAAAAAAAAAAAaIDAtAAAAAAAAGBAAAAAAAAAkMC0AAAAAAAAYEAAAAAAAACSwMcAAAAAAAAAAAAAAAAAAJDAtAAAAAAAQFlAAAAAAABslsDLAAAAAABAUEAAAAAAAACQwLQAAAAAAABgQAAAAAAAAJjAtAAAAAAAAGBAAAAAAAAAmsDHAAAAAAAAAAAAAAAAAACYwLQAAAAAAEBXQAAAAAAAjJ7AxgAAAAAAQFBAAAAAAACIncDLAAAAAACAWEAAAAAAAACYwM8AAAAAAAAAAAAAAAAAAJDA0wAAAAAAgFZAAAAAAABogMCwAAAAAAAAYEAAAAAAAACgwLAAAAAAAABgQAAAAAAAAKHAwwAAAAAAAAAAAAAAAAAAoMDFAAAAAADAYkAAAAAAAACgwLAAAAAAAABgQAAAAAAAAKTAsAAAAAAAAGBAAAAAAAAApcDDAAAAAAAAAAAAAAAAAACkwLQAAAAAAMBhQAAAAAAA5KbAxwAAAAAAgFxAAAAAAAAApMDLAAAAAAAAAAAAAAAAAACgwLAAAAAAAABgQAAAAAAAAKjAsAAAAAAAAGBAAAAAAAAAqcDDAAAAAAAAAAAAAAAAAACowMUAAAAAAMBgQAAAAAAAAKjAsAAAAAAAAGBAAAAAAAAArMCwAAAAAAAAYEAAAAAAAACtwMMAAAAAAAAAAAAAAAAAAKzAsAAAAAAAgF9AAAAAAAAEr8DHAAAAAABAYEAAAAAAAACswMsAAAAAAAAAAAAAAAAAAKjAzwAAAAAAgF5AAAAAAAAAoMCsAAAAAACAXkAAAAAAAIawwKwAAAAAAIBdQAAAAAAAirHAvgAAAAAAQGBAAAAAAAAIscDDAAAAAAAAAAAAAAAAAASwwKwAAAAAAIBcQAAAAAAAjrLAvgAAAAAAwGFAAAAAAAAAssCwAAAAAAAAYUAAAAAAAACzwMMAAAAAAAAAAAAAAAAAALLAxwAAAAAAQGFAAAAAAAAEsMDTAAAAAACAW0AAAAAAAACgwNcAAAAAAABUQAAAAAAAaIDA3wAAAAAAgFNAAAAAAACAUcD/AAAAAAAgYkD//////3dMPw==");

    encodedBuf = encodeBase64(bufReal);
    ASSERT_EQ(
        encodedBuf,
        "AK5H4XoUru8/AAAAAAAAAAAAAAAAAAAAAP+/Y7r/////ADicRQAAAABuAAAAPAAAAAAA4GJA/wdouv///39OAAAAAAAAZUD/v2O6////fzwAAAAAAEBiQP8fcLr///9/PAAAAAAAIGNA/wd4uv///39PAAAAAACAYkD/H3C6////f1MAAAAAAMBbQP+/Y7r///9/QAAAAAAAwGJA/w+Auv///388AAAAAACAXED/75C6////f1MAAAAAAMBhQP8PgLr///9/PAAAAAAAAGBA/w+guv///388AAAAAACAX0D/L7C6////fzgAAAAAAABLQP+vvLr///9/TwAAAAAAAAAA/y+wuv///39TAAAAAABgYkD/D6C6////f1cAAAAAAMBaQP8PgLr///9/PAAAAAAAwFpA/1/Buv///388AAAAAAAAYED/D8i6////f08AAAAAAAAAAP9fwbr///9/UQAAAAAAwGRA/1/Buv///388AAAAAADAXUD/n+C6////fzwAAAAAAABgQP8P6Lr///9/TwAAAAAAAAAA/5/guv///39AAAAAAAAgY0D/f/a6////f1MAAAAAAMBZQP+f4Lr///9/VwAAAAAAAFVA/1/Buv///39bAAAAAACAVUD/D4C6////f18AAAAAAAAAAP+/Y7r///9/QAAAAAAAQFxA//8hu////39AAAAAAAAAYED/HzC7////f1MAAAAAAAAAAP//Ibv///9/VgAAAAAAQFZA//8Hu////39AAAAAAAAAYED/H0C7////f0AAAAAAAABgQP8fULv///9/UwAAAAAAAAAA/x9Au////39EAAAAAAAgYED//2+7////f1cAAAAAAMBfQP8fQLv///9/WwAAAAAAAAAA//8Hu////39EAAAAAABAUkD//627////f0QAAAAAAMBXQP9/6Lv///9/VgAAAAAAgGNA/3/Bu////39bAAAAAAAAAAD//4a7////f18AAAAAACBkQP//B7v///9/YwAAAAAAQGJA/79juv///39IAAAAAABAWED//0+8////f0wAAAAAAEBcQP//j7z///9/XwAAAAAAYGNA//8JvP///39QAAAAAAAAWED//wG9////f2MAAAAAAAAAAP//Cbz///9/ZQAAAAAAAD5A//8JvP///39rAAAAAAAAAAD/v2O6////f3kAAAAAAEBdQP+/Y7r///9/SAAAAAAAAExAAABIQwAAAIBEAAAAAAAAS0AAAKVDAAAAgEgAAAAAAEBdQACAxUMAAACAWwAAAAAAAFNAAICEQwAAAIBfAAAAAACAUkAAAAdDAAAAgEAAAAAAAABJQACAE0QAAACARAAAAAAAQFxAAMAjRAAAAIBXAAAAAAAAAAAAQANEAAAAgEQAAAAAAABgQAAAQEQAAACARAAAAAAAAGBAAABgRAAAAIBXAAAAAAAAAAAAAEBEAAAAgFsAAAAAAAAAAABAA0QAAACAQAAAAAAAAGBAAACARAAAAIBAAAAAAAAAYEAAAJBEAAAAgFMAAAAAAAAAAAAAgEQAAACAQAAAAAAAQFlAAGCzRAAAAIBXAAAAAABAUEAAAIBEAAAAgEAAAAAAAABgQAAAwEQAAACAQAAAAAAAAGBAAADQRAAAAIBTAAAAAAAAAAAAAMBEAAAAgEAAAAAAAEBXQABg9EQAAACAUgAAAAAAQFBAAEDsRAAAAIBXAAAAAACAWEAAAMBEAAAAgFsAAAAAAAAAAAAAgEQAAACAXwAAAAAAgFZAAEADRAAAAIA8AAAAAAAAYEAAAABFAAAAgDwAAAAAAABgQAAACEUAAACATwAAAAAAAAAAAAAARQAAAIBRAAAAAADAYkAAAABFAAAAgDwAAAAAAABgQAAAIEUAAACAPAAAAAAAAGBAAAAoRQAAAIBPAAAAAAAAAAAAACBFAAAAgEAAAAAAAMBhQAAgN0UAAACAUwAAAAAAgFxAAAAgRQAAAIBXAAAAAAAAAAAAAABFAAAAgDwAAAAAAABgQAAAQEUAAACAPAAAAAAAAGBAAABIRQAAAIBPAAAAAAAAAAAAAEBFAAAAgFEAAAAAAMBgQAAAQEUAAACAPAAAAAAAAGBAAABgRQAAAIA8AAAAAAAAYEAAAGhFAAAAgE8AAAAAAAAAAAAAYEUAAACAPAAAAAAAgF9AACB4RQAAAIBTAAAAAABAYEAAAGBFAAAAgFcAAAAAAAAAAAAAQEUAAACAWwAAAAAAgF5AAAAARQAAAIA4AAAAAACAXkAAMIRFAAAAgDgAAAAAAIBdQABQjEUAAACASgAAAAAAQGBAAECIRQAAAIBPAAAAAAAAAAAAIIBFAAAAgDgAAAAAAIBcQABwlEUAAACASgAAAAAAwGFAAACQRQAAAIA8AAAAAAAAYUAAAJhFAAAAgE8AAAAAAAAAAAAAkEUAAACAUwAAAAAAQGFAACCARQAAAIBfAAAAAACAW0AAAABFAAAAgGMAAAAAAABUQABAA0QAAACAawAAAAAAgFNAAACMQgAAAID/AAAAAACAUUD/v2O6////fw==");
  }
}

TEST_F(QuantileDigestTest, merge) {
  testMerge<double>(true);
  testMerge<float>(true);

  testMerge<double>(false);
  testMerge<float>(false);
}

TEST_F(QuantileDigestTest, mergeWithJava) {
  testMergeWithJava<double>(true);
  testMergeWithJava<float>(true);

  testMergeWithJava<double>(false);
  testMergeWithJava<float>(false);
}

TEST_F(QuantileDigestTest, mergeWithEmpty) {
  testMergeEmpty<double>(true);
  testMergeEmpty<float>(true);

  testMergeEmpty<double>(false);
  testMergeEmpty<float>(false);
}

TEST_F(QuantileDigestTest, infinity) {
  const double kInf = std::numeric_limits<double>::infinity();
  const float kFInf = std::numeric_limits<float>::infinity();
  std::vector<double> quantiles{0.0, 0.3, 0.4, 0.5, 0.6, 0.7, 1.0};
  testQuantiles<double>(
      {0.0, kInf, -kInf}, quantiles, {-kInf, -kInf, 0.0, 0.0, 0.0, kInf, kInf});
  testQuantiles<float>(
      {0.0, kFInf, -kFInf},
      quantiles,
      {-kFInf, -kFInf, 0.0, 0.0, 0.0, kFInf, kFInf});
}

TEST_F(QuantileDigestTest, minMax) {
  const double kAccuracy = 0.05;
  QuantileDigest<int64_t> digestBigint{
      StlAllocator<int64_t>(allocator()), kAccuracy};
  QuantileDigest<double> digestDouble{
      StlAllocator<double>(allocator()), kAccuracy};
  QuantileDigest<float> digestReal{StlAllocator<float>(allocator()), kAccuracy};

  int64_t from = -12345;
  int64_t to = 54321;
  for (auto i = from; i <= to; ++i) {
    digestBigint.add(i, 1);
    digestDouble.add(static_cast<double>(i), 1);
    digestReal.add(static_cast<float>(i), 1);
  }

  auto rankError = (to - from + 1) * kAccuracy;
  ASSERT_LE(std::abs(from - digestBigint.getMin()), rankError);
  ASSERT_LE(std::abs(to - digestBigint.getMax()), rankError);
  ASSERT_LE(std::abs(from - digestDouble.getMin()), rankError);
  ASSERT_LE(std::abs(to - digestDouble.getMax()), rankError);
  ASSERT_LE(std::abs(from - digestReal.getMin()), rankError);
  ASSERT_LE(std::abs(to - digestReal.getMax()), rankError);
}

TEST_F(QuantileDigestTest, scale) {
  testScale<int64_t>();
  testScale<double>();
  testScale<float>();
}

TEST_F(QuantileDigestTest, scaleMatchesJava) {
  // Test with int64_t QDigest
  {
    std::string encodedBuf =
        "AK5H4XoUru8/AAAAAAAAAAAAAAAAAAAAAHjs////////hxMAAAAAAABUAAAAFAAAAAAA4HJAeOz//////38MAAAAAACAWUAa7v//////fxAAAAAAAABnQAHv//////9/IwAAAAAAwGNAGu7//////38nAAAAAAAAAAB47P//////fxAAAAAAAIBtQBTw//////9/EAAAAAAAAGFADPL//////38QAAAAAAAAb0AI8///////fyMAAAAAAAAAAAzy//////9/JwAAAAAAAHFAFPD//////38UAAAAAABgdEAg9P//////fxQAAAAAAKBzQAr2//////9/JwAAAAAAgGdAIPT//////38rAAAAAACAZEAU8P//////fxAAAAAAAMBvQAL4//////9/EAAAAAAAAHBAAPn//////38jAAAAAAAAAAAC+P//////fyUAAAAAAIBvQAL4//////9/EAAAAAAAgGlANP3//////38AAAAAAAAAAED+////////fx4AAAAAAABlQFb///////9/IgAAAAAAAHVABv7//////38nAAAAAADAckAO/P//////fysAAAAAACBxQAL4//////9/LwAAAAAAAAAAFPD//////38zAAAAAABAdEB47P//////fwgAAAAAAABNQEYAAAAAAACADAAAAAAAQF5AhwAAAAAAAIAfAAAAAAAAAAAAAAAAAAAAgAwAAAAAAABgQAABAAAAAACADAAAAAAAAGBAgAEAAAAAAIAfAAAAAAAAAAAAAQAAAAAAgCMAAAAAAAAAAAAAAAAAAACADAAAAAAAAGBAAAIAAAAAAIAMAAAAAAAAYECAAgAAAAAAgB8AAAAAAAAAAAACAAAAAACAIQAAAAAAQFtAAAIAAAAAAIAnAAAAAABgYkAAAAAAAAAAgAwAAAAAAABgQAAEAAAAAACADAAAAAAAAGBAgAQAAAAAAIAfAAAAAAAAAAAABAAAAAAAgAwAAAAAAEBZQJsFAAAAAACAIwAAAAAAQFBAAAQAAAAAAIAMAAAAAAAAYEAABgAAAAAAgAwAAAAAAABgQIAGAAAAAACAHwAAAAAAAAAAAAYAAAAAAIAMAAAAAABAV0CjBwAAAAAAgB4AAAAAAEBQQGIHAAAAAACAIwAAAAAAgFhAAAYAAAAAAIAnAAAAAACAVkAABAAAAAAAgCsAAAAAAAAAAAAAAAAAAACADAAAAAAAAGBAAAgAAAAAAIAMAAAAAAAAYECACAAAAAAAgB8AAAAAAAAAAAAIAAAAAACAIQAAAAAAwGJAAAgAAAAAAIAMAAAAAAAAYEAACgAAAAAAgAwAAAAAAABgQIAKAAAAAACAHwAAAAAAAAAAAAoAAAAAAIAQAAAAAADAYUByCwAAAAAAgCMAAAAAAIBcQAAKAAAAAACAJwAAAAAAAAAAAAgAAAAAAIAMAAAAAAAAYEAADAAAAAAAgAwAAAAAAABgQIAMAAAAAACAHwAAAAAAAAAAAAwAAAAAAIAhAAAAAADAYEAADAAAAAAAgAwAAAAAAABgQAAOAAAAAACADAAAAAAAAGBAgA4AAAAAAIAfAAAAAAAAAAAADgAAAAAAgAwAAAAAAIBfQIIPAAAAAACAIwAAAAAAQGBAAA4AAAAAAIAnAAAAAAAAAAAADAAAAAAAgCsAAAAAAIBeQAAIAAAAAACALwAAAAAAgFpAAAAAAAAAAIAMAAAAAACAXkCGEAAAAAAAgAwAAAAAAIBdQIoRAAAAAACAHgAAAAAAQGBACBEAAAAAAIAjAAAAAAAAAAAEEAAAAAAAgAwAAAAAAIBcQI4SAAAAAACAHgAAAAAAwGFAABIAAAAAAIAQAAAAAAAAYUAAEwAAAAAAgCMAAAAAAAAAAAASAAAAAACAJwAAAAAAQGFABBAAAAAAAIAzAAAAAABAVEAAAAAAAAAAgP8AAAAAAAAAAHjs//////9/";
    std::string decodedBuf = decodeBase64(encodedBuf);
    QuantileDigest<int64_t> digest{
        StlAllocator<int64_t>(allocator()), decodedBuf.data()};

    double scaleFactor = 2.5;
    digest.scale(scaleFactor);

    std::string scaledBuf(digest.serializedByteSize(), '\0');
    digest.serialize(scaledBuf.data());

    // Expected serialized form after scaling
    std::string expectedScaledBuf = decodeBase64(
        "AK5H4XoUru8/AAAAAAAAAAAAAAAAAAAAAHjs////////hxMAAAAAAABUAAAAEAAAAAAAmIdAeOz//////38IAAAAAADgb0Aa7v//////fwwAAAAAAMB8QAHv//////9/IwAAAAAAsHhAGu7//////38nAAAAAAAAAAB47P//////fwwAAAAAAHCCQBTw//////9/DAAAAAAAQHVADPL//////38MAAAAAABgg0AI8///////fyMAAAAAAAAAAAzy//////9/JwAAAAAAQIVAFPD//////38QAAAAAAB4iUAg9P//////fxAAAAAAAIiIQAr2//////9/JwAAAAAAYH1AIPT//////38rAAAAAACgeUAU8P//////fwwAAAAAANiDQAL4//////9/DAAAAAAAAIRAAPn//////38jAAAAAAAAAAAC+P//////fyUAAAAAALCDQAL4//////9/DAAAAAAA4H9ANP3//////38AAAAAAAAAFED+////////fx4AAAAAAEB6QFb///////9/IgAAAAAAQIpABv7//////38nAAAAAABwh0AO/P//////fysAAAAAAGiFQAL4//////9/LwAAAAAAAAAAFPD//////38zAAAAAABQiUB47P//////fwQAAAAAACBiQEYAAAAAAACACAAAAAAA6HJAhwAAAAAAAIAfAAAAAAAAAAAAAAAAAAAAgAgAAAAAAAB0QAABAAAAAACACAAAAAAAAHRAgAEAAAAAAIAfAAAAAAAAAAAAAQAAAAAAgCMAAAAAAAAAAAAAAAAAAACACAAAAAAAAHRAAAIAAAAAAIAIAAAAAAAAdECAAgAAAAAAgB8AAAAAAAAAAAACAAAAAACAIQAAAAAACHFAAAIAAAAAAIAnAAAAAAD4dkAAAAAAAAAAgAgAAAAAAAB0QAAEAAAAAACACAAAAAAAAHRAgAQAAAAAAIAfAAAAAAAAAAAABAAAAAAAgAgAAAAAAJBvQJsFAAAAAACAIwAAAAAAUGRAAAQAAAAAAIAIAAAAAAAAdEAABgAAAAAAgAgAAAAAAAB0QIAGAAAAAACAHwAAAAAAAAAAAAYAAAAAAIAIAAAAAAAQbUCjBwAAAAAAgB4AAAAAAFBkQGIHAAAAAACAIwAAAAAAoG5AAAYAAAAAAIAnAAAAAAAgbEAABAAAAAAAgCsAAAAAAAAAAAAAAAAAAACACAAAAAAAAHRAAAgAAAAAAIAIAAAAAAAAdECACAAAAAAAgB8AAAAAAAAAAAAIAAAAAACAIQAAAAAAcHdAAAgAAAAAAIAIAAAAAAAAdEAACgAAAAAAgAgAAAAAAAB0QIAKAAAAAACAHwAAAAAAAAAAAAoAAAAAAIAMAAAAAAAwdkByCwAAAAAAgCMAAAAAANBxQAAKAAAAAACAJwAAAAAAAAAAAAgAAAAAAIAIAAAAAAAAdEAADAAAAAAAgAgAAAAAAAB0QIAMAAAAAACAHwAAAAAAAAAAAAwAAAAAAIAhAAAAAADwdEAADAAAAAAAgAgAAAAAAAB0QAAOAAAAAACACAAAAAAAAHRAgA4AAAAAAIAfAAAAAAAAAAAADgAAAAAAgAgAAAAAALBzQIIPAAAAAACAIwAAAAAAUHRAAA4AAAAAAIAnAAAAAAAAAAAADAAAAAAAgCsAAAAAABBzQAAIAAAAAACALwAAAAAAkHBAAAAAAAAAAIAIAAAAAAAQc0CGEAAAAAAAgAgAAAAAAHByQIoRAAAAAACAHgAAAAAAUHRACBEAAAAAAIAjAAAAAAAAAAAEEAAAAAAAgAgAAAAAANBxQI4SAAAAAACAHgAAAAAAMHZAABIAAAAAAIAMAAAAAABAdUAAEwAAAAAAgCMAAAAAAAAAAAASAAAAAACAJwAAAAAAkHVABBAAAAAAAIAzAAAAAABQaUAAAAAAAAAAgP8AAAAAAAAAAHjs//////9/");

    ASSERT_EQ(scaledBuf, expectedScaledBuf);
  }

  // Test with double QDigest
  {
    std::string encodedBuf =
        "AK5H4XoUru8/AAAAAAAAAAAAAAAAAAAAAP//////d0y/AAAAAACHs0B0AAAAsAAAAAAAAGBA//////93TD+sAAAAAABAVkD//////wBNP6wAAAAAAABUQP//////gE0/vwAAAAAAwFRA//////8ATT/DAAAAAAAAAAD//////3dMP6wAAAAAAABUQP//////gE4/sAAAAAAAAGBA//////8ATz/DAAAAAAAAAAD//////wBOP8cAAAAAAABaQP//////d0w/sAAAAAAAAFhA//////8BUD+wAAAAAAAAUED//////wFRP8MAAAAAAAAAAP//////AVA/rAAAAAAAAFBA//////8BUj+wAAAAAAAAWED//////wFTP8MAAAAAAAAAAP//////AVI/xwAAAAAA4GBA//////8BUD+0AAAAAAAAYED//////wFUP7AAAAAAAMBfQP//////A1Y/rAAAAAAAAFBA//////8BVz/DAAAAAAAAAAD//////wNWP8cAAAAAAIBcQP//////AVQ/ywAAAAAAQFVA//////8BUD+0AAAAAAAAYED//////wFYP6wAAAAAAABQQP//////AVo/rAAAAAAAAFBA//////+BWz/DAAAAAAAAWUD//////wFaP8cAAAAAAABVQP//////AVg/tAAAAAAAoGJA//////9XXD+wAAAAAADAVkD//////wFfP8IAAAAAAMBcQP//////B14/xwAAAAAAAFZA//////9XXD/LAAAAAABAV0D//////wFYP88AAAAAAAAAAP//////AVA/0wAAAAAAgGJA//////93TD+0AAAAAAAAV0D//////wNkP7QAAAAAAIBbQP//////S2Y/xwAAAAAAAAAA//////8DZD/KAAAAAAAAYED//////0dgP7QAAAAAAABgQP//////A2g/xQAAAAAAwFZA//////8DaD+0AAAAAADAX0D//////wduP8YAAAAAAIBbQP//////T2w/ywAAAAAAAAAA//////8DaD/PAAAAAACAXUD//////0dgP7gAAAAAAABgQP//////B3A/uAAAAAAAAGBA//////8HeD+4AAAAAABAVkD//////z99P8sAAAAAAAAAAP//////B3g/zwAAAAAAAFdA//////8HcD/TAAAAAABgYED//////0dgP9cAAAAAAIBYQP//////d0w/vAAAAAAAAGBA//////8PiD/AAAAAAAAAYED//////x+QP9MAAAAAAIBcQP//////74A/xAAAAAAAAFhA//////8/oD/XAAAAAAAAAAD//////++AP98AAAAAAABZQP//////d0w/vAAAAAAAAExAAAAAAAAAacC4AAAAAAAAS0AAAAAAAKB0wLwAAAAAAEBdQAAAAAAAsHjAzwAAAAAAAFNAAAAAAACQcMDTAAAAAACAUkAAAAAAAOBgwLQAAAAAAABJQAAAAAAAcILAuAAAAAAAQFxAAAAAAAB4hMDLAAAAAAAAAAAAAAAAAGiAwLgAAAAAAABgQAAAAAAAAIjAuAAAAAAAAGBAAAAAAAAAjMDLAAAAAAAAAAAAAAAAAACIwM8AAAAAAAAAAAAAAAAAaIDAtAAAAAAAAGBAAAAAAAAAkMC0AAAAAAAAYEAAAAAAAACSwMcAAAAAAAAAAAAAAAAAAJDAtAAAAAAAQFlAAAAAAABslsDLAAAAAABAUEAAAAAAAACQwLQAAAAAAABgQAAAAAAAAJjAtAAAAAAAAGBAAAAAAAAAmsDHAAAAAAAAAAAAAAAAAACYwLQAAAAAAEBXQAAAAAAAjJ7AxgAAAAAAQFBAAAAAAACIncDLAAAAAACAWEAAAAAAAACYwM8AAAAAAAAAAAAAAAAAAJDA0wAAAAAAgFZAAAAAAABogMCwAAAAAAAAYEAAAAAAAACgwLAAAAAAAABgQAAAAAAAAKHAwwAAAAAAAAAAAAAAAAAAoMDFAAAAAADAYkAAAAAAAACgwLAAAAAAAABgQAAAAAAAAKTAsAAAAAAAAGBAAAAAAAAApcDDAAAAAAAAAAAAAAAAAACkwLQAAAAAAMBhQAAAAAAA5KbAxwAAAAAAgFxAAAAAAAAApMDLAAAAAAAAAAAAAAAAAACgwLAAAAAAAABgQAAAAAAAAKjAsAAAAAAAAGBAAAAAAAAAqcDDAAAAAAAAAAAAAAAAAACowMUAAAAAAMBgQAAAAAAAAKjAsAAAAAAAAGBAAAAAAAAArMCwAAAAAAAAYEAAAAAAAACtwMMAAAAAAAAAAAAAAAAAAKzAsAAAAAAAgF9AAAAAAAAEr8DHAAAAAABAYEAAAAAAAACswMsAAAAAAAAAAAAAAAAAAKjAzwAAAAAAgF5AAAAAAAAAoMCsAAAAAACAXkAAAAAAAIawwKwAAAAAAIBdQAAAAAAAirHAvgAAAAAAQGBAAAAAAAAIscDDAAAAAAAAAAAAAAAAAASwwKwAAAAAAIBcQAAAAAAAjrLAvgAAAAAAwGFAAAAAAAAAssCwAAAAAAAAYUAAAAAAAACzwMMAAAAAAAAAAAAAAAAAALLAxwAAAAAAQGFAAAAAAAAEsMDTAAAAAACAW0AAAAAAAACgwNcAAAAAAABUQAAAAAAAaIDA3wAAAAAAgFNAAAAAAACAUcD/AAAAAAAgYkD//////3dMPw==";
    std::string decodedBuf = decodeBase64(encodedBuf);

    QuantileDigest<double> digest{
        StlAllocator<double>(allocator()), decodedBuf.data()};

    double scaleFactor = 2.5;
    digest.scale(scaleFactor);

    std::string scaledBuf(digest.serializedByteSize(), '\0');
    digest.serialize(scaledBuf.data());

    // Expected serialized form after scaling
    std::string expectedScaledBuf = decodeBase64(
        "AK5H4XoUru8/AAAAAAAAAAAAAAAAAAAAAP//////d0y/AAAAAACHs0B0AAAArAAAAAAAAHRA//////93TD+oAAAAAADQa0D//////wBNP6gAAAAAAABpQP//////gE0/vwAAAAAA8GlA//////8ATT/DAAAAAAAAAAD//////3dMP6gAAAAAAABpQP//////gE4/rAAAAAAAAHRA//////8ATz/DAAAAAAAAAAD//////wBOP8cAAAAAAEBwQP//////d0w/rAAAAAAAAG5A//////8BUD+sAAAAAAAAZED//////wFRP8MAAAAAAAAAAP//////AVA/qAAAAAAAAGRA//////8BUj+sAAAAAAAAbkD//////wFTP8MAAAAAAAAAAP//////AVI/xwAAAAAAGHVA//////8BUD+wAAAAAAAAdED//////wFUP6wAAAAAANhzQP//////A1Y/qAAAAAAAAGRA//////8BVz/DAAAAAAAAAAD//////wNWP8cAAAAAANBxQP//////AVQ/ywAAAAAAkGpA//////8BUD+wAAAAAAAAdED//////wFYP6gAAAAAAABkQP//////AVo/qAAAAAAAAGRA//////+BWz/DAAAAAABAb0D//////wFaP8cAAAAAAEBqQP//////AVg/sAAAAAAASHdA//////9XXD+sAAAAAABwbED//////wFfP8IAAAAAAPhxQP//////B14/xwAAAAAAgGtA//////9XXD/LAAAAAAAQbUD//////wFYP88AAAAAAAAAAP//////AVA/0wAAAAAAIHdA//////93TD+wAAAAAADAbED//////wNkP7AAAAAAADBxQP//////S2Y/xwAAAAAAAAAA//////8DZD/KAAAAAAAAdED//////0dgP7AAAAAAAAB0QP//////A2g/xQAAAAAAcGxA//////8DaD+wAAAAAADYc0D//////wduP8YAAAAAADBxQP//////T2w/ywAAAAAAAAAA//////8DaD/PAAAAAABwckD//////0dgP7QAAAAAAAB0QP//////B3A/tAAAAAAAAHRA//////8HeD+0AAAAAADQa0D//////z99P8sAAAAAAAAAAP//////B3g/zwAAAAAAwGxA//////8HcD/TAAAAAAB4dED//////0dgP9cAAAAAAKBuQP//////d0w/uAAAAAAAAHRA//////8PiD+8AAAAAAAAdED//////x+QP9MAAAAAANBxQP//////74A/wAAAAAAAAG5A//////8/oD/XAAAAAAAAAAD//////++AP98AAAAAAEBvQP//////d0w/uAAAAAAAgGFAAAAAAAAAacC0AAAAAADgYEAAAAAAAKB0wLgAAAAAAEhyQAAAAAAAsHjAzwAAAAAAwGdAAAAAAACQcMDTAAAAAAAgZ0AAAAAAAOBgwLAAAAAAAEBfQAAAAAAAcILAtAAAAAAAqHFAAAAAAAB4hMDLAAAAAAAAAAAAAAAAAGiAwLQAAAAAAAB0QAAAAAAAAIjAtAAAAAAAAHRAAAAAAAAAjMDLAAAAAAAAAAAAAAAAAACIwM8AAAAAAAAAAAAAAAAAaIDAsAAAAAAAAHRAAAAAAAAAkMCwAAAAAAAAdEAAAAAAAACSwMcAAAAAAAAAAAAAAAAAAJDAsAAAAAAAkG9AAAAAAABslsDLAAAAAABQZEAAAAAAAACQwLAAAAAAAAB0QAAAAAAAAJjAsAAAAAAAAHRAAAAAAAAAmsDHAAAAAAAAAAAAAAAAAACYwLAAAAAAABBtQAAAAAAAjJ7AxgAAAAAAUGRAAAAAAACIncDLAAAAAACgbkAAAAAAAACYwM8AAAAAAAAAAAAAAAAAAJDA0wAAAAAAIGxAAAAAAABogMCsAAAAAAAAdEAAAAAAAACgwKwAAAAAAAB0QAAAAAAAAKHAwwAAAAAAAAAAAAAAAAAAoMDFAAAAAABwd0AAAAAAAACgwKwAAAAAAAB0QAAAAAAAAKTArAAAAAAAAHRAAAAAAAAApcDDAAAAAAAAAAAAAAAAAACkwLAAAAAAADB2QAAAAAAA5KbAxwAAAAAA0HFAAAAAAAAApMDLAAAAAAAAAAAAAAAAAACgwKwAAAAAAAB0QAAAAAAAAKjArAAAAAAAAHRAAAAAAAAAqcDDAAAAAAAAAAAAAAAAAACowMUAAAAAAPB0QAAAAAAAAKjArAAAAAAAAHRAAAAAAAAArMCsAAAAAAAAdEAAAAAAAACtwMMAAAAAAAAAAAAAAAAAAKzArAAAAAAAsHNAAAAAAAAEr8DHAAAAAABQdEAAAAAAAACswMsAAAAAAAAAAAAAAAAAAKjAzwAAAAAAEHNAAAAAAAAAoMCoAAAAAAAQc0AAAAAAAIawwKgAAAAAAHByQAAAAAAAirHAvgAAAAAAUHRAAAAAAAAIscDDAAAAAAAAAAAAAAAAAASwwKgAAAAAANBxQAAAAAAAjrLAvgAAAAAAMHZAAAAAAAAAssCsAAAAAABAdUAAAAAAAACzwMMAAAAAAAAAAAAAAAAAALLAxwAAAAAAkHVAAAAAAAAEsMDTAAAAAAAwcUAAAAAAAACgwNcAAAAAAABpQAAAAAAAaIDA3wAAAAAAYGhAAAAAAACAUcD/AAAAAACodkD//////3dMPw==");
    ASSERT_EQ(scaledBuf, expectedScaledBuf);
  }

  // Test with float QDigest
  {
    std::string encodedBuf =
        "AK5H4XoUru8/AAAAAAAAAAAAAAAAAAAAAP+/Y7r/////ADicRQAAAABuAAAAPAAAAAAA4GJA/wdouv///39OAAAAAAAAZUD/v2O6////fzwAAAAAAEBiQP8fcLr///9/PAAAAAAAIGNA/wd4uv///39PAAAAAACAYkD/H3C6////f1MAAAAAAMBbQP+/Y7r///9/QAAAAAAAwGJA/w+Auv///388AAAAAACAXED/75C6////f1MAAAAAAMBhQP8PgLr///9/PAAAAAAAAGBA/w+guv///388AAAAAACAX0D/L7C6////fzgAAAAAAABLQP+vvLr///9/TwAAAAAAAAAA/y+wuv///39TAAAAAABgYkD/D6C6////f1cAAAAAAMBaQP8PgLr///9/PAAAAAAAwFpA/1/Buv///388AAAAAAAAYED/D8i6////f08AAAAAAAAAAP9fwbr///9/UQAAAAAAwGRA/1/Buv///388AAAAAADAXUD/n+C6////fzwAAAAAAABgQP8P6Lr///9/TwAAAAAAAAAA/5/guv///39AAAAAAAAgY0D/f/a6////f1MAAAAAAMBZQP+f4Lr///9/VwAAAAAAAFVA/1/Buv///39bAAAAAACAVUD/D4C6////f18AAAAAAAAAAP+/Y7r///9/QAAAAAAAQFxA//8hu////39AAAAAAAAAYED/HzC7////f1MAAAAAAAAAAP//Ibv///9/VgAAAAAAQFZA//8Hu////39AAAAAAAAAYED/H0C7////f0AAAAAAAABgQP8fULv///9/UwAAAAAAAAAA/x9Au////39EAAAAAAAgYED//2+7////f1cAAAAAAMBfQP8fQLv///9/WwAAAAAAAAAA//8Hu////39EAAAAAABAUkD//627////f0QAAAAAAMBXQP9/6Lv///9/VgAAAAAAgGNA/3/Bu////39bAAAAAAAAAAD//4a7////f18AAAAAACBkQP//B7v///9/YwAAAAAAQGJA/79juv///39IAAAAAABAWED//0+8////f0wAAAAAAEBcQP//j7z///9/XwAAAAAAYGNA//8JvP///39QAAAAAAAAWED//wG9////f2MAAAAAAAAAAP//Cbz///9/ZQAAAAAAAD5A//8JvP///39rAAAAAAAAAAD/v2O6////f3kAAAAAAEBdQP+/Y7r///9/SAAAAAAAAExAAABIQwAAAIBEAAAAAAAAS0AAAKVDAAAAgEgAAAAAAEBdQACAxUMAAACAWwAAAAAAAFNAAICEQwAAAIBfAAAAAACAUkAAAAdDAAAAgEAAAAAAAABJQACAE0QAAACARAAAAAAAQFxAAMAjRAAAAIBXAAAAAAAAAAAAQANEAAAAgEQAAAAAAABgQAAAQEQAAACARAAAAAAAAGBAAABgRAAAAIBXAAAAAAAAAAAAAEBEAAAAgFsAAAAAAAAAAABAA0QAAACAQAAAAAAAAGBAAACARAAAAIBAAAAAAAAAYEAAAJBEAAAAgFMAAAAAAAAAAAAAgEQAAACAQAAAAAAAQFlAAGCzRAAAAIBXAAAAAABAUEAAAIBEAAAAgEAAAAAAAABgQAAAwEQAAACAQAAAAAAAAGBAAADQRAAAAIBTAAAAAAAAAAAAAMBEAAAAgEAAAAAAAEBXQABg9EQAAACAUgAAAAAAQFBAAEDsRAAAAIBXAAAAAACAWEAAAMBEAAAAgFsAAAAAAAAAAAAAgEQAAACAXwAAAAAAgFZAAEADRAAAAIA8AAAAAAAAYEAAAABFAAAAgDwAAAAAAABgQAAACEUAAACATwAAAAAAAAAAAAAARQAAAIBRAAAAAADAYkAAAABFAAAAgDwAAAAAAABgQAAAIEUAAACAPAAAAAAAAGBAAAAoRQAAAIBPAAAAAAAAAAAAACBFAAAAgEAAAAAAAMBhQAAgN0UAAACAUwAAAAAAgFxAAAAgRQAAAIBXAAAAAAAAAAAAAABFAAAAgDwAAAAAAABgQAAAQEUAAACAPAAAAAAAAGBAAABIRQAAAIBPAAAAAAAAAAAAAEBFAAAAgFEAAAAAAMBgQAAAQEUAAACAPAAAAAAAAGBAAABgRQAAAIA8AAAAAAAAYEAAAGhFAAAAgE8AAAAAAAAAAAAAYEUAAACAPAAAAAAAgF9AACB4RQAAAIBTAAAAAABAYEAAAGBFAAAAgFcAAAAAAAAAAAAAQEUAAACAWwAAAAAAgF5AAAAARQAAAIA4AAAAAACAXkAAMIRFAAAAgDgAAAAAAIBdQABQjEUAAACASgAAAAAAQGBAAECIRQAAAIBPAAAAAAAAAAAAIIBFAAAAgDgAAAAAAIBcQABwlEUAAACASgAAAAAAwGFAAACQRQAAAIA8AAAAAAAAYUAAAJhFAAAAgE8AAAAAAAAAAAAAkEUAAACAUwAAAAAAQGFAACCARQAAAIBfAAAAAACAW0AAAABFAAAAgGMAAAAAAABUQABAA0QAAACAawAAAAAAgFNAAACMQgAAAID/AAAAAACAUUD/v2O6////fw==";
    std::string decodedBuf = decodeBase64(encodedBuf);

    QuantileDigest<float> digest{
        StlAllocator<float>(allocator()), decodedBuf.data()};

    // Scale the digest
    double scaleFactor = 2.5;
    digest.scale(scaleFactor);

    // Serialize the scaled digest
    std::string scaledBuf(digest.serializedByteSize(), '\0');
    digest.serialize(scaledBuf.data());

    // Expected serialized form after scaling
    std::string expectedScaledBuf = decodeBase64(
        "AK5H4XoUru8/AAAAAAAAAAAAAAAAAAAAAP+/Y7r/////ADicRQAAAABuAAAAOAAAAAAAmHdA/wdouv///39OAAAAAABAekD/v2O6////fzgAAAAAANB2QP8fcLr///9/OAAAAAAA6HdA/wd4uv///39PAAAAAAAgd0D/H3C6////f1MAAAAAAFhxQP+/Y7r///9/PAAAAAAAcHdA/w+Auv///384AAAAAADQcUD/75C6////f1MAAAAAADB2QP8PgLr///9/OAAAAAAAAHRA/w+guv///384AAAAAACwc0D/L7C6////fzQAAAAAAOBgQP+vvLr///9/TwAAAAAAAAAA/y+wuv///39TAAAAAAD4dkD/D6C6////f1cAAAAAALhwQP8PgLr///9/OAAAAAAAuHBA/1/Buv///384AAAAAAAAdED/D8i6////f08AAAAAAAAAAP9fwbr///9/UQAAAAAA8HlA/1/Buv///384AAAAAACYckD/n+C6////fzgAAAAAAAB0QP8P6Lr///9/TwAAAAAAAAAA/5/guv///388AAAAAADod0D/f/a6////f1MAAAAAABhwQP+f4Lr///9/VwAAAAAAQGpA/1/Buv///39bAAAAAADgakD/D4C6////f18AAAAAAAAAAP+/Y7r///9/PAAAAAAAqHFA//8hu////388AAAAAAAAdED/HzC7////f1MAAAAAAAAAAP//Ibv///9/VgAAAAAA0GtA//8Hu////388AAAAAAAAdED/H0C7////fzwAAAAAAAB0QP8fULv///9/UwAAAAAAAAAA/x9Au////39AAAAAAAAodED//2+7////f1cAAAAAANhzQP8fQLv///9/WwAAAAAAAAAA//8Hu////39AAAAAAADQZkD//627////f0AAAAAAALBtQP9/6Lv///9/VgAAAAAAYHhA/3/Bu////39bAAAAAAAAAAD//4a7////f18AAAAAACh5QP//B7v///9/YwAAAAAA0HZA/79juv///39EAAAAAABQbkD//0+8////f0gAAAAAAKhxQP//j7z///9/XwAAAAAAOHhA//8JvP///39MAAAAAAAAbkD//wG9////f2MAAAAAAAAAAP//Cbz///9/ZQAAAAAAwFJA//8JvP///39rAAAAAAAAAAD/v2O6////f3kAAAAAAEhyQP+/Y7r///9/RAAAAAAAgGFAAABIQwAAAIBAAAAAAADgYEAAAKVDAAAAgEQAAAAAAEhyQACAxUMAAACAWwAAAAAAwGdAAICEQwAAAIBfAAAAAAAgZ0AAAAdDAAAAgDwAAAAAAEBfQACAE0QAAACAQAAAAAAAqHFAAMAjRAAAAIBXAAAAAAAAAAAAQANEAAAAgEAAAAAAAAB0QAAAQEQAAACAQAAAAAAAAHRAAABgRAAAAIBXAAAAAAAAAAAAAEBEAAAAgFsAAAAAAAAAAABAA0QAAACAPAAAAAAAAHRAAACARAAAAIA8AAAAAAAAdEAAAJBEAAAAgFMAAAAAAAAAAAAAgEQAAACAPAAAAAAAkG9AAGCzRAAAAIBXAAAAAABQZEAAAIBEAAAAgDwAAAAAAAB0QAAAwEQAAACAPAAAAAAAAHRAAADQRAAAAIBTAAAAAAAAAAAAAMBEAAAAgDwAAAAAABBtQABg9EQAAACAUgAAAAAAUGRAAEDsRAAAAIBXAAAAAACgbkAAAMBEAAAAgFsAAAAAAAAAAAAAgEQAAACAXwAAAAAAIGxAAEADRAAAAIA4AAAAAAAAdEAAAABFAAAAgDgAAAAAAAB0QAAACEUAAACATwAAAAAAAAAAAAAARQAAAIBRAAAAAABwd0AAAABFAAAAgDgAAAAAAAB0QAAAIEUAAACAOAAAAAAAAHRAAAAoRQAAAIBPAAAAAAAAAAAAACBFAAAAgDwAAAAAADB2QAAgN0UAAACAUwAAAAAA0HFAAAAgRQAAAIBXAAAAAAAAAAAAAABFAAAAgDgAAAAAAAB0QAAAQEUAAACAOAAAAAAAAHRAAABIRQAAAIBPAAAAAAAAAAAAAEBFAAAAgFEAAAAAAPB0QAAAQEUAAACAOAAAAAAAAHRAAABgRQAAAIA4AAAAAAAAdEAAAGhFAAAAgE8AAAAAAAAAAAAAYEUAAACAOAAAAAAAsHNAACB4RQAAAIBTAAAAAABQdEAAAGBFAAAAgFcAAAAAAAAAAAAAQEUAAACAWwAAAAAAEHNAAAAARQAAAIA0AAAAAAAQc0AAMIRFAAAAgDQAAAAAAHByQABQjEUAAACASgAAAAAAUHRAAECIRQAAAIBPAAAAAAAAAAAAIIBFAAAAgDQAAAAAANBxQABwlEUAAACASgAAAAAAMHZAAACQRQAAAIA4AAAAAABAdUAAAJhFAAAAgE8AAAAAAAAAAAAAkEUAAACAUwAAAAAAkHVAACCARQAAAIBfAAAAAAAwcUAAAABFAAAAgGMAAAAAAABpQABAA0QAAACAawAAAAAAYGhAAACMQgAAAID/AAAAAADgZUD/v2O6////fw==");

    ASSERT_EQ(scaledBuf, expectedScaledBuf);
  }
}

TEST_F(QuantileDigestTest, hugeWeight) {
  VELOX_ASSERT_THROW(
      testHugeWeight<int64_t>(), "Weighted count in digest is too large");
  VELOX_ASSERT_THROW(
      testHugeWeight<double>(), "Weighted count in digest is too large");
  VELOX_ASSERT_THROW(
      testHugeWeight<float>(), "Weighted count in digest is too large");
}

TEST_F(QuantileDigestTest, quantileAtValue) {
  testQuantileAtValue<int64_t>();
  testQuantileAtValue<double>();
  testQuantileAtValue<float>();
}

} // namespace facebook::velox::functions

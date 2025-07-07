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

#include "velox/functions/lib/TDigest.h"

#include <random>

#include <folly/base64.h>
#include <gtest/gtest.h>

#include "velox/common/testutil/RandomSeed.h"
#include "velox/functions/lib/tests/QuantileDigestTestBase.h"

namespace facebook::velox::functions {
namespace {

constexpr double kSumError = 1e-4;
constexpr double kRankError = 0.01;

class TDigestTest : public QuantileDigestTestBase {};

TEST_F(TDigestTest, addElementsInOrder) {
  constexpr int N = 1e6;
  TDigest digest;
  ASSERT_EQ(digest.compression(), tdigest::kDefaultCompression);
  std::vector<int16_t> positions;
  for (int i = 0; i < N; ++i) {
    digest.add(positions, i);
  }
  digest.compress(positions);
  ASSERT_NEAR(digest.sum(), 1.0 * N * (N - 1) / 2, kSumError);
  for (auto q : kQuantiles) {
    auto v = digest.estimateQuantile(q);
    ASSERT_NEAR(v / (N - 1), q, kRankError);
  }
}

TEST_F(TDigestTest, addElementsRandomized) {
  constexpr int N = 1e5;
  double values[N];
  TDigest digest;
  std::vector<int16_t> positions;
  std::default_random_engine gen(common::testutil::getRandomSeed(42));
  std::uniform_real_distribution<> dist;
  for (int i = 0; i < N; ++i) {
    auto v = dist(gen);
    digest.add(positions, v);
    values[i] = v;
  }
  digest.compress(positions);
  std::sort(std::begin(values), std::end(values));
  CHECK_QUANTILES(folly::Range(values, N), digest, kSumError, kRankError);
}

TEST_F(TDigestTest, fewElements) {
  TDigest digest;
  std::vector<int16_t> positions;
  digest.compress(positions);
  ASSERT_EQ(digest.sum(), 0);
  for (auto q : kQuantiles) {
    ASSERT_TRUE(std::isnan(digest.estimateQuantile(q)));
  }
  digest.add(positions, 1.0);
  digest.compress(positions);
  ASSERT_EQ(digest.sum(), 1);
  for (auto q : kQuantiles) {
    ASSERT_EQ(digest.estimateQuantile(q), 1.0);
  }
}

// IMPORTANT: All these errors cannot be caught by TRY in Presto, so we should
// not make them user errors.  If in another engine these are catchable errors,
// throw user errors in the corresponding UDFs before they reach the TDigest
// implementation.
TEST_F(TDigestTest, invalid) {
  TDigest digest;
  ASSERT_THROW(digest.setCompression(NAN), VeloxRuntimeError);
  ASSERT_THROW(digest.setCompression(0), VeloxRuntimeError);
  ASSERT_THROW(digest.setCompression(1000.1), VeloxRuntimeError);
  std::vector<int16_t> positions;
  ASSERT_THROW(digest.add(positions, NAN), VeloxRuntimeError);
  ASSERT_THROW(digest.add(positions, 1, 0), VeloxRuntimeError);
  ASSERT_THROW(digest.estimateQuantile(1.1), VeloxRuntimeError);
}

TEST_F(TDigestTest, unalignedSerialization) {
  constexpr int N = 1e4;
  TDigest digest;
  std::vector<int16_t> positions;
  for (int i = 0; i < N; ++i) {
    digest.add(positions, i);
  }
  digest.compress(positions);
  ASSERT_NEAR(digest.sum(), 1.0 * N * (N - 1) / 2, kSumError);
  std::string buf(1 + digest.serializedByteSize(), '\0');
  for (int offset = 0; offset < 2; ++offset) {
    SCOPED_TRACE(fmt::format("offset={}", offset));
    digest.serialize(buf.data() + offset);
    TDigest digest2;
    digest2.mergeDeserialized(positions, buf.data() + offset);
    digest2.compress(positions);
    for (auto q : kQuantiles) {
      auto v = digest2.estimateQuantile(q);
      ASSERT_NEAR(v / (N - 1), q, kRankError);
    }
  }
}

TEST_F(TDigestTest, mergeEmpty) {
  std::vector<int16_t> positions;
  TDigest<> digests[2];
  std::string buf(digests[1].serializedByteSize(), '\0');
  digests[1].serialize(buf.data());
  digests[0].mergeDeserialized(positions, buf.data());
  digests[0].compress(positions);
  ASSERT_EQ(digests[0].sum(), 0);
  ASSERT_TRUE(std::isnan(digests[0].estimateQuantile(0.5)));
  digests[0].add(positions, 1.0);
  digests[0].compress(positions);
  ASSERT_EQ(digests[0].sum(), 1);
  ASSERT_EQ(digests[0].estimateQuantile(0.5), 1);
  digests[0].mergeDeserialized(positions, buf.data());
  digests[0].compress(positions);
  ASSERT_EQ(digests[0].sum(), 1);
  ASSERT_EQ(digests[0].estimateQuantile(0.5), 1);
}

TEST_F(TDigestTest, deserializeJava) {
  std::vector<int16_t> positions;
  {
    SCOPED_TRACE(
        "select to_base64(cast(tdigest_agg(x) as varbinary)) from (values (2.0)) as t(x)");
    auto data = decodeBase64(
        "AQAAAAAAAAAAQAAAAAAAAABAAAAAAAAAAEAAAAAAAABZQAAAAAAAAPA/AQAAAAAAAAAAAPA/AAAAAAAAAEA=");
    TDigest digest;
    digest.mergeDeserialized(positions, data.data());
    digest.compress(positions);
    ASSERT_EQ(digest.compression(), tdigest::kDefaultCompression);
    ASSERT_EQ(digest.sum(), 2.0);
    for (auto q : kQuantiles) {
      ASSERT_EQ(digest.estimateQuantile(q), 2.0);
    }
  }
  {
    SCOPED_TRACE(
        "select to_base64(cast(tdigest_agg(x, w, c) as varbinary)) from (values (2.0, 2, 200.0)) as t(x, w, c)");
    auto data = decodeBase64(
        "AQAAAAAAAAAAQAAAAAAAAABAAAAAAAAAEEAAAAAAAABpQAAAAAAAAABAAQAAAAAAAAAAAABAAAAAAAAAAEA=");
    TDigest digest;
    digest.setCompression(200);
    digest.mergeDeserialized(positions, data.data());
    digest.compress(positions);
    ASSERT_EQ(digest.compression(), 200);
    ASSERT_EQ(digest.sum(), 4.0);
    for (auto q : kQuantiles) {
      ASSERT_EQ(digest.estimateQuantile(q), 2.0);
    }
  }
  {
    SCOPED_TRACE(
        "select to_base64(cast(tdigest_agg(cast(x as double)) as varbinary)) from unnest(sequence(0, 1000)) as t(x)");
    auto data = decodeBase64(
        "AQAAAAAAAAAAAAAAAAAAQI9AAAAAAFCMHkEAAAAAAABZQAAAAAAASI9AMgAAAAAAAAAAAPA/AAAAAAAA8D8AAAAAAADwPwAAAAAAAPA/AAAAAAAA8D8AAAAAAADwPwAAAAAAAPA/AAAAAAAAAEAAAAAAAAAAQAAAAAAAAAhAAAAAAAAAEEAAAAAAAAAUQAAAAAAAABxAAAAAAAAAIkAAAAAAAAAoQAAAAAAAADBAAAAAAAAANEAAAAAAAAA6QAAAAAAAgEBAAAAAAACAREAAAAAAAABJQAAAAAAAAE5AAAAAAABAUUAAAAAAAEBTQAAAAAAAgFRAAAAAAADAU0AAAAAAAABSQAAAAAAAAFBAAAAAAAAAS0AAAAAAAIBGQAAAAAAAAEJAAAAAAAAAPUAAAAAAAAA2QAAAAAAAADFAAAAAAAAAKkAAAAAAAAAkQAAAAAAAACBAAAAAAAAAGEAAAAAAAAAUQAAAAAAAAAhAAAAAAAAACEAAAAAAAAAAQAAAAAAAAPA/AAAAAAAA8D8AAAAAAADwPwAAAAAAAPA/AAAAAAAA8D8AAAAAAADwPwAAAAAAAPA/AAAAAAAA8D8AAAAAAAAAAAAAAAAAAPA/AAAAAAAAAEAAAAAAAAAIQAAAAAAAABBAAAAAAAAAFEAAAAAAAAAYQAAAAAAAAB5AAAAAAAAAI0AAAAAAAAAoQAAAAAAAAC9AAAAAAAAANEAAAAAAAAA6QAAAAAAAAEFAAAAAAABARkAAAAAAAEBNQAAAAAAAIFNAAAAAAADgWEAAAAAAACBgQAAAAAAAwGRAAAAAAABwakAAAAAAAKhwQAAAAAAAsHRAAAAAAABAeUAAAAAAADh+QAAAAAAAoIFAAAAAAAD8g0AAAAAAAByGQAAAAAAA9IdAAAAAAACAiUAAAAAAAMSKQAAAAAAAyItAAAAAAACUjEAAAAAAADCNQAAAAAAAqI1AAAAAAAAEjkAAAAAAAEyOQAAAAAAAhI5AAAAAAACwjkAAAAAAANCOQAAAAAAA6I5AAAAAAAD8jkAAAAAAAAiPQAAAAAAAEI9AAAAAAAAYj0AAAAAAACCPQAAAAAAAKI9AAAAAAAAwj0AAAAAAADiPQAAAAAAAQI9A");
    TDigest digest;
    digest.mergeDeserialized(positions, data.data());
    digest.compress(positions);
    ASSERT_NEAR(digest.sum(), 500500, kSumError);
    for (auto q : kQuantiles) {
      auto v = digest.estimateQuantile(q);
      ASSERT_NEAR(v / 1000, q, kRankError);
    }
  }
  {
    SCOPED_TRACE(
        "select to_base64(cast(tdigest_agg(cast(x as double), 1, 50) as varbinary)) as x from unnest(sequence(0, 1000)) as t(x)");
    auto data = decodeBase64(
        "AQAAAAAAAAAAAAAAAAAAQI9AAAAAAFCMHkEAAAAAAABJQAAAAAAASI9AHAAAAAAAAAAAAPA/AAAAAAAA8D8AAAAAAADwPwAAAAAAAABAAAAAAAAAAEAAAAAAAAAQQAAAAAAAABxAAAAAAAAAJkAAAAAAAAAyQAAAAAAAAD5AAAAAAACAR0AAAAAAAABRQAAAAAAAQFZAAAAAAAAgYkAAAAAAAMBkQAAAAAAAYGFAAAAAAABAWUAAAAAAAIBQQAAAAAAAgERAAAAAAAAAOUAAAAAAAAAuQAAAAAAAACJAAAAAAAAAFEAAAAAAAAAIQAAAAAAAAABAAAAAAAAA8D8AAAAAAADwPwAAAAAAAPA/AAAAAAAAAAAAAAAAAADwPwAAAAAAAABAAAAAAAAADEAAAAAAAAAWQAAAAAAAACFAAAAAAAAALEAAAAAAAAA3QAAAAAAAwEJAAAAAAADATkAAAAAAAABZQAAAAAAAsGNAAAAAAACAbUAAAAAAABB2QAAAAAAAyH9AAAAAAACohEAAAAAAAGiIQAAAAAAABItAAAAAAACwjEAAAAAAALiNQAAAAAAAWI5AAAAAAAC4jkAAAAAAAPCOQAAAAAAAEI9AAAAAAAAkj0AAAAAAADCPQAAAAAAAOI9AAAAAAABAj0A=");
    TDigest digest;
    digest.setCompression(50);
    digest.mergeDeserialized(positions, data.data());
    digest.compress(positions);
    ASSERT_NEAR(digest.sum(), 500500, kSumError);
    for (auto q : kQuantiles) {
      auto v = digest.estimateQuantile(q);
      ASSERT_NEAR(v / 1000, q, kRankError);
    }
  }
  {
    SCOPED_TRACE(
        "select to_base64(cast(tdigest_agg(x, w) as varbinary)) from (values (0.0, 1), (1.0, 100)) as t(x, w)");
    auto data = decodeBase64(
        "AQAAAAAAAAAAAAAAAAAAAPA/AAAAAAAAWUAAAAAAAABZQAAAAAAAQFlAAgAAAAAAAAAAAPA/AAAAAAAAWUAAAAAAAAAAAAAAAAAAAPA/");
    TDigest digest;
    digest.mergeDeserialized(positions, data.data());
    digest.compress(positions);
    double values[101];
    values[0] = 0;
    std::fill(values + 1, values + 101, 1);
    CHECK_QUANTILES(folly::Range(values, 101), digest, kSumError, kRankError);
  }
  {
    SCOPED_TRACE(
        "select to_base64(cast(tdigest_agg(cast(x as double), 1001 - x) as varbinary)) from unnest(sequence(1, 1000)) as t(x)");
    auto data = decodeBase64(
        "AQAAAAAAAADwPwAAAAAAQI9AAAAAMIjto0EAAAAAAABZQAAAAABQjB5BLAAAAAAAAAAAQI9AAAAAAAA4j0AAAAAAADCPQAAAAAAAKI9AAAAAAAAcn0AAAAAAAEanQAAAAAAAUbNAAAAAAADhukAAAAAAAO3EQAAAAAAA4M9AAAAAAEDU10AAAAAAwEDhQAAAAABA4edAAAAAAEB57kAAAAAAwELxQAAAAABgiu1AAAAAAKAE50AAAAAA4LzgQAAAAADAANdAAAAAAABYz0AAAAAAAHzEQAAAAAAAqbpAAAAAAAD0sEAAAAAAAOClQAAAAAAA+JtAAAAAAADgkUAAAAAAAJCFQAAAAAAAEH1AAAAAAADAckAAAAAAAOBmQAAAAAAAQF9AAAAAAACAVEAAAAAAAIBJQAAAAAAAAEVAAAAAAAAAN0AAAAAAAAAzQAAAAAAAACBAAAAAAAAAHEAAAAAAAAAYQAAAAAAAABRAAAAAAAAAEEAAAAAAAAAIQAAAAAAAAABAAAAAAAAA8D8AAAAAAADwPwAAAAAAAABAAAAAAAAACEAAAAAAAAAQQCT88Sq+/xVAQP1fAVD/H0C9Xrrw9v4nQHwxjlL1/jFAoIdSJV/9OkDMzMzMzHxEQM6g0QNUOE9AN9zhXw23V0AB/5K759VhQNPiJszvSmpATJIkSZK8ckDxSWWlSwl5QMvdEa6CIH9A8tiKoOFUgkCutAVRS7qEQHt4eHh4v4ZAwq5fKvlsiECiNVqjNcqJQLdt27Zt44pAJEmSJEnEi0CGUZ0mB3mMQN/NI1SfCY1ACis2j1d6jUBTSimllNKNQHsUrkfhGo5Aul0rJzxTjkCQwvUoXH+OQPQxOB+Do45AsK+vr6+/jkB6nud5nteOQOpNb3rT645AvYbyGsr7jkAAAAAAAAiPQAAAAAAAEI9AAAAAAAAYj0AAAAAAACCPQAAAAAAAKI9AAAAAAAAwj0AAAAAAADiPQAAAAAAAQI9A");
    TDigest digest;
    digest.mergeDeserialized(positions, data.data());
    digest.compress(positions);
    std::vector<double> values;
    values.reserve(500500);
    for (int i = 1; i <= 1000; ++i) {
      values.insert(values.end(), 1001 - i, i);
    }
    CHECK_QUANTILES(values, digest, kSumError, kRankError);
  }
}

TEST_F(TDigestTest, mergeJava) {
  // select to_base64(cast(tdigest_agg(cast(x as double)) as varbinary)) from
  // unnest(sequence(0, 999, 2)) as t(x)
  auto javaData = decodeBase64(
      "AQAAAAAAAAAAAAAAAAAAMI9AAAAAAOB0DkEAAAAAAABZQAAAAAAAQH9ALwAAAAAAAAAAAPA/AAAAAAAA8D8AAAAAAADwPwAAAAAAAPA/AAAAAAAA8D8AAAAAAADwPwAAAAAAAPA/AAAAAAAAAEAAAAAAAAAAQAAAAAAAAAhAAAAAAAAAEEAAAAAAAAAUQAAAAAAAABhAAAAAAAAAIEAAAAAAAAAkQAAAAAAAAChAAAAAAAAALkAAAAAAAAAzQAAAAAAAADdAAAAAAAAAO0AAAAAAAAA/QAAAAAAAAEFAAAAAAAAAQkAAAAAAAIBCQAAAAAAAgEFAAAAAAAAAQEAAAAAAAAA8QAAAAAAAADhAAAAAAAAANEAAAAAAAAAwQAAAAAAAACpAAAAAAAAAJkAAAAAAAAAgQAAAAAAAABxAAAAAAAAAFEAAAAAAAAAQQAAAAAAAAAhAAAAAAAAAAEAAAAAAAAAAQAAAAAAAAABAAAAAAAAA8D8AAAAAAADwPwAAAAAAAPA/AAAAAAAA8D8AAAAAAADwPwAAAAAAAPA/AAAAAAAA8D8AAAAAAAAAAAAAAAAAAABAAAAAAAAAEEAAAAAAAAAYQAAAAAAAACBAAAAAAAAAJEAAAAAAAAAoQAAAAAAAAC5AAAAAAAAAM0AAAAAAAAA4QAAAAAAAAD9AAAAAAAAAREAAAAAAAIBJQAAAAAAAQFBAAAAAAADAVEAAAAAAAEBaQAAAAAAAgGBAAAAAAADAZEAAAAAAAABqQAAAAAAAIHBAAAAAAADAc0AAAAAAANB3QAAAAAAAMHxAAAAAAABggEAAAAAAAKCCQAAAAAAAuIRAAAAAAACYhkAAAAAAADiIQAAAAAAAmIlAAAAAAAC4ikAAAAAAAKCLQAAAAAAAYIxAAAAAAAD4jEAAAAAAAHCNQAAAAAAA0I1AAAAAAAAYjkAAAAAAAFCOQAAAAAAAeI5AAAAAAACYjkAAAAAAALiOQAAAAAAA0I5AAAAAAADgjkAAAAAAAPCOQAAAAAAAAI9AAAAAAAAQj0AAAAAAACCPQAAAAAAAMI9A");
  std::vector<int16_t> positions;
  auto makeTDigest = [&] {
    TDigest<> digest;
    for (int i = 0; i < 500; ++i) {
      digest.add(positions, 1 + 2 * i);
    }
    return digest;
  };
  auto validate = [](const TDigest<>& digest) {
    ASSERT_NEAR(digest.sum(), 499500, kSumError);
    for (auto q : kQuantiles) {
      auto v = digest.estimateQuantile(q);
      ASSERT_NEAR(v / 1000, q, kRankError);
    }
  };
  {
    SCOPED_TRACE("C++ + Java");
    auto digest = makeTDigest();
    digest.mergeDeserialized(positions, javaData.data());
    digest.compress(positions);
    validate(digest);
  }
  {
    SCOPED_TRACE("Java + C++");
    auto digest = makeTDigest();
    digest.compress(positions);
    std::string buf(digest.serializedByteSize(), '\0');
    digest.serialize(buf.data());
    TDigest<> javaDigest;
    javaDigest.mergeDeserialized(positions, javaData.data());
    javaDigest.mergeDeserialized(positions, buf.data());
    javaDigest.compress(positions);
    validate(javaDigest);
  }
}

TEST_F(TDigestTest, mergeNoOverlap) {
  constexpr int N = 1e5;
  TDigest<> digests[2];
  std::vector<int16_t> positions;
  for (int i = 0; i < N; ++i) {
    digests[0].add(positions, i);
    digests[1].add(positions, i + N);
  }
  digests[1].compress(positions);
  std::string buf(digests[1].serializedByteSize(), '\0');
  digests[1].serialize(buf.data());
  digests[0].mergeDeserialized(positions, buf.data());
  digests[0].compress(positions);
  ASSERT_NEAR(digests[0].sum(), N * (2.0 * N - 1), kSumError);
  for (auto q : kQuantiles) {
    auto v = digests[0].estimateQuantile(q);
    ASSERT_NEAR(v / (2 * N - 1), q, kRankError);
  }
}

TEST_F(TDigestTest, mergeOverlap) {
  constexpr int N = 1e5;
  TDigest digest;
  std::vector<int16_t> positions;
  std::vector<double> values;
  values.reserve(2 * N);
  for (int i = 0; i < N; ++i) {
    digest.add(positions, i);
    values.insert(values.end(), 2, i);
  }
  digest.compress(positions);
  std::string buf(digest.serializedByteSize(), '\0');
  digest.serialize(buf.data());
  digest.mergeDeserialized(positions, buf.data());
  digest.compress(positions);
  CHECK_QUANTILES(values, digest, kSumError, kRankError);
}

TEST_F(TDigestTest, normalDistribution) {
  constexpr int N = 1e5;
  std::vector<int16_t> positions;
  double values[N];
  std::default_random_engine gen(common::testutil::getRandomSeed(42));
  for (double mean : {0, 1000}) {
    SCOPED_TRACE(fmt::format("mean={}", mean));
    std::normal_distribution<> dist(mean, 1);
    TDigest digest;
    for (int i = 0; i < N; ++i) {
      auto v = dist(gen);
      digest.add(positions, v);
      values[i] = v;
    }
    digest.compress(positions);
    std::sort(values, values + N);
    CHECK_QUANTILES(folly::Range(values, N), digest, kSumError, kRankError);
  }
}

TEST_F(TDigestTest, addWeighed) {
  std::vector<int16_t> positions;
  TDigest digest;
  std::vector<double> values;
  values.reserve(5050);
  for (int i = 1; i <= 100; ++i) {
    digest.add(positions, i, i);
    values.insert(values.end(), i, i);
  }
  digest.compress(positions);
  CHECK_QUANTILES(values, digest, kSumError, kRankError);
}

TEST_F(TDigestTest, merge) {
  std::vector<int16_t> positions;
  std::default_random_engine gen(common::testutil::getRandomSeed(42));
  std::vector<double> values;
  std::string buf;
  auto test = [&](int numDigests, int size, double mean, double stddev) {
    SCOPED_TRACE(fmt::format(
        "numDigests={} size={} mean={} stddev={}",
        numDigests,
        size,
        mean,
        stddev));
    values.clear();
    values.reserve(numDigests * size);
    std::normal_distribution<> dist(mean, stddev);
    TDigest digest;
    for (int i = 0; i < numDigests; ++i) {
      TDigest current;
      for (int j = 0; j < size; ++j) {
        auto v = dist(gen);
        current.add(positions, v);
        values.push_back(v);
      }
      current.compress(positions);
      buf.resize(current.serializedByteSize());
      current.serialize(buf.data());
      digest.mergeDeserialized(positions, buf.data());
    }
    digest.compress(positions);
    std::sort(std::begin(values), std::end(values));
    CHECK_QUANTILES(values, digest, kSumError, kRankError);
  };
  test(2, 5e4, 0, 50);
  test(100, 1000, 500, 20);
  test(1e4, 10, 500, 20);
}

TEST_F(TDigestTest, infinity) {
  std::vector<int16_t> positions;
  TDigest digest;
  digest.add(positions, 0.0);
  digest.add(positions, INFINITY);
  digest.add(positions, -INFINITY);
  digest.compress(positions);
  ASSERT_TRUE(std::isnan(digest.sum()));
  ASSERT_EQ(digest.estimateQuantile(0), -INFINITY);
  ASSERT_EQ(digest.estimateQuantile(0.3), -INFINITY);
  ASSERT_EQ(digest.estimateQuantile(0.4), 0.0);
  ASSERT_EQ(digest.estimateQuantile(0.5), 0.0);
  ASSERT_EQ(digest.estimateQuantile(0.6), 0.0);
  ASSERT_EQ(digest.estimateQuantile(0.7), INFINITY);
  ASSERT_EQ(digest.estimateQuantile(1), INFINITY);
}

TEST_F(TDigestTest, scale) {
  std::vector<int16_t> positions;
  TDigest digest;
  for (int i = 1; i <= 5; ++i) {
    digest.add(positions, i);
  }
  digest.compress(positions);
  double originalSum = digest.sum();

  // Scale weights by negative
  ASSERT_THROW(digest.scale(-1), VeloxRuntimeError);

  // Scale weights by 1.7
  digest.scale(1.7);
  digest.compress(positions);
  ASSERT_NEAR(digest.sum(), originalSum * 1.7, kSumError);
}

TEST_F(TDigestTest, largeScalePreservesWeights) {
  TDigest digest;
  std::vector<int16_t> positions;
  std::normal_distribution<double> normal(1000, 100);
  std::default_random_engine gen(common::testutil::getRandomSeed(42));
  constexpr int N = 1e5;
  std::vector<double> values;
  values.reserve(N);
  for (int i = 0; i < N; ++i) {
    double value = normal(gen);
    digest.add(positions, value);
    values.push_back(value);
  }
  digest.compress(positions);
  // Store original percentiles and sum
  std::vector<double> originalPercentiles;
  for (auto q : kQuantiles) {
    originalPercentiles.push_back(digest.estimateQuantile(q));
  }
  double originalSum = digest.sum();

  // Scale TDigest
  double scaleFactor = std::numeric_limits<int>::max() * 2.0;
  digest.scale(scaleFactor);
  digest.compress(positions);

  // Verify sum is scaled correctly.
  ASSERT_NEAR(digest.sum(), originalSum * scaleFactor, kSumError * scaleFactor);
  // Verify percentiles remain the same.
  std::sort(values.begin(), values.end());
  for (size_t i = 0; i < std::size(kQuantiles); ++i) {
    ASSERT_NEAR(
        digest.estimateQuantile(kQuantiles[i]),
        originalPercentiles[i],
        kRankError * (values.back() - values.front()));
  }
}

TEST_F(TDigestTest, quantileAtValue) {
  // Test small range.
  {
    TDigest digest;
    std::vector<int16_t> positions;

    // Add values from 10 to 20
    for (int i = 10; i <= 20; ++i) {
      digest.add(positions, i);
    }
    digest.compress(positions);

    ASSERT_NEAR(digest.getCdf(5), 0.0, 0.001); // Below min
    ASSERT_NEAR(digest.getCdf(25), 1.0, 0.001); // Above max
    ASSERT_NEAR(digest.getCdf(10), 0.05, 0.01); // Min value
    ASSERT_NEAR(digest.getCdf(20), 0.95, 0.01); // Max value
    ASSERT_NEAR(digest.getCdf(15), 0.5, 0.01); // Median
  }
  std::vector<int16_t> positions;

  // Test with uniform distribution
  {
    TDigest digest;
    std::vector<double> values;
    constexpr int N = 1000;

    // Add values from 1 to N
    for (int i = 1; i <= N; ++i) {
      digest.add(positions, i);
      values.push_back(i);
    }
    digest.compress(positions);

    ASSERT_NEAR(digest.getCdf(1), 0.0005, 0.001); // Min value
    ASSERT_NEAR(digest.getCdf(250), 0.25, 0.01); // 25th percentile
    ASSERT_NEAR(digest.getCdf(500), 0.5, 0.01); // Median
    ASSERT_NEAR(digest.getCdf(750), 0.75, 0.01); // 75th percentile
    ASSERT_NEAR(digest.getCdf(1000), 0.9995, 0.001); // Max value
    // Check values outside the range
    ASSERT_NEAR(digest.getCdf(0), 0.0, 0.001); // Below min
    ASSERT_NEAR(digest.getCdf(1001), 1.0, 0.001); // Above max
  }

  // Test with normal distribution
  {
    TDigest digest;
    std::default_random_engine gen(common::testutil::getRandomSeed(42));
    std::normal_distribution<double> normal(100, 10);
    constexpr int N = 10000;
    std::vector<double> values;
    for (int i = 0; i < N; ++i) {
      double value = normal(gen);
      digest.add(positions, value);
      values.push_back(value);
    }
    digest.compress(positions);

    ASSERT_NEAR(digest.getCdf(90), 0.16, 0.02);
    ASSERT_NEAR(digest.getCdf(100), 0.5, 0.02);
    ASSERT_NEAR(digest.getCdf(110), 0.84, 0.02);
  }

  // Test with skewed distribution (exponential)
  {
    TDigest digest;
    std::default_random_engine gen(common::testutil::getRandomSeed(42));
    std::exponential_distribution<double> exp(0.1); // lambda = 0.1
    constexpr int N = 10000;
    for (int i = 0; i < N; ++i) {
      double value = exp(gen);
      digest.add(positions, value);
    }
    digest.compress(positions);

    // For exponential distribution with lambda=0.1:
    // CDF(x) = 1 - e^(-0.1x)
    ASSERT_NEAR(digest.getCdf(0), 0.0, 0.01);
    ASSERT_NEAR(digest.getCdf(6.93), 0.5, 0.02); // median = ln(2)/lambda
    ASSERT_NEAR(digest.getCdf(10), 0.63, 0.02); // 1 - e^(-1) ≈ 0.63
    ASSERT_NEAR(digest.getCdf(20), 0.86, 0.02); // 1 - e^(-2) ≈ 0.86
    ASSERT_NEAR(digest.getCdf(30), 0.95, 0.02); // 1 - e^(-3) ≈ 0.95
  }

  // Test with edge cases
  {
    TDigest digest;

    // Add a mix of values including extremes
    digest.add(positions, -1000);
    digest.add(positions, -100);
    for (int i = 0; i < 98; ++i) {
      digest.add(positions, i);
    }
    digest.add(positions, 1000);
    digest.compress(positions);

    // Check CDF at extremes
    ASSERT_NEAR(digest.getCdf(-1000), 0.005, 0.01); // Minimum value (1/100)
    ASSERT_NEAR(digest.getCdf(1000), 0.995, 0.01); // Maximum value (99/100)

    // Check CDF at values outside the range
    ASSERT_NEAR(digest.getCdf(-2000), 0.0, 0.001);
    ASSERT_NEAR(digest.getCdf(2000), 1.0, 0.001);
  }
}

// Test the fix for floating-point precision issues with large totalWeight
// values. Before the fix, large weights could cause weightSoFar - totalWeight
// to exceed kEpsilon, throwing an error in Velox check.
TEST_F(TDigestTest, largeWeightFloatingPointPrecision) {
  std::vector<int16_t> positions;
  TDigest digest;
  // Create a scenario with very large weights that would have failed before the
  // fix
  constexpr double largeWeight = 1e12;
  constexpr int numValues = 100;
  for (int i = 0; i < numValues; ++i) {
    digest.add(positions, i * 1.0, static_cast<int64_t>(largeWeight));
  }
  ASSERT_NO_THROW(digest.compress(positions));
  ASSERT_GT(digest.totalWeight(), 0);
  ASSERT_FALSE(std::isnan(digest.estimateQuantile(0.5)));
}

} // namespace
} // namespace facebook::velox::functions

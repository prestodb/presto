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
#include "velox/common/testutil/RandomSeed.h"

#include <folly/base64.h>
#include <gtest/gtest.h>

#include <random>

namespace facebook::velox::functions {
namespace {

constexpr double kSumError = 1e-4;
constexpr double kRankError = 0.01;

// Small hack to modify the TDigest sum in the serilaized buffer, so we can test
// that the relative error algorithm in sum testing code works.
void alterTDigestSumInTheBuffer(std::string& buf, const TDigest<>& digest) {
  // Find and adjust the sum in the buffer.
  double originalSum = digest.sum();
  // Based on the TDigest::serialize() we expect the sum at this position.
  const size_t sumOffset = sizeof(int8_t) * 2 + sizeof(double) * 2;
  ASSERT_EQ(memcmp(buf.data() + sumOffset, &originalSum, sizeof(double)), 0);

  *(double*)(buf.data() + sumOffset) += TDigest<>::kEpsilon * 2;
}

constexpr double kQuantiles[] = {
    0.0001, 0.0200, 0.0300, 0.04000, 0.0500, 0.1000, 0.2000,
    0.3000, 0.4000, 0.5000, 0.6000,  0.7000, 0.8000, 0.9000,
    0.9500, 0.9600, 0.9700, 0.9800,  0.9999,
};

void checkQuantiles(
    folly::Range<const double*> values,
    const TDigest<>& digest) {
  VELOX_CHECK(std::is_sorted(values.begin(), values.end()));
  auto sum = std::accumulate(values.begin(), values.end(), 0.0);
  ASSERT_NEAR(digest.sum(), sum, kSumError);
  for (auto q : kQuantiles) {
    auto v = digest.estimateQuantile(q);
    ASSERT_LE(values.front(), v);
    ASSERT_LE(v, values.back());
    auto hi = std::lower_bound(values.begin(), values.end(), v);
    auto lo = hi;
    while (lo != values.begin() && v > *lo) {
      --lo;
    }
    while (std::next(hi) != values.end() && *hi == *std::next(hi)) {
      ++hi;
    }
    auto l = (lo - values.begin()) / (values.size() - 1.0);
    auto r = (hi - values.begin()) / (values.size() - 1.0);
    if (q < l) {
      ASSERT_NEAR(l, q, kRankError);
    } else if (q > r) {
      ASSERT_NEAR(r, q, kRankError);
    }
  }
}

#define CHECK_QUANTILES(_values, _digest) \
  do {                                    \
    SCOPED_TRACE("CHECK_QUANTILES");      \
    checkQuantiles((_values), (_digest)); \
  } while (false)

std::string decodeBase64(std::string_view input) {
  std::string decoded(folly::base64DecodedSize(input), '\0');
  folly::base64Decode(input, decoded.data());
  return decoded;
}

TEST(TDigestTest, addElementsInOrder) {
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

TEST(TDigestTest, addElementsRandomized) {
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
  CHECK_QUANTILES(folly::Range(values, N), digest);
}

TEST(TDigestTest, fewElements) {
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
TEST(TDigestTest, invalid) {
  TDigest digest;
  ASSERT_THROW(digest.setCompression(NAN), VeloxRuntimeError);
  ASSERT_THROW(digest.setCompression(0), VeloxRuntimeError);
  ASSERT_THROW(digest.setCompression(1000.1), VeloxRuntimeError);
  std::vector<int16_t> positions;
  ASSERT_THROW(digest.add(positions, NAN), VeloxRuntimeError);
  ASSERT_THROW(digest.add(positions, 1, 0), VeloxRuntimeError);
  ASSERT_THROW(digest.estimateQuantile(1.1), VeloxRuntimeError);
}

TEST(TDigestTest, unalignedSerialization) {
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

TEST(TDigestTest, mergeEmpty) {
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

TEST(TDigestTest, deserializeJava) {
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
    CHECK_QUANTILES(folly::Range(values, 101), digest);
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
    CHECK_QUANTILES(values, digest);
  }
}

TEST(TDigestTest, mergeJava) {
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

TEST(TDigestTest, mergeNoOverlap) {
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

TEST(TDigestTest, mergeOverlap) {
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
  CHECK_QUANTILES(values, digest);
}

TEST(TDigestTest, normalDistribution) {
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
    CHECK_QUANTILES(folly::Range(values, N), digest);
  }
}

TEST(TDigestTest, addWeighed) {
  std::vector<int16_t> positions;
  TDigest digest;
  std::vector<double> values;
  values.reserve(5050);
  for (int i = 1; i <= 100; ++i) {
    digest.add(positions, i, i);
    values.insert(values.end(), i, i);
  }
  digest.compress(positions);
  CHECK_QUANTILES(values, digest);
}

TEST(TDigestTest, merge) {
  std::vector<int16_t> positions;
  std::default_random_engine gen(common::testutil::getRandomSeed(42));
  std::vector<double> values;
  std::string buf;
  auto test = [&](int numDigests,
                  int size,
                  double mean,
                  double stddev,
                  bool alterSum = false) {
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
      if (alterSum) {
        alterTDigestSumInTheBuffer(buf, current);
      }
      digest.mergeDeserialized(positions, buf.data());
    }
    digest.compress(positions);
    std::sort(std::begin(values), std::end(values));
    CHECK_QUANTILES(values, digest);
  };
  test(2, 5e4, 0, 50);
  test(100, 1000, 500, 20);
  test(1e4, 10, 500, 20);

  test(5, 5e4, 0, 50, true);
}

TEST(TDigestTest, infinity) {
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

} // namespace
} // namespace facebook::velox::functions

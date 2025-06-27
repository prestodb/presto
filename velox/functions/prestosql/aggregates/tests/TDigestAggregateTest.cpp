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
#include <folly/base64.h>
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/lib/TDigest.h"
#include "velox/functions/lib/aggregates/tests/utils/AggregationTestBase.h"
#include "velox/functions/prestosql/types/TDigestType.h"
#include "velox/vector/ComplexVector.h"

using namespace facebook::velox::functions::aggregate::test;

namespace facebook::velox::aggregate::test {

class TDigestAggregateTest : public AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
  }

  RowVectorPtr createExpectedVector(const std::string& base64EncodedString) {
    std::string encodedString = decodeBase64(base64EncodedString);
    auto expectedVector =
        makeRowVector({makeFlatVector<facebook::velox::StringView>(
            {facebook::velox::StringView(
                encodedString.data(),
                static_cast<int32_t>(encodedString.size()))},
            facebook::velox::TDigestType::get(DOUBLE()))});
    return expectedVector;
  }

  std::string decodeBase64(std::string_view input) {
    std::string decoded(folly::base64DecodedSize(input), '\0');
    folly::base64Decode(input, decoded.data());
    return decoded;
  }
};

TEST_F(TDigestAggregateTest, testAggregation) {
  std::vector<double> inputs = {1.0, 2.0, 3.0, 4.0, 5.0};
  std::vector<int64_t> weights = {1, 1, 1, 1, 1};
  auto vectors = makeRowVector({makeFlatVector<double>(inputs)});
  createDuckDbTable({vectors});
  auto expected = createExpectedVector(
      "AQAAAAAAAADwPwAAAAAAABRAAAAAAAAALkAAAAAAAABZQAAAAAAAABRABQAAAAAAAAAAAPA/AAAAAAAA8D8AAAAAAADwPwAAAAAAAPA/AAAAAAAA8D8AAAAAAADwPwAAAAAAAABAAAAAAAAACEAAAAAAAAAQQAAAAAAAABRA");
  testAggregations({vectors}, {}, {"tdigest_agg(c0)"}, {expected});
}

TEST_F(TDigestAggregateTest, testAggregationWithWeight) {
  std::vector<double> inputs = {1.0, 2.0, 3.0, 4.0, 5.0};
  int64_t weight = 2;
  auto vectors = makeRowVector(
      {makeFlatVector<double>(inputs),
       makeConstant<int64_t>(
           weight, static_cast<vector_size_t>(inputs.size()))});
  createDuckDbTable({vectors});
  auto expected = createExpectedVector(
      "AQAAAAAAAADwPwAAAAAAABRAAAAAAAAAPkAAAAAAAABZQAAAAAAAACRABQAAAAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAQAAAAAAAAABAAAAAAAAAAEAAAAAAAADwPwAAAAAAAABAAAAAAAAACEAAAAAAAAAQQAAAAAAAABRA");
  testAggregations({vectors}, {}, {"tdigest_agg(c0, c1)"}, {expected});
}

TEST_F(TDigestAggregateTest, testAggregationWithWeightAndCompression) {
  std::vector<double> inputs = {1.0, 2.0, 3.0, 4.0, 5.0};
  int64_t weight = 2;
  double compression = 50.0;
  auto vectors = makeRowVector(
      {makeFlatVector<double>(inputs),
       makeConstant<int64_t>(weight, static_cast<vector_size_t>(inputs.size())),
       makeConstant<double>(
           compression, static_cast<vector_size_t>(inputs.size()))});
  createDuckDbTable({vectors});
  auto expected = createExpectedVector(
      "AQAAAAAAAADwPwAAAAAAABRAAAAAAAAAPkAAAAAAAABJQAAAAAAAACRABQAAAAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAQAAAAAAAAABAAAAAAAAAAEAAAAAAAADwPwAAAAAAAABAAAAAAAAACEAAAAAAAAAQQAAAAAAAABRA");
  testAggregations({vectors}, {}, {"tdigest_agg(c0, c1, c2)"}, {expected});
}

TEST_F(TDigestAggregateTest, testAggregationWithVaryingWeights) {
  std::vector<double> inputs = {1.0, 2.0, 3.0, 4.0, 5.0};
  std::vector<int64_t> weights = {1, 2, 3, 4, 5};
  auto vectors = makeRowVector(
      {makeFlatVector<double>(inputs), makeFlatVector<int64_t>(weights)});
  createDuckDbTable({vectors});
  auto expected = createExpectedVector(
      "AQAAAAAAAADwPwAAAAAAABRAAAAAAACAS0AAAAAAAABZQAAAAAAAAC5ABQAAAAAAAAAAAPA/AAAAAAAAAEAAAAAAAAAIQAAAAAAAABBAAAAAAAAAFEAAAAAAAADwPwAAAAAAAABAAAAAAAAACEAAAAAAAAAQQAAAAAAAABRA");
  testAggregations({vectors}, {}, {"tdigest_agg(c0, c1)"}, {expected});
}

TEST_F(TDigestAggregateTest, testAggregationWithVaryingCompression) {
  std::vector<double> inputs = {1.0, 2.0, 3.0, 4.0, 5.0};
  int64_t weight = 2;
  std::vector<double> compression = {10.0, 11.0, 12.0, 13.0, 14.0};
  auto vectors = makeRowVector(
      {makeFlatVector<double>(inputs),
       makeConstant<int64_t>(weight, static_cast<vector_size_t>(inputs.size())),
       makeFlatVector<double>(compression)});
  createDuckDbTable({vectors});
  VELOX_ASSERT_THROW(
      testAggregations({vectors}, {}, {"tdigest_agg(c0, c1, c2)"}, {""}),
      "Compression factor must be same for all rows");
}

TEST_F(TDigestAggregateTest, testAggregationMap) {
  std::vector<double> inputs = {891.25};
  std::vector<int64_t> weights = {636};
  std::vector<double> compression = {83.72};
  std::vector<bool> mask = {false};
  auto vectors = makeRowVector(
      {makeFlatVector<double>(inputs),
       makeFlatVector<int64_t>(weights),
       makeFlatVector<double>(compression),
       makeFlatVector<bool>(mask)});
  createDuckDbTable({vectors});
  //   auto expected = makeRowVector({makeAllNullFlatVector<Varbinary>(1)});
  //  facebook::velox::TDigestType::get(DOUBLE())});
  testAggregations(
      {vectors},
      {},
      {"tdigest_agg(c0, c1, c2) filter (where c3)"},
      {"VALUES (NULL)"});
}

} // namespace facebook::velox::aggregate::test

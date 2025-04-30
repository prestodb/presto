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

#include "velox/core/Expressions.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/lib/TDigest.h"
#include "velox/functions/lib/aggregates/tests/utils/AggregationTestBase.h"
#include "velox/functions/prestosql/types/TDigestRegistration.h"
#include "velox/functions/prestosql/types/TDigestType.h"

using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::functions::aggregate::test;

namespace facebook::velox::aggregate::test {
namespace {

class MergeAggregateTest : public AggregationTestBase {
 protected:
  static const std::vector<double> kQuantiles;

  facebook::velox::functions::TDigest<> createTDigest(const char* inputData) {
    facebook::velox::functions::TDigest<> tdigest;
    std::vector<int16_t> positions;
    tdigest.mergeDeserialized(positions, inputData);
    tdigest.compress(positions);
    return tdigest;
  }

  // Helper function to check TDigest equality similar to Java's
  // TDIGEST_EQUALITY
  bool tdigestEquals(
      const std::string& actualBase64,
      const std::string& expectedBase64) {
    if (actualBase64.empty() && expectedBase64.empty()) {
      return true;
    }
    if (actualBase64.empty() || expectedBase64.empty()) {
      return false;
    }

    // Decode base64 strings to binary
    auto actualBinary = folly::base64Decode(actualBase64);
    auto expectedBinary = folly::base64Decode(expectedBase64);

    // Create TDigest objects
    facebook::velox::functions::TDigest<> actual =
        createTDigest(actualBinary.data());
    facebook::velox::functions::TDigest<> expected =
        createTDigest(expectedBinary.data());

    // Compare quantiles
    for (double quantile : kQuantiles) {
      double actualQuantile = actual.estimateQuantile(quantile);
      double expectedQuantile = expected.estimateQuantile(quantile);
      double tolerance = std::max(1.0, std::abs(actualQuantile * 0.01));
      if (std::abs(actualQuantile - expectedQuantile) > tolerance) {
        return false;
      }
    }

    // Compare sum with tolerance
    if (std::abs(actual.sum() - expected.sum()) > 0.0001) {
      return false;
    }

    // Compare other properties
    return actual.totalWeight() == expected.totalWeight() &&
        actual.min() == expected.min() && actual.max() == expected.max() &&
        actual.compression() == expected.compression();
  }
};

const std::vector<double> MergeAggregateTest::kQuantiles =
    {0.01, 0.05, 0.1, 0.25, 0.50, 0.75, 0.9, 0.95, 0.99};

TEST_F(MergeAggregateTest, mergeTDigestMatchJava) {
  registerTDigestType();
  // Base64-encoded TDigests from Presto Java
  // TDigest: values 1.0 - 50.0
  const std::string kTDigest1 =
      "AQAAAAAAAADwPwAAAAAAAElAAAAAAADsk0AAAAAAAABZQAAAAAAAAElAJgAAAAAAAAAAAPA/AAAAAAAA8D8AAAAAAADwPwAAAAAAAPA/AAAAAAAA8D8AAAAAAADwPwAAAAAAAPA/AAAAAAAA8D8AAAAAAADwPwAAAAAAAPA/AAAAAAAA8D8AAAAAAADwPwAAAAAAAPA/AAAAAAAAAEAAAAAAAAAAQAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAQAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAQAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAQAAAAAAAAABAAAAAAAAA8D8AAAAAAADwPwAAAAAAAPA/AAAAAAAA8D8AAAAAAADwPwAAAAAAAPA/AAAAAAAA8D8AAAAAAADwPwAAAAAAAPA/AAAAAAAA8D8AAAAAAADwPwAAAAAAAPA/AAAAAAAA8D8AAAAAAADwPwAAAAAAAABAAAAAAAAACEAAAAAAAAAQQAAAAAAAABRAAAAAAAAAGEAAAAAAAAAcQAAAAAAAACBAAAAAAAAAIkAAAAAAAAAkQAAAAAAAACZAAAAAAAAAKEAAAAAAAAAqQAAAAAAAAC1AAAAAAACAMEAAAAAAAIAyQAAAAAAAgDRAAAAAAACANkAAAAAAAIA4QAAAAAAAgDpAAAAAAACAPEAAAAAAAIA+QAAAAAAAQEBAAAAAAABAQUAAAAAAAEBCQAAAAAAAAENAAAAAAACAQ0AAAAAAAABEQAAAAAAAgERAAAAAAAAARUAAAAAAAIBFQAAAAAAAAEZAAAAAAACARkAAAAAAAABHQAAAAAAAgEdAAAAAAAAASEAAAAAAAIBIQAAAAAAAAElA";
  // TDigest: values 51.0 - 100.0
  const std::string kTDigest2 =
      "AQAAAAAAAIBJQAAAAAAAAFlAAAAAAAB+rUAAAAAAAABZQAAAAAAAAElAJgAAAAAAAAAAAPA/AAAAAAAA8D8AAAAAAADwPwAAAAAAAPA/AAAAAAAA8D8AAAAAAADwPwAAAAAAAPA/AAAAAAAA8D8AAAAAAADwPwAAAAAAAPA/AAAAAAAA8D8AAAAAAADwPwAAAAAAAPA/AAAAAAAAAEAAAAAAAAAAQAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAQAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAQAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAQAAAAAAAAABAAAAAAAAA8D8AAAAAAADwPwAAAAAAAPA/AAAAAAAA8D8AAAAAAADwPwAAAAAAAPA/AAAAAAAA8D8AAAAAAADwPwAAAAAAAPA/AAAAAAAA8D8AAAAAAADwPwAAAAAAAPA/AAAAAAAA8D8AAAAAAIBJQAAAAAAAAEpAAAAAAACASkAAAAAAAABLQAAAAAAAgEtAAAAAAAAATEAAAAAAAIBMQAAAAAAAAE1AAAAAAACATUAAAAAAAABOQAAAAAAAgE5AAAAAAAAAT0AAAAAAAIBPQAAAAAAAIFBAAAAAAACgUEAAAAAAACBRQAAAAAAAoFFAAAAAAAAgUkAAAAAAAKBSQAAAAAAAIFNAAAAAAACgU0AAAAAAACBUQAAAAAAAoFRAAAAAAAAgVUAAAAAAAKBVQAAAAAAAAFZAAAAAAABAVkAAAAAAAIBWQAAAAAAAwFZAAAAAAAAAV0AAAAAAAEBXQAAAAAAAgFdAAAAAAADAV0AAAAAAAABYQAAAAAAAQFhAAAAAAACAWEAAAAAAAMBYQAAAAAAAAFlA";
  // Create TDigest vectors from base64 strings
  auto tdigestData = makeFlatVector<std::string>({kTDigest1, kTDigest2});
  // Create plan. Project expressions is used instead as DuckDB does not
  // recognize tdigest(double) in a projection string.
  std::vector<core::TypedExprPtr> projectExprs;
  auto fieldAccess =
      std::make_shared<core::FieldAccessTypedExpr>(VARCHAR(), "c0");
  auto fromBase64 = std::make_shared<core::CallTypedExpr>(
      VARBINARY(), std::vector<core::TypedExprPtr>{fieldAccess}, "from_base64");
  auto castToTDigest = std::make_shared<core::CastTypedExpr>(
      TDIGEST(DOUBLE()), fromBase64, true);
  auto aggResultAccess =
      std::make_shared<core::FieldAccessTypedExpr>(TDIGEST(DOUBLE()), "a0");
  auto castToVarbinary =
      std::make_shared<core::CastTypedExpr>(VARBINARY(), aggResultAccess, true);
  auto toBase64 = std::make_shared<core::CallTypedExpr>(
      VARCHAR(), std::vector<core::TypedExprPtr>{castToVarbinary}, "to_base64");
  auto op = PlanBuilder()
                .values({makeRowVector({tdigestData})})
                .projectExpressions({castToTDigest})
                .singleAggregation({}, {"merge(p0)"})
                .projectExpressions({toBase64})
                .planNode();
  auto result = readSingleValue(op);
  // Expected merged TDigest base64 string from Java
  const std::string expectedMergedTDigest =
      "AQAAAAAAAADwPwAAAAAAAFlAAAAAAAC6s0AAAAAAAABZQAAAAAAAAFlAKgAAAAAAAAAAAPA/AAAAAAAA8D8AAAAAAADwPwAAAAAAAPA/AAAAAAAA8D8AAAAAAADwPwAAAAAAAPA/AAAAAAAA8D8AAAAAAADwPwAAAAAAAPA/AAAAAAAAAEAAAAAAAAAAQAAAAAAAAABAAAAAAAAACEAAAAAAAAAIQAAAAAAAABBAAAAAAAAAEEAAAAAAAAAUQAAAAAAAABRAAAAAAAAAFEAAAAAAAAAUQAAAAAAAABRAAAAAAAAAFEAAAAAAAAAUQAAAAAAAABRAAAAAAAAAEEAAAAAAAAAQQAAAAAAAAAhAAAAAAAAACEAAAAAAAAAAQAAAAAAAAABAAAAAAAAAAEAAAAAAAADwPwAAAAAAAPA/AAAAAAAA8D8AAAAAAADwPwAAAAAAAPA/AAAAAAAA8D8AAAAAAADwPwAAAAAAAPA/AAAAAAAA8D8AAAAAAADwPwAAAAAAAPA/AAAAAAAAAEAAAAAAAAAIQAAAAAAAABBAAAAAAAAAFEAAAAAAAAAYQAAAAAAAABxAAAAAAAAAIEAAAAAAAAAiQAAAAAAAACRAAAAAAAAAJ0AAAAAAAAArQAAAAAAAAC9AAAAAAAAAMkAAAAAAAAA1QAAAAAAAgDhAAAAAAACAPEAAAAAAAIBAQAAAAAAAAENAAAAAAACARUAAAAAAAABIQAAAAAAAgEpAAAAAAAAATUAAAAAAAIBPQAAAAAAAAFFAAAAAAAAgUkAAAAAAACBTQAAAAAAAAFRAAAAAAADAVEAAAAAAAGBVQAAAAAAA4FVAAAAAAABgVkAAAAAAAMBWQAAAAAAAAFdAAAAAAABAV0AAAAAAAIBXQAAAAAAAwFdAAAAAAAAAWEAAAAAAAEBYQAAAAAAAgFhAAAAAAADAWEAAAAAAAABZQA==";
  ASSERT_TRUE(
      tdigestEquals(result.value<TypeKind::VARCHAR>(), expectedMergedTDigest));
}

TEST_F(MergeAggregateTest, mergeTDigestOneNullMatchJava) {
  registerTDigestType();
  // Base64-encoded TDigests from Presto Java
  // TDigest: values 1.0, 2.0, 3.0
  const std::string kTDigest1 =
      "AQAAAAAAAADwPwAAAAAAAAhAAAAAAAAAGEAAAAAAAABZQAAAAAAAAAhAAwAAAAAAAAAAAPA/AAAAAAAA8D8AAAAAAADwPwAAAAAAAPA/AAAAAAAAAEAAAAAAAAAIQA==";
  // Null TDigest
  const std::string kTDigest2;
  // Create TDigest vectors from base64 strings
  auto tdigestData = makeFlatVector<std::string>({kTDigest1, kTDigest2});
  // Create plan. Project expressions is used instead as DuckDB does not
  // recognize tdigest(double) in a projection string.
  std::vector<core::TypedExprPtr> projectExprs;
  auto fieldAccess =
      std::make_shared<core::FieldAccessTypedExpr>(VARCHAR(), "c0");
  auto fromBase64 = std::make_shared<core::CallTypedExpr>(
      VARBINARY(), std::vector<core::TypedExprPtr>{fieldAccess}, "from_base64");
  auto castToTDigest = std::make_shared<core::CastTypedExpr>(
      TDIGEST(DOUBLE()), fromBase64, true);
  auto aggResultAccess =
      std::make_shared<core::FieldAccessTypedExpr>(TDIGEST(DOUBLE()), "a0");
  auto castToVarbinary =
      std::make_shared<core::CastTypedExpr>(VARBINARY(), aggResultAccess, true);
  auto toBase64 = std::make_shared<core::CallTypedExpr>(
      VARCHAR(), std::vector<core::TypedExprPtr>{castToVarbinary}, "to_base64");
  auto op = PlanBuilder()
                .values({makeRowVector({tdigestData})})
                .projectExpressions({castToTDigest})
                .singleAggregation({}, {"merge(p0)"})
                .projectExpressions({toBase64})
                .planNode();
  auto result = readSingleValue(op);
  // Expected merged TDigest base64 string from Java
  const std::string expectedMergedTDigest =
      "AQAAAAAAAADwPwAAAAAAAAhAAAAAAAAAGEAAAAAAAABZQAAAAAAAAAhAAwAAAAAAAAAAAPA/AAAAAAAA8D8AAAAAAADwPwAAAAAAAPA/AAAAAAAAAEAAAAAAAAAIQA==";
  ASSERT_TRUE(
      tdigestEquals(result.value<TypeKind::VARCHAR>(), expectedMergedTDigest));
}
} // namespace
} // namespace facebook::velox::aggregate::test

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

#include "velox/dwio/dwrf/writer/FlushPolicy.h"

using namespace ::testing;
using namespace facebook::dwio::common;
using namespace facebook::velox::memory;

namespace facebook::velox::dwrf {

TEST(DefaultFlushPolicyTest, PredicateTest) {
  struct TestCase {
    explicit TestCase(
        uint64_t stripeSizeThreshold,
        uint64_t dictionarySizeThreshold,
        uint64_t stripeSize,
        uint64_t dictionarySize,
        bool shouldFlush)
        : stripeSizeThreshold{stripeSizeThreshold},
          dictionarySizeThreshold{dictionarySizeThreshold},
          stripeSize{stripeSize},
          dictionarySize{dictionarySize},
          shouldFlush{shouldFlush} {}

    const uint64_t stripeSizeThreshold;
    const uint64_t dictionarySizeThreshold;
    const uint64_t stripeSize;
    const uint64_t dictionarySize;
    const bool shouldFlush;
  };

  std::vector<TestCase> testCases{
      TestCase{200, 20, 0, 0, false},
      TestCase{200, 20, 40, 15, false},
      TestCase{200, 20, 40, 21, true},
      TestCase{200, 20, 400, 15, true},
      TestCase{200, 20, 400, 21, true}};
  for (const auto& testCase : testCases) {
    DefaultFlushPolicy policy{
        testCase.stripeSizeThreshold, testCase.dictionarySizeThreshold};
    EXPECT_EQ(
        testCase.shouldFlush,
        policy(testCase.stripeSize, testCase.dictionarySize));
  }
}

TEST(RowsPerStripeFlushPolicyTest, EmptyFile) {
  // Empty vector creation succeeds.
  RowsPerStripeFlushPolicy policy({});

  auto config = std::make_shared<Config>();
  WriterContext context{config, getDefaultScopedMemoryPool()};

  ASSERT_THROW(policy(true, context), exception::LoggedException);
  ASSERT_THROW(policy(false, context), exception::LoggedException);
}

TEST(RowsPerStripeFlushPolicyTest, InvalidCases) {
  // Vector with 0 rows, throws
  ASSERT_THROW(
      RowsPerStripeFlushPolicy policy({5, 7, 0, 10}),
      exception::LoggedException);
}

TEST(RowsPerStripeFlushPolicyTest, FlushTest) {
  RowsPerStripeFlushPolicy policy({5, 7, 12});

  auto config = std::make_shared<Config>();
  WriterContext context{config, getDefaultScopedMemoryPool()};

  context.stripeRowCount = 1;
  ASSERT_FALSE(policy(true, context)) << "First Stripe with one row";
  ASSERT_FALSE(policy(false, context)) << "First Stripe with one row";

  context.stripeRowCount = 5;
  ASSERT_TRUE(policy(true, context)) << "First Stripe end";
  ASSERT_TRUE(policy(false, context)) << "First Stripe end";

  context.stripeRowCount = 6;
  ASSERT_THROW(policy(true, context), exception::LoggedException)
      << "First Stripe has more rows";
  ASSERT_THROW(policy(false, context), exception::LoggedException)
      << "First Stripe has more rows";

  context.stripeIndex = 1;
  context.stripeRowCount = 6;
  ASSERT_FALSE(policy(true, context)) << "Second stripe before boundary";
  ASSERT_FALSE(policy(false, context)) << "Second stripe before boundary";

  context.stripeRowCount = 7;
  ASSERT_TRUE(policy(true, context)) << "Second stipe boundary";
  ASSERT_TRUE(policy(false, context)) << "Second stipe boundary";

  context.stripeIndex = 2;
  context.stripeRowCount = 12;
  ASSERT_FALSE(policy(true, context)) << "Last Stripe should return false";
  ASSERT_FALSE(policy(false, context)) << "Last Stripe should return false";

  context.stripeIndex = 3;
  context.stripeRowCount = 0;
  ASSERT_THROW(policy(true, context), exception::LoggedException)
      << "Out of Range Stripe";
  ASSERT_THROW(policy(false, context), exception::LoggedException)
      << "Out of Range Stripe";
}

TEST(FlushPolicyTest, PredicateTest) {
  struct TestCase {
    explicit TestCase(
        uint64_t stripeSizeThreshold,
        uint64_t dictionarySizeThreshold,
        uint64_t stripeSize,
        uint64_t dictionarySize,
        bool shouldFlush)
        : stripeSizeThreshold{stripeSizeThreshold},
          dictionarySizeThreshold{dictionarySizeThreshold},
          stripeSize{stripeSize},
          dictionarySize{dictionarySize},
          shouldFlush{shouldFlush} {}

    const uint64_t stripeSizeThreshold;
    const uint64_t dictionarySizeThreshold;
    const uint64_t stripeSize;
    const uint64_t dictionarySize;
    const bool shouldFlush;
  };

  std::vector<TestCase> testCases{
      TestCase{200, 20, 0, 0, false},
      TestCase{200, 20, 40, 15, false},
      TestCase{200, 20, 40, 21, true},
      TestCase{200, 20, 400, 15, true},
      TestCase{200, 20, 400, 21, true}};
  for (const auto& testCase : testCases) {
    DefaultFlushPolicy policy{
        testCase.stripeSizeThreshold, testCase.dictionarySizeThreshold};
    EXPECT_EQ(
        testCase.shouldFlush,
        policy(testCase.stripeSize, testCase.dictionarySize));
  }
}

} // namespace facebook::velox::dwrf

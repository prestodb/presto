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

#include <gtest/gtest.h>

#include "velox/dwio/common/FlushPolicy.h"
#include "velox/dwio/dwrf/writer/FlushPolicy.h"

using namespace ::testing;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::memory;

namespace facebook::velox::dwrf {

class DefaultFlushPolicyTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }
};

TEST_F(DefaultFlushPolicyTest, StripeProgressTest) {
  struct TestCase {
    const uint64_t stripeSizeThreshold;
    const int64_t stripeSize;
    const bool shouldFlush;
  };

  std::vector<TestCase> testCases{
      TestCase{
          .stripeSizeThreshold = 200, .stripeSize = 0, .shouldFlush = false},
      TestCase{
          .stripeSizeThreshold = 200, .stripeSize = 40, .shouldFlush = false},
      TestCase{
          .stripeSizeThreshold = 200, .stripeSize = 400, .shouldFlush = true}};
  for (const auto& testCase : testCases) {
    DefaultFlushPolicy policy{
        testCase.stripeSizeThreshold, /*dictionarySizeThreshold=*/0};
    EXPECT_EQ(
        testCase.shouldFlush,
        policy.shouldFlush(dwio::common::StripeProgress{
            .stripeSizeEstimate = testCase.stripeSize}));
  }
}

TEST_F(DefaultFlushPolicyTest, AdditionalCriteriaTest) {
  struct TestCase {
    const bool flushStripe;
    const bool overMemoryBudget;
    const uint64_t dictionarySizeThreshold;
    const uint64_t dictionarySize;
    const FlushDecision decision;
  };

  std::vector<TestCase> testCases{
      TestCase{
          .flushStripe = false,
          .overMemoryBudget = false,
          .dictionarySizeThreshold = 20,
          .dictionarySize = 15,
          .decision = FlushDecision::SKIP},
      TestCase{
          .flushStripe = false,
          .overMemoryBudget = true,
          .dictionarySizeThreshold = 20,
          .dictionarySize = 15,
          .decision = FlushDecision::SKIP},
      TestCase{
          .flushStripe = true,
          .overMemoryBudget = false,
          .dictionarySizeThreshold = 20,
          .dictionarySize = 15,
          .decision = FlushDecision::SKIP},
      TestCase{
          .flushStripe = true,
          .overMemoryBudget = true,
          .dictionarySizeThreshold = 20,
          .dictionarySize = 15,
          .decision = FlushDecision::SKIP},
      TestCase{
          .flushStripe = false,
          .overMemoryBudget = false,
          .dictionarySizeThreshold = 20,
          .dictionarySize = 42,
          .decision = FlushDecision::FLUSH_DICTIONARY},
      TestCase{
          .flushStripe = false,
          .overMemoryBudget = true,
          .dictionarySizeThreshold = 20,
          .dictionarySize = 42,
          .decision = FlushDecision::FLUSH_DICTIONARY},
      TestCase{
          .flushStripe = true,
          .overMemoryBudget = false,
          .dictionarySizeThreshold = 20,
          .dictionarySize = 42,
          .decision = FlushDecision::SKIP},
      TestCase{
          .flushStripe = true,
          .overMemoryBudget = true,
          .dictionarySizeThreshold = 20,
          .dictionarySize = 42,
          .decision = FlushDecision::SKIP}};
  for (const auto& testCase : testCases) {
    DefaultFlushPolicy policy{
        /*stripeSizeThreshold=*/1000, testCase.dictionarySizeThreshold};
    EXPECT_EQ(
        testCase.decision,
        policy.shouldFlushDictionary(
            testCase.flushStripe,
            testCase.overMemoryBudget,
            // StripeProgress is not used in this test.
            dwio::common::StripeProgress{},
            testCase.dictionarySize))
        << fmt::format(
               "flushStripe = {}, overMemoryBudget = {}, dictionarySize = {}",
               testCase.flushStripe,
               testCase.overMemoryBudget,
               testCase.dictionarySize);
  }
}

TEST_F(DefaultFlushPolicyTest, EarlyDictionaryEvaluation) {
  // Test the precedence of decisions.
  struct TestCase {
    dwio::common::StripeProgress stripeProgress;
    const uint64_t stripeSizeThreshold;
    const bool flushStripe;
    const uint64_t dictionarySizeThreshold;
    const uint64_t dictionarySize;
    const FlushDecision decision;
  };

  std::vector<TestCase> testCases{
      TestCase{
          .stripeProgress =
              dwio::common::StripeProgress{.stripeSizeEstimate = 100},
          .stripeSizeThreshold = 200,
          .flushStripe = false,
          .dictionarySizeThreshold = 20,
          .dictionarySize = 15,
          .decision = FlushDecision::EVALUATE_DICTIONARY},
      TestCase{
          .stripeProgress =
              dwio::common::StripeProgress{.stripeSizeEstimate = 100},
          .stripeSizeThreshold = 200,
          .flushStripe = false,
          .dictionarySizeThreshold = 20,
          .dictionarySize = 42,
          .decision = FlushDecision::FLUSH_DICTIONARY},
      TestCase{
          .stripeProgress =
              dwio::common::StripeProgress{.stripeSizeEstimate = 100},
          .stripeSizeThreshold = 200,
          .flushStripe = true,
          .dictionarySizeThreshold = 20,
          .dictionarySize = 42,
          .decision = FlushDecision::SKIP}};
  for (const auto& testCase : testCases) {
    DefaultFlushPolicy policy{
        testCase.stripeSizeThreshold, testCase.dictionarySizeThreshold};
    EXPECT_EQ(
        testCase.decision,
        policy.shouldFlushDictionary(
            testCase.flushStripe,
            /*overMemoryBudget=*/false,
            testCase.stripeProgress,
            testCase.dictionarySize))
        << fmt::format(
               "flushStripe = {}, stripeSizeThreshold = {}, estimatedStripeSize = {}, dictionarySizeThreshold = {}, dictionarySize = {}",
               testCase.flushStripe,
               testCase.stripeSizeThreshold,
               testCase.stripeProgress.stripeSizeEstimate,
               testCase.dictionarySizeThreshold,
               testCase.dictionarySize);
  }

  // Test dictionary evaluation signals throughout the stripe.
  DefaultFlushPolicy policy{
      /*stripeSizeThreshold=*/1000,
      /*dictionarySizeThreshold=*/std::numeric_limits<uint64_t>::max()};
  EXPECT_EQ(
      policy.shouldFlushDictionary(
          /*flushStripe=*/false,
          /*overMemoryBudget=*/false,
          dwio::common::StripeProgress{.stripeSizeEstimate = 100},
          /*dictionarySize=*/20),
      FlushDecision::SKIP);
  EXPECT_EQ(
      policy.shouldFlushDictionary(
          /*flushStripe=*/false,
          /*overMemoryBudget=*/false,
          dwio::common::StripeProgress{.stripeSizeEstimate = 200},
          /*dictionarySize=*/20),
      FlushDecision::SKIP);
  EXPECT_EQ(
      policy.shouldFlushDictionary(
          /*flushStripe=*/false,
          /*overMemoryBudget=*/false,
          dwio::common::StripeProgress{.stripeSizeEstimate = 400},
          /*dictionarySize=*/20),
      FlushDecision::EVALUATE_DICTIONARY);
  EXPECT_EQ(
      policy.shouldFlushDictionary(
          /*flushStripe=*/false,
          /*overMemoryBudget=*/false,
          dwio::common::StripeProgress{.stripeSizeEstimate = 500},
          /*dictionarySize=*/20),
      FlushDecision::SKIP);
  EXPECT_EQ(
      policy.shouldFlushDictionary(
          /*flushStripe=*/false,
          /*overMemoryBudget=*/false,
          dwio::common::StripeProgress{.stripeSizeEstimate = 700},
          /*dictionarySize=*/20),
      FlushDecision::EVALUATE_DICTIONARY);
  EXPECT_EQ(
      policy.shouldFlushDictionary(
          /*flushStripe=*/false,
          /*overMemoryBudget=*/false,
          dwio::common::StripeProgress{.stripeSizeEstimate = 900},
          /*dictionarySize=*/20),
      FlushDecision::SKIP);
}

TEST_F(DefaultFlushPolicyTest, EmptyFile) {
  // Empty vector creation succeeds.
  RowsPerStripeFlushPolicy policy({});

  ASSERT_THROW(
      policy.shouldFlush(dwio::common::StripeProgress{}),
      exception::LoggedException);
}

TEST_F(DefaultFlushPolicyTest, InvalidCases) {
  // Vector with 0 rows, throws
  ASSERT_THROW(
      RowsPerStripeFlushPolicy policy({5, 7, 0, 10}),
      exception::LoggedException);
}

// RowsPerStripeFlushPolicy has no dictionary flush criteria.
TEST_F(DefaultFlushPolicyTest, DictionaryCriteriaTest) {
  auto config = std::make_shared<Config>();
  WriterContext context{
      config, memory::memoryManager()->addRootPool("DictionaryCriteriaTest")};

  RowsPerStripeFlushPolicy policy({42});
  EXPECT_EQ(
      FlushDecision::SKIP,
      policy.shouldFlushDictionary(
          /*flushStripe=*/false,
          /*overMemoryBudget=*/false,
          // StripeProgress is not used in this test.
          dwio::common::StripeProgress{},
          context));
  EXPECT_EQ(
      FlushDecision::SKIP,
      policy.shouldFlushDictionary(
          /*flushStripe=*/false,
          /*overMemoryBudget=*/true,
          // StripeProgress is not used in this test.
          dwio::common::StripeProgress{},
          context));
  EXPECT_EQ(
      FlushDecision::SKIP,
      policy.shouldFlushDictionary(
          /*flushStripe=*/true,
          /*overMemoryBudget=*/false,
          // StripeProgress is not used in this test.
          dwio::common::StripeProgress{},
          context));
  EXPECT_EQ(
      FlushDecision::SKIP,
      policy.shouldFlushDictionary(
          /*flushStripe=*/true,
          /*overMemoryBudget=*/true,
          // StripeProgress is not used in this test.
          dwio::common::StripeProgress{},
          context));
}

TEST_F(DefaultFlushPolicyTest, FlushTest) {
  RowsPerStripeFlushPolicy policy({5, 7, 12});

  ASSERT_FALSE(policy.shouldFlush(
      dwio::common::StripeProgress{.stripeIndex = 0, .stripeRowCount = 1}))
      << "First Stripe with one row";
  ASSERT_TRUE(policy.shouldFlush(
      dwio::common::StripeProgress{.stripeIndex = 0, .stripeRowCount = 5}))
      << "First Stripe end";
  ASSERT_THROW(
      policy.shouldFlush(
          dwio::common::StripeProgress{.stripeIndex = 0, .stripeRowCount = 6}),
      exception::LoggedException)
      << "First Stripe has more rows";

  ASSERT_FALSE(policy.shouldFlush(
      dwio::common::StripeProgress{.stripeIndex = 1, .stripeRowCount = 6}))
      << "Second stripe before boundary";
  ASSERT_TRUE(policy.shouldFlush(
      dwio::common::StripeProgress{.stripeIndex = 1, .stripeRowCount = 7}))
      << "Second stipe boundary";

  ASSERT_FALSE(policy.shouldFlush(
      dwio::common::StripeProgress{.stripeIndex = 2, .stripeRowCount = 12}))
      << "Last Stripe should return false";

  ASSERT_THROW(
      policy.shouldFlush(
          dwio::common::StripeProgress{.stripeIndex = 3, .stripeRowCount = 0}),
      exception::LoggedException)
      << "Out of Range Stripe";
}
} // namespace facebook::velox::dwrf

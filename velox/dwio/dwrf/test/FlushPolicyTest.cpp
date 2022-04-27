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

TEST(DefaultFlushPolicyTest, StripeProgressTest) {
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
        testCase.stripeSizeThreshold, /* dictionarySizeThreshold */ 0};
    EXPECT_EQ(
        testCase.shouldFlush,
        policy.shouldFlush(dwio::common::StripeProgress{
            .stripeSizeEstimate = testCase.stripeSize}));
  }
}

TEST(DefaultFlushPolicyTest, AdditionalCriteriaTest) {
  struct TestCase {
    const bool stripeProgressDecision;
    const bool overMemoryBudget;
    const uint64_t dictionarySizeThreshold;
    const uint64_t dictionarySize;
    const FlushDecision decision;
  };

  std::vector<TestCase> testCases{
      TestCase{
          .stripeProgressDecision = false,
          .overMemoryBudget = false,
          .dictionarySizeThreshold = 20,
          .dictionarySize = 15,
          .decision = FlushDecision::SKIP},
      TestCase{
          .stripeProgressDecision = false,
          .overMemoryBudget = true,
          .dictionarySizeThreshold = 20,
          .dictionarySize = 15,
          .decision = FlushDecision::SKIP},
      TestCase{
          .stripeProgressDecision = true,
          .overMemoryBudget = false,
          .dictionarySizeThreshold = 20,
          .dictionarySize = 15,
          .decision = FlushDecision::SKIP},
      TestCase{
          .stripeProgressDecision = true,
          .overMemoryBudget = true,
          .dictionarySizeThreshold = 20,
          .dictionarySize = 15,
          .decision = FlushDecision::SKIP},
      TestCase{
          .stripeProgressDecision = false,
          .overMemoryBudget = false,
          .dictionarySizeThreshold = 20,
          .dictionarySize = 42,
          .decision = FlushDecision::FLUSH_DICTIONARY},
      TestCase{
          .stripeProgressDecision = false,
          .overMemoryBudget = true,
          .dictionarySizeThreshold = 20,
          .dictionarySize = 42,
          .decision = FlushDecision::FLUSH_DICTIONARY},
      TestCase{
          .stripeProgressDecision = true,
          .overMemoryBudget = false,
          .dictionarySizeThreshold = 20,
          .dictionarySize = 42,
          .decision = FlushDecision::FLUSH_DICTIONARY},
      TestCase{
          .stripeProgressDecision = true,
          .overMemoryBudget = true,
          .dictionarySizeThreshold = 20,
          .dictionarySize = 42,
          .decision = FlushDecision::FLUSH_DICTIONARY}};
  for (const auto& testCase : testCases) {
    DefaultFlushPolicy policy{
        /* stripeSizeThreshold */ 0, testCase.dictionarySizeThreshold};
    EXPECT_EQ(
        testCase.decision,
        policy.shouldFlushDictionary(
            testCase.stripeProgressDecision,
            testCase.overMemoryBudget,
            testCase.dictionarySize));
  }
}

TEST(StaticBudgetFlushPolicyTest, StripeProgressTest) {
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
    StaticBudgetFlushPolicy policy{
        testCase.stripeSizeThreshold, /* dictionarySizeThreshold */ 0};
    EXPECT_EQ(
        testCase.shouldFlush,
        policy.shouldFlush(dwio::common::StripeProgress{
            .stripeSizeEstimate = testCase.stripeSize}));
  }
}

TEST(StaticBudgetFlushPolicyTest, AdditionalCriteriaTest) {
  struct TestCase {
    const bool stripeProgressDecision;
    const bool overMemoryBudget;
    const uint64_t dictionarySizeThreshold;
    const uint64_t dictionarySize;
    const FlushDecision decision;
  };

  std::vector<TestCase> testCases{
      TestCase{
          .stripeProgressDecision = false,
          .overMemoryBudget = false,
          .dictionarySizeThreshold = 20,
          .dictionarySize = 15,
          .decision = FlushDecision::SKIP},
      TestCase{
          .stripeProgressDecision = false,
          .overMemoryBudget = true,
          .dictionarySizeThreshold = 20,
          .dictionarySize = 15,
          .decision = FlushDecision::ABANDON_DICTIONARY},
      TestCase{
          .stripeProgressDecision = true,
          .overMemoryBudget = false,
          .dictionarySizeThreshold = 20,
          .dictionarySize = 15,
          .decision = FlushDecision::SKIP},
      TestCase{
          .stripeProgressDecision = true,
          .overMemoryBudget = true,
          .dictionarySizeThreshold = 20,
          .dictionarySize = 15,
          .decision = FlushDecision::SKIP},
      TestCase{
          .stripeProgressDecision = false,
          .overMemoryBudget = false,
          .dictionarySizeThreshold = 20,
          .dictionarySize = 42,
          .decision = FlushDecision::FLUSH_DICTIONARY},
      TestCase{
          .stripeProgressDecision = false,
          .overMemoryBudget = true,
          .dictionarySizeThreshold = 20,
          .dictionarySize = 42,
          .decision = FlushDecision::ABANDON_DICTIONARY},
      TestCase{
          .stripeProgressDecision = true,
          .overMemoryBudget = false,
          .dictionarySizeThreshold = 20,
          .dictionarySize = 42,
          .decision = FlushDecision::FLUSH_DICTIONARY},
      TestCase{
          .stripeProgressDecision = true,
          .overMemoryBudget = true,
          .dictionarySizeThreshold = 20,
          .dictionarySize = 42,
          .decision = FlushDecision::FLUSH_DICTIONARY}};
  for (const auto& testCase : testCases) {
    StaticBudgetFlushPolicy policy{
        /* stripeSizeThreshold */ 0, testCase.dictionarySizeThreshold};
    EXPECT_EQ(
        testCase.decision,
        policy.shouldFlushDictionary(
            testCase.stripeProgressDecision,
            testCase.overMemoryBudget,
            testCase.dictionarySize));
  }
}

TEST(RowsPerStripeFlushPolicyTest, EmptyFile) {
  // Empty vector creation succeeds.
  RowsPerStripeFlushPolicy policy({});

  ASSERT_THROW(
      policy.shouldFlush(dwio::common::StripeProgress{}),
      exception::LoggedException);
}

TEST(RowsPerStripeFlushPolicyTest, InvalidCases) {
  // Vector with 0 rows, throws
  ASSERT_THROW(
      RowsPerStripeFlushPolicy policy({5, 7, 0, 10}),
      exception::LoggedException);
}

// RowsPerStripeFlushPolicy has no dictionary flush criteria.
TEST(RowsPerSTripeFlushPolicyTest, DictionaryCriteriaTest) {
  auto config = std::make_shared<Config>();
  WriterContext context{config, getDefaultScopedMemoryPool()};

  RowsPerStripeFlushPolicy policy({42});
  EXPECT_EQ(
      FlushDecision::SKIP, policy.shouldFlushDictionary(false, false, context));
  EXPECT_EQ(
      FlushDecision::SKIP, policy.shouldFlushDictionary(false, true, context));
  EXPECT_EQ(
      FlushDecision::SKIP, policy.shouldFlushDictionary(true, false, context));
  EXPECT_EQ(
      FlushDecision::SKIP, policy.shouldFlushDictionary(true, true, context));
}

TEST(RowsPerStripeFlushPolicyTest, FlushTest) {
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

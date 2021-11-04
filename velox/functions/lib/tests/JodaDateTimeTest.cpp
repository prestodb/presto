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
#include "velox/functions/lib/JodaDateTime.h"
#include "velox/common/base/Exceptions.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

using namespace facebook::velox;

namespace facebook::velox::functions {

namespace {

class JodaDateTimeTest : public testing::Test {};

TEST_F(JodaDateTimeTest, parseLiterals) {
  std::vector<std::string_view> expected;

  expected = {" "};
  EXPECT_EQ(expected, JodaFormatter(" ").literalTokens());

  expected = {" ", ""};
  EXPECT_EQ(expected, JodaFormatter(" Y").literalTokens());

  expected = {"", " "};
  EXPECT_EQ(expected, JodaFormatter("YY ").literalTokens());

  expected = {" 132&2618*673 *--+= }{[]\\:"};
  EXPECT_EQ(
      expected, JodaFormatter(" 132&2618*673 *--+= }{[]\\:").literalTokens());

  expected = {"   ", " &^  "};
  EXPECT_EQ(expected, JodaFormatter("   YYYY &^  ").literalTokens());

  expected = {"", "  % & ", " ", ""};
  EXPECT_EQ(expected, JodaFormatter("Y  % & YYY YYYYY").literalTokens());
}

TEST_F(JodaDateTimeTest, parsePatterns) {
  std::vector<JodaFormatSpecifier> expected;

  expected = {JodaFormatSpecifier::YEAR_OF_ERA};
  EXPECT_EQ(expected, JodaFormatter("Y").patternTokens());

  expected = {
      JodaFormatSpecifier::YEAR_OF_ERA,
      JodaFormatSpecifier::MONTH_OF_YEAR,
      JodaFormatSpecifier::DAY_OF_MONTH};
  EXPECT_EQ(expected, JodaFormatter("YYYY-MM-dd").patternTokens());
}

TEST_F(JodaDateTimeTest, parsePatternCount) {
  std::vector<size_t> expected;

  expected = {};
  EXPECT_EQ(expected, JodaFormatter("  ").patternTokensCount());

  expected = {1};
  EXPECT_EQ(expected, JodaFormatter("Y").patternTokensCount());

  expected = {4, 2, 2, 1};
  EXPECT_EQ(expected, JodaFormatter("YYYY YY YY Y").patternTokensCount());

  expected = {4, 2, 2, 1, 10};
  EXPECT_EQ(
      expected, JodaFormatter("YYYY-MM-dd m YYYYYYYYYY").patternTokensCount());
}

TEST_F(JodaDateTimeTest, invalid) {
  EXPECT_THROW(JodaFormatter(""), VeloxUserError);
  EXPECT_THROW(JodaFormatter("p"), VeloxUserError);
  EXPECT_THROW(JodaFormatter("P"), VeloxUserError);
  EXPECT_THROW(JodaFormatter("YDM u"), VeloxUserError);
}

} // namespace

} // namespace facebook::velox::functions

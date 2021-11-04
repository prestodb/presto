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
#include <cctype>
#include <unordered_map>
#include "velox/common/base/Exceptions.h"

namespace facebook::velox::functions {

namespace {

static constexpr size_t kJodaReserveSize{8};

inline JodaFormatSpecifier getSpecifier(char c) {
  static std::unordered_map<char, JodaFormatSpecifier> specifierMap{
      {'G', JodaFormatSpecifier::ERA},
      {'C', JodaFormatSpecifier::CENTURY_OF_ERA},
      {'Y', JodaFormatSpecifier::YEAR_OF_ERA},
      {'x', JodaFormatSpecifier::WEEK_YEAR},
      {'w', JodaFormatSpecifier::WEEK_OF_WEEK_YEAR},
      {'e', JodaFormatSpecifier::DAY_OF_WEEK},
      {'E', JodaFormatSpecifier::DAY_OF_WEEK_TEXT},
      {'y', JodaFormatSpecifier::YEAR},
      {'D', JodaFormatSpecifier::DAY_OF_YEAR},
      {'M', JodaFormatSpecifier::MONTH_OF_YEAR},
      {'d', JodaFormatSpecifier::DAY_OF_MONTH},
      {'a', JodaFormatSpecifier::HALFDAY_OF_DAY},
      {'K', JodaFormatSpecifier::HOUR_OF_HALFDAY},
      {'h', JodaFormatSpecifier::CLOCK_HOUR_OF_HALFDAY},
      {'H', JodaFormatSpecifier::HOUR_OF_DAY},
      {'k', JodaFormatSpecifier::CLOCK_HOUR_OF_DAY},
      {'m', JodaFormatSpecifier::MINUTE_OF_HOUR},
      {'s', JodaFormatSpecifier::SECOND_OF_MINUTE},
      {'S', JodaFormatSpecifier::FRACTION_OF_SECOND},
      {'z', JodaFormatSpecifier::TIMEZONE},
      {'Z', JodaFormatSpecifier::TIMEZONE_OFFSET_ID},
  };

  auto it = specifierMap.find(c);
  if (it == specifierMap.end()) {
    VELOX_USER_FAIL("Illegal pattern component: '{}'", c);
  }
  return it->second;
}

} // namespace

JodaFormatter::JodaFormatter(std::string format) : format_(std::move(format)) {
  literalTokens_.reserve(kJodaReserveSize);
  patternTokens_.reserve(kJodaReserveSize);
  patternTokensCount_.reserve(kJodaReserveSize);

  if (format_.empty()) {
    VELOX_USER_FAIL("Invalid pattern specification.");
  }

  const char* cur = format_.data();
  const char* end = cur + format_.size();

  // While we don't exhaust the buffer, we always interleave reading one
  // literal token, and one pattern, even if the literal token is empty.
  //
  // - pattern tokens are consecutive runs of the same letter (a-zA-Z).
  // - literal tokens are anything else in between.
  while (cur < end) {
    // 1. Always try to read and add a literal token first, if it's empty.
    const char* literalEnd = cur;

    while ((literalEnd < end) && !std::isalpha(*literalEnd)) {
      ++literalEnd;
    }

    literalTokens_.emplace_back(cur, literalEnd - cur);
    cur = literalEnd;

    // 2. If we still have at least one char left, try to read a pattern.
    if (cur < end) {
      const char* patternEnd = cur;

      while ((patternEnd < end) && (*cur == *patternEnd)) {
        ++patternEnd;
      }

      patternTokens_.emplace_back(getSpecifier(*cur));
      patternTokensCount_.emplace_back(patternEnd - cur);
      cur = patternEnd;
    }
  }

  // The first and last tokens are always literals, even if they are empty.
  if (literalTokens_.size() == patternTokens_.size()) {
    literalTokens_.emplace_back("");
  }
  VELOX_CHECK_EQ(literalTokens_.size(), patternTokens_.size() + 1);
}

} // namespace facebook::velox::functions

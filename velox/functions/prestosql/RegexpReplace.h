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
#pragma once

#include <string>

#include <re2/re2.h>

#include "folly/CPortability.h"

#include "velox/common/base/Exceptions.h"
#include "velox/functions/lib/Re2Functions.h"

namespace facebook::velox::functions {

/// This function preprocesses an input pattern string to follow RE2 syntax for
/// Re2RegexpReplacePresto. Specifically, Presto using RE2J supports named
/// capturing groups as (?<name>regex) or (?P<name>regex), but RE2 only supports
/// (?P<name>regex), so we convert the former format to the latter.
FOLLY_ALWAYS_INLINE std::string preparePrestoRegexpReplacePattern(
    const StringView& pattern) {
  static const RE2 kRegex("[(][?]<([^>]*)>");

  std::string newPattern = pattern.getString();
  RE2::GlobalReplace(&newPattern, kRegex, R"((?P<\1>)");

  return newPattern;
}

/// This function preprocesses an input replacement string to follow RE2 syntax
/// for Re2RegexpReplacePresto. Specifically, Presto using RE2J supports
/// referencing capturing groups with $g or ${name} in replacement, but RE2 only
/// supports referencing numbered capturing groups with \g. So we replace
/// references to named groups with references to the corresponding numbered
/// groups. In addition, Presto using RE2J expects the literal $ character to be
/// escaped as \$, but RE2 does not allow escaping $ in replacement, so we
/// unescape \$ in this function.
FOLLY_ALWAYS_INLINE std::string preparePrestoRegexpReplaceReplacement(
    const RE2& re,
    const StringView& replacement) {
  if (replacement.size() == 0) {
    return std::string{};
  }

  auto newReplacement = replacement.getString();

  static const RE2 kExtractRegex(R"(\${([^}]*)})");
  VELOX_DCHECK(
      kExtractRegex.ok(),
      "Invalid regular expression {}: {}.",
      R"(\${([^}]*)})",
      kExtractRegex.error());

  // If newReplacement contains a reference to a
  // named capturing group ${name}, replace the name with its index.
  re2::StringPiece groupName[2];
  while (kExtractRegex.Match(
      newReplacement,
      0,
      newReplacement.size(),
      RE2::UNANCHORED,
      groupName,
      2)) {
    auto groupIter = re.NamedCapturingGroups().find(groupName[1].as_string());
    if (groupIter == re.NamedCapturingGroups().end()) {
      VELOX_USER_FAIL(
          "Invalid replacement sequence: unknown group {{ {} }}.",
          groupName[1].as_string());
    }

    RE2::GlobalReplace(
        &newReplacement,
        fmt::format(R"(\${{{}}})", groupName[1].as_string()),
        fmt::format("${}", groupIter->second));
  }

  // Convert references to numbered capturing groups from $g to \g.
  static const RE2 kConvertRegex(R"(\$(\d+))");
  VELOX_DCHECK(
      kConvertRegex.ok(),
      "Invalid regular expression {}: {}.",
      R"(\$(\d+))",
      kConvertRegex.error());
  RE2::GlobalReplace(&newReplacement, kConvertRegex, R"(\\\1)");

  // Un-escape dollar-sign '$'.
  static const RE2 kUnescapeRegex(R"(\\\$)");
  VELOX_DCHECK(
      kUnescapeRegex.ok(),
      "Invalid regular expression {}: {}.",
      R"(\\\$)",
      kUnescapeRegex.error());
  RE2::GlobalReplace(&newReplacement, kUnescapeRegex, "$");

  return newReplacement;
}

template <typename T>
using Re2RegexpReplacePresto = Re2RegexpReplace<
    T,
    preparePrestoRegexpReplacePattern,
    preparePrestoRegexpReplaceReplacement>;

} // namespace facebook::velox::functions

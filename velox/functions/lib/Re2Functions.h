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

#include <memory>
#include <vector>

#include <re2/re2.h>
#include "velox/expression/VectorFunction.h"
#include "velox/functions/Udf.h"
#include "velox/vector/BaseVector.h"

namespace facebook::velox::functions {

/// Representation of different kinds of patterns.
enum class PatternKind {
  /// Pattern containing wildcard character '_' only, such as _, __, ____.
  kExactlyN,
  /// Pattern containing wildcard characters ('_' or '%') only with at least one
  /// '%', such as ___%, _%__.
  kAtLeastN,
  /// Pattern with no wildcard characters, such as 'presto', 'foo'.
  kFixed,
  /// Pattern with single wildcard chars(_) & normal chars, such as
  /// '_pr_es_to_'.
  kRelaxedFixed,
  /// Fixed pattern followed by one or more '%', such as 'hello%', 'foo%%%%'.
  kPrefix,
  /// kRelaxedFixed pattern followed by one or more '%', such as '_pr_es_to_%',
  /// '_pr_es_to_%%%%'.
  kRelaxedPrefix,
  /// Fixed pattern preceded by one or more '%', such as '%foo', '%%%hello'.
  kSuffix,
  /// kRelaxedFixed preceded by one or more '%', such as '%_pr_es_to_',
  /// '%%%_pr_es_to_'.
  kRelaxedSuffix,
  /// Patterns matching '%{c0}%', such as '%foo%%', '%%%hello%'.
  kSubstring,
  /// Patterns matching '%{c0}%{c1}%', such as '%%foo%%bar%%', '%foo%bar%'.
  /// Note: Unlike kSubstring, kSubstrings applies only to constant patterns
  /// as pattern parsing is expensive.
  kSubstrings,
  /// Patterns which do not fit any of the above types, such as 'hello_world',
  /// '_presto%'.
  kGeneric,
};

// Kind of sub-pattern.
enum SubPatternKind {
  /// e.g. '___'.
  kSingleCharWildcard = 0,
  // e.g. '%%'.
  kAnyCharsWildcard = 1,
  // e.g. 'abc'.
  kLiteralString = 2
};

struct SubPatternMetadata {
  SubPatternKind kind;
  // The index of current pattern in terms of 'bytes'.
  size_t start;
  // Length in terms of bytes.
  size_t length;
};

class PatternMetadata {
 public:
  static PatternMetadata generic();

  static PatternMetadata atLeastN(size_t length);

  static PatternMetadata exactlyN(size_t length);

  static PatternMetadata fixed(const std::string& fixedPattern);

  static PatternMetadata relaxedFixed(
      std::string fixedPattern,
      std::vector<SubPatternMetadata> subPatterns);

  static PatternMetadata prefix(const std::string& fixedPattern);

  static PatternMetadata relaxedPrefix(
      std::string fixedPattern,
      std::vector<SubPatternMetadata> subPatterns);

  static PatternMetadata suffix(const std::string& fixedPattern);

  static PatternMetadata relaxedSuffix(
      std::string fixedPattern,
      std::vector<SubPatternMetadata> subPatterns);

  static PatternMetadata substring(const std::string& fixedPattern);

  static PatternMetadata substrings(std::vector<std::string> substrings);

  static std::vector<std::string> parseSubstrings(
      const std::string_view& pattern);

  PatternKind patternKind() const {
    return patternKind_;
  }

  size_t length() const {
    return length_;
  }

  const std::vector<SubPatternMetadata>& subPatterns() const {
    return subPatterns_;
  }

  const std::string& fixedPattern() const {
    return fixedPattern_;
  }

  const std::vector<std::string>& substrings() const {
    return substrings_;
  }

 private:
  PatternMetadata(
      PatternKind patternKind,
      size_t length,
      std::string fixedPattern,
      std::vector<SubPatternMetadata> subPatterns,
      std::vector<std::string> substrings);

  PatternKind patternKind_;

  /// Contains the length of the unescaped fixed pattern for patterns of kind
  /// k[Relaxed]Fixed, k[Relaxed]Prefix, k[Relaxed]Suffix and
  /// k[Relaxed]Substring. Contains the count of wildcard character '_' for
  /// patterns of kind kExactlyN and kAtLeastN. Contains 0 otherwise.
  size_t length_;

  /// Contains the fixed pattern in patterns of kind k[Relaxed]Fixed,
  /// k[Relaxed]Prefix, k[Relaxed]Suffix and k[Relaxed]Substring.
  std::string fixedPattern_;

  /// Contains the literal/single char wildcard sub patterns, it is only
  /// used for kRelaxedXxx patterns. e.g. If the pattern is: _pr_sto%, we will
  /// have four sub-patterns here: _, pr, _ and sto.
  std::vector<SubPatternMetadata> subPatterns_;

  std::vector<std::string> substrings_;
};

inline const int kMaxCompiledRegexes = 20;

/// The functions in this file use RE2 as the regex engine. RE2 is fast, but
/// supports only a subset of PCRE syntax and in particular does not support
/// backtracking and associated features (e.g. backreferences).
/// See https://github.com/google/re2/wiki/Syntax for more information.

/// re2Match(string, pattern) → bool
///
/// Returns whether str matches the regex pattern.  pattern will be parsed using
/// RE2 pattern syntax, a subset of PCRE. If the pattern is invalid, throws an
/// exception.
std::shared_ptr<exec::VectorFunction> makeRe2Match(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config);

std::vector<std::shared_ptr<exec::FunctionSignature>> re2MatchSignatures();

/// re2Search(string, pattern) → bool
///
/// Returns whether str has a substr that matches the regex pattern.  pattern
/// will be parsed using RE2 pattern syntax, a subset of PCRE. If the pattern is
/// invalid, throws an exception.
std::shared_ptr<exec::VectorFunction> makeRe2Search(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config);

std::vector<std::shared_ptr<exec::FunctionSignature>> re2SearchSignatures();

/// re2Extract(string, pattern, group_id) → string
/// re2Extract(string, pattern) → string
///
/// If string has a substring that matches the given pattern, returns the
/// substring matching the given group in the pattern. pattern will be parsed
/// using the RE2 pattern syntax, a subset of PCRE. Groups are 1-indexed.
/// Providing zero as the group_id extracts and returns the entire match; this
/// is more efficient than extracting a subgroup. Extracting the first subgroup
/// is more efficient than extracting larger indexes; use non-capturing
/// subgroups (?:...) if the pattern includes groups that don't need to be
/// captured.
///
/// If the pattern is invalid or the group id is out of range, throws an
/// exception. If the pattern does not match, returns null.
///
/// If group_id parameter is not specified, extracts and returns the entire
/// match.
std::shared_ptr<exec::VectorFunction> makeRe2Extract(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config,
    const bool emptyNoMatch);

std::vector<std::shared_ptr<exec::FunctionSignature>> re2ExtractSignatures();

/// Return the pair {pattern kind, length of the fixed pattern} for fixed,
/// prefix, and suffix patterns. Return the pair {pattern kind, number of '_'
/// characters} for patterns with wildcard characters only. Return
/// {kGenericPattern, 0} for generic patterns).
PatternMetadata determinePatternKind(
    std::string_view pattern,
    std::optional<char> escapeChar);

std::shared_ptr<exec::VectorFunction> makeLike(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config);

std::vector<std::shared_ptr<exec::FunctionSignature>> likeSignatures();

/// re2ExtractAll(string, pattern, group_id) → array<string>
/// re2ExtractAll(string, pattern) → array<string>
///
/// If string has a substring that matches the given pattern, returns ALL of the
/// substrings matching the given group in the pattern. pattern will be parsed
/// using the RE2 pattern syntax, a subset of PCRE. Groups are 1-indexed.
/// Providing zero as the group_id extracts and returns the entire match; this
/// is more efficient than extracting a subgroup. Extracting the first subgroup
/// is more efficient than extracting larger indexes; use non-capturing
/// subgroups (?:...) if the pattern includes groups that don't need to be
/// captured.
///
/// If the pattern is invalid or the group id is out of range, throws an
/// exception. If the pattern does not match, returns null.
///
/// If group_id parameter is not specified, extracts and returns the entire
/// match.
std::shared_ptr<exec::VectorFunction> makeRe2ExtractAll(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config);

std::vector<std::shared_ptr<exec::FunctionSignature>> re2ExtractAllSignatures();

namespace detail {

// A cache of compiled regular expressions (RE2 instances). Allows up to
// 'kMaxCompiledRegexes' different expressions.
//
// Compiling regular expressions is expensive. It can take up to 200 times
// more CPU time to compile a regex vs. evaluate it.
class ReCache {
 public:
  RE2* findOrCompile(const StringView& pattern);

  Expected<RE2*> tryFindOrCompile(const StringView& pattern);

 private:
  folly::F14FastMap<std::string, std::unique_ptr<RE2>> cache_;
};

} // namespace detail

/// regexp_replace(string, pattern, replacement) -> string
/// regexp_replace(string, pattern) -> string
///
/// If string has substrings that match the given pattern, return a new string
/// that has all the matched substrings replaced with the given replacement
/// sequence or removed if no replacement sequence is provided. pattern will
/// be parsed using the RE2 pattern syntax, a subset of PCRE. If pattern is
/// invalid for RE2, this function throws an exception. replacement is a string
/// that may contain references to the named or numbered capturing groups in the
/// pattern. If referenced capturing group names in replacement are invalid for
/// RE2, this function throws an exception.
template <
    typename T,
    std::string (*prepareRegexpPattern)(const StringView&),
    std::string (*prepareRegexpReplacement)(const RE2&, const StringView&)>
struct Re2RegexpReplace {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Varchar>* /*string*/,
      const arg_type<Varchar>* pattern,
      const arg_type<Varchar>* replacement) {
    if (pattern != nullptr) {
      const auto processedPattern = prepareRegexpPattern(*pattern);
      re_.emplace(processedPattern, RE2::Quiet);
      VELOX_USER_CHECK(
          re_->ok(),
          "Invalid regular expression {}: {}.",
          processedPattern,
          re_->error());
    }

    if (replacement != nullptr) {
      // Constant 'replacement' with non-constant 'pattern' needs to be
      // processed separately for each row.
      if (pattern != nullptr) {
        ensureProcessedReplacement(re_.value(), *replacement);
        constantReplacement_ = true;
      }
    }
  }

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& inputTypes,
      const core::QueryConfig& config,
      const arg_type<Varchar>* string,
      const arg_type<Varchar>* pattern) {
    initialize(inputTypes, config, string, pattern, nullptr);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& out,
      const arg_type<Varchar>& string,
      const arg_type<Varchar>& pattern,
      const arg_type<Varchar>& replacement = StringView{}) {
    auto& re = ensurePattern(pattern);
    const auto& processedReplacement =
        ensureProcessedReplacement(re, replacement);

    result_.assign(string.data(), string.size());
    RE2::GlobalReplace(&result_, re, processedReplacement);

    UDFOutputString::assign(out, result_);
  }

 private:
  RE2& ensurePattern(const arg_type<Varchar>& pattern) {
    if (!re_.has_value()) {
      auto processedPattern = prepareRegexpPattern(pattern);
      return *cache_.findOrCompile(StringView(processedPattern));
    } else {
      return re_.value();
    }
  }

  const std::string& ensureProcessedReplacement(
      RE2& re,
      const arg_type<Varchar>& replacement) {
    if (!constantReplacement_) {
      processedReplacement_ = prepareRegexpReplacement(re, replacement);
    }

    return processedReplacement_;
  }

  // Used when pattern is constant.
  std::optional<RE2> re_;

  // True if replacement is constant.
  bool constantReplacement_{false};

  // Constant replacement if 'constantReplacement_' is true, or 'current'
  // replacement.
  std::string processedReplacement_;

  // Used when pattern is not constant.
  detail::ReCache cache_;

  // Scratch memory to store result of replacement.
  std::string result_;
};

template <typename TExec>
struct Re2RegexpSplit {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  static constexpr int32_t reuse_strings_from_arg = 0;

  void call(
      out_type<Array<Varchar>>& out,
      const arg_type<Varchar>& string,
      const arg_type<Varchar>& pattern) {
    auto* re = cache_.findOrCompile(pattern);

    const auto re2String = re2::StringPiece(string.data(), string.size());

    size_t pos = 0;
    const char* start = string.data();

    re2::StringPiece subMatches[1];
    while (re->Match(
        re2String,
        pos,
        string.size(),
        RE2::Anchor::UNANCHORED,
        subMatches,
        1)) {
      const auto fullMatch = subMatches[0];
      const auto offset = fullMatch.data() - start;
      const auto size = fullMatch.size();

      out.add_item().setNoCopy(StringView(string.data() + pos, offset - pos));

      pos = offset + size;
      if (UNLIKELY(size == 0)) {
        ++pos;
      }
    }

    out.add_item().setNoCopy(
        StringView(string.data() + pos, string.size() - pos));
  }

 private:
  detail::ReCache cache_;
};

std::shared_ptr<exec::VectorFunction> makeRegexpReplaceWithLambda(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config);

std::vector<std::shared_ptr<exec::FunctionSignature>>
regexpReplaceWithLambdaSignatures();

} // namespace facebook::velox::functions

template <>
struct fmt::formatter<facebook::velox::functions::PatternKind>
    : formatter<int> {
  auto format(facebook::velox::functions::PatternKind s, format_context& ctx)
      const {
    return formatter<int>::format(static_cast<int>(s), ctx);
  }
};

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
    const std::vector<exec::VectorFunctionArg>& inputArgs);

std::vector<std::shared_ptr<exec::FunctionSignature>> re2MatchSignatures();

/// re2Search(string, pattern) → bool
///
/// Returns whether str has a substr that matches the regex pattern.  pattern
/// will be parsed using RE2 pattern syntax, a subset of PCRE. If the pattern is
/// invalid, throws an exception.
std::shared_ptr<exec::VectorFunction> makeRe2Search(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs);

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
    const bool emptyNoMatch);

std::vector<std::shared_ptr<exec::FunctionSignature>> re2ExtractSignatures();

std::shared_ptr<exec::VectorFunction> makeLike(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs);

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
    const std::vector<exec::VectorFunctionArg>& inputArgs);

std::vector<std::shared_ptr<exec::FunctionSignature>> re2ExtractAllSignatures();

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

  std::string processedReplacement_;
  std::string result_;
  std::optional<RE2> re_;

  FOLLY_ALWAYS_INLINE void initialize(
      const core::QueryConfig& /*config*/,
      const arg_type<Varchar>* /*string*/,
      const arg_type<Varchar>* pattern,
      const arg_type<Varchar>* replacement) {
    VELOX_USER_CHECK(
        pattern != nullptr, "Pattern of regexp_replace must be constant.");
    VELOX_USER_CHECK(
        replacement != nullptr,
        "Replacement sequence of regexp_replace must be constant.");

    auto processedPattern = prepareRegexpPattern(*pattern);

    re_.emplace(processedPattern, RE2::Quiet);
    if (UNLIKELY(!re_->ok())) {
      VELOX_USER_FAIL(
          "Invalid regular expression {}: {}.", processedPattern, re_->error());
    }

    processedReplacement_ = prepareRegexpReplacement(*re_, *replacement);
  }

  FOLLY_ALWAYS_INLINE void initialize(
      const core::QueryConfig& config,
      const arg_type<Varchar>* string,
      const arg_type<Varchar>* pattern) {
    StringView emptyReplacement;

    initialize(config, string, pattern, &emptyReplacement);
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& out,
      const arg_type<Varchar>& string,
      const arg_type<Varchar>& /*pattern*/,
      const arg_type<Varchar>& /*replacement*/ = StringView{}) {
    result_.assign(string.data(), string.size());
    RE2::GlobalReplace(&result_, *re_, processedReplacement_);

    UDFOutputString::assign(out, result_);

    return true;
  }
};

} // namespace facebook::velox::functions

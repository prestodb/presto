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
#include "velox/functions/lib/Re2Functions.h"
#include "velox/functions/lib/string/StringImpl.h"

#include <re2/re2.h>

namespace facebook::velox::functions {
namespace {

static const int kMaxCompiledRegexes = 20;

void checkForBadPattern(const RE2& re) {
  if (UNLIKELY(!re.ok())) {
    VELOX_USER_FAIL("invalid regular expression:{}", re.error());
  }
}

template <typename T>
re2::StringPiece toStringPiece(const T& s) {
  return re2::StringPiece(s.data(), s.size());
}

// A cache of compiled regular expressions (RE2 instances). Allows up to
// 'kMaxCompiledRegexes' different expressions.
//
// Compiling regular expressions is expensive. It can take up to 200 times
// more CPU time to compile a regex vs. evaluate it.
class ReCache {
 public:
  RE2* findOrCompile(const StringView& pattern) {
    const std::string key = pattern;

    auto reIt = cache_.find(key);
    if (reIt != cache_.end()) {
      return reIt->second.get();
    }

    VELOX_USER_CHECK_LT(
        cache_.size(), kMaxCompiledRegexes, "Max number of regex reached");

    auto re = std::make_unique<RE2>(toStringPiece(pattern), RE2::Quiet);
    checkForBadPattern(*re);

    auto [it, inserted] = cache_.emplace(key, std::move(re));
    VELOX_CHECK(inserted);

    return it->second.get();
  }

 private:
  folly::F14FastMap<std::string, std::unique_ptr<RE2>> cache_;
};

std::string printTypesCsv(
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  std::string result;
  result.reserve(inputArgs.size() * 10);
  for (const auto& input : inputArgs) {
    folly::toAppend(
        result.empty() ? "" : ", ", input.type->toString(), &result);
  }
  return result;
}

// If v is a non-null constant vector, returns the constant value. Otherwise
// returns nullopt.
template <typename T>
std::optional<T> getIfConstant(const BaseVector& v) {
  if (v.encoding() == VectorEncoding::Simple::CONSTANT &&
      v.isNullAt(0) == false) {
    return v.as<ConstantVector<T>>()->valueAt(0);
  }
  return std::nullopt;
}

FlatVector<bool>& ensureWritableBool(
    const SelectivityVector& rows,
    exec::EvalCtx& context,
    VectorPtr& result) {
  context.ensureWritable(rows, BOOLEAN(), result);
  return *result->as<FlatVector<bool>>();
}

FlatVector<StringView>& ensureWritableStringView(
    const SelectivityVector& rows,
    exec::EvalCtx& context,
    VectorPtr& result) {
  context.ensureWritable(rows, VARCHAR(), result);
  auto* flat = result->as<FlatVector<StringView>>();
  flat->mutableValues(rows.end());
  return *flat;
}

bool re2FullMatch(StringView str, const RE2& re) {
  return RE2::FullMatch(toStringPiece(str), re);
}

bool re2PartialMatch(StringView str, const RE2& re) {
  return RE2::PartialMatch(toStringPiece(str), re);
}

bool re2Extract(
    FlatVector<StringView>& result,
    int row,
    const RE2& re,
    const exec::LocalDecodedVector& strs,
    std::vector<re2::StringPiece>& groups,
    int32_t groupId,
    bool emptyNoMatch) {
  const StringView str = strs->valueAt<StringView>(row);
  DCHECK_GT(groups.size(), groupId);
  if (!re.Match(
          toStringPiece(str),
          0,
          str.size(),
          RE2::UNANCHORED, // Full match not required.
          groups.data(),
          groupId + 1)) {
    if (emptyNoMatch) {
      result.setNoCopy(row, StringView(nullptr, 0));
      return true;
    } else {
      result.setNull(row, true);
      return false;
    }
  } else {
    const re2::StringPiece extracted = groups[groupId];
    result.setNoCopy(row, StringView(extracted.data(), extracted.size()));
    return !StringView::isInline(extracted.size());
  }
}

std::string likePatternToRe2(
    StringView pattern,
    std::optional<char> escapeChar,
    bool& validPattern) {
  std::string regex;
  validPattern = true;
  regex.reserve(pattern.size() * 2);
  regex.append("^");
  bool escaped = false;
  for (const char c : pattern) {
    if (escaped && !(c == '%' || c == '_' || c == escapeChar)) {
      validPattern = false;
    }
    if (!escaped && c == escapeChar) {
      escaped = true;
    } else {
      switch (c) {
        case '%':
          regex.append(escaped ? "%" : ".*");
          escaped = false;
          break;
        case '_':
          regex.append(escaped ? "_" : ".");
          escaped = false;
          break;
        // Escape all the meta characters in re2
        case '\\':
        case '|':
        case '^':
        case '$':
        case '.':
        case '*':
        case '+':
        case '?':
        case '(':
        case ')':
        case '[':
        case ']':
        case '{':
        case '}':
          regex.append("\\"); // Append the meta character after the escape.
          [[fallthrough]];
        default:
          regex.append(1, c);
          escaped = false;
      }
    }
  }
  if (escaped) {
    validPattern = false;
  }

  regex.append("$");
  return regex;
}

template <bool (*Fn)(StringView, const RE2&)>
class Re2MatchConstantPattern final : public exec::VectorFunction {
 public:
  explicit Re2MatchConstantPattern(StringView pattern)
      : re_(toStringPiece(pattern), RE2::Quiet) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& resultRef) const final {
    VELOX_CHECK_EQ(args.size(), 2);
    FlatVector<bool>& result = ensureWritableBool(rows, context, resultRef);
    exec::LocalDecodedVector toSearch(context, *args[0], rows);
    try {
      checkForBadPattern(re_);
    } catch (const std::exception& e) {
      context.setErrors(rows, std::current_exception());
      return;
    }

    context.applyToSelectedNoThrow(rows, [&](vector_size_t i) {
      result.set(i, Fn(toSearch->valueAt<StringView>(i), re_));
    });
  }

 private:
  RE2 re_;
};

template <bool (*Fn)(StringView, const RE2&)>
class Re2Match final : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& resultRef) const override {
    VELOX_CHECK_EQ(args.size(), 2);
    if (auto pattern = getIfConstant<StringView>(*args[1])) {
      Re2MatchConstantPattern<Fn>(*pattern).apply(
          rows, args, outputType, context, resultRef);
      return;
    }
    // General case.
    FlatVector<bool>& result = ensureWritableBool(rows, context, resultRef);
    exec::LocalDecodedVector toSearch(context, *args[0], rows);
    exec::LocalDecodedVector pattern(context, *args[1], rows);
    context.applyToSelectedNoThrow(rows, [&](vector_size_t row) {
      auto& re = *cache_.findOrCompile(pattern->valueAt<StringView>(row));
      result.set(row, Fn(toSearch->valueAt<StringView>(row), re));
    });
  }

 private:
  mutable ReCache cache_;
};

void checkForBadGroupId(int64_t groupId, const RE2& re) {
  if (UNLIKELY(groupId < 0 || groupId > re.NumberOfCapturingGroups())) {
    VELOX_USER_FAIL("No group {} in regex '{}'", groupId, re.pattern());
  }
}

template <typename T>
class Re2SearchAndExtractConstantPattern final : public exec::VectorFunction {
 public:
  explicit Re2SearchAndExtractConstantPattern(
      StringView pattern,
      bool emptyNoMatch)
      : re_(toStringPiece(pattern), RE2::Quiet), emptyNoMatch_(emptyNoMatch) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& resultRef) const final {
    VELOX_CHECK(args.size() == 2 || args.size() == 3);
    // TODO: Potentially re-use the string vector, not just the buffer.
    FlatVector<StringView>& result =
        ensureWritableStringView(rows, context, resultRef);

    // apply() will not be invoked if the selection is empty.
    try {
      checkForBadPattern(re_);
    } catch (const std::exception& e) {
      context.setErrors(rows, std::current_exception());
      return;
    }

    exec::LocalDecodedVector toSearch(context, *args[0], rows);
    bool mustRefSourceStrings = false;
    FOLLY_DECLARE_REUSED(groups, std::vector<re2::StringPiece>);
    // Common case: constant group id.
    if (args.size() == 2) {
      groups.resize(1);
      context.applyToSelectedNoThrow(rows, [&](vector_size_t i) {
        mustRefSourceStrings |=
            re2Extract(result, i, re_, toSearch, groups, 0, emptyNoMatch_);
      });
      if (mustRefSourceStrings) {
        result.acquireSharedStringBuffers(toSearch->base());
      }
      return;
    }

    if (const auto groupId = getIfConstant<T>(*args[2])) {
      try {
        checkForBadGroupId(*groupId, re_);
      } catch (const std::exception& e) {
        context.setErrors(rows, std::current_exception());
        return;
      }

      groups.resize(*groupId + 1);
      context.applyToSelectedNoThrow(rows, [&](vector_size_t i) {
        mustRefSourceStrings |= re2Extract(
            result, i, re_, toSearch, groups, *groupId, emptyNoMatch_);
      });
      if (mustRefSourceStrings) {
        result.acquireSharedStringBuffers(toSearch->base());
      }
      return;
    }

    // Less common case: variable group id. Resize the groups vector to
    // number of capturing groups + 1.
    exec::LocalDecodedVector groupIds(context, *args[2], rows);

    groups.resize(re_.NumberOfCapturingGroups() + 1);
    context.applyToSelectedNoThrow(rows, [&](vector_size_t i) {
      T group = groupIds->valueAt<T>(i);
      checkForBadGroupId(group, re_);
      mustRefSourceStrings |=
          re2Extract(result, i, re_, toSearch, groups, group, emptyNoMatch_);
    });
    if (mustRefSourceStrings) {
      result.acquireSharedStringBuffers(toSearch->base());
    }
  }

 private:
  RE2 re_;
  const bool emptyNoMatch_;
};

// The factory function we provide returns a unique instance for each call, so
// this is safe.
template <typename T>
class Re2SearchAndExtract final : public exec::VectorFunction {
 public:
  explicit Re2SearchAndExtract(bool emptyNoMatch)
      : emptyNoMatch_(emptyNoMatch) {}
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& resultRef) const final {
    VELOX_CHECK(args.size() == 2 || args.size() == 3);
    // Handle the common case of a constant pattern.
    if (auto pattern = getIfConstant<StringView>(*args[1])) {
      Re2SearchAndExtractConstantPattern<T>(*pattern, emptyNoMatch_)
          .apply(rows, args, outputType, context, resultRef);
      return;
    }

    // The general case. Further optimizations are possible to avoid regex
    // recompilation, but a constant pattern is by far the most common case.
    FlatVector<StringView>& result =
        ensureWritableStringView(rows, context, resultRef);
    exec::LocalDecodedVector toSearch(context, *args[0], rows);
    exec::LocalDecodedVector pattern(context, *args[1], rows);
    bool mustRefSourceStrings = false;
    FOLLY_DECLARE_REUSED(groups, std::vector<re2::StringPiece>);
    if (args.size() == 2) {
      groups.resize(1);
      context.applyToSelectedNoThrow(rows, [&](vector_size_t i) {
        auto& re = *cache_.findOrCompile(pattern->valueAt<StringView>(i));
        mustRefSourceStrings |=
            re2Extract(result, i, re, toSearch, groups, 0, emptyNoMatch_);
      });
    } else {
      exec::LocalDecodedVector groupIds(context, *args[2], rows);
      context.applyToSelectedNoThrow(rows, [&](vector_size_t i) {
        const auto groupId = groupIds->valueAt<T>(i);
        auto& re = *cache_.findOrCompile(pattern->valueAt<StringView>(i));
        checkForBadGroupId(groupId, re);
        groups.resize(groupId + 1);
        mustRefSourceStrings |=
            re2Extract(result, i, re, toSearch, groups, groupId, emptyNoMatch_);
      });
    }
    if (mustRefSourceStrings) {
      result.acquireSharedStringBuffers(toSearch->base());
    }
  }

 private:
  const bool emptyNoMatch_;
  mutable ReCache cache_;
};

namespace {

// Sub-pattern's stats in a top-level pattern.
struct SubPatternStats {
  SubPatternKind kind;

  // Number of times this sub-pattern occurs in the overall pattern.
  size_t count = 0;

  // First index of this pattern kind.
  std::optional<size_t> firstIndex = std::nullopt;

  // Last index of this pattern kind. lastIndex will be set as the same value as
  // firstIndex if the kind of sub-pattern only occur once.
  std::optional<size_t> lastIndex = std::nullopt;

  void update(size_t index) {
    count++;
    if (!firstIndex.has_value()) {
      firstIndex = index;
    }

    lastIndex = index;
  }
};

// Construct SubPatternMetadata from subPatternKinds, subPatternRanges.
// Caller need to make sure the specified range only contains
// fixed(kLiteralString, kSingleWildcard) patterns.
size_t buildFixedSubPatterns(
    const std::vector<SubPatternKind>& subPatternKinds,
    const std::vector<std::pair<size_t, size_t>>& subPatternRanges,
    size_t start,
    size_t end,
    std::vector<SubPatternMetadata>& subPatterns) {
  size_t indexInFixedPattern = 0;
  for (auto i = start; i < end; i++) {
    const auto kind = subPatternKinds[i];
    if (kind == SubPatternKind::kLiteralString ||
        kind == SubPatternKind::kSingleCharWildcard) {
      subPatterns.push_back(
          {kind, indexInFixedPattern, subPatternRanges[i].second});
    } else {
      VELOX_UNREACHABLE();
    }
    indexInFixedPattern += subPatternRanges[i].second;
  }

  return indexInFixedPattern;
}

// Return the length of the fixed part(literal chars or single char wildcard) of
// the pattern, it is mainly used to get the length of the fixed part in a
// pattern, so we can extract the fixed part out.
size_t fixedLength(
    const std::vector<SubPatternKind>& subPatternKinds,
    const std::vector<std::pair<size_t, size_t>>& subPatternRanges) {
  size_t result = 0;
  for (auto i = 0; i < subPatternKinds.size(); i++) {
    if (subPatternKinds[i] != SubPatternKind::kAnyCharsWildcard) {
      result += subPatternRanges[i].second;
    }
  }

  return result;
}

// Return the number of bytes in the specified unicode character. Returns 1 if
// specified character is not a valid UTF-8.
size_t unicodeCharLength(const char* str) {
  auto size = utf8proc_char_length(str);
  // Skip bad byte if we get utf length < 0.
  return UNLIKELY(size < 0) ? 1 : size;
}

} // namespace

// Match string 'input' with a fixed pattern (with no wildcard characters).
bool matchExactPattern(
    StringView input,
    const std::string& pattern,
    size_t length) {
  if (FOLLY_LIKELY(pattern.size() > 0)) {
    return input.size() == pattern.size() &&
        std::memcmp(input.data(), pattern.data(), length) == 0;
  }

  return input.size() == 0;
}

std::pair<bool, int32_t> matchRelaxedFixedForwardAscii(
    StringView input,
    const PatternMetadata& patternMetadata,
    size_t start) {
  if (input.size() - start < patternMetadata.length()) {
    return std::make_pair(false, -1);
  }

  for (const auto& subPattern : patternMetadata.subPatterns()) {
    if (subPattern.kind == SubPatternKind::kLiteralString &&
        std::memcmp(
            input.data() + start + subPattern.start,
            patternMetadata.fixedPattern().data() + subPattern.start,
            subPattern.length) != 0) {
      return std::make_pair(false, -1);
    }
  }

  return std::make_pair(true, start + patternMetadata.length());
}

std::pair<bool, int32_t> matchRelaxedFixedForwardUnicode(
    StringView input,
    const PatternMetadata& patternMetadata,
    size_t start) {
  // Compare the length first.
  if (input.size() - start < patternMetadata.length()) {
    return std::make_pair(false, -1);
  }

  auto cursor = start;
  for (const auto& subPattern : patternMetadata.subPatterns()) {
    if (subPattern.kind == SubPatternKind::kSingleCharWildcard) {
      // Match every single char wildcard.
      for (auto i = 0; i < subPattern.length; i++) {
        if (cursor >= input.size()) {
          return std::make_pair(false, -1);
        }

        auto numBytes = unicodeCharLength(input.data() + cursor);
        cursor += numBytes;
      }
    } else {
      const auto currentLength = subPattern.length;
      if (cursor + currentLength > input.size() ||
          std::memcmp(
              input.data() + cursor,
              patternMetadata.fixedPattern().data() + subPattern.start,
              currentLength) != 0) {
        return std::make_pair(false, -1);
      }

      cursor += currentLength;
    }
  }

  return std::make_pair(true, cursor);
}

// Match the input(from the position of start) with relaxed pattern forward.
// Returns a pair:
// - first: a bool indicates whether matches the pattern.
// - second: an integer indicates where is cursor in the input when we finished
// matching if 'first' is true, -1 otherwise.
template <bool isAscii>
std::pair<bool, int32_t> matchRelaxedFixedForward(
    StringView input,
    const PatternMetadata& patternMetadata,
    size_t start) {
  if constexpr (isAscii) {
    return matchRelaxedFixedForwardAscii(input, patternMetadata, start);
  } else {
    return matchRelaxedFixedForwardUnicode(input, patternMetadata, start);
  }
}

// Match the input(from the position of start) with relaxed pattern backward.
// Unlike matchRelaxedFixedForward which has different path for utf8 and
// ascii, this function only has implementation for utf8 because only utf8
// input use this function.
bool matchRelaxedFixedBackwardUnicode(
    StringView input,
    const PatternMetadata& patternMetadata,
    size_t start) {
  // Compare the length first.
  if ((start + 1) < patternMetadata.length()) {
    return false;
  }

  const auto& subPatterns = patternMetadata.subPatterns();
  auto cursor = start;
  for (int32_t i = subPatterns.size() - 1; i >= 0; i--) {
    const auto subPattern = subPatterns[i];
    if (subPattern.kind == SubPatternKind::kSingleCharWildcard) {
      int32_t charsToSkip = subPattern.length;
      while (charsToSkip > 0) {
        // We need to skip the number of 'first byte' -- skip one 'first byte'
        // means skip one character.
        if (utf8proc_char_first_byte(input.data() + cursor)) {
          charsToSkip--;
        }
        cursor--;
      }
    } else {
      const auto currentLength = subPattern.length;
      const auto startIdx = cursor - (currentLength - 1);
      if (std::memcmp(
              input.data() + startIdx,
              patternMetadata.fixedPattern().data() + subPattern.start,
              currentLength) != 0) {
        return false;
      }

      cursor -= currentLength;
    }
  }

  return true;
}

// Match the first 'length' characters of string 'input' and prefix pattern.
bool matchPrefixPattern(
    StringView input,
    const std::string& pattern,
    size_t length) {
  return input.size() >= length &&
      std::memcmp(input.data(), pattern.data(), length) == 0;
}

// Match the last 'length' characters of string 'input' and suffix pattern.
bool matchSuffixPattern(
    StringView input,
    const std::string& pattern,
    size_t length) {
  return input.size() >= length &&
      std::memcmp(
          input.data() + input.size() - length,
          pattern.data() + pattern.size() - length,
          length) == 0;
}

bool matchSubstringPattern(
    const StringView& input,
    const std::string& fixedPattern) {
  return (
      std::string_view(input).find(std::string_view(fixedPattern)) !=
      std::string::npos);
}

// Return true if the input VARCHAR argument is all-ASCII for the specified
// rows.
FOLLY_ALWAYS_INLINE static bool isAsciiArg(
    const SelectivityVector& rows,
    const VectorPtr& arg) {
  VELOX_DCHECK(
      arg->type()->isVarchar(), "Input vector is expected to be VARCHAR type.");

  return arg->asUnchecked<SimpleVector<StringView>>()->computeAndSetIsAscii(
      rows);
}

template <PatternKind P>
class OptimizedLike final : public exec::VectorFunction {
 public:
  explicit OptimizedLike(PatternMetadata patternMetadata)
      : patternMetadata_(std::move(patternMetadata)) {}

  template <bool isAscii>
  static bool match(
      const StringView& input,
      const PatternMetadata& patternMetadata) {
    if constexpr (isAscii) {
      switch (P) {
        case PatternKind::kExactlyN:
          return input.size() == patternMetadata.length();
        case PatternKind::kAtLeastN:
          return input.size() >= patternMetadata.length();
        case PatternKind::kFixed:
          return matchExactPattern(
              input, patternMetadata.fixedPattern(), patternMetadata.length());
        case PatternKind::kRelaxedFixed: {
          auto pair = matchRelaxedFixedForward<true>(input, patternMetadata, 0);
          return pair.first && pair.second == input.size();
        }
        case PatternKind::kPrefix:
          return matchPrefixPattern(
              input, patternMetadata.fixedPattern(), patternMetadata.length());
        case PatternKind::kRelaxedPrefix:
          return matchRelaxedFixedForward<true>(input, patternMetadata, 0)
              .first;
        case PatternKind::kSuffix:
          return matchSuffixPattern(
              input, patternMetadata.fixedPattern(), patternMetadata.length());
        case PatternKind::kRelaxedSuffix:
          if (input.size() < patternMetadata.length()) {
            return false;
          }
          return matchRelaxedFixedForward<true>(
                     input,
                     patternMetadata,
                     input.size() - patternMetadata.length())
              .first;
        case PatternKind::kSubstring:
          return matchSubstringPattern(input, patternMetadata.fixedPattern());
      }
    } else {
      switch (P) {
        case PatternKind::kExactlyN:
          return stringImpl::cappedLength<false>(
                     input, patternMetadata.length() + 1) ==
              patternMetadata.length();
        case PatternKind::kAtLeastN:
          return stringImpl::cappedLength<isAscii>(
                     input, patternMetadata.length() + 1) >=
              patternMetadata.length();
        case PatternKind::kFixed:
          return matchExactPattern(
              input, patternMetadata.fixedPattern(), patternMetadata.length());
        case PatternKind::kRelaxedFixed: {
          auto pair =
              matchRelaxedFixedForward<false>(input, patternMetadata, 0);
          return pair.first && pair.second == input.size();
        }
        case PatternKind::kPrefix:
          return matchPrefixPattern(
              input, patternMetadata.fixedPattern(), patternMetadata.length());
        case PatternKind::kRelaxedPrefix: {
          return matchRelaxedFixedForward<false>(input, patternMetadata, 0)
              .first;
        }
        case PatternKind::kSuffix:
          return matchSuffixPattern(
              input, patternMetadata.fixedPattern(), patternMetadata.length());
        case PatternKind::kRelaxedSuffix:
          return matchRelaxedFixedBackwardUnicode(
              input, patternMetadata, input.size() - 1);
        case PatternKind::kSubstring:
          return matchSubstringPattern(input, patternMetadata.fixedPattern());
      }
    }
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& resultRef) const final {
    VELOX_CHECK(args.size() == 2 || args.size() == 3);

    constexpr bool isUtf8SensitivePattern =
        (P == PatternKind::kExactlyN || P == PatternKind::kAtLeastN ||
         P == PatternKind::kRelaxedFixed || P == PatternKind::kRelaxedPrefix ||
         P == PatternKind::kRelaxedSuffix);

    bool needsUtf8Processing =
        isUtf8SensitivePattern && !isAsciiArg(rows, args[0]);
    FlatVector<bool>& result = ensureWritableBool(rows, context, resultRef);
    exec::DecodedArgs decodedArgs(rows, args, context);
    auto toSearch = decodedArgs.at(0);

    if (toSearch->isIdentityMapping()) {
      auto input = toSearch->data<StringView>();
      if (!needsUtf8Processing) {
        context.applyToSelectedNoThrow(rows, [&](vector_size_t i) {
          result.set(i, match</*isAscii*/ true>(input[i], patternMetadata_));
        });
      } else {
        context.applyToSelectedNoThrow(rows, [&](vector_size_t i) {
          result.set(i, match</*isAscii*/ false>(input[i], patternMetadata_));
        });
      }
      return;
    }

    if (toSearch->isConstantMapping()) {
      auto input = toSearch->valueAt<StringView>(0);

      bool matchResult;
      if (!needsUtf8Processing) {
        matchResult = match</*isAscii*/ true>(input, patternMetadata_);
      } else {
        matchResult = match</*isAscii*/ false>(input, patternMetadata_);
      }
      context.applyToSelectedNoThrow(
          rows, [&](vector_size_t i) { result.set(i, matchResult); });
      return;
    }

    // Since the likePattern and escapeChar (2nd and 3rd args) are both
    // constants, so the first arg is expected to be either of flat or constant
    // vector only. This code path is unreachable.
    VELOX_UNREACHABLE();
  }

 private:
  const PatternMetadata patternMetadata_;
};

// This function is used when pattern and escape are constants. And there is not
// fast path that avoids compiling the regular expression.
class LikeWithRe2 final : public exec::VectorFunction {
 public:
  LikeWithRe2(StringView pattern, std::optional<char> escapeChar) {
    RE2::Options opt{RE2::Quiet};
    opt.set_dot_nl(true);
    re_.emplace(
        toStringPiece(likePatternToRe2(pattern, escapeChar, validPattern_)),
        opt);
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& resultRef) const final {
    VELOX_CHECK(args.size() == 2 || args.size() == 3);

    if (!validPattern_) {
      auto error = std::make_exception_ptr(std::invalid_argument(
          "Escape character must be followed by '%', '_' or the escape character itself"));
      context.setErrors(rows, error);
      return;
    }

    // apply() will not be invoked if the selection is empty.
    try {
      checkForBadPattern(*re_);
    } catch (const std::exception& e) {
      context.setErrors(rows, std::current_exception());
      return;
    }

    FlatVector<bool>& result = ensureWritableBool(rows, context, resultRef);

    exec::DecodedArgs decodedArgs(rows, args, context);
    auto toSearch = decodedArgs.at(0);
    if (toSearch->isIdentityMapping()) {
      auto rawStrings = toSearch->data<StringView>();
      context.applyToSelectedNoThrow(rows, [&](vector_size_t i) {
        result.set(i, re2FullMatch(rawStrings[i], *re_));
      });
      return;
    }

    if (toSearch->isConstantMapping()) {
      bool match = re2FullMatch(toSearch->valueAt<StringView>(0), *re_);
      context.applyToSelectedNoThrow(
          rows, [&](vector_size_t i) { result.set(i, match); });
      return;
    }

    // Since the likePattern and escapeChar (2nd and 3rd args) are both
    // constants, so the first arg is expected to be either of flat or constant
    // vector only. This code path is unreachable.
    VELOX_UNREACHABLE();
  }

 private:
  std::optional<RE2> re_;
  bool validPattern_;
};

// This function is constructed when pattern or escape are not constants.
// It allows up to kMaxCompiledRegexes different regular expressions to be
// compiled throughout the query lifetime per expression and thread of
// execution, note that optimized regular expressions that are not compiled are
// not counted.
class LikeGeneric final : public exec::VectorFunction {
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& type,
      exec::EvalCtx& context,
      VectorPtr& result) const final {
    VectorPtr localResult;
    bool isAscii = isAsciiArg(rows, args[0]);
    auto applyWithRegex = [&](const StringView& input,
                              const StringView& pattern,
                              const std::optional<char>& escapeChar) -> bool {
      auto* re = findOrCompileRegex(pattern, escapeChar);
      return re2FullMatch(input, *re);
    };

    auto applyRow = [&](const StringView& input,
                        const StringView& pattern,
                        const std::optional<char>& escapeChar) -> bool {
      PatternMetadata patternMetadata =
          determinePatternKind(std::string_view(pattern), escapeChar);

      if (isAscii) {
        switch (patternMetadata.patternKind()) {
          case PatternKind::kExactlyN:
            return OptimizedLike<PatternKind::kExactlyN>::match<
                /*isAscii*/ true>(input, patternMetadata);
          case PatternKind::kAtLeastN:
            return OptimizedLike<PatternKind::kAtLeastN>::match<
                /*isAscii*/ true>(input, patternMetadata);
          case PatternKind::kFixed:
            return OptimizedLike<PatternKind::kFixed>::match</*isAscii*/ true>(
                input, patternMetadata);
          case PatternKind::kPrefix:
            return OptimizedLike<PatternKind::kPrefix>::match</*isAscii*/ true>(
                input, patternMetadata);
          case PatternKind::kSuffix:
            return OptimizedLike<PatternKind::kSuffix>::match</*isAscii*/ true>(
                input, patternMetadata);
          case PatternKind::kSubstring:
            return OptimizedLike<PatternKind::kSubstring>::match<
                /*isAscii*/ true>(input, patternMetadata);
          default:
            return applyWithRegex(input, pattern, escapeChar);
        }
      } else {
        switch (patternMetadata.patternKind()) {
          case PatternKind::kExactlyN:
            return OptimizedLike<PatternKind::kExactlyN>::match<
                /*isAscii*/ false>(input, patternMetadata);
          case PatternKind::kAtLeastN:
            return OptimizedLike<PatternKind::kAtLeastN>::match<
                /*isAscii*/ false>(input, patternMetadata);
          case PatternKind::kFixed:
            return OptimizedLike<PatternKind::kFixed>::match</*isAscii*/ false>(
                input, patternMetadata);
          case PatternKind::kPrefix:
            return OptimizedLike<PatternKind::kPrefix>::match<
                /*isAscii*/ false>(input, patternMetadata);
          case PatternKind::kSuffix:
            return OptimizedLike<PatternKind::kSuffix>::match<
                /*isAscii*/ false>(input, patternMetadata);
          case PatternKind::kSubstring:
            return OptimizedLike<PatternKind::kSubstring>::match<
                /*isAscii*/ false>(input, patternMetadata);
          default:
            return applyWithRegex(input, pattern, escapeChar);
        }
      }
    };

    context.ensureWritable(rows, type, localResult);
    exec::VectorWriter<bool> vectorWriter;
    vectorWriter.init(*localResult->asFlatVector<bool>());
    exec::DecodedArgs decodedArgs(rows, args, context);

    exec::VectorReader<Varchar> inputReader(decodedArgs.at(0));
    exec::VectorReader<Varchar> patternReader(decodedArgs.at(1));

    if (args.size() == 2) {
      context.applyToSelectedNoThrow(rows, [&](auto row) {
        vectorWriter.setOffset(row);
        vectorWriter.current() =
            applyRow(inputReader[row], patternReader[row], std::nullopt);
        vectorWriter.commit(true);
      });
    } else {
      VELOX_CHECK_EQ(args.size(), 3);
      exec::VectorReader<Varchar> escapeReader(decodedArgs.at(2));
      context.applyToSelectedNoThrow(rows, [&](auto row) {
        vectorWriter.setOffset(row);
        auto escapeChar = escapeReader[row];
        VELOX_USER_CHECK_EQ(
            escapeChar.size(), 1, "Escape string must be a single character");
        vectorWriter.current() = applyRow(
            inputReader[row], patternReader[row], escapeChar.data()[0]);
        vectorWriter.commit(true);
      });
    }

    vectorWriter.finish();
    context.moveOrCopyResult(localResult, rows, result);
  }

 private:
  RE2* findOrCompileRegex(
      const StringView& pattern,
      std::optional<char> escapeChar) const {
    const auto key =
        std::pair<std::string, std::optional<char>>{pattern, escapeChar};

    auto reIt = compiledRegularExpressions_.find(key);
    if (reIt != compiledRegularExpressions_.end()) {
      return reIt->second.get();
    }

    VELOX_USER_CHECK_LT(
        compiledRegularExpressions_.size(),
        kMaxCompiledRegexes,
        "Max number of regex reached");

    bool validEscapeUsage;
    auto regex = likePatternToRe2(pattern, escapeChar, validEscapeUsage);
    VELOX_USER_CHECK(
        validEscapeUsage,
        "Escape character must be followed by '%', '_' or the escape character itself");

    RE2::Options opt{RE2::Quiet};
    opt.set_dot_nl(true);
    auto re = std::make_unique<RE2>(toStringPiece(regex), opt);
    checkForBadPattern(*re);

    auto [it, inserted] =
        compiledRegularExpressions_.emplace(key, std::move(re));
    VELOX_CHECK(inserted);

    return it->second.get();
  }

  mutable folly::F14FastMap<
      std::pair<std::string, std::optional<char>>,
      std::unique_ptr<RE2>>
      compiledRegularExpressions_;
};

void re2ExtractAll(
    exec::VectorWriter<Array<Varchar>>& resultWriter,
    const RE2& re,
    const exec::LocalDecodedVector& inputStrs,
    const int row,
    std::vector<re2::StringPiece>& groups,
    int32_t groupId) {
  resultWriter.setOffset(row);

  auto& arrayWriter = resultWriter.current();

  const StringView str = inputStrs->valueAt<StringView>(row);
  const re2::StringPiece input = toStringPiece(str);
  size_t pos = 0;

  while (re.Match(
      input, pos, input.size(), RE2::UNANCHORED, groups.data(), groupId + 1)) {
    DCHECK_GT(groups.size(), groupId);

    const re2::StringPiece fullMatch = groups[0];
    const re2::StringPiece subMatch = groups[groupId];

    arrayWriter.add_item().setNoCopy(
        StringView(subMatch.data(), subMatch.size()));
    pos = fullMatch.data() + fullMatch.size() - input.data();
    if (UNLIKELY(fullMatch.size() == 0)) {
      ++pos;
    }
  }

  resultWriter.commit();
}

template <typename T>
class Re2ExtractAllConstantPattern final : public exec::VectorFunction {
 public:
  explicit Re2ExtractAllConstantPattern(StringView pattern)
      : re_(toStringPiece(pattern), RE2::Quiet) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& resultRef) const final {
    VELOX_CHECK(args.size() == 2 || args.size() == 3);
    try {
      checkForBadPattern(re_);
    } catch (const std::exception& e) {
      context.setErrors(rows, std::current_exception());
      return;
    }

    BaseVector::ensureWritable(
        rows, ARRAY(VARCHAR()), context.pool(), resultRef);
    exec::VectorWriter<Array<Varchar>> resultWriter;
    resultWriter.init(*resultRef->as<ArrayVector>());

    exec::LocalDecodedVector inputStrs(context, *args[0], rows);
    FOLLY_DECLARE_REUSED(groups, std::vector<re2::StringPiece>);

    if (args.size() == 2) {
      // Case 1: No groupId -- use 0 as the default groupId
      //
      groups.resize(1);
      context.applyToSelectedNoThrow(rows, [&](vector_size_t row) {
        re2ExtractAll(resultWriter, re_, inputStrs, row, groups, 0);
      });
    } else if (const auto _groupId = getIfConstant<T>(*args[2])) {
      // Case 2: Constant groupId
      //
      try {
        checkForBadGroupId(*_groupId, re_);
      } catch (const std::exception& e) {
        context.setErrors(rows, std::current_exception());
        return;
      }

      groups.resize(*_groupId + 1);
      context.applyToSelectedNoThrow(rows, [&](vector_size_t row) {
        re2ExtractAll(resultWriter, re_, inputStrs, row, groups, *_groupId);
      });
    } else {
      // Case 3: Variable groupId, so resize the groups vector to accommodate
      // number of capturing groups + 1.
      exec::LocalDecodedVector groupIds(context, *args[2], rows);

      groups.resize(re_.NumberOfCapturingGroups() + 1);
      context.applyToSelectedNoThrow(rows, [&](vector_size_t row) {
        const T groupId = groupIds->valueAt<T>(row);
        checkForBadGroupId(groupId, re_);
        re2ExtractAll(resultWriter, re_, inputStrs, row, groups, groupId);
      });
    }

    resultWriter.finish();

    resultRef->as<ArrayVector>()
        ->elements()
        ->asFlatVector<StringView>()
        ->acquireSharedStringBuffers(inputStrs->base());
  }

 private:
  RE2 re_;
};

template <typename T>
class Re2ExtractAll final : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& resultRef) const final {
    VELOX_CHECK(args.size() == 2 || args.size() == 3);
    // Use Re2ExtractAllConstantPattern if it's constant regexp pattern.
    //
    if (auto pattern = getIfConstant<StringView>(*args[1])) {
      Re2ExtractAllConstantPattern<T>(*pattern).apply(
          rows, args, outputType, context, resultRef);
      return;
    }

    BaseVector::ensureWritable(
        rows, ARRAY(VARCHAR()), context.pool(), resultRef);
    exec::VectorWriter<Array<Varchar>> resultWriter;
    resultWriter.init(*resultRef->as<ArrayVector>());

    exec::LocalDecodedVector inputStrs(context, *args[0], rows);
    exec::LocalDecodedVector pattern(context, *args[1], rows);
    FOLLY_DECLARE_REUSED(groups, std::vector<re2::StringPiece>);

    if (args.size() == 2) {
      // Case 1: No groupId -- use 0 as the default groupId
      //
      groups.resize(1);
      context.applyToSelectedNoThrow(rows, [&](vector_size_t row) {
        auto& re = *cache_.findOrCompile(pattern->valueAt<StringView>(row));
        re2ExtractAll(resultWriter, re, inputStrs, row, groups, 0);
      });
    } else {
      // Case 2: Has groupId
      //
      exec::LocalDecodedVector groupIds(context, *args[2], rows);
      context.applyToSelectedNoThrow(rows, [&](vector_size_t row) {
        const T groupId = groupIds->valueAt<T>(row);
        auto& re = *cache_.findOrCompile(pattern->valueAt<StringView>(row));
        checkForBadGroupId(groupId, re);
        groups.resize(groupId + 1);
        re2ExtractAll(resultWriter, re, inputStrs, row, groups, groupId);
      });
    }

    resultWriter.finish();
    resultRef->as<ArrayVector>()
        ->elements()
        ->asFlatVector<StringView>()
        ->acquireSharedStringBuffers(inputStrs->base());
  }

 private:
  mutable ReCache cache_;
};

template <bool (*Fn)(StringView, const RE2&)>
std::shared_ptr<exec::VectorFunction> makeRe2MatchImpl(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  if (inputArgs.size() != 2 || !inputArgs[0].type->isVarchar() ||
      !inputArgs[1].type->isVarchar()) {
    VELOX_UNSUPPORTED(
        "{} expected (VARCHAR, VARCHAR) but got ({})",
        name,
        printTypesCsv(inputArgs));
  }

  BaseVector* constantPattern = inputArgs[1].constantValue.get();

  if (constantPattern != nullptr && !constantPattern->isNullAt(0)) {
    return std::make_shared<Re2MatchConstantPattern<Fn>>(
        constantPattern->as<ConstantVector<StringView>>()->valueAt(0));
  }

  return std::make_shared<Re2Match<Fn>>();
}

} // namespace

std::shared_ptr<exec::VectorFunction> makeRe2Match(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/) {
  return makeRe2MatchImpl<re2FullMatch>(name, inputArgs);
}

std::vector<std::shared_ptr<exec::FunctionSignature>> re2MatchSignatures() {
  // varchar, varchar -> boolean
  return {exec::FunctionSignatureBuilder()
              .returnType("boolean")
              .argumentType("varchar")
              .argumentType("varchar")
              .build()};
}

std::shared_ptr<exec::VectorFunction> makeRe2Search(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/) {
  return makeRe2MatchImpl<re2PartialMatch>(name, inputArgs);
}

std::vector<std::shared_ptr<exec::FunctionSignature>> re2SearchSignatures() {
  // varchar, varchar -> boolean
  return {exec::FunctionSignatureBuilder()
              .returnType("boolean")
              .argumentType("varchar")
              .argumentType("varchar")
              .build()};
}

std::shared_ptr<exec::VectorFunction> makeRe2Extract(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/,
    const bool emptyNoMatch) {
  auto numArgs = inputArgs.size();
  VELOX_USER_CHECK(
      numArgs == 2 || numArgs == 3,
      "{} requires 2 or 3 arguments, but got {}",
      name,
      numArgs);

  VELOX_USER_CHECK(
      inputArgs[0].type->isVarchar(),
      "{} requires first argument of type VARCHAR, but got {}",
      name,
      inputArgs[0].type->toString());

  VELOX_USER_CHECK(
      inputArgs[1].type->isVarchar(),
      "{} requires second argument of type VARCHAR, but got {}",
      name,
      inputArgs[1].type->toString());

  TypeKind groupIdTypeKind = TypeKind::INTEGER;
  if (numArgs == 3) {
    groupIdTypeKind = inputArgs[2].type->kind();
    VELOX_USER_CHECK(
        groupIdTypeKind == TypeKind::INTEGER ||
            groupIdTypeKind == TypeKind::BIGINT,
        "{} requires third argument of type INTEGER or BIGINT, but got {}",
        name,
        mapTypeKindToName(groupIdTypeKind));
  }

  BaseVector* constantPattern = inputArgs[1].constantValue.get();

  if (constantPattern != nullptr && !constantPattern->isNullAt(0)) {
    auto pattern =
        constantPattern->as<ConstantVector<StringView>>()->valueAt(0);
    switch (groupIdTypeKind) {
      case TypeKind::INTEGER:
        return std::make_shared<Re2SearchAndExtractConstantPattern<int32_t>>(
            pattern, emptyNoMatch);
      case TypeKind::BIGINT:
        return std::make_shared<Re2SearchAndExtractConstantPattern<int64_t>>(
            pattern, emptyNoMatch);
      default:
        VELOX_UNREACHABLE();
    }
  }

  switch (groupIdTypeKind) {
    case TypeKind::INTEGER:
      return std::make_shared<Re2SearchAndExtract<int32_t>>(emptyNoMatch);
    case TypeKind::BIGINT:
      return std::make_shared<Re2SearchAndExtract<int64_t>>(emptyNoMatch);
    default:
      VELOX_UNREACHABLE();
  }
}

std::vector<std::shared_ptr<exec::FunctionSignature>> re2ExtractSignatures() {
  // varchar, varchar -> boolean
  // varchar, varchar, integer|bigint -> boolean
  return {
      exec::FunctionSignatureBuilder()
          .returnType("varchar")
          .argumentType("varchar")
          .argumentType("varchar")
          .build(),
      exec::FunctionSignatureBuilder()
          .returnType("varchar")
          .argumentType("varchar")
          .argumentType("varchar")
          .argumentType("bigint")
          .build(),
      exec::FunctionSignatureBuilder()
          .returnType("varchar")
          .argumentType("varchar")
          .argumentType("varchar")
          .argumentType("integer")
          .build(),
  };
}

PatternMetadata PatternMetadata::generic() {
  return {PatternKind::kGeneric, 0, "", {}};
}

PatternMetadata PatternMetadata::atLeastN(size_t length) {
  return {PatternKind::kAtLeastN, length, "", {}};
}

PatternMetadata PatternMetadata::exactlyN(size_t length) {
  return {PatternKind::kExactlyN, length, "", {}};
}

PatternMetadata PatternMetadata::fixed(const std::string& fixedPattern) {
  return {PatternKind::kFixed, fixedPattern.length(), fixedPattern, {}};
}

PatternMetadata PatternMetadata::relaxedFixed(
    std::string fixedPattern,
    std::vector<SubPatternMetadata> subPatterns) {
  const auto fixedLength = fixedPattern.length();
  return {
      PatternKind::kRelaxedFixed,
      fixedLength,
      std::move(fixedPattern),
      std::move(subPatterns)};
}

PatternMetadata PatternMetadata::prefix(const std::string& fixedPattern) {
  return {PatternKind::kPrefix, fixedPattern.length(), fixedPattern, {}};
}

PatternMetadata PatternMetadata::relaxedPrefix(
    std::string fixedPattern,
    std::vector<SubPatternMetadata> subPatterns) {
  const auto fixedLength = fixedPattern.length();
  return {
      PatternKind::kRelaxedPrefix,
      fixedLength,
      std::move(fixedPattern),
      std::move(subPatterns)};
}

PatternMetadata PatternMetadata::suffix(const std::string& fixedPattern) {
  return {PatternKind::kSuffix, fixedPattern.length(), fixedPattern, {}};
}

PatternMetadata PatternMetadata::relaxedSuffix(
    std::string fixedPattern,
    std::vector<SubPatternMetadata> subPatterns) {
  const auto fixedLength = fixedPattern.length();
  return {
      PatternKind::kRelaxedSuffix,
      fixedLength,
      std::move(fixedPattern),
      std::move(subPatterns)};
}

PatternMetadata PatternMetadata::substring(const std::string& fixedPattern) {
  return {PatternKind::kSubstring, fixedPattern.length(), fixedPattern, {}};
}

PatternMetadata::PatternMetadata(
    PatternKind patternKind,
    size_t length,
    std::string fixedPattern,
    std::vector<SubPatternMetadata> subPatterns)
    : patternKind_{patternKind},
      length_{length},
      fixedPattern_(std::move(fixedPattern)),
      subPatterns_(std::move(subPatterns)) {}

// Iterates through a pattern string. Transparently handles escape sequences.
class PatternStringIterator {
 public:
  PatternStringIterator(
      std::string_view pattern,
      std::optional<char> escapeChar)
      : pattern_(pattern), escapeChar_(escapeChar) {}

  // Advance the cursor to next char, escape char is automatically handled.
  // Return true if the cursor is advanced successfully, false otherwise(reached
  // the end of the pattern string).
  bool next() {
    if (nextStart_ == pattern_.size()) {
      return false;
    }

    currentStart_ = nextStart_;
    auto currentChar = charAt(currentStart_);
    if (currentChar == escapeChar_) {
      // Escape char should be followed by another char.
      VELOX_USER_CHECK_LT(
          currentStart_ + 1,
          pattern_.size(),
          "Escape character must be followed by '%', '_' or the escape character itself: {}, escape {}",
          pattern_,
          escapeChar_.value())

      currentChar = charAt(currentStart_ + 1);
      // The char follows escapeChar can only be one of (%, _, escapeChar).
      if (currentChar == escapeChar_ || currentChar == '_' ||
          currentChar == '%') {
        charKind_ = CharKind::kNormal;
      } else {
        VELOX_USER_FAIL(
            "Escape character must be followed by '%', '_' or the escape character itself: {}, escape {}",
            pattern_,
            escapeChar_.value())
      }
      // One escape char plus the current char.
      nextStart_ = currentStart_ + 2;
    } else {
      if (currentChar == '_') {
        charKind_ = CharKind::kSingleCharWildcard;
        nextStart_ = currentStart_ + 1;
      } else if (currentChar == '%') {
        charKind_ = CharKind::kAnyCharsWildcard;
        nextStart_ = currentStart_ + 1;
      } else {
        charKind_ = CharKind::kNormal;

        // Unicode.
        if (currentChar & 0x80) {
          auto numBytes = unicodeCharLength(pattern_.data() + currentStart_);
          nextStart_ = currentStart_ + numBytes;
        } else {
          nextStart_ = currentStart_ + 1;
        }
      }
    }

    return true;
  }

  // Char at current cursor, since it can be a multibyte character, here we use
  // a string_view to represent.
  std::string_view current() const {
    // Escaped.
    if (charAt(currentStart_) == escapeChar_) {
      return std::string_view(pattern_.data() + currentStart_ + 1, 1);
    } else {
      return std::string_view(
          pattern_.data() + currentStart_, nextStart_ - currentStart_);
    }
  }

  bool isAnyCharsWildcard() const {
    return charKind_ == CharKind::kAnyCharsWildcard;
  }

  bool isSingleCharWildcard() const {
    return charKind_ == CharKind::kSingleCharWildcard;
  }

 private:
  // Represents the state of current cursor/char.
  enum class CharKind {
    // Wildcard char: %.
    // NOTE: If escape char is set as '\', for pattern '\%%', the first '%' is
    // not a wildcard, just a literal '%', the second '%' is a wildcard.
    kAnyCharsWildcard,
    // Wildcard char: _.
    // NOTE: If escape char is set as '\', for pattern '\__', the first '_' is
    // not a wildcard, just a literal '_', the second '_' is a wildcard.
    kSingleCharWildcard,
    // Chars that are not escape char & not wildcard char.
    kNormal
  };

  // Char at current cursor.
  char charAt(size_t index) const {
    VELOX_DCHECK(index < pattern_.size())
    return pattern_.data()[index];
  }

  std::string_view pattern_;
  const std::optional<char> escapeChar_;

  // Index of current char(including the escape char if there is).
  size_t currentStart_{0};
  // Index of next char.
  size_t nextStart_{0};
  CharKind charKind_{CharKind::kNormal};
};

// Is the specified sub-patterns an optimization candidate?
// Return true if it *might* be optimized, return false if it is not
// optimize-able.
bool isOptimizedLikeCandidate(
    const std::array<SubPatternStats, 3>& stats,
    int numSubPatterns,
    SubPatternKind firstPatternKind,
    SubPatternKind lastPatternKind) {
  if (stats[kLiteralString].count == 0) {
    return true;
  }

  // More than 2 '%' , no fast path for it.
  if (stats[kAnyCharsWildcard].count > 2) {
    return false;
  }

  // Only 2 '%', but the '%' is not at the beginning and end of the pattern, no
  // fast path for it.
  if (stats[kAnyCharsWildcard].count == 2 &&
      (firstPatternKind != SubPatternKind::kAnyCharsWildcard ||
       lastPatternKind != SubPatternKind::kAnyCharsWildcard)) {
    return false;
  }

  // Only one '%', but it is not the first/last pattern, no fast path for it.
  if (stats[kAnyCharsWildcard].count == 1 &&
      (stats[kAnyCharsWildcard].firstIndex > 0 &&
       stats[kAnyCharsWildcard].firstIndex < numSubPatterns - 1)) {
    return false;
  }

  return true;
}

// Parse the pattern into sub-patterns.
std::optional<std::string> parsePattern(
    std::string_view pattern,
    std::optional<char> escapeChar,
    std::vector<SubPatternKind>& subPatternKinds,
    std::vector<std::pair<size_t, size_t>>& subPatternRanges) {
  PatternStringIterator iterator{pattern, escapeChar};

  // Iterate through the pattern string to collect the stats for the simple
  // patterns that we can optimize.
  std::ostringstream os;
  std::optional<SubPatternKind> previousKind;
  size_t currentSubPatternStart = 0;
  size_t cursor = 0;

  while (iterator.next()) {
    SubPatternKind currentKind;
    if (iterator.isSingleCharWildcard()) {
      currentKind = SubPatternKind::kSingleCharWildcard;
    } else if (iterator.isAnyCharsWildcard()) {
      currentKind = SubPatternKind::kAnyCharsWildcard;
    } else {
      currentKind = SubPatternKind::kLiteralString;
    }

    // Set the 'previousKind' to currentKind if it has not been set yet, which
    // only occur once(the first char) during parsing.
    if (FOLLY_UNLIKELY(!previousKind.has_value())) {
      previousKind = currentKind;
    }

    // New sub pattern occurs.
    if (currentKind != previousKind) {
      subPatternKinds.push_back(previousKind.value());
      subPatternRanges.push_back(
          {currentSubPatternStart, cursor - currentSubPatternStart});
      currentSubPatternStart = cursor;
      previousKind = currentKind;
    }

    // Advance the cursor.
    std::string_view currentChar = iterator.current();
    cursor += currentChar.size();

    // We only need to collect the unescaped chars if user specified escape
    // char.
    if (escapeChar.has_value()) {
      os << iterator.current();
    }
  }

  // Handle the last sub-pattern.
  subPatternKinds.push_back(previousKind.value());
  subPatternRanges.push_back(
      {currentSubPatternStart, cursor - currentSubPatternStart});

  return escapeChar.has_value() ? std::make_optional(os.str()) : std::nullopt;
}

PatternMetadata determinePatternKind(
    std::string_view pattern,
    std::optional<char> escapeChar) {
  if (FOLLY_UNLIKELY(pattern.empty())) {
    return PatternMetadata::fixed("");
  }

  // Parse the pattern into sub-patterns.
  std::vector<SubPatternKind> subPatternKinds;
  std::vector<std::pair<size_t, size_t>> subPatternRanges;

  std::optional<std::string> parsedPattern =
      parsePattern(pattern, escapeChar, subPatternKinds, subPatternRanges);
  std::string_view unescapedPattern =
      escapeChar.has_value() ? parsedPattern.value() : pattern;

  const auto numSubPatterns = subPatternKinds.size();
  std::array<SubPatternStats, 3> stats = {
      SubPatternStats{kSingleCharWildcard},
      SubPatternStats{kAnyCharsWildcard},
      SubPatternStats{kLiteralString}};

  // Collect the sub-pattern stats.
  for (auto i = 0; i < numSubPatterns; i++) {
    stats[subPatternKinds[i]].update(i);
  }

  // Determine optimized pattern base on stats we have.
  const auto firstSubPatternKind = subPatternKinds[0];
  const auto firstSubPatternLength = subPatternRanges[0].second;

  const auto lastSubPatternKind = subPatternKinds[numSubPatterns - 1];
  const auto lastSubPatternStart = subPatternRanges[numSubPatterns - 1].first;
  const auto lastSubPatternLength = subPatternRanges[numSubPatterns - 1].second;

  // Fail fast if we have no fast path for it.
  if (!isOptimizedLikeCandidate(
          stats, numSubPatterns, firstSubPatternKind, lastSubPatternKind)) {
    return PatternMetadata::generic();
  }

  // Single sub-pattern.
  if (numSubPatterns == 1) {
    if (firstSubPatternKind == SubPatternKind::kSingleCharWildcard) {
      return PatternMetadata::exactlyN(firstSubPatternLength);
    } else if (firstSubPatternKind == SubPatternKind::kAnyCharsWildcard) {
      return PatternMetadata::atLeastN(0);
    }

    return PatternMetadata::fixed(std::string(unescapedPattern));
  } else { // Multiple sub-patterns.
    // No kLiteralString sub-pattern.
    if (stats[kLiteralString].count == 0) {
      const auto singleCharacterWildcardCount =
          fixedLength(subPatternKinds, subPatternRanges);
      return PatternMetadata::atLeastN(singleCharacterWildcardCount);
    } else {
      // At this point, the pattern contains at least one kLiteralString
      // sub-pattern.

      // If there are only one literal sub-pattern and several any-wildcard
      // sub-patterns.
      if (stats[kSingleCharWildcard].count == 0 &&
          stats[kLiteralString].count == 1 &&
          stats[kAnyCharsWildcard].count > 0) {
        if (firstSubPatternKind == SubPatternKind::kLiteralString) {
          return PatternMetadata::prefix(
              std::string(unescapedPattern, 0, firstSubPatternLength));
        } else if (lastSubPatternKind == SubPatternKind::kLiteralString) {
          return PatternMetadata::suffix(std::string(
              unescapedPattern, lastSubPatternStart, lastSubPatternLength));
        } else if (
            numSubPatterns == 3 &&
            firstSubPatternKind == SubPatternKind::kAnyCharsWildcard &&
            lastSubPatternKind == SubPatternKind::kAnyCharsWildcard) {
          return PatternMetadata::substring(std::string(
              unescapedPattern,
              subPatternRanges[1].first,
              subPatternRanges[1].second));
        }
      }

      // No any-wildcard sub-pattern.
      if (stats[kAnyCharsWildcard].count == 0 &&
          stats[kSingleCharWildcard].count > 0) {
        std::vector<SubPatternMetadata> subPatterns;
        buildFixedSubPatterns(
            subPatternKinds, subPatternRanges, 0, numSubPatterns, subPatterns);
        return PatternMetadata::relaxedFixed(
            std::string(unescapedPattern), std::move(subPatterns));
      }

      // Pattern contains kAnyCharsWildcard, kSingleCharWildcard &
      // kLiteralString.
      if (stats[kSingleCharWildcard].count > 0 &&
          stats[kAnyCharsWildcard].count > 0) {
        const auto firstOfLiteralOrSingleWildcard = std::min(
            stats[kLiteralString].firstIndex.value(),
            stats[kSingleCharWildcard].firstIndex.value());
        const auto lastOfLiteralOrSingleWildcard = std::max(
            stats[kLiteralString].lastIndex.value(),
            stats[kSingleCharWildcard].lastIndex.value());

        if (lastOfLiteralOrSingleWildcard <
            stats[kAnyCharsWildcard].firstIndex) {
          std::vector<SubPatternMetadata> subPatterns;
          size_t fixedLength = buildFixedSubPatterns(
              subPatternKinds,
              subPatternRanges,
              firstOfLiteralOrSingleWildcard,
              lastOfLiteralOrSingleWildcard + 1,
              subPatterns);
          return PatternMetadata::relaxedPrefix(
              std::string(
                  unescapedPattern,
                  subPatternRanges[firstOfLiteralOrSingleWildcard].first,
                  fixedLength),
              std::move(subPatterns));
        } else if (
            firstOfLiteralOrSingleWildcard >
            stats[kAnyCharsWildcard].lastIndex) {
          std::vector<SubPatternMetadata> subPatterns;
          const auto fixedLength = buildFixedSubPatterns(
              subPatternKinds,
              subPatternRanges,
              firstOfLiteralOrSingleWildcard,
              lastOfLiteralOrSingleWildcard + 1,
              subPatterns);
          return PatternMetadata::relaxedSuffix(
              std::string(
                  unescapedPattern,
                  subPatternRanges[firstOfLiteralOrSingleWildcard].first,
                  fixedLength),
              std::move(subPatterns));
        }
      }
    }
  }

  return PatternMetadata::generic();
}

std::shared_ptr<exec::VectorFunction> makeLike(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/) {
  auto numArgs = inputArgs.size();

  std::optional<char> escapeChar;
  if (numArgs == 3) {
    BaseVector* escape = inputArgs[2].constantValue.get();
    if (!escape) {
      return std::make_shared<LikeGeneric>();
    }

    auto constantEscape = escape->as<ConstantVector<StringView>>();
    if (constantEscape->isNullAt(0)) {
      return std::make_shared<exec::ApplyNeverCalled>();
    }

    // TODO(xumingming) Presto actually support multi-byte escape char(see [1]),
    // we should support too.
    //
    // [1].https://github.com/facebookincubator/velox/issues/8363
    try {
      VELOX_USER_CHECK_EQ(
          constantEscape->valueAt(0).size(),
          1,
          "Escape string must be a single character");
    } catch (...) {
      return std::make_shared<exec::AlwaysFailingVectorFunction>(
          std::current_exception());
    }
    escapeChar = constantEscape->valueAt(0).data()[0];
  }

  BaseVector* constantPattern = inputArgs[1].constantValue.get();
  if (!constantPattern) {
    return std::make_shared<LikeGeneric>();
  }

  if (constantPattern->isNullAt(0)) {
    return std::make_shared<exec::ApplyNeverCalled>();
  }
  auto pattern = constantPattern->as<ConstantVector<StringView>>()->valueAt(0);

  PatternMetadata patternMetadata = PatternMetadata::generic();
  try {
    patternMetadata =
        determinePatternKind(std::string_view(pattern), escapeChar);
  } catch (...) {
    return std::make_shared<exec::AlwaysFailingVectorFunction>(
        std::current_exception());
  }

  switch (patternMetadata.patternKind()) {
    case PatternKind::kExactlyN:
      return std::make_shared<OptimizedLike<PatternKind::kExactlyN>>(
          patternMetadata);
    case PatternKind::kAtLeastN:
      return std::make_shared<OptimizedLike<PatternKind::kAtLeastN>>(
          patternMetadata);
    case PatternKind::kFixed:
      return std::make_shared<OptimizedLike<PatternKind::kFixed>>(
          patternMetadata);
    case PatternKind::kRelaxedFixed:
      return std::make_shared<OptimizedLike<PatternKind::kRelaxedFixed>>(
          patternMetadata);
    case PatternKind::kPrefix:
      return std::make_shared<OptimizedLike<PatternKind::kPrefix>>(
          patternMetadata);
    case PatternKind::kRelaxedPrefix:
      return std::make_shared<OptimizedLike<PatternKind::kRelaxedPrefix>>(
          patternMetadata);
    case PatternKind::kSuffix:
      return std::make_shared<OptimizedLike<PatternKind::kSuffix>>(
          patternMetadata);
    case PatternKind::kRelaxedSuffix:
      return std::make_shared<OptimizedLike<PatternKind::kRelaxedSuffix>>(
          patternMetadata);
    case PatternKind::kSubstring:
      return std::make_shared<OptimizedLike<PatternKind::kSubstring>>(
          patternMetadata);
    default:
      return std::make_shared<LikeWithRe2>(pattern, escapeChar);
  }
}

std::vector<std::shared_ptr<exec::FunctionSignature>> likeSignatures() {
  // varchar, varchar -> boolean
  // varchar, varchar, varchar -> boolean
  return {
      exec::FunctionSignatureBuilder()
          .returnType("boolean")
          .argumentType("varchar")
          .constantArgumentType("varchar")
          .build(),
      exec::FunctionSignatureBuilder()
          .returnType("boolean")
          .argumentType("varchar")
          .constantArgumentType("varchar")
          .constantArgumentType("varchar")
          .build(),
  };
}

std::shared_ptr<exec::VectorFunction> makeRe2ExtractAll(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/) {
  auto numArgs = inputArgs.size();
  VELOX_USER_CHECK(
      numArgs == 2 || numArgs == 3,
      "{} requires 2 or 3 arguments, but got {}",
      name,
      numArgs);

  VELOX_USER_CHECK(
      inputArgs[0].type->isVarchar(),
      "{} requires first argument of type VARCHAR, but got {}",
      name,
      inputArgs[0].type->toString());

  VELOX_USER_CHECK(
      inputArgs[1].type->isVarchar(),
      "{} requires second argument of type VARCHAR, but got {}",
      name,
      inputArgs[1].type->toString());

  TypeKind groupIdTypeKind = TypeKind::INTEGER;
  if (numArgs == 3) {
    groupIdTypeKind = inputArgs[2].type->kind();
    VELOX_USER_CHECK(
        groupIdTypeKind == TypeKind::INTEGER ||
            groupIdTypeKind == TypeKind::BIGINT,
        "{} requires third argument of type INTEGER or BIGINT, but got {}",
        name,
        mapTypeKindToName(groupIdTypeKind));
  }

  BaseVector* constantPattern = inputArgs[1].constantValue.get();
  if (constantPattern != nullptr && !constantPattern->isNullAt(0)) {
    auto pattern =
        constantPattern->as<ConstantVector<StringView>>()->valueAt(0);
    switch (groupIdTypeKind) {
      case TypeKind::INTEGER:
        return std::make_shared<Re2ExtractAllConstantPattern<int32_t>>(pattern);
      case TypeKind::BIGINT:
        return std::make_shared<Re2ExtractAllConstantPattern<int64_t>>(pattern);
      default:
        VELOX_UNREACHABLE();
    }
  }

  switch (groupIdTypeKind) {
    case TypeKind::INTEGER:
      return std::make_shared<Re2ExtractAll<int32_t>>();
    case TypeKind::BIGINT:
      return std::make_shared<Re2ExtractAll<int64_t>>();
    default:
      VELOX_UNREACHABLE();
  }
}

std::vector<std::shared_ptr<exec::FunctionSignature>>
re2ExtractAllSignatures() {
  // varchar, varchar -> array<varchar>
  // varchar, varchar, integer|bigint -> array<varchar>
  return {
      exec::FunctionSignatureBuilder()
          .returnType("array(varchar)")
          .argumentType("varchar")
          .argumentType("varchar")
          .build(),
      exec::FunctionSignatureBuilder()
          .returnType("array(varchar)")
          .argumentType("varchar")
          .argumentType("varchar")
          .argumentType("bigint")
          .build(),
      exec::FunctionSignatureBuilder()
          .returnType("array(varchar)")
          .argumentType("varchar")
          .argumentType("varchar")
          .argumentType("integer")
          .build(),
  };
}

} // namespace facebook::velox::functions

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

#include <re2/re2.h>
#include <memory>
#include <optional>
#include <string>

namespace facebook::velox::functions {
namespace {

using ::facebook::velox::exec::EvalCtx;
using ::facebook::velox::exec::Expr;
using ::facebook::velox::exec::VectorFunction;
using ::facebook::velox::exec::VectorFunctionArg;
using ::re2::RE2;

static const int kMaxCompiledRegexes = 20;

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

template <typename T>
re2::StringPiece toStringPiece(const T& s) {
  return re2::StringPiece(s.data(), s.size());
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

void checkForBadPattern(const RE2& re) {
  if (UNLIKELY(!re.ok())) {
    VELOX_USER_FAIL("invalid regular expression:{}", re.error());
  }
}

FlatVector<bool>& ensureWritableBool(
    const SelectivityVector& rows,
    EvalCtx& context,
    VectorPtr& result) {
  context.ensureWritable(rows, BOOLEAN(), result);
  return *result->as<FlatVector<bool>>();
}

FlatVector<StringView>& ensureWritableStringView(
    const SelectivityVector& rows,
    EvalCtx& context,
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
class Re2MatchConstantPattern final : public VectorFunction {
 public:
  explicit Re2MatchConstantPattern(StringView pattern)
      : re_(toStringPiece(pattern), RE2::Quiet) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      EvalCtx& context,
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
class Re2Match final : public VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      EvalCtx& context,
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
      RE2 re(toStringPiece(pattern->valueAt<StringView>(row)), RE2::Quiet);
      checkForBadPattern(re);
      result.set(row, Fn(toSearch->valueAt<StringView>(row), re));
    });
  }
};

void checkForBadGroupId(int64_t groupId, const RE2& re) {
  if (UNLIKELY(groupId < 0 || groupId > re.NumberOfCapturingGroups())) {
    VELOX_USER_FAIL("No group {} in regex '{}'", groupId, re.pattern());
  }
}

template <typename T>
class Re2SearchAndExtractConstantPattern final : public VectorFunction {
 public:
  explicit Re2SearchAndExtractConstantPattern(
      StringView pattern,
      bool emptyNoMatch)
      : re_(toStringPiece(pattern), RE2::Quiet), emptyNoMatch_(emptyNoMatch) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      EvalCtx& context,
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
}; // namespace

// The factory function we provide returns a unique instance for each call, so
// this is safe.
template <typename T>
class Re2SearchAndExtract final : public VectorFunction {
 public:
  explicit Re2SearchAndExtract(bool emptyNoMatch)
      : emptyNoMatch_(emptyNoMatch) {}
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      EvalCtx& context,
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
        RE2 re(toStringPiece(pattern->valueAt<StringView>(i)), RE2::Quiet);
        checkForBadPattern(re);
        mustRefSourceStrings |=
            re2Extract(result, i, re, toSearch, groups, 0, emptyNoMatch_);
      });
    } else {
      exec::LocalDecodedVector groupIds(context, *args[2], rows);
      context.applyToSelectedNoThrow(rows, [&](vector_size_t i) {
        const auto groupId = groupIds->valueAt<T>(i);
        RE2 re(toStringPiece(pattern->valueAt<StringView>(i)), RE2::Quiet);
        checkForBadPattern(re);
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
};

// Match string 'input' with a fixed pattern (with no wildcard characters).
bool matchExactPattern(
    StringView input,
    const std::string& pattern,
    size_t length) {
  return input.size() == pattern.size() &&
      std::memcmp(input.data(), pattern.data(), length) == 0;
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

template <PatternKind P>
class OptimizedLike final : public VectorFunction {
 public:
  OptimizedLike(std::string pattern, size_t reducedPatternLength)
      : pattern_{std::move(pattern)},
        reducedPatternLength_{reducedPatternLength} {}

  static bool match(
      const StringView& input,
      const std::string& pattern,
      size_t reducedPatternLength) {
    switch (P) {
      case PatternKind::kExactlyN:
        return input.size() == reducedPatternLength;
      case PatternKind::kAtLeastN:
        return input.size() >= reducedPatternLength;
      case PatternKind::kFixed:
        return matchExactPattern(input, pattern, reducedPatternLength);
      case PatternKind::kPrefix:
        return matchPrefixPattern(input, pattern, reducedPatternLength);
      case PatternKind::kSuffix:
        return matchSuffixPattern(input, pattern, reducedPatternLength);
      case PatternKind::kSubstring:
        return matchSubstringPattern(input, pattern);
    }
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      EvalCtx& context,
      VectorPtr& resultRef) const final {
    VELOX_CHECK(args.size() == 2 || args.size() == 3);
    FlatVector<bool>& result = ensureWritableBool(rows, context, resultRef);
    exec::DecodedArgs decodedArgs(rows, args, context);
    auto toSearch = decodedArgs.at(0);

    if (toSearch->isIdentityMapping()) {
      auto input = toSearch->data<StringView>();
      context.applyToSelectedNoThrow(rows, [&](vector_size_t i) {
        result.set(i, match(input[i], pattern_, reducedPatternLength_));
      });
      return;
    }
    if (toSearch->isConstantMapping()) {
      auto input = toSearch->valueAt<StringView>(0);
      bool matchResult = match(input, pattern_, reducedPatternLength_);
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
  const std::string pattern_;
  const size_t reducedPatternLength_;
};

// This function is used when pattern and escape are constants. And there is not
// fast path that avoids compiling the regular expression.
class LikeWithRe2 final : public VectorFunction {
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
      EvalCtx& context,
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
// compiled throughout the query life per function, note that optimized regular
// expressions that are not compiled are not counted.
class LikeGeneric final : public VectorFunction {
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& type,
      EvalCtx& context,
      VectorPtr& result) const final {
    VectorPtr localResult;

    auto applyWithRegex = [&](const StringView& input,
                              const StringView& pattern,
                              const std::optional<char>& escapeChar) -> bool {
      RE2::Options opt{RE2::Quiet};
      opt.set_dot_nl(true);
      bool validEscapeUsage;
      auto regex = likePatternToRe2(pattern, escapeChar, validEscapeUsage);
      VELOX_USER_CHECK(
          validEscapeUsage,
          "Escape character must be followed by '%', '_' or the escape character itself");

      auto key =
          std::pair<StringView, std::optional<char>>{pattern, escapeChar};

      auto [it, inserted] = compiledRegularExpressions_.emplace(
          key, std::make_unique<RE2>(toStringPiece(regex), opt));
      VELOX_USER_CHECK_LE(
          compiledRegularExpressions_.size(),
          kMaxCompiledRegexes,
          "Max number of regex reached");
      checkForBadPattern(*it->second);
      return re2FullMatch(input, *it->second);
    };

    auto applyRow = [&](const StringView& input,
                        const StringView& pattern,
                        const std::optional<char>& escapeChar) -> bool {
      PatternMetadata patternMetadata =
          determinePatternKind(std::string_view(pattern), escapeChar);
      const auto reducedLength = patternMetadata.length;
      const auto& fixedPattern = patternMetadata.fixedPattern;

      switch (patternMetadata.patternKind) {
        case PatternKind::kExactlyN:
          return OptimizedLike<PatternKind::kExactlyN>::match(
              input, pattern, reducedLength);
        case PatternKind::kAtLeastN:
          return OptimizedLike<PatternKind::kAtLeastN>::match(
              input, pattern, reducedLength);
        case PatternKind::kFixed:
          return OptimizedLike<PatternKind::kFixed>::match(
              input, fixedPattern, reducedLength);
        case PatternKind::kPrefix:
          return OptimizedLike<PatternKind::kPrefix>::match(
              input, fixedPattern, reducedLength);
        case PatternKind::kSuffix:
          return OptimizedLike<PatternKind::kSuffix>::match(
              input, fixedPattern, reducedLength);
        case PatternKind::kSubstring:
          return OptimizedLike<PatternKind::kSubstring>::match(
              input, fixedPattern, reducedLength);
        default:
          return applyWithRegex(input, pattern, escapeChar);
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
class Re2ExtractAllConstantPattern final : public VectorFunction {
 public:
  explicit Re2ExtractAllConstantPattern(StringView pattern)
      : re_(toStringPiece(pattern), RE2::Quiet) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      EvalCtx& context,
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
class Re2ExtractAll final : public VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      EvalCtx& context,
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
        RE2 re(toStringPiece(pattern->valueAt<StringView>(row)), RE2::Quiet);
        checkForBadPattern(re);
        re2ExtractAll(resultWriter, re, inputStrs, row, groups, 0);
      });
    } else {
      // Case 2: Has groupId
      //
      exec::LocalDecodedVector groupIds(context, *args[2], rows);
      context.applyToSelectedNoThrow(rows, [&](vector_size_t row) {
        const T groupId = groupIds->valueAt<T>(row);
        RE2 re(toStringPiece(pattern->valueAt<StringView>(row)), RE2::Quiet);
        checkForBadPattern(re);
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
};

template <bool (*Fn)(StringView, const RE2&)>
std::shared_ptr<VectorFunction> makeRe2MatchImpl(
    const std::string& name,
    const std::vector<VectorFunctionArg>& inputArgs) {
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
  static std::shared_ptr<Re2Match<Fn>> kMatchExpr =
      std::make_shared<Re2Match<Fn>>();
  return kMatchExpr;
}

} // namespace

std::shared_ptr<VectorFunction> makeRe2Match(
    const std::string& name,
    const std::vector<VectorFunctionArg>& inputArgs,
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

std::shared_ptr<VectorFunction> makeRe2Search(
    const std::string& name,
    const std::vector<VectorFunctionArg>& inputArgs,
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

std::shared_ptr<VectorFunction> makeRe2Extract(
    const std::string& name,
    const std::vector<VectorFunctionArg>& inputArgs,
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

std::string unescape(
    std::string_view pattern,
    size_t start,
    size_t end,
    std::optional<char> escapeChar) {
  if (!escapeChar) {
    return std::string(pattern.data() + start, end - start);
  }

  std::ostringstream os;
  auto cursor = pattern.begin() + start;
  auto endCursor = pattern.begin() + end;
  while (cursor < endCursor) {
    auto previous = cursor;

    // Find the next escape char.
    cursor = std::find(cursor, endCursor, escapeChar.value());
    if (cursor < endCursor) {
      // There are non-escape chars, append them.
      if (previous < cursor) {
        os.write(previous, cursor - previous);
      }

      // Make sure there is a following normal char.
      VELOX_USER_CHECK(
          cursor + 1 < endCursor,
          "Escape character must be followed by '%', '_' or the escape character itself");

      // Make sure the escaped char is valid.
      cursor++;
      auto current = *cursor;
      VELOX_USER_CHECK(
          current == escapeChar || current == '_' || current == '%',
          "Escape character must be followed by '%', '_' or the escape character itself");

      // Append the escaped char.
      os << current;
    } else {
      // Escape char not found, append all the non-escape chars.
      os.write(previous, endCursor - previous);
      break;
    }

    // Advance the cursor.
    cursor++;
  }

  return os.str();
}

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

    isPreviousWildcard_ =
        (charKind_ == CharKind::kSingleCharWildcard ||
         charKind_ == CharKind::kAnyCharsWildcard);

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
      } else if (currentChar == '%') {
        charKind_ = CharKind::kAnyCharsWildcard;
      } else {
        charKind_ = CharKind::kNormal;
      }
      nextStart_ = currentStart_ + 1;
    }

    return true;
  }

  // Start index of the current character.
  size_t currentStart() const {
    return currentStart_;
  }

  bool isAnyCharsWildcard() const {
    return charKind_ == CharKind::kAnyCharsWildcard;
  }

  bool isSingleCharWildcard() const {
    return charKind_ == CharKind::kSingleCharWildcard;
  }

  bool isWildcard() {
    return isAnyCharsWildcard() || isSingleCharWildcard();
  }

  bool isPreviousWildcard() {
    return isPreviousWildcard_;
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
    VELOX_DCHECK(index >= 0 && index < pattern_.size())
    return pattern_.data()[index];
  }

  std::string_view pattern_;
  const std::optional<char> escapeChar_;

  size_t currentStart_{0};
  size_t nextStart_{0};
  CharKind charKind_{CharKind::kNormal};
  bool isPreviousWildcard_{false};
};

PatternMetadata determinePatternKind(
    std::string_view pattern,
    std::optional<char> escapeChar) {
  const size_t patternLength = pattern.size();

  // Index of the first % or _ character(not escaped).
  int32_t wildcardStart = -1;
  // Index of the first character that is not % and not _.
  int32_t fixedPatternStart = -1;
  // Index of the last character in the fixed pattern, used to retrieve the
  // fixed string for patterns of type kSubstring.
  int32_t fixedPatternEnd = -1;
  // Count of wildcard character sequences in pattern.
  size_t numWildcardSequences = 0;
  // Total number of % characters.
  size_t anyCharacterWildcardCount = 0;
  // Total number of _ characters.
  size_t singleCharacterWildcardCount = 0;

  PatternStringIterator iterator{pattern, escapeChar};

  // Iterate through the pattern string to collect the stats for the simple
  // patterns that we can optimize.
  while (iterator.next()) {
    const size_t currentStart = iterator.currentStart();
    if (iterator.isWildcard()) {
      if (wildcardStart == -1) {
        wildcardStart = currentStart;
      }

      if (iterator.isSingleCharWildcard()) {
        ++singleCharacterWildcardCount;
      } else {
        ++anyCharacterWildcardCount;
      }

      if (!iterator.isPreviousWildcard()) {
        ++numWildcardSequences;
      }

      // Mark the end of the fixed pattern.
      if (fixedPatternStart != -1 && fixedPatternEnd == -1) {
        fixedPatternEnd = currentStart - 1;
      }
    } else {
      // Record the first fixed pattern start.
      if (fixedPatternStart == -1) {
        fixedPatternStart = currentStart;
      } else {
        // This is not the first fixed pattern, not supported, so fallback.
        if (iterator.isPreviousWildcard()) {
          return PatternMetadata{PatternKind::kGeneric, 0};
        }
      }
    }
  }

  // The pattern end may not been marked if there is no wildcard char after
  // pattern start, so we mark it here.
  if (fixedPatternStart != -1 && fixedPatternEnd == -1) {
    fixedPatternEnd = patternLength - 1;
  }

  // At this point pattern has max of one fixed pattern.
  // Pattern contains wildcard characters only.
  if (fixedPatternStart == -1) {
    if (anyCharacterWildcardCount == 0) {
      return PatternMetadata{
          PatternKind::kExactlyN, singleCharacterWildcardCount};
    }
    return PatternMetadata{
        PatternKind::kAtLeastN, singleCharacterWildcardCount};
  }

  // At this point pattern contains exactly one fixed pattern.
  // Pattern contains no wildcard characters (is a fixed pattern).
  if (wildcardStart == -1) {
    auto fixedPattern = unescape(pattern, 0, patternLength, escapeChar);
    return PatternMetadata{
        PatternKind::kFixed, fixedPattern.size(), fixedPattern};
  }

  // Pattern is generic if it has '_' wildcard characters and a fixed pattern.
  if (singleCharacterWildcardCount > 0) {
    return PatternMetadata{PatternKind::kGeneric, 0};
  }

  // Classify pattern as prefix, fixed center, or suffix pattern based on the
  // position and count of the wildcard character sequence and fixed pattern.
  if (fixedPatternStart < wildcardStart) {
    auto fixedPattern = unescape(pattern, 0, wildcardStart, escapeChar);
    return PatternMetadata{
        PatternKind::kPrefix, fixedPattern.size(), fixedPattern};
  }

  // if numWildcardSequences > 1, then fixed pattern must be in between them.
  if (numWildcardSequences == 2) {
    auto fixedPattern =
        unescape(pattern, fixedPatternStart, fixedPatternEnd + 1, escapeChar);
    return PatternMetadata{
        PatternKind::kSubstring, fixedPattern.size(), fixedPattern};
  }

  auto fixedPattern =
      unescape(pattern, fixedPatternStart, patternLength, escapeChar);

  return PatternMetadata{
      PatternKind::kSuffix, fixedPattern.size(), fixedPattern};
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

  PatternMetadata patternMetadata;
  try {
    patternMetadata =
        determinePatternKind(std::string_view(pattern), escapeChar);
  } catch (...) {
    return std::make_shared<exec::AlwaysFailingVectorFunction>(
        std::current_exception());
  }

  size_t reducedLength = patternMetadata.length;
  auto fixedPattern = patternMetadata.fixedPattern;

  switch (patternMetadata.patternKind) {
    case PatternKind::kExactlyN:
      return std::make_shared<OptimizedLike<PatternKind::kExactlyN>>(
          pattern, reducedLength);
    case PatternKind::kAtLeastN:
      return std::make_shared<OptimizedLike<PatternKind::kAtLeastN>>(
          pattern, reducedLength);
    case PatternKind::kFixed:
      return std::make_shared<OptimizedLike<PatternKind::kFixed>>(
          fixedPattern, reducedLength);
    case PatternKind::kPrefix:
      return std::make_shared<OptimizedLike<PatternKind::kPrefix>>(
          fixedPattern, reducedLength);
    case PatternKind::kSuffix:
      return std::make_shared<OptimizedLike<PatternKind::kSuffix>>(
          fixedPattern, reducedLength);
    case PatternKind::kSubstring:
      return std::make_shared<OptimizedLike<PatternKind::kSubstring>>(
          fixedPattern, reducedLength);
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

std::shared_ptr<VectorFunction> makeRe2ExtractAll(
    const std::string& name,
    const std::vector<VectorFunctionArg>& inputArgs,
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

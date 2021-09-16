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
#include <optional>

#include "velox/expression/EvalCtx.h"
#include "velox/expression/Expr.h"
#include "velox/expression/VectorUdfTypeSystem.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::functions {
namespace {

using ::facebook::velox::exec::EvalCtx;
using ::facebook::velox::exec::Expr;
using ::facebook::velox::exec::VectorFunction;
using ::facebook::velox::exec::VectorFunctionArg;
using ::re2::RE2;

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
    velox::memory::MemoryPool* pool,
    std::shared_ptr<BaseVector>* result) {
  BaseVector::ensureWritable(rows, BOOLEAN(), pool, result);
  return *(*result)->as<FlatVector<bool>>();
}

FlatVector<StringView>& ensureWritableStringView(
    const SelectivityVector& rows,
    velox::memory::MemoryPool* pool,
    std::shared_ptr<BaseVector>* result) {
  BaseVector::ensureWritable(rows, VARCHAR(), pool, result);
  auto* flat = (*result)->as<FlatVector<StringView>>();
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
    int32_t groupId) {
  const StringView str = strs->valueAt<StringView>(row);
  DCHECK_GT(groups.size(), groupId);
  if (!re.Match(
          toStringPiece(str),
          0,
          str.size(),
          RE2::UNANCHORED, // Full match not required.
          groups.data(),
          groupId + 1)) {
    result.setNull(row, true);
    return false;
  } else {
    const re2::StringPiece extracted = groups[groupId];
    result.setNoCopy(row, StringView(extracted.data(), extracted.size()));
    return !StringView::isInline(extracted.size());
  }
}

template <bool (*Fn)(StringView, const RE2&)>
class Re2MatchConstantPattern final : public VectorFunction {
 public:
  explicit Re2MatchConstantPattern(StringView pattern)
      : re_(toStringPiece(pattern), RE2::Quiet) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      Expr* /* caller */,
      EvalCtx* context,
      VectorPtr* resultRef) const final {
    VELOX_CHECK_EQ(args.size(), 2);
    FlatVector<bool>& result =
        ensureWritableBool(rows, context->pool(), resultRef);
    exec::LocalDecodedVector toSearch(context, *args[0], rows);
    checkForBadPattern(re_);
    rows.applyToSelected([&](int i) {
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
      Expr* caller,
      EvalCtx* context,
      VectorPtr* resultRef) const override {
    VELOX_CHECK_EQ(args.size(), 2);
    if (auto pattern = getIfConstant<StringView>(*args[1])) {
      Re2MatchConstantPattern<Fn>(*pattern).apply(
          rows, args, caller, context, resultRef);
      return;
    }
    // General case.
    FlatVector<bool>& result =
        ensureWritableBool(rows, context->pool(), resultRef);
    exec::LocalDecodedVector toSearch(context, *args[0], rows);
    exec::LocalDecodedVector pattern(context, *args[1], rows);
    rows.applyToSelected([&](int row) {
      RE2 re(toStringPiece(pattern->valueAt<StringView>(row)), RE2::Quiet);
      checkForBadPattern(re);
      result.set(row, Fn(toSearch->valueAt<StringView>(row), re));
    });
  }
};

void checkForBadGroupId(int groupId, const RE2& re) {
  if (UNLIKELY(groupId < 0 || groupId > re.NumberOfCapturingGroups())) {
    VELOX_USER_FAIL("No group {} in regex '{}'", groupId, re.pattern());
  }
}

template <typename T>
class Re2SearchAndExtractConstantPattern final : public VectorFunction {
 public:
  explicit Re2SearchAndExtractConstantPattern(StringView pattern)
      : re_(toStringPiece(pattern), RE2::Quiet) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      Expr* /* caller */,
      EvalCtx* context,
      VectorPtr* resultRef) const final {
    VELOX_CHECK(args.size() == 2 || args.size() == 3);
    // TODO: Potentially re-use the string vector, not just the buffer.
    FlatVector<StringView>& result =
        ensureWritableStringView(rows, context->pool(), resultRef);

    // apply() will not be invoked if the selection is empty.
    checkForBadPattern(re_);

    exec::LocalDecodedVector toSearch(context, *args[0], rows);
    bool mustRefSourceStrings = false;
    FOLLY_DECLARE_REUSED(groups, std::vector<re2::StringPiece>);
    // Common case: constant group id.
    if (args.size() == 2) {
      groups.resize(1);
      rows.applyToSelected([&](int i) {
        mustRefSourceStrings |= re2Extract(result, i, re_, toSearch, groups, 0);
      });
      if (mustRefSourceStrings) {
        result.acquireSharedStringBuffers(toSearch->base());
      }
      return;
    }

    if (const auto groupId = getIfConstant<T>(*args[2])) {
      checkForBadGroupId(*groupId, re_);
      groups.resize(*groupId + 1);
      rows.applyToSelected([&](int i) {
        mustRefSourceStrings |=
            re2Extract(result, i, re_, toSearch, groups, *groupId);
      });
      if (mustRefSourceStrings) {
        result.acquireSharedStringBuffers(toSearch->base());
      }
      return;
    }

    // Less common case: variable group id. Resize the groups vector to
    // accommodate the largest group id referenced.
    exec::LocalDecodedVector groupIds(context, *args[2], rows);
    T maxGroupId = 0, minGroupId = 0;
    rows.applyToSelected([&](int i) {
      maxGroupId = std::max(groupIds->valueAt<T>(i), maxGroupId);
      minGroupId = std::min(groupIds->valueAt<T>(i), minGroupId);
    });
    checkForBadGroupId(maxGroupId, re_);
    checkForBadGroupId(minGroupId, re_);
    groups.resize(maxGroupId + 1);
    rows.applyToSelected([&](int i) {
      T group = groupIds->valueAt<T>(i);
      mustRefSourceStrings |=
          re2Extract(result, i, re_, toSearch, groups, group);
    });
    if (mustRefSourceStrings) {
      result.acquireSharedStringBuffers(toSearch->base());
    }
  }

 private:
  RE2 re_;
};

// The factory function we provide returns a unique instance for each call, so
// this is safe.
template <typename T>
class Re2SearchAndExtract final : public VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      Expr* caller,
      EvalCtx* context,
      VectorPtr* resultRef) const final {
    VELOX_CHECK(args.size() == 2 || args.size() == 3);
    // Handle the common case of a constant pattern.
    if (auto pattern = getIfConstant<StringView>(*args[1])) {
      Re2SearchAndExtractConstantPattern<T>(*pattern).apply(
          rows, args, caller, context, resultRef);
      return;
    }
    // The general case. Further optimizations are possible to avoid regex
    // recompilation, but a constant pattern is by far the most common case.
    FlatVector<StringView>& result =
        ensureWritableStringView(rows, context->pool(), resultRef);
    exec::LocalDecodedVector toSearch(context, *args[0], rows);
    exec::LocalDecodedVector pattern(context, *args[1], rows);
    bool mustRefSourceStrings = false;
    FOLLY_DECLARE_REUSED(groups, std::vector<re2::StringPiece>);
    if (args.size() == 2) {
      groups.resize(1);
      rows.applyToSelected([&](int i) {
        RE2 re(toStringPiece(pattern->valueAt<StringView>(i)), RE2::Quiet);
        checkForBadPattern(re);
        mustRefSourceStrings |= re2Extract(result, i, re, toSearch, groups, 0);
      });
    } else {
      exec::LocalDecodedVector groupIds(context, *args[2], rows);
      rows.applyToSelected([&](int i) {
        const auto groupId = groupIds->valueAt<T>(i);
        RE2 re(toStringPiece(pattern->valueAt<StringView>(i)), RE2::Quiet);
        checkForBadPattern(re);
        checkForBadGroupId(groupId, re);
        groups.resize(groupId + 1);
        mustRefSourceStrings |=
            re2Extract(result, i, re, toSearch, groups, groupId);
      });
    }
    if (mustRefSourceStrings) {
      result.acquireSharedStringBuffers(toSearch->base());
    }
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
    const std::vector<VectorFunctionArg>& inputArgs) {
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
    const std::vector<VectorFunctionArg>& inputArgs) {
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
    const std::vector<VectorFunctionArg>& inputArgs) {
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
            pattern);
      case TypeKind::BIGINT:
        return std::make_shared<Re2SearchAndExtractConstantPattern<int64_t>>(
            pattern);
      default:
        VELOX_UNREACHABLE();
    }
  }

  switch (groupIdTypeKind) {
    case TypeKind::INTEGER:
      return std::make_shared<Re2SearchAndExtract<int32_t>>();
    case TypeKind::BIGINT:
      return std::make_shared<Re2SearchAndExtract<int64_t>>();
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

} // namespace facebook::velox::functions

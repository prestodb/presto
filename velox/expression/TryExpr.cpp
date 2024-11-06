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
#include "velox/expression/TryExpr.h"
#include "velox/expression/ScopedVarSetter.h"

namespace facebook::velox::exec {

void TryExpr::evalSpecialForm(
    const SelectivityVector& rows,
    EvalCtx& context,
    VectorPtr& result) {
  ScopedVarSetter throwOnError(context.mutableThrowOnError(), false);
  ScopedVarSetter captureErrorDetails(
      context.mutableCaptureErrorDetails(), false);

  ScopedThreadSkipErrorDetails skipErrorDetails(true);

  // It's possible with nested TRY expressions that some rows already threw
  // exceptions in earlier expressions that haven't been handled yet. To avoid
  // incorrectly handling them here, store those errors and temporarily reset
  // the errors in context to nullptr, so we only handle errors coming from
  // expressions that are children of this TRY expression.
  // This also prevents this TRY expression from leaking exceptions to the
  // parent TRY expression, so the parent won't incorrectly null out rows that
  // threw exceptions which this expression already handled.
  ScopedVarSetter<EvalErrorsPtr> errorsSetter(context.errorsPtr(), nullptr);

  // Allocate error vector to avoid repeated re-allocations for every failed
  // row.
  context.ensureErrorsVectorSize(rows.end());

  inputs_[0]->eval(rows, context, result);

  nullOutErrors(rows, context, result);
}

void TryExpr::evalSpecialFormSimplified(
    const SelectivityVector& rows,
    EvalCtx& context,
    VectorPtr& result) {
  ScopedVarSetter throwOnError(context.mutableThrowOnError(), false);
  ScopedVarSetter captureErrorDetails(
      context.mutableCaptureErrorDetails(), false);

  ScopedThreadSkipErrorDetails skipErrorDetails(true);

  // It's possible with nested TRY expressions that some rows already threw
  // exceptions in earlier expressions that haven't been handled yet. To avoid
  // incorrectly handling them here, store those errors and temporarily reset
  // the errors in context to nullptr, so we only handle errors coming from
  // expressions that are children of this TRY expression.
  // This also prevents this TRY expression from leaking exceptions to the
  // parent TRY expression, so the parent won't incorrectly null out rows that
  // threw exceptions which this expression already handled.
  ScopedVarSetter<EvalErrorsPtr> errorsSetter(context.errorsPtr(), nullptr);

  inputs_[0]->evalSimplified(rows, context, result);

  nullOutErrors(rows, context, result);
}

namespace {

// Apply onError methods of registered listeners on every row that encounters
// errors. The error vector must exist.
void applyListenersOnError(
    const SelectivityVector& rows,
    const EvalCtx& context) {
  const auto* errors = context.errors();
  VELOX_CHECK_NOT_NULL(errors);

  vector_size_t numErrors = 0;
  rows.applyToSelected([&](auto row) {
    if (errors->hasErrorAt(row)) {
      ++numErrors;
    }
  });

  if (numErrors == 0) {
    return;
  }

  exprSetListeners().withRLock([&](auto& listeners) {
    if (!listeners.empty()) {
      for (auto& listener : listeners) {
        listener->onError(numErrors, context.execCtx()->queryCtx()->queryId());
      }
    }
  });
}
} // namespace

void TryExpr::nullOutErrors(
    const SelectivityVector& rows,
    EvalCtx& context,
    VectorPtr& result) const {
  const auto* errors = context.errors();
  if (!errors) {
    return;
  }

  if (!errors->hasError()) {
    return;
  }

  applyListenersOnError(rows, context);

  if (result->isConstantEncoding()) {
    // Since it's constant, if any row is NULL they're all NULL, so check row
    // 0 arbitrarily.
    if (result->isNullAt(0)) {
      // The result is already a NULL constant, so this is a no-op.
      return;
    }

    auto size = result->size();
    VELOX_DCHECK_GE(size, rows.end());

    auto nulls = allocateNulls(size, context.pool());
    auto rawNulls = nulls->asMutable<uint64_t>();
    rows.applyToSelected([&](auto row) {
      if (errors->hasErrorAt(row)) {
        bits::setNull(rawNulls, row, true);
      }
    });

    // Wrap in dictionary indices all pointing to index 0.
    auto indices = allocateIndices(size, context.pool());
    result = BaseVector::wrapInDictionary(nulls, indices, size, result);
  } else if (
      result.use_count() == 1 && result->isNullsWritable() &&
      result->size() >= rows.end()) {
    auto* rawNulls = result->mutableRawNulls();
    rows.applyToSelected([&](auto row) {
      if (errors->hasErrorAt(row)) {
        bits::setNull(rawNulls, row, true);
      }
    });
  } else {
    auto nulls = allocateNulls(rows.end(), context.pool());
    auto* rawNulls = nulls->asMutable<uint64_t>();
    auto indices = allocateIndices(rows.end(), context.pool());
    auto* rawIndices = indices->asMutable<vector_size_t>();

    rows.applyToSelected([&](auto row) {
      rawIndices[row] = row;
      if (errors->hasErrorAt(row)) {
        bits::setNull(rawNulls, row, true);
      }
    });

    result = BaseVector::wrapInDictionary(nulls, indices, rows.end(), result);
  }
}

TypePtr TryCallToSpecialForm::resolveType(
    const std::vector<TypePtr>& argTypes) {
  VELOX_CHECK_EQ(
      argTypes.size(),
      1,
      "TRY expressions expect exactly 1 argument, received: {}",
      argTypes.size());
  return argTypes[0];
}

ExprPtr TryCallToSpecialForm::constructSpecialForm(
    const TypePtr& type,
    std::vector<ExprPtr>&& compiledChildren,
    bool /* trackCpuUsage */,
    const core::QueryConfig& /*config*/) {
  VELOX_CHECK_EQ(
      compiledChildren.size(),
      1,
      "TRY expressions expect exactly 1 argument, received: {}",
      compiledChildren.size());
  return std::make_shared<TryExpr>(type, std::move(compiledChildren[0]));
}
} // namespace facebook::velox::exec

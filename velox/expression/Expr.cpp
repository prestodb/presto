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
#include <boost/lexical_cast.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <fstream>

#include "velox/common/base/Exceptions.h"
#include "velox/common/base/Fs.h"
#include "velox/common/base/SuccinctPrinter.h"
#include "velox/common/process/ThreadDebugInfo.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/core/Expressions.h"
#include "velox/expression/CastExpr.h"
#include "velox/expression/ConstantExpr.h"
#include "velox/expression/Expr.h"
#include "velox/expression/ExprCompiler.h"
#include "velox/expression/FieldReference.h"
#include "velox/expression/PeeledEncoding.h"
#include "velox/expression/ScopedVarSetter.h"
#include "velox/expression/VectorFunction.h"
#include "velox/vector/SelectivityVector.h"
#include "velox/vector/VectorSaver.h"

DEFINE_bool(
    force_eval_simplified,
    false,
    "Whether to overwrite queryCtx and force the "
    "use of simplified expression evaluation path.");

DEFINE_bool(
    velox_experimental_save_input_on_fatal_signal,
    false,
    "This is an experimental flag only to be used for debugging "
    "purposes. If set to true, serializes the input vector data and "
    "all the SQL expressions in the ExprSet that is currently "
    "executing, whenever a fatal signal is encountered. Enabling "
    "this flag makes the signal handler async signal unsafe, so it "
    "should only be used for debugging purposes. The vector and SQLs "
    "are serialized to files in directories specified by either "
    "'velox_save_input_on_expression_any_failure_path' or "
    "'velox_save_input_on_expression_system_failure_path'");

namespace facebook::velox::exec {

folly::Synchronized<std::vector<std::shared_ptr<ExprSetListener>>>&
exprSetListeners() {
  static folly::Synchronized<std::vector<std::shared_ptr<ExprSetListener>>>
      kListeners;
  return kListeners;
}

bool registerExprSetListener(std::shared_ptr<ExprSetListener> listener) {
  return exprSetListeners().withWLock([&](auto& listeners) {
    for (const auto& existingListener : listeners) {
      if (existingListener == listener) {
        // Listener already registered. Do not register again.
        return false;
      }
    }
    listeners.push_back(std::move(listener));
    return true;
  });
}

bool unregisterExprSetListener(
    const std::shared_ptr<ExprSetListener>& listener) {
  return exprSetListeners().withWLock([&](auto& listeners) {
    for (auto it = listeners.begin(); it != listeners.end(); ++it) {
      if ((*it) == listener) {
        listeners.erase(it);
        return true;
      }
    }

    // Listener not found.
    return false;
  });
}

namespace {

bool isMember(
    const std::vector<FieldReference*>& fields,
    FieldReference& field) {
  return std::find(fields.begin(), fields.end(), &field) != fields.end();
}

// Returns true if input expression or any sub-expression is an IF, AND or OR.
bool hasConditionals(Expr* expr) {
  if (expr->isConditional()) {
    return true;
  }

  for (const auto& child : expr->inputs()) {
    if (hasConditionals(child.get())) {
      return true;
    }
  }

  return false;
}

void checkOrSetEmptyResult(
    const TypePtr& type,
    memory::MemoryPool* pool,
    VectorPtr& result) {
  if (!result) {
    result = BaseVector::createNullConstant(type, 0, pool);
  }
}
} // namespace

Expr::Expr(
    TypePtr type,
    std::vector<std::shared_ptr<Expr>>&& inputs,
    std::shared_ptr<VectorFunction> vectorFunction,
    std::string name,
    bool trackCpuUsage)
    : type_(std::move(type)),
      inputs_(std::move(inputs)),
      name_(std::move(name)),
      vectorFunction_(std::move(vectorFunction)),
      specialForm_{false},
      supportsFlatNoNullsFastPath_{
          vectorFunction_->supportsFlatNoNullsFastPath() &&
          type_->isPrimitiveType() && type_->isFixedWidth() &&
          allSupportFlatNoNullsFastPath(inputs_)},
      trackCpuUsage_{trackCpuUsage} {
  constantInputs_.reserve(inputs_.size());
  inputIsConstant_.reserve(inputs_.size());
  for (auto& expr : inputs_) {
    if (auto constantExpr = expr->as<ConstantExpr>()) {
      constantInputs_.emplace_back(constantExpr->value());
      inputIsConstant_.push_back(true);
    } else {
      constantInputs_.emplace_back(nullptr);
      inputIsConstant_.push_back(false);
    }
  }
}

// static
bool Expr::isSameFields(
    const std::vector<FieldReference*>& fields1,
    const std::vector<FieldReference*>& fields2) {
  if (fields1.size() != fields2.size()) {
    return false;
  }
  return std::all_of(
      fields1.begin(), fields1.end(), [&fields2](const auto& field) {
        return isMember(fields2, *field);
      });
}

bool Expr::isSubsetOfFields(
    const std::vector<FieldReference*>& subset,
    const std::vector<FieldReference*>& superset) {
  if (subset.size() > superset.size()) {
    return false;
  }
  return std::all_of(
      subset.begin(), subset.end(), [&superset](const auto& field) {
        return isMember(superset, *field);
      });
}

// static
bool Expr::allSupportFlatNoNullsFastPath(
    const std::vector<std::shared_ptr<Expr>>& exprs) {
  for (const auto& expr : exprs) {
    if (!expr->supportsFlatNoNullsFastPath()) {
      return false;
    }
  }

  return true;
}

void Expr::clearMetaData() {
  metaDataComputed_ = false;
  for (auto& child : inputs_) {
    child->clearMetaData();
  }
  propagatesNulls_ = false;
  distinctFields_.clear();
  multiplyReferencedFields_.clear();
  hasConditionals_ = false;
  deterministic_ = true;
  sameAsParentDistinctFields_ = false;
}

void Expr::mergeFields(
    std::vector<FieldReference*>& distinctFields,
    std::unordered_set<FieldReference*>& multiplyReferencedFields,
    const std::vector<FieldReference*>& moreFields) {
  for (auto* newField : moreFields) {
    if (isMember(distinctFields, *newField)) {
      multiplyReferencedFields.insert(newField);
    } else {
      distinctFields.emplace_back(newField);
    }
  }
}

void Expr::computeDistinctFields() {
  for (auto& input : inputs_) {
    mergeFields(
        distinctFields_, multiplyReferencedFields_, input->distinctFields_);
  }
}

void Expr::computeMetadata() {
  if (metaDataComputed_) {
    return;
  }

  // Compute metadata for all the inputs.
  for (auto& input : inputs_) {
    input->computeMetadata();
  }

  // (1) Compute deterministic_.
  // An expression is deterministic if it is a deterministic function call or a
  // special form, and all its inputs are also deterministic.
  if (vectorFunction_) {
    deterministic_ = vectorFunction_->isDeterministic();
  } else {
    VELOX_CHECK(isSpecialForm());
    deterministic_ = true;
  }

  for (auto& input : inputs_) {
    deterministic_ &= input->deterministic_;
  }

  // (2) Compute distinctFields_ and multiplyReferencedFields_.
  computeDistinctFields();

  // (3) Compute propagatesNulls_.
  // propagatesNulls_ is true iff a null in any of the columns this
  // depends on makes the Expr null.
  if (isSpecialForm() && !is<ConstantExpr>() && !is<FieldReference>() &&
      !is<CastExpr>()) {
    as<SpecialForm>()->computePropagatesNulls();
  } else {
    if (vectorFunction_ && !vectorFunction_->isDefaultNullBehavior()) {
      propagatesNulls_ = false;
    } else {
      // Logic for handling default-null vector functions.
      // cast, constant and fieldReference expressions act as vector functions
      // with default null behavior.

      // If the function has default null behavior, the Expr propagates nulls if
      // the set of fields null-propagating arguments depend on is a superset of
      // the fields non null-propagating arguments depend on.
      std::unordered_set<FieldReference*> nullPropagating, nonNullPropagating;
      for (auto& input : inputs_) {
        if (input->propagatesNulls_) {
          nullPropagating.insert(
              input->distinctFields_.begin(), input->distinctFields_.end());
        } else {
          nonNullPropagating.insert(
              input->distinctFields_.begin(), input->distinctFields_.end());
        }
      }

      // propagatesNulls_ is true if nonNullPropagating is subset of
      // nullPropagating.
      propagatesNulls_ = true;
      for (auto* field : nonNullPropagating) {
        if (!nullPropagating.count(field)) {
          propagatesNulls_ = false;
          break;
        }
      }
    }
  }

  for (auto& input : inputs_) {
    if (isSameFields(distinctFields_, input->distinctFields_)) {
      input->sameAsParentDistinctFields_ = true;
    }
  }

  // (5) Compute hasConditionals_.
  hasConditionals_ = hasConditionals(this);

  metaDataComputed_ = true;
}

namespace {
void rethrowFirstError(
    const SelectivityVector& rows,
    const ErrorVectorPtr& errors) {
  auto errorSize = errors->size();
  rows.testSelected([&](vector_size_t row) {
    if (row >= errorSize) {
      return false;
    }
    if (!errors->isNullAt(row)) {
      auto exceptionPtr =
          std::static_pointer_cast<std::exception_ptr>(errors->valueAt(row));
      std::rethrow_exception(*exceptionPtr);
    }
    return true;
  });
}

// Sets errors in 'context' to be the union of 'argumentErrors' for 'rows' and
// 'errors'. If 'context' throws on first error and 'argumentErrors'
// has errors, throws the first error in 'argumentErrors' scoped to 'rows'.
// Otherwise sets 'errors()' of 'context' to the union of the errors. This is
// used after all arguments of a function call have been evaluated and
// we decide on whether to throw or what errors to leave in 'context' for the
// caller.
void mergeOrThrowArgumentErrors(
    const SelectivityVector& rows,
    ErrorVectorPtr& originalErrors,
    ErrorVectorPtr& argumentErrors,
    EvalCtx& context) {
  if (argumentErrors) {
    if (context.throwOnError()) {
      rethrowFirstError(rows, argumentErrors);
    }
    context.addErrors(rows, argumentErrors, originalErrors);
  }
  context.swapErrors(originalErrors);
}

// Returns true if vector is a LazyVector that hasn't been loaded yet or
// is not dictionary or constant encoded.
bool isFlat(const BaseVector& vector) {
  auto encoding = vector.encoding();
  if (encoding == VectorEncoding::Simple::LAZY) {
    if (!vector.asUnchecked<LazyVector>()->isLoaded()) {
      return true;
    }

    encoding = vector.loadedVector()->encoding();
  }
  return !(
      encoding == VectorEncoding::Simple::DICTIONARY ||
      encoding == VectorEncoding::Simple::CONSTANT);
}

inline void checkResultInternalState(VectorPtr& result) {
#ifndef NDEBUG
  if (result != nullptr) {
    result->validate();
  }
#endif
}

} // namespace

template <typename EvalArg>
bool Expr::evalArgsDefaultNulls(
    MutableRemainingRows& rows,
    EvalArg evalArg,
    EvalCtx& context,
    VectorPtr& result) {
  ErrorVectorPtr argumentErrors;
  ErrorVectorPtr originalErrors;
  LocalDecodedVector decoded(context);
  // Store pre-existing errors locally and clear them from
  // 'context'. We distinguish between argument errors and
  // pre-existing ones.
  if (context.errors()) {
    context.swapErrors(originalErrors);
  }

  inputValues_.resize(inputs_.size());
  {
    ScopedVarSetter throwErrors(
        context.mutableThrowOnError(), throwArgumentErrors(context));

    for (int32_t i = 0; i < inputs_.size(); ++i) {
      evalArg(i);
      const uint64_t* flatNulls = nullptr;
      auto& arg = inputValues_[i];
      if (arg->mayHaveNulls()) {
        decoded.get()->decode(*arg, rows.rows());
        flatNulls = decoded.get()->nulls(&rows.rows());
      }
      // A null with no error deselects the row.
      // An error adds itself to argument errors.
      if (context.errors()) {
        // There are new errors.
        context.ensureErrorsVectorSize(*context.errorsPtr(), rows.rows().end());
        auto newErrors = context.errors();
        assert(newErrors); // lint
        if (flatNulls) {
          // There are both nulls and errors. Only a null with no error removes
          // a row.
          auto errorNulls = newErrors->rawNulls();
          auto rowBits = rows.mutableRows().asMutableRange().bits();
          auto nwords = bits::nwords(rows.rows().end());
          for (auto j = 0; j < nwords; ++j) {
            auto nullNoError =
                errorNulls ? flatNulls[j] | errorNulls[j] : flatNulls[j];
            rowBits[j] &= nullNoError;
          }
          rows.mutableRows().updateBounds();
        }
        context.moveAppendErrors(argumentErrors);
      } else if (flatNulls) {
        rows.deselectNulls(flatNulls);
      }

      if (!rows.rows().hasSelections()) {
        break;
      }
    }
  }

  mergeOrThrowArgumentErrors(
      rows.rows(), originalErrors, argumentErrors, context);

  if (!rows.deselectErrors()) {
    releaseInputValues(context);
    setAllNulls(rows.originalRows(), context, result);
    return false;
  }

  return true;
}

template <typename EvalArg>
bool Expr::evalArgsWithNulls(
    MutableRemainingRows& rows,
    EvalArg evalArg,
    EvalCtx& context,
    VectorPtr& result) {
  inputValues_.resize(inputs_.size());
  for (int32_t i = 0; i < inputs_.size(); ++i) {
    evalArg(i);
    if (!rows.deselectErrors()) {
      break;
    }
  }
  if (!rows.rows().hasSelections()) {
    releaseInputValues(context);
    setAllNulls(rows.originalRows(), context, result);
    return false;
  }
  return true;
}

void Expr::evalSimplified(
    const SelectivityVector& rows,
    EvalCtx& context,
    VectorPtr& result) {
  if (!rows.hasSelections()) {
    checkOrSetEmptyResult(type(), context.pool(), result);
    return;
  }

  LocalSelectivityVector nonNullHolder(&context);

  // First we try to update the initial selectivity vector, setting null for
  // every null on input fields (if default null behavior).
  if (propagatesNulls_) {
    removeSureNulls(rows, context, nonNullHolder);
  }

  // If the initial non null holder couldn't be created, start with the input
  // `rows`.
  auto* remainingRows = nonNullHolder.get() ? nonNullHolder.get() : &rows;

  if (remainingRows->hasSelections()) {
    evalSimplifiedImpl(*remainingRows, context, result);
  }

  if (!type()->isFunction()) {
    addNulls(rows, remainingRows->asRange().bits(), context, result);
  }
}

void Expr::releaseInputValues(EvalCtx& evalCtx) {
  evalCtx.releaseVectors(inputValues_);
  inputValues_.clear();
}

void Expr::evalSimplifiedImpl(
    const SelectivityVector& rows,
    EvalCtx& context,
    VectorPtr& result) {
  // Handle special form expressions.
  if (isSpecialForm()) {
    evalSpecialFormSimplified(rows, context, result);
    return;
  }

  MutableRemainingRows remainingRows(rows, context);
  const bool defaultNulls = vectorFunction_->isDefaultNullBehavior();
  auto evalArg = [&](int32_t i) {
    auto& inputValue = inputValues_[i];
    inputs_[i]->evalSimplified(remainingRows.rows(), context, inputValue);
    BaseVector::flattenVector(inputValue);
    VELOX_CHECK(
        inputValue->encoding() == VectorEncoding::Simple::FLAT ||
        inputValue->encoding() == VectorEncoding::Simple::ARRAY ||
        inputValue->encoding() == VectorEncoding::Simple::MAP ||
        inputValue->encoding() == VectorEncoding::Simple::ROW ||
        inputValue->encoding() == VectorEncoding::Simple::FUNCTION);
  };

  if (defaultNulls) {
    if (!evalArgsDefaultNulls(remainingRows, evalArg, context, result)) {
      return;
    }
  } else {
    if (!evalArgsWithNulls(remainingRows, evalArg, context, result)) {
      return;
    }
  }

  // Apply the actual function.
  try {
    vectorFunction_->apply(
        remainingRows.rows(), inputValues_, type(), context, result);
  } catch (const VeloxException&) {
    throw;
  } catch (const std::exception& e) {
    VELOX_USER_FAIL(e.what());
  }

  // Make sure the returned vector has its null bitmap properly set.
  addNulls(rows, remainingRows.rows().asRange().bits(), context, result);
  releaseInputValues(context);
}

namespace {

/// Data needed to generate exception context for the top-level expression. It
/// also provides functionality to persist both data and sql to disk for
/// debugging purpose
class ExprExceptionContext {
 public:
  ExprExceptionContext(
      const Expr* FOLLY_NONNULL expr,
      const RowVector* FOLLY_NONNULL vector,
      const ExprSet* FOLLY_NULLABLE parentExprSet)
      : expr_(expr), vector_(vector), parentExprSet_(parentExprSet) {}

  /// Persist data and sql on disk. Data will be persisted in $basePath/vector
  /// and sql will be persisted in $basePath/sql
  void persistDataAndSql(const char* FOLLY_NONNULL basePath) {
    // Exception already persisted or failed to persist. We don't persist again
    // in this situation.
    if (!dataPath_.empty()) {
      return;
    }

    // Persist vector to disk
    try {
      auto dataPathOpt = common::generateTempFilePath(basePath, "vector");
      if (!dataPathOpt.has_value()) {
        dataPath_ = "Failed to create file for saving input vector.";
        return;
      }
      dataPath_ = dataPathOpt.value();
      saveVectorToFile(vector_, dataPath_.c_str());
    } catch (std::exception& e) {
      dataPath_ = e.what();
      return;
    }

    // Persist sql to disk
    auto sql = expr_->toSql();
    try {
      auto sqlPathOpt = common::generateTempFilePath(basePath, "sql");
      if (!sqlPathOpt.has_value()) {
        sqlPath_ = "Failed to create file for saving SQL.";
        return;
      }
      sqlPath_ = sqlPathOpt.value();
      saveStringToFile(sql, sqlPath_.c_str());
    } catch (std::exception& e) {
      sqlPath_ = e.what();
      return;
    }

    if (parentExprSet_ != nullptr) {
      std::stringstream allSql;
      auto exprs = parentExprSet_->exprs();
      for (int i = 0; i < exprs.size(); ++i) {
        if (i > 0) {
          allSql << ", ";
        }
        allSql << exprs[i]->toSql();
      }
      try {
        auto sqlPathOpt = common::generateTempFilePath(basePath, "allExprSql");
        if (!sqlPathOpt.has_value()) {
          allExprSqlPath_ =
              "Failed to create file for saving all SQL expressions.";
          return;
        }
        allExprSqlPath_ = sqlPathOpt.value();
        saveStringToFile(allSql.str(), allExprSqlPath_.c_str());
      } catch (std::exception& e) {
        allExprSqlPath_ = e.what();
        return;
      }
    }
  }

  const Expr* FOLLY_NONNULL expr() const {
    return expr_;
  }

  const RowVector* FOLLY_NONNULL vector() const {
    return vector_;
  }

  const std::string& dataPath() const {
    return dataPath_;
  }

  const std::string& sqlPath() const {
    return sqlPath_;
  }

  const std::string& allExprSqlPath() const {
    return allExprSqlPath_;
  }

 private:
  /// The expression.
  const Expr* FOLLY_NONNULL expr_;

  /// The input vector, i.e. EvalCtx::row(). In some cases, input columns are
  /// re-used for results. Hence, 'vector' may no longer contain input data at
  /// the time of exception.
  const RowVector* FOLLY_NONNULL vector_;

  // The parent ExprSet that is executing this expression.
  const ExprSet* FOLLY_NULLABLE parentExprSet_;

  /// Path of the file storing the serialized 'vector'. Used to avoid
  /// serializing vector repeatedly in cases when multiple rows generate
  /// exceptions. This happens when exceptions are suppressed by TRY/AND/OR.
  std::string dataPath_{""};

  /// Path of the file storing the expression SQL. Used to avoid writing SQL
  /// repeatedly in cases when multiple rows generate exceptions.
  std::string sqlPath_{""};

  /// Path of the file storing the SQL for all expressions in the ExprSet that
  /// was executing this expression. Useful if the bug that caused the error was
  /// encountered due to some mutation from running the other expressions.
  std::string allExprSqlPath_{"N/A"};
};

/// Used to generate context for an error occurred while evaluating
/// top-level expression or top-level context for an error occurred while
/// evaluating top-level expression. If
/// FLAGS_velox_save_input_on_expression_failure_path
/// is not empty, saves the input vector and expression SQL to files in
/// that directory.
///
/// Returns the output of Expr::toString() for the top-level
/// expression along with the paths of the files storing the input vector and
/// expression SQL.
///
/// This function may be called multiple times if exceptions are suppressed by
/// TRY/AND/OR. The input vector will be saved only on first call and the
/// file path will be saved in ExprExceptionContext::dataPath and
/// used in subsequent calls. If an error occurs while saving the input
/// vector, the error message is saved in
/// ExprExceptionContext::dataPath and save operation is not
/// attempted again on subsequent calls.
std::string onTopLevelException(VeloxException::Type exceptionType, void* arg) {
  auto* context = static_cast<ExprExceptionContext*>(arg);

  const char* basePath =
      FLAGS_velox_save_input_on_expression_any_failure_path.c_str();
  if (strlen(basePath) == 0 && exceptionType == VeloxException::Type::kSystem) {
    basePath = FLAGS_velox_save_input_on_expression_system_failure_path.c_str();
  }
  if (strlen(basePath) == 0) {
    return context->expr()->toString();
  }

  // Save input vector to a file.
  context->persistDataAndSql(basePath);

  return fmt::format(
      "{}. Input data: {}. SQL expression: {}. All SQL expressions: {}.",
      context->expr()->toString(),
      context->dataPath(),
      context->sqlPath(),
      context->allExprSqlPath());
}

/// Used to generate context for an error occurred while evaluating
/// sub-expression. Returns the output of Expr::toString() for the
/// sub-expression.
std::string onException(VeloxException::Type /*exceptionType*/, void* arg) {
  return static_cast<Expr*>(arg)->toString();
}
} // namespace

void Expr::evalFlatNoNulls(
    const SelectivityVector& rows,
    EvalCtx& context,
    VectorPtr& result,
    const ExprSet* parentExprSet) {
  if (shouldEvaluateSharedSubexp()) {
    evaluateSharedSubexpr(
        rows,
        context,
        result,
        [&](const SelectivityVector& rows,
            EvalCtx& context,
            VectorPtr& result) {
          evalFlatNoNullsImpl(rows, context, result, parentExprSet);
        });
  } else {
    evalFlatNoNullsImpl(rows, context, result, parentExprSet);
  }
}

void Expr::evalFlatNoNullsImpl(
    const SelectivityVector& rows,
    EvalCtx& context,
    VectorPtr& result,
    const ExprSet* parentExprSet) {
  ExprExceptionContext exprExceptionContext{this, context.row(), parentExprSet};
  ExceptionContextSetter exceptionContext(
      {parentExprSet ? onTopLevelException : onException,
       parentExprSet ? (void*)&exprExceptionContext : this});

  if (!rows.hasSelections()) {
    checkOrSetEmptyResult(type(), context.pool(), result);
    return;
  }

  if (isSpecialForm()) {
    evalSpecialFormWithStats(rows, context, result);
    return;
  }

  inputValues_.resize(inputs_.size());
  for (int32_t i = 0; i < inputs_.size(); ++i) {
    if (constantInputs_[i]) {
      // No need to re-evaluate constant expression. Simply move constant values
      // from constantInputs_.
      inputValues_[i] = std::move(constantInputs_[i]);
      inputValues_[i]->resize(rows.end());
    } else {
      inputs_[i]->evalFlatNoNulls(rows, context, inputValues_[i]);
    }
  }

  applyFunction(rows, context, result);

  // Move constant values back to constantInputs_.
  for (int32_t i = 0; i < inputs_.size(); ++i) {
    if (inputIsConstant_[i]) {
      constantInputs_[i] = std::move(inputValues_[i]);
      VELOX_CHECK_NULL(inputValues_[i]);
    }
  }
  releaseInputValues(context);
}

void Expr::eval(
    const SelectivityVector& rows,
    EvalCtx& context,
    VectorPtr& result,
    const ExprSet* parentExprSet) {
  if (supportsFlatNoNullsFastPath_ && context.throwOnError() &&
      context.inputFlatNoNulls() && rows.countSelected() < 1'000) {
    evalFlatNoNulls(rows, context, result, parentExprSet);
    checkResultInternalState(result);
    return;
  }

  // Make sure to include current expression in the error message in case of an
  // exception.
  ExprExceptionContext exprExceptionContext{this, context.row(), parentExprSet};
  ExceptionContextSetter exceptionContext(
      {parentExprSet ? onTopLevelException : onException,
       parentExprSet ? (void*)&exprExceptionContext : this});

  if (!rows.hasSelections()) {
    checkOrSetEmptyResult(type(), context.pool(), result);
    checkResultInternalState(result);
    return;
  }

  // Check if there are any IFs, ANDs or ORs. These expressions are special
  // because not all of their sub-expressions get evaluated on all the rows
  // all the time. Therefore, we should delay loading lazy vectors until we
  // know the minimum subset of rows needed to be loaded.
  //
  // If there is only one field, load it unconditionally. The very first IF,
  // AND or OR will have to load it anyway. Pre-loading enables peeling of
  // encodings at a higher level in the expression tree and avoids repeated
  // peeling and wrapping in the sub-nodes.
  //
  // Also load fields referenced by shared sub expressions to ensure that if
  // there is an encoding on the loaded vector, then it is always peeled before
  // evaluating sub-expression. Otherwise, the first call to
  // evaluateSharedSubexpr might pass rows before peeling and the next one pass
  // rows after peeling.
  //
  // Finally, for non-null propagating expressions, load multiply referenced
  // inputs unconditionally as it is hard to keep track of the superset of rows
  // that would end up being evaluated among all its children (and hence need to
  // be loaded). This is because any of the children might have null propagating
  // expressions that end up operating on a reduced set of rows. So, one sub
  // tree might need only a subset, whereas other might need a different subset.
  //
  // TODO: Re-work the logic of deciding when to load which field.
  if (!hasConditionals_ || distinctFields_.size() == 1 ||
      shouldEvaluateSharedSubexp()) {
    // Load lazy vectors if any.
    for (auto* field : distinctFields_) {
      context.ensureFieldLoaded(field->index(context), rows);
    }
  } else if (
      !propagatesNulls_ && !evaluatesArgumentsOnNonIncreasingSelection()) {
    // Load multiply-referenced fields at common parent expr with "rows".  Delay
    // loading fields that are not in multiplyReferencedFields_.  In case
    // evaluatesArgumentsOnNonIncreasingSelection() is true, this is delayed
    // until we process the inputs of ConjunctExpr.
    for (const auto& field : multiplyReferencedFields_) {
      context.ensureFieldLoaded(field->index(context), rows);
    }
  }

  if (inputs_.empty()) {
    evalAll(rows, context, result);
    checkResultInternalState(result);
    return;
  }

  evalEncodings(rows, context, result);
  checkResultInternalState(result);
}

template <typename TEval>
void Expr::evaluateSharedSubexpr(
    const SelectivityVector& rows,
    EvalCtx& context,
    VectorPtr& result,
    TEval eval) {
  // Captures the inputs referenced by distinctFields_.
  std::vector<const BaseVector*> expressionInputFields;
  for (auto* field : distinctFields_) {
    expressionInputFields.push_back(
        context.getField(field->index(context)).get());
  }

  // Find the cached results for the same inputs, or create an entry if one
  // doesn't exist.
  auto sharedSubexprResultsIter =
      sharedSubexprResults_.find(expressionInputFields);
  if (sharedSubexprResultsIter == sharedSubexprResults_.end()) {
    auto maxSharedSubexprResultsCached = context.execCtx()
                                             ->queryCtx()
                                             ->queryConfig()
                                             .maxSharedSubexprResultsCached();
    if (sharedSubexprResults_.size() < maxSharedSubexprResultsCached) {
      // If we have room left in the cache, add it.
      sharedSubexprResultsIter =
          sharedSubexprResults_
              .insert(
                  std::pair(std::move(expressionInputFields), SharedResults()))
              .first;
    } else {
      // Otherwise, simply evaluate it and return without caching the results.
      eval(rows, context, result);

      return;
    }
  }

  auto& [sharedSubexprRows, sharedSubexprValues] =
      sharedSubexprResultsIter->second;

  if (sharedSubexprValues == nullptr) {
    eval(rows, context, result);

    if (!sharedSubexprRows) {
      sharedSubexprRows = context.execCtx()->getSelectivityVector(rows.end());
    }

    *sharedSubexprRows = rows;
    if (context.errors()) {
      // Clear the rows which failed to compute.
      context.deselectErrors(*sharedSubexprRows);
      if (!sharedSubexprRows->hasSelections()) {
        // Do not store a reference to 'result' if we cannot use any rows from
        // it.
        return;
      }
    }

    sharedSubexprValues = result;
    return;
  }

  if (rows.isSubset(*sharedSubexprRows)) {
    // We have results for all requested rows. No need to compute anything.
    context.moveOrCopyResult(sharedSubexprValues, rows, result);
    return;
  }

  // We are missing results for some or all of the requested rows. Need to
  // compute these and save for future use.

  // Identify a subset of rows that need to be computed: rows -
  // sharedSubexprRows_.
  LocalSelectivityVector missingRowsHolder(context, rows);
  auto missingRows = missingRowsHolder.get();
  missingRows->deselect(*sharedSubexprRows);
  VELOX_DCHECK(missingRows->hasSelections());

  // Fix finalSelection to avoid losing values outside missingRows.
  // Final selection of rows need to include sharedSubexprRows_, missingRows and
  // current final selection of rows if set.
  LocalSelectivityVector newFinalSelectionHolder(context, *sharedSubexprRows);
  auto newFinalSelection = newFinalSelectionHolder.get();
  newFinalSelection->select(*missingRows);
  if (!context.isFinalSelection()) {
    newFinalSelection->select(*context.finalSelection());
  }

  ScopedFinalSelectionSetter setter(
      context, newFinalSelection, true /*checkCondition*/, true /*override*/);

  eval(*missingRows, context, sharedSubexprValues);

  // Clear the rows which failed to compute.
  context.deselectErrors(*missingRows);

  sharedSubexprRows->select(*missingRows);
  context.moveOrCopyResult(sharedSubexprValues, rows, result);
}

SelectivityVector* singleRow(
    LocalSelectivityVector& holder,
    vector_size_t row) {
  auto rows = holder.get(row + 1, false);
  rows->setValid(row, true);
  rows->updateBounds();
  return rows;
}

Expr::PeelEncodingsResult Expr::peelEncodings(
    EvalCtx& context,
    ContextSaver& saver,
    const SelectivityVector& rows,
    LocalDecodedVector& localDecoded,
    LocalSelectivityVector& newRowsHolder,
    LocalSelectivityVector& finalRowsHolder) {
  if (context.wrapEncoding() == VectorEncoding::Simple::CONSTANT) {
    return Expr::PeelEncodingsResult::empty();
  }

  // Prepare the rows and vectors to peel.

  // Use finalSelection to generate peel to ensure those rows can be translated
  // and ensure consistent peeling across multiple calls to this expression if
  // its a shared subexpression.
  const auto& rowsToPeel =
      context.isFinalSelection() ? rows : *context.finalSelection();
  auto numFields = context.row()->childrenSize();
  std::vector<VectorPtr> vectorsToPeel;
  vectorsToPeel.reserve(distinctFields_.size());
  for (auto* field : distinctFields_) {
    auto fieldIndex = field->index(context);
    assert(fieldIndex >= 0 && fieldIndex < numFields);
    auto fieldVector = context.getField(fieldIndex);
    if (fieldVector->isConstantEncoding()) {
      // Make sure constant encoded fields are loaded
      fieldVector = context.ensureFieldLoaded(fieldIndex, rowsToPeel);
    }
    vectorsToPeel.push_back(fieldVector);
  }

  // Attempt peeling.
  VELOX_CHECK(!vectorsToPeel.empty());
  std::vector<VectorPtr> peeledVectors;
  auto peeledEncoding = PeeledEncoding::peel(
      vectorsToPeel, rowsToPeel, localDecoded, propagatesNulls_, peeledVectors);

  if (!peeledEncoding) {
    return Expr::PeelEncodingsResult::empty();
  }

  // Translate the relevant rows.
  SelectivityVector* newFinalSelection = nullptr;
  if (!context.isFinalSelection()) {
    newFinalSelection = peeledEncoding->translateToInnerRows(
        *context.finalSelection(), finalRowsHolder);
  }
  auto newRows = peeledEncoding->translateToInnerRows(rows, newRowsHolder);

  // Save context and set the peel, peeled fields and final selection (if
  // applicable).
  context.saveAndReset(saver, rows);
  context.setPeeledEncoding(peeledEncoding);
  if (newFinalSelection) {
    *context.mutableFinalSelection() = newFinalSelection;
  }
  DCHECK_EQ(peeledVectors.size(), distinctFields_.size());
  for (int i = 0; i < peeledVectors.size(); ++i) {
    auto fieldIndex = distinctFields_[i]->index(context);
    context.setPeeled(fieldIndex, peeledVectors[i]);
  }

  // If the expression depends on one dictionary, results are cacheable.
  bool mayCache = false;
  if (context.cacheEnabled()) {
    mayCache = distinctFields_.size() == 1 &&
        VectorEncoding::isDictionary(context.wrapEncoding()) &&
        !peeledVectors[0]->memoDisabled();
  }

  common::testutil::TestValue::adjust(
      "facebook::velox::exec::Expr::peelEncodings::mayCache", &mayCache);
  return {newRows, finalRowsHolder.get(), mayCache};
}

void Expr::evalEncodings(
    const SelectivityVector& rows,
    EvalCtx& context,
    VectorPtr& result) {
  if (deterministic_ && !skipFieldDependentOptimizations()) {
    bool hasFlat = false;
    for (auto* field : distinctFields_) {
      if (isFlat(*context.getField(field->index(context)))) {
        hasFlat = true;
        break;
      }
    }

    if (!hasFlat) {
      VectorPtr wrappedResult;
      // Attempt peeling and bound the scope of the context used for it.

      withContextSaver([&](ContextSaver& saveContext) {
        LocalSelectivityVector newRowsHolder(context);
        LocalSelectivityVector finalRowsHolder(context);
        LocalDecodedVector decodedHolder(context);
        auto peelEncodingsResult = peelEncodings(
            context,
            saveContext,
            rows,
            decodedHolder,
            newRowsHolder,
            finalRowsHolder);
        auto* newRows = peelEncodingsResult.newRows;
        if (newRows) {
          VectorPtr peeledResult;
          // peelEncodings() can potentially produce an empty selectivity
          // vector if all selected values we are waiting for are nulls. So,
          // here we check for such a case.
          if (newRows->hasSelections()) {
            if (peelEncodingsResult.mayCache) {
              evalWithMemo(*newRows, context, peeledResult);
            } else {
              evalWithNulls(*newRows, context, peeledResult);
            }
          }
          wrappedResult = context.getPeeledEncoding()->wrap(
              this->type(), context.pool(), peeledResult, rows);
        }
      });

      if (wrappedResult != nullptr) {
        context.moveOrCopyResult(wrappedResult, rows, result);
        return;
      }
    }
  }
  evalWithNulls(rows, context, result);
}

bool Expr::removeSureNulls(
    const SelectivityVector& rows,
    EvalCtx& context,
    LocalSelectivityVector& nullHolder) {
  SelectivityVector* result = nullptr;
  for (auto* field : distinctFields_) {
    VectorPtr values;
    field->evalSpecialForm(rows, context, values);

    if (isLazyNotLoaded(*values)) {
      continue;
    }

    if (values->mayHaveNulls()) {
      LocalDecodedVector decoded(context, *values, rows);
      if (auto* rawNulls = decoded->nulls(&rows)) {
        if (!result) {
          result = nullHolder.get(rows);
        }
        auto bits = result->asMutableRange().bits();
        bits::andBits(bits, rawNulls, rows.begin(), rows.end());
      }
    }
  }
  if (result) {
    result->updateBounds();
    return result->countSelected() != rows.countSelected();
  }
  return false;
}

void Expr::addNulls(
    const SelectivityVector& rows,
    const uint64_t* FOLLY_NULLABLE rawNulls,
    EvalCtx& context,
    VectorPtr& result) const {
  EvalCtx::addNulls(rows, rawNulls, context, type(), result);
}

void Expr::evalWithNulls(
    const SelectivityVector& rows,
    EvalCtx& context,
    VectorPtr& result) {
  if (!rows.hasSelections()) {
    checkOrSetEmptyResult(type(), context.pool(), result);
    return;
  }

  if (propagatesNulls_ && !skipFieldDependentOptimizations()) {
    bool mayHaveNulls = false;
    for (auto* field : distinctFields_) {
      const auto& vector = context.getField(field->index(context));
      if (isLazyNotLoaded(*vector)) {
        continue;
      }

      if (vector->mayHaveNulls()) {
        mayHaveNulls = true;
        break;
      }
    }

    if (mayHaveNulls) {
      LocalSelectivityVector nonNullHolder(context);
      if (removeSureNulls(rows, context, nonNullHolder)) {
        ScopedVarSetter noMoreNulls(context.mutableNullsPruned(), true);
        if (nonNullHolder.get()->hasSelections()) {
          evalAll(*nonNullHolder.get(), context, result);
        }
        auto rawNonNulls = nonNullHolder.get()->asRange().bits();
        addNulls(rows, rawNonNulls, context, result);
        return;
      }
    }
  }
  evalAll(rows, context, result);
}

// Optimization that attempts to cache results for inputs that are dictionary
// encoded and use the same base vector between subsequent input batches. Since
// this hold onto a reference to the base vector and the cached results, it can
// be memory intensive. Therefore in order to reduce this consumption and ensure
// it is only employed for cases where it can be useful, it only starts caching
// result after it encounters the same base at least twice.
void Expr::evalWithMemo(
    const SelectivityVector& rows,
    EvalCtx& context,
    VectorPtr& result) {
  VectorPtr base;
  distinctFields_[0]->evalSpecialForm(rows, context, base);

  if (base.get() != baseOfDictionaryRawPtr_ ||
      baseOfDictionaryWeakPtr_.expired()) {
    baseOfDictionaryRepeats_ = 0;
    baseOfDictionaryWeakPtr_ = base;
    baseOfDictionaryRawPtr_ = base.get();
    context.releaseVector(baseOfDictionary_);
    context.releaseVector(dictionaryCache_);
    evalWithNulls(rows, context, result);
    return;
  }
  ++baseOfDictionaryRepeats_;

  if (baseOfDictionaryRepeats_ == 1) {
    evalWithNulls(rows, context, result);
    baseOfDictionary_ = base;
    dictionaryCache_ = result;
    if (!cachedDictionaryIndices_) {
      cachedDictionaryIndices_ =
          context.execCtx()->getSelectivityVector(rows.end());
    }
    *cachedDictionaryIndices_ = rows;
    context.deselectErrors(*cachedDictionaryIndices_);
    return;
  }

  if (cachedDictionaryIndices_) {
    LocalSelectivityVector cachedHolder(context, rows);
    auto cached = cachedHolder.get();
    VELOX_DCHECK(cached != nullptr);
    cached->intersect(*cachedDictionaryIndices_);
    if (cached->hasSelections()) {
      context.ensureWritable(rows, type(), result);
      result->copy(dictionaryCache_.get(), *cached, nullptr);
    }
  }
  LocalSelectivityVector uncachedHolder(context, rows);
  auto uncached = uncachedHolder.get();
  VELOX_DCHECK(uncached != nullptr);
  if (cachedDictionaryIndices_) {
    uncached->deselect(*cachedDictionaryIndices_);
  }
  if (uncached->hasSelections()) {
    // Fix finalSelection at "rows" if uncached rows is a strict subset to
    // avoid losing values not in uncached rows that were copied earlier into
    // "result" from the cached rows.
    ScopedFinalSelectionSetter scopedFinalSelectionSetter(
        context, &rows, uncached->countSelected() < rows.countSelected());

    evalWithNulls(*uncached, context, result);
    context.deselectErrors(*uncached);
    context.exprSet()->addToMemo(this);
    auto newCacheSize = uncached->end();

    // dictionaryCache_ is valid only for cachedDictionaryIndices_. Hence, a
    // safe call to BaseVector::ensureWritable must include all the rows not
    // covered by cachedDictionaryIndices_. If BaseVector::ensureWritable is
    // called only for a subset of rows not covered by
    // cachedDictionaryIndices_, it will attempt to copy rows that are not
    // valid leading to a crash.
    LocalSelectivityVector allUncached(context, dictionaryCache_->size());
    allUncached.get()->setAll();
    allUncached.get()->deselect(*cachedDictionaryIndices_);
    context.ensureWritable(*allUncached.get(), type(), dictionaryCache_);

    if (cachedDictionaryIndices_->size() < newCacheSize) {
      cachedDictionaryIndices_->resize(newCacheSize, false);
    }

    cachedDictionaryIndices_->select(*uncached);

    // Resize the dictionaryCache_ to accommodate all the necessary rows.
    if (dictionaryCache_->size() < uncached->end()) {
      dictionaryCache_->resize(uncached->end());
    }
    dictionaryCache_->copy(result.get(), *uncached, nullptr);
  }
  context.releaseVector(base);
}

void Expr::setAllNulls(
    const SelectivityVector& rows,
    EvalCtx& context,
    VectorPtr& result) const {
  if (result) {
    BaseVector::ensureWritable(rows, type(), context.pool(), result);
    LocalSelectivityVector notNulls(context, rows.end());
    notNulls.get()->setAll();
    notNulls.get()->deselect(rows);
    result->addNulls(notNulls.get()->asRange().bits(), rows);
    return;
  }
  result = BaseVector::createNullConstant(type(), rows.end(), context.pool());
}

namespace {
void computeIsAsciiForInputs(
    const VectorFunction* vectorFunction,
    const std::vector<VectorPtr>& inputValues,
    const SelectivityVector& rows) {
  std::vector<size_t> indices;
  if (vectorFunction->ensureStringEncodingSetAtAllInputs()) {
    for (auto i = 0; i < inputValues.size(); i++) {
      indices.push_back(i);
    }
  }
  for (auto& index : vectorFunction->ensureStringEncodingSetAt()) {
    indices.push_back(index);
  }

  // Compute string encoding for input vectors at indicies.
  for (auto& index : indices) {
    // Some arguments are optional and hence may not exist. And some
    // functions operate on dynamic types, but we only scan them when the
    // type is string.
    if (index < inputValues.size() &&
        inputValues[index]->type()->kind() == TypeKind::VARCHAR) {
      auto* vector =
          inputValues[index]->template as<SimpleVector<StringView>>();

      VELOX_CHECK(vector, inputValues[index]->toString());
      vector->computeAndSetIsAscii(rows);
    }
  }
}

/// Computes asciiness on specified inputs for propagation.
std::optional<bool> computeIsAsciiForResult(
    const VectorFunction* vectorFunction,
    const std::vector<VectorPtr>& inputValues,
    const SelectivityVector& rows) {
  std::vector<size_t> indices;
  if (vectorFunction->propagateStringEncodingFromAllInputs()) {
    for (auto i = 0; i < inputValues.size(); i++) {
      indices.push_back(i);
    }
  } else if (vectorFunction->propagateStringEncodingFrom().has_value()) {
    indices = vectorFunction->propagateStringEncodingFrom().value();
  }

  if (indices.empty()) {
    return std::nullopt;
  }

  // Return false if at least one input is not all ASCII.
  // Return true if all inputs are all ASCII.
  // Return unknown otherwise.
  bool isAsciiSet = true;
  for (auto& index : indices) {
    if (index < inputValues.size() &&
        inputValues[index]->type()->kind() == TypeKind::VARCHAR) {
      auto* vector =
          inputValues[index]->template as<SimpleVector<StringView>>();
      auto isAscii = vector->isAscii(rows);
      if (!isAscii.has_value()) {
        isAsciiSet = false;
      } else if (!isAscii.value()) {
        return false;
      }
    }
  }

  return isAsciiSet ? std::optional(true) : std::nullopt;
}

inline bool isPeelable(VectorEncoding::Simple encoding) {
  switch (encoding) {
    case VectorEncoding::Simple::CONSTANT:
    case VectorEncoding::Simple::DICTIONARY:
      return true;
    default:
      return false;
  }
}

} // namespace

void Expr::evalAll(
    const SelectivityVector& rows,
    EvalCtx& context,
    VectorPtr& result) {
  if (!rows.hasSelections()) {
    checkOrSetEmptyResult(type(), context.pool(), result);
    return;
  }

  if (shouldEvaluateSharedSubexp()) {
    evaluateSharedSubexpr(
        rows,
        context,
        result,
        [&](const SelectivityVector& rows,
            EvalCtx& context,
            VectorPtr& result) { evalAllImpl(rows, context, result); });
  } else {
    evalAllImpl(rows, context, result);
  }
}

bool Expr::throwArgumentErrors(const EvalCtx& context) const {
  bool defaultNulls = vectorFunction_->isDefaultNullBehavior();
  return context.throwOnError() &&
      (!defaultNulls ||
       (supportsFlatNoNullsFastPath() && context.inputFlatNoNulls()));
}

void Expr::evalAllImpl(
    const SelectivityVector& rows,
    EvalCtx& context,
    VectorPtr& result) {
  VELOX_DCHECK(rows.hasSelections());

  if (isSpecialForm()) {
    evalSpecialFormWithStats(rows, context, result);
    return;
  }
  bool tryPeelArgs = deterministic_ ? true : false;
  bool defaultNulls = vectorFunction_->isDefaultNullBehavior();

  // Tracks what subset of rows shall un-evaluated inputs and current expression
  // evaluates. Initially points to rows.
  MutableRemainingRows remainingRows(rows, context);
  if (defaultNulls) {
    if (!evalArgsDefaultNulls(
            remainingRows,
            [&](auto i) {
              inputs_[i]->eval(remainingRows.rows(), context, inputValues_[i]);
              tryPeelArgs =
                  tryPeelArgs && isPeelable(inputValues_[i]->encoding());
            },
            context,
            result)) {
      return;
    }
  } else {
    if (!evalArgsWithNulls(
            remainingRows,
            [&](auto i) {
              inputs_[i]->eval(remainingRows.rows(), context, inputValues_[i]);
              tryPeelArgs =
                  tryPeelArgs && isPeelable(inputValues_[i]->encoding());
            },
            context,
            result)) {
      return;
    }
  }

  if (!tryPeelArgs ||
      !applyFunctionWithPeeling(remainingRows.rows(), context, result)) {
    applyFunction(remainingRows.rows(), context, result);
  }

  // Write non-selected rows in remainingRows as nulls in the result if some
  // rows have been skipped.
  if (remainingRows.hasChanged()) {
    addNulls(rows, remainingRows.rows().asRange().bits(), context, result);
  }
  releaseInputValues(context);
}

bool Expr::applyFunctionWithPeeling(
    const SelectivityVector& applyRows,
    EvalCtx& context,
    VectorPtr& result) {
  LocalDecodedVector localDecoded(context);
  LocalSelectivityVector newRowsHolder(context);
  // Attempt peeling.
  std::vector<VectorPtr> peeledVectors;
  auto peeledEncoding = PeeledEncoding::peel(
      inputValues_,
      applyRows,
      localDecoded,
      vectorFunction_->isDefaultNullBehavior(),
      peeledVectors);
  if (!peeledEncoding) {
    return false;
  }
  inputValues_ = std::move(peeledVectors);
  peeledVectors.clear();

  // Translate the relevant rows.
  // Note: We do not need to translate final selection since at this stage those
  // rows are not used but isFinalSelection() is only used to check whether
  // pre-existing rows need to be preserved.
  auto newRows = peeledEncoding->translateToInnerRows(applyRows, newRowsHolder);

  withContextSaver([&](ContextSaver& saver) {
    // Save context and set the peel.
    context.saveAndReset(saver, applyRows);
    context.setPeeledEncoding(peeledEncoding);

    // Apply the function.
    VectorPtr peeledResult;
    applyFunction(*newRows, context, peeledResult);
    VectorPtr wrappedResult = context.getPeeledEncoding()->wrap(
        this->type(), context.pool(), peeledResult, applyRows);
    context.moveOrCopyResult(wrappedResult, applyRows, result);

    // Recycle peeledResult if it's not owned by the result vector. Examples of
    // when this can happen is when the result is a primitive constant vector,
    // or when moveOrCopyResult copies wrappedResult content.
    context.releaseVector(peeledResult);
  });

  return true;
}

void Expr::applyFunction(
    const SelectivityVector& rows,
    EvalCtx& context,
    VectorPtr& result) {
  stats_.numProcessedVectors += 1;
  stats_.numProcessedRows += rows.countSelected();
  auto timer = cpuWallTimer();

  computeIsAsciiForInputs(vectorFunction_.get(), inputValues_, rows);
  auto isAscii = type()->isVarchar()
      ? computeIsAsciiForResult(vectorFunction_.get(), inputValues_, rows)
      : std::nullopt;

  try {
    vectorFunction_->apply(rows, inputValues_, type(), context, result);
  } catch (const VeloxException&) {
    throw;
  } catch (const std::exception& e) {
    VELOX_USER_FAIL(e.what());
  }

  if (!result) {
    MutableRemainingRows remainingRows(rows, context);

    // If there are rows with no result and no exception this is a bug in the
    // function implementation.
    if (remainingRows.deselectErrors()) {
      try {
        // This isn't performant, but it gives us the relevant context and
        // should only apply when the UDF is buggy (hopefully rarely).
        VELOX_USER_FAIL(
            "Function neither returned results nor threw exception.");
      } catch (const std::exception&) {
        context.setErrors(remainingRows.rows(), std::current_exception());
      }
    }

    // Since result was empty, and either the function set errors for every
    // row or we did above, set it to be all NULL.
    result = BaseVector::createNullConstant(type(), rows.end(), context.pool());
  }

  if (isAscii.has_value()) {
    result->asUnchecked<SimpleVector<StringView>>()->setIsAscii(
        isAscii.value(), rows);
  }
}

void Expr::evalSpecialFormWithStats(
    const SelectivityVector& rows,
    EvalCtx& context,
    VectorPtr& result) {
  stats_.numProcessedVectors += 1;
  stats_.numProcessedRows += rows.countSelected();
  auto timer = cpuWallTimer();

  evalSpecialForm(rows, context, result);
}

namespace {
void printExprTree(
    const exec::Expr& expr,
    const std::string& indent,
    bool withStats,
    std::stringstream& out,
    std::unordered_map<const exec::Expr*, uint32_t>& uniqueExprs) {
  auto it = uniqueExprs.find(&expr);
  if (it != uniqueExprs.end()) {
    // Common sub-expression. Print the full expression, but skip the stats.
    // Add ID of the expression it duplicates.
    out << indent << expr.toString(true) << " -> " << expr.type()->toString();
    out << " [CSE #" << it->second << "]" << std::endl;
    return;
  }

  uint32_t id = uniqueExprs.size() + 1;
  uniqueExprs.insert({&expr, id});

  const auto& stats = expr.stats();
  out << indent << expr.toString(false);
  if (withStats) {
    out << " [cpu time: " << succinctNanos(stats.timing.cpuNanos)
        << ", rows: " << stats.numProcessedRows
        << ", batches: " << stats.numProcessedVectors << "]";
  }
  out << " -> " << expr.type()->toString() << " [#" << id << "]" << std::endl;

  auto newIndent = indent + "   ";
  for (const auto& input : expr.inputs()) {
    printExprTree(*input, newIndent, withStats, out, uniqueExprs);
  }
}
} // namespace

std::string Expr::toString(bool recursive) const {
  if (recursive) {
    std::stringstream out;
    out << name_;
    appendInputs(out);
    return out.str();
  }

  return name_;
}

std::string Expr::toSql(std::vector<VectorPtr>* complexConstants) const {
  std::stringstream out;
  out << "\"" << name_ << "\"";
  appendInputsSql(out, complexConstants);
  return out.str();
}

void Expr::appendInputs(std::stringstream& stream) const {
  if (!inputs_.empty()) {
    stream << "(";
    for (auto i = 0; i < inputs_.size(); ++i) {
      if (i > 0) {
        stream << ", ";
      }
      stream << inputs_[i]->toString();
    }
    stream << ")";
  }
}

void Expr::appendInputsSql(
    std::stringstream& stream,
    std::vector<VectorPtr>* complexConstants) const {
  if (!inputs_.empty()) {
    stream << "(";
    for (auto i = 0; i < inputs_.size(); ++i) {
      if (i > 0) {
        stream << ", ";
      }
      stream << inputs_[i]->toSql(complexConstants);
    }
    stream << ")";
  } else if (vectorFunction_ != nullptr) {
    // Function with no inputs.
    stream << "()";
  }
}

bool Expr::isConstant() const {
  if (!isDeterministic()) {
    return false;
  }
  for (auto& input : inputs_) {
    if (!input->is<ConstantExpr>()) {
      return false;
    }
  }
  return true;
}

namespace {

common::Subfield extractSubfield(
    const Expr* expr,
    const folly::F14FastMap<std::string, int32_t>& shadowedNames) {
  std::vector<std::unique_ptr<common::Subfield::PathElement>> path;
  for (;;) {
    if (auto* ref = expr->as<FieldReference>()) {
      const auto& name = ref->name();
      // When the field name is empty string, it typically means that the field
      // name was not set in the parent type.
      if (name == "") {
        expr = expr->inputs()[0].get();
        continue;
      }
      path.push_back(std::make_unique<common::Subfield::NestedField>(name));
      if (!ref->inputs().empty()) {
        expr = ref->inputs()[0].get();
        continue;
      }
      if (shadowedNames.count(name) > 0) {
        return {};
      }
      std::reverse(path.begin(), path.end());
      return common::Subfield(std::move(path));
    }
    if (!expr->vectorFunction()) {
      return {};
    }
    auto* subscript =
        dynamic_cast<const Subscript*>(expr->vectorFunction().get());
    if (!subscript || !subscript->canPushdown()) {
      return {};
    }
    auto* index = expr->inputs()[1]->as<ConstantExpr>();
    if (!index) {
      return {};
    }
    switch (index->value()->typeKind()) {
      case TypeKind::TINYINT:
        path.push_back(std::make_unique<common::Subfield::LongSubscript>(
            index->value()->as<ConstantVector<int8_t>>()->value()));
        break;
      case TypeKind::SMALLINT:
        path.push_back(std::make_unique<common::Subfield::LongSubscript>(
            index->value()->as<ConstantVector<int16_t>>()->value()));
        break;
      case TypeKind::INTEGER:
        path.push_back(std::make_unique<common::Subfield::LongSubscript>(
            index->value()->as<ConstantVector<int32_t>>()->value()));
        break;
      case TypeKind::BIGINT:
        path.push_back(std::make_unique<common::Subfield::LongSubscript>(
            index->value()->as<ConstantVector<int64_t>>()->value()));
        break;
      case TypeKind::VARCHAR:
        path.push_back(std::make_unique<common::Subfield::StringSubscript>(
            index->value()->as<ConstantVector<StringView>>()->value()));
        break;
      default:
        return {};
    }
    expr = expr->inputs()[0].get();
  }
}

} // namespace

void Expr::extractSubfieldsImpl(
    folly::F14FastMap<std::string, int32_t>* shadowedNames,
    std::vector<common::Subfield>* subfields) const {
  auto subfield = extractSubfield(this, *shadowedNames);
  if (subfield.valid()) {
    subfields->push_back(std::move(subfield));
    return;
  }
  for (auto& input : inputs_) {
    input->extractSubfieldsImpl(shadowedNames, subfields);
  }
}

std::vector<common::Subfield> Expr::extractSubfields() const {
  folly::F14FastMap<std::string, int32_t> shadowedNames;
  std::vector<common::Subfield> subfields;
  extractSubfieldsImpl(&shadowedNames, &subfields);
  return subfields;
}

ExprSet::ExprSet(
    const std::vector<core::TypedExprPtr>& sources,
    core::ExecCtx* execCtx,
    bool enableConstantFolding)
    : execCtx_(execCtx) {
  exprs_ = compileExpressions(sources, execCtx, this, enableConstantFolding);
  std::vector<FieldReference*> allDistinctFields;
  for (auto& expr : exprs_) {
    Expr::mergeFields(
        distinctFields_, multiplyReferencedFields_, expr->distinctFields());
  }
}

namespace {
void addStats(
    const exec::Expr& expr,
    std::unordered_map<std::string, exec::ExprStats>& stats,
    std::unordered_set<const exec::Expr*>& uniqueExprs) {
  auto it = uniqueExprs.find(&expr);
  if (it != uniqueExprs.end()) {
    // Common sub-expression. Skip to avoid double counting.
    return;
  }

  uniqueExprs.insert(&expr);

  // Do not aggregate empty stats.
  if (expr.stats().numProcessedRows) {
    stats[expr.name()].add(expr.stats());
  }

  for (const auto& input : expr.inputs()) {
    addStats(*input, stats, uniqueExprs);
  }
}

std::string makeUuid() {
  return boost::lexical_cast<std::string>(boost::uuids::random_generator()());
}
} // namespace

std::unordered_map<std::string, exec::ExprStats> ExprSet::stats() const {
  std::unordered_map<std::string, exec::ExprStats> stats;
  std::unordered_set<const exec::Expr*> uniqueExprs;
  for (const auto& expr : exprs()) {
    addStats(*expr, stats, uniqueExprs);
  }

  return stats;
}

ExprSet::~ExprSet() {
  exprSetListeners().withRLock([&](auto& listeners) {
    if (!listeners.empty()) {
      auto exprStats = stats();

      std::vector<std::string> sqls;
      for (const auto& expr : exprs()) {
        try {
          sqls.emplace_back(expr->toSql());
        } catch (const std::exception& e) {
          LOG_EVERY_N(WARNING, 100) << "Failed to generate SQL: " << e.what();
          sqls.emplace_back("<failed to generate>");
        }
      }

      auto uuid = makeUuid();
      for (const auto& listener : listeners) {
        listener->onCompletion(
            uuid, {exprStats, sqls, execCtx()->queryCtx()->queryId()});
      }
    }
  });
}

std::string ExprSet::toString(bool compact) const {
  std::unordered_map<const exec::Expr*, uint32_t> uniqueExprs;
  std::stringstream out;
  for (auto i = 0; i < exprs_.size(); ++i) {
    if (i > 0) {
      out << std::endl;
    }
    if (compact) {
      out << exprs_[i]->toString(true /*recursive*/);
    } else {
      printExprTree(*exprs_[i], "", false /*withStats*/, out, uniqueExprs);
    }
  }
  return out.str();
}

namespace {
void printInputAndExprs(
    const BaseVector* vector,
    const std::vector<std::shared_ptr<Expr>>& exprs) {
  const char* basePath =
      FLAGS_velox_save_input_on_expression_any_failure_path.c_str();
  if (strlen(basePath) == 0) {
    basePath = FLAGS_velox_save_input_on_expression_system_failure_path.c_str();
  }
  if (strlen(basePath) == 0) {
    return;
  }
  // Persist vector to disk
  try {
    auto dataPathOpt = common::generateTempFilePath(basePath, "vector");
    if (!dataPathOpt.has_value()) {
      return;
    }
    saveVectorToFile(vector, dataPathOpt.value().c_str());
    LOG(ERROR) << "Input vector data: " << dataPathOpt.value();
  } catch (std::exception& e) {
    LOG(ERROR) << "Error serializing Input vector data: " << e.what();
  }

  try {
    std::stringstream allSql;
    for (int i = 0; i < exprs.size(); ++i) {
      if (i > 0) {
        allSql << ", ";
      }
      allSql << exprs[i]->toSql();
    }
    auto sqlPathOpt = common::generateTempFilePath(basePath, "allExprSql");
    if (!sqlPathOpt.has_value()) {
      return;
    }
    saveStringToFile(allSql.str(), sqlPathOpt.value().c_str());
    LOG(ERROR) << "SQL expression: " << sqlPathOpt.value();
  } catch (std::exception& e) {
    LOG(ERROR) << "Error serializing SQL expression: " << e.what();
  }
}
} // namespace

void ExprSet::eval(
    int32_t begin,
    int32_t end,
    bool initialize,
    const SelectivityVector& rows,
    EvalCtx& context,
    std::vector<VectorPtr>& result) {
  result.resize(exprs_.size());
  if (initialize) {
    clearSharedSubexprs();
  }

  // Make sure LazyVectors, referenced by multiple expressions, are loaded
  // for all the "rows".
  //
  // Consider projection with 2 expressions: f(a) AND g(b), h(b)
  // If b is a LazyVector and f(a) AND g(b) expression is evaluated first, it
  // will load b only for rows where f(a) is true. However, h(b) projection
  // needs all rows for "b".
  for (const auto& field : multiplyReferencedFields_) {
    context.ensureFieldLoaded(field->index(context), rows);
  }

  if (FLAGS_velox_experimental_save_input_on_fatal_signal) {
    auto other = process::GetThreadDebugInfo();
    process::ThreadDebugInfo debugInfo;
    if (other) {
      debugInfo.queryId_ = other->queryId_;
      debugInfo.taskId_ = other->taskId_;
    }
    debugInfo.callback_ = [&]() {
      printInputAndExprs(context.row(), this->exprs());
    };
    process::ScopedThreadDebugInfo scopedDebugInfo(debugInfo);

    for (int32_t i = begin; i < end; ++i) {
      exprs_[i]->eval(rows, context, result[i], this);
    }
    return;
  }

  for (int32_t i = begin; i < end; ++i) {
    exprs_[i]->eval(rows, context, result[i], this);
  }
}

void ExprSet::clearSharedSubexprs() {
  for (auto& expr : toReset_) {
    expr->reset();
  }
}

void ExprSet::clear() {
  clearSharedSubexprs();
  for (auto* memo : memoizingExprs_) {
    memo->clearMemo();
  }
  distinctFields_.clear();
  multiplyReferencedFields_.clear();
}

void ExprSetSimplified::eval(
    int32_t begin,
    int32_t end,
    bool initialize,
    const SelectivityVector& rows,
    EvalCtx& context,
    std::vector<VectorPtr>& result) {
  result.resize(exprs_.size());
  if (initialize) {
    clearSharedSubexprs();
  }
  for (int32_t i = begin; i < end; ++i) {
    exprs_[i]->evalSimplified(rows, context, result[i]);
  }
}

std::unique_ptr<ExprSet> makeExprSetFromFlag(
    std::vector<core::TypedExprPtr>&& source,
    core::ExecCtx* execCtx) {
  if (execCtx->queryCtx()->queryConfig().exprEvalSimplified() ||
      FLAGS_force_eval_simplified) {
    return std::make_unique<ExprSetSimplified>(std::move(source), execCtx);
  }
  return std::make_unique<ExprSet>(std::move(source), execCtx);
}

std::string printExprWithStats(const exec::ExprSet& exprSet) {
  const auto& exprs = exprSet.exprs();
  std::unordered_map<const exec::Expr*, uint32_t> uniqueExprs;
  std::stringstream out;
  for (auto i = 0; i < exprs.size(); ++i) {
    if (i > 0) {
      out << std::endl;
    }
    printExprTree(*exprs[i], "", true /*withStats*/, out, uniqueExprs);
  }
  return out.str();
}

void SimpleExpressionEvaluator::evaluate(
    exec::ExprSet* exprSet,
    const SelectivityVector& rows,
    const RowVector& input,
    VectorPtr& result) {
  EvalCtx context(ensureExecCtx(), exprSet, &input);
  std::vector<VectorPtr> results = {result};
  exprSet->eval(0, 1, true, rows, context, results);
  result = results[0];
}

core::ExecCtx* SimpleExpressionEvaluator::ensureExecCtx() {
  if (!execCtx_) {
    execCtx_ = std::make_unique<core::ExecCtx>(pool_, queryCtx_);
  }
  return execCtx_.get();
}

} // namespace facebook::velox::exec

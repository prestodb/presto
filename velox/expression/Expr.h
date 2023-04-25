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

#include <vector>

#include <folly/container/F14Map.h>

#include "velox/common/time/CpuWallTimer.h"
#include "velox/core/Expressions.h"
#include "velox/expression/DecodedArgs.h"
#include "velox/expression/EvalCtx.h"
#include "velox/vector/SimpleVector.h"

/// GFlag used to enable saving input vector and expression SQL on disk in case
/// of any (user or system) error during expression evaluation. The value
/// specifies a path to a directory where the vectors will be saved. That
/// directory must exist and be writable.
DECLARE_string(velox_save_input_on_expression_any_failure_path);

/// GFlag used to enable saving input vector and expression SQL on disk in case
/// of a system error during expression evaluation. The value specifies a path
/// to a directory where the vectors will be saved. That directory must exist
/// and be writable. This flag is ignored if
/// velox_save_input_on_expression_any_failure_path flag is set.
DECLARE_string(velox_save_input_on_expression_system_failure_path);

namespace facebook::velox::exec {

class ExprSet;
class FieldReference;
class VectorFunction;

struct ExprStats {
  /// Requires QueryConfig.exprTrackCpuUsage() to be 'true'.
  CpuWallTiming timing;

  /// Number of processed rows.
  uint64_t numProcessedRows{0};

  /// Number of processed vectors / batches. Allows to compute average batch
  /// size.
  uint64_t numProcessedVectors{0};

  void add(const ExprStats& other) {
    timing.add(other.timing);
    numProcessedRows += other.numProcessedRows;
    numProcessedVectors += other.numProcessedVectors;
  }

  std::string toString() const {
    return fmt::format(
        "timing: {}, numProcessedRows: {}, numProcessedVectors: {}",
        timing.toString(),
        numProcessedRows,
        numProcessedVectors);
  }
};

/// Maintains a set of rows for evaluation and removes rows with
/// nulls or errors as needed. Helps to avoid copying SelectivityVector in cases
/// when evaluation doesn't encounter nulls or errors.
class MutableRemainingRows {
 public:
  /// @param rows Initial set of rows.
  MutableRemainingRows(const SelectivityVector& rows, EvalCtx& context)
      : context_{context},
        originalRows_{&rows},
        rows_{&rows},
        mutableRowsHolder_{context} {}

  const SelectivityVector& originalRows() const {
    return *originalRows_;
  }

  /// @return current set of rows which may be different from the initial set if
  /// deselectNulls or deselectErrors were called.
  const SelectivityVector& rows() const {
    return *rows_;
  }

  SelectivityVector& mutableRows() {
    ensureMutableRemainingRows();
    return *mutableRows_;
  }

  /// Removes rows with nulls.
  /// @return true if at least one row remains.
  bool deselectNulls(const uint64_t* rawNulls) {
    ensureMutableRemainingRows();
    mutableRows_->deselectNulls(rawNulls, rows_->begin(), rows_->end());

    return mutableRows_->hasSelections();
  }

  /// Removes rows with errors (as recorded in EvalCtx::errors).
  /// @return true if at least one row remains.
  bool deselectErrors() {
    ensureMutableRemainingRows();
    context_.deselectErrors(*mutableRows_);

    return mutableRows_->hasSelections();
  }

  /// @return true if current set of rows might be different from the original
  /// set of rows, which may happen if deselectNull() or deselectErrors() were
  /// called. May return 'true' even if current set of rows is the same as
  /// original set. Returns 'false' only if current set of rows is for sure the
  /// same as original.
  bool mayHaveChanged() const {
    return mutableRows_ != nullptr && !mutableRows_->isAllSelected();
  }

 private:
  void ensureMutableRemainingRows() {
    if (mutableRows_ == nullptr) {
      mutableRows_ = mutableRowsHolder_.get(*rows_);
      rows_ = mutableRows_;
    }
  }

  EvalCtx& context_;
  const SelectivityVector* const originalRows_;

  const SelectivityVector* rows_;

  SelectivityVector* mutableRows_{nullptr};
  LocalSelectivityVector mutableRowsHolder_;
};

// An executable expression.
class Expr {
 public:
  Expr(
      TypePtr type,
      std::vector<std::shared_ptr<Expr>>&& inputs,
      std::string name,
      bool specialForm,
      bool supportsFlatNoNullsFastPath,
      bool trackCpuUsage)
      : type_(std::move(type)),
        inputs_(std::move(inputs)),
        name_(std::move(name)),
        vectorFunction_(nullptr),
        specialForm_{specialForm},
        supportsFlatNoNullsFastPath_{supportsFlatNoNullsFastPath},
        trackCpuUsage_{trackCpuUsage} {}

  Expr(
      TypePtr type,
      std::vector<std::shared_ptr<Expr>>&& inputs,
      std::shared_ptr<VectorFunction> vectorFunction,
      std::string name,
      bool trackCpuUsage);

  virtual ~Expr() = default;

  /// Evaluates the expression for the specified 'rows'.
  ///
  /// @param topLevel Boolean indicating whether this is a top-level expression
  /// or one of the sub-expressions. Used to setup exception context.
  void eval(
      const SelectivityVector& rows,
      EvalCtx& context,
      VectorPtr& result,
      bool topLevel = false);

  /// Evaluates the expression using fast path that assumes all inputs and
  /// intermediate results are flat or constant and have no nulls.
  ///
  /// This path doesn't peel off constant encoding and therefore may be
  /// expensive to apply to expressions that manipulate strings of complex
  /// types. It may also be expensive to apply to large batches. Hence, this
  /// path is enabled only for batch sizes less than 1'000 and expressions where
  /// all input and intermediate types are primitive and not strings.
  ///
  /// @param topLevel Boolean indicating whether this is a top-level expression
  /// or one of the sub-expressions. Used to setup exception context.
  void evalFlatNoNulls(
      const SelectivityVector& rows,
      EvalCtx& context,
      VectorPtr& result,
      bool topLevel = false);

  void evalFlatNoNullsImpl(
      const SelectivityVector& rows,
      EvalCtx& context,
      VectorPtr& result,
      bool topLevel);

  // Simplified path for expression evaluation (flattens all vectors).
  void evalSimplified(
      const SelectivityVector& rows,
      EvalCtx& context,
      VectorPtr& result);

  // Evaluates 'this', including inputs. This is defined only for
  // exprs that have custom error handling or evaluate their arguments
  // conditionally.
  virtual void evalSpecialForm(
      const SelectivityVector& /*rows*/,
      EvalCtx& /*context*/,
      VectorPtr& /*result*/) {
    VELOX_NYI();
  }

  // Allow special form expressions to overwrite and implement a simplified
  // path; fallback to the regular implementation by default.
  virtual void evalSpecialFormSimplified(
      const SelectivityVector& rows,
      EvalCtx& context,
      VectorPtr& result) {
    evalSpecialForm(rows, context, result);
  }

  virtual void computeMetadata();

  virtual void reset() {
    sharedSubexprResults_.clear();
  }

  void clearMemo() {
    baseDictionary_ = nullptr;
    dictionaryCache_ = nullptr;
    cachedDictionaryIndices_ = nullptr;
  }

  const TypePtr& type() const {
    return type_;
  }

  const std::string& name() const {
    return name_;
  }

  bool isString() const {
    return type()->kind() == TypeKind::VARCHAR;
  }

  bool isSpecialForm() const {
    return specialForm_;
  }

  virtual bool isConditional() const {
    return false;
  }

  bool isDeterministic() const {
    return deterministic_;
  }

  virtual bool isConstant() const;

  bool supportsFlatNoNullsFastPath() const {
    return supportsFlatNoNullsFastPath_;
  }

  bool isMultiplyReferenced() const {
    return isMultiplyReferenced_;
  }

  void setMultiplyReferenced() {
    isMultiplyReferenced_ = true;
  }

  // True if 'this' Expr tree is null for a null in any of the columns
  // this depends on.
  virtual bool propagatesNulls() const {
    return propagatesNulls_;
  }

  const std::vector<FieldReference*>& distinctFields() const {
    return distinctFields_;
  }

  static bool isSameFields(
      const std::vector<FieldReference*>& fields1,
      const std::vector<FieldReference*>& fields2);

  static bool isSubsetOfFields(
      const std::vector<FieldReference*>& subset,
      const std::vector<FieldReference*>& superset);

  static bool allSupportFlatNoNullsFastPath(
      const std::vector<std::shared_ptr<Expr>>& exprs);

  const std::vector<std::shared_ptr<Expr>>& inputs() const {
    return inputs_;
  }

  /// @param recursive If true, the output includes input expressions and all
  /// their inputs recursively.
  virtual std::string toString(bool recursive = true) const;

  /// Return the expression as SQL string.
  /// @param complexConstants An optional std::vector of VectorPtr to record
  /// complex constants (Array, Maps, Structs, ...) that aren't accurately
  /// expressable as sql. If not given, they will be converted to
  /// SQL-expressable simple constants.
  virtual std::string toSql(
      std::vector<VectorPtr>* FOLLY_NULLABLE complexConstants = nullptr) const;

  const ExprStats& stats() const {
    return stats_;
  }

  // Adds nulls from 'rawNulls' to positions of 'result' given by
  // 'rows'. Ensures that '*result' is writable, of sufficient size
  // and that it can take nulls. Makes a new '*result' when
  // appropriate.
  static void addNulls(
      const SelectivityVector& rows,
      const uint64_t* FOLLY_NULLABLE rawNulls,
      EvalCtx& context,
      const TypePtr& type,
      VectorPtr& result);

  void addNulls(
      const SelectivityVector& rows,
      const uint64_t* FOLLY_NULLABLE rawNulls,
      EvalCtx& context,
      VectorPtr& result);

  auto& vectorFunction() const {
    return vectorFunction_;
  }

  auto& inputValues() {
    return inputValues_;
  }

  void setAllNulls(
      const SelectivityVector& rows,
      EvalCtx& context,
      VectorPtr& result) const;

 private:
  struct PeelEncodingsResult {
    SelectivityVector* FOLLY_NULLABLE newRows;
    SelectivityVector* FOLLY_NULLABLE newFinalSelection;
    bool mayCache;

    static PeelEncodingsResult empty() {
      return {nullptr, nullptr, false};
    }
  };

  PeelEncodingsResult peelEncodings(
      EvalCtx& context,
      ScopedContextSaver& saver,
      const SelectivityVector& rows,
      LocalDecodedVector& localDecoded,
      LocalSelectivityVector& newRowsHolder,
      LocalSelectivityVector& finalRowsHolder);

  void evalEncodings(
      const SelectivityVector& rows,
      EvalCtx& context,
      VectorPtr& result);

  void evalWithMemo(
      const SelectivityVector& rows,
      EvalCtx& context,
      VectorPtr& result);

  void evalWithNulls(
      const SelectivityVector& rows,
      EvalCtx& context,
      VectorPtr& result);

  void
  evalAll(const SelectivityVector& rows, EvalCtx& context, VectorPtr& result);

  void evalAllImpl(
      const SelectivityVector& rows,
      EvalCtx& context,
      VectorPtr& result);

  // Checks 'inputValues_' for peelable wrappers (constants,
  // dictionaries etc) and applies the function of 'this' to distinct
  // values as opposed to all values. Wraps the return value into a
  // dictionary or constant so that we get the right
  // cardinality. Returns true if the function was called. Returns
  // false if no encodings could be peeled off.
  bool applyFunctionWithPeeling(
      const SelectivityVector& applyRows,
      EvalCtx& context,
      VectorPtr& result);

  // Calls the function of 'this' on arguments in
  // 'inputValues_'. Handles cases of VectorFunction and SimpleFunction.
  void applyFunction(
      const SelectivityVector& rows,
      EvalCtx& context,
      VectorPtr& result);

  // Returns true if values in 'distinctFields_' have nulls that are
  // worth skipping. If so, the rows in 'rows' with at least one sure
  // null are deselected in 'nullHolder->get()'.
  bool removeSureNulls(
      const SelectivityVector& rows,
      EvalCtx& context,
      LocalSelectivityVector& nullHolder);

  /// Returns true if this is a deterministic shared sub-expressions with at
  /// least one input (i.e. not a constant or field access expression).
  /// Evaluation of such expression is optimized by memoizing and reusing
  /// the results of prior evaluations. That logic is implemented in
  /// 'evaluateSharedSubexpr'.
  bool shouldEvaluateSharedSubexp() const {
    return deterministic_ && isMultiplyReferenced_ && !inputs_.empty();
  }

  /// Evaluate common sub-expression. Check if sharedSubexprValues_ already has
  /// values for all 'rows'. If not, compute missing values.
  template <typename TEval>
  void evaluateSharedSubexpr(
      const SelectivityVector& rows,
      EvalCtx& context,
      VectorPtr& result,
      TEval eval);

  /// Return true if errors in evaluation 'vectorFunction_' arguments should be
  /// thrown as soon as they happen. False if argument errors will be converted
  /// into a null if another argument for the same row is null.
  bool throwArgumentErrors(const EvalCtx& context) const;

  void evalSimplifiedImpl(
      const SelectivityVector& rows,
      EvalCtx& context,
      VectorPtr& result);

  void evalSpecialFormWithStats(
      const SelectivityVector& rows,
      EvalCtx& context,
      VectorPtr& result);

 protected:
  void appendInputs(std::stringstream& stream) const;

  void appendInputsSql(
      std::stringstream& stream,
      std::vector<VectorPtr>* FOLLY_NULLABLE complexConstants) const;

  /// Release 'inputValues_' back to vector pool in 'evalCtx' so they can be
  /// reused.
  void releaseInputValues(EvalCtx& evalCtx);

  // Evaluates arguments of 'this' for 'rows'. 'rows' is updated to
  // remove rows where an argument is null. If an argument gets an
  // error and a subsequent argument has a null for the same row the
  // error is masked and the row is removed from 'rows'. If
  // 'context.throwOnError()' is true, an error in arguments that is
  // not cancelled by a null will be thrown. Otherwise the errors
  // found in arguments are left in place and added to errors that may
  // have been in 'context' on entry. The rows where an argument had a
  // null or error are removed from 'rows' at before returning. If
  // 'rows' goes empty, we return false as soon as 'rows' is empty and set
  // 'result' to nulls for initial 'rows'.. 'evalArg(i)' is called for
  // evaluating the 'ith' argument.
  template <typename EvalArg>
  bool evalArgsDefaultNulls(
      MutableRemainingRows& rows,
      EvalArg evalArg,
      EvalCtx& context,
      VectorPtr& result);

  // Evaluates arguments of 'this'. A null or does not remove its row
  // from 'rows'. If 'context.throwOnError()' is false, errors are
  // accumulated in 'context' adding themselves or overwriting
  // previous errors for the same row. An error removes the
  // corresponding row from 'rows.  Returns false if 'rows' goes
  // empty and sets 'result'  to null for initial 'rows'. . 'evalArgs(i)' is
  // called for evaluating the 'ith' argument.
  template <typename EvalArg>
  bool evalArgsWithNulls(
      MutableRemainingRows& rows,
      EvalArg evalArg,
      EvalCtx& context,
      VectorPtr& result);

  /// Returns an instance of CpuWallTimer if cpu usage tracking is enabled. Null
  /// otherwise.
  std::unique_ptr<CpuWallTimer> cpuWallTimer() {
    return trackCpuUsage_ ? std::make_unique<CpuWallTimer>(stats_.timing)
                          : nullptr;
  }

  const TypePtr type_;
  const std::vector<std::shared_ptr<Expr>> inputs_;
  const std::string name_;
  const std::shared_ptr<VectorFunction> vectorFunction_;
  const bool specialForm_;
  const bool supportsFlatNoNullsFastPath_;
  const bool trackCpuUsage_;

  std::vector<VectorPtr> constantInputs_;
  std::vector<bool> inputIsConstant_;

  // TODO make the following metadata const, e.g. call computeMetadata in the
  // constructor

  // The distinct references to input columns in 'inputs_'
  // subtrees. Empty if this is the same as 'distinctFields_' of
  // parent Expr.
  std::vector<FieldReference * FOLLY_NONNULL> distinctFields_;

  // Fields referenced by multiple inputs, which is subset of distinctFields_.
  // Used to determine pre-loading of lazy vectors at current expr.
  std::unordered_set<FieldReference * FOLLY_NONNULL> multiplyReferencedFields_;

  // True if a null in any of 'distinctFields_' causes 'this' to be
  // null for the row.
  bool propagatesNulls_ = false;

  // True if this and all children are deterministic.
  bool deterministic_ = true;

  // True if this or a sub-expression is an IF, AND or OR.
  bool hasConditionals_ = false;

  bool isMultiplyReferenced_ = false;

  std::vector<VectorPtr> inputValues_;

  struct SharedResults {
    // The rows for which 'sharedSubexprValues_' has a value.
    std::unique_ptr<SelectivityVector> sharedSubexprRows_ = nullptr;
    // If multiply referenced or literal, these are the values.
    VectorPtr sharedSubexprValues_ = nullptr;
  };

  // Maps the inputs referenced by distinctFields_ captuered when
  // evaluateSharedSubexpr() is called to the cached shared results.
  std::map<std::vector<const BaseVector*>, SharedResults> sharedSubexprResults_;

  VectorPtr baseDictionary_;

  // Values computed for the base dictionary, 1:1 to the positions in
  // 'baseDictionary_'.
  VectorPtr dictionaryCache_;

  // The indices that are valid in 'dictionaryCache_'.
  std::unique_ptr<SelectivityVector> cachedDictionaryIndices_;

  // Count of executions where this is wrapped in a dictionary so that
  // results could be cached.
  int32_t numCachableInput_{0};

  // Count of times the cacheable vector is seen for a non-first time.
  int32_t numCacheableRepeats_{0};

  /// Runtime statistics. CPU time, wall time and number of processed rows.
  ExprStats stats_;
};

/// Generate a selectivity vector of a single row.
SelectivityVector* FOLLY_NONNULL
singleRow(LocalSelectivityVector& holder, vector_size_t row);

using ExprPtr = std::shared_ptr<Expr>;

// A set of Exprs that get evaluated together. Common subexpressions
// can be deduplicated. This is the top level handle on an expression
// and is used also if only one Expr is to be evaluated. TODO: Rename to
// ExprList.
// Note: Caller must ensure that lazy vectors associated with field references
// used by the expressions in this ExprSet are pre-loaded (before running
// evaluation on them) if they are also used/referenced outside the context of
// this ExprSet. If however such an association cannot be made with certainty,
// then its advisable to pre-load all lazy vectors to avoid issues associated
// with partial loading.
class ExprSet {
 public:
  explicit ExprSet(
      const std::vector<core::TypedExprPtr>& source,
      core::ExecCtx* FOLLY_NONNULL execCtx,
      bool enableConstantFolding = true);

  virtual ~ExprSet();

  // Initialize and evaluate all expressions available in this ExprSet.
  void eval(
      const SelectivityVector& rows,
      EvalCtx& ctx,
      std::vector<VectorPtr>& result) {
    eval(0, exprs_.size(), true, rows, ctx, result);
  }

  // Evaluate from expression `begin` to `end`.
  virtual void eval(
      int32_t begin,
      int32_t end,
      bool initialize,
      const SelectivityVector& rows,
      EvalCtx& ctx,
      std::vector<VectorPtr>& result);

  void clear();

  core::ExecCtx* FOLLY_NULLABLE execCtx() const {
    return execCtx_;
  }

  auto size() const {
    return exprs_.size();
  }

  const std::vector<std::shared_ptr<Expr>>& exprs() const {
    return exprs_;
  }

  const std::shared_ptr<Expr>& expr(int32_t index) const {
    return exprs_[index];
  }

  const std::vector<FieldReference*>& distinctFields() const {
    return distinctFields_;
  }

  // Flags a shared subexpression which needs to be reset (e.g. previously
  // computed results must be deleted) when evaluating new batch of data.
  void addToReset(const std::shared_ptr<Expr>& expr) {
    toReset_.emplace_back(expr);
  }

  // Flags an expression that remembers the results for a dictionary.
  void addToMemo(Expr* FOLLY_NONNULL expr) {
    memoizingExprs_.insert(expr);
  }

  /// Returns text representation of the expression set.
  /// @param compact If true, uses one-line representation for each expression.
  /// Otherwise, prints a tree of expressions one node per line.
  std::string toString(bool compact = true) const;

  /// Returns evaluation statistics as a map keyed on function or special form
  /// name. If a function or a special form occurs in the expression
  /// multiple times, the statistics will be aggregated across all calls.
  /// Statistics will be missing for functions and special forms that didn't get
  /// evaluated.
  std::unordered_map<std::string, exec::ExprStats> stats() const;

 protected:
  void clearSharedSubexprs();

  std::vector<std::shared_ptr<Expr>> exprs_;

  // The distinct references to input columns among all expressions in ExprSet.
  std::vector<FieldReference * FOLLY_NONNULL> distinctFields_;

  // Fields referenced by multiple expressions in ExprSet.
  std::unordered_set<FieldReference * FOLLY_NONNULL> multiplyReferencedFields_;

  // Distinct Exprs reachable from 'exprs_' for which reset() needs to
  // be called at the start of eval().
  std::vector<std::shared_ptr<Expr>> toReset_;

  // Exprs which retain memoized state, e.g. from running over dictionaries.
  std::unordered_set<Expr*> memoizingExprs_;
  core::ExecCtx* FOLLY_NONNULL const execCtx_;
};

class ExprSetSimplified : public ExprSet {
 public:
  ExprSetSimplified(
      const std::vector<core::TypedExprPtr>& source,
      core::ExecCtx* FOLLY_NONNULL execCtx)
      : ExprSet(source, execCtx, /*enableConstantFolding*/ false) {}

  virtual ~ExprSetSimplified() override {}

  // Initialize and evaluate all expressions available in this ExprSet.
  void eval(
      const SelectivityVector& rows,
      EvalCtx& ctx,
      std::vector<VectorPtr>& result) {
    eval(0, exprs_.size(), true, rows, ctx, result);
  }

  void eval(
      int32_t begin,
      int32_t end,
      bool initialize,
      const SelectivityVector& rows,
      EvalCtx& ctx,
      std::vector<VectorPtr>& result) override;
};

// Factory method that takes `kExprEvalSimplified` (query parameter) into
// account and instantiates the correct ExprSet class.
std::unique_ptr<ExprSet> makeExprSetFromFlag(
    std::vector<core::TypedExprPtr>&& source,
    core::ExecCtx* FOLLY_NONNULL execCtx);

/// Returns a string representation of the expression trees annotated with
/// runtime statistics. Expected to be called after calling ExprSet::eval one or
/// more times. If called before ExprSet::eval runtime statistics will be all
/// zeros.
std::string printExprWithStats(const ExprSet& exprSet);

struct ExprSetCompletionEvent {
  /// Aggregated runtime stats keyed on expression name (e.g. built-in
  /// expression like and, or, switch or a function name).
  std::unordered_map<std::string, exec::ExprStats> stats;
  /// List containing sql representation of each top level expression in ExprSet
  std::vector<std::string> sqls;
  // Query id corresponding query
  std::string queryId;
};

/// Listener invoked on ExprSet destruction.
class ExprSetListener {
 public:
  virtual ~ExprSetListener() = default;

  /// Called on ExprSet destruction. Provides runtime statistics about
  /// expression evaluation.
  /// @param uuid Universally unique identifier of the set of expressions.
  /// @param event Runtime stats.
  virtual void onCompletion(
      const std::string& uuid,
      const ExprSetCompletionEvent& event) = 0;

  /// Called when a batch of rows encounters errors processing one or more
  /// rows in a try expression to provide information about these errors. This
  /// function must neither change rows nor errors.
  /// @param rows Rows where errors exist.
  /// @param errors Error vector produced inside the try expression.
  virtual void onError(
      const SelectivityVector& rows,
      const ErrorVector& errors) = 0;
};

/// Return the ExprSetListeners having been registered.
folly::Synchronized<std::vector<std::shared_ptr<ExprSetListener>>>&
exprSetListeners();

/// Register a listener to be invoked on ExprSet destruction. Returns true if
/// listener was successfully registered, false if listener is already
/// registered.
bool registerExprSetListener(std::shared_ptr<ExprSetListener> listener);

/// Unregister a listener registered earlier. Returns true if listener was
/// unregistered successfully, false if listener was not found.
bool unregisterExprSetListener(
    const std::shared_ptr<ExprSetListener>& listener);

} // namespace facebook::velox::exec

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

namespace facebook::velox::exec {

class ExprSet;
class FieldReference;
class VectorFunction;

struct ExprStats {
  CpuWallTiming timing;
  uint64_t numProcessedRows{0};

  void add(const ExprStats& other) {
    timing.add(other.timing);
    numProcessedRows += other.numProcessedRows;
  }
};

// An executable expression.
class Expr {
 public:
  Expr(
      std::shared_ptr<const Type> type,
      std::vector<std::shared_ptr<Expr>>&& inputs,
      std::string name)
      : type_(std::move(type)),
        inputs_(std::move(inputs)),
        name_(std::move(name)),
        vectorFunction_(nullptr) {}

  Expr(
      std::shared_ptr<const Type> type,
      std::vector<std::shared_ptr<Expr>>&& inputs,
      std::shared_ptr<VectorFunction> vectorFunction,
      std::string name)
      : type_(std::move(type)),
        inputs_(std::move(inputs)),
        name_(std::move(name)),
        vectorFunction_(std::move(vectorFunction)) {}

  virtual ~Expr() = default;

  void eval(const SelectivityVector& rows, EvalCtx* context, VectorPtr* result);

  // Simplified path for expression evaluation (flattens all vectors).
  void evalSimplified(
      const SelectivityVector& rows,
      EvalCtx* context,
      VectorPtr* result);

  // Evaluates 'this', including inputs. This is defined only for
  // exprs that have custom error handling or evaluate their arguments
  // conditionally.
  virtual void evalSpecialForm(
      const SelectivityVector& /*rows*/,
      EvalCtx* /*context*/,
      VectorPtr* /*result*/) {
    VELOX_NYI();
  }

  // Allow special form expressions to overwrite and implement a simplified
  // path; fallback to the regular implementation by default.
  virtual void evalSpecialFormSimplified(
      const SelectivityVector& rows,
      EvalCtx* context,
      VectorPtr* result) {
    evalSpecialForm(rows, context, result);
  }

  virtual void computeMetadata();

  virtual void reset() {
    if (sharedSubexprRows_) {
      sharedSubexprRows_->clearAll();
    }
    if (BaseVector::isReusableFlatVector(sharedSubexprValues_)) {
      sharedSubexprValues_->resize(0);
    } else {
      sharedSubexprValues_ = nullptr;
    }
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

  virtual bool isSpecialForm() const {
    return false;
  }

  virtual bool isConditional() const {
    return false;
  }

  bool isDeterministic() const {
    return deterministic_;
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

  const std::vector<std::shared_ptr<Expr>>& inputs() const {
    return inputs_;
  }

  /// @param recursive If true, the output includes input expressions and all
  /// their inputs recursively.
  virtual std::string toString(bool recursive = true) const;

  const ExprStats& stats() const {
    return stats_;
  }

 private:
  void setAllNulls(
      const SelectivityVector& rows,
      EvalCtx* context,
      VectorPtr* result) const;

  struct PeelEncodingsResult {
    SelectivityVector* newRows;
    SelectivityVector* newFinalSelection;
    bool mayCache;

    static PeelEncodingsResult empty() {
      return {nullptr, nullptr, false};
    }
  };

  PeelEncodingsResult peelEncodings(
      EvalCtx* context,
      ContextSaver* saver,
      const SelectivityVector& rows,
      LocalDecodedVector& localDecoded,
      LocalSelectivityVector& newRowsHolder,
      LocalSelectivityVector& finalRowsHolder);

  void evalEncodings(
      const SelectivityVector& rows,
      EvalCtx* context,
      VectorPtr* result);

  void evalWithMemo(
      const SelectivityVector& rows,
      EvalCtx* context,
      VectorPtr* result);

  void evalWithNulls(
      const SelectivityVector& rows,
      EvalCtx* context,
      VectorPtr* result);

  void
  evalAll(const SelectivityVector& rows, EvalCtx* context, VectorPtr* result);

  static void setDictionaryWrapping(
      DecodedVector& decoded,
      const SelectivityVector& rows,
      BaseVector& firstWrapper,
      EvalCtx* context);

  // Adds nulls from 'rawNulls' to positions of 'result' given by
  // 'rows'. Ensures that '*result' is writable, of sufficient size
  // and that it can take nulls. Makes a new '*result' when
  // appropriate.
  void addNulls(
      const SelectivityVector& rows,
      const uint64_t* rawNulls,
      EvalCtx* context,
      VectorPtr* result);

  // Checks 'inputValues_' for peelable wrappers (constants,
  // dictionaries etc) and applies the function of 'this' to distinct
  // values as opposed to all values. Wraps the return value into a
  // dictionary or constant so that we get the right
  // cardinality. Returns true if the function was called. Returns
  // false if no encodings could be peeled off.
  bool applyFunctionWithPeeling(
      const SelectivityVector& rows,
      const SelectivityVector& applyRows,
      EvalCtx* context,
      VectorPtr* result);

  // Calls the function of 'this' on arguments in
  // 'inputValues_'. Handles cases of VectorFunction and SimpleFunction.
  void applyFunction(
      const SelectivityVector& rows,
      EvalCtx* context,
      VectorPtr* result);

  // Calls 'vectorFunction_' on values in 'inputValues_'.
  void applyVectorFunction(
      const SelectivityVector& rows,
      EvalCtx* context,
      VectorPtr* result);

  void applySingleConstArgVectorFunction(
      const SelectivityVector& rows,
      EvalCtx* context,
      VectorPtr* result);

  // Returns true if values in 'distinctFields_' have nulls that are
  // worth skipping. If so, the rows in 'rows' with at least one sure
  // null are deselected in 'nullHolder->get()'.
  bool removeSureNulls(
      const SelectivityVector& rows,
      EvalCtx* context,
      LocalSelectivityVector& nullHolder);

  // If this is a common subexpression, checks if there is a previously
  // calculated result and populates the 'result'.
  bool checkGetSharedSubexprValues(
      const SelectivityVector& rows,
      EvalCtx* context,
      VectorPtr* result);

  // If this is a common subexpression, stores the newly calculated result.
  void checkUpdateSharedSubexprValues(
      const SelectivityVector& rows,
      EvalCtx* context,
      const VectorPtr& result);

  void evalSimplifiedImpl(
      const SelectivityVector& rows,
      EvalCtx* context,
      VectorPtr* result);

 protected:
  void appendInputs(std::stringstream& stream) const;

  const std::shared_ptr<const Type> type_;
  const std::vector<std::shared_ptr<Expr>> inputs_;
  const std::string name_;
  const std::shared_ptr<VectorFunction> vectorFunction_;

  // TODO make the following metadata const, e.g. call computeMetadata in the
  // constructor

  // The distinct references to input columns in 'inputs_'
  // subtrees. Empty if this is the same as 'distinctFields_' of
  // parent Expr.
  std::vector<FieldReference*> distinctFields_;

  // True if a null in any of 'distinctFields_' causes 'this' to be
  // null for the row.
  bool propagatesNulls_ = false;

  // True if this and all children are deterministic.
  bool deterministic_ = true;

  // True if this or a sub-expression is an IF, AND or OR.
  bool hasConditionals_ = false;

  bool isMultiplyReferenced_ = false;

  std::vector<VectorPtr> inputValues_;

  // If multiply referenced or literal, these are the values.
  VectorPtr sharedSubexprValues_;

  // The rows for which 'sharedSubexprValues_' has a value.
  std::unique_ptr<SelectivityVector> sharedSubexprRows_;

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

using ExprPtr = std::shared_ptr<Expr>;

// A set of Exprs that get evaluated together. Common subexpressions
// can be deduplicated. This is the top level handle on an expression
// and is used also if only one Expr is to be evaluated. TODO: Rename to
// ExprList.
class ExprSet {
 public:
  explicit ExprSet(
      std::vector<std::shared_ptr<const core::ITypedExpr>>&& source,
      core::ExecCtx* execCtx,
      bool enableConstantFolding = true);

  virtual ~ExprSet();

  // Initialize and evaluate all expressions available in this ExprSet.
  void eval(
      const SelectivityVector& rows,
      EvalCtx* ctx,
      std::vector<VectorPtr>* result) {
    eval(0, exprs_.size(), true, rows, ctx, result);
  }

  // Evaluate from expression `begin` to `end`.
  virtual void eval(
      int32_t begin,
      int32_t end,
      bool initialize,
      const SelectivityVector& rows,
      EvalCtx* ctx,
      std::vector<VectorPtr>* result);

  void clear();

  core::ExecCtx* execCtx() const {
    return execCtx_;
  }

  const std::vector<std::shared_ptr<Expr>>& exprs() const {
    return exprs_;
  }

  const std::shared_ptr<Expr>& expr(int32_t index) const {
    return exprs_[index];
  }

  // Flags a shared subexpression which needs to be reset (e.g. previously
  // computed results must be deleted) when evaluating new batch of data.
  void addToReset(const std::shared_ptr<Expr>& expr) {
    toReset_.emplace_back(expr);
  }

  // Flags an expression that remembers the results for a dictionary.
  void addToMemo(Expr* expr) {
    memoizingExprs_.insert(expr);
  }

 protected:
  void clearSharedSubexprs();

  std::vector<std::shared_ptr<Expr>> exprs_;

  // Distinct Exprs reachable from 'exprs_' for which reset() needs to
  // be called at the start of eval().
  std::vector<std::shared_ptr<Expr>> toReset_;

  // Exprs which retain memoized state, e.g. from running over dictionaries.
  std::unordered_set<Expr*> memoizingExprs_;
  core::ExecCtx* const execCtx_;
};

class ExprSetSimplified : public ExprSet {
 public:
  ExprSetSimplified(
      std::vector<std::shared_ptr<const core::ITypedExpr>>&& source,
      core::ExecCtx* execCtx)
      : ExprSet(std::move(source), execCtx, /*enableConstantFolding*/ false) {}

  virtual ~ExprSetSimplified() override {}

  // Initialize and evaluate all expressions available in this ExprSet.
  void eval(
      const SelectivityVector& rows,
      EvalCtx* ctx,
      std::vector<VectorPtr>* result) {
    eval(0, exprs_.size(), true, rows, ctx, result);
  }

  void eval(
      int32_t begin,
      int32_t end,
      bool initialize,
      const SelectivityVector& rows,
      EvalCtx* ctx,
      std::vector<VectorPtr>* result) override;
};

// Factory method that takes `kExprEvalSimplified` (query parameter) into
// account and instantiates the correct ExprSet class.
std::unique_ptr<ExprSet> makeExprSetFromFlag(
    std::vector<std::shared_ptr<const core::ITypedExpr>>&& source,
    core::ExecCtx* execCtx);

/// Returns a string representation of the expression trees annotated with
/// runtime statistics. Expected to be called after calling ExprSet::eval one or
/// more times. If called before ExprSet::eval runtime statistics will be all
/// zeros.
std::string printExprWithStats(const ExprSet& exprSet);

struct ExprSetCompletionEvent {
  /// Aggregated runtime stats keyed on expression name (e.g. built-in
  /// expression like and, or, switch or a function name).
  std::unordered_map<std::string, exec::ExprStats> stats;
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
};

/// Register a listener to be invoked on ExprSet destruction. Returns true if
/// listener was successfully registered, false if listener is already
/// registered.
bool registerExprSetListener(std::shared_ptr<ExprSetListener> listener);

/// Unregister a listener registered earlier. Returns true if listener was
/// unregistered successfully, false if listener was not found.
bool unregisterExprSetListener(
    const std::shared_ptr<ExprSetListener>& listener);

} // namespace facebook::velox::exec

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
#include "velox/common/base/SuccinctPrinter.h"
#include "velox/core/Expressions.h"
#include "velox/expression/ConstantExpr.h"
#include "velox/expression/Expr.h"
#include "velox/expression/ExprCompiler.h"
#include "velox/expression/FieldReference.h"
#include "velox/expression/VarSetter.h"
#include "velox/expression/VectorFunction.h"
#include "velox/vector/SelectivityVector.h"
#include "velox/vector/VectorSaver.h"

DEFINE_bool(
    force_eval_simplified,
    false,
    "Whether to overwrite queryCtx and force the "
    "use of simplified expression evaluation path.");

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

void mergeFields(
    std::vector<FieldReference*>& distinctFields,
    std::unordered_set<FieldReference*>& multiplyReferencedFields_,
    const std::vector<FieldReference*>& moreFields) {
  for (auto* newField : moreFields) {
    if (isMember(distinctFields, *newField)) {
      multiplyReferencedFields_.insert(newField);
    } else {
      distinctFields.emplace_back(newField);
    }
  }
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
    if (auto constantExpr = std::dynamic_pointer_cast<ConstantExpr>(expr)) {
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

void Expr::computeMetadata() {
  // Sets propagatesNulls if all subtrees propagate nulls.
  // Sets isDeterministic to false if some subtree is non-deterministic.
  // Sets 'distinctFields_' to be the union of 'distinctFields_' of inputs.
  // If one of the inputs has the identical set of distinct fields, then
  // the input's distinct fields are set to empty.
  if (isSpecialForm()) {
    // 'propagatesNulls_' will be adjusted after inputs are processed.
    propagatesNulls_ = true;
    deterministic_ = true;
  } else if (vectorFunction_) {
    propagatesNulls_ = vectorFunction_->isDefaultNullBehavior();
    deterministic_ = vectorFunction_->isDeterministic();
  }

  for (auto& input : inputs_) {
    input->computeMetadata();
    deterministic_ &= input->deterministic_;
    propagatesNulls_ &= input->propagatesNulls_;
    mergeFields(
        distinctFields_, multiplyReferencedFields_, input->distinctFields_);
  }
  if (isSpecialForm()) {
    propagatesNulls_ = propagatesNulls();
  }
  for (auto& input : inputs_) {
    if (!input->isMultiplyReferenced_ &&
        isSameFields(distinctFields_, input->distinctFields_)) {
      input->distinctFields_.clear();
    }
  }

  hasConditionals_ = hasConditionals(this);
}

namespace {
// Returns true if vector is a LazyVector that hasn't been loaded yet or
// is not dictionary, sequence or constant encoded.
bool isFlat(const BaseVector& vector) {
  auto encoding = vector.encoding();
  if (encoding == VectorEncoding::Simple::LAZY) {
    if (!vector.asUnchecked<LazyVector>()->isLoaded()) {
      return true;
    }

    encoding = vector.loadedVector()->encoding();
  }
  return !(
      encoding == VectorEncoding::Simple::SEQUENCE ||
      encoding == VectorEncoding::Simple::DICTIONARY ||
      encoding == VectorEncoding::Simple::CONSTANT);
}

} // namespace

void Expr::evalSimplified(
    const SelectivityVector& rows,
    EvalCtx& context,
    VectorPtr& result) {
  if (!rows.hasSelections()) {
    // empty input, return an empty vector of the right type
    result = BaseVector::createNullConstant(type(), 0, context.pool());
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
  addNulls(rows, remainingRows->asRange().bits(), context, result);
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

  SelectivityVector remainingRows = rows;
  inputValues_.resize(inputs_.size());
  const bool defaultNulls = vectorFunction_->isDefaultNullBehavior();

  LocalDecodedVector decodedVector(context);
  for (int32_t i = 0; i < inputs_.size(); ++i) {
    auto& inputValue = inputValues_[i];
    inputs_[i]->evalSimplified(remainingRows, context, inputValue);

    BaseVector::flattenVector(inputValue, rows.end());
    VELOX_CHECK_EQ(VectorEncoding::Simple::FLAT, inputValue->encoding());

    // If the resulting vector has nulls, merge them into our current remaining
    // rows bitmap.
    if (defaultNulls && inputValue->mayHaveNulls()) {
      decodedVector.get()->decode(*inputValue, rows);
      if (auto* rawNulls = decodedVector->nulls()) {
        remainingRows.deselectNulls(
            rawNulls, remainingRows.begin(), remainingRows.end());
      }
    }

    // All rows are null, return a null constant.
    if (!remainingRows.hasSelections()) {
      releaseInputValues(context);
      result =
          BaseVector::createNullConstant(type(), rows.size(), context.pool());
      return;
    }
  }

  // Apply the actual function.
  vectorFunction_->apply(remainingRows, inputValues_, type(), context, result);

  // Make sure the returned vector has its null bitmap properly set.
  addNulls(rows, remainingRows.asRange().bits(), context, result);
  releaseInputValues(context);
}

namespace {
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
      "{}. Input data: {}. SQL expression: {}.",
      context->expr()->toString(),
      context->dataPath(),
      context->sqlPath());
}

/// Used to generate context for an error occurred while evaluating
/// sub-expression. Returns the output of Expr::toString() for the
/// sub-expression.
std::string onException(VeloxException::Type /*exceptionType*/, void* arg) {
  return static_cast<Expr*>(arg)->toString();
}

/// Generates a file path in specified directory. Returns std::nullopt on
/// failure.
std::optional<std::string> generateFilePath(
    const char* basePath,
    const char* prefix) {
  auto path = fmt::format("{}/velox_{}_XXXXXX", basePath, prefix);
  auto fd = mkstemp(path.data());
  if (fd == -1) {
    return std::nullopt;
  }
  return path;
}
} // namespace

void ExprExceptionContext::persistDataAndSql(const char* basePath) {
  // Exception already persisted or failed to persist. We don't persist again in
  // this situation.
  if (!dataPath_.empty()) {
    return;
  }

  auto dataPath = generateFilePath(basePath, "vector");
  auto sqlPath = generateFilePath(basePath, "sql");
  if (!dataPath.has_value()) {
    dataPath_ = "Failed to create file for saving input vector.";
    return;
  }
  if (!sqlPath.has_value()) {
    sqlPath_ = "Failed to create file for saving expression SQL.";
    return;
  }

  // Persist vector to disk
  try {
    std::ofstream outputFile(dataPath.value(), std::ofstream::binary);
    saveVector(*vector_, outputFile);
    outputFile.close();
    dataPath_ = dataPath.value();
  } catch (...) {
    dataPath_ =
        fmt::format("Failed to save input vector to {}.", dataPath.value());
  }

  // Persist sql to disk
  try {
    auto sql = expr_->toSql();
    std::ofstream outputFile(sqlPath.value(), std::ofstream::binary);
    outputFile.write(sql.data(), sql.size());
    outputFile.close();
    sqlPath_ = sqlPath.value();
  } catch (...) {
    sqlPath_ =
        fmt::format("Failed to save expression SQL to {}.", sqlPath.value());
  }
}

void Expr::evalFlatNoNulls(
    const SelectivityVector& rows,
    EvalCtx& context,
    VectorPtr& result,
    bool topLevel) {
  ExprExceptionContext exprExceptionContext{this, context.row()};
  ExceptionContextSetter exceptionContext(
      {topLevel ? onTopLevelException : onException,
       topLevel ? (void*)&exprExceptionContext : this});

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
      inputValues_[i]->resize(rows.size());
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
    bool topLevel) {
  if (supportsFlatNoNullsFastPath_ && context.throwOnError() &&
      context.inputFlatNoNulls() && rows.countSelected() < 1'000) {
    evalFlatNoNulls(rows, context, result, topLevel);
    return;
  }

  // Make sure to include current expression in the error message in case of an
  // exception.
  ExprExceptionContext exprExceptionContext{this, context.row()};
  ExceptionContextSetter exceptionContext(
      {topLevel ? onTopLevelException : onException,
       topLevel ? (void*)&exprExceptionContext : this});

  if (!rows.hasSelections()) {
    // empty input, return an empty vector of the right type
    result = BaseVector::createNullConstant(type(), 0, context.pool());
    return;
  }

  // Check if there are any IFs, ANDs or ORs. These expressions are special
  // because not all of their sub-expressions get evaluated on all the rows
  // all the time. Therefore, we should delay loading lazy vectors until we
  // know the minimum subset of rows needed to be loaded.
  //
  // Load fields multiply referenced by inputs unconditionally. It's hard to
  // know the superset of rows the multiple inputs need to load.
  //
  // If there is only one field, load it unconditionally. The very first IF,
  // AND or OR will have to load it anyway. Pre-loading enables peeling of
  // encodings at a higher level in the expression tree and avoids repeated
  // peeling and wrapping in the sub-nodes.
  //
  // TODO: Re-work the logic of deciding when to load which field.
  if (!hasConditionals_ || distinctFields_.size() == 1) {
    // Load lazy vectors if any.
    for (const auto& field : distinctFields_) {
      context.ensureFieldLoaded(field->index(context), rows);
    }
  } else if (!propagatesNulls_) {
    // Load multiply-referenced fields at common parent expr with "rows".
    // Delay loading fields that are not in multiplyReferencedFields_.
    for (const auto& field : multiplyReferencedFields_) {
      context.ensureFieldLoaded(field->index(context), rows);
    }
  }

  if (inputs_.empty()) {
    evalAll(rows, context, result);
    return;
  }

  // Check if this expression has been evaluated already. If so, fetch and
  // return the previously computed result.
  if (checkGetSharedSubexprValues(rows, context, result)) {
    return;
  }

  evalEncodings(rows, context, result);

  checkUpdateSharedSubexprValues(rows, context, result);
}

bool Expr::checkGetSharedSubexprValues(
    const SelectivityVector& rows,
    EvalCtx& context,
    VectorPtr& result) {
  // Common subexpression optimization and peeling off of encodings and lazy
  // vectors do not work well together. There are cases when expression
  // initially is evaluated on rows before peeling and later is evaluated on
  // rows after peeling. In this case the row numbers in sharedSubexprRows_ are
  // not comparable to 'rows'.
  //
  // For now, disable the optimization if any encodings have been peeled off.

  if (!deterministic_ || !isMultiplyReferenced_ || !sharedSubexprValues_ ||
      context.wrapEncoding() != VectorEncoding::Simple::FLAT) {
    return false;
  }

  if (!rows.isSubset(*sharedSubexprRows_)) {
    LocalSelectivityVector missingRowsHolder(context, rows);
    auto missingRows = missingRowsHolder.get();
    VELOX_DCHECK_NOT_NULL(missingRows);
    missingRows->deselect(*sharedSubexprRows_);

    // Add the missingRows to sharedSubexprRows_ that will eventually be
    // evaluated and added to sharedSubexprValues_.
    sharedSubexprRows_->select(*missingRows);

    // Fix finalSelection to avoid losing values outside missingRows.
    LocalSelectivityVector newFinalSelectionHolder(
        context, *sharedSubexprRows_);
    auto newFinalSelection = newFinalSelectionHolder.get();
    VELOX_DCHECK_NOT_NULL(newFinalSelection);
    if (!context.isFinalSelection()) {
      // In case currently set finalSelection does not include all rows in
      // sharedSubexprRows_.
      VELOX_DCHECK_NOT_NULL(context.finalSelection());
      newFinalSelection->select(*context.finalSelection());
    }
    VarSetter finalSelectionPreservePrecomputedValues(
        context.mutableFinalSelection(),
        const_cast<const SelectivityVector*>(newFinalSelection));
    VarSetter isFinalSelectionPreservePrecomputedValues(
        context.mutableIsFinalSelection(), false, context.isFinalSelection());

    evalEncodings(*missingRows, context, sharedSubexprValues_);
  }
  context.moveOrCopyResult(sharedSubexprValues_, rows, result);
  return true;
}

void Expr::checkUpdateSharedSubexprValues(
    const SelectivityVector& rows,
    EvalCtx& context,
    const VectorPtr& result) {
  if (!isMultiplyReferenced_ || sharedSubexprValues_ ||
      context.wrapEncoding() != VectorEncoding::Simple::FLAT) {
    return;
  }

  if (!sharedSubexprRows_) {
    sharedSubexprRows_ = context.execCtx()->getSelectivityVector(rows.size());
  }
  *sharedSubexprRows_ = rows;
  sharedSubexprValues_ = result;
}

namespace {
inline void setPeeled(
    const VectorPtr& leaf,
    int32_t fieldIndex,
    EvalCtx& context,
    std::vector<VectorPtr>& peeled) {
  if (peeled.size() <= fieldIndex) {
    peeled.resize(context.row()->childrenSize());
  }
  assert(peeled.size() > fieldIndex);
  peeled[fieldIndex] = leaf;
}

void setDictionaryWrapping(
    DecodedVector& decoded,
    const SelectivityVector& rows,
    BaseVector& firstWrapper,
    EvalCtx& context) {
  auto wrapping = decoded.dictionaryWrapping(firstWrapper, rows);
  context.setDictionaryWrap(
      std::move(wrapping.indices), std::move(wrapping.nulls));
}
} // namespace

SelectivityVector* translateToInnerRows(
    const SelectivityVector& rows,
    DecodedVector& decoded,
    LocalSelectivityVector& newRowsHolder) {
  auto baseSize = decoded.base()->size();
  auto indices = decoded.indices();
  // If the wrappers add nulls, do not enable the inner rows. The
  // indices for places that a dictionary sets to null are not
  // defined. Null adding dictionaries are not peeled off non
  // null-propagating Exprs.
  auto flatNulls = decoded.hasExtraNulls() ? decoded.nulls() : nullptr;

  auto* newRows = newRowsHolder.get(baseSize, false);
  rows.applyToSelected([&](vector_size_t row) {
    if (!flatNulls || !bits::isBitNull(flatNulls, row)) {
      newRows->setValid(indices[row], true);
    }
  });
  newRows->updateBounds();

  return newRows;
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
  std::vector<VectorPtr> peeledVectors;
  std::vector<VectorPtr> maybePeeled;
  std::vector<bool> constantFields;
  int numLevels = 0;
  bool peeled;
  bool nonConstant = false;
  auto numFields = context.row()->childrenSize();
  int32_t firstPeeled = -1;
  do {
    peeled = true;
    BufferPtr firstIndices;
    BufferPtr firstLengths;
    for (const auto& field : distinctFields_) {
      auto fieldIndex = field->index(context);
      assert(fieldIndex >= 0 && fieldIndex < numFields);
      auto leaf = peeledVectors.empty() ? context.getField(fieldIndex)
                                        : peeledVectors[fieldIndex];
      if (!constantFields.empty() && constantFields[fieldIndex]) {
        setPeeled(leaf, fieldIndex, context, maybePeeled);
        continue;
      }
      if (numLevels == 0 && leaf->isConstant(rows)) {
        leaf = context.ensureFieldLoaded(fieldIndex, rows);
        setPeeled(leaf, fieldIndex, context, maybePeeled);
        constantFields.resize(numFields);
        constantFields.at(fieldIndex) = true;
        continue;
      }
      nonConstant = true;
      auto encoding = leaf->encoding();
      if (encoding == VectorEncoding::Simple::DICTIONARY) {
        if (firstLengths) {
          // having a mix of dictionary and sequence encoded fields
          peeled = false;
          break;
        }
        if (!propagatesNulls_ && leaf->rawNulls()) {
          // A dictionary that adds nulls over an Expr that is not null for a
          // null argument cannot be peeled.
          peeled = false;
          break;
        }
        BufferPtr indices = leaf->wrapInfo();
        if (!firstIndices) {
          firstIndices = std::move(indices);
        } else if (indices != firstIndices) {
          // different fields use different dictionaries
          peeled = false;
          break;
        }
        if (firstPeeled == -1) {
          firstPeeled = fieldIndex;
        }
        setPeeled(leaf->valueVector(), fieldIndex, context, maybePeeled);
      } else if (encoding == VectorEncoding::Simple::SEQUENCE) {
        if (firstIndices) {
          // having a mix of dictionary and sequence encoded fields
          peeled = false;
          break;
        }
        BufferPtr lengths = leaf->wrapInfo();
        if (!firstLengths) {
          firstLengths = std::move(lengths);
        } else if (lengths != firstLengths) {
          // different fields use different sequences
          peeled = false;
          break;
        }
        if (firstPeeled == -1) {
          firstPeeled = fieldIndex;
        }
        setPeeled(leaf->valueVector(), fieldIndex, context, maybePeeled);
      } else {
        // Non-peelable encoding.
        peeled = false;
        break;
      }
    }
    if (peeled) {
      ++numLevels;
      peeledVectors = std::move(maybePeeled);
    }
  } while (peeled && nonConstant);

  if (numLevels == 0 && nonConstant) {
    return Expr::PeelEncodingsResult::empty();
  }

  // We peel off the wrappers and make a new selection.
  SelectivityVector* newRows;
  SelectivityVector* newFinalSelection;
  if (firstPeeled == -1) {
    // All the fields are constant across the rows of interest.
    newRows = singleRow(newRowsHolder, rows.begin());
    context.saveAndReset(saver, rows);
    context.setConstantWrap(rows.begin());
  } else {
    auto decoded = localDecoded.get();
    auto firstWrapper = context.getField(firstPeeled).get();
    const auto& rowsToDecode =
        context.isFinalSelection() ? rows : *context.finalSelection();
    decoded->makeIndices(*firstWrapper, rowsToDecode, numLevels);

    newRows = translateToInnerRows(rows, *decoded, newRowsHolder);

    if (!context.isFinalSelection()) {
      newFinalSelection = translateToInnerRows(
          *context.finalSelection(), *decoded, finalRowsHolder);
    }

    context.saveAndReset(saver, rows);

    if (!context.isFinalSelection()) {
      *context.mutableFinalSelection() = newFinalSelection;
    }

    setDictionaryWrapping(*decoded, rowsToDecode, *firstWrapper, context);
  }
  int numPeeled = 0;
  for (int i = 0; i < peeledVectors.size(); ++i) {
    auto& values = peeledVectors[i];
    if (!values) {
      continue;
    }
    if (!constantFields.empty() && constantFields[i]) {
      context.setPeeled(
          i, BaseVector::wrapInConstant(rows.size(), rows.begin(), values));
    } else {
      context.setPeeled(i, values);
      ++numPeeled;
    }
  }
  // If the expression depends on one dictionary, results are cacheable.
  bool mayCache = numPeeled == 1 && constantFields.empty();
  return {newRows, newFinalSelection, mayCache};
}

void Expr::evalEncodings(
    const SelectivityVector& rows,
    EvalCtx& context,
    VectorPtr& result) {
  if (deterministic_ && !distinctFields_.empty()) {
    bool hasNonFlat = false;
    for (const auto& field : distinctFields_) {
      if (!isFlat(*context.getField(field->index(context)))) {
        hasNonFlat = true;
        break;
      }
    }

    if (hasNonFlat) {
      VectorPtr wrappedResult;
      // Attempt peeling and bound the scope of the context used for it.
      {
        ContextSaver saveContext;
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
          // peelEncodings() can potentially produce an empty selectivity vector
          // if all selected values we are waiting for are nulls. So, here we
          // check for such a case.
          if (newRows->hasSelections()) {
            if (peelEncodingsResult.mayCache) {
              evalWithMemo(*newRows, context, peeledResult);
            } else {
              evalWithNulls(*newRows, context, peeledResult);
            }
          }
          wrappedResult =
              context.applyWrapToPeeledResult(this->type(), peeledResult, rows);
        }
      }
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
      if (auto* rawNulls = decoded->nulls()) {
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
    return true;
  }
  return false;
}

void Expr::addNulls(
    const SelectivityVector& rows,
    const uint64_t* rawNulls,
    EvalCtx& context,
    VectorPtr& result) {
  // If there's no `result` yet, return a NULL ContantVector.
  if (!result) {
    result =
        BaseVector::createNullConstant(type(), rows.size(), context.pool());
    return;
  }

  // If result is already a NULL ConstantVector, do nothing.
  if (result->isConstantEncoding() && result->mayHaveNulls()) {
    return;
  }

  if (!result.unique() || !result->isNullsWritable()) {
    BaseVector::ensureWritable(
        SelectivityVector::empty(), type(), context.pool(), result);
  }

  if (result->size() < rows.end()) {
    result->resize(rows.end());
  }

  result->addNulls(rawNulls, rows);
}

void Expr::evalWithNulls(
    const SelectivityVector& rows,
    EvalCtx& context,
    VectorPtr& result) {
  if (!rows.hasSelections()) {
    // empty input, return an empty vector of the right type
    result = BaseVector::createNullConstant(type(), 0, context.pool());
    return;
  }

  if (propagatesNulls_) {
    bool mayHaveNulls = false;
    for (const auto& field : distinctFields_) {
      const auto& vector = context.getField(field->index(context));
      if (isLazyNotLoaded(*vector)) {
        continue;
      }

      if (vector->mayHaveNulls()) {
        mayHaveNulls = true;
        break;
      }
    }

    if (mayHaveNulls && !distinctFields_.empty()) {
      LocalSelectivityVector nonNullHolder(context);
      if (removeSureNulls(rows, context, nonNullHolder)) {
        VarSetter noMoreNulls(context.mutableNullsPruned(), true);
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

namespace {
void deselectErrors(EvalCtx& context, SelectivityVector& rows) {
  auto errors = context.errors();
  if (!errors) {
    return;
  }
  // A non-null in errors resets the row. AND with the errors null mask.
  rows.deselectNonNulls(
      errors->rawNulls(), rows.begin(), std::min(errors->size(), rows.end()));
}
} // namespace

void Expr::evalWithMemo(
    const SelectivityVector& rows,
    EvalCtx& context,
    VectorPtr& result) {
  VectorPtr base;
  distinctFields_[0]->evalSpecialForm(rows, context, base);
  ++numCachableInput_;
  if (baseDictionary_ == base) {
    ++numCacheableRepeats_;
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
      bool updateFinalSelection = context.isFinalSelection() &&
          (uncached->countSelected() < rows.countSelected());
      VarSetter finalSelectionMemo(
          context.mutableFinalSelection(), &rows, updateFinalSelection);
      VarSetter isFinalSelectionMemo(
          context.mutableIsFinalSelection(), false, updateFinalSelection);

      evalWithNulls(*uncached, context, result);
      deselectErrors(context, *uncached);
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
    return;
  }
  context.releaseVector(baseDictionary_);
  baseDictionary_ = base;
  evalWithNulls(rows, context, result);

  context.releaseVector(dictionaryCache_);
  dictionaryCache_ = result;
  if (!cachedDictionaryIndices_) {
    cachedDictionaryIndices_ =
        context.execCtx()->getSelectivityVector(rows.end());
  }
  *cachedDictionaryIndices_ = rows;
  deselectErrors(context, *cachedDictionaryIndices_);
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
  result = BaseVector::createNullConstant(type(), rows.size(), context.pool());
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
    case VectorEncoding::Simple::SEQUENCE:
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
    // empty input, return an empty vector of the right type
    result = BaseVector::createNullConstant(type(), 0, context.pool());
    return;
  }
  if (isSpecialForm()) {
    evalSpecialFormWithStats(rows, context, result);
    return;
  }
  bool tryPeelArgs = deterministic_ ? true : false;
  bool defaultNulls = vectorFunction_->isDefaultNullBehavior();

  // Tracks what subset of rows shall un-evaluated inputs and current expression
  // evaluates. Initially points to rows.
  const SelectivityVector* remainingRows = &rows;

  // Points to a mutable remainingRows, allocated using
  // mutableRemainingRowsHolder only if needed.
  SelectivityVector* mutableRemainingRows = nullptr;
  LocalSelectivityVector mutableRemainingRowsHolder(context);

  inputValues_.resize(inputs_.size());
  for (int32_t i = 0; i < inputs_.size(); ++i) {
    inputs_[i]->eval(*remainingRows, context, inputValues_[i]);
    tryPeelArgs = tryPeelArgs && isPeelable(inputValues_[i]->encoding());

    // Avoid subsequent computation on rows with known null output.
    if (defaultNulls && inputValues_[i]->mayHaveNulls()) {
      LocalDecodedVector decoded(context, *inputValues_[i], *remainingRows);

      if (auto* rawNulls = decoded->nulls()) {
        // Allocate remainingRows before the first time writing to it.
        if (mutableRemainingRows == nullptr) {
          mutableRemainingRows = mutableRemainingRowsHolder.get(rows);
          remainingRows = mutableRemainingRows;
        }

        mutableRemainingRows->deselectNulls(
            rawNulls, remainingRows->begin(), remainingRows->end());

        if (!remainingRows->hasSelections()) {
          releaseInputValues(context);
          setAllNulls(rows, context, result);
          return;
        }
      }
    }
  }

  // If any errors occurred evaluating the arguments, it's possible (even
  // likely) that the values for those arguments were not defined which could
  // lead to undefined behavior if we try to evaluate the current function on
  // them.  It's safe to skip evaluating them since the value for this branch
  // of the expression tree will be NULL for those rows anyway.
  if (context.errors()) {
    // Allocate remainingRows before the first time writing to it.
    if (mutableRemainingRows == nullptr) {
      mutableRemainingRows = mutableRemainingRowsHolder.get(rows);
      remainingRows = mutableRemainingRows;
    }
    deselectErrors(context, *mutableRemainingRows);

    // All rows have at least one null output or error.
    if (!remainingRows->hasSelections()) {
      releaseInputValues(context);
      setAllNulls(rows, context, result);
      return;
    }
  }

  if (!tryPeelArgs ||
      !applyFunctionWithPeeling(rows, *remainingRows, context, result)) {
    applyFunction(*remainingRows, context, result);
  }

  // Write non-selected rows in remainingRows as nulls in the result if some
  // rows have been skipped.
  if (mutableRemainingRows != nullptr) {
    addNulls(rows, mutableRemainingRows->asRange().bits(), context, result);
  }
  releaseInputValues(context);
}

namespace {
void setPeeledArg(
    VectorPtr arg,
    int32_t index,
    int32_t numArgs,
    std::vector<VectorPtr>& peeledArgs) {
  if (peeledArgs.empty()) {
    peeledArgs.resize(numArgs);
  }
  peeledArgs[index] = arg;
}
} // namespace

bool Expr::applyFunctionWithPeeling(
    const SelectivityVector& rows,
    const SelectivityVector& applyRows,
    EvalCtx& context,
    VectorPtr& result) {
  if (context.wrapEncoding() == VectorEncoding::Simple::CONSTANT) {
    return false;
  }
  int numLevels = 0;
  bool peeled;
  int32_t numConstant = 0;
  auto numArgs = inputValues_.size();
  // Holds the outermost wrapper. This may be the last reference after
  // peeling for a temporary dictionary, hence use a shared_ptr.
  VectorPtr firstWrapper = nullptr;
  std::vector<bool> constantArgs;
  do {
    peeled = true;
    BufferPtr firstIndices;
    BufferPtr firstLengths;
    std::vector<VectorPtr> maybePeeled;
    for (auto i = 0; i < inputValues_.size(); ++i) {
      auto leaf = inputValues_[i];
      if (!constantArgs.empty() && constantArgs[i]) {
        setPeeledArg(leaf, i, numArgs, maybePeeled);
        continue;
      }
      if ((numLevels == 0 && leaf->isConstant(rows)) ||
          leaf->isConstantEncoding()) {
        if (leaf->isConstantEncoding()) {
          setPeeledArg(leaf, i, numArgs, maybePeeled);
        } else {
          setPeeledArg(
              BaseVector::wrapInConstant(leaf->size(), rows.begin(), leaf),
              i,
              numArgs,
              maybePeeled);
        }
        constantArgs.resize(numArgs);
        constantArgs.at(i) = true;
        ++numConstant;
        continue;
      }
      auto encoding = leaf->encoding();
      if (encoding == VectorEncoding::Simple::DICTIONARY) {
        if (firstLengths) {
          // having a mix of dictionary and sequence encoded fields
          peeled = false;
          break;
        }
        if (!vectorFunction_->isDefaultNullBehavior() && leaf->rawNulls()) {
          // A dictionary that adds nulls over an Expr that is not null for a
          // null argument cannot be peeled.
          peeled = false;
          break;
        }
        BufferPtr indices = leaf->wrapInfo();
        if (!firstIndices) {
          firstIndices = std::move(indices);
        } else if (indices != firstIndices) {
          // different fields use different dictionaries
          peeled = false;
          break;
        }
        if (!firstWrapper) {
          firstWrapper = leaf;
        }
        setPeeledArg(leaf->valueVector(), i, numArgs, maybePeeled);
      } else if (encoding == VectorEncoding::Simple::SEQUENCE) {
        if (firstIndices) {
          // having a mix of dictionary and sequence encoded fields
          peeled = false;
          break;
        }
        BufferPtr lengths = leaf->wrapInfo();
        if (!firstLengths) {
          firstLengths = std::move(lengths);
        } else if (lengths != firstLengths) {
          // different fields use different sequences
          peeled = false;
          break;
        }
        if (!firstWrapper) {
          firstWrapper = leaf;
        }
        setPeeledArg(leaf->valueVector(), i, numArgs, maybePeeled);
      } else {
        // Non-peelable encoding.
        peeled = false;
        break;
      }
    }
    if (peeled) {
      ++numLevels;
      inputValues_ = std::move(maybePeeled);
    }
  } while (peeled && numConstant != numArgs);
  if (!numLevels) {
    return false;
  }
  LocalSelectivityVector newRowsHolder(context);
  ContextSaver saver;
  // We peel off the wrappers and make a new selection.
  SelectivityVector* newRows;
  LocalDecodedVector localDecoded(context);
  if (numConstant == numArgs) {
    // All the fields are constant across the rows of interest.
    newRows = singleRow(newRowsHolder, rows.begin());

    context.saveAndReset(saver, rows);
    context.setConstantWrap(rows.begin());
  } else {
    auto decoded = localDecoded.get();
    decoded->makeIndices(*firstWrapper, rows, numLevels);
    newRows = translateToInnerRows(applyRows, *decoded, newRowsHolder);
    context.saveAndReset(saver, rows);
    setDictionaryWrapping(*decoded, rows, *firstWrapper, context);

    // 'newRows' comes from the set of row numbers in the base vector. These
    // numbers may be larger than rows.end(). Hence, we need to resize constant
    // inputs.
    if (newRows->end() > rows.end() && numConstant) {
      for (int i = 0; i < constantArgs.size(); ++i) {
        if (!constantArgs.empty() && constantArgs[i]) {
          inputValues_[i] =
              BaseVector::wrapInConstant(newRows->end(), 0, inputValues_[i]);
        }
      }
    }
  }

  VectorPtr peeledResult;
  applyFunction(*newRows, context, peeledResult);
  VectorPtr wrappedResult =
      context.applyWrapToPeeledResult(this->type(), peeledResult, applyRows);
  context.moveOrCopyResult(wrappedResult, rows, result);
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

  vectorFunction_->apply(rows, inputValues_, type(), context, result);

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
    // Common sub-expression. Print the full expression, but skip the stats. Add
    // ID of the expression it duplicates.
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

std::string Expr::toSql() const {
  std::stringstream out;
  out << "\"" << name_ << "\"";
  appendInputsSql(out);
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

void Expr::appendInputsSql(std::stringstream& stream) const {
  if (!inputs_.empty()) {
    stream << "(";
    for (auto i = 0; i < inputs_.size(); ++i) {
      if (i > 0) {
        stream << ", ";
      }
      stream << inputs_[i]->toSql();
    }
    stream << ")";
  }
}

ExprSet::ExprSet(
    const std::vector<core::TypedExprPtr>& sources,
    core::ExecCtx* execCtx,
    bool enableConstantFolding)
    : execCtx_(execCtx) {
  exprs_ = compileExpressions(sources, execCtx, this, enableConstantFolding);
  std::vector<FieldReference*> allDistinctFields;
  for (auto& expr : exprs_) {
    mergeFields(
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

ExprSet::~ExprSet() {
  exprSetListeners().withRLock([&](auto& listeners) {
    if (!listeners.empty()) {
      std::unordered_map<std::string, exec::ExprStats> stats;
      std::unordered_set<const exec::Expr*> uniqueExprs;
      for (const auto& expr : exprs()) {
        addStats(*expr, stats, uniqueExprs);
      }

      auto uuid = makeUuid();
      for (auto& listener : listeners) {
        listener->onCompletion(uuid, {stats});
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

  for (int32_t i = begin; i < end; ++i) {
    exprs_[i]->eval(rows, context, result[i], true /*topLevel*/);
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
  if (execCtx->queryCtx()->config().exprEvalSimplified() ||
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
} // namespace facebook::velox::exec

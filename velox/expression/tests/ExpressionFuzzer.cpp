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

#include <folly/Random.h>
#include <glog/logging.h>
#include <exception>

#include "velox/common/base/Exceptions.h"
#include "velox/expression/Expr.h"
#include "velox/expression/tests/ExpressionFuzzer.h"
#include "velox/expression/tests/VectorFuzzer.h"
#include "velox/type/Type.h"
#include "velox/vector/tests/VectorMaker.h"

DEFINE_int32(
    batch_size,
    100,
    "The number of elements on each generated vector.");

DEFINE_int32(
    null_chance,
    10,
    "Chance of adding a null constant to the plan, or null value in a vector "
    "(expressed using '1 in x' semantic).");

namespace facebook::velox::test {

namespace {

// Called if at least one of the ptrs has an exception.
void compareExceptions(
    std::exception_ptr commonPtr,
    std::exception_ptr simplifiedPtr) {
  // If we don't have two exceptions, fail.
  if (!commonPtr || !simplifiedPtr) {
    if (!commonPtr) {
      LOG(ERROR) << "Only simplified path threw exception:";
      std::rethrow_exception(simplifiedPtr);
    }
    LOG(ERROR) << "Only common path threw exception:";
    std::rethrow_exception(commonPtr);
  }

  // Otherwise, make sure the exceptions are the same.
  try {
    std::rethrow_exception(commonPtr);
  } catch (const VeloxException& ve1) {
    try {
      std::rethrow_exception(simplifiedPtr);
    } catch (const VeloxException& ve2) {
      // Error messages sometimes differ; check at least error codes.
      VELOX_CHECK_EQ(ve1.errorCode(), ve2.errorCode());
      VELOX_CHECK_EQ(ve1.errorSource(), ve2.errorSource());
      VELOX_CHECK_EQ(ve1.exceptionName(), ve2.exceptionName());
      if (ve1.message() != ve2.message()) {
        LOG(WARNING) << "Two different VeloxExceptions were thrown:\n\t"
                     << ve1.message() << "\nand\n\t" << ve2.message();
      }
      return;
    } catch (const std::exception& e2) {
      LOG(WARNING) << "Two different exceptions were thrown:\n\t"
                   << ve1.message() << "\nand\n\t" << e2.what();
    }
  } catch (const std::exception& e1) {
    try {
      std::rethrow_exception(simplifiedPtr);
    } catch (const std::exception& e2) {
      if (e1.what() != e2.what()) {
        LOG(WARNING) << "Two different std::exceptions were thrown:\n\t"
                     << e1.what() << "\nand\n\t" << e2.what();
      }
      return;
    }
  }
  VELOX_FAIL("Got two incompatible exceptions.");
}

void compareVectors(const VectorPtr& vec1, const VectorPtr& vec2) {
  VELOX_CHECK_EQ(vec1->size(), vec2->size());

  // Print vector contents if in verbose mode.
  size_t vectorSize = vec1->size();
  if (VLOG_IS_ON(1)) {
    LOG(INFO) << "== Result contents (common vs. simple): ";
    for (auto i = 0; i < vectorSize; i++) {
      LOG(INFO) << "At " << i << ": [" << vec1->toString(i) << " vs "
                << vec2->toString(i) << "]";
    }
    LOG(INFO) << "===================";
  }

  for (auto i = 0; i < vectorSize; i++) {
    VELOX_CHECK(
        vec1->equalValueAt(vec2.get(), i, i), "Different results at idx {}", i);
  }
  LOG(INFO) << "All results match.";
}

VectorFuzzer::Options getFuzzerOptions() {
  VectorFuzzer::Options opts;
  opts.vectorSize = FLAGS_batch_size;
  opts.stringVariableLength = true;
  opts.stringLength = 100;
  opts.nullChance = FLAGS_null_chance;
  return opts;
}

} // namespace

std::string CallableSignature::toString() const {
  std::string buf = name;
  buf.append("( ");
  for (const auto& arg : args) {
    buf.append(arg->toString());
    buf.append(" ");
  }
  buf.append(") -> ");
  buf.append(returnType->toString());
  return buf;
}

class ExpressionFuzzer {
 public:
  ExpressionFuzzer(
      std::vector<CallableSignature> signatures,
      size_t initialSeed)
      : signatures_(std::move(signatures)),
        vectorFuzzer_(getFuzzerOptions(), execCtx_.pool()) {
    seed(initialSeed);

    // Generates signaturesMap, which maps a given type to the function
    // signature that return it.
    for (const auto& it : signatures_) {
      signaturesMap_[it.returnType->kind()].push_back(&it);
    }
  }

 private:
  void seed(size_t seed) {
    currentSeed_ = seed;
    vectorFuzzer_.reSeed(seed);
    rng_.seed(currentSeed_);
  }

  void reSeed() {
    seed(folly::Random::rand32(rng_));
  }

  // Returns true 1/n of times.
  bool oneIn(size_t n) {
    return folly::Random::oneIn(n, rng_);
  }

  RowVectorPtr generateRowVector() {
    std::vector<VectorPtr> vectors;
    vectors.reserve(inputRowTypes_.size());
    size_t idx = 0;

    for (const auto& inputRowType : inputRowTypes_) {
      auto vector = vectorFuzzer_.fuzz(inputRowType);

      // If verbose mode, print the whole vector.
      if (VLOG_IS_ON(1)) {
        for (size_t i = 0; i < vector->size(); ++i) {
          if (vector->isNullAt(i)) {
            LOG(INFO) << "C" << idx << "[" << i << "]: null";
          } else {
            LOG(INFO) << "C" << idx << "[" << i << "]: " << vector->toString(i);
          }
        }
      }
      LOG(INFO) << "\t" << vector->toString();
      vectors.emplace_back(vector);
      ++idx;
    }
    return vectors.empty() ? nullptr : vectorMaker_.rowVector(vectors);
  }

  core::TypedExprPtr generateArgConstant(const TypePtr& arg) {
    // One in ten times return a NULL constant.
    if (oneIn(FLAGS_null_chance)) {
      return std::make_shared<core::ConstantTypedExpr>(
          variant::null(arg->kind()));
    }
    return std::make_shared<core::ConstantTypedExpr>(
        vectorFuzzer_.randVariant(arg));
  }

  core::TypedExprPtr generateArgColumn(const TypePtr& arg) {
    inputRowTypes_.emplace_back(arg);

    return std::make_shared<core::FieldAccessTypedExpr>(
        arg,
        std::vector<core::TypedExprPtr>{
            std::make_shared<const core::InputTypedExpr>(ROW({arg}))},
        fmt::format("c{}", inputRowTypes_.size() - 1));
  }

  std::vector<core::TypedExprPtr> generateArgs(const CallableSignature& input) {
    std::vector<core::TypedExprPtr> outputArgs;

    for (const auto& arg : input.args) {
      size_t type = folly::Random::rand32(3, rng_);

      // Toss a coin a choose between a constant, a column reference, or another
      // expression (function).
      //
      // TODO: Add more expression types:
      // - Conjunctions
      // - IF/ELSE/SWITCH
      // - Lambdas
      // - Try
      if (type == 0) {
        outputArgs.emplace_back(generateArgConstant(arg));
      } else if (type == 1) {
        outputArgs.emplace_back(generateArgColumn(arg));
      } else if (type == 2) {
        outputArgs.emplace_back(generateExpression(arg));
      } else {
        VELOX_UNREACHABLE();
      }
    }
    return outputArgs;
  }

  core::TypedExprPtr generateExpression(const TypePtr& returnType) {
    // If no functions can return `returnType`, return a constant instead.
    auto it = signaturesMap_.find(returnType->kind());
    if (it == signaturesMap_.end()) {
      LOG(INFO) << "Couldn't find any function to return '"
                << returnType->toString() << "'. Returning a constant instead.";
      return generateArgConstant(returnType);
    }

    // Randomly pick a function that can return `returnType`.
    const auto& eligible = it->second;
    size_t idx = folly::Random::rand32(eligible.size(), rng_);
    const auto& chosen = eligible[idx];

    // Generate the function args recursively.
    auto args = generateArgs(*chosen);
    return std::make_shared<core::CallTypedExpr>(
        chosen->returnType, args, chosen->name);
  }

 public:
  void go(size_t steps) {
    for (size_t i = 0; i < steps; ++i) {
      LOG(INFO) << "==============================> Started iteration " << i
                << " (seed: " << currentSeed_ << ")";
      inputRowTypes_.clear();

      // Pick a random signature to chose the root return type.
      size_t idx = folly::Random::rand32(signatures_.size(), rng_);
      const auto& rootType = signatures_[idx].returnType;

      // Generate an expression tree.
      auto plan = generateExpression(rootType);
      LOG(INFO) << "Generated expression: " << plan->toString();

      // Generate the input data vectors.
      auto rowVector = generateRowVector();
      SelectivityVector rows{rowVector ? rowVector->size() : 1};

      if (rowVector) {
        LOG(INFO) << "Generated " << rowVector->childrenSize() << " vectors:";
        for (const auto& child : rowVector->children()) {
          LOG(INFO) << "\t" << child->toString();
        }
      }

      // Execute expression using both common and simplified evals.
      std::vector<VectorPtr> commonEvalResult(1);
      std::vector<VectorPtr> simplifiedEvalResult(1);

      std::exception_ptr exceptionCommonPtr;
      std::exception_ptr exceptionSimplifiedPtr;

      VLOG(1) << "Starting common eval execution.";

      // Execute with common expression eval path.
      try {
        exec::ExprSet exprSetCommon({plan}, &execCtx_);
        exec::EvalCtx evalCtxCommon(&execCtx_, &exprSetCommon, rowVector.get());

        exprSetCommon.eval(rows, &evalCtxCommon, &commonEvalResult);
      } catch (...) {
        exceptionCommonPtr = std::current_exception();
      }

      VLOG(1) << "Starting simplified eval execution.";

      // Execute with simplified expression eval path.
      try {
        exec::ExprSetSimplified exprSetSimplified({plan}, &execCtx_);
        exec::EvalCtx evalCtxSimplified(
            &execCtx_, &exprSetSimplified, rowVector.get());

        exprSetSimplified.eval(rows, &evalCtxSimplified, &simplifiedEvalResult);
      } catch (...) {
        exceptionSimplifiedPtr = std::current_exception();
      }

      // Compare results or exceptions (if any). Fail is anything is different.
      if (exceptionCommonPtr || exceptionSimplifiedPtr) {
        compareExceptions(exceptionCommonPtr, exceptionSimplifiedPtr);
        LOG(INFO)
            << "Both paths failed with compatible exceptions. Continuing.";
      } else {
        compareVectors(commonEvalResult.front(), simplifiedEvalResult.front());
      }

      LOG(INFO) << "==============================> Done with iteration " << i;
      reSeed();
    }
  }

 private:
  folly::Random::DefaultGenerator rng_;
  size_t currentSeed_{0};

  const std::vector<CallableSignature> signatures_;

  // Maps a given type to the functions that return that type.
  std::unordered_map<TypeKind, std::vector<const CallableSignature*>>
      signaturesMap_;

  std::shared_ptr<core::QueryCtx> queryCtx_{core::QueryCtx::create()};
  std::unique_ptr<memory::MemoryPool> pool_{
      memory::getDefaultScopedMemoryPool()};
  core::ExecCtx execCtx_{pool_.get(), queryCtx_.get()};

  test::VectorMaker vectorMaker_{execCtx_.pool()};
  VectorFuzzer vectorFuzzer_;

  // Contains the input column references that need to be generated for one
  // particular iteration.
  std::vector<TypePtr> inputRowTypes_;
};

void expressionFuzzer(
    std::vector<CallableSignature> signatures,
    size_t steps,
    size_t seed) {
  ExpressionFuzzer(std::move(signatures), seed).go(steps);
}

} // namespace facebook::velox::test

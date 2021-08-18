/*
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

#include "gtest/gtest.h"

#include "velox/expression/tests/VectorFuzzer.h"
#include "velox/functions/common/CoreFunctions.h"
#include "velox/functions/common/tests/FunctionBaseTest.h"

using namespace facebook::velox;
using functions::test::FunctionBaseTest;

// This test suite tests the simplified eval engine by:
//
//  1. Generating random vectors and encodings using VectorFuzzer;
//  2. Executing expressions against the generate vectors using the simplified
//  and common eval engines
//  3. Asserting that result vectors are the same.
//
class EvalSimplifiedTest : public FunctionBaseTest {
 protected:
  void assertEqualVectors(const VectorPtr& expected, const VectorPtr& actual) {
    size_t vectorSize = expected->size();

    // If one of the vectors is constant, they will report kMaxElements as
    // size().
    if (expected->isConstantEncoding() || actual->isConstantEncoding()) {
      // If one is constant, use the size of the other; if both are, assume size
      // is 1.
      vectorSize = std::min(expected->size(), actual->size());
      if (vectorSize == BaseVector::kMaxElements) {
        vectorSize = 1;
      }
    } else {
      ASSERT_EQ(expected->size(), actual->size());
    }
    FunctionBaseTest::assertEqualVectors(
        expected, actual, vectorSize, fmt::format(" (seed {}).", seed_));
  }

  // Generate random (but deterministic) input row vectors.
  RowVectorPtr genRowVector(
      const RowTypePtr& types,
      folly::Random::DefaultGenerator& rng) {
    if (types == nullptr) {
      return nullptr;
    }

    std::vector<VectorPtr> vectors;
    vectors.reserve(types->size());

    for (const auto& type : types->children()) {
      size_t seed = folly::Random::rand32(rng);
      vectors.emplace_back(VectorFuzzer(seed).fuzz(type, execCtx_.pool()));
      LOG(INFO) << "\t" << vectors.back()->toString();
    }
    return makeRowVector(vectors);
  }

  void runTest(
      const std::string& exprString,
      const RowTypePtr& types = nullptr) {
    auto expr = makeTypedExpr(exprString, types);

    // Instantiate common eval.
    exec::ExprSet exprSetCommon({expr}, &execCtx_);

    // Instantiate simplified eval.
    exec::ExprSetSimplified exprSetSimplified({expr}, &execCtx_);
    SelectivityVector rows(batchSize_);

    for (size_t i = 0; i < iterations_; ++i) {
      LOG(INFO) << "============== Starting iteration with seed: " << seed_;
      folly::Random::DefaultGenerator rng(seed_);

      // Generate the input vectors.
      auto rowVector = genRowVector(types, rng);

      exec::EvalCtx evalCtxCommon(&execCtx_, &exprSetCommon, rowVector.get());
      exec::EvalCtx evalCtxSimplified(
          &execCtx_, &exprSetSimplified, rowVector.get());

      // Evaluate using both engines.
      std::vector<VectorPtr> commonEvalResult{nullptr};
      std::vector<VectorPtr> simplifiedEvalResult{nullptr};

      exprSetCommon.eval(rows, &evalCtxCommon, &commonEvalResult);
      exprSetSimplified.eval(rows, &evalCtxSimplified, &simplifiedEvalResult);

      // Compare results.
      assertEqualVectors(
          commonEvalResult.front(), simplifiedEvalResult.front());

      // Update the seed for the next iteration.
      seed_ = folly::Random::rand32(rng);
    }
  }

  const size_t batchSize_{100};
  const size_t iterations_{100};

  // Initial seed. Will get refreshed on each iteration. In order to reproduce
  // a test result, check the seed in the logs and paste it in here.
  int32_t seed_{123456};
};

TEST_F(EvalSimplifiedTest, constantOnly) {
  runTest("1 + 3 * 2 + 10 * 2");
}

TEST_F(EvalSimplifiedTest, constantAndInput) {
  runTest("1 + c0 - 2 + c0", ROW({"c0"}, {BIGINT()}));
}

TEST_F(EvalSimplifiedTest, strings) {
  runTest("md5(c0)", ROW({"c0"}, {VARCHAR()}));
}

TEST_F(EvalSimplifiedTest, doubles) {
  runTest("ceil(c1) * c0", ROW({"c0", "c1"}, {DOUBLE(), DOUBLE()}));
}

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

#include <gtest/gtest.h>
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/exec/tests/utils/TempFilePath.h"
#include "velox/expression/Expr.h"
#include "velox/expression/SignatureBinder.h"
#include "velox/expression/fuzzer/ExpressionFuzzer.h"
#include "velox/expression/fuzzer/FuzzerRunner.h"
#include "velox/expression/tests/ExpressionRunner.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/vector/VectorSaver.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::test {

class ExpressionRunnerUnitTest : public testing::Test, public VectorTestBase {
 public:
  void SetUp() override {
    velox::functions::prestosql::registerAllScalarFunctions();
  }

 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  std::shared_ptr<memory::MemoryPool> pool_{
      memory::memoryManager()->addLeafPool()};
  std::shared_ptr<core::QueryCtx> queryCtx_{core::QueryCtx::create()};
  core::ExecCtx execCtx_{pool_.get(), queryCtx_.get()};
};

TEST_F(ExpressionRunnerUnitTest, run) {
  auto inputFile = exec::test::TempFilePath::create();
  auto sqlFile = exec::test::TempFilePath::create();
  auto resultFile = exec::test::TempFilePath::create();
  const auto inputPathStr = inputFile->getPath();
  const char* inputPath = inputPathStr.data();
  const auto resultPathStr = resultFile->getPath();
  const char* resultPath = resultPathStr.data();
  const int vectorSize = 100;

  VectorMaker vectorMaker(pool_.get());
  auto inputVector = vectorMaker.rowVector(
      {"c0"}, {vectorMaker.flatVector<StringView>(vectorSize, [](auto) {
        return "abc";
      })});
  auto resultVector = vectorMaker.flatVector<int64_t>(
      vectorSize, [](auto row) { return row * 100; });
  saveVectorToFile(inputVector.get(), inputPath);
  saveVectorToFile(resultVector.get(), resultPath);

  for (bool useSeperatePoolForInput : {true, false}) {
    LOG(INFO) << "Using useSeperatePoolForInput: " << useSeperatePoolForInput;
    EXPECT_NO_THROW(ExpressionRunner::run(
        inputPath,
        "length(c0)",
        "",
        resultPath,
        "verify",
        0,
        "",
        "",
        false,
        useSeperatePoolForInput));
  }
}

TEST_F(ExpressionRunnerUnitTest, persistAndReproComplexSql) {
  // Create a constant vector of ARRAY(Dictionary-Encoded INT)
  auto dictionaryVector = wrapInDictionary(
      makeIndices({{2, 4, 0, 1}}), makeFlatVector<int32_t>({{1, 2, 3, 4, 5}}));
  auto arrVector = makeArrayVector({0}, dictionaryVector, {});
  auto constantExpr = std::make_shared<core::ConstantTypedExpr>(
      BaseVector::wrapInConstant(1, 0, arrVector));

  ASSERT_EQ(
      constantExpr->toString(),
      "4 elements starting at 0 {[0->2] 3, [1->4] 5, [2->0] 1, [3->1] 2}");

  auto sqlExpr = exec::ExprSet({constantExpr}, &execCtx_, false).expr(0);

  // Self contained SQL that flattens complex constant.
  auto selfContainedSql = sqlExpr->toSql();
  ASSERT_EQ(
      selfContainedSql,
      "ARRAY['3'::INTEGER, '5'::INTEGER, '1'::INTEGER, '2'::INTEGER]");

  std::vector<VectorPtr> complexConstants;
  auto complexConstantsSql = sqlExpr->toSql(&complexConstants);
  ASSERT_EQ(complexConstantsSql, "__complex_constant(c0)");

  auto rowVector = makeRowVector(complexConstants);

  // Emulate a reproduce from complex constant SQL
  auto sqlFile = exec::test::TempFilePath::create();
  auto complexConstantsFile = exec::test::TempFilePath::create();
  const auto complexConstantsFilePathStr = complexConstantsFile->getPath();
  auto sqlPathStr = sqlFile->getPath();
  auto sqlPath = sqlPathStr.c_str();
  auto complexConstantsPath = complexConstantsFilePathStr.c_str();

  // Write to file..
  saveStringToFile(complexConstantsSql, sqlPath);
  saveVectorToFile(rowVector.get(), complexConstantsPath);

  // Reproduce from file.
  auto reproSql = restoreStringFromFile(sqlPath);
  auto reproComplexConstants =
      restoreVectorFromFile(complexConstantsPath, pool_.get());

  auto reproExprs = ExpressionRunner::parseSql(
      reproSql, nullptr, pool_.get(), reproComplexConstants);
  ASSERT_EQ(reproExprs.size(), 1);
  // Note that ConstantExpr makes a copy of sharedConstantValue_ to guard
  // against race conditions, which in effect falttens the array.
  ASSERT_EQ(reproExprs[0]->toString(), "4 elements starting at 0 {3, 5, 1, 2}");
}

TEST_F(ExpressionRunnerUnitTest, primitiveConstantsInexpressibleInSql) {
  auto varbinaryData =
      vectorMaker_.flatVector<StringView>({"12"_sv}, VARBINARY());
  auto constantExpr = std::make_shared<const core::ConstantTypedExpr>(
      BaseVector::wrapInConstant(1, 0, varbinaryData));

  auto sqlExpr = exec::ExprSet({constantExpr}, &execCtx_, false).expr(0);

  ASSERT_THROW(sqlExpr->toSql(), VeloxUserError);
  std::vector<VectorPtr> complexConstants;
  auto complexConstantsSql = sqlExpr->toSql(&complexConstants);
  ASSERT_EQ(complexConstantsSql, "__complex_constant(c0)");
}
} // namespace facebook::velox::test

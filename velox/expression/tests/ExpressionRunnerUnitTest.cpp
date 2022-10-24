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
#include "FuzzerRunner.h"
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/exec/tests/utils/TempFilePath.h"
#include "velox/expression/SignatureBinder.h"
#include "velox/expression/tests/ExpressionFuzzer.h"
#include "velox/expression/tests/ExpressionRunner.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/vector/VectorSaver.h"

namespace facebook::velox::test {

class ExpressionRunnerUnitTest : public testing::Test {
 public:
  void SetUp() override {
    velox::functions::prestosql::registerAllScalarFunctions();
  }

 protected:
  std::unique_ptr<memory::MemoryPool> pool_{
      memory::getDefaultScopedMemoryPool()};
};

TEST_F(ExpressionRunnerUnitTest, run) {
  auto inputFile = exec::test::TempFilePath::create();
  auto sqlFile = exec::test::TempFilePath::create();
  auto resultFile = exec::test::TempFilePath::create();
  const char* inputPath = inputFile->path.data();
  const char* sqlPath = sqlFile->path.data();
  const char* resultPath = resultFile->path.data();
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

  std::string sql = "length(\"c0\")";
  saveStringToFile(sql, sqlPath);
  EXPECT_NO_THROW(
      ExpressionRunner::run(inputPath, sqlPath, resultPath, "verify"));
}

} // namespace facebook::velox::test

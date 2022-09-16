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

#include "velox/substrait/tests/JsonToProtoConverter.h"

#include "velox/common/base/tests/Fs.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/dwio/common/tests/utils/DataFiles.h"

#include "velox/substrait/SubstraitToVeloxPlan.h"
#include "velox/substrait/TypeUtils.h"
#include "velox/substrait/VeloxToSubstraitType.h"
using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::substrait;
namespace vestrait = facebook::velox::substrait;

class FunctionTest : public ::testing::Test {
 protected:
  std::shared_ptr<core::QueryCtx> queryCtx_ = core::QueryCtx::createForTest();

  std::unique_ptr<memory::ScopedMemoryPool> pool_ =
      memory::getDefaultScopedMemoryPool();

  std::shared_ptr<vestrait::SubstraitParser> substraitParser_ =
      std::make_shared<vestrait::SubstraitParser>();

  std::shared_ptr<vestrait::SubstraitVeloxPlanConverter> planConverter_ =
      std::make_shared<vestrait::SubstraitVeloxPlanConverter>();
};

TEST_F(FunctionTest, makeNames) {
  std::string prefix = "n";
  int size = 0;
  std::vector<std::string> names = substraitParser_->makeNames(prefix, size);
  ASSERT_EQ(names.size(), size);

  size = 5;
  names = substraitParser_->makeNames(prefix, size);
  ASSERT_EQ(names.size(), size);
  for (int i = 0; i < size; i++) {
    std::string expected = "n_" + std::to_string(i);
    ASSERT_EQ(names[i], expected);
  }
}

TEST_F(FunctionTest, makeNodeName) {
  std::string nodeName = substraitParser_->makeNodeName(1, 0);
  ASSERT_EQ(nodeName, "n1_0");
}

TEST_F(FunctionTest, getIdxFromNodeName) {
  std::string nodeName = "n1_0";
  int index = substraitParser_->getIdxFromNodeName(nodeName);
  ASSERT_EQ(index, 0);
}

TEST_F(FunctionTest, getNameBeforeDelimiter) {
  std::string functionSpec = "lte:fp64_fp64";
  std::string_view funcName = getNameBeforeDelimiter(functionSpec, ":");
  ASSERT_EQ(funcName, "lte");

  functionSpec = "lte:";
  funcName = getNameBeforeDelimiter(functionSpec, ":");
  ASSERT_EQ(funcName, "lte");

  functionSpec = "lte";
  funcName = getNameBeforeDelimiter(functionSpec, ":");
  ASSERT_EQ(funcName, "lte");
}

TEST_F(FunctionTest, constructFunctionMap) {
  std::string planPath =
      getDataFilePath("velox/substrait/tests", "data/q1_first_stage.json");
  ::substrait::Plan substraitPlan;
  JsonToProtoConverter::readFromFile(planPath, substraitPlan);
  planConverter_->constructFunctionMap(substraitPlan);

  auto functionMap = planConverter_->getFunctionMap();
  ASSERT_EQ(functionMap.size(), 9);

  std::string function = planConverter_->findFunction(1);
  ASSERT_EQ(function, "lte:fp64_fp64");

  function = planConverter_->findFunction(2);
  ASSERT_EQ(function, "and:bool_bool");

  function = planConverter_->findFunction(3);
  ASSERT_EQ(function, "subtract:opt_fp64_fp64");

  function = planConverter_->findFunction(4);
  ASSERT_EQ(function, "multiply:opt_fp64_fp64");

  function = planConverter_->findFunction(5);
  ASSERT_EQ(function, "add:opt_fp64_fp64");

  function = planConverter_->findFunction(6);
  ASSERT_EQ(function, "sum:opt_fp64");

  function = planConverter_->findFunction(7);
  ASSERT_EQ(function, "count:opt_fp64");

  function = planConverter_->findFunction(8);
  ASSERT_EQ(function, "count:opt_i32");

  function = planConverter_->findFunction(9);
  ASSERT_EQ(function, "is_not_null:fp64");
}

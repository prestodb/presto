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

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/dwio/common/tests/utils/DataFiles.h"

#include "velox/core/QueryCtx.h"
#include "velox/substrait/SubstraitToVeloxPlan.h"
#include "velox/substrait/TypeUtils.h"
#include "velox/substrait/VariantToVectorConverter.h"

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::substrait;
namespace vestrait = facebook::velox::substrait;

class FunctionTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  std::shared_ptr<core::QueryCtx> queryCtx_ = core::QueryCtx::create();

  std::shared_ptr<memory::MemoryPool> pool_ =
      memory::memoryManager()->addLeafPool();

  std::shared_ptr<vestrait::SubstraitParser> substraitParser_ =
      std::make_shared<vestrait::SubstraitParser>();

  std::shared_ptr<vestrait::SubstraitVeloxPlanConverter> planConverter_ =
      std::make_shared<vestrait::SubstraitVeloxPlanConverter>(pool_.get());
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

TEST_F(FunctionTest, setVectorFromVariants) {
  auto resultVec = setVectorFromVariants(
      BOOLEAN(), {variant(false), variant(true)}, pool_.get());
  ASSERT_EQ(false, resultVec->asFlatVector<bool>()->valueAt(0));
  ASSERT_EQ(true, resultVec->asFlatVector<bool>()->valueAt(1));

  auto min8 = std::numeric_limits<int8_t>::min();
  auto max8 = std::numeric_limits<int8_t>::max();
  resultVec = setVectorFromVariants(
      TINYINT(), {variant(min8), variant(max8)}, pool_.get());
  EXPECT_EQ(min8, resultVec->asFlatVector<int8_t>()->valueAt(0));
  EXPECT_EQ(max8, resultVec->asFlatVector<int8_t>()->valueAt(1));

  auto min16 = std::numeric_limits<int16_t>::min();
  auto max16 = std::numeric_limits<int16_t>::max();
  resultVec = setVectorFromVariants(
      SMALLINT(), {variant(min16), variant(max16)}, pool_.get());
  EXPECT_EQ(min16, resultVec->asFlatVector<int16_t>()->valueAt(0));
  EXPECT_EQ(max16, resultVec->asFlatVector<int16_t>()->valueAt(1));

  auto min32 = std::numeric_limits<int32_t>::min();
  auto max32 = std::numeric_limits<int32_t>::max();
  resultVec = setVectorFromVariants(
      INTEGER(), {variant(min32), variant(max32)}, pool_.get());
  EXPECT_EQ(min32, resultVec->asFlatVector<int32_t>()->valueAt(0));
  EXPECT_EQ(max32, resultVec->asFlatVector<int32_t>()->valueAt(1));

  auto min64 = std::numeric_limits<int64_t>::min();
  auto max64 = std::numeric_limits<int64_t>::max();
  resultVec = setVectorFromVariants(
      BIGINT(), {variant(min64), variant(max64)}, pool_.get());
  EXPECT_EQ(min64, resultVec->asFlatVector<int64_t>()->valueAt(0));
  EXPECT_EQ(max64, resultVec->asFlatVector<int64_t>()->valueAt(1));

  // Floats are harder to compare because of low-precision. Just making sure
  // they don't throw.
  EXPECT_NO_THROW(setVectorFromVariants(
      REAL(), {variant(float(0.99L)), variant(float(-1.99L))}, pool_.get()));

  resultVec = setVectorFromVariants(
      DOUBLE(), {variant(double(0.99L)), variant(double(-1.99L))}, pool_.get());
  ASSERT_EQ(double(0.99L), resultVec->asFlatVector<double>()->valueAt(0));
  ASSERT_EQ(double(-1.99L), resultVec->asFlatVector<double>()->valueAt(1));

  resultVec = setVectorFromVariants(
      VARCHAR(), {variant(""), variant("asdf")}, pool_.get());
  ASSERT_EQ("", resultVec->asFlatVector<StringView>()->valueAt(0).str());
  ASSERT_EQ("asdf", resultVec->asFlatVector<StringView>()->valueAt(1).str());

  ASSERT_ANY_THROW(setVectorFromVariants(
      VARBINARY(), {variant(""), variant("asdf")}, pool_.get()));

  resultVec = setVectorFromVariants(
      TIMESTAMP(),
      {variant(Timestamp(9020, 0)), variant(Timestamp(8875, 0))},
      pool_.get());
  ASSERT_EQ(
      "1970-01-01T02:30:20.000000000",
      resultVec->asFlatVector<Timestamp>()->valueAt(0).toString());
  ASSERT_EQ(
      "1970-01-01T02:27:55.000000000",
      resultVec->asFlatVector<Timestamp>()->valueAt(1).toString());

  resultVec = setVectorFromVariants(
      DATE(), {variant(9020), variant(8875)}, pool_.get());
  ASSERT_EQ(
      "1994-09-12",
      DATE()->toString(resultVec->asFlatVector<int32_t>()->valueAt(0)));
  ASSERT_EQ(
      "1994-04-20",
      DATE()->toString(resultVec->asFlatVector<int32_t>()->valueAt(1)));

  resultVec = setVectorFromVariants(
      INTERVAL_DAY_TIME(), {variant(9020LL), variant(8875LL)}, pool_.get());
  ASSERT_TRUE(resultVec->type()->isIntervalDayTime());
  ASSERT_EQ(9020, resultVec->asFlatVector<int64_t>()->valueAt(0));
  ASSERT_EQ(8875, resultVec->asFlatVector<int64_t>()->valueAt(1));

  resultVec = setVectorFromVariants(
      INTERVAL_YEAR_MONTH(), {variant(20), variant(30)}, pool_.get());
  ASSERT_TRUE(resultVec->type()->isIntervalYearMonth());
  ASSERT_EQ(20, resultVec->asFlatVector<int32_t>()->valueAt(0));
  ASSERT_EQ(30, resultVec->asFlatVector<int32_t>()->valueAt(1));
}

TEST_F(FunctionTest, getFunctionType) {
  std::vector<std::string> types =
      SubstraitParser::getSubFunctionTypes("sum:opt_i32");
  ASSERT_EQ("i32", types[0]);

  types = SubstraitParser::getSubFunctionTypes("sum:i32");
  ASSERT_EQ("i32", types[0]);

  types = SubstraitParser::getSubFunctionTypes("split:req_str_str");
  ASSERT_EQ(2, types.size());
  ASSERT_EQ("str", types[0]);
  ASSERT_EQ("str", types[1]);
}

TEST_F(FunctionTest, getInputTypes) {
  std::vector<TypePtr> types = SubstraitParser::getInputTypes("sum:opt_i32");
  ASSERT_EQ(types[0]->kind(), TypeKind::INTEGER);

  types = SubstraitParser::getInputTypes("and:opt_bool_bool");
  ASSERT_EQ(2, types.size());
  ASSERT_EQ(types[0]->kind(), TypeKind::BOOLEAN);
  ASSERT_EQ(types[1]->kind(), TypeKind::BOOLEAN);

  types = SubstraitParser::getInputTypes("function:i32_str_ts_date_fp64");
  ASSERT_EQ(types[0]->kind(), TypeKind::INTEGER);
  ASSERT_EQ(types[1]->kind(), TypeKind::VARCHAR);
  ASSERT_EQ(types[2]->kind(), TypeKind::TIMESTAMP);
  ASSERT_EQ(types[3]->kind(), TypeKind::INTEGER);
  ASSERT_EQ(types[4]->kind(), TypeKind::DOUBLE);
}

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

#include "gtest/gtest.h"

#include "velox/expression/CastExpr.h"
#include "velox/expression/CoalesceExpr.h"
#include "velox/expression/ConjunctExpr.h"
#include "velox/expression/ConstantExpr.h"
#include "velox/expression/RegisterSpecialForm.h"
#include "velox/expression/SpecialFormRegistry.h"
#include "velox/expression/SwitchExpr.h"
#include "velox/expression/TryExpr.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::core;
using namespace facebook::velox::exec;
using namespace facebook::velox::test;

class FunctionCallToSpecialFormTest : public testing::Test,
                                      public VectorTestBase {
 protected:
  static void SetUpTestCase() {
    registerFunctionCallToSpecialForms();
    memory::MemoryManager::testingSetInstance({});
  }
  const core::QueryConfig config_{{}};
};

TEST_F(FunctionCallToSpecialFormTest, andCall) {
  ASSERT_TRUE(isFunctionCallToSpecialFormRegistered("and"));

  auto type = resolveTypeForSpecialForm("and", {BOOLEAN(), BOOLEAN()});
  ASSERT_EQ(type, BOOLEAN());

  auto specialForm = constructSpecialForm(
      "and",
      BOOLEAN(),
      {std::make_shared<ConstantExpr>(
           vectorMaker_.constantVector<bool>({true})),
       std::make_shared<ConstantExpr>(
           vectorMaker_.constantVector<bool>({false}))},
      false,
      config_);
  ASSERT_EQ(typeid(*specialForm), typeid(const ConjunctExpr&));
}

TEST_F(FunctionCallToSpecialFormTest, castCall) {
  ASSERT_TRUE(isFunctionCallToSpecialFormRegistered("cast"));

  ASSERT_THROW(resolveTypeForSpecialForm("cast", {}), VeloxRuntimeError);

  auto specialForm = constructSpecialForm(
      "cast",
      DOUBLE(),
      {std::make_shared<ConstantExpr>(
          vectorMaker_.constantVector<int32_t>({0}))},
      false,
      config_);
  ASSERT_EQ(typeid(*specialForm), typeid(const CastExpr&));
}

TEST_F(FunctionCallToSpecialFormTest, coalesceCall) {
  ASSERT_TRUE(isFunctionCallToSpecialFormRegistered("coalesce"));

  auto type = resolveTypeForSpecialForm("coalesce", {BOOLEAN()});
  ASSERT_EQ(type, BOOLEAN());

  auto specialForm = constructSpecialForm(
      "coalesce",
      INTEGER(),
      {std::make_shared<ConstantExpr>(
          vectorMaker_.constantVector<int32_t>({0}))},
      false,
      config_);
  ASSERT_EQ(typeid(*specialForm), typeid(const CoalesceExpr&));
}

TEST_F(FunctionCallToSpecialFormTest, ifCall) {
  ASSERT_TRUE(isFunctionCallToSpecialFormRegistered("if"));

  auto type =
      resolveTypeForSpecialForm("if", {BOOLEAN(), INTEGER(), INTEGER()});
  ASSERT_EQ(type, INTEGER());

  auto specialForm = constructSpecialForm(
      "if",
      INTEGER(),
      {std::make_shared<ConstantExpr>(
           vectorMaker_.constantVector<bool>({true})),
       std::make_shared<ConstantExpr>(
           vectorMaker_.constantVector<int32_t>({0})),
       std::make_shared<ConstantExpr>(
           vectorMaker_.constantVector<int32_t>({1}))},
      false,
      config_);
  ASSERT_EQ(typeid(*specialForm), typeid(const SwitchExpr&));
}

TEST_F(FunctionCallToSpecialFormTest, orCall) {
  ASSERT_TRUE(isFunctionCallToSpecialFormRegistered("or"));

  auto type = resolveTypeForSpecialForm("or", {BOOLEAN(), BOOLEAN()});
  ASSERT_EQ(type, BOOLEAN());

  auto specialForm = constructSpecialForm(
      "or",
      BOOLEAN(),
      {std::make_shared<ConstantExpr>(
           vectorMaker_.constantVector<bool>({true})),
       std::make_shared<ConstantExpr>(
           vectorMaker_.constantVector<bool>({false}))},
      false,
      config_);
  ASSERT_EQ(typeid(*specialForm), typeid(const ConjunctExpr&));
}

TEST_F(FunctionCallToSpecialFormTest, switchCall) {
  ASSERT_TRUE(isFunctionCallToSpecialFormRegistered("switch"));

  auto type = resolveTypeForSpecialForm(
      "switch", {BOOLEAN(), INTEGER(), BOOLEAN(), INTEGER(), INTEGER()});
  ASSERT_EQ(type, INTEGER());

  auto specialForm = constructSpecialForm(
      "switch",
      INTEGER(),
      {std::make_shared<ConstantExpr>(
           vectorMaker_.constantVector<bool>({true})),
       std::make_shared<ConstantExpr>(
           vectorMaker_.constantVector<int32_t>({0})),
       std::make_shared<ConstantExpr>(
           vectorMaker_.constantVector<bool>({false})),
       std::make_shared<ConstantExpr>(
           vectorMaker_.constantVector<int32_t>({1})),
       std::make_shared<ConstantExpr>(
           vectorMaker_.constantVector<int32_t>({2}))},
      false,
      config_);
  ASSERT_EQ(typeid(*specialForm), typeid(const SwitchExpr&));
}

TEST_F(FunctionCallToSpecialFormTest, tryCall) {
  ASSERT_TRUE(isFunctionCallToSpecialFormRegistered("try"));

  auto type = resolveTypeForSpecialForm("try", {BOOLEAN()});
  ASSERT_EQ(type, BOOLEAN());

  auto specialForm = constructSpecialForm(
      "try",
      INTEGER(),
      {std::make_shared<ConstantExpr>(
          vectorMaker_.constantVector<int32_t>({0}))},
      false,
      config_);
  ASSERT_EQ(typeid(*specialForm), typeid(const TryExpr&));
}

TEST_F(FunctionCallToSpecialFormTest, notASpecialForm) {
  ASSERT_FALSE(isFunctionCallToSpecialFormRegistered("not_a_special_form"));

  auto type = resolveTypeForSpecialForm("not_a_special_form", {BOOLEAN()});
  ASSERT_EQ(type, nullptr);

  auto specialForm = constructSpecialForm(
      "not_a_special_form",
      INTEGER(),
      {std::make_shared<ConstantExpr>(
          vectorMaker_.constantVector<int32_t>({0}))},
      false,
      config_);
  ASSERT_EQ(specialForm, nullptr);
}

class FunctionCallToSpecialFormSanitizeNameTest : public testing::Test,
                                                  public VectorTestBase {
 protected:
  static void SetUpTestCase() {
    // This class does not pre-register the special forms.
    memory::MemoryManager::testingSetInstance({});
  }
};

TEST_F(FunctionCallToSpecialFormSanitizeNameTest, sanitizeName) {
  // Make sure no special forms are registered.
  unregisterAllFunctionCallToSpecialForm();

  ASSERT_FALSE(isFunctionCallToSpecialFormRegistered("and"));
  ASSERT_FALSE(isFunctionCallToSpecialFormRegistered("AND"));
  ASSERT_FALSE(isFunctionCallToSpecialFormRegistered("or"));
  ASSERT_FALSE(isFunctionCallToSpecialFormRegistered("OR"));

  registerFunctionCallToSpecialForm(
      "and", std::make_unique<ConjunctCallToSpecialForm>(true /* isAnd */));
  registerFunctionCallToSpecialForm(
      "OR", std::make_unique<ConjunctCallToSpecialForm>(false /* isAnd */));

  auto testLookup = [this](const std::string& name) {
    auto type = resolveTypeForSpecialForm(name, {BOOLEAN(), BOOLEAN()});
    ASSERT_EQ(type, BOOLEAN());

    auto specialForm = constructSpecialForm(
        name,
        BOOLEAN(),
        {std::make_shared<ConstantExpr>(
             vectorMaker_.constantVector<bool>({true})),
         std::make_shared<ConstantExpr>(
             vectorMaker_.constantVector<bool>({false}))},
        false,
        core::QueryConfig{{}});
    ASSERT_EQ(typeid(*specialForm), typeid(const ConjunctExpr&));
  };

  testLookup("and");
  testLookup("AND");
  testLookup("or");
  testLookup("OR");
}

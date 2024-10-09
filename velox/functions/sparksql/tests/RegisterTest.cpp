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

#include "velox/functions/sparksql/Register.h"

#include <gtest/gtest.h>

#include "velox/expression/CastExpr.h"
#include "velox/expression/SpecialFormRegistry.h"
#include "velox/functions/Registerer.h"
#include "velox/functions/tests/RegistryTestUtil.h"

namespace facebook::velox::functions::sparksql::test {

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_vector_func_one,
    VectorFuncOne::signatures(),
    std::make_unique<VectorFuncOne>());

class RegisterTest : public testing::Test {
 public:
  RegisterTest() {
    registerFunction<FuncTwo, int64_t, double, double>(
        {"func_two_double", "Func_Two_Double_Alias"});
    registerFunction<FuncTwo, int64_t, int64_t, int64_t>({"func_two_bigint"});

    VELOX_REGISTER_VECTOR_FUNCTION(udf_vector_func_one, "vector_func_one");
    VELOX_REGISTER_VECTOR_FUNCTION(
        udf_vector_func_one, "Vector_Func_One_Alias");

    exec::registerFunctionCallToSpecialForm(
        "cast", std::make_unique<exec::CastCallToSpecialForm>());
  }
};

TEST_F(RegisterTest, listFunctionNames) {
  auto names = listFunctionNames();
  EXPECT_EQ(names.size(), 6);
  std::sort(names.begin(), names.end());

  EXPECT_EQ(names[0], "cast");
  EXPECT_EQ(names[1], "func_two_bigint");
  EXPECT_EQ(names[2], "func_two_double");
  EXPECT_EQ(names[3], "func_two_double_alias");
  EXPECT_EQ(names[4], "vector_func_one");
  EXPECT_EQ(names[5], "vector_func_one_alias");
}

} // namespace facebook::velox::functions::sparksql::test

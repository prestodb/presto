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

#include "velox/substrait/VeloxSubstraitSignature.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

using namespace facebook::velox;
using namespace facebook::velox::substrait;

namespace facebook::velox::substrait::test {

class VeloxSubstraitSignatureTest : public ::testing::Test {
 protected:
  void SetUp() override {
    Test::SetUp();
    functions::prestosql::registerAllScalarFunctions();
  }

  static TypePtr fromSubstraitSignature(const std::string& signature) {
    return VeloxSubstraitSignature::fromSubstraitSignature(signature);
  }

  static std::string toSubstraitSignature(const TypePtr& type) {
    return VeloxSubstraitSignature::toSubstraitSignature(type);
  }

  static std::string toSubstraitSignature(
      const std::string& functionName,
      const std::vector<TypePtr>& arguments) {
    return VeloxSubstraitSignature::toSubstraitSignature(
        functionName, arguments);
  }
};

TEST_F(VeloxSubstraitSignatureTest, toSubstraitSignatureWithType) {
  ASSERT_EQ(toSubstraitSignature(BOOLEAN()), "bool");

  ASSERT_EQ(toSubstraitSignature(TINYINT()), "i8");
  ASSERT_EQ(toSubstraitSignature(SMALLINT()), "i16");
  ASSERT_EQ(toSubstraitSignature(INTEGER()), "i32");
  ASSERT_EQ(toSubstraitSignature(BIGINT()), "i64");
  ASSERT_EQ(toSubstraitSignature(REAL()), "fp32");
  ASSERT_EQ(toSubstraitSignature(DOUBLE()), "fp64");
  ASSERT_EQ(toSubstraitSignature(VARCHAR()), "str");
  ASSERT_EQ(toSubstraitSignature(VARBINARY()), "vbin");
  ASSERT_EQ(toSubstraitSignature(TIMESTAMP()), "ts");
  ASSERT_EQ(toSubstraitSignature(DATE()), "date");
  ASSERT_EQ(toSubstraitSignature(ARRAY(BOOLEAN())), "list");
  ASSERT_EQ(toSubstraitSignature(ARRAY(INTEGER())), "list");
  ASSERT_EQ(toSubstraitSignature(MAP(INTEGER(), BIGINT())), "map");
  ASSERT_EQ(toSubstraitSignature(ROW({INTEGER(), BIGINT()})), "struct");
  ASSERT_EQ(toSubstraitSignature(ROW({ARRAY(INTEGER())})), "struct");
  ASSERT_EQ(toSubstraitSignature(ROW({MAP(INTEGER(), INTEGER())})), "struct");
  ASSERT_EQ(toSubstraitSignature(ROW({ROW({INTEGER()})})), "struct");
  ASSERT_EQ(toSubstraitSignature(UNKNOWN()), "u!name");
}

TEST_F(
    VeloxSubstraitSignatureTest,
    toSubstraitSignatureWithFunctionNameAndArguments) {
  ASSERT_EQ(toSubstraitSignature("eq", {INTEGER(), INTEGER()}), "eq:i32_i32");
  ASSERT_EQ(toSubstraitSignature("gt", {INTEGER(), INTEGER()}), "gt:i32_i32");
  ASSERT_EQ(toSubstraitSignature("lt", {INTEGER(), INTEGER()}), "lt:i32_i32");
  ASSERT_EQ(toSubstraitSignature("gte", {INTEGER(), INTEGER()}), "gte:i32_i32");
  ASSERT_EQ(toSubstraitSignature("lte", {INTEGER(), INTEGER()}), "lte:i32_i32");

  ASSERT_EQ(
      toSubstraitSignature("and", {BOOLEAN(), BOOLEAN()}), "and:bool_bool");
  ASSERT_EQ(toSubstraitSignature("or", {BOOLEAN(), BOOLEAN()}), "or:bool_bool");
  ASSERT_EQ(toSubstraitSignature("not", {BOOLEAN()}), "not:bool");
  ASSERT_EQ(
      toSubstraitSignature("xor", {BOOLEAN(), BOOLEAN()}), "xor:bool_bool");

  ASSERT_EQ(
      toSubstraitSignature("between", {INTEGER(), INTEGER(), INTEGER()}),
      "between:i32_i32_i32");

  ASSERT_EQ(
      toSubstraitSignature("plus", {INTEGER(), INTEGER()}), "plus:i32_i32");
  ASSERT_EQ(
      toSubstraitSignature("divide", {INTEGER(), INTEGER()}), "divide:i32_i32");

  ASSERT_EQ(
      toSubstraitSignature("cardinality", {ARRAY(INTEGER())}),
      "cardinality:list");
  ASSERT_EQ(
      toSubstraitSignature("array_sum", {ARRAY(INTEGER())}), "array_sum:list");

  ASSERT_EQ(toSubstraitSignature("sum", {INTEGER()}), "sum:i32");
  ASSERT_EQ(toSubstraitSignature("avg", {INTEGER()}), "avg:i32");
  ASSERT_EQ(toSubstraitSignature("count", {INTEGER()}), "count:i32");

  auto functionType = std::make_shared<const FunctionType>(
      std::vector<TypePtr>{INTEGER(), VARCHAR()}, BIGINT());
  std::vector<TypePtr> types = {MAP(INTEGER(), VARCHAR()), functionType};
  ASSERT_ANY_THROW(toSubstraitSignature("transform_keys", std::move(types)));
}

TEST_F(VeloxSubstraitSignatureTest, fromSubstraitSignature) {
  ASSERT_EQ(fromSubstraitSignature("bool")->kind(), TypeKind::BOOLEAN);
  ASSERT_EQ(fromSubstraitSignature("i8")->kind(), TypeKind::TINYINT);
  ASSERT_EQ(fromSubstraitSignature("i16")->kind(), TypeKind::SMALLINT);
  ASSERT_EQ(fromSubstraitSignature("i32")->kind(), TypeKind::INTEGER);
  ASSERT_EQ(fromSubstraitSignature("i64")->kind(), TypeKind::BIGINT);
  ASSERT_EQ(fromSubstraitSignature("fp32")->kind(), TypeKind::REAL);
  ASSERT_EQ(fromSubstraitSignature("fp64")->kind(), TypeKind::DOUBLE);
  ASSERT_EQ(fromSubstraitSignature("str")->kind(), TypeKind::VARCHAR);
  ASSERT_EQ(fromSubstraitSignature("vbin")->kind(), TypeKind::VARBINARY);
  ASSERT_EQ(fromSubstraitSignature("ts")->kind(), TypeKind::TIMESTAMP);
  ASSERT_EQ(fromSubstraitSignature("date")->kind(), TypeKind::INTEGER);
  ASSERT_ANY_THROW(fromSubstraitSignature("other")->kind());
}

} // namespace facebook::velox::substrait::test

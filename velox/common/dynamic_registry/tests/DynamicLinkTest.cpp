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

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/dynamic_registry/DynamicLibraryLoader.h"
#include "velox/expression/SimpleFunctionRegistry.h"
#include "velox/functions/FunctionRegistry.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

namespace facebook::velox::functions::test {

class DynamicLinkTest : public FunctionBaseTest {};

namespace {
std::string getLibraryPath(const std::string& filename) {
  return fmt::format(
      "{}/{}{}",
      VELOX_TEST_DYNAMIC_LIBRARY_PATH,
      filename,
      VELOX_TEST_DYNAMIC_LIBRARY_PATH_SUFFIX);
}
} // namespace

TEST_F(DynamicLinkTest, dynamicLoadFunc) {
  const auto dynamicFunction = [&]() {
    return evaluateOnce<int64_t>("dynamic()", makeRowVector(ROW({}), 1));
  };

  const auto dynamicFunctionNestedCall = [&]() {
    return evaluateOnce<int64_t>(
        "mod(dynamic(), 10)", makeRowVector(ROW({}), 1));
  };

  VELOX_ASSERT_THROW(
      dynamicFunction(), "Scalar function doesn't exist: dynamic.");

  auto signaturesBefore = getFunctionSignatures().size();
  std::string libraryPath = getLibraryPath("libvelox_function_dynamic");
  loadDynamicLibrary(libraryPath);
  auto signaturesAfter = getFunctionSignatures().size();
  EXPECT_EQ(signaturesAfter, signaturesBefore + 1);
  EXPECT_EQ(123, dynamicFunction());

  auto& registry = exec::simpleFunctions();
  auto resolved = registry.resolveFunction("dynamic", {});
  EXPECT_EQ(TypeKind::BIGINT, resolved->type()->kind());

  EXPECT_EQ(3, dynamicFunctionNestedCall());
}

TEST_F(DynamicLinkTest, dynamicLoadSameFuncTwice) {
  const auto dynamicFunction = [&]() {
    return evaluateOnce<int64_t>("dynamic()", makeRowVector(ROW({}), 1));
  };
  auto& registry = exec::simpleFunctions();
  std::string libraryPath = getLibraryPath("libvelox_function_dynamic");

  loadDynamicLibrary(libraryPath);
  auto resolved = registry.resolveFunction("dynamic", {});
  EXPECT_EQ(TypeKind::BIGINT, resolved->type()->kind());

  auto signaturesBefore = getFunctionSignatures().size();

  loadDynamicLibrary(libraryPath);
  auto signaturesAfterSecond = getFunctionSignatures().size();
  EXPECT_EQ(signaturesAfterSecond, signaturesBefore);
  auto resolvedAfterSecond = registry.resolveFunction("dynamic", {});
  EXPECT_EQ(TypeKind::BIGINT, resolvedAfterSecond->type()->kind());
}

TEST_F(DynamicLinkTest, dynamicLoadOverwriteFunc) {
  const auto dynamicIntFunction = [&]() {
    return evaluateOnce<int64_t>(
        "dynamic_overwrite()", makeRowVector(ROW({}), 1));
  };
  const auto dynamicVarcharFunction = [&]() {
    return evaluateOnce<std::string>(
        "dynamic_overwrite()", makeRowVector(ROW({}), 1));
  };

  auto& registry = exec::simpleFunctions();
  auto signaturesBefore = getFunctionSignatures().size();

  VELOX_ASSERT_THROW(
      dynamicVarcharFunction(),
      "Scalar function doesn't exist: dynamic_overwrite.");

  std::string libraryPath =
      getLibraryPath("libvelox_overwrite_varchar_function_dynamic");
  loadDynamicLibrary(libraryPath);
  auto signaturesAfterFirst = getFunctionSignatures().size();
  EXPECT_EQ(signaturesAfterFirst, signaturesBefore + 1);
  EXPECT_EQ("123", dynamicVarcharFunction());
  auto resolved = registry.resolveFunction("dynamic_overwrite", {});
  EXPECT_EQ(TypeKind::VARCHAR, resolved->type()->kind());

  VELOX_ASSERT_THROW(
      dynamicIntFunction(),
      "Expression evaluation result is not of expected type: dynamic_overwrite() -> CONSTANT vector of type VARCHAR");

  std::string libraryPathInt =
      getLibraryPath("libvelox_overwrite_int_function_dynamic");
  loadDynamicLibrary(libraryPathInt);

  // The first function loaded should be overwritten.
  VELOX_ASSERT_THROW(
      dynamicVarcharFunction(),
      "Expression evaluation result is not of expected type: dynamic_overwrite() -> CONSTANT vector of type BIGINT");
  EXPECT_EQ(123, dynamicIntFunction());
  auto signaturesAfterSecond = getFunctionSignatures().size();
  EXPECT_EQ(signaturesAfterSecond, signaturesAfterFirst);
  auto resolvedAfterSecond = registry.resolveFunction("dynamic_overwrite", {});
  EXPECT_EQ(TypeKind::BIGINT, resolvedAfterSecond->type()->kind());
}

TEST_F(DynamicLinkTest, dynamicLoadErrFunc) {
  const auto dynamicFunctionFail = [&](const std::optional<int64_t> a,
                                       std::optional<int64_t> b) {
    return evaluateOnce<int64_t>("dynamic_err(c0)", a, b);
  };

  const auto dynamicFunctionSuccess =
      [&](const facebook::velox::RowVectorPtr& arr) {
        return evaluateOnce<int64_t>("dynamic_err(c0)", arr);
      };

  auto signaturesBefore = getFunctionSignatures().size();
  VELOX_ASSERT_THROW(
      dynamicFunctionFail(0, 0), "Scalar function doesn't exist: dynamic_err.");

  std::string libraryPath = getLibraryPath("libvelox_function_err_dynamic");
  loadDynamicLibrary(libraryPath);

  auto signaturesAfter = getFunctionSignatures().size();
  EXPECT_EQ(signaturesAfter, signaturesBefore + 1);

  // Expecting a fail because we are not passing in an array.
  VELOX_ASSERT_THROW(
      dynamicFunctionFail(0, 0),
      "Scalar function signature is not supported: dynamic_err(BIGINT). Supported signatures: (array(bigint)) -> bigint.");

  auto check = makeRowVector(
      {makeNullableArrayVector(std::vector<std::vector<std::optional<int64_t>>>{
          {0, 1, 3, 4, 5, 6, 7, 8, 9}})});

  // Expecting a success because we are passing in an array.
  EXPECT_EQ(9, dynamicFunctionSuccess(check));
}

TEST_F(DynamicLinkTest, dynamicLoadOverloadFunc) {
  const auto dynamicIntFunction = [&](const std::optional<int64_t> a) {
    return evaluateOnce<int64_t>("dynamic_overload(c0)", a);
  };
  const auto dynamicVarcharFunction = [&](const std::optional<std::string> a) {
    return evaluateOnce<std::string>("dynamic_overload(c0)", a);
  };

  auto& registry = exec::simpleFunctions();
  auto signaturesBefore = getFunctionSignatures().size();

  VELOX_ASSERT_THROW(
      dynamicVarcharFunction("1"),
      "Scalar function doesn't exist: dynamic_overload.");

  std::string libraryPath =
      getLibraryPath("libvelox_overload_varchar_function_dynamic");
  loadDynamicLibrary(libraryPath);
  auto signaturesAfterFirst = getFunctionSignatures().size();
  EXPECT_EQ(signaturesAfterFirst, signaturesBefore + 1);
  EXPECT_EQ("1", dynamicVarcharFunction("1"));
  auto resolved = registry.resolveFunction("dynamic_overload", {VARCHAR()});
  EXPECT_EQ(TypeKind::VARCHAR, resolved->type()->kind());

  // We expect no dynamic_5 with int arguments yet.
  VELOX_ASSERT_THROW(
      dynamicIntFunction(0),
      "Scalar function signature is not supported: dynamic_overload(BIGINT). Supported signatures: (varchar) -> varchar.");

  std::string libraryPathInt =
      getLibraryPath("libvelox_overload_int_function_dynamic");
  loadDynamicLibrary(libraryPathInt);

  // The first function loaded should NOT be overwritten.
  EXPECT_EQ("0", dynamicVarcharFunction("0"));
  EXPECT_EQ(0, dynamicIntFunction(0));
  auto signaturesAfterSecond = getFunctionSignatures().size();
  EXPECT_EQ(signaturesAfterSecond, signaturesAfterFirst);
  auto resolvedAfterSecond =
      registry.resolveFunction("dynamic_overload", {BIGINT()});
  EXPECT_EQ(TypeKind::BIGINT, resolvedAfterSecond->type()->kind());
}

} // namespace facebook::velox::functions::test

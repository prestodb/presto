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

#include "velox/core/SimpleFunctionMetadata.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/functions/prestosql/types/JsonType.h"
#include "velox/type/Type.h"

// Test for simple function type analysis.
namespace facebook::velox::core {
namespace {
class TypeAnalysisTest : public testing::Test {
 protected:
  template <typename... Args>
  void testHasGeneric(bool expected) {
    TypeAnalysisResults results;
    (TypeAnalysis<Args>().run(results), ...);
    ASSERT_EQ(expected, results.stats.hasGeneric);
  }

  template <typename... Args>
  void testHasVariadic(bool expected) {
    TypeAnalysisResults results;
    (TypeAnalysis<Args>().run(results), ...);
    ASSERT_EQ(expected, results.stats.hasVariadic);
  }

  template <typename... Args>
  void testHasVariadicOfGeneric(bool expected) {
    TypeAnalysisResults results;
    (TypeAnalysis<Args>().run(results), ...);
    ASSERT_EQ(expected, results.stats.hasVariadicOfGeneric);
  }

  template <typename... Args>
  void testCountConcrete(size_t expected) {
    TypeAnalysisResults results;
    (TypeAnalysis<Args>().run(results), ...);
    ASSERT_EQ(expected, results.stats.concreteCount);
  }

  template <typename... Args>
  void testStringType(const std::vector<std::string>& expected) {
    TypeAnalysisResults results;
    std::vector<std::string> types;

    (
        [&]() {
          // Clear string representation but keep other collected information to
          // accumulate.
          results.resetTypeString();
          TypeAnalysis<Args>().run(results);
          types.push_back(results.typeAsString());
        }(),
        ...);
    ASSERT_EQ(expected, types);
  }

  template <typename... Args>
  void testPhysicalType(const TypePtr& expected) {
    TypeAnalysisResults results;
    (TypeAnalysis<Args>().run(results), ...);
    ASSERT_EQ(expected->toString(), results.physicalType->toString());
  }

  template <typename... Args>
  void testVariables(
      const std::map<std::string, exec::SignatureVariable>& expected) {
    TypeAnalysisResults results;
    (TypeAnalysis<Args>().run(results), ...);
    ASSERT_EQ(expected, results.variablesInformation);
  }

  template <typename... Args>
  void testRank(uint32_t expected) {
    TypeAnalysisResults results;
    (TypeAnalysis<Args>().run(results), ...);
    ASSERT_EQ(expected, results.stats.getRank());
  }

  template <typename... Args>
  uint32_t getPriority() {
    TypeAnalysisResults results;
    (TypeAnalysis<Args>().run(results), ...);
    return results.stats.computePriority();
  }
};

TEST_F(TypeAnalysisTest, hasGeneric) {
  testHasGeneric<int32_t>(false);
  testHasGeneric<int32_t, int32_t>(false);
  testHasGeneric<Variadic<int32_t>>(false);
  testHasGeneric<Map<Array<int32_t>, Array<int32_t>>>(false);

  testHasGeneric<Map<Array<Any>, Array<int32_t>>>(true);
  testHasGeneric<Map<Array<Generic<T1>>, Array<int32_t>>>(true);
  testHasGeneric<Map<Array<Generic<T1, true, true>>, Array<int32_t>>>(true);
  testHasGeneric<Map<Array<Generic<T1, true, false>>, Array<int32_t>>>(true);
  testHasGeneric<Map<Array<Comparable<T1>>, Array<int32_t>>>(true);
  testHasGeneric<Map<Array<Orderable<T1>>, Array<int32_t>>>(true);

  testHasGeneric<Map<Array<int32_t>, Any>>(true);
  testHasGeneric<Variadic<Any>>(true);
  testHasGeneric<Any>(true);
  testHasGeneric<int32_t, Any>(true);
  testHasGeneric<Any, int32_t>(true);
}

TEST_F(TypeAnalysisTest, hasVariadic) {
  testHasVariadic<int32_t>(false);
  testHasVariadic<Map<Array<int32_t>, Array<int32_t>>>(false);
  testHasVariadic<Map<Array<int32_t>, Any>>(false);
  testHasVariadic<int32_t, Array<int32_t>>(false);

  testHasVariadic<Variadic<int32_t>>(true);
  testHasVariadic<Variadic<Any>>(true);
  testHasVariadic<Variadic<int64_t>, Array<int32_t>>(true);
  testHasVariadic<int32_t, Variadic<Array<int32_t>>>(true);
}

TEST_F(TypeAnalysisTest, hasVariadicOfGeneric) {
  testHasVariadicOfGeneric<int32_t>(false);
  testHasVariadicOfGeneric<Map<Array<int32_t>, Array<int32_t>>>(false);
  testHasVariadicOfGeneric<Map<Array<int32_t>, Any>>(false);
  testHasVariadicOfGeneric<int32_t, Array<int32_t>>(false);
  testHasVariadicOfGeneric<Variadic<int32_t>>(false);
  testHasVariadicOfGeneric<Variadic<int64_t>, Array<int32_t>>(false);
  testHasVariadicOfGeneric<int32_t, Variadic<Array<int32_t>>>(false);
  testHasVariadicOfGeneric<Variadic<int32_t>, Any>(false);
  testHasVariadicOfGeneric<Any, Variadic<int32_t>>(false);

  testHasVariadicOfGeneric<Variadic<Any>>(true);
  testHasVariadicOfGeneric<Variadic<Comparable<T2>>>(true);
  testHasVariadicOfGeneric<Variadic<Generic<T2, true, false>>>(true);

  testHasVariadicOfGeneric<Variadic<Any>, int32_t>(true);
  testHasVariadicOfGeneric<int32_t, Variadic<Array<Any>>>(true);
  testHasVariadicOfGeneric<int32_t, Variadic<Map<int64_t, Array<Generic<T1>>>>>(
      true);
}

TEST_F(TypeAnalysisTest, countConcrete) {
  testCountConcrete<>(0);
  testCountConcrete<int32_t>(1);
  testCountConcrete<int32_t, int32_t>(2);
  testCountConcrete<int32_t, int32_t, double>(3);
  testCountConcrete<Any>(0);
  testCountConcrete<Generic<T1>>(0);
  testCountConcrete<Generic<T1, true, false>>(0);
  testCountConcrete<Orderable<T1>>(0);

  testCountConcrete<Variadic<Any>>(0);
  testCountConcrete<Variadic<int32_t>>(1);
  testCountConcrete<Variadic<Array<Any>>>(1);

  testCountConcrete<Map<Array<int32_t>, Array<int32_t>>>(5);
  testCountConcrete<Map<Array<int32_t>, Any>>(3);
  testCountConcrete<int32_t, Array<int32_t>>(3);
  testCountConcrete<Variadic<int64_t>, Array<int32_t>>(3);
  testCountConcrete<int32_t, Variadic<Array<int32_t>>>(3);
  testCountConcrete<Variadic<int32_t>, Any>(1);
  testCountConcrete<Any, Variadic<int32_t>>(1);

  testCountConcrete<Variadic<Any>>(0);
  testCountConcrete<Variadic<Any>, int32_t>(1);
  testCountConcrete<int32_t, Variadic<Array<Any>>>(2);
}

TEST_F(TypeAnalysisTest, testStringType) {
  testStringType<int32_t>({"integer"});
  testStringType<int64_t>({"bigint"});
  testStringType<double>({"double"});
  testStringType<float>({"real"});
  testStringType<Date>({"date"});

  testStringType<ShortDecimal<P1, S1>>({"decimal(i1,i5)"});
  testStringType<LongDecimal<P1, S1>>({"decimal(i1,i5)"});

  testStringType<Array<int32_t>>({"array(integer)"});
  testStringType<Map<Any, int32_t>>({"map(any, integer)"});
  testStringType<Row<int32_t, int32_t>>({"row(integer, integer)"});

  testStringType<Any>({"any"});
  testStringType<Generic<T1>>({"__user_T1"});

  testStringType<Variadic<int32_t>>({"integer"});

  testStringType<int32_t, int64_t, Map<Array<int32_t>, Generic<T2>>>({
      "integer",
      "bigint",
      "map(array(integer), __user_T2)",
  });

  testStringType<int32_t, int64_t, Map<Array<int32_t>, Orderable<T2>>>({
      "integer",
      "bigint",
      "map(array(integer), __user_T2)",
  });
  testStringType<int32_t, int64_t, Map<Array<int32_t>, Comparable<T2>>>({
      "integer",
      "bigint",
      "map(array(integer), __user_T2)",
  });
  testStringType<Array<int32_t>>({"array(integer)"});
  testStringType<Map<int64_t, double>>({"map(bigint, double)"});
  testStringType<Row<Any, double, Generic<T1>>>(
      {"row(any, double, __user_T1)"});

  testStringType<Json>({"json"});
  testStringType<Array<Json>>({"array(json)"});
}

TEST_F(TypeAnalysisTest, testVariables) {
  testVariables<int32_t>({});
  testVariables<Array<int32_t>>({});
  testVariables<Any>({});

  testVariables<Generic<T1>>(
      {{"__user_T1",
        exec::SignatureVariable(
            "__user_T1",
            std::nullopt,
            exec::ParameterType::kTypeParameter,
            false,
            false,
            false)}});

  testVariables<Orderable<T1>>(
      {{"__user_T1",
        exec::SignatureVariable(
            "__user_T1",
            std::nullopt,
            exec::ParameterType::kTypeParameter,
            false,
            true /*orderableTypesOnly*/,
            true)}});

  testVariables<Generic<T1, true, true>>(
      {{"__user_T1",
        exec::SignatureVariable(
            "__user_T1",
            std::nullopt,
            exec::ParameterType::kTypeParameter,
            false,
            true /*orderableTypesOnly*/,
            true /*comparableTypesOnly*/)}});

  testVariables<Comparable<T1>>(
      {{"__user_T1",
        exec::SignatureVariable(
            "__user_T1",
            std::nullopt,
            exec::ParameterType::kTypeParameter,
            false,
            false /*orderableTypesOnly*/,
            true /*comparableTypesOnly*/)}});

  testVariables<Map<Any, int32_t>>({});
  testVariables<Variadic<int32_t>>({});
  testVariables<int32_t, Generic<T5>, Map<Array<int32_t>, Orderable<T2>>>(
      {{"__user_T5",
        exec::SignatureVariable(
            "__user_T5",
            std::nullopt,
            exec::ParameterType::kTypeParameter,
            false,
            false /*orderableTypesOnly*/,
            false /*comparableTypesOnly*/)},
       {"__user_T2",
        exec::SignatureVariable(
            "__user_T2",
            std::nullopt,
            exec::ParameterType::kTypeParameter,
            false,
            true /*orderableTypesOnly*/,
            true /*comparableTypesOnly*/)}});

  testVariables<LongDecimal<P1, S1>>({
      {"i1",
       exec::SignatureVariable(
           "i1",
           std::nullopt,
           exec::ParameterType::kIntegerParameter,
           false,
           false /*orderableTypesOnly*/,
           false /*comparableTypesOnly*/)},
      {"i5",
       exec::SignatureVariable(
           "i5",
           std::nullopt,
           exec::ParameterType::kIntegerParameter,
           false,
           false /*orderableTypesOnly*/,
           false /*comparableTypesOnly*/)},
  });

  testVariables<ShortDecimal<P2, S2>>({
      {"i2",
       exec::SignatureVariable(
           "i2",
           std::nullopt,
           exec::ParameterType::kIntegerParameter,
           false,
           false /*orderableTypesOnly*/,
           false /*comparableTypesOnly*/)},
      {"i6",
       exec::SignatureVariable(
           "i6",
           std::nullopt,
           exec::ParameterType::kIntegerParameter,
           false,
           false /*orderableTypesOnly*/,
           false /*comparableTypesOnly*/)},
  });
}

TEST_F(TypeAnalysisTest, testRank) {
  testRank<int32_t>(1);
  testRank<Array<int32_t>>(1);
  testRank<Array<int32_t>, int, double>(1);

  testRank<Variadic<int32_t>>(2);
  testRank<Array<int32_t>, int, Variadic<int32_t>>(2);
  testRank<Variadic<Array<int32_t>>>(2);

  testRank<Any>(3);
  testRank<Array<int32_t>, Any, Variadic<int32_t>>(3);
  testRank<Array<int32_t>, Generic<T2>>(3);
  testRank<Array<int32_t>, Comparable<T2>>(3);

  testRank<Array<Any>, Generic<T2>>(3);
  testRank<Array<Any>, int32_t>(3);
  testRank<Array<int32_t>, Any, Any>(3);

  testRank<Variadic<Any>>(4);
  testRank<Array<int32_t>, Any, Variadic<Array<Any>>>(4);
  testRank<Array<int32_t>, Any, Variadic<Array<Generic<T2, true, true>>>>(4);
}

TEST_F(TypeAnalysisTest, testPriority) {
  static size_t count = 0;
  auto test = [&](auto a, auto b) {
    ASSERT_LT(a, b) << "test id:" << count++;
    count++;
  };

  test(getPriority<int32_t, int32_t>(), getPriority<Variadic<int32_t>>());

  test(getPriority<Variadic<int32_t>>(), getPriority<Variadic<Any>>());

  test(getPriority<Variadic<int32_t>>(), getPriority<Any, Any>());

  test(getPriority<Any, Any>(), getPriority<Variadic<Any>>());

  test(getPriority<int32_t, Any>(), getPriority<Any, Any>());

  test(
      getPriority<Any, Variadic<Array<Any>>>(),
      getPriority<Any, Variadic<Any>>());
}

TEST_F(TypeAnalysisTest, physicalType) {
  testPhysicalType<bool>(BOOLEAN());
  testPhysicalType<int32_t>(INTEGER());
  testPhysicalType<int64_t>(BIGINT());
  testPhysicalType<float>(REAL());
  testPhysicalType<double>(DOUBLE());
  testPhysicalType<Date>(INTEGER());
  testPhysicalType<Timestamp>(TIMESTAMP());
  testPhysicalType<Varchar>(VARCHAR());
  testPhysicalType<Varbinary>(VARBINARY());

  testPhysicalType<ShortDecimal<P1, S1>>(BIGINT());
  testPhysicalType<LongDecimal<P1, S1>>(HUGEINT());

  testPhysicalType<Array<int32_t>>(ARRAY(INTEGER()));
  testPhysicalType<Array<Date>>(ARRAY(INTEGER()));
  testPhysicalType<Array<Array<float>>>(ARRAY(ARRAY(REAL())));
  testPhysicalType<Array<Generic<T1>>>(ARRAY(UNKNOWN()));
  testPhysicalType<Array<Array<Generic<T1>>>>(ARRAY(ARRAY(UNKNOWN())));
  testPhysicalType<Array<ShortDecimal<P1, S1>>>(ARRAY(BIGINT()));

  testPhysicalType<Map<int32_t, Varchar>>(MAP(INTEGER(), VARCHAR()));
  testPhysicalType<Map<int32_t, Array<Date>>>(MAP(INTEGER(), ARRAY(INTEGER())));
  testPhysicalType<Map<Generic<T1>, Generic<T2>>>(MAP(UNKNOWN(), UNKNOWN()));
  testPhysicalType<Map<Generic<T1>, Array<Generic<T2>>>>(
      MAP(UNKNOWN(), ARRAY(UNKNOWN())));
  testPhysicalType<Map<int32_t, LongDecimal<P1, S1>>>(
      MAP(INTEGER(), HUGEINT()));

  testPhysicalType<Row<int32_t, Array<double>, Variadic<bool>>>(
      ROW({INTEGER(), ARRAY(DOUBLE()), BOOLEAN()}));

  testPhysicalType<Json>(VARCHAR());
  testPhysicalType<Array<Json>>(ARRAY(VARCHAR()));

  testPhysicalType<Any>(UNKNOWN());
  testPhysicalType<Row<Date, Any>>(ROW({INTEGER(), UNKNOWN()}));
}

} // namespace
} // namespace facebook::velox::core

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

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include "velox/benchmarks/ExpressionBenchmarkBuilder.h"

using namespace facebook;

using namespace facebook::velox;

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  memory::MemoryManager::initialize({});

  ExpressionBenchmarkBuilder benchmarkBuilder;
  const vector_size_t vectorSize = 1000;
  auto vectorMaker = benchmarkBuilder.vectorMaker();
  auto emptyInput = vectorMaker.flatVector<std::string>(
      vectorSize, [](auto /*row*/) { return ""; });
  auto validInput = vectorMaker.flatVector<std::string>(
      vectorSize, [](auto row) { return std::to_string(row); });
  auto invalidInput = vectorMaker.flatVector<std::string>(
      vectorSize, [](auto /*row*/) { return "$"; });
  auto validDoubleStringInput = vectorMaker.flatVector<std::string>(
      vectorSize, [](auto row) { return fmt::format("{}.12345678910", row); });
  auto validNaNInput = vectorMaker.flatVector<std::string>(
      vectorSize, [](auto /*row*/) { return "NaN"; });
  auto validInfinityInput = vectorMaker.flatVector<std::string>(
      vectorSize, [](auto /*row*/) { return "Infinity"; });
  auto invalidNaNInput = vectorMaker.flatVector<std::string>(
      vectorSize, [](auto /*row*/) { return "nan"; });
  auto invalidInfinityInput = vectorMaker.flatVector<std::string>(
      vectorSize, [](auto /*row*/) { return "infinity"; });
  auto spaceInput = vectorMaker.flatVector<std::string>(
      vectorSize, [](auto /*row*/) { return "   "; });
  auto integerInput = vectorMaker.flatVector<int32_t>(
      vectorSize, [&](auto j) { return 12345 * j; }, nullptr);
  auto bigintInput = vectorMaker.flatVector<int64_t>(
      vectorSize,
      [&](auto j) {
        return facebook::velox::HugeInt::build(12345 * j, 56789 * j + 12345);
      },
      nullptr);
  auto decimalInput = vectorMaker.flatVector<int64_t>(
      vectorSize, [&](auto j) { return 12345 * j; }, nullptr, DECIMAL(9, 2));
  auto shortDecimalInput = vectorMaker.flatVector<int64_t>(
      vectorSize,
      [&](auto j) { return 123456789 * j; },
      nullptr,
      DECIMAL(18, 6));
  auto longDecimalInput = vectorMaker.flatVector<int128_t>(
      vectorSize,
      [&](auto j) {
        return facebook::velox::HugeInt::build(12345 * j, 56789 * j + 12345);
      },
      nullptr,
      DECIMAL(38, 16));
  auto largeRealInput = vectorMaker.flatVector<float>(
      vectorSize, [&](auto j) { return 12345678.0 * j; });
  auto smallRealInput = vectorMaker.flatVector<float>(
      vectorSize, [&](auto j) { return 1.2345678 * j; });
  auto smallDoubleInput = vectorMaker.flatVector<double>(
      vectorSize, [&](auto j) { return -0.00012345678 / j; });
  auto largeDoubleInput = vectorMaker.flatVector<double>(
      vectorSize, [&](auto j) { return -123456.7 / j; });
  auto timestampInput =
      vectorMaker.flatVector<Timestamp>(vectorSize, [&](auto j) {
        return Timestamp(1695859694 + j / 1000, j % 1000 * 1'000'000);
      });
  auto validDateStrings = vectorMaker.flatVector<std::string>(
      vectorSize,
      [](auto row) { return fmt::format("2024-05-{:02d}", 1 + row % 30); });
  auto invalidDateStrings = vectorMaker.flatVector<std::string>(
      vectorSize, [](auto row) { return fmt::format("2024-05...{}", row); });

  benchmarkBuilder
      .addBenchmarkSet(
          "cast_varhar_as_date",
          vectorMaker.rowVector(
              {"empty", "invalid_date", "valid_date"},
              {emptyInput, invalidDateStrings, validDateStrings}))
      .addExpression("try_cast_invalid_empty_input", "try_cast(empty as date) ")
      .addExpression(
          "tryexpr_cast_invalid_empty_input", "try(cast (empty as date))")
      .addExpression(
          "try_cast_invalid_input", "try_cast(invalid_date as date) ")
      .addExpression(
          "tryexpr_cast_invalid_input", "try(cast (invalid_date as date))")
      .addExpression("cast_valid", "cast(valid_date as date)");

  benchmarkBuilder
      .addBenchmarkSet(
          "cast_varhar_as_timestamp",
          vectorMaker.rowVector(
              {"empty", "invalid_date", "valid_date"},
              {emptyInput, invalidDateStrings, validDateStrings}))
      .addExpression(
          "try_cast_invalid_empty_input", "try_cast(empty as timestamp) ")
      .addExpression(
          "tryexpr_cast_invalid_empty_input", "try(cast (empty as timestamp))")
      .addExpression(
          "try_cast_invalid_input", "try_cast(invalid_date as timestamp) ")
      .addExpression(
          "tryexpr_cast_invalid_input", "try(cast (invalid_date as timestamp))")
      .addExpression("cast_valid", "cast(valid_date as timestamp)");

  benchmarkBuilder
      .addBenchmarkSet(
          "cast_timestamp_as_varchar",
          vectorMaker.rowVector({"timestamp"}, {timestampInput}))
      .addExpression("cast", "cast (timestamp as varchar)");

  benchmarkBuilder
      .addBenchmarkSet(
          "cast_varchar_as_double",
          vectorMaker.rowVector(
              {"valid",
               "valid_nan",
               "valid_infinity",
               "invalid_nan",
               "invalid_infinity",
               "space"},
              {validDoubleStringInput,
               validNaNInput,
               validInfinityInput,
               invalidNaNInput,
               invalidInfinityInput,
               spaceInput}))
      .addExpression("cast_valid", "cast (valid as double)")
      .addExpression("cast_valid_nan", "cast (valid_nan as double)")
      .addExpression("cast_valid_infinity", "cast (valid_infinity as double)")
      .addExpression("try_cast_invalid_nan", "try_cast (invalid_nan as double)")
      .addExpression(
          "try_cast_invalid_infinity", "try_cast (invalid_infinity as double)")
      .addExpression("try_cast_space", "try_cast (space as double)");

  benchmarkBuilder
      .addBenchmarkSet(
          "cast",
          vectorMaker.rowVector(
              {"valid",
               "empty",
               "invalid",
               "integer",
               "bigint",
               "decimal",
               "short_decimal",
               "long_decimal",
               "large_real",
               "small_real",
               "small_double",
               "large_double"},
              {validInput,
               emptyInput,
               invalidInput,
               integerInput,
               bigintInput,
               decimalInput,
               shortDecimalInput,
               longDecimalInput,
               largeRealInput,
               smallRealInput,
               smallDoubleInput,
               largeDoubleInput}))
      .addExpression("try_cast_invalid_empty_input", "try_cast (empty as int) ")
      .addExpression(
          "tryexpr_cast_invalid_empty_input", "try (cast (empty as int))")
      .addExpression("try_cast_invalid_number", "try_cast (invalid as int)")
      .addExpression(
          "tryexpr_cast_invalid_number", "try (cast (invalid as int))")
      .addExpression("try_cast_valid", "try_cast (valid as int)")
      .addExpression("tryexpr_cast_valid", "try (cast (valid as int))")
      .addExpression("cast_valid", "cast(valid as int)")
      .addExpression(
          "cast_decimal_to_inline_string", "cast (decimal as varchar)")
      .addExpression("cast_short_decimal", "cast (short_decimal as varchar)")
      .addExpression("cast_long_decimal", "cast (long_decimal as varchar)")
      .addExpression(
          "cast_large_real_to_scientific_notation",
          "cast(large_real as varchar)")
      .addExpression(
          "cast_small_real_to_standard_notation", "cast(small_real as varchar)")
      .addExpression(
          "cast_small_double_to_scientific_notation",
          "cast(small_double as varchar)")
      .addExpression(
          "cast_large_double_to_standard_notation",
          "cast(large_double as varchar)")
      .addExpression("cast_real_as_int", "cast (small_real as integer)")
      .addExpression("cast_decimal_as_bigint", "cast (short_decimal as bigint)")
      .addExpression(
          "cast_int_as_short_decimal", "cast (integer as decimal(18,6))")
      .addExpression(
          "cast_int_as_long_decimal", "cast (integer as decimal(38,16))")
      .addExpression(
          "cast_bigint_as_short_decimal", "cast (bigint as decimal(18,6))")
      .addExpression(
          "cast_bigint_as_long_decimal", "cast (bigint as decimal(38,16))")
      .withIterations(100)
      .disableTesting();

  benchmarkBuilder.registerBenchmarks();
  folly::runBenchmarks();
  return 0;
}

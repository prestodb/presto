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
#include <folly/Random.h>
#include <folly/init/Init.h>
#include "velox/experimental/codegen/CodegenCompiledExpressionTransform.h"
#include "velox/experimental/codegen/benchmark/CodegenBenchmark.h"

using namespace facebook::velox;
using namespace facebook::velox::codegen;
namespace {
class SingleOutputNotDefaultNullsBenchmark : public BenchmarkGTest {};
} // namespace

// This is the typical situation where codegen is much slower
// constProjection_100x10000_reference                          1.68ns  594.03M
// constProjection_100x10000_compiled                14.59%    11.54ns   86.69M
// constProjection_100x10000_vectorFunc              66.92%     2.52ns  397.51M
// constProjection_100x10000_compileTime              0.03%     5.49us  182.09K
// constProjection_1000x1000_reference                         12.03ns   83.16M
// constProjection_1000x1000_compiled                56.63%    21.23ns   47.10M
// constProjection_1000x1000_vectorFunc             797.90%     1.51ns  663.53M
// constProjection_1000x1000_compileTime              0.22%     5.57us  179.39K
TEST_F(SingleOutputNotDefaultNullsBenchmark, DISABLED_Const) {
  auto inputRowType4 = ROW({"a"}, std::vector<TypePtr>{BIGINT()});

  benchmark.benchmarkExpressionsMult<BigintType>(
      "constProjection", {"1"}, inputRowType4, typicalBatches, true);
}

// if_And3_100x10000_reference                                 49.26ns   20.30M
// if_And3_100x10000_compiled 246.60%    19.98ns   50.06M
// if_And3_100x10000_vectorFunc 431.59%    11.41ns   87.61M
// if_And3_1000x1000_reference                                 59.85ns   16.71M
// if_And3_1000x1000_compiled 297.05%    20.15ns   49.63M
// if_And3_1000x1000_vectorFunc                     982.23%     6.09ns 164.10M
TEST_F(SingleOutputNotDefaultNullsBenchmark, DISABLED_if_And3) {
  auto inputRowType4 =
      ROW({"a", "b", "c", "d"},
          std::vector<TypePtr>{BIGINT(), BIGINT(), BIGINT(), BIGINT()});

  benchmark.benchmarkExpressionsMult<BigintType>(
      "if_And3",
      {"if(a > b and b> c, 1, d)"},
      inputRowType4,
      typicalBatches,
      true);
}

// test2_100x10000_reference                                   22.77ns   43.91M
// test2_100x10000_compiled                         279.81%     8.14ns 122.87M
// test2_100x10000_vectorFunc                       954.79%     2.39ns 419.25M
// test2_1000x1000_reference                                   33.96ns   29.44M
// test2_1000x1000_compiled 190.01%    17.87ns   55.95M
// test2_1000x1000_vectorFunc                       912.77%     3.72ns 268.76M
TEST_F(SingleOutputNotDefaultNullsBenchmark, DISABLED_test2) {
  auto inputRowType4 =
      ROW({"a", "b", "c", "d"},
          std::vector<TypePtr>{DOUBLE(), DOUBLE(), BIGINT(), BIGINT()});

  benchmark.benchmarkExpressionsMult<DoubleType>(
      "test2",
      {"if(c>d, if(a > b,1,2), 0)"},
      inputRowType4,
      typicalBatches,
      true);
}

// switchForm1_100x10000_reference                             25.67ns   38.96M
// switchForm1_100x10000_compiled 160.88%    15.96ns   62.67M
// switchForm1_100x10000_vectorFunc                 291.69%     8.80ns 113.63M
// switchForm1_1000x1000_reference                             40.22ns   24.86M
// switchForm1_1000x1000_compiled 198.32%    20.28ns   49.30M
// switchForm1_1000x1000_vectorFunc                 658.26%     6.11ns 163.65M
TEST_F(SingleOutputNotDefaultNullsBenchmark, DISABLED_SwitchForm1) {
  auto inputRowType4 =
      ROW({"a", "b", "c", "d"},
          std::vector<TypePtr>{BIGINT(), BIGINT(), BIGINT(), BIGINT()});

  benchmark.benchmarkExpressionsMult<BigintType, BigintType>(
      "switchForm1",
      {"CASE a WHEN b THEN c WHEN c THEN d WHEN d THEN b ELSE a END"},
      inputRowType4,
      typicalBatches,
      true);
}

// switchForm2Eq_100x10000_reference                           27.53ns   36.33M
// switchForm2Eq_100x10000_compiled 173.58%    15.86ns   63.06M
// switchForm2Eq_100x10000_vectorFunc               310.13%     8.88ns 112.66M
// switchForm2Eq_1000x1000_reference                           40.88ns   24.46M
// switchForm2Eq_1000x1000_compiled 193.98%    21.08ns   47.45M
// switchForm2Eq_1000x1000_vectorFunc               688.87%     5.93ns 168.50M
TEST_F(SingleOutputNotDefaultNullsBenchmark, DISABLED_SwitchForm2Eq) {
  auto inputRowType4 =
      ROW({"a", "b", "c", "d"},
          std::vector<TypePtr>{BIGINT(), BIGINT(), BIGINT(), BIGINT()});

  benchmark.benchmarkExpressionsMult<BigintType>(
      "switchForm2Eq",
      {"CASE WHEN a=b THEN c WHEN a=c THEN d WHEN a=d THEN b ELSE a END"},
      inputRowType4,
      typicalBatches,
      true);
}

// switchForm2NE_100x10000_reference                           34.64ns   28.86M
// switchForm2NE_100x10000_compiled 147.47%    23.49ns   42.57M
// switchForm2NE_100x10000_vectorFunc 252.79%    13.71ns   72.97M
// switchForm2NE_1000x1000_reference                           43.00ns   23.25M
// switchForm2NE_1000x1000_compiled 213.81%    20.11ns   49.72M
// switchForm2NE_1000x1000_vectorFunc               672.54%     6.39ns 156.40M
TEST_F(SingleOutputNotDefaultNullsBenchmark, DISABLED_SwitchForm2NE) {
  auto inputRowType4 =
      ROW({"a", "b", "c", "d"},
          std::vector<TypePtr>{BIGINT(), BIGINT(), BIGINT(), BIGINT()});

  benchmark.benchmarkExpressionsMult<BigintType>(
      "switchForm2NE",
      {"CASE WHEN a>b THEN c WHEN a>c THEN d WHEN a>d THEN b ELSE a END"},
      inputRowType4,
      typicalBatches,
      true);
}

// switchForm2Null_100x10000_reference                 23.93ns   41.79M
// switchForm2Null_100x10000_compiled       226.51%    10.56ns   94.66M
// switchForm2Null_100x10000_vectorFunc     929.01%     2.58ns  388.22M
// switchForm2Null_100x10000_compileTime      0.40%     5.92us  168.79K
// switchForm2Null_1000x1000_reference                 38.95ns   25.68M
// switchForm2Null_1000x1000_compiled       169.56%    22.97ns   43.54M
// switchForm2Null_1000x1000_vectorFunc    1251.10%     3.11ns  321.24M
// switchForm2Null_1000x1000_compileTime      0.57%     6.83us  146.37K
TEST_F(SingleOutputNotDefaultNullsBenchmark, DISABLED_SwitchForm2NULL) {
  auto inputRowType4 =
      ROW({"a", "b", "c", "d"},
          std::vector<TypePtr>{BIGINT(), BIGINT(), BIGINT(), BIGINT()});

  benchmark.benchmarkExpressionsMult<BigintType>(
      "switchForm2Null",
      {"CASE WHEN a IS NULL THEN 0 WHEN b IS NULL THEN 1 WHEN c IS NULL THEN 2 WHEN d is NULL THEN 3 ELSE 4 END"},
      inputRowType4,
      typicalBatches,
      true);
}

// There is nothing to fuse here we expect to be at partiy
// TODO investigate why we are slower. @ 75%
// Velox can move a to results saving a bunch of work
// coalesce_100x10000_reference                                 8.27ns 120.85M
// coalesce_100x10000_compiled                       59.73%    13.85ns   72.18M
// coalesce_100x10000_vectorFunc                    141.34%     5.85ns 170.81M
// coalesce_1000x1000_reference                                18.30ns   54.63M
// coalesce_1000x1000_compiled                       96.30%    19.01ns   52.61M
// coalesce_1000x1000_vectorFunc                    361.35%     5.07ns 197.42M
TEST_F(SingleOutputNotDefaultNullsBenchmark, DISABLED_coalesce) {
  auto inputRowType4 =
      ROW({"a", "b", "c", "d"},
          std::vector<TypePtr>{DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE()});

  benchmark.benchmarkExpressionsMult<DoubleType>(
      "coalesce",
      {"\"coalesce\"(a,b,c,d)"},
      inputRowType4,
      typicalBatches,
      true);
}

// AddCoalesce_100x10000_reference                             30.24ns   33.07M
// AddCoalesce_100x10000_compiled 287.56%    10.52ns   95.09M
// AddCoalesce_100x10000_vectorFunc 291.76%    10.36ns   96.48M
// AddCoalesce_1000x1000_reference                             42.23ns   23.68M
// AddCoalesce_1000x1000_compiled 220.12%    19.18ns   52.13M
// AddCoalesce_1000x1000_vectorFunc                 749.06%     5.64ns 177.39M
TEST_F(SingleOutputNotDefaultNullsBenchmark, DISABLED_AddCoalesce) {
  auto inputRowType4 =
      ROW({"a", "b", "c", "d"},
          std::vector<TypePtr>{DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE()});

  benchmark.benchmarkExpressionsMult<DoubleType>(
      "AddCoalesce",
      {"\"coalesce\"(a,0.0)+\"coalesce\"(b,0.0)+\"coalesce\"(c,0.0)+\"coalesce\"(d,0.0)"},
      inputRowType4,
      typicalBatches,
      true);
}

// test2_100x10000_reference                                   15.75ns   63.48M
// test2_100x10000_compiled 116.46%    13.53ns   73.93M
// test2_100x10000_vectorFunc                       160.31%     9.83ns 101.75M
// test2_1000x1000_reference                                   28.51ns   35.07M
// test2_1000x1000_compiled 145.48%    19.60ns   51.02M
// test2_1000x1000_vectorFunc                       560.56%     5.09ns 196.60M
TEST_F(SingleOutputNotDefaultNullsBenchmark, DISABLED_CoalesceAdd) {
  auto inputRowType4 =
      ROW({"a", "b", "c", "d"},
          std::vector<TypePtr>{DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE()});

  benchmark.benchmarkExpressionsMult<DoubleType>(
      "test2",
      {"\"coalesce\"(a+b, a+c, a+d)"},
      inputRowType4,
      typicalBatches,
      true);
}

//****************************************************
// filter benchmarks
// TODO: add test with coalesce

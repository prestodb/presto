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
class SingleOutputDefaultNullsBenchmark : public BenchmarkGTest {};
} // namespace

// Note: TODO investigate, re-run and optimize  when input is not null.

// 0% null:
// ============================================================================
// computeDepth5_1x1000000_reference                           35.34ns   28.30M
// computeDepth5_1x1000000_compiled                 326.65%    10.82ns   92.43M
// computeDepth5_1x1000000_vectorFunc               749.55%     4.71ns  212.09M
// computeDepth5_1x1000000_compileTime                0.73%     4.87us  205.30K
// computeDepth5_10x100000_reference                           13.66ns   73.22M
// computeDepth5_10x100000_compiled                 262.13%     5.21ns  191.93M
// computeDepth5_10x100000_vectorFunc               326.40%     4.18ns  238.98M
// computeDepth5_10x100000_compileTime                0.28%     4.94us  202.41K
// computeDepth5_100x10000_reference                           11.21ns   89.18M
// computeDepth5_100x10000_compiled                 127.40%     8.80ns  113.62M
// computeDepth5_100x10000_vectorFunc               277.88%     4.04ns  247.83M
// computeDepth5_100x10000_compileTime                0.21%     5.34us  187.18K
// computeDepth5_1000x1000_reference                           26.78ns   37.34M
// computeDepth5_1000x1000_compiled                 147.23%    18.19ns   54.98M
// computeDepth5_1000x1000_vectorFunc               686.48%     3.90ns  256.34M
// ============================================================================

/*
DefaultNull disabled
computeDepth5_10x100000_reference                           17.12ns   58.41M
computeDepth5_10x100000_compiled                 163.46%    10.47ns   95.47M
computeDepth5_10x100000_vectorFunc               164.90%    10.38ns   96.31M
computeDepth5_10x100000_compileTime                0.29%     5.93us  168.52K
computeDepth5_100x10000_reference                           14.03ns   71.28M
computeDepth5_100x10000_compiled                 146.19%     9.60ns  104.21M
computeDepth5_100x10000_vectorFunc               139.90%    10.03ns   99.72M
computeDepth5_100x10000_compileTime                0.22%     6.28us  159.24K

DefaultNull enabled
computeDepth5_10x100000_reference                           16.89ns   59.20M
computeDepth5_10x100000_compiled                 280.56%     6.02ns  166.09M
computeDepth5_10x100000_vectorFunc               287.03%     5.89ns  169.92M
computeDepth5_10x100000_compileTime                0.29%     5.76us  173.67K
computeDepth5_100x10000_reference                           13.95ns   71.66M
computeDepth5_100x10000_compiled                 187.44%     7.44ns  134.32M
computeDepth5_100x10000_vectorFunc               360.24%     3.87ns  258.16M
computeDepth5_100x10000_compileTime                0.23%     6.07us  164.84K
*/
TEST_F(SingleOutputDefaultNullsBenchmark, DISABLED_computeDepth5) {
  auto inputRowType4 =
      ROW({"a", "b", "c", "d"},
          std::vector<TypePtr>{DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE()});

  benchmark.benchmarkExpressionsMult<DoubleType>(
      "computeDepth5",
      {"a/1.1 * b* abs(c)+ 100.11"},
      inputRowType4,
      typicalBatches,
      true);
}

/*
DefaultNull disabled
plusThreeTimes_10x100000_reference                          10.72ns   93.33M
plusThreeTimes_10x100000_compiled                107.66%     9.95ns  100.48M
plusThreeTimes_10x100000_vectorFunc              151.76%     7.06ns  141.63M
plusThreeTimes_10x100000_compileTime               0.15%     7.07us  141.39K
plusThreeTimes_100x10000_reference                           9.65ns  103.64M
plusThreeTimes_100x10000_compiled                 67.74%    14.24ns   70.21M
plusThreeTimes_100x10000_vectorFunc              161.96%     5.96ns  167.86M
plusThreeTimes_100x10000_compileTime               0.16%     6.08us  164.60K

DefaultNull enabled
plusThreeTimes_10x100000_reference                           9.76ns  102.41M
plusThreeTimes_10x100000_compiled                165.59%     5.90ns  169.58M
plusThreeTimes_10x100000_vectorFunc              390.51%     2.50ns  399.93M
plusThreeTimes_10x100000_compileTime               0.15%     6.33us  158.07K
plusThreeTimes_100x10000_reference                           8.53ns  117.30M
plusThreeTimes_100x10000_compiled                115.63%     7.37ns  135.63M
plusThreeTimes_100x10000_vectorFunc              233.17%     3.66ns  273.51M
plusThreeTimes_100x10000_compileTime               0.14%     6.07us  164.66K
*/
TEST_F(SingleOutputDefaultNullsBenchmark, DISABLED_plusThreeTimes) {
  auto inputRowType4 =
      ROW({"a", "b", "c", "d"},
          std::vector<TypePtr>{DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE()});

  benchmark.benchmarkExpressionsMult<DoubleType>(
      "plusThreeTimes", {"a + b + c"}, inputRowType4, typicalBatches, true);
}

/*
DefaultNull disabled
plus4Times_10x100000_reference                              14.75ns   67.81M
plus4Times_10x100000_compiled                    113.97%    12.94ns   77.29M
plus4Times_10x100000_vectorFunc                  146.25%    10.08ns   99.17M
plus4Times_10x100000_compileTime                   0.26%     5.77us  173.45K
plus4Times_100x10000_reference                              11.80ns   84.74M
plus4Times_100x10000_compiled                     97.78%    12.07ns   82.86M
plus4Times_100x10000_vectorFunc                  137.29%     8.60ns  116.34M
plus4Times_100x10000_compileTime                   0.20%     5.87us  170.44K

DefaultNull enabled
plus4Times_10x100000_reference                              14.42ns   69.35M
plus4Times_10x100000_compiled                    191.67%     7.52ns  132.93M
plus4Times_10x100000_vectorFunc                  382.67%     3.77ns  265.39M
plus4Times_10x100000_compileTime                   0.23%     6.29us  158.90K
plus4Times_100x10000_reference                              11.50ns   86.98M
plus4Times_100x10000_compiled                     98.19%    11.71ns   85.41M
plus4Times_100x10000_vectorFunc                  258.13%     4.45ns  224.52M
plus4Times_100x10000_compileTime                   0.18%     6.54us  152.96K
*/
TEST_F(SingleOutputDefaultNullsBenchmark, DISABLED_plus4Times) {
  auto inputRowType4 =
      ROW({"a", "b", "c", "d"},
          std::vector<TypePtr>{DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE()});

  benchmark.benchmarkExpressionsMult<DoubleType>(
      "plus4Times", {"a + b+ c + d"}, inputRowType4, typicalBatches, true);
}

/*
DefaultNull disabled
plus6Times_10x100000_reference                              20.17ns   49.58M
plus6Times_10x100000_compiled                    105.96%    19.03ns   52.54M
plus6Times_10x100000_vectorFunc                  129.69%    15.55ns   64.30M
plus6Times_10x100000_compileTime                   0.34%     6.00us  166.65K
plus6Times_100x10000_reference                              16.37ns   61.07M
plus6Times_100x10000_compiled                     87.31%    18.75ns   53.32M
plus6Times_100x10000_vectorFunc                  107.01%    15.30ns   65.36M
plus6Times_100x10000_compileTime                   0.27%     6.08us  164.59K

DefaultNull enabled
plus6Times_10x100000_reference                              20.72ns   48.25M
plus6Times_10x100000_compiled                    230.29%     9.00ns  111.12M
plus6Times_10x100000_vectorFunc                  350.54%     5.91ns  169.15M
plus6Times_10x100000_compileTime                   0.31%     6.58us  151.87K
plus6Times_100x10000_reference                              17.18ns   58.19M
plus6Times_100x10000_compiled                    121.95%    14.09ns   70.96M
plus6Times_100x10000_vectorFunc                  260.44%     6.60ns  151.55M
plus6Times_100x10000_compileTime                   0.28%     6.05us  165.18K
*/
TEST_F(SingleOutputDefaultNullsBenchmark, DISABLED_plus6Times) {
  auto inputRowType4 =
      ROW({"a", "b", "c", "d", "e", "f"},
          std::vector<TypePtr>{
              DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE()});

  benchmark.benchmarkExpressionsMult<DoubleType>(
      "plus6Times", {"a + b+ c + d+e+f"}, inputRowType4, typicalBatches, true);
}

/*
DefaultNull disabled
castAdd2_10x100000_reference                                36.38ns   27.49M
castAdd2_10x100000_compiled                      151.72%    23.98ns   41.70M
castAdd2_10x100000_vectorFunc                    167.53%    21.72ns   46.05M
castAdd2_10x100000_compileTime                     0.58%     6.28us  159.32K
castAdd2_100x10000_reference                                36.79ns   27.18M
castAdd2_100x10000_compiled                      148.13%    24.84ns   40.26M
castAdd2_100x10000_vectorFunc                    170.09%    21.63ns   46.23M
castAdd2_100x10000_compileTime                     0.59%     6.27us  159.38K

DefaultNull enabled
castAdd2_10x100000_reference                                36.95ns   27.06M
castAdd2_10x100000_compiled                      178.61%    20.69ns   48.34M
castAdd2_10x100000_vectorFunc                    250.03%    14.78ns   67.66M
castAdd2_10x100000_compileTime                     0.57%     6.46us  154.78K
castAdd2_100x10000_reference                                37.71ns   26.52M
castAdd2_100x10000_compiled                      149.58%    25.21ns   39.67M
castAdd2_100x10000_vectorFunc                    245.98%    15.33ns   65.23M
castAdd2_100x10000_compileTime                     0.58%     6.52us  153.43K
*/
TEST_F(SingleOutputDefaultNullsBenchmark, DISABLED_castAdd2) {
  auto inputRowType4 =
      ROW({"a", "b", "c", "d"},
          std::vector<TypePtr>{DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE()});

  benchmark.benchmarkExpressionsMult<DoubleType>(
      "castAdd2",
      {"cast (a as int) + cast (b as int)"},
      inputRowType4,
      typicalBatches,
      true);
}

/*
DefaultNull disabled
castAdd4_10x100000_reference                                62.79ns   15.93M
castAdd4_10x100000_compiled                      109.81%    57.18ns   17.49M
castAdd4_10x100000_vectorFunc                    115.29%    54.46ns   18.36M
castAdd4_10x100000_compileTime                     1.02%     6.17us  161.95K
castAdd4_100x10000_reference                                65.34ns   15.30M
castAdd4_100x10000_compiled                      108.49%    60.23ns   16.60M
castAdd4_100x10000_vectorFunc                    120.07%    54.42ns   18.38M
castAdd4_100x10000_compileTime                     1.05%     6.21us  161.12K

DefaultNull enabled
castAdd4_10x100000_reference                                64.02ns   15.62M
castAdd4_10x100000_compiled                      185.10%    34.59ns   28.91M
castAdd4_10x100000_vectorFunc                    197.40%    32.43ns   30.83M
castAdd4_10x100000_compileTime                     1.01%     6.36us  157.17K
castAdd4_100x10000_reference                                66.39ns   15.06M
castAdd4_100x10000_compiled                      171.30%    38.76ns   25.80M
castAdd4_100x10000_vectorFunc                    199.87%    33.22ns   30.11M
castAdd4_100x10000_compileTime                     1.05%     6.35us  157.47K
*/
TEST_F(SingleOutputDefaultNullsBenchmark, DISABLED_castAdd4) {
  auto inputRowType4 =
      ROW({"a", "b", "c", "d"},
          std::vector<TypePtr>{DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE()});

  benchmark.benchmarkExpressionsMult<DoubleType>(
      "castAdd4",
      {"cast (a as int) + cast (b as int)+ cast (c as int)+ cast (d as int)"},
      inputRowType4,
      typicalBatches,
      true);
}

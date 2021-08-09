/*
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
class StringBenchmarks : public BenchmarkGTest {
 public:
  std::shared_ptr<const RowType> inputRowType =
      ROW({"str1", "str2", "str3", "double1", "double2"},
          std::vector<TypePtr>{
              VARCHAR(),
              VARCHAR(),
              VARCHAR(),
              DOUBLE(),
              DOUBLE()});
};
// Note: the last part of the file contains tests on long strings, all others
// uses the default (max 11 char).
//*****************************************************************************
//**************************Lower Case Study **********************************
//*****************************************************************************

// Note: small strings < 11 char
//============================================================================
// lower_100x10000_reference                                   59.77ns   16.73M
// lower_100x10000_compiled                         115.01%    51.97ns   19.24M
// lower_100x10000_vectorFunc                       293.85%    20.34ns   49.16M
// lower_100x10000_compileTime                        1.07%     5.59us  179.01K
// lower_1000x1000_reference                                   60.30ns   16.58M
// lower_1000x1000_compiled                         100.93%    59.75ns   16.74M
// lower_1000x1000_vectorFunc                       207.23%    29.10ns   34.37M
TEST_F(StringBenchmarks, lower) {
  benchmark.benchmarkExpressionsMult<BigintType>(
      "lower", {"lower(str1)"}, inputRowType, typicalBatches, true);
}

// Lower after disabling the inplace optimization
//============================================================================
// lower_100x10000_reference                                   73.58ns   13.59M
// lower_100x10000_compiled                         143.87%    51.14ns   19.55M
// lower_100x10000_vectorFunc                       373.33%    19.71ns   50.74M
// lower_100x10000_compileTime                        1.34%     5.49us  182.18K
// lower_1000x1000_reference                                   84.47ns   11.84M
// lower_1000x1000_compiled                         132.80%    63.61ns   15.72M
// lower_1000x1000_vectorFunc                       309.38%    27.30ns   36.62M
//============================================================================

//*****************************************************************************
//************************* coalesceLowerUpper Case Study *********************
//*****************************************************************************

// There is better speedups with vectorFunc so it might be something with the
// engine here that is adding overhead.
//============================================================================
// coalesceLowerUpper_100x10000_reference                     101.99ns    9.81M
// coalesceLowerUpper_100x10000_compiled            160.55%    63.52ns   15.74M
// coalesceLowerUpper_100x10000_vectorFunc          248.86%    40.98ns   24.40M
// coalesceLowerUpper_100x10000_compileTime           1.74%     5.88us  170.13K
// coalesceLowerUpper_1000x1000_reference                     110.81ns    9.02M
// coalesceLowerUpper_1000x1000_compiled            151.71%    73.04ns   13.69M
//============================================================================
TEST_F(StringBenchmarks, coalesceLowerUpper) {
  benchmark.benchmarkExpressionsMult<BigintType>(
      "coalesceLowerUpper",
      {"\"coalesce\"(upper(str1), lower(str2))"},
      inputRowType,
      typicalBatches,
      true);
}

// Run coalesceLowerUpper with out in place optimizations.
//============================================================================
// coalesceLowerUpper_100x10000_reference                     129.95ns    7.70M
// coalesceLowerUpper_100x10000_compiled            205.27%    63.31ns   15.80M
// coalesceLowerUpper_100x10000_vectorFunc          297.29%    43.71ns   22.88M
// coalesceLowerUpper_100x10000_compileTime           2.15%     6.04us  165.48K
// coalesceLowerUpper_1000x1000_reference                     142.56ns    7.01M
// coalesceLowerUpper_1000x1000_compiled            197.54%    72.17ns   13.86M
// coalesceLowerUpper_1000x1000_vectorFunc          281.06%    50.72ns   19.72M
//============================================================================

//*****************************************************************************
//************************** LowerOfUpper Case Study **************************
//*****************************************************************************
//============================================================================
// lowerOfUpper_100x10000_reference                            79.09ns   12.64M
// lowerOfUpper_100x10000_compiled                  103.49%    76.42ns   13.09M
// lowerOfUpper_100x10000_vectorFunc                215.90%    36.63ns   27.30M
// lowerOfUpper_100x10000_compileTime                 1.47%     5.39us  185.47K
// lowerOfUpper_1000x1000_reference                            88.90ns   11.25M
// lowerOfUpper_1000x1000_compiled                  103.44%    85.95ns   11.63M
// lowerOfUpper_1000x1000_vectorFunc                216.15%    41.13ns   24.31M
//============================================================================
TEST_F(StringBenchmarks, lowerOfUpper) {
  benchmark.benchmarkExpressionsMult<BigintType>(
      "lowerOfUpper",
      {"lower(upper(str1))"},
      inputRowType,
      typicalBatches,
      true);
}

// !DID NOT RERUN THIS AFTER REBASE!
// LowerOfUpper lower after disabling inplace optimizations
// Result : in place opt contributes to +40%-80% in velox.
//============================================================================
// lowerOfUpper_100x10000_reference                           112.22ns    8.91M
// lowerOfUpper_100x10000_compiled                  143.44%    78.24ns   12.78M
// lowerOfUpper_100x10000_vectorFunc                298.01%    37.65ns   26.56M
// lowerOfUpper_1000x1000_reference                           124.22ns    8.05M
// lowerOfUpper_1000x1000_compiled                  141.67%    87.68ns   11.41M
// lowerOfUpper_1000x1000_vectorFunc                260.12%    47.75ns   20.94M
//============================================================================

// !DID NOT RERUN THIS AFTER REBASE!
// LowerOfUpper after also disabling the ASCII propagation and the inplace.
// Result : propagation opt contributes to 15-20% of the regression.
// With default nulls optimizations we will be even better
//============================================================================
// lowerOfUpper_1x1000000_reference                            96.47ns   10.37M
// lowerOfUpper_1x1000000_compiled                  165.00%    58.47ns   17.10M
// lowerOfUpper_1x1000000_vectorFunc                242.44%    39.79ns   25.13M
// lowerOfUpper_1x1000000_compileTime                 1.11%     8.73us  114.61K
// lowerOfUpper_10x100000_reference                           116.28ns    8.60M
// lowerOfUpper_10x100000_compiled                  199.91%    58.17ns   17.19M
// lowerOfUpper_10x100000_vectorFunc                271.71%    42.80ns   23.37M
// lowerOfUpper_10x100000_compileTime                 1.42%     8.21us  121.87K
// lowerOfUpper_100x10000_reference                            95.68ns   10.45M
// lowerOfUpper_100x10000_compiled                  180.80%    52.92ns   18.90M
// lowerOfUpper_100x10000_vectorFunc                230.10%    41.58ns   24.05M
// lowerOfUpper_100x10000_compileTime                 1.20%     7.97us  125.52K
//============================================================================

//*****************************************************************************
//************************** CastString Case Study ****************************
//*****************************************************************************
// Had to change batch maker manually to generate
// string that represent numbers for the tests in this case study
// TODO: enable generating strings that represent numbers

//============================================================================
// castAndAdd_1x1000000_reference                             111.81ns    8.94M
// castAndAdd_1x1000000_compiled                    101.86%   109.77ns    9.11M
// castAndAdd_1x1000000_vectorFunc                  105.62%   105.87ns    9.45M
// castAndAdd_1x1000000_compileTime                   1.46%     7.67us  130.36K
// castAndAdd_10x100000_reference                              91.28ns   10.95M
// castAndAdd_10x100000_compiled                     98.65%    92.53ns   10.81M
// castAndAdd_10x100000_vectorFunc                  100.22%    91.08ns   10.98M
// castAndAdd_10x100000_compileTime                   1.23%     7.44us  134.41K
// castAndAdd_100x10000_reference                              98.08ns   10.20M
// castAndAdd_100x10000_compiled                    103.35%    94.91ns   10.54M
// castAndAdd_100x10000_vectorFunc                  105.37%    93.09ns   10.74M
// castAndAdd_100x10000_compileTime                   1.31%     7.50us  133.35K
//============================================================================
TEST_F(StringBenchmarks, DISABLED_castAndAdd) {
  // TODO: Casting fails here because str1 is not a valid double string.
  benchmark.benchmarkExpressionsMult<BigintType>(
      "castAndAdd",
      {"cast(str1 as double)+ cast(str2 as double)"},
      inputRowType,
      typicalBatches,
      true);
}

//============================================================================
// coalesceCastAndAdd_1x1_reference                           123.74us    8.08K
// coalesceCastAndAdd_1x1_compiled                  105.98%   116.76us    8.56K
// coalesceCastAndAdd_1x1_vectorFunc               2701.34%     4.58us  218.30K
// coalesceCastAndAdd_1x1_compileTime                 0.00%     12.17s   82.18m
// coalesceCastAndAdd_100000x1_reference                       27.57us   36.28K
// coalesceCastAndAdd_100000x1_compiled             125.13%    22.03us   45.39K
// coalesceCastAndAdd_100000x1_vectorFunc           382.34%     7.21us  138.70K
// coalesceCastAndAdd_100000x1_compileTime           19.02%   144.96us    6.90K
// coalesceCastAndAdd_100x10000_reference                     171.70ns    5.82M
// coalesceCastAndAdd_100x10000_compiled             96.52%   177.90ns    5.62M
// coalesceCastAndAdd_100x10000_vectorFunc          104.83%   163.79ns    6.11M
// coalesceCastAndAdd_100x10000_compileTime           1.59%    10.83us   92.37K
//============================================================================
TEST_F(StringBenchmarks, DISABLED_coalesceCastAndAdd) {
  // TODO: This test is failling with a exceptioon  because str1 (and possibly
  // str2) are random string and not always convertible to double
  benchmark.benchmarkExpressionsMult<BigintType>(
      "coalesceCastAndAdd",
      {"\"coalesce\"(cast(str1 as double)+ cast(str2 as double), double1+ double2)"},
      inputRowType,
      typicalBatches,
      true);
}

//*****************************************************************************
//************************** Compare Case Study *******************************
//*****************************************************************************
// compare2_100x10000_reference                                 9.48ns  105.44M
// compare2_100x10000_compiled                       94.74%    10.01ns   99.89M
// compare2_100x10000_vectorFunc                    120.84%     7.85ns  127.41M
// compare2_100x10000_compileTime                     0.18%     5.19us  192.66K
// compare2_1000x1000_reference                                20.29ns   49.29M
// compare2_1000x1000_compiled                      108.08%    18.77ns   53.27M
// compare2_1000x1000_vectorFunc                    312.85%     6.49ns  154.19M
//============================================================================
TEST_F(StringBenchmarks, compare2) {
  benchmark.benchmarkExpressionsMult<BigintType>(
      "compare2", {"str1 == str2"}, inputRowType, typicalBatches, true);
}

//============================================================================
// compare3_100x10000_reference                                23.30ns   42.91M
// compare3_100x10000_compiled                      153.21%    15.21ns   65.74M
// compare3_100x10000_vectorFunc                    175.40%    13.29ns   75.27M
// compare3_100x10000_compileTime                     0.44%     5.31us  188.19K
// compare3_1000x1000_reference                                34.37ns   29.09M
// compare3_1000x1000_compiled                      145.36%    23.65ns   42.29M
// compare3_1000x1000_vectorFunc                    297.81%    11.54ns   86.64M
//============================================================================
TEST_F(StringBenchmarks, compare3) {
  benchmark.benchmarkExpressionsMult<BigintType>(
      "compare3",
      {"str1 == str2 and str2 == 'aaa'"},
      inputRowType,
      typicalBatches,
      true);
}

//*****************************************************************************
//************************** coalesce  Case Study *********************
//*****************************************************************************
//============================================================================
// coalesceStrings_100x10000_reference                         29.31ns   34.12M
// coalesceStrings_100x10000_compiled                92.83%    31.58ns   31.67M
// coalesceStrings_100x10000_vectorFunc             219.81%    13.33ns   74.99M
// coalesceStrings_100x10000_compileTime              0.52%     5.66us  176.75K
// coalesceStrings_1000x1000_reference                         40.18ns   24.89M
// coalesceStrings_1000x1000_compiled                93.15%    43.13ns   23.18M
// coalesceStrings_1000x1000_vectorFunc             236.10%    17.02ns   58.76M
//============================================================================
// TODO: investigate why vectorFunc is much faster than compiled
TEST_F(StringBenchmarks, coalesceStrings) {
  benchmark.benchmarkExpressionsMult<BigintType>(
      "coalesceStrings",
      {"\"coalesce\"(str1, str2, str3)"},
      inputRowType,
      typicalBatches,
      true);
}

//============================================================================
// coalesceConstant_100x10000_compileTime             0.41%     5.02us  199.07K
// coalesceConstant_1000x1000_reference                        30.84ns   32.43M
// coalesceConstant_1000x1000_compiled               64.65%    47.70ns   20.96M
// coalesceConstant_1000x1000_vectorFunc            113.27%    27.23ns   36.73M
// coalesceConstant_100x10000_reference                        20.74ns   48.23M
// coalesceConstant_100x10000_compiled               59.31%    34.96ns   28.60M
// coalesceConstant_100x10000_vectorFunc            125.36%    16.54ns   60.45M
//============================================================================
// TODO: This basically here to track the benefit of constructing constant
// string on velox buffers which is not done yet.
TEST_F(StringBenchmarks, coalesceStringAndConstant) {
  benchmark.benchmarkExpressionsMult<BigintType>(
      "coalesceConstant",
      {"\"coalesce\"( 'aabbccdd', str1)"},
      inputRowType,
      typicalBatches,
      true);
}

//*****************************************************************************
//************************** if  Case Study ***********************************
//*****************************************************************************
//============================================================================
// ifString1_100x10000_reference                               67.26ns   14.87M
// ifString1_100x10000_compiled                     163.18%    41.22ns   24.26M
// ifString1_100x10000_vectorFunc                   298.45%    22.54ns   44.37M
// ifString1_100x10000_compileTime                    1.29%     5.20us  192.18K
// ifString1_1000x1000_reference                               73.86ns   13.54M
// ifString1_1000x1000_compiled                     149.50%    49.41ns   20.24M
// ifString1_1000x1000_vectorFunc                   347.22%    21.27ns   47.01M
//============================================================================
// This is not default null, why is vectorFunc much faster
TEST_F(StringBenchmarks, ifString1) {
  benchmark.benchmarkExpressionsMult<BigintType>(
      "ifString1",
      {"if(length(str1)> 5, str2, str3)"},
      inputRowType,
      typicalBatches,
      true);
}

//============================================================================
// ifString2_100x10000_reference                              106.74ns    9.37M
// ifString2_100x10000_compiled                     147.83%    72.20ns   13.85M
// ifString2_100x10000_vectorFunc                   213.04%    50.10ns   19.96M
// ifString2_1000x1000_reference                              123.24ns    8.11M
// ifString2_1000x1000_compiled                     151.57%    81.31ns   12.30M
// ifString2_1000x1000_vectorFunc                   215.71%    57.13ns   17.50M
//============================================================================

TEST_F(StringBenchmarks, ifString2) {
  benchmark.benchmarkExpressionsMult<BigintType>(
      "ifString2",
      {"if(length(str1)> 5, upper(str1), lower(str3))"},
      inputRowType,
      typicalBatches,
      true);
}

//*****************************************************************************
//************************* Larger Strings numbers ****************************
//*****************************************************************************

// !DID NOT RERUN THIS AFTER REBASE!
// Lower with max string length 1000
//============================================================================
// lower_1x1000000_reference                                    1.13us  887.33K
// lower_1x1000000_compiled                         157.84%   714.01ns    1.40M
// lower_1x1000000_vectorFunc                       320.97%   351.12ns    2.85M
// lower_1x1000000_compileTime                       15.21%     7.41us  134.92K
// lower_10x100000_reference                                  596.00ns    1.68M
// lower_10x100000_compiled                         106.83%   557.89ns    1.79M
// lower_10x100000_vectorFunc                       180.22%   330.71ns    3.02M
// lower_10x100000_compileTime                        7.97%     7.48us  133.71K
// lower_100x10000_reference                                  569.93ns    1.75M
// lower_100x10000_compiled                         117.86%   483.56ns    2.07M
// lower_100x10000_vectorFunc                       129.77%   439.19ns    2.28M
// lower_100x10000_compileTime                        7.64%     7.46us  134.03K
//============================================================================

// !DID NOT RERUN THIS AFTER REBASE!
// Lower with max string length 1000 and inplace disabled
//============================================================================
// lower_1x1000000_reference                                    1.07us  936.65K
// lower_1x1000000_compiled                         146.64%   728.06ns    1.37M
// lower_1x1000000_vectorFunc                       179.19%   595.80ns    1.68M
// lower_1x1000000_compileTime                        8.55%    12.49us   80.08K
// lower_10x100000_reference                                  709.59ns    1.41M
// lower_10x100000_compiled                         120.58%   588.47ns    1.70M
// lower_10x100000_vectorFunc                       150.83%   470.47ns    2.13M
// lower_10x100000_compileTime                        7.28%     9.75us  102.59K
// lower_100x10000_reference                                  702.65ns    1.42M
// lower_100x10000_compiled                         119.94%   585.81ns    1.71M
// lower_100x10000_vectorFunc                       162.58%   432.18ns    2.31M
// lower_100x10000_compileTime                        7.87%     8.93us  111.94K
//============================================================================

// !DID NOT RERUN THIS AFTER REBASE!
// LowerOfUpper max 1000
//============================================================================
// lower_1x1000000_reference                                    1.10us  907.63K
// lower_1x1000000_compiled                         145.32%   758.17ns    1.32M
// lower_1x1000000_vectorFunc                       222.77%   494.57ns    2.02M
// lower_1x1000000_compileTime                       13.94%     7.90us  126.50K
// lower_10x100000_reference                                  566.06ns    1.77M
// lower_10x100000_compiled                          94.84%   596.88ns    1.68M
// lower_10x100000_vectorFunc                       139.87%   404.71ns    2.47M
// lower_10x100000_compileTime                        6.86%     8.25us  121.18K
// lower_100x10000_reference                                  485.99ns    2.06M
// lower_100x10000_compiled                          93.51%   519.74ns    1.92M
// lower_100x10000_vectorFunc                       118.88%   408.81ns    2.45M
// lower_100x10000_compileTime                        5.93%     8.19us  122.09K
//============================================================================

// !DID NOT RERUN THIS AFTER REBASE!
// LowerOfUpper max 1000 and propagation disabled
//============================================================================
// lowerOfUpper_1x1000000_reference                             1.34us  745.20K
// lowerOfUpper_1x1000000_compiled                  170.46%   787.23ns    1.27M
// lowerOfUpper_1x1000000_vectorFunc                246.81%   543.71ns    1.84M
// lowerOfUpper_1x1000000_compileTime                15.41%     8.71us  114.80K
// lowerOfUpper_10x100000_reference                             1.14us  876.90K
// lowerOfUpper_10x100000_compiled                  166.72%   684.03ns    1.46M
// lowerOfUpper_10x100000_vectorFunc                183.13%   622.73ns    1.61M
// lowerOfUpper_10x100000_compileTime                12.81%     8.90us  112.34K
// lowerOfUpper_100x10000_reference                             1.98us  504.26K
// lowerOfUpper_100x10000_compiled                  289.88%   684.12ns    1.46M
// lowerOfUpper_100x10000_vectorFunc                362.56%   546.97ns    1.83M
// lowerOfUpper_100x10000_compileTime                21.99%     9.02us  110.89K
//============================================================================

// TODO 1: Investigate copying
// TODO 2: Investigate using a global bump allocator instead of std::string
// TODO 3: Add ascii propagation optimization

// !DID NOT RERUN THIS AFTER REBASE!
// length = 100 fixed propagation disabled and in place disabled
//============================================================================
// lowerOfUpper_1x1000000_reference                           347.74ns    2.88M
// lowerOfUpper_1x1000000_compiled                  136.95%   253.91ns    3.94M
// lowerOfUpper_1x1000000_vectorFunc                185.57%   187.38ns    5.34M
// lowerOfUpper_1x1000000_compileTime                 4.01%     8.66us  115.45K
// lowerOfUpper_10x100000_reference                           314.14ns    3.18M
// lowerOfUpper_10x100000_compiled                  133.83%   234.72ns    4.26M
// lowerOfUpper_10x100000_vectorFunc                155.68%   201.79ns    4.96M
// lowerOfUpper_10x100000_compileTime                 3.46%     9.09us  110.02K
// lowerOfUpper_100x10000_reference                           289.79ns    3.45M
// lowerOfUpper_100x10000_compiled                  124.11%   233.48ns    4.28M
// lowerOfUpper_100x10000_vectorFunc                150.24%   192.88ns    5.18M
// lowerOfUpper_100x10000_compileTime                 3.44%     8.42us  118.76K
//============================================================================

// compare2 large strings: (max 1000)

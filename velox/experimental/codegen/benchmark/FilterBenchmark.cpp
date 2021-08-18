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
class FilterBenchmark : public BenchmarkGTest {};
} // namespace

/*
compile filter:0,merge:0,defaultnull:1                       0.00fs  Infinity
filterSimple_10x100000_reference                            23.69ns   42.20M
filterSimple_10x100000_compiled                  114.82%    20.63ns   48.46M
filterSimple_10x100000_vectorFunc                455.95%     5.20ns  192.43M
filterSimple_10x100000_compileTime                 0.39%     6.04us  165.51K
compile filter:1,merge:0,defaultnull:1                       0.00fs  Infinity
filterSimple_10x100000_reference                            24.08ns   41.52M
filterSimple_10x100000_compiled                  188.82%    12.75ns   78.41M
filterSimple_10x100000_vectorFunc                928.46%     2.59ns  385.53M
filterSimple_10x100000_compileTime                 0.40%     5.99us  166.86K
compile filter:1,merge:1,defaultnull:0                       0.00fs  Infinity
filterSimple_10x100000_reference                            23.72ns   42.16M
filterSimple_10x100000_compiled                  269.39%     8.81ns  113.57M
filterSimple_10x100000_vectorFunc                270.59%     8.77ns  114.07M
filterSimple_10x100000_compileTime                 0.37%     6.49us  154.00K
compile filter:1,merge:1,defaultnull:1                       0.00fs  Infinity
filterSimple_10x100000_reference                            23.72ns   42.15M
filterSimple_10x100000_compiled                  351.98%     6.74ns  148.37M
filterSimple_10x100000_vectorFunc                344.23%     6.89ns  145.10M
filterSimple_10x100000_compileTime                 0.40%     5.97us  167.42K

compile filter:0,merge:0,defaultnull:1                       0.00fs  Infinity
filterSimple_100x10000_reference                            23.90ns   41.84M
filterSimple_100x10000_compiled                  111.75%    21.39ns   46.75M
filterSimple_100x10000_vectorFunc                914.51%     2.61ns  382.59M
filterSimple_100x10000_compileTime                 0.36%     6.59us  151.83K
compile filter:1,merge:0,defaultnull:1                       0.00fs  Infinity
filterSimple_100x10000_reference                            23.29ns   42.93M
filterSimple_100x10000_compiled                  164.79%    14.14ns   70.75M
filterSimple_100x10000_vectorFunc                876.63%     2.66ns  376.34M
filterSimple_100x10000_compileTime                 0.34%     6.81us  146.85K
compile filter:1,merge:1,defaultnull:0                       0.00fs  Infinity
filterSimple_100x10000_reference                            24.19ns   41.33M
filterSimple_100x10000_compiled                  275.72%     8.78ns  113.96M
filterSimple_100x10000_vectorFunc                397.05%     6.09ns  164.11M
filterSimple_100x10000_compileTime                 0.40%     5.98us  167.27K
compile filter:1,merge:1,defaultnull:1                       0.00fs  Infinity
filterSimple_100x10000_reference                            23.93ns   41.80M
filterSimple_100x10000_compiled                  308.84%     7.75ns  129.08M
filterSimple_100x10000_vectorFunc                466.19%     5.13ns  194.85M
filterSimple_100x10000_compileTime                 0.38%     6.23us  160.43K
 */
TEST_F(FilterBenchmark, DISABLED_FilterSimple) {
  auto inputRowType4 =
      ROW({"a", "b", "c", "d"},
          std::vector<TypePtr>{BIGINT(), BIGINT(), BIGINT(), BIGINT()});

  benchmark.benchmarkExpressionsMult<BigintType>(
      "filterSimple",
      "a > b",
      {"a+1"},
      inputRowType4,
      typicalBatches,
      defaultFilterFlags,
      true);
}

/*
compile filter:0,merge:0,defaultnull:1                       0.00fs  Infinity
filterDefaultNull_10x100000_reference                       54.31ns   18.41M
filterDefaultNull_10x100000_compiled             101.86%    53.32ns   18.75M
filterDefaultNull_10x100000_vectorFunc          2138.89%     2.54ns  393.82M
filterDefaultNull_10x100000_compileTime            0.75%     7.21us  138.78K
compile filter:1,merge:0,defaultnull:1                       0.00fs  Infinity
filterDefaultNull_10x100000_reference                       51.54ns   19.40M
filterDefaultNull_10x100000_compiled             443.70%    11.62ns   86.08M
filterDefaultNull_10x100000_vectorFunc           733.92%     7.02ns  142.39M
filterDefaultNull_10x100000_compileTime            0.66%     7.81us  128.03K
compile filter:1,merge:1,defaultnull:0                       0.00fs  Infinity
filterDefaultNull_10x100000_reference                       52.30ns   19.12M
filterDefaultNull_10x100000_compiled             369.98%    14.14ns   70.74M
filterDefaultNull_10x100000_vectorFunc           417.00%    12.54ns   79.73M
filterDefaultNull_10x100000_compileTime            0.79%     6.63us  150.78K
compile filter:1,merge:1,defaultnull:1                       0.00fs  Infinity
filterDefaultNull_10x100000_reference                       51.10ns   19.57M
filterDefaultNull_10x100000_compiled             659.69%     7.75ns  129.10M
filterDefaultNull_10x100000_vectorFunc           809.53%     6.31ns  158.42M
filterDefaultNull_10x100000_compileTime            0.76%     6.72us  148.90K

compile filter:0,merge:0,defaultnull:1                       0.00fs  Infinity
filterDefaultNull_100x10000_reference                       49.40ns   20.24M
filterDefaultNull_100x10000_compiled             103.38%    47.78ns   20.93M
filterDefaultNull_100x10000_vectorFunc          1923.72%     2.57ns  389.44M
filterDefaultNull_100x10000_compileTime            0.72%     6.86us  145.81K
compile filter:1,merge:0,defaultnull:1                       0.00fs  Infinity
filterDefaultNull_100x10000_reference                       50.03ns   19.99M
filterDefaultNull_100x10000_compiled             426.95%    11.72ns   85.33M
filterDefaultNull_100x10000_vectorFunc          1969.18%     2.54ns  393.58M
filterDefaultNull_100x10000_compileTime            0.77%     6.49us  154.14K
compile filter:1,merge:1,defaultnull:0                       0.00fs  Infinity
filterDefaultNull_100x10000_reference                       50.13ns   19.95M
filterDefaultNull_100x10000_compiled             340.61%    14.72ns   67.95M
filterDefaultNull_100x10000_vectorFunc           413.38%    12.13ns   82.46M
filterDefaultNull_100x10000_compileTime            0.76%     6.57us  152.26K
compile filter:1,merge:1,defaultnull:1                       0.00fs  Infinity
filterDefaultNull_100x10000_reference                       49.54ns   20.18M
filterDefaultNull_100x10000_compiled             645.31%     7.68ns  130.25M
filterDefaultNull_100x10000_vectorFunc           996.83%     4.97ns  201.20M
filterDefaultNull_100x10000_compileTime            0.72%     6.93us  144.34K
*/
TEST_F(FilterBenchmark, DISABLED_FilterDefaultNull) {
  auto inputRowType4 =
      ROW({"a", "b", "c", "d"},
          std::vector<TypePtr>{BIGINT(), BIGINT(), BIGINT(), BIGINT()});

  benchmark.benchmarkExpressionsMult<BigintType>(
      "filterDefaultNull",
      "a > b AND c > d",
      {"a+1"},
      inputRowType4,
      typicalBatches,
      defaultFilterFlags,
      true);
}

// logically equivalent to FilterDefaultNull
/*
compile filter:0,merge:0,defaultnull:1                       0.00fs  Infinity
filterDefaultNullNotOr_10x100000_reference                  51.00ns   19.61M
filterDefaultNullNotOr_10x100000_compiled        104.25%    48.92ns   20.44M
filterDefaultNullNotOr_10x100000_vectorFunc     2224.25%     2.29ns  436.10M
filterDefaultNullNotOr_10x100000_compileTime       0.76%     6.69us  149.46K
compile filter:1,merge:0,defaultnull:1                       0.00fs  Infinity
filterDefaultNullNotOr_10x100000_reference                  50.83ns   19.67M
filterDefaultNullNotOr_10x100000_compiled        463.18%    10.97ns   91.13M
filterDefaultNullNotOr_10x100000_vectorFunc     2243.22%     2.27ns  441.33M
filterDefaultNullNotOr_10x100000_compileTime       0.75%     6.82us  146.63K
compile filter:1,merge:1,defaultnull:0                       0.00fs  Infinity
filterDefaultNullNotOr_10x100000_reference                  50.80ns   19.69M
filterDefaultNullNotOr_10x100000_compiled        354.51%    14.33ns   69.79M
filterDefaultNullNotOr_10x100000_vectorFunc      391.09%    12.99ns   76.99M
filterDefaultNullNotOr_10x100000_compileTime       0.75%     6.74us  148.29K
compile filter:1,merge:1,defaultnull:1                       0.00fs  Infinity
filterDefaultNullNotOr_10x100000_reference                  50.80ns   19.69M
filterDefaultNullNotOr_10x100000_compiled        659.35%     7.70ns  129.79M
filterDefaultNullNotOr_10x100000_vectorFunc      816.64%     6.22ns  160.76M
filterDefaultNullNotOr_10x100000_compileTime       0.75%     6.82us  146.66K

compile filter:0,merge:0,defaultnull:1                       0.00fs  Infinity
filterDefaultNullNotOr_100x10000_reference                  50.40ns   19.84M
filterDefaultNullNotOr_100x10000_compiled        102.99%    48.94ns   20.43M
filterDefaultNullNotOr_100x10000_vectorFunc     2016.70%     2.50ns  400.11M
filterDefaultNullNotOr_100x10000_compileTime       0.76%     6.66us  150.11K
compile filter:1,merge:0,defaultnull:1                       0.00fs  Infinity
filterDefaultNullNotOr_100x10000_reference                  49.42ns   20.24M
filterDefaultNullNotOr_100x10000_compiled        416.44%    11.87ns   84.27M
filterDefaultNullNotOr_100x10000_vectorFunc     2005.17%     2.46ns  405.76M
filterDefaultNullNotOr_100x10000_compileTime       0.72%     6.88us  145.39K
compile filter:1,merge:1,defaultnull:0                       0.00fs  Infinity
filterDefaultNullNotOr_100x10000_reference                  51.43ns   19.44M
filterDefaultNullNotOr_100x10000_compiled        331.69%    15.51ns   64.49M
filterDefaultNullNotOr_100x10000_vectorFunc      401.82%    12.80ns   78.13M
filterDefaultNullNotOr_100x10000_compileTime       0.75%     6.87us  145.60K
compile filter:1,merge:1,defaultnull:1                       0.00fs  Infinity
filterDefaultNullNotOr_100x10000_reference                  50.59ns   19.77M
filterDefaultNullNotOr_100x10000_compiled        727.55%     6.95ns  143.81M
filterDefaultNullNotOr_100x10000_vectorFunc     1270.63%     3.98ns  251.15M
filterDefaultNullNotOr_100x10000_compileTime       0.74%     6.85us  146.01K
*/
TEST_F(FilterBenchmark, DISABLED_FilterDefaultNullNotOr) {
  auto inputRowType4 =
      ROW({"a", "b", "c", "d"},
          std::vector<TypePtr>{BIGINT(), BIGINT(), BIGINT(), BIGINT()});

  benchmark.benchmarkExpressionsMult<BigintType>(
      "filterDefaultNullNotOr",
      "NOT (a <= b OR c <= d)",
      {"a+1"},
      inputRowType4,
      typicalBatches,
      defaultFilterFlags,
      true);
}

/*
compile filter:0,merge:0,defaultnull:1                       0.00fs  Infinity
filterDefaultNullIsNull_10x100000_reference                 44.93ns   22.26M
filterDefaultNullIsNull_10x100000_compiled       102.60%    43.79ns   22.84M
filterDefaultNullIsNull_10x100000_vectorFunc    1918.78%     2.34ns  427.09M
filterDefaultNullIsNull_10x100000_compileTime      0.74%     6.08us  164.59K
compile filter:1,merge:0,defaultnull:1                       0.00fs  Infinity
filterDefaultNullIsNull_10x100000_reference                 44.86ns   22.29M
filterDefaultNullIsNull_10x100000_compiled       327.74%    13.69ns   73.06M
filterDefaultNullIsNull_10x100000_vectorFunc     864.16%     5.19ns  192.63M
filterDefaultNullIsNull_10x100000_compileTime      0.73%     6.11us  163.71K
compile filter:1,merge:1,defaultnull:0                       0.00fs  Infinity
filterDefaultNullIsNull_10x100000_reference                 44.64ns   22.40M
filterDefaultNullIsNull_10x100000_compiled       506.18%     8.82ns  113.39M
filterDefaultNullIsNull_10x100000_vectorFunc     504.80%     8.84ns  113.08M
filterDefaultNullIsNull_10x100000_compileTime      0.74%     6.04us  165.62K
compile filter:1,merge:1,defaultnull:1                       0.00fs  Infinity
filterDefaultNullIsNull_10x100000_reference                 44.49ns   22.48M
filterDefaultNullIsNull_10x100000_compiled       618.13%     7.20ns  138.94M
filterDefaultNullIsNull_10x100000_vectorFunc     782.73%     5.68ns  175.94M
filterDefaultNullIsNull_10x100000_compileTime      0.69%     6.41us  155.90K

compile filter:0,merge:0,defaultnull:1                       0.00fs  Infinity
filterDefaultNullIsNull_100x10000_reference                 45.55ns   21.96M
filterDefaultNullIsNull_100x10000_compiled       105.56%    43.15ns   23.18M
filterDefaultNullIsNull_100x10000_vectorFunc    1760.15%     2.59ns  386.46M
filterDefaultNullIsNull_100x10000_compileTime      0.65%     7.00us  142.83K
compile filter:1,merge:0,defaultnull:1                       0.00fs  Infinity
filterDefaultNullIsNull_100x10000_reference                 45.88ns   21.80M
filterDefaultNullIsNull_100x10000_compiled       296.67%    15.46ns   64.67M
filterDefaultNullIsNull_100x10000_vectorFunc    1800.86%     2.55ns  392.55M
filterDefaultNullIsNull_100x10000_compileTime      0.72%     6.34us  157.82K
compile filter:1,merge:1,defaultnull:0                       0.00fs  Infinity
filterDefaultNullIsNull_100x10000_reference                 45.38ns   22.03M
filterDefaultNullIsNull_100x10000_compiled       507.90%     8.94ns  111.91M
filterDefaultNullIsNull_100x10000_vectorFunc     739.18%     6.14ns  162.87M
filterDefaultNullIsNull_100x10000_compileTime      0.79%     5.76us  173.75K
compile filter:1,merge:1,defaultnull:1                       0.00fs  Infinity
filterDefaultNullIsNull_100x10000_reference                 45.33ns   22.06M
filterDefaultNullIsNull_100x10000_compiled       611.13%     7.42ns  134.80M
filterDefaultNullIsNull_100x10000_vectorFunc     899.93%     5.04ns  198.51M
filterDefaultNullIsNull_100x10000_compileTime      0.77%     5.89us  169.80K
*/
TEST_F(FilterBenchmark, DISABLED_FilterDefaultNullIsNull) {
  auto inputRowType4 =
      ROW({"a", "b"}, std::vector<TypePtr>{BIGINT(), BIGINT()});

  benchmark.benchmarkExpressionsMult<BigintType>(
      "filterDefaultNullIsNull",
      "b > 5 AND NOT a IS NULL",
      {"a+1"},
      inputRowType4,
      typicalBatches,
      defaultFilterFlags,
      true);
}

/*
compile filter:0,merge:0,defaultnull:1                       0.00fs  Infinity
filterDefaultNull4_10x100000_reference                      64.09ns   15.60M
filterDefaultNull4_10x100000_compiled            103.89%    61.69ns   16.21M
filterDefaultNull4_10x100000_vectorFunc         2799.06%     2.29ns  436.75M
filterDefaultNull4_10x100000_compileTime           0.97%     6.61us  151.19K
compile filter:1,merge:0,defaultnull:1                       0.00fs  Infinity
filterDefaultNull4_10x100000_reference                      63.35ns   15.79M
filterDefaultNull4_10x100000_compiled            570.52%    11.10ns   90.06M
filterDefaultNull4_10x100000_vectorFunc         1241.39%     5.10ns  195.96M
filterDefaultNull4_10x100000_compileTime           0.92%     6.86us  145.79K
compile filter:1,merge:1,defaultnull:0                       0.00fs  Infinity
filterDefaultNull4_10x100000_reference                      65.43ns   15.28M
filterDefaultNull4_10x100000_compiled            352.98%    18.54ns   53.95M
filterDefaultNull4_10x100000_vectorFunc          365.86%    17.88ns   55.92M
filterDefaultNull4_10x100000_compileTime           0.94%     6.96us  143.69K
compile filter:1,merge:1,defaultnull:1                       0.00fs  Infinity
filterDefaultNull4_10x100000_reference                      65.49ns   15.27M
filterDefaultNull4_10x100000_compiled            761.44%     8.60ns  116.27M
filterDefaultNull4_10x100000_vectorFunc          861.88%     7.60ns  131.60M
filterDefaultNull4_10x100000_compileTime           0.90%     7.29us  137.15K

compile filter:0,merge:0,defaultnull:1                       0.00fs  Infinity
filterDefaultNull4_100x10000_reference                      66.88ns   14.95M
filterDefaultNull4_100x10000_compiled            102.92%    64.99ns   15.39M
filterDefaultNull4_100x10000_vectorFunc         2970.84%     2.25ns  444.20M
filterDefaultNull4_100x10000_compileTime           0.97%     6.89us  145.13K
compile filter:1,merge:0,defaultnull:1                       0.00fs  Infinity
filterDefaultNull4_100x10000_reference                      66.06ns   15.14M
filterDefaultNull4_100x10000_compiled            650.10%    10.16ns   98.41M
filterDefaultNull4_100x10000_vectorFunc         2673.77%     2.47ns  404.73M
filterDefaultNull4_100x10000_compileTime           0.95%     6.95us  143.84K
compile filter:1,merge:1,defaultnull:0                       0.00fs  Infinity
filterDefaultNull4_100x10000_reference                      65.37ns   15.30M
filterDefaultNull4_100x10000_compiled            322.30%    20.28ns   49.30M
filterDefaultNull4_100x10000_vectorFunc          370.32%    17.65ns   56.65M
filterDefaultNull4_100x10000_compileTime           0.98%     6.69us  149.52K
compile filter:1,merge:1,defaultnull:1                       0.00fs  Infinity
filterDefaultNull4_100x10000_reference                      63.20ns   15.82M
filterDefaultNull4_100x10000_compiled            837.25%     7.55ns  132.48M
filterDefaultNull4_100x10000_vectorFunc         1215.43%     5.20ns  192.33M
filterDefaultNull4_100x10000_compileTime           0.94%     6.72us  148.79K
*/
TEST_F(FilterBenchmark, DISABLED_FilterDefaultNull4) {
  auto inputRowType4 =
      ROW({"a", "b", "c", "d"},
          std::vector<TypePtr>{BIGINT(), BIGINT(), BIGINT(), BIGINT()});

  benchmark.benchmarkExpressionsMult<BigintType>(
      "filterDefaultNull4",
      "a > 5 AND b > 5 AND c > 5 and d > 5",
      {"a+1"},
      inputRowType4,
      typicalBatches,
      defaultFilterFlags,
      true);
}

// filter columns are not superset of projection columns
/*
compile filter:0,merge:0,defaultnull:1                       0.00fs  Infinity
filterDefaultNullNotSuper_10x100000_reference               50.69ns   19.73M
filterDefaultNullNotSuper_10x100000_compiled     103.21%    49.12ns   20.36M
filterDefaultNullNotSuper_10x100000_vectorFunc  2170.17%     2.34ns  428.09M
filterDefaultNullNotSuper_10x100000_compileTime    0.75%     6.80us  147.08K
compile filter:1,merge:0,defaultnull:1                       0.00fs  Infinity
filterDefaultNullNotSuper_10x100000_reference               52.45ns   19.07M
filterDefaultNullNotSuper_10x100000_compiled     388.27%    13.51ns   74.03M
filterDefaultNullNotSuper_10x100000_vectorFunc  2298.54%     2.28ns  438.23M
filterDefaultNullNotSuper_10x100000_compileTime    0.79%     6.65us  150.28K
compile filter:1,merge:1,defaultnull:0                       0.00fs  Infinity
filterDefaultNullNotSuper_10x100000_reference               51.87ns   19.28M
filterDefaultNullNotSuper_10x100000_compiled     413.22%    12.55ns   79.67M
filterDefaultNullNotSuper_10x100000_vectorFunc   469.73%    11.04ns   90.57M
filterDefaultNullNotSuper_10x100000_compileTime    0.76%     6.80us  147.15K
compile filter:1,merge:1,defaultnull:1                       0.00fs  Infinity
filterDefaultNullNotSuper_10x100000_reference               50.28ns   19.89M
filterDefaultNullNotSuper_10x100000_compiled     574.17%     8.76ns  114.20M
filterDefaultNullNotSuper_10x100000_vectorFunc   592.90%     8.48ns  117.92M
filterDefaultNullNotSuper_10x100000_compileTime    0.75%     6.73us  148.63K

compile filter:0,merge:0,defaultnull:1                       0.00fs  Infinity
filterDefaultNullNotSuper_100x10000_reference               51.15ns   19.55M
filterDefaultNullNotSuper_100x10000_compiled     103.73%    49.31ns   20.28M
filterDefaultNullNotSuper_100x10000_vectorFunc  1759.57%     2.91ns  344.02M
filterDefaultNullNotSuper_100x10000_compileTime    0.75%     6.85us  146.01K
compile filter:1,merge:0,defaultnull:1                       0.00fs  Infinity
filterDefaultNullNotSuper_100x10000_reference               53.12ns   18.83M
filterDefaultNullNotSuper_100x10000_compiled     357.03%    14.88ns   67.22M
filterDefaultNullNotSuper_100x10000_vectorFunc  1996.39%     2.66ns  375.85M
filterDefaultNullNotSuper_100x10000_compileTime    0.78%     6.83us  146.33K
compile filter:1,merge:1,defaultnull:0                       0.00fs  Infinity
filterDefaultNullNotSuper_100x10000_reference               50.52ns   19.79M
filterDefaultNullNotSuper_100x10000_compiled     381.40%    13.25ns   75.50M
filterDefaultNullNotSuper_100x10000_vectorFunc   515.57%     9.80ns  102.05M
filterDefaultNullNotSuper_100x10000_compileTime    0.75%     6.74us  148.43K
compile filter:1,merge:1,defaultnull:1                       0.00fs  Infinity
filterDefaultNullNotSuper_100x10000_reference               52.33ns   19.11M
filterDefaultNullNotSuper_100x10000_compiled     567.04%     9.23ns  108.36M
filterDefaultNullNotSuper_100x10000_vectorFunc   812.88%     6.44ns  155.34M
filterDefaultNullNotSuper_100x10000_compileTime    0.71%     7.33us  136.47K
*/
TEST_F(FilterBenchmark, DISABLED_FilterDefaultNullNotSuper) {
  auto inputRowType4 =
      ROW({"a", "b", "c", "d"},
          std::vector<TypePtr>{BIGINT(), BIGINT(), BIGINT(), BIGINT()});

  benchmark.benchmarkExpressionsMult<BigintType>(
      "filterDefaultNullNotSuper",
      "a > 5 AND b > 5",
      {"c+1"},
      inputRowType4,
      typicalBatches,
      defaultFilterFlags,
      true);
}

/*
compile filter:0,merge:0,defaultnull:1                       0.00fs  Infinity
filterNullOr_10x100000_reference                            32.06ns   31.19M
filterNullOr_10x100000_compiled                  109.26%    29.35ns   34.08M
filterNullOr_10x100000_vectorFunc                660.19%     4.86ns  205.91M
filterNullOr_10x100000_compileTime                 0.49%     6.60us  151.48K
compile filter:1,merge:0,defaultnull:1                       0.00fs  Infinity
filterNullOr_10x100000_reference                            33.29ns   30.04M
filterNullOr_10x100000_compiled                  162.55%    20.48ns   48.83M
filterNullOr_10x100000_vectorFunc                653.66%     5.09ns  196.36M
filterNullOr_10x100000_compileTime                 0.49%     6.77us  147.79K
compile filter:1,merge:1,defaultnull:0                       0.00fs  Infinity
filterNullOr_10x100000_reference                            33.81ns   29.58M
filterNullOr_10x100000_compiled                  425.16%     7.95ns  125.75M
filterNullOr_10x100000_vectorFunc                451.65%     7.49ns  133.59M
filterNullOr_10x100000_compileTime                 0.49%     6.91us  144.72K
compile filter:1,merge:1,defaultnull:1                       0.00fs  Infinity
filterNullOr_10x100000_reference                            34.32ns   29.14M
filterNullOr_10x100000_compiled                  447.31%     7.67ns  130.35M
filterNullOr_10x100000_vectorFunc                453.31%     7.57ns  132.10M
filterNullOr_10x100000_compileTime                 0.52%     6.65us  150.38K

compile filter:0,merge:0,defaultnull:1                       0.00fs  Infinity
filterNullOr_100x10000_reference                            34.82ns   28.72M
filterNullOr_100x10000_compiled                  115.13%    30.24ns   33.07M
filterNullOr_100x10000_vectorFunc               1432.15%     2.43ns  411.33M
filterNullOr_100x10000_compileTime                 0.53%     6.53us  153.07K
compile filter:1,merge:0,defaultnull:1                       0.00fs  Infinity
filterNullOr_100x10000_reference                            33.91ns   29.49M
filterNullOr_100x10000_compiled                  171.28%    19.80ns   50.52M
filterNullOr_100x10000_vectorFunc               1388.55%     2.44ns  409.53M
filterNullOr_100x10000_compileTime                 0.52%     6.55us  152.67K
compile filter:1,merge:1,defaultnull:0                       0.00fs  Infinity
filterNullOr_100x10000_reference                            33.88ns   29.51M
filterNullOr_100x10000_compiled                  437.91%     7.74ns  129.24M
filterNullOr_100x10000_vectorFunc                691.67%     4.90ns  204.13M
filterNullOr_100x10000_compileTime                 0.51%     6.58us  151.94K
compile filter:1,merge:1,defaultnull:1                       0.00fs  Infinity
filterNullOr_100x10000_reference                            33.62ns   29.74M
filterNullOr_100x10000_compiled                  430.06%     7.82ns  127.91M
filterNullOr_100x10000_vectorFunc                738.89%     4.55ns  219.77M
filterNullOr_100x10000_compileTime                 0.48%     6.96us  143.63K
 */
TEST_F(FilterBenchmark, DISABLED_FilterNullOr) {
  auto inputRowType4 =
      ROW({"a", "b", "c", "d"},
          std::vector<TypePtr>{BIGINT(), BIGINT(), BIGINT(), BIGINT()});

  benchmark.benchmarkExpressionsMult<BigintType>(
      "filterNullOr",
      "(NOT a IS NULL) OR (NOT b IS NULL) OR (NOT c IS NULL) OR (NOT d IS NULL)",
      {"a+1"},
      inputRowType4,
      typicalBatches,
      defaultFilterFlags,
      true);
}

// logically equivalent to FilterNullOr
/*
compile filter:0,merge:0,defaultnull:1                       0.00fs  Infinity
filterNullOrEquiv_10x100000_reference                       29.59ns   33.79M
filterNullOrEquiv_10x100000_compiled             114.96%    25.74ns   38.85M
filterNullOrEquiv_10x100000_vectorFunc           597.94%     4.95ns  202.05M
filterNullOrEquiv_10x100000_compileTime            0.50%     5.89us  169.70K
compile filter:1,merge:0,defaultnull:1                       0.00fs  Infinity
filterNullOrEquiv_10x100000_reference                       29.89ns   33.45M
filterNullOrEquiv_10x100000_compiled             138.01%    21.66ns   46.17M
filterNullOrEquiv_10x100000_vectorFunc           556.72%     5.37ns  186.24M
filterNullOrEquiv_10x100000_compileTime            0.50%     6.02us  166.18K
compile filter:1,merge:1,defaultnull:0                       0.00fs  Infinity
filterNullOrEquiv_10x100000_reference                       30.26ns   33.05M
filterNullOrEquiv_10x100000_compiled             568.92%     5.32ns  188.01M
filterNullOrEquiv_10x100000_vectorFunc           539.84%     5.61ns  178.40M
filterNullOrEquiv_10x100000_compileTime            0.52%     5.85us  170.81K
compile filter:1,merge:1,defaultnull:1                       0.00fs  Infinity
filterNullOrEquiv_10x100000_reference                       29.78ns   33.58M
filterNullOrEquiv_10x100000_compiled             504.15%     5.91ns  169.30M
filterNullOrEquiv_10x100000_vectorFunc           548.67%     5.43ns  184.25M
filterNullOrEquiv_10x100000_compileTime            0.51%     5.84us  171.34K

compile filter:0,merge:0,defaultnull:1                       0.00fs  Infinity
filterNullOrEquiv_100x10000_reference                       31.30ns   31.95M
filterNullOrEquiv_100x10000_compiled             109.49%    28.58ns   34.99M
filterNullOrEquiv_100x10000_vectorFunc           570.86%     5.48ns  182.41M
filterNullOrEquiv_100x10000_compileTime            0.52%     6.05us  165.19K
compile filter:1,merge:0,defaultnull:1                       0.00fs  Infinity
filterNullOrEquiv_100x10000_reference                       31.41ns   31.84M
filterNullOrEquiv_100x10000_compiled             137.62%    22.82ns   43.82M
filterNullOrEquiv_100x10000_vectorFunc          1212.68%     2.59ns  386.10M
filterNullOrEquiv_100x10000_compileTime            0.54%     5.80us  172.36K
compile filter:1,merge:1,defaultnull:0                       0.00fs  Infinity
filterNullOrEquiv_100x10000_reference                       30.19ns   33.13M
filterNullOrEquiv_100x10000_compiled             593.72%     5.08ns  196.69M
filterNullOrEquiv_100x10000_vectorFunc          1349.94%     2.24ns  447.20M
filterNullOrEquiv_100x10000_compileTime            0.52%     5.78us  173.10K
compile filter:1,merge:1,defaultnull:1                       0.00fs  Infinity
filterNullOrEquiv_100x10000_reference                       33.35ns   29.99M
filterNullOrEquiv_100x10000_compiled             640.87%     5.20ns  192.17M
filterNullOrEquiv_100x10000_vectorFunc           640.92%     5.20ns  192.19M
filterNullOrEquiv_100x10000_compileTime            0.56%     5.96us  167.85K
*/
TEST_F(FilterBenchmark, DISABLED_FilterNullEquiv) {
  auto inputRowType4 =
      ROW({"a", "b", "c", "d"},
          std::vector<TypePtr>{BIGINT(), BIGINT(), BIGINT(), BIGINT()});

  benchmark.benchmarkExpressionsMult<BigintType>(
      "filterNullOrEquiv",
      "NOT COALESCE(a,b,c,d) IS NULL",
      {"a+1"},
      inputRowType4,
      typicalBatches,
      defaultFilterFlags,
      true);
}

/*
compile filter:0,merge:0,defaultnull:1                       0.00fs  Infinity
filterIf_10x100000_reference                                45.34ns   22.06M
filterIf_10x100000_compiled                      102.73%    44.13ns   22.66M
filterIf_10x100000_vectorFunc                   2164.53%     2.09ns  477.41M
filterIf_10x100000_compileTime                     0.66%     6.84us  146.26K
compile filter:1,merge:0,defaultnull:1                       0.00fs  Infinity
filterIf_10x100000_reference                                46.81ns   21.36M
filterIf_10x100000_compiled                      177.56%    26.36ns   37.93M
filterIf_10x100000_vectorFunc                    961.21%     4.87ns  205.34M
filterIf_10x100000_compileTime                     0.69%     6.74us  148.29K
compile filter:1,merge:1,defaultnull:0                       0.00fs  Infinity
filterIf_10x100000_reference                                46.91ns   21.32M
filterIf_10x100000_compiled                      265.05%    17.70ns   56.51M
filterIf_10x100000_vectorFunc                    262.34%    17.88ns   55.93M
filterIf_10x100000_compileTime                     0.70%     6.73us  148.66K
compile filter:1,merge:1,defaultnull:1                       0.00fs  Infinity
filterIf_10x100000_reference                                45.32ns   22.06M
filterIf_10x100000_compiled                      261.54%    17.33ns   57.70M
filterIf_10x100000_vectorFunc                    257.24%    17.62ns   56.76M
filterIf_10x100000_compileTime                     0.70%     6.45us  155.01K

compile filter:0,merge:0,defaultnull:1                       0.00fs  Infinity
filterIf_100x10000_reference                                45.29ns   22.08M
filterIf_100x10000_compiled                      103.01%    43.97ns   22.74M
filterIf_100x10000_vectorFunc                   1834.37%     2.47ns  405.03M
filterIf_100x10000_compileTime                     0.66%     6.82us  146.69K
compile filter:1,merge:0,defaultnull:1                       0.00fs  Infinity
filterIf_100x10000_reference                                45.36ns   22.05M
filterIf_100x10000_compiled                      168.23%    26.96ns   37.09M
filterIf_100x10000_vectorFunc                   1695.21%     2.68ns  373.75M
filterIf_100x10000_compileTime                     0.67%     6.74us  148.26K
compile filter:1,merge:1,defaultnull:0                       0.00fs  Infinity
filterIf_100x10000_reference                                45.49ns   21.98M
filterIf_100x10000_compiled                      244.71%    18.59ns   53.79M
filterIf_100x10000_vectorFunc                    293.80%    15.48ns   64.58M
filterIf_100x10000_compileTime                     0.66%     6.87us  145.50K
compile filter:1,merge:1,defaultnull:1                       0.00fs  Infinity
filterIf_100x10000_reference                                47.80ns   20.92M
filterIf_100x10000_compiled                      221.53%    21.58ns   46.35M
filterIf_100x10000_vectorFunc                    304.63%    15.69ns   63.73M
filterIf_100x10000_compileTime                     0.69%     6.94us  144.13K
*/
TEST_F(FilterBenchmark, DISABLED_FilterIf) {
  auto inputRowType4 =
      ROW({"a", "b", "c", "d"},
          std::vector<TypePtr>{BIGINT(), BIGINT(), BIGINT(), BIGINT()});

  benchmark.benchmarkExpressionsMult<BigintType>(
      "filterIf",
      "if(a>b,c,d) > 6",
      {"a+1"},
      inputRowType4,
      typicalBatches,
      defaultFilterFlags,
      true);
}

/*
compile filter:0,merge:0,defaultnull:1                       0.00fs  Infinity
filterCase_10x100000_reference                              54.11ns   18.48M
filterCase_10x100000_compiled                    106.86%    50.64ns   19.75M
filterCase_10x100000_vectorFunc                 2372.79%     2.28ns  438.49M
filterCase_10x100000_compileTime                   0.84%     6.45us  155.00K
compile filter:1,merge:0,defaultnull:1                       0.00fs  Infinity
filterCase_10x100000_reference                              52.83ns   18.93M
filterCase_10x100000_compiled                    348.09%    15.18ns   65.89M
filterCase_10x100000_vectorFunc                 2285.78%     2.31ns  432.68M
filterCase_10x100000_compileTime                   0.91%     5.80us  172.37K
compile filter:1,merge:1,defaultnull:0                       0.00fs  Infinity
filterCase_10x100000_reference                              54.03ns   18.51M
filterCase_10x100000_compiled                    837.21%     6.45ns  154.94M
filterCase_10x100000_vectorFunc                  903.25%     5.98ns  167.17M
filterCase_10x100000_compileTime                   0.92%     5.88us  169.94K
compile filter:1,merge:1,defaultnull:1                       0.00fs  Infinity
filterCase_10x100000_reference                              55.38ns   18.06M
filterCase_10x100000_compiled                    855.57%     6.47ns  154.49M
filterCase_10x100000_vectorFunc                  951.02%     5.82ns  171.72M
filterCase_10x100000_compileTime                   0.93%     5.96us  167.74K

compile filter:0,merge:0,defaultnull:1                       0.00fs  Infinity
filterCase_100x10000_reference                              56.30ns   17.76M
filterCase_100x10000_compiled                    110.03%    51.17ns   19.54M
filterCase_100x10000_vectorFunc                 2036.41%     2.76ns  361.70M
filterCase_100x10000_compileTime                   0.94%     6.00us  166.71K
compile filter:1,merge:0,defaultnull:1                       0.00fs  Infinity
filterCase_100x10000_reference                              54.50ns   18.35M
filterCase_100x10000_compiled                    348.59%    15.64ns   63.96M
filterCase_100x10000_vectorFunc                 2117.75%     2.57ns  388.55M
filterCase_100x10000_compileTime                   0.92%     5.93us  168.52K
compile filter:1,merge:1,defaultnull:0                       0.00fs  Infinity
filterCase_100x10000_reference                              54.59ns   18.32M
filterCase_100x10000_compiled                    879.51%     6.21ns  161.11M
filterCase_100x10000_vectorFunc                 1460.48%     3.74ns  267.53M
filterCase_100x10000_compileTime                   0.89%     6.11us  163.70K
compile filter:1,merge:1,defaultnull:1                       0.00fs  Infinity
filterCase_100x10000_reference                              54.89ns   18.22M
filterCase_100x10000_compiled                    799.39%     6.87ns  145.63M
filterCase_100x10000_vectorFunc                 1428.02%     3.84ns  260.15M
filterCase_100x10000_compileTime                   0.92%     5.95us  168.08K
*/
TEST_F(FilterBenchmark, DISABLED_FilterCase) {
  auto inputRowType4 =
      ROW({"a", "b", "c", "d"},
          std::vector<TypePtr>{BIGINT(), BIGINT(), BIGINT(), BIGINT()});
  benchmark.benchmarkExpressionsMult<BigintType>(
      "filterCase",
      "CASE a WHEN b THEN b WHEN c THEN c WHEN d THEN d ELSE a END < 5",
      {"a+1"},
      inputRowType4,
      typicalBatches,
      defaultFilterFlags,
      true);
}

// logically equivalent to FilterCase
/*
compile filter:0,merge:0,defaultnull:1                       0.00fs  Infinity
filterCaseEquiv_10x100000_reference                         25.71ns   38.89M
filterCaseEquiv_10x100000_compiled               112.89%    22.78ns   43.90M
filterCaseEquiv_10x100000_vectorFunc             530.98%     4.84ns  206.51M
filterCaseEquiv_10x100000_compileTime              0.43%     5.98us  167.13K
compile filter:1,merge:0,defaultnull:1                       0.00fs  Infinity
filterCaseEquiv_10x100000_reference                         26.13ns   38.27M
filterCaseEquiv_10x100000_compiled               192.17%    13.60ns   73.54M
filterCaseEquiv_10x100000_vectorFunc            1117.85%     2.34ns  427.78M
filterCaseEquiv_10x100000_compileTime              0.44%     5.89us  169.78K
compile filter:1,merge:1,defaultnull:0                       0.00fs  Infinity
filterCaseEquiv_10x100000_reference                         25.37ns   39.41M
filterCaseEquiv_10x100000_compiled               403.17%     6.29ns  158.90M
filterCaseEquiv_10x100000_vectorFunc             430.09%     5.90ns  169.51M
filterCaseEquiv_10x100000_compileTime              0.43%     5.95us  168.13K
compile filter:1,merge:1,defaultnull:1                       0.00fs  Infinity
filterCaseEquiv_10x100000_reference                         25.63ns   39.02M
filterCaseEquiv_10x100000_compiled               368.84%     6.95ns  143.91M
filterCaseEquiv_10x100000_vectorFunc             380.23%     6.74ns  148.36M
filterCaseEquiv_10x100000_compileTime              0.39%     6.60us  151.61K

compile filter:0,merge:0,defaultnull:1                       0.00fs  Infinity
filterCaseEquiv_100x10000_reference                         26.56ns   37.65M
filterCaseEquiv_100x10000_compiled               111.18%    23.89ns   41.86M
filterCaseEquiv_100x10000_vectorFunc             850.51%     3.12ns  320.18M
filterCaseEquiv_100x10000_compileTime              0.44%     6.06us  164.94K
compile filter:1,merge:0,defaultnull:1                       0.00fs  Infinity
filterCaseEquiv_100x10000_reference                         26.67ns   37.49M
filterCaseEquiv_100x10000_compiled               173.30%    15.39ns   64.97M
filterCaseEquiv_100x10000_vectorFunc            1029.77%     2.59ns  386.10M
filterCaseEquiv_100x10000_compileTime              0.46%     5.78us  173.06K
compile filter:1,merge:1,defaultnull:0                       0.00fs  Infinity
filterCaseEquiv_100x10000_reference                         28.31ns   35.33M
filterCaseEquiv_100x10000_compiled               380.50%     7.44ns  134.41M
filterCaseEquiv_100x10000_vectorFunc             795.45%     3.56ns  281.00M
filterCaseEquiv_100x10000_compileTime              0.48%     5.84us  171.20K
compile filter:1,merge:1,defaultnull:1                       0.00fs  Infinity
filterCaseEquiv_100x10000_reference                         26.74ns   37.39M
filterCaseEquiv_100x10000_compiled               326.48%     8.19ns  122.09M
filterCaseEquiv_100x10000_vectorFunc             498.32%     5.37ns  186.34M
filterCaseEquiv_100x10000_compileTime              0.39%     6.81us  146.78K
compile filter:0,merge:0,defaultnull:1                       0.00fs  Infinity
*/
TEST_F(FilterBenchmark, DISABLED_FilterCaseEquiv) {
  auto inputRowType4 =
      ROW({"a", "b", "c", "d"},
          std::vector<TypePtr>{BIGINT(), BIGINT(), BIGINT(), BIGINT()});
  benchmark.benchmarkExpressionsMult<BigintType>(
      "filterCaseEquiv",
      "a < 5",
      {"a+1"},
      inputRowType4,
      typicalBatches,
      defaultFilterFlags,
      true);
}

/*
compile filter:0,merge:0,defaultnull:1                       0.00fs  Infinity
filterComplex_10x100000_reference                          182.64ns    5.48M
filterComplex_10x100000_compiled                 101.39%   180.13ns    5.55M
filterComplex_10x100000_vectorFunc              8492.81%     2.15ns  464.99M
filterComplex_10x100000_compileTime                2.92%     6.25us  160.03K
compile filter:1,merge:0,defaultnull:1                       0.00fs  Infinity
filterComplex_10x100000_reference                          180.96ns    5.53M
filterComplex_10x100000_compiled                 385.80%    46.91ns   21.32M
filterComplex_10x100000_vectorFunc              8377.67%     2.16ns  462.96M
filterComplex_10x100000_compileTime                2.68%     6.75us  148.04K
compile filter:1,merge:1,defaultnull:0                       0.00fs  Infinity
filterComplex_10x100000_reference                          182.84ns    5.47M
filterComplex_10x100000_compiled                4391.66%     4.16ns  240.19M
filterComplex_10x100000_vectorFunc               437.74%    41.77ns   23.94M
filterComplex_10x100000_compileTime                2.75%     6.66us  150.23K
compile filter:1,merge:1,defaultnull:1                       0.00fs  Infinity
filterComplex_10x100000_reference                          188.13ns    5.32M
filterComplex_10x100000_compiled                4421.71%     4.25ns  235.04M
filterComplex_10x100000_vectorFunc               448.02%    41.99ns   23.81M
filterComplex_10x100000_compileTime                2.87%     6.56us  152.45K

compile filter:0,merge:0,defaultnull:1                       0.00fs  Infinity
filterComplex_100x10000_reference                          182.84ns    5.47M
filterComplex_100x10000_compiled                 100.25%   182.39ns    5.48M
filterComplex_100x10000_vectorFunc              3701.53%     4.94ns  202.45M
filterComplex_100x10000_compileTime                2.68%     6.83us  146.52K
compile filter:1,merge:0,defaultnull:1                       0.00fs  Infinity
filterComplex_100x10000_reference                          177.97ns    5.62M
filterComplex_100x10000_compiled                 382.88%    46.48ns   21.51M
filterComplex_100x10000_vectorFunc              8008.42%     2.22ns  449.99M
filterComplex_100x10000_compileTime                2.76%     6.45us  155.00K
compile filter:1,merge:1,defaultnull:0                       0.00fs  Infinity
filterComplex_100x10000_reference                          177.29ns    5.64M
filterComplex_100x10000_compiled                47585.81%   372.56ps    2.68G
filterComplex_100x10000_vectorFunc               418.21%    42.39ns   23.59M
filterComplex_100x10000_compileTime                2.57%     6.91us  144.71K
compile filter:1,merge:1,defaultnull:1                       0.00fs  Infinity
filterComplex_100x10000_reference                          180.17ns    5.55M
filterComplex_100x10000_compiled                48966.06%   367.94ps    2.72G
filterComplex_100x10000_vectorFunc               415.12%    43.40ns   23.04M
filterComplex_100x10000_compileTime                2.77%     6.51us  153.55K
*/
TEST_F(FilterBenchmark, DISABLED_FilterComplex) {
  auto inputRowType4 =
      ROW({"a", "b", "c", "d"},
          std::vector<TypePtr>{DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE()});

  benchmark.benchmarkExpressionsMult<DoubleType>(
      "filterComplex",
      "CASE (CAST (COALESCE(a,1.0)*COALESCE(b, 1.0)/COALESCE(c,1.0)-COALESCE(d,1.0) AS INTEGER) % 4)"
      "  WHEN 0 THEN a "
      "  WHEN 1 THEN b "
      "  WHEN 2 THEN c "
      "  ELSE d "
      "END > 1.0",
      {"a+1.0"},
      inputRowType4,
      typicalBatches,
      defaultFilterFlags,
      true);
}

/*
compile filter:0,merge:0,defaultnull:1                       0.00fs  Infinity
filterOp_10x100000_reference                                34.90ns   28.65M
filterOp_10x100000_compiled                      101.35%    34.43ns   29.04M
filterOp_10x100000_vectorFunc                    718.20%     4.86ns  205.80M
filterOp_10x100000_compileTime                     0.56%     6.22us  160.71K
compile filter:1,merge:0,defaultnull:1                       0.00fs  Infinity
filterOp_10x100000_reference                                35.91ns   27.85M
filterOp_10x100000_compiled                      244.47%    14.69ns   68.08M
filterOp_10x100000_vectorFunc                   1669.49%     2.15ns  464.93M
filterOp_10x100000_compileTime                     0.57%     6.29us  159.06K
compile filter:1,merge:1,defaultnull:0                       0.00fs  Infinity
filterOp_10x100000_reference                                35.82ns   27.92M
filterOp_10x100000_compiled                      294.70%    12.16ns   82.27M
filterOp_10x100000_vectorFunc                    363.60%     9.85ns  101.50M
filterOp_10x100000_compileTime                     0.55%     6.48us  154.26K
compile filter:1,merge:1,defaultnull:1                       0.00fs  Infinity
filterOp_10x100000_reference                                34.48ns   29.00M
filterOp_10x100000_compiled                      545.86%     6.32ns  158.32M
filterOp_10x100000_vectorFunc                    850.64%     4.05ns  246.72M
filterOp_10x100000_compileTime                     0.55%     6.30us  158.77K

compile filter:0,merge:0,defaultnull:1                       0.00fs  Infinity
filterOp_100x10000_reference                                30.91ns   32.36M
filterOp_100x10000_compiled                      100.26%    30.82ns   32.44M
filterOp_100x10000_vectorFunc                   1530.42%     2.02ns  495.20M
filterOp_100x10000_compileTime                     0.49%     6.27us  159.60K
compile filter:1,merge:0,defaultnull:1                       0.00fs  Infinity
filterOp_100x10000_reference                                31.15ns   32.10M
filterOp_100x10000_compiled                      192.59%    16.17ns   61.83M
filterOp_100x10000_vectorFunc                   1663.29%     1.87ns  533.96M
filterOp_100x10000_compileTime                     0.49%     6.35us  157.42K
compile filter:1,merge:1,defaultnull:0                       0.00fs  Infinity
filterOp_100x10000_reference                                33.01ns   30.30M
filterOp_100x10000_compiled                      238.41%    13.84ns   72.23M
filterOp_100x10000_vectorFunc                    333.52%     9.90ns  101.04M
filterOp_100x10000_compileTime                     0.50%     6.65us  150.37K
compile filter:1,merge:1,defaultnull:1                       0.00fs  Infinity
filterOp_100x10000_reference                                31.54ns   31.71M
filterOp_100x10000_compiled                      442.94%     7.12ns  140.46M
filterOp_100x10000_vectorFunc                    826.79%     3.81ns  262.18M
filterOp_100x10000_compileTime                     0.50%     6.32us  158.13K
*/
TEST_F(FilterBenchmark, DISABLED_FilterOp) {
  auto inputRowType4 =
      ROW({"a", "b", "c", "d"},
          std::vector<TypePtr>{DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE()});

  benchmark.benchmarkExpressionsMult<DoubleType>(
      "filterOp",
      "1.1*a+b/1.01+c*c > d",
      {"a+1.0"},
      inputRowType4,
      typicalBatches,
      defaultFilterFlags,
      true);
}

/*
filterOpSimplifiable_10x100000_reference                    30.72ns   32.55M
filterOpSimplifiable_10x100000_compiled          102.88%    29.86ns   33.49M
filterOpSimplifiable_10x100000_vectorFunc       1401.37%     2.19ns  456.19M
filterOpSimplifiable_10x100000_compileTime         0.47%     6.57us  152.13K
compile filter:1,merge:0,defaultnull:1                       0.00fs  Infinity
filterOpSimplifiable_10x100000_reference                    31.31ns   31.94M
filterOpSimplifiable_10x100000_compiled          793.63%     3.95ns  253.47M
filterOpSimplifiable_10x100000_vectorFunc       1357.78%     2.31ns  433.65M
filterOpSimplifiable_10x100000_compileTime         0.48%     6.50us  153.85K
compile filter:1,merge:1,defaultnull:0                       0.00fs  Infinity
filterOpSimplifiable_10x100000_reference                    30.58ns   32.71M
filterOpSimplifiable_10x100000_compiled         7861.42%   388.94ps    2.57G
filterOpSimplifiable_10x100000_vectorFunc        793.46%     3.85ns  259.50M
filterOpSimplifiable_10x100000_compileTime         0.42%     7.34us  136.15K
compile filter:1,merge:1,defaultnull:1                       0.00fs  Infinity
filterOpSimplifiable_10x100000_reference                    30.57ns   32.71M
filterOpSimplifiable_10x100000_compiled         19807.14%   154.35ps    6.48G
filterOpSimplifiable_10x100000_vectorFunc       1733.13%     1.76ns  566.88M
filterOpSimplifiable_10x100000_compileTime         0.48%     6.39us  156.54K

compile filter:0,merge:0,defaultnull:1                       0.00fs  Infinity
filterOpSimplifiable_100x10000_reference                    25.87ns   38.66M
filterOpSimplifiable_100x10000_compiled           97.23%    26.61ns   37.59M
filterOpSimplifiable_100x10000_vectorFunc       1328.66%     1.95ns  513.60M
filterOpSimplifiable_100x10000_compileTime         0.40%     6.44us  155.28K
compile filter:1,merge:0,defaultnull:1                       0.00fs  Infinity
filterOpSimplifiable_100x10000_reference                    25.16ns   39.75M
filterOpSimplifiable_100x10000_compiled          549.74%     4.58ns  218.51M
filterOpSimplifiable_100x10000_vectorFunc       1285.77%     1.96ns  511.06M
filterOpSimplifiable_100x10000_compileTime         0.40%     6.26us  159.75K
compile filter:1,merge:1,defaultnull:0                       0.00fs  Infinity
filterOpSimplifiable_100x10000_reference                    24.79ns   40.34M
filterOpSimplifiable_100x10000_compiled             inf%     0.00fs  Infinity
filterOpSimplifiable_100x10000_vectorFunc        878.21%     2.82ns  354.26M
filterOpSimplifiable_100x10000_compileTime         0.35%     7.07us  141.45K
compile filter:1,merge:1,defaultnull:1                       0.00fs  Infinity
filterOpSimplifiable_100x10000_reference                    25.07ns   39.89M
filterOpSimplifiable_100x10000_compiled             inf%     0.00fs  Infinity
filterOpSimplifiable_100x10000_vectorFunc       1310.60%     1.91ns  522.78M
filterOpSimplifiable_100x10000_compileTime         0.41%     6.11us  163.57K
*/
TEST_F(FilterBenchmark, DISABLED_FilterOpSimplifiable) {
  auto inputRowType4 =
      ROW({"a", "b", "c", "d"},
          std::vector<TypePtr>{DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE()});

  benchmark.benchmarkExpressionsMult<DoubleType>(
      "filterOpSimplifiable",
      "a > b * b * b * (1.0/b) * (1.0/b) * (1.0/b)",
      {"a+1.0"},
      inputRowType4,
      typicalBatches,
      defaultFilterFlags,
      true);
}

// logically equivalent FilterOpSimplifiable
/*
compile filter:0,merge:0,defaultnull:1                       0.00fs  Infinity
filterOpSimplifiableEquiv_10x100000_reference               24.76ns   40.39M
filterOpSimplifiableEquiv_10x100000_compiled     100.65%    24.60ns   40.66M
filterOpSimplifiableEquiv_10x100000_vectorFunc  1138.77%     2.17ns  460.00M
filterOpSimplifiableEquiv_10x100000_compileTime    0.39%     6.33us  157.96K
compile filter:1,merge:0,defaultnull:1                       0.00fs  Infinity
filterOpSimplifiableEquiv_10x100000_reference               25.15ns   39.76M
filterOpSimplifiableEquiv_10x100000_compiled     877.17%     2.87ns  348.77M
filterOpSimplifiableEquiv_10x100000_vectorFunc   522.42%     4.81ns  207.72M
filterOpSimplifiableEquiv_10x100000_compileTime    0.40%     6.23us  160.63K
compile filter:1,merge:1,defaultnull:0                       0.00fs  Infinity
filterOpSimplifiableEquiv_10x100000_reference               25.61ns   39.05M
filterOpSimplifiableEquiv_10x100000_compiled    6770.06%   378.26ps    2.64G
filterOpSimplifiableEquiv_10x100000_vectorFunc   612.61%     4.18ns  239.22M
filterOpSimplifiableEquiv_10x100000_compileTime    0.42%     6.03us  165.84K
compile filter:1,merge:1,defaultnull:1                       0.00fs  Infinity
filterOpSimplifiableEquiv_10x100000_reference               24.48ns   40.85M
filterOpSimplifiableEquiv_10x100000_compiled    51113.54%    47.90ps   20.88G
filterOpSimplifiableEquiv_10x100000_vectorFunc  3761.60%   650.83ps    1.54G
filterOpSimplifiableEquiv_10x100000_compileTime    0.40%     6.10us  164.02K

compile filter:0,merge:0,defaultnull:1                       0.00fs  Infinity
filterOpSimplifiableEquiv_100x10000_reference               24.27ns   41.20M
filterOpSimplifiableEquiv_100x10000_compiled     100.67%    24.11ns   41.48M
filterOpSimplifiableEquiv_100x10000_vectorFunc  1099.62%     2.21ns  453.03M
filterOpSimplifiableEquiv_100x10000_compileTime    0.40%     6.06us  165.03K
compile filter:1,merge:0,defaultnull:1                       0.00fs  Infinity
filterOpSimplifiableEquiv_100x10000_reference               24.14ns   41.42M
filterOpSimplifiableEquiv_100x10000_compiled     778.47%     3.10ns  322.42M
filterOpSimplifiableEquiv_100x10000_vectorFunc  1200.47%     2.01ns  497.20M
filterOpSimplifiableEquiv_100x10000_compileTime    0.39%     6.15us  162.66K
compile filter:1,merge:1,defaultnull:0                       0.00fs  Infinity
filterOpSimplifiableEquiv_100x10000_reference               24.88ns   40.20M
filterOpSimplifiableEquiv_100x10000_compiled        inf%     0.00fs  Infinity
filterOpSimplifiableEquiv_100x10000_vectorFunc   706.63%     3.52ns  284.05M
filterOpSimplifiableEquiv_100x10000_compileTime    0.40%     6.25us  160.09K
compile filter:1,merge:1,defaultnull:1                       0.00fs  Infinity
filterOpSimplifiableEquiv_100x10000_reference               24.20ns   41.32M
filterOpSimplifiableEquiv_100x10000_compiled        inf%     0.00fs  Infinity
filterOpSimplifiableEquiv_100x10000_vectorFunc  2694.42%   898.19ps    1.11G
filterOpSimplifiableEquiv_100x10000_compileTime    0.40%     6.06us  164.99K
*/
TEST_F(FilterBenchmark, DISABLED_FilterOpSimplifiableEquiv) {
  auto inputRowType4 =
      ROW({"a", "b", "c", "d"},
          std::vector<TypePtr>{DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE()});

  benchmark.benchmarkExpressionsMult<DoubleType>(
      "filterOpSimplifiableEquiv",
      "a > 1.0 AND b != 0.0",
      {"a+1.0"},
      inputRowType4,
      typicalBatches,
      defaultFilterFlags,
      true);
}

// similar to DISABLED_FilterOpSimplifiable but not everything is filtered out
/*
compile filter:0,merge:0,defaultnull:1                       0.00fs  Infinity
filterOpSimplifiableNotEmpty_10x100000_referenc             33.99ns   29.42M
filterOpSimplifiableNotEmpty_10x100000_compiled  100.04%    33.98ns   29.43M
filterOpSimplifiableNotEmpty_10x100000_vectorFu 1573.62%     2.16ns  462.91M
filterOpSimplifiableNotEmpty_10x100000_compileT    0.55%     6.14us  162.99K
compile filter:1,merge:0,defaultnull:1                       0.00fs  Infinity
filterOpSimplifiableNotEmpty_10x100000_referenc             33.75ns   29.63M
filterOpSimplifiableNotEmpty_10x100000_compiled  628.16%     5.37ns  186.12M
filterOpSimplifiableNotEmpty_10x100000_vectorFu 1556.36%     2.17ns  461.15M
filterOpSimplifiableNotEmpty_10x100000_compileT    0.54%     6.23us  160.61K
compile filter:1,merge:1,defaultnull:0                       0.00fs  Infinity
filterOpSimplifiableNotEmpty_10x100000_referenc             34.27ns   29.18M
filterOpSimplifiableNotEmpty_10x100000_compiled  288.11%    11.89ns   84.08M
filterOpSimplifiableNotEmpty_10x100000_vectorFu  298.69%    11.47ns   87.17M
filterOpSimplifiableNotEmpty_10x100000_compileT    0.56%     6.09us  164.11K
compile filter:1,merge:1,defaultnull:1                       0.00fs  Infinity
filterOpSimplifiableNotEmpty_10x100000_referenc             33.98ns   29.43M
filterOpSimplifiableNotEmpty_10x100000_compiled 1353.86%     2.51ns  398.41M
filterOpSimplifiableNotEmpty_10x100000_vectorFu 1728.96%     1.97ns  508.80M
filterOpSimplifiableNotEmpty_10x100000_compileT    0.57%     5.96us  167.88K

compile filter:0,merge:0,defaultnull:1                       0.00fs  Infinity
filterOpSimplifiableNotEmpty_100x10000_referenc             29.92ns   33.42M
filterOpSimplifiableNotEmpty_100x10000_compiled  103.50%    28.91ns   34.59M
filterOpSimplifiableNotEmpty_100x10000_vectorFu 1511.38%     1.98ns  505.15M
filterOpSimplifiableNotEmpty_100x10000_compileT    0.49%     6.11us  163.70K
compile filter:1,merge:0,defaultnull:1                       0.00fs  Infinity
filterOpSimplifiableNotEmpty_100x10000_referenc             29.22ns   34.23M
filterOpSimplifiableNotEmpty_100x10000_compiled  452.79%     6.45ns  154.97M
filterOpSimplifiableNotEmpty_100x10000_vectorFu 1524.63%     1.92ns  521.82M
filterOpSimplifiableNotEmpty_100x10000_compileT    0.47%     6.21us  161.08K
compile filter:1,merge:1,defaultnull:0                       0.00fs  Infinity
filterOpSimplifiableNotEmpty_100x10000_referenc             29.34ns   34.08M
filterOpSimplifiableNotEmpty_100x10000_compiled  210.71%    13.92ns   71.81M
filterOpSimplifiableNotEmpty_100x10000_vectorFu  251.03%    11.69ns   85.56M
filterOpSimplifiableNotEmpty_100x10000_compileT    0.48%     6.14us  162.76K
compile filter:1,merge:1,defaultnull:1                       0.00fs  Infinity
filterOpSimplifiableNotEmpty_100x10000_referenc             30.20ns   33.12M
filterOpSimplifiableNotEmpty_100x10000_compiled  707.91%     4.27ns  234.44M
filterOpSimplifiableNotEmpty_100x10000_vectorFu 1349.02%     2.24ns  446.76M
filterOpSimplifiableNotEmpty_100x10000_compileT    0.46%     6.53us  153.12K
 */
TEST_F(FilterBenchmark, DISABLED_FilterOpSimplifiableNotEmpty) {
  auto inputRowType4 =
      ROW({"a", "b", "c", "d"},
          std::vector<TypePtr>{DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE()});

  benchmark.benchmarkExpressionsMult<DoubleType>(
      "filterOpSimplifiableNotEmpty",
      "a > 0.99 * b * b * b * (1.0/b) * (1.0/b) * (1.0/b)",
      {"a+1.0"},
      inputRowType4,
      typicalBatches,
      defaultFilterFlags,
      true);
}

// logically equivalent FilterOpSimplifiable
/*
compile filter:0,merge:0,defaultnull:1                       0.00fs  Infinity
filterOpSimplifiableNotEmptyEquiv_10x100000_ref             27.69ns   36.12M
filterOpSimplifiableNotEmptyEquiv_10x100000_com  104.36%    26.53ns   37.69M
filterOpSimplifiableNotEmptyEquiv_10x100000_vec 1275.78%     2.17ns  460.75M
filterOpSimplifiableNotEmptyEquiv_10x100000_com    0.42%     6.67us  149.93K
compile filter:1,merge:0,defaultnull:1                       0.00fs  Infinity
filterOpSimplifiableNotEmptyEquiv_10x100000_ref             27.05ns   36.97M
filterOpSimplifiableNotEmptyEquiv_10x100000_com  640.23%     4.22ns  236.72M
filterOpSimplifiableNotEmptyEquiv_10x100000_vec 1246.42%     2.17ns  460.85M
filterOpSimplifiableNotEmptyEquiv_10x100000_com    0.43%     6.35us  157.49K
compile filter:1,merge:1,defaultnull:0                       0.00fs  Infinity
filterOpSimplifiableNotEmptyEquiv_10x100000_ref             27.42ns   36.47M
filterOpSimplifiableNotEmptyEquiv_10x100000_com  571.57%     4.80ns  208.47M
filterOpSimplifiableNotEmptyEquiv_10x100000_vec  641.54%     4.27ns  233.99M
filterOpSimplifiableNotEmptyEquiv_10x100000_com    0.46%     5.92us  169.06K
compile filter:1,merge:1,defaultnull:1                       0.00fs  Infinity
filterOpSimplifiableNotEmptyEquiv_10x100000_ref             27.35ns   36.56M
filterOpSimplifiableNotEmptyEquiv_10x100000_com 1899.69%     1.44ns  694.48M
filterOpSimplifiableNotEmptyEquiv_10x100000_vec 3077.93%   888.71ps    1.13G
filterOpSimplifiableNotEmptyEquiv_10x100000_com    0.43%     6.38us  156.73K

compile filter:0,merge:0,defaultnull:1                       0.00fs  Infinity
filterOpSimplifiableNotEmptyEquiv_100x10000_ref             28.03ns   35.68M
filterOpSimplifiableNotEmptyEquiv_100x10000_com  105.47%    26.58ns   37.63M
filterOpSimplifiableNotEmptyEquiv_100x10000_vec  552.97%     5.07ns  197.28M
filterOpSimplifiableNotEmptyEquiv_100x10000_com    0.45%     6.18us  161.88K
compile filter:1,merge:0,defaultnull:1                       0.00fs  Infinity
filterOpSimplifiableNotEmptyEquiv_100x10000_ref             26.69ns   37.47M
filterOpSimplifiableNotEmptyEquiv_100x10000_com  511.94%     5.21ns  191.81M
filterOpSimplifiableNotEmptyEquiv_100x10000_vec 1336.96%     2.00ns  500.93M
filterOpSimplifiableNotEmptyEquiv_100x10000_com    0.43%     6.14us  162.98K
compile filter:1,merge:1,defaultnull:0                       0.00fs  Infinity
filterOpSimplifiableNotEmptyEquiv_100x10000_ref             26.49ns   37.76M
filterOpSimplifiableNotEmptyEquiv_100x10000_com  461.76%     5.74ns  174.34M
filterOpSimplifiableNotEmptyEquiv_100x10000_vec  835.89%     3.17ns  315.59M
filterOpSimplifiableNotEmptyEquiv_100x10000_com    0.41%     6.51us  153.62K
compile filter:1,merge:1,defaultnull:1                       0.00fs  Infinity
filterOpSimplifiableNotEmptyEquiv_100x10000_ref             26.54ns   37.68M
filterOpSimplifiableNotEmptyEquiv_100x10000_com  906.64%     2.93ns  341.61M
filterOpSimplifiableNotEmptyEquiv_100x10000_vec 2509.62%     1.06ns  945.60M
filterOpSimplifiableNotEmptyEquiv_100x10000_com    0.44%     6.05us  165.41K
 */
TEST_F(FilterBenchmark, DISABLED_FilterOpSimplifiableNotEmptyEquiv) {
  auto inputRowType4 =
      ROW({"a", "b", "c", "d"},
          std::vector<TypePtr>{DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE()});

  benchmark.benchmarkExpressionsMult<DoubleType>(
      "filterOpSimplifiableNotEmptyEquiv",
      "a > 0.99 AND b != 0.0",
      {"a+1.0"},
      inputRowType4,
      typicalBatches,
      defaultFilterFlags,
      true);
}

/*
compile filter:0,merge:0,defaultnull:1                       0.00fs  Infinity
filterLogical_10x100000_reference                           66.29ns   15.09M
filterLogical_10x100000_compiled                  99.43%    66.67ns   15.00M
filterLogical_10x100000_vectorFunc              3035.95%     2.18ns  458.00M
filterLogical_10x100000_compileTime                0.98%     6.76us  147.95K
compile filter:1,merge:0,defaultnull:1                       0.00fs  Infinity
filterLogical_10x100000_reference                           66.33ns   15.08M
filterLogical_10x100000_compiled                 238.31%    27.83ns   35.93M
filterLogical_10x100000_vectorFunc              1324.54%     5.01ns  199.69M
filterLogical_10x100000_compileTime                0.95%     6.98us  143.20K
compile filter:1,merge:1,defaultnull:0                       0.00fs  Infinity
filterLogical_10x100000_reference                           70.55ns   14.17M
filterLogical_10x100000_compiled                 444.79%    15.86ns   63.05M
filterLogical_10x100000_vectorFunc               548.60%    12.86ns   77.76M
filterLogical_10x100000_compileTime                1.01%     6.99us  143.12K
compile filter:1,merge:1,defaultnull:1                       0.00fs  Infinity
filterLogical_10x100000_reference                           68.07ns   14.69M
filterLogical_10x100000_compiled                 433.72%    15.69ns   63.72M
filterLogical_10x100000_vectorFunc               455.72%    14.94ns   66.95M
filterLogical_10x100000_compileTime                0.95%     7.13us  140.18K

compile filter:0,merge:0,defaultnull:1                       0.00fs  Infinity
filterLogical_100x10000_reference                           60.13ns   16.63M
filterLogical_100x10000_compiled                  99.04%    60.71ns   16.47M
filterLogical_100x10000_vectorFunc              3216.47%     1.87ns  534.95M
filterLogical_100x10000_compileTime                0.84%     7.15us  139.85K
compile filter:1,merge:0,defaultnull:1                       0.00fs  Infinity
filterLogical_100x10000_reference                           58.30ns   17.15M
filterLogical_100x10000_compiled                 216.03%    26.99ns   37.06M
filterLogical_100x10000_vectorFunc              2932.57%     1.99ns  503.03M
filterLogical_100x10000_compileTime                0.84%     6.96us  143.76K
compile filter:1,merge:1,defaultnull:0                       0.00fs  Infinity
filterLogical_100x10000_reference                           58.17ns   17.19M
filterLogical_100x10000_compiled                 360.65%    16.13ns   62.00M
filterLogical_100x10000_vectorFunc               446.32%    13.03ns   76.72M
filterLogical_100x10000_compileTime                0.82%     7.07us  141.43K
compile filter:1,merge:1,defaultnull:1                       0.00fs  Infinity
filterLogical_100x10000_reference                           59.31ns   16.86M
filterLogical_100x10000_compiled                 362.69%    16.35ns   61.15M
filterLogical_100x10000_vectorFunc               458.41%    12.94ns   77.29M
filterLogical_100x10000_compileTime                0.83%     7.14us  140.13K
*/
TEST_F(FilterBenchmark, DISABLED_FilterLogical) {
  auto inputRowType4 =
      ROW({"a", "b", "c", "d"},
          std::vector<TypePtr>{DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE()});

  benchmark.benchmarkExpressionsMult<DoubleType>(
      "filterLogical",
      "((1.01*b*c/d+3.141592653)*(1.0-1.0/a) > 2.71828 AND a > 1.0) OR (1.01*b*c/d+3.141592653)*(1.0-1.0/a) <= 2.71828",
      {"a+1.0"},
      inputRowType4,
      typicalBatches,
      defaultFilterFlags,
      true);
}

// Logically equivalent to FilterLogical: (A and B) OR (NOT A) == (NOT A) OR B
/*
compile filter:0,merge:0,defaultnull:1                       0.00fs  Infinity
filterLogicalEquiv_10x100000_reference                      60.09ns   16.64M
filterLogicalEquiv_10x100000_compiled            100.79%    59.62ns   16.77M
filterLogicalEquiv_10x100000_vectorFunc         2783.08%     2.16ns  463.17M
filterLogicalEquiv_10x100000_compileTime           0.87%     6.94us  144.09K
compile filter:1,merge:0,defaultnull:1                       0.00fs  Infinity
filterLogicalEquiv_10x100000_reference                      58.25ns   17.17M
filterLogicalEquiv_10x100000_compiled            231.64%    25.14ns   39.77M
filterLogicalEquiv_10x100000_vectorFunc         1242.39%     4.69ns  213.30M
filterLogicalEquiv_10x100000_compileTime           0.83%     6.99us  142.99K
compile filter:1,merge:1,defaultnull:0                       0.00fs  Infinity
filterLogicalEquiv_10x100000_reference                      58.99ns   16.95M
filterLogicalEquiv_10x100000_compiled            508.36%    11.60ns   86.18M
filterLogicalEquiv_10x100000_vectorFunc          654.73%     9.01ns  110.99M
filterLogicalEquiv_10x100000_compileTime           0.74%     7.92us  126.22K
compile filter:1,merge:1,defaultnull:1                       0.00fs  Infinity
filterLogicalEquiv_10x100000_reference                      60.64ns   16.49M
filterLogicalEquiv_10x100000_compiled            502.93%    12.06ns   82.93M
filterLogicalEquiv_10x100000_vectorFunc          676.38%     8.97ns  111.54M
filterLogicalEquiv_10x100000_compileTime           0.89%     6.85us  146.09K

compile filter:0,merge:0,defaultnull:1                       0.00fs  Infinity
filterLogicalEquiv_100x10000_reference                      55.05ns   18.17M
filterLogicalEquiv_100x10000_compiled            106.39%    51.74ns   19.33M
filterLogicalEquiv_100x10000_vectorFunc         2868.63%     1.92ns  521.12M
filterLogicalEquiv_100x10000_compileTime           0.80%     6.90us  144.88K
compile filter:1,merge:0,defaultnull:1                       0.00fs  Infinity
filterLogicalEquiv_100x10000_reference                      54.27ns   18.43M
filterLogicalEquiv_100x10000_compiled            228.21%    23.78ns   42.05M
filterLogicalEquiv_100x10000_vectorFunc         3005.04%     1.81ns  553.72M
filterLogicalEquiv_100x10000_compileTime           0.77%     7.02us  142.41K
compile filter:1,merge:1,defaultnull:0                       0.00fs  Infinity
filterLogicalEquiv_100x10000_reference                      53.87ns   18.56M
filterLogicalEquiv_100x10000_compiled            326.25%    16.51ns   60.57M
filterLogicalEquiv_100x10000_vectorFunc          588.15%     9.16ns  109.19M
filterLogicalEquiv_100x10000_compileTime           0.80%     6.76us  147.98K
compile filter:1,merge:1,defaultnull:1                       0.00fs  Infinity
filterLogicalEquiv_100x10000_reference                      52.49ns   19.05M
filterLogicalEquiv_100x10000_compiled            447.01%    11.74ns   85.17M
filterLogicalEquiv_100x10000_vectorFunc          566.99%     9.26ns  108.03M
filterLogicalEquiv_100x10000_compileTime           0.80%     6.53us  153.13K
*/
TEST_F(FilterBenchmark, DISABLED_FilterLogicalEquiv) {
  auto inputRowType4 =
      ROW({"a", "b", "c", "d"},
          std::vector<TypePtr>{DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE()});

  benchmark.benchmarkExpressionsMult<DoubleType>(
      "filterLogicalEquiv",
      "(1.01*b*c/d+3.14159)*(1.0-1.0/a) <= 2.71828 OR a > 1.0",
      {"a+1.0"},
      inputRowType4,
      typicalBatches,
      defaultFilterFlags,
      true);
}

// X > num AND X <= num == FALSE
/*
compile filter:0,merge:0,defaultnull:1                       0.00fs  Infinity
filterLogicalFalse_10x100000_reference                      41.78ns   23.93M
filterLogicalFalse_10x100000_compiled             99.34%    42.06ns   23.77M
filterLogicalFalse_10x100000_vectorFunc          897.41%     4.66ns  214.78M
filterLogicalFalse_10x100000_compileTime           0.58%     7.19us  139.18K
compile filter:1,merge:0,defaultnull:1                       0.00fs  Infinity
filterLogicalFalse_10x100000_reference                      42.46ns   23.55M
filterLogicalFalse_10x100000_compiled           1006.71%     4.22ns  237.10M
filterLogicalFalse_10x100000_vectorFunc          821.95%     5.17ns  193.58M
filterLogicalFalse_10x100000_compileTime           0.58%     7.35us  136.12K
compile filter:1,merge:1,defaultnull:0                       0.00fs  Infinity
filterLogicalFalse_10x100000_reference                      42.84ns   23.34M
filterLogicalFalse_10x100000_compiled           4096.41%     1.05ns  956.14M
filterLogicalFalse_10x100000_vectorFunc          405.04%    10.58ns   94.54M
filterLogicalFalse_10x100000_compileTime           0.62%     6.86us  145.73K
compile filter:1,merge:1,defaultnull:1                       0.00fs  Infinity
filterLogicalFalse_10x100000_reference                      42.63ns   23.46M
filterLogicalFalse_10x100000_compiled           619489.24%     6.88ps  145.32G
filterLogicalFalse_10x100000_vectorFunc         19817.11%   215.12ps    4.65G
filterLogicalFalse_10x100000_compileTime           0.59%     7.19us  139.13K

compile filter:0,merge:0,defaultnull:1                       0.00fs  Infinity
filterLogicalFalse_100x10000_reference                      36.00ns   27.78M
filterLogicalFalse_100x10000_compiled            101.13%    35.60ns   28.09M
filterLogicalFalse_100x10000_vectorFunc          751.11%     4.79ns  208.63M
filterLogicalFalse_100x10000_compileTime           0.53%     6.85us  146.02K
compile filter:1,merge:0,defaultnull:1                       0.00fs  Infinity
filterLogicalFalse_100x10000_reference                      36.24ns   27.59M
filterLogicalFalse_100x10000_compiled            694.53%     5.22ns  191.64M
filterLogicalFalse_100x10000_vectorFunc         1792.43%     2.02ns  494.58M
filterLogicalFalse_100x10000_compileTime           0.51%     7.14us  140.02K
compile filter:1,merge:1,defaultnull:0                       0.00fs  Infinity
filterLogicalFalse_100x10000_reference                      37.42ns   26.72M
filterLogicalFalse_100x10000_compiled           124300.65%    30.10ps   33.22G
filterLogicalFalse_100x10000_vectorFunc          347.88%    10.76ns   92.97M
filterLogicalFalse_100x10000_compileTime           0.51%     7.29us  137.12K
compile filter:1,merge:1,defaultnull:1                       0.00fs  Infinity
filterLogicalFalse_100x10000_reference                      36.21ns   27.61M
filterLogicalFalse_100x10000_compiled               inf%     0.00fs  Infinity
filterLogicalFalse_100x10000_vectorFunc         9395.25%   385.44ps    2.59G
filterLogicalFalse_100x10000_compileTime           0.50%     7.18us  139.33K
*/
TEST_F(FilterBenchmark, DISABLED_FilterLogicalFalse) {
  auto inputRowType4 =
      ROW({"a", "b", "c", "d"},
          std::vector<TypePtr>{DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE()});
  benchmark.benchmarkExpressionsMult<DoubleType>(
      "filterLogicalFalse",
      "(1.01*b*c/d+3.141592653)*(1.0-1.0/a) > 2.71828 AND (1.01*b*c/d+3.141592653)*(1.0-1.0/a) <= 2.71828",
      {"a+1.0"},
      inputRowType4,
      typicalBatches,
      defaultFilterFlags,
      true);
}

// logically equivalent to FilterLogicalFalse
/*
compile filter:0,merge:0,defaultnull:1                       0.00fs  Infinity
filterLogicalFalseEquiv_10x100000_reference                  0.00fs  Infinity
filterLogicalFalseEquiv_10x100000_compiled         -nan%     0.00fs  Infinity
filterLogicalFalseEquiv_10x100000_vectorFunc       0.00%     2.11ns  473.06M
filterLogicalFalseEquiv_10x100000_compileTime      0.00%     5.93us  168.51K
compile filter:1,merge:0,defaultnull:1                       0.00fs  Infinity
filterLogicalFalseEquiv_10x100000_reference                  0.00fs  Infinity
filterLogicalFalseEquiv_10x100000_compiled         -nan%     0.00fs  Infinity
filterLogicalFalseEquiv_10x100000_vectorFunc       0.00%     2.01ns  497.35M
filterLogicalFalseEquiv_10x100000_compileTime      0.00%     5.84us  171.14K
compile filter:1,merge:1,defaultnull:0                       0.00fs  Infinity
filterLogicalFalseEquiv_10x100000_reference                  0.00fs  Infinity
filterLogicalFalseEquiv_10x100000_compiled         -nan%     0.00fs  Infinity
filterLogicalFalseEquiv_10x100000_vectorFunc       -nan%     0.00fs  Infinity
filterLogicalFalseEquiv_10x100000_compileTime      0.00%     5.79us  172.85K
compile filter:1,merge:1,defaultnull:1                       0.00fs  Infinity
filterLogicalFalseEquiv_10x100000_reference                  0.00fs  Infinity
filterLogicalFalseEquiv_10x100000_compiled         -nan%     0.00fs  Infinity
filterLogicalFalseEquiv_10x100000_vectorFunc       -nan%     0.00fs  Infinity
filterLogicalFalseEquiv_10x100000_compileTime      0.00%     7.02us  142.38K

compile filter:0,merge:0,defaultnull:1                       0.00fs  Infinity
filterLogicalFalseEquiv_100x10000_reference                265.63ps    3.76G
filterLogicalFalseEquiv_100x10000_compiled       110.89%   239.54ps    4.17G
filterLogicalFalseEquiv_100x10000_vectorFunc      14.26%     1.86ns  536.70M
filterLogicalFalseEquiv_100x10000_compileTime      0.00%     5.87us  170.26K
compile filter:1,merge:0,defaultnull:1                       0.00fs  Infinity
filterLogicalFalseEquiv_100x10000_reference                224.78ps    4.45G
filterLogicalFalseEquiv_100x10000_compiled        96.29%   233.44ps    4.28G
filterLogicalFalseEquiv_100x10000_vectorFunc      11.90%     1.89ns  529.60M
filterLogicalFalseEquiv_100x10000_compileTime      0.00%     5.84us  171.29K
compile filter:1,merge:1,defaultnull:0                       0.00fs  Infinity
filterLogicalFalseEquiv_100x10000_reference                255.93ps    3.91G
filterLogicalFalseEquiv_100x10000_compiled          inf%     0.00fs  Infinity
filterLogicalFalseEquiv_100x10000_vectorFunc        inf%     0.00fs  Infinity
filterLogicalFalseEquiv_100x10000_compileTime      0.00%     5.83us  171.50K
compile filter:1,merge:1,defaultnull:1                       0.00fs  Infinity
filterLogicalFalseEquiv_100x10000_reference                243.58ps    4.11G
filterLogicalFalseEquiv_100x10000_compiled          inf%     0.00fs  Infinity
filterLogicalFalseEquiv_100x10000_vectorFunc        inf%     0.00fs  Infinity
filterLogicalFalseEquiv_100x10000_compileTime      0.00%     5.91us  169.29K
*/
TEST_F(FilterBenchmark, DISABLED_FilterLogicalFalseEquiv) {
  auto inputRowType4 =
      ROW({"a", "b", "c", "d"},
          std::vector<TypePtr>{DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE()});

  benchmark.benchmarkExpressionsMult<DoubleType>(
      "filterLogicalFalseEquiv",
      "1 > 2",
      {"a+1.0"},
      inputRowType4,
      typicalBatches,
      defaultFilterFlags,
      true);
}

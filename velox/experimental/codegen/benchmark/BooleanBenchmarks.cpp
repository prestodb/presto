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

#include "velox/experimental/codegen/benchmark/CodegenBenchmark.h"
using namespace facebook::velox;
using namespace facebook::velox::codegen;
namespace {
class BooleanBenchmark : public BenchmarkGTest {
 public:
  std::shared_ptr<const RowType> inputRowType =
      ROW({"a", "b", "c", "d", "e"},
          std::vector<TypePtr>{
              BOOLEAN(),
              BOOLEAN(),
              BOOLEAN(),
              DOUBLE(),
              DOUBLE()});
};
} // namespace

// computeDepth5_10x100000_compiled 166.12%    16.59ns   60.27M
// computeDepth5_10x100000_vectorFunc 172.54%    15.97ns   62.60M
// computeDepth5_10x100000_compileTime 0.26%    10.66us   93.85K
// computeDepth5_100x10000_reference                           21.38ns   46.77M
// computeDepth5_100x10000_compiled 190.56%    11.22ns   89.13M
// computeDepth5_100x10000_vectorFunc               238.95%     8.95n    119.04K
//============================================================================
TEST_F(BooleanBenchmark, DISABLED_singleAnd) {
  CodegenBenchmark benchmark;

  benchmark.benchmarkExpressionsMult<BooleanType>(
      "singleAnd", {"a and b"}, inputRowType, typicalBatches, true);

  folly::runBenchmarks();
  benchmark.compareResults();
}

// ifSingleAnd_1000x1000_reference                             25.68ns   38.94M
// ifSingleAnd_1000x1000_compiled                   164.73%    15.59ns   64.15M
// ifSingleAnd_1000x1000_vectorFunc                 754.81%     3.40ns  293.96M
// ifSingleAnd_100x10000_reference                             23.47ns   42.60M
// ifSingleAnd_100x10000_compiled                   220.57%    10.64ns   93.96M
// ifSingleAnd_100x10000_vectorFunc                 317.58%     7.39ns  135.29M
//============================================================================
TEST_F(BooleanBenchmark, DISABLED_ifSingleAnd) {
  std::shared_ptr<const RowType> inputRowType =
      ROW({"a", "b", "d", "e"},
          std::vector<TypePtr>{BOOLEAN(), BOOLEAN(), DOUBLE(), DOUBLE()});

  benchmark.benchmarkExpressionsMult<DoubleType>(
      "ifSingleAnd",
      {"if(a and b, 1.1, 1.2)"},
      inputRowType,
      typicalBatches,
      true);
}

// andOrAnd_100x10000_reference                                51.02ns   19.60M
// andOrAnd_100x10000_compiled                      215.86%    23.64ns   42.31M
// andOrAnd_100x10000_vectorFunc                    246.50%    20.70ns   48.31M
// andOrAnd_1000x1000_reference                                56.85ns   17.59M
// andOrAnd_1000x1000_compiled                      293.00%    19.40ns   51.54M
// andOrAnd_1000x1000_vectorFunc                    756.52%     7.51ns  133.07M
//============================================================================
TEST_F(BooleanBenchmark, DISABLED_andOrAnd) {
  std::shared_ptr<const RowType> inputRowType =
      ROW({"a", "b", "c", "d"},
          std::vector<TypePtr>{BOOLEAN(), BOOLEAN(), BOOLEAN(), BOOLEAN()});

  benchmark.benchmarkExpressionsMult<BooleanType>(
      "andOrAnd", {"a and b or c and d"}, inputRowType, typicalBatches, true);
}

// nestedIfTwoLevel_1000x1000_reference                        46.90ns   21.32M
// nestedIfTwoLevel_1000x1000_compiled              245.76%    19.08ns   52.40M
// nestedIfTwoLevel_1000x1000_vectorFunc            788.00%     5.95ns  168.02M
// nestedIfTwoLevel_100x10000_reference                        44.47ns   22.49M
// nestedIfTwoLevel_100x10000_compiled              173.31%    25.66ns   38.97M
// nestedIfTwoLevel_100x10000_vectorFunc            259.96%    17.11ns   58.46M
//============================================================================
TEST_F(BooleanBenchmark, DISABLED_nestedIfTwoLevel) {
  std::shared_ptr<const RowType> inputRowType =
      ROW({"a", "b", "c", "d"},
          std::vector<TypePtr>{BOOLEAN(), BOOLEAN(), BOOLEAN(), BOOLEAN()});

  benchmark.benchmarkExpressionsMult<BooleanType>(
      "nestedIfTwoLevel",
      {"if(a and b, if(c and d,1,2), if(c or d,3,4))"},
      inputRowType,
      typicalBatches,
      true);
}

// gt_100x10000_reference                        12.35ns   80.99M
// gt_100x10000_compiled              157.45%     7.84ns  127.52M
// gt_100x10000_vectorFunc            222.92%     5.54ns  180.54M
// gt_1000x1000_reference                        19.69ns   50.79M
// gt_1000x1000_compiled              127.68%    15.42ns   64.85M
// gt_1000x1000_vectorFunc            605.84%     3.25ns  307.69M
TEST_F(BooleanBenchmark, DISABLED_gt) {
  std::shared_ptr<const RowType> inputRowType =
      ROW({"a", "b", "c", "d"},
          std::vector<TypePtr>{BIGINT(), BIGINT(), BIGINT(), BIGINT()});

  benchmark.benchmarkExpressionsMult<BooleanType>(
      "gt", {"a>b"}, inputRowType, typicalBatches, true);
}

// gt_and_gt_1000x1000_reference                               43.57ns   22.95M
// gt_and_gt_1000x1000_compiled                     236.43%    18.43ns   54.26M
// gt_and_gt_1000x1000_vectorFunc                   725.26%     6.01ns  166.45M
// gt_and_gt_100x10000_reference                               41.66ns   24.00M
// gt_and_gt_100x10000_compiled                     240.07%    17.35ns   57.62M
// gt_and_gt_100x10000_vectorFunc                   318.82%    13.07ns   76.53M
//============================================================================
TEST_F(BooleanBenchmark, DISABLED_gt_and_gt) {
  std::shared_ptr<const RowType> inputRowType =
      ROW({"a", "b", "c", "d"},
          std::vector<TypePtr>{BIGINT(), BIGINT(), BIGINT(), BIGINT()});

  benchmark.benchmarkExpressionsMult<BooleanType>(
      "gt_and_gt", {"a>b and c >d"}, inputRowType, typicalBatches, true);
}

// Awesome speedup1
// gt_and_gt_100x10000_reference                               92.06ns   10.86M
// gt_and_gt_100x10000_compiled                     462.31%    19.91ns   50.22M
// gt_and_gt_100x10000_vectorFunc                   583.30%    15.78ns   63.36M
// gt_and_gt_1000x1000_reference                              104.90ns    9.53M
// gt_and_gt_1000x1000_compiled                     503.32%    20.84ns   47.98M
// gt_and_gt_1000x1000_vectorFunc                  1424.11%     7.37ns  135.76M
TEST_F(BooleanBenchmark, DISABLED_gt_and_gt_or_gt_gt) {
  std::shared_ptr<const RowType> inputRowType =
      ROW({"a", "b", "c", "d"},
          std::vector<TypePtr>{BIGINT(), BIGINT(), BIGINT(), BIGINT()});

  benchmark.benchmarkExpressionsMult<BooleanType>(
      "gt_and_gt_or_gt_gt",
      {"(a>b and c >d) or (a<b and c <d) "},
      inputRowType,
      typicalBatches,
      true);
}

// Very interesting case where codegen is very worse
// basically when the if condition is a constant bool
// nestedIfThreeLevelCodegenWorse_100x10000_refere              4.83ns  206.99M
// nestedIfThreeLevelCodegenWorse_100x10000_compil   23.51%    20.55ns   48.65M
// nestedIfThreeLevelCodegenWorse_100x10000_vector   28.88%    16.73ns   59.77M
// nestedIfThreeLevelCodegenWorse_100x10000_compil    0.09%     5.32us  188.01K
// nestedIfThreeLevelCodegenWorse_1000x1000_refere             16.90ns   59.17M
// nestedIfThreeLevelCodegenWorse_1000x1000_compil   86.42%    19.56ns   51.14M
// nestedIfThreeLevelCodegenWorse_1000x1000_vector  253.37%     6.67ns  149.91M
TEST_F(BooleanBenchmark, DISABLED_nestedIfThreeLevelCodegenWorse) {
  std::shared_ptr<const RowType> inputRowType =
      ROW({"a", "b", "c", "d"},
          std::vector<TypePtr>{BOOLEAN(), BOOLEAN(), BOOLEAN(), BOOLEAN()});

  benchmark.benchmarkExpressionsMult<BigintType>(
      "nestedIfThreeLevelCodegenWorse",
      {"if(a, if(b, if(c, 1, 2), if(d, 4, 5)), if(b, if(c, 6, 7), if(d, 8,9)))"},
      inputRowType,
      typicalBatches,
      true);
}

// codegenWorseSimple_100x10000_reference                       3.55ns  281.92M
// codegenWorseSimple_100x10000_compiled             54.55%     6.50ns  153.79M
// codegenWorseSimple_100x10000_vectorFunc          238.30%     1.49ns  671.83M
// codegenWorseSimple_1000x1000_reference                      13.23ns   75.60M
// codegenWorseSimple_1000x1000_compiled             90.25%    14.66ns   68.24M
// codegenWorseSimple_1000x1000_vectorFunc           530.57%     2.49ns 401.13M
//============================================================================
TEST_F(BooleanBenchmark, DISABLED_codegenWorseSimple) {
  std::shared_ptr<const RowType> inputRowType =
      ROW({"a", "b", "c", "d"},
          std::vector<TypePtr>{BOOLEAN(), BOOLEAN(), BOOLEAN(), BOOLEAN()});

  benchmark.benchmarkExpressionsMult<BigintType>(
      "codegenWorseSimple",
      {"if(a, 1, 2)"},
      inputRowType,
      typicalBatches,
      true);
}

// Introducing computation in the condition make it fine
//============================================================================
//
// codegenWorseVariant1_100x10000_reference 22.34ns 44.76M
// codegenWorseVariant1_100x10000_compiled 210.03%    10.64ns   94.02M
// codegenWorseVariant1_100x10000_vectorFunc        296.61%     7.53ns   132.77M
// codegenWorseVariant1_1000x1000_reference 26.07ns 38.35M
// codegenWorseVariant1_1000x1000_compiled 170.71%    15.27ns   65.47M
// codegenWorseVariant1_1000x1000_vectorFunc        784.61%     3.32ns   300.91M
TEST_F(BooleanBenchmark, DISABLED_codegenWorseVariant1) {
  std::shared_ptr<const RowType> inputRowType =
      ROW({"a", "b", "c", "d"},
          std::vector<TypePtr>{BOOLEAN(), BOOLEAN(), BOOLEAN(), BOOLEAN()});

  benchmark.benchmarkExpressionsMult<BigintType>(
      "codegenWorseVariant1",
      {"if(a or b, 1, 2)"},
      inputRowType,
      typicalBatches,
      true);
}

// Introducing computation in branches does not make codegen better
//============================================================================
// codegenWorseVariant2_100x10000_reference                     9.00ns   111.11M
// codegenWorseVariant2_100x10000_compiled 61.61 %           m 14.61ns    68.45M
// codegenWorseVariant2_100x10000_vectorFunc         98.30%     9.16ns   109.22M
// codegenWorseVariant2_1000x1000_reference 23.35ns 42.82M
// codegenWorseVariant2_1000x1000_compiled 129.37%    18.05ns   55.40M
// codegenWorseVariant2_1000x1000_vectorFunc        540.04%     4.32ns   231.27M
//============================================================================
TEST_F(BooleanBenchmark, DISABLED_codegenWorseVariant2) {
  std::shared_ptr<const RowType> inputRowType =
      ROW({"a", "c", "d"}, std::vector<TypePtr>{BOOLEAN(), BIGINT(), BIGINT()});

  benchmark.benchmarkExpressionsMult<BigintType>(
      "codegenWorseVariant2",
      {"if(a, c+1, d+2)"},
      inputRowType,
      typicalBatches,
      true);
}

// mix_100x10000_reference              55.62ns 17.98M
// mix_100x10000_compiled    225.47%    24.67ns   40.54M
// mix_100x10000_vectorFunc  263.39%    21.12ns   47.36M
// mix_1000x1000_reference               64.53ns 15.50M
// mix_1000x1000_compiled    215.35%    29.97ns   33.37M
// mix_1000x1000_vectorFunc  364.60%    17.70ns   56.50M
//============================================================================
TEST_F(BooleanBenchmark, DISABLED_mix) {
  std::shared_ptr<const RowType> inputRowType =
      ROW({"a", "b", "c"}, std::vector<TypePtr>{DOUBLE(), DOUBLE(), BOOLEAN()});

  benchmark.benchmarkExpressionsMult<BooleanType>(
      "mix",
      {"c or round(a,cast(1 as int))==round(b, cast (2 as int))"},

      inputRowType,
      typicalBatches,
      true);
}

// TODO we should do better for such deep expression
// mix2_100x10000_reference 84.87ns 11.78M
// mix2_100x10000_compiled   289.58%    29.31ns   34.12M
// mix2_100x10000_vectorFunc 346.25%    24.51ns   40.80M
// mix2_1000x1000_reference             93.99ns 10.64M
// mix2_1000x1000_compiled   316.56%    29.69ns   33.68M
// mix2_1000x1000_vectorFunc 585.03%    16.07ns   62.24M
TEST_F(BooleanBenchmark, DISABLED_mix2) {
  std::shared_ptr<const RowType> inputRowType =
      ROW({"a", "b", "c", "d"},
          std::vector<TypePtr>{DOUBLE(), DOUBLE(), BOOLEAN(), BOOLEAN()});

  benchmark.benchmarkExpressionsMult<BooleanType>(
      "mix2",
      {"(d and c) or round(a, cast (5 as int))==round(b,cast (4 as int))"},
      inputRowType,
      typicalBatches,
      true);
}

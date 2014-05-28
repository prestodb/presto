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
package com.facebook.presto.benchmark;

import com.facebook.presto.testing.LocalQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import io.airlift.log.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.benchmark.BenchmarkQueryRunner.createLocalQueryRunner;
import static com.facebook.presto.benchmark.BenchmarkQueryRunner.createLocalSampledQueryRunner;
import static com.google.common.base.Preconditions.checkNotNull;

public class BenchmarkSuite
{
    private static final Logger LOGGER = Logger.get(BenchmarkSuite.class);

    public static List<AbstractBenchmark> createBenchmarks(LocalQueryRunner localQueryRunner, LocalQueryRunner localSampledQueryRunner)
    {
        return ImmutableList.<AbstractBenchmark>of(
                // hand built benchmarks
                new CountAggregationBenchmark(localQueryRunner),
                new DoubleSumAggregationBenchmark(localQueryRunner),
                new HashAggregationBenchmark(localQueryRunner),
                new PredicateFilterBenchmark(localQueryRunner),
                new RawStreamingBenchmark(localQueryRunner),
                new Top100Benchmark(localQueryRunner),
                new OrderByBenchmark(localQueryRunner),
                new HashBuildBenchmark(localQueryRunner),
                new HashJoinBenchmark(localQueryRunner),
                new HashBuildAndJoinBenchmark(localQueryRunner),
                new HandTpchQuery1(localQueryRunner),
                new HandTpchQuery6(localQueryRunner),

                // sql benchmarks
                new GroupBySumWithArithmeticSqlBenchmark(localQueryRunner),
                new CountAggregationSqlBenchmark(localQueryRunner),
                new SqlDoubleSumAggregationBenchmark(localQueryRunner),
                new CountWithFilterSqlBenchmark(localQueryRunner),
                new GroupByAggregationSqlBenchmark(localQueryRunner),
                new PredicateFilterSqlBenchmark(localQueryRunner),
                new RawStreamingSqlBenchmark(localQueryRunner),
                new Top100SqlBenchmark(localQueryRunner),
                new SqlHashJoinBenchmark(localQueryRunner),
                new SqlJoinWithPredicateBenchmark(localQueryRunner),
                new VarBinaryMaxAggregationSqlBenchmark(localQueryRunner),
                new SqlDistinctMultipleFields(localQueryRunner),
                new SqlDistinctSingleField(localQueryRunner),
                new SqlTpchQuery1(localQueryRunner),
                new SqlTpchQuery6(localQueryRunner),
                new SqlLikeBenchmark(localQueryRunner),
                new SqlInBenchmark(localQueryRunner),
                new SqlSemiJoinInPredicateBenchmark(localQueryRunner),
                new SqlRegexpLikeBenchmark(localQueryRunner),
                new SqlApproximatePercentileBenchmark(localQueryRunner),
                new SqlBetweenBenchmark(localQueryRunner),

                // Sampled sql benchmarks
                new RenamingBenchmark("sampled_", new GroupBySumWithArithmeticSqlBenchmark(localSampledQueryRunner)),
                new RenamingBenchmark("sampled_", new CountAggregationSqlBenchmark(localSampledQueryRunner)),
                new RenamingBenchmark("sampled_", new SqlJoinWithPredicateBenchmark(localSampledQueryRunner)),
                new RenamingBenchmark("sampled_", new SqlDoubleSumAggregationBenchmark(localSampledQueryRunner)),

                // statistics benchmarks
                new StatisticsBenchmark.LongVarianceBenchmark(localQueryRunner),
                new StatisticsBenchmark.LongVariancePopBenchmark(localQueryRunner),
                new StatisticsBenchmark.DoubleVarianceBenchmark(localQueryRunner),
                new StatisticsBenchmark.DoubleVariancePopBenchmark(localQueryRunner),
                new StatisticsBenchmark.LongStdDevBenchmark(localQueryRunner),
                new StatisticsBenchmark.LongStdDevPopBenchmark(localQueryRunner),
                new StatisticsBenchmark.DoubleStdDevBenchmark(localQueryRunner),
                new StatisticsBenchmark.DoubleStdDevPopBenchmark(localQueryRunner),

                new SqlApproximateCountDistinctLongBenchmark(localQueryRunner),
                new SqlApproximateCountDistinctDoubleBenchmark(localQueryRunner),
                new SqlApproximateCountDistinctVarBinaryBenchmark(localQueryRunner)
        );
    }

    private final String outputDirectory;

    public BenchmarkSuite(String outputDirectory)
    {
        this.outputDirectory = checkNotNull(outputDirectory, "outputDirectory is null");
    }

    private File createOutputFile(String fileName)
            throws IOException
    {
        File outputFile = new File(fileName);
        Files.createParentDirs(outputFile);
        return outputFile;
    }

    public void runAllBenchmarks()
            throws IOException
    {
        try (LocalQueryRunner localQueryRunner = createLocalQueryRunner();
                LocalQueryRunner localSampledQueryRunner = createLocalSampledQueryRunner()) {
            List<AbstractBenchmark> benchmarks = createBenchmarks(localQueryRunner, localSampledQueryRunner);

            LOGGER.info("=== Pre-running all benchmarks for JVM warmup ===");
            for (AbstractBenchmark benchmark : benchmarks) {
                benchmark.runBenchmark();
            }

            LOGGER.info("=== Actually running benchmarks for metrics ===");
            for (AbstractBenchmark benchmark : benchmarks) {
                try (OutputStream jsonOut = new FileOutputStream(createOutputFile(String.format("%s/json/%s.json", outputDirectory, benchmark.getBenchmarkName())));
                        OutputStream jsonAvgOut = new FileOutputStream(createOutputFile(String.format("%s/json-avg/%s.json", outputDirectory, benchmark.getBenchmarkName())));
                        OutputStream csvOut = new FileOutputStream(createOutputFile(String.format("%s/csv/%s.csv", outputDirectory, benchmark.getBenchmarkName())));
                        OutputStream odsOut = new FileOutputStream(createOutputFile(String.format("%s/ods/%s.json", outputDirectory, benchmark.getBenchmarkName())))) {
                    benchmark.runBenchmark(
                            new ForwardingBenchmarkResultWriter(
                                    ImmutableList.of(
                                            new JsonBenchmarkResultWriter(jsonOut),
                                            new JsonAvgBenchmarkResultWriter(jsonAvgOut),
                                            new SimpleLineBenchmarkResultWriter(csvOut),
                                            new OdsBenchmarkResultWriter("presto.benchmark." + benchmark.getBenchmarkName(), odsOut)
                                    )
                            )
                    );
                }
            }
        }
    }

    private static class ForwardingBenchmarkResultWriter
            implements BenchmarkResultHook
    {
        private final List<BenchmarkResultHook> benchmarkResultHooks;

        private ForwardingBenchmarkResultWriter(List<BenchmarkResultHook> benchmarkResultHooks)
        {
            checkNotNull(benchmarkResultHooks, "benchmarkResultWriters is null");
            this.benchmarkResultHooks = ImmutableList.copyOf(benchmarkResultHooks);
        }

        @Override
        public BenchmarkResultHook addResults(Map<String, Long> results)
        {
            checkNotNull(results, "results is null");
            for (BenchmarkResultHook benchmarkResultHook : benchmarkResultHooks) {
                benchmarkResultHook.addResults(results);
            }
            return this;
        }

        @Override
        public void finished()
        {
            for (BenchmarkResultHook benchmarkResultHook : benchmarkResultHooks) {
                benchmarkResultHook.finished();
            }
        }
    }

    public static void main(String[] args)
            throws IOException
    {
        String outputDirectory = checkNotNull(System.getProperty("outputDirectory"), "Must specify -DoutputDirectory=...");
        new BenchmarkSuite(outputDirectory).runAllBenchmarks();
    }
}

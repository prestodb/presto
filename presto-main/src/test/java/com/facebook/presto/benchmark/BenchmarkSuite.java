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

import com.facebook.presto.tpch.TpchBlocksProvider;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import io.airlift.log.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.util.Threads.daemonThreadsNamed;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class BenchmarkSuite
{
    private static final Logger LOGGER = Logger.get(BenchmarkSuite.class);

    public static List<AbstractBenchmark> createBenchmarks(ExecutorService executor, TpchBlocksProvider tpchBlocksProvider)
    {
        return ImmutableList.<AbstractBenchmark>of(
                // hand built benchmarks
                new CountAggregationBenchmark(executor, tpchBlocksProvider),
                new DoubleSumAggregationBenchmark(executor, tpchBlocksProvider),
                new HashAggregationBenchmark(executor, tpchBlocksProvider),
                new PredicateFilterBenchmark(executor, tpchBlocksProvider),
                new RawStreamingBenchmark(executor, tpchBlocksProvider),
                new Top100Benchmark(executor, tpchBlocksProvider),
                new OrderByBenchmark(executor, tpchBlocksProvider),
                new HashBuildBenchmark(executor, tpchBlocksProvider),
                new HashJoinBenchmark(executor, tpchBlocksProvider),
                new HashBuildAndJoinBenchmark(executor, tpchBlocksProvider),
                new HandTpchQuery1(executor, tpchBlocksProvider),
                new HandTpchQuery6(executor, tpchBlocksProvider),

                // sql benchmarks
                new GroupBySumWithArithmeticSqlBenchmark(executor, tpchBlocksProvider),
                new CountAggregationSqlBenchmark(executor, tpchBlocksProvider),
                new SqlDoubleSumAggregationBenchmark(executor, tpchBlocksProvider),
                new CountWithFilterSqlBenchmark(executor, tpchBlocksProvider),
                new GroupByAggregationSqlBenchmark(executor, tpchBlocksProvider),
                new PredicateFilterSqlBenchmark(executor, tpchBlocksProvider),
                new RawStreamingSqlBenchmark(executor, tpchBlocksProvider),
                new Top100SqlBenchmark(executor, tpchBlocksProvider),
                new SqlHashJoinBenchmark(executor, tpchBlocksProvider),
                new SqlJoinWithPredicateBenchmark(executor, tpchBlocksProvider),
                new VarBinaryMaxAggregationSqlBenchmark(executor, tpchBlocksProvider),
                new SqlDistinctMultipleFields(executor, tpchBlocksProvider),
                new SqlDistinctSingleField(executor, tpchBlocksProvider),
                new SqlTpchQuery1(executor, tpchBlocksProvider),
                new SqlTpchQuery6(executor, tpchBlocksProvider),
                new SqlLikeBenchmark(executor, tpchBlocksProvider),
                new SqlInBenchmark(executor, tpchBlocksProvider),
                new SqlSemiJoinInPredicateBenchmark(executor, tpchBlocksProvider),
                new SqlRegexpLikeBenchmark(executor, tpchBlocksProvider),
                new SqlApproximatePercentileBenchmark(executor, tpchBlocksProvider),

                // statistics benchmarks
                new StatisticsBenchmark.LongVarianceBenchmark(executor, tpchBlocksProvider),
                new StatisticsBenchmark.LongVariancePopBenchmark(executor, tpchBlocksProvider),
                new StatisticsBenchmark.DoubleVarianceBenchmark(executor, tpchBlocksProvider),
                new StatisticsBenchmark.DoubleVariancePopBenchmark(executor, tpchBlocksProvider),
                new StatisticsBenchmark.LongStdDevBenchmark(executor, tpchBlocksProvider),
                new StatisticsBenchmark.LongStdDevPopBenchmark(executor, tpchBlocksProvider),
                new StatisticsBenchmark.DoubleStdDevBenchmark(executor, tpchBlocksProvider),
                new StatisticsBenchmark.DoubleStdDevPopBenchmark(executor, tpchBlocksProvider),

                new SqlApproximateCountDistinctLongBenchmark(executor, tpchBlocksProvider),
                new SqlApproximateCountDistinctDoubleBenchmark(executor, tpchBlocksProvider),
                new SqlApproximateCountDistinctVarBinaryBenchmark(executor, tpchBlocksProvider)
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
        ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("test"));
        try {
            List<AbstractBenchmark> benchmarks = createBenchmarks(executor, AbstractOperatorBenchmark.DEFAULT_TPCH_BLOCKS_PROVIDER);

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
        finally {
            executor.shutdownNow();
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

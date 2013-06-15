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

import static com.google.common.base.Preconditions.checkNotNull;

public class BenchmarkSuite
{
    private static final Logger LOGGER = Logger.get(BenchmarkSuite.class);

    public static List<AbstractBenchmark> createBenchmarks(TpchBlocksProvider tpchBlocksProvider)
    {
        return ImmutableList.<AbstractBenchmark>of(
                new CountAggregationBenchmark(tpchBlocksProvider),
                new DoubleSumAggregationBenchmark(tpchBlocksProvider),
                new HashAggregationBenchmark(tpchBlocksProvider),
                new PredicateFilterBenchmark(tpchBlocksProvider),
                new RawStreamingBenchmark(tpchBlocksProvider),
                new Top100Benchmark(tpchBlocksProvider),

                // benchmarks for new non-integrated features
                new InMemoryOrderByBenchmark(tpchBlocksProvider),
                new HashBuildBenchmark(tpchBlocksProvider),
                new HashJoinBenchmark(tpchBlocksProvider),
                new HashBuildAndJoinBenchmark(tpchBlocksProvider),

                // sql benchmarks
                new GroupBySumWithArithmeticSqlBenchmark(tpchBlocksProvider),
                new CountAggregationSqlBenchmark(tpchBlocksProvider),
                new SqlDoubleSumAggregationBenchmark(tpchBlocksProvider),
                new CountWithFilterSqlBenchmark(tpchBlocksProvider),
                new GroupByAggregationSqlBenchmark(tpchBlocksProvider),
                new PredicateFilterSqlBenchmark(tpchBlocksProvider),
                new RawStreamingSqlBenchmark(tpchBlocksProvider),
                new Top100SqlBenchmark(tpchBlocksProvider),
                new SqlHashJoinBenchmark(tpchBlocksProvider),
                new VarBinaryMaxAggregationSqlBenchmark(tpchBlocksProvider),
                new SqlDistinctMultipleFields(tpchBlocksProvider),
                new SqlDistinctSingleField(tpchBlocksProvider),
                new SqlTpchQuery1(tpchBlocksProvider),
                new SqlTpchQuery6(tpchBlocksProvider),
                new HandTpchQuery6(tpchBlocksProvider),
                new SqlLikeBenchmark(tpchBlocksProvider),
                new SqlInBenchmark(tpchBlocksProvider),
                new SqlRegexpLikeBenchmark(tpchBlocksProvider),
                new SqlApproximatePercentileBenchmark(tpchBlocksProvider),

                // statistics benchmarks
                new StatisticsBenchmark.LongVarianceBenchmark(tpchBlocksProvider),
                new StatisticsBenchmark.LongVariancePopBenchmark(tpchBlocksProvider),
                new StatisticsBenchmark.DoubleVarianceBenchmark(tpchBlocksProvider),
                new StatisticsBenchmark.DoubleVariancePopBenchmark(tpchBlocksProvider),
                new StatisticsBenchmark.LongStdDevBenchmark(tpchBlocksProvider),
                new StatisticsBenchmark.LongStdDevPopBenchmark(tpchBlocksProvider),
                new StatisticsBenchmark.DoubleStdDevBenchmark(tpchBlocksProvider),
                new StatisticsBenchmark.DoubleStdDevPopBenchmark(tpchBlocksProvider),

                new SqlApproximateCountDistinctLongBenchmark(tpchBlocksProvider),
                new SqlApproximateCountDistinctDoubleBenchmark(tpchBlocksProvider),
                new SqlApproximateCountDistinctVarBinaryBenchmark(tpchBlocksProvider)
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
        List<AbstractBenchmark> benchmarks = createBenchmarks(AbstractOperatorBenchmark.DEFAULT_TPCH_BLOCKS_PROVIDER);

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

package com.facebook.presto.benchmark;

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

    public static final List<AbstractBenchmark> BENCHMARKS = ImmutableList.<AbstractBenchmark>of(
            new CountAggregationBenchmark(),
            new DoubleSumAggregationBenchmark(),
            new HashAggregationBenchmark(),
            new PredicateFilterBenchmark(),
            new RawStreamingBenchmark(),
            new Top100Benchmark(),

            // benchmarks for new non-integrated features
            new InMemoryOrderByBenchmark(),
            new HashBuildBenchmark(),
            new HashJoinBenchmark(),
            new HashBuildAndJoinBenchmark(),

            // sql benchmarks
            new GroupBySumWithArithmeticSqlBenchmark(),
            new CountAggregationSqlBenchmark(),
            new SqlDoubleSumAggregationBenchmark(),
            new CountWithFilterSqlBenchmark(),
            new GroupByAggregationSqlBenchmark(),
            new PredicateFilterSqlBenchmark(),
            new RawStreamingSqlBenchmark(),
            new Top100SqlBenchmark(),
            new SqlHashJoinBenchmark(),
            new VarBinaryMaxAggregationSqlBenchmark(),
            new SqlDistinctMultipleFields(),
            new SqlDistinctSingleField(),
            new SqlTpchQuery1(),

            // statistics benchmarks
            new StatisticsBenchmark.LongVarianceBenchmark(),
            new StatisticsBenchmark.LongVariancePopBenchmark(),
            new StatisticsBenchmark.DoubleVarianceBenchmark(),
            new StatisticsBenchmark.DoubleVariancePopBenchmark(),
            new StatisticsBenchmark.LongStdDevBenchmark(),
            new StatisticsBenchmark.LongStdDevPopBenchmark(),
            new StatisticsBenchmark.DoubleStdDevBenchmark(),
            new StatisticsBenchmark.DoubleStdDevPopBenchmark(),

            new SqlApproximateCountDistinctLongBenchmark(),
            new SqlApproximateCountDistinctDoubleBenchmark(),
            new SqlApproximateCountDistinctVarBinaryBenchmark()
    );

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
        LOGGER.info("=== Pre-running all benchmarks for JVM warmup ===");
        for (AbstractBenchmark benchmark : BENCHMARKS) {
            benchmark.runBenchmark();
        }

        LOGGER.info("=== Actually running benchmarks for metrics ===");
        for (AbstractBenchmark benchmark : BENCHMARKS) {
            try (OutputStream jmeterOut = new FileOutputStream(createOutputFile(String.format("%s/jmeter/%s.jtl", outputDirectory, benchmark.getBenchmarkName())));
                 OutputStream jsonOut = new FileOutputStream(createOutputFile(String.format("%s/json/%s.json", outputDirectory, benchmark.getBenchmarkName())));
                 OutputStream jsonAvgOut = new FileOutputStream(createOutputFile(String.format("%s/json-avg/%s.json", outputDirectory, benchmark.getBenchmarkName())));
                 OutputStream csvOut = new FileOutputStream(createOutputFile(String.format("%s/csv/%s.csv", outputDirectory, benchmark.getBenchmarkName())));
                 OutputStream odsOut = new FileOutputStream(createOutputFile(String.format("%s/ods/%s.json", outputDirectory, benchmark.getBenchmarkName())))) {
                benchmark.runBenchmark(
                        new ForwardingBenchmarkResultWriter(
                                ImmutableList.of(
                                        new JMeterBenchmarkResultWriter(benchmark.getDefaultResult(), jmeterOut),
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

package com.facebook.presto.benchmark;

import com.facebook.presto.tpch.TpchDataProvider;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

public class BenchmarkSuite
{
    private static final TpchDataProvider TPCH_DATA_PROVIDER = new TpchDataProvider();
    public static final List<AbstractBenchmark> BENCHMARKS = ImmutableList.<AbstractBenchmark>of(
            new SampleTcphBenchmark(TPCH_DATA_PROVIDER)
    );

    private final String outputDirectory;

    public BenchmarkSuite(String outputDirectory)
    {
        this.outputDirectory = Preconditions.checkNotNull(outputDirectory, "outputDirectory is null");
    }

    private File createOutputFile(String fileName) throws IOException
    {
        File outputFile = new File(fileName);
        Files.createParentDirs(outputFile);
        return outputFile;
    }

    public void runAllBenchmarks()
            throws IOException
    {
        for (AbstractBenchmark benchmark : BENCHMARKS) {
            try (OutputStream jmeterOut = new FileOutputStream(createOutputFile(String.format("%s/jmeter/%s.jtl", outputDirectory, benchmark.getBenchmarkName())));
                 OutputStream jsonOut = new FileOutputStream(createOutputFile(String.format("%s/json/%s.json", outputDirectory, benchmark.getBenchmarkName())));
                 OutputStream csvOut = new FileOutputStream(createOutputFile(String.format("%s/csv/%s.csv", outputDirectory, benchmark.getBenchmarkName())))) {
                benchmark.runBenchmark(
                        new ForwardingBenchmarkResultWriter(
                                ImmutableList.of(
                                        new JMeterBenchmarkResultWriter(benchmark.getDefaultResult(), jmeterOut),
                                        new JsonBenchmarkResultWriter(jsonOut),
                                        new SimpleLineBenchmarkResultWriter(csvOut)
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
            Preconditions.checkNotNull(benchmarkResultHooks, "benchmarkResultWriters is null");
            this.benchmarkResultHooks = ImmutableList.copyOf(benchmarkResultHooks);
        }

        @Override
        public BenchmarkResultHook addResults(Map<String, Long> results)
        {
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

    public static void main(String[] args) throws IOException
    {
        String outputDirectory = Preconditions.checkNotNull(System.getProperty("outputDirectory"), "Must specify -DoutputDirectory=...");
        new BenchmarkSuite(outputDirectory).runAllBenchmarks();
    }
}

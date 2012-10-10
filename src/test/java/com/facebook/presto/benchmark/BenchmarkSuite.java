package com.facebook.presto.benchmark;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class BenchmarkSuite
{
    public static final List<AbstractBenchmark> BENCHMARKS = ImmutableList.<AbstractBenchmark>of(
            new BinaryOperatorBenchmark(),
            new CountAggregationBenchmark(),
            new DicRleGroupByBenchmark(),
            new DictionaryAggregationBenchmark(),
            new PredicateFilterBenchmark(),
            new RawStreamingBenchmark(),
            new RleHashAggregationBenchmark(),
            new RlePipelinedAggregationBenchmark()
    );

    private final String outputDirectory;

    public BenchmarkSuite(String outputDirectory)
    {
        this.outputDirectory = checkNotNull(outputDirectory, "outputDirectory is null");
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

    public static void main(String[] args) throws IOException
    {
        String outputDirectory = checkNotNull(System.getProperty("outputDirectory"), "Must specify -DoutputDirectory=...");
        new BenchmarkSuite(outputDirectory).runAllBenchmarks();
    }
}

package com.facebook.presto.benchmark;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

public class BenchmarkSuite
{
    public static final List<AbstractBenchmark> BENCHMARKS = ImmutableList.<AbstractBenchmark>of(
            // TODO: drop benchmarks into this list
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
            OutputStream jmeterOut = new FileOutputStream(createOutputFile(String.format("%s/jmeter/%s.jtl", outputDirectory, benchmark.getBenchmarkResultName())));
            OutputStream csvOut = new FileOutputStream(createOutputFile(String.format("%s/csv/%s.csv", outputDirectory, benchmark.getBenchmarkResultName())));
            benchmark.runBenchmark(
                    new ForwardingBenchmarkResultWriter(
                            ImmutableList.of(
                                    new JMeterBenchmarkResultWriter(jmeterOut),
                                    new SimpleLineBenchmarkResultWriter(csvOut)
                            )
                    )
            );
            csvOut.close();
            jmeterOut.close();
        }
    }

    private static class ForwardingBenchmarkResultWriter
            implements BenchmarkResultHook
    {
        List<BenchmarkResultHook> benchmarkResultHooks;

        private ForwardingBenchmarkResultWriter(List<BenchmarkResultHook> benchmarkResultHooks)
        {
            Preconditions.checkNotNull(benchmarkResultHooks, "benchmarkResultWriters is null");
            this.benchmarkResultHooks = ImmutableList.copyOf(benchmarkResultHooks);
        }

        @Override
        public BenchmarkResultHook addResult(long result)
        {
            for (BenchmarkResultHook benchmarkResultHook : benchmarkResultHooks) {
                benchmarkResultHook.addResult(result);
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

package com.facebook.presto.benchmark;

import io.airlift.units.Duration;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public abstract class AbstractBenchmark
{
    private final String benchmarkResultName; // Should include units of result in the name
    private final int warmupIterations;
    private final int measuredIterations;

    protected AbstractBenchmark(String benchmarkResultName, int warmupIterations, int measuredIterations)
    {
        checkNotNull(benchmarkResultName, "benchmarkResultName is null");
        checkArgument(warmupIterations >= 0, "warmupIterations should not be negative");
        checkArgument(measuredIterations >= 0, "measuredIterations should not be negative");

        this.benchmarkResultName = benchmarkResultName;
        this.warmupIterations = warmupIterations;
        this.measuredIterations = measuredIterations;
    }

    public String getBenchmarkResultName()
    {
        return benchmarkResultName;
    }

    /**
     * Initialize any state necessary to run benchmark. This is run once at start up.
     */
    protected void setUp()
    {
        // Default: no-op
    }

    /**
     * Runs the benchmark and returns the performance metric result
     */
    protected abstract long runOnce();

    /**
     * Clean up any state from the benchmark. This is run once after all the iterations are complete.
     */
    protected void tearDown()
    {
        // Default: no-op
    }

    public void runBenchmark()
    {
        runBenchmark(null);
    }

    public void runBenchmark(@Nullable BenchmarkResultHook benchmarkResultHook)
    {
        System.out.println(String.format("Running benchmark: %s", getBenchmarkResultName()));
        long start = System.nanoTime();
        setUp();
        for (int i = 0; i < warmupIterations; i++) {
            runOnce();
        }
        for (int i = 0; i < measuredIterations; i++) {
            long result = runOnce();
            System.out.println(String.format("%s: %d", getBenchmarkResultName(), result));
            if (benchmarkResultHook != null) {
                benchmarkResultHook.addResult(result);
            }
        }
        tearDown();
        if (benchmarkResultHook != null) {
            benchmarkResultHook.finished();
        }
        Duration duration = Duration.nanosSince(start);
        System.out.println(String.format("Finished benchmark: %s, Elapsed: %s", getBenchmarkResultName(), duration));
    }
}

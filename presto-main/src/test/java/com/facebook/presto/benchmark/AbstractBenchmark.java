package com.facebook.presto.benchmark;

import io.airlift.log.Logger;
import io.airlift.units.Duration;

import javax.annotation.Nullable;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public abstract class AbstractBenchmark
{
    private static final Logger LOGGER = Logger.get(AbstractBenchmark.class);

    private final String benchmarkName;
    private final int warmupIterations;
    private final int measuredIterations;

    protected AbstractBenchmark(String benchmarkName, int warmupIterations, int measuredIterations)
    {
        checkNotNull(benchmarkName, "benchmarkName is null");
        checkArgument(warmupIterations >= 0, "warmupIterations must not be negative");
        checkArgument(measuredIterations >= 0, "measuredIterations must not be negative");

        this.benchmarkName = benchmarkName;
        this.warmupIterations = warmupIterations;
        this.measuredIterations = measuredIterations;
    }

    public String getBenchmarkName()
    {
        return benchmarkName;
    }

    /**
     * Some monitoring tools only accept one result. Return the name of the result that
     * should be tracked.
     */
    protected abstract String getDefaultResult();

    /**
     * Initialize any state necessary to run benchmark. This is run once at start up.
     */
    protected void setUp()
    {
        // Default: no-op
    }

    /**
     * Runs the benchmark and returns the result metrics
     */
    protected abstract Map<String, Long> runOnce();

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
        LOGGER.info("Running benchmark: %s", getBenchmarkName());
        long start = System.nanoTime();
        setUp();
        try {
            for (int i = 0; i < warmupIterations; i++) {
                runOnce();
            }
            for (int i = 0; i < measuredIterations; i++) {
                Map<String, Long> results = runOnce();
                if (benchmarkResultHook != null) {
                    benchmarkResultHook.addResults(results);
                }
            }
        } finally {
            tearDown();
        }
        if (benchmarkResultHook != null) {
            benchmarkResultHook.finished();
        }
        Duration duration = Duration.nanosSince(start);
        LOGGER.info("Finished benchmark: %s, Elapsed: %s", getBenchmarkName(), duration);
    }
}

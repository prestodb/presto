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
package io.prestosql.benchmark;

import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.annotation.Nullable;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.prestosql.benchmark.FormatUtils.formatCount;
import static io.prestosql.benchmark.FormatUtils.formatCountRate;
import static io.prestosql.benchmark.FormatUtils.formatDataRate;
import static io.prestosql.benchmark.FormatUtils.formatDataSize;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public abstract class AbstractBenchmark
{
    private final String benchmarkName;
    private final int warmupIterations;
    private final int measuredIterations;

    protected AbstractBenchmark(String benchmarkName, int warmupIterations, int measuredIterations)
    {
        requireNonNull(benchmarkName, "benchmarkName is null");
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

    protected int getWarmupIterations()
    {
        return warmupIterations;
    }

    protected int getMeasuredIterations()
    {
        return measuredIterations;
    }

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
        AverageBenchmarkResults averageBenchmarkResults = new AverageBenchmarkResults();
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
                averageBenchmarkResults.addResults(results);
            }
        }
        catch (Throwable t) {
            throw new RuntimeException("Exception in " + getBenchmarkName(), t);
        }
        finally {
            tearDown();
        }
        if (benchmarkResultHook != null) {
            benchmarkResultHook.finished();
        }

        Map<String, Double> resultsAvg = averageBenchmarkResults.getAverageResultsValues();
        Duration cpuNanos = new Duration(resultsAvg.get("cpu_nanos"), NANOSECONDS);

        long inputRows = resultsAvg.get("input_rows").longValue();
        DataSize inputBytes = new DataSize(resultsAvg.get("input_bytes"), BYTE);

        long outputRows = resultsAvg.get("output_rows").longValue();
        DataSize outputBytes = new DataSize(resultsAvg.get("output_bytes"), BYTE);

        DataSize memory = new DataSize(resultsAvg.get("peak_memory"), BYTE);
        System.out.printf("%35s :: %8.3f cpu ms :: %5s peak memory :: in %5s,  %6s,  %8s,  %8s :: out %5s,  %6s,  %8s,  %8s%n",
                getBenchmarkName(),
                cpuNanos.getValue(MILLISECONDS),

                formatDataSize(memory, true),

                formatCount(inputRows),
                formatDataSize(inputBytes, true),
                formatCountRate(inputRows, cpuNanos, true),
                formatDataRate(inputBytes, cpuNanos, true),

                formatCount(outputRows),
                formatDataSize(outputBytes, true),
                formatCountRate(outputRows, cpuNanos, true),
                formatDataRate(outputBytes, cpuNanos, true));
    }
}

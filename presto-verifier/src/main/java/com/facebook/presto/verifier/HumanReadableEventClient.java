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
package com.facebook.presto.verifier;

import com.facebook.presto.util.Types;
import io.airlift.event.client.AbstractEventClient;
import io.airlift.stats.QuantileDigest;
import io.airlift.units.Duration;

import javax.inject.Inject;

import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;
import java.util.regex.Pattern;

import static java.lang.Double.POSITIVE_INFINITY;
import static java.lang.Double.isNaN;
import static java.lang.String.format;
import static java.lang.System.out;
import static java.util.concurrent.TimeUnit.SECONDS;

public class HumanReadableEventClient
        extends AbstractEventClient
        implements Closeable
{
    private static final double LARGE_SPEEDUP = 0.5;
    private static final double SMALL_SPEEDUP = 1.0;
    private static final double SMALL_REGRESSION = 2.0;

    private final boolean alwaysPrint;
    private final QuantileDigest cpuRatioAll = new QuantileDigest(0.01);
    private final QuantileDigest cpuRatioLargeSpeedup = new QuantileDigest(0.01);
    private final QuantileDigest cpuRatioSmallSpeedup = new QuantileDigest(0.01);
    private final QuantileDigest cpuRatioSmallRegression = new QuantileDigest(0.01);
    private final QuantileDigest cpuRatioLargeRegression = new QuantileDigest(0.01);
    private final Duration regressionMinCpuTime;
    private final boolean checkCpu;
    private final String skipCpuCheckRegex;

    @Inject
    public HumanReadableEventClient(VerifierConfig config)
    {
        this.alwaysPrint = config.isAlwaysReport();
        regressionMinCpuTime = config.getRegressionMinCpuTime();
        checkCpu = config.isCheckCpuEnabled();
        skipCpuCheckRegex = config.getSkipCpuCheckRegex();
    }

    @Override
    public <T> void postEvent(T event)
            throws IOException
    {
        VerifierQueryEvent queryEvent = Types.checkType(event, VerifierQueryEvent.class, "event");

        Optional<Double> cpuRatio = getCpuRatio(queryEvent);
        if (cpuRatio.isPresent()) {
            recordCpuRatio(cpuRatio.get());
        }

        if (alwaysPrint || queryEvent.isFailed()
                || (cpuRatio.map(ratio -> ratio > SMALL_REGRESSION).orElse(false) && isCheckCpu(queryEvent))) {
            printEvent(queryEvent);
        }
    }

    private boolean isCheckCpu(VerifierQueryEvent queryEvent)
    {
        if (Pattern.matches(skipCpuCheckRegex, queryEvent.getTestQuery()) ||
                Pattern.matches(skipCpuCheckRegex, queryEvent.getControlQuery())) {
            return false;
        }
        return checkCpu;
    }

    private void recordCpuRatio(double cpuRatio)
    {
        if (cpuRatio <= LARGE_SPEEDUP) {
            cpuRatioLargeSpeedup.add(doubleToSortableLong(cpuRatio));
        }
        else if (cpuRatio <= SMALL_SPEEDUP) {
            cpuRatioSmallSpeedup.add(doubleToSortableLong(cpuRatio));
        }
        else if (cpuRatio <= SMALL_REGRESSION) {
            cpuRatioSmallRegression.add(doubleToSortableLong(cpuRatio));
        }
        else {
            cpuRatioLargeRegression.add(doubleToSortableLong(cpuRatio));
        }
        cpuRatioAll.add(doubleToSortableLong(cpuRatio));
    }

    @Override
    public void close()
    {
        long totalCount = (long) cpuRatioAll.getCount();

        StringBuilder builder = new StringBuilder();
        builder.append("\n");
        builder.append("CPU Ratio\n");
        builder.append("Bucket      (p50)  count      (%)\n");
        builder.append("---------------------------------\n");
        builder.append(formatBucket(0, LARGE_SPEEDUP, cpuRatioLargeSpeedup, totalCount) + "\n");
        builder.append(formatBucket(LARGE_SPEEDUP, SMALL_SPEEDUP, cpuRatioSmallSpeedup, totalCount) + "\n");
        builder.append(formatBucket(SMALL_SPEEDUP, SMALL_REGRESSION, cpuRatioSmallRegression, totalCount) + "\n");
        builder.append(formatBucket(SMALL_REGRESSION, POSITIVE_INFINITY, cpuRatioLargeRegression, totalCount) + "\n");
        builder.append("\n");
        builder.append("CPU Ratio Distribution\n");
        builder.append("-----------------------\n");
        builder.append(format("count: %s\n", cpuRatioAll.getCount()));
        builder.append(format("min: %3.1f\n", 100 * sortableLongToDouble(cpuRatioAll.getMin())));
        builder.append(format("p01: %3.1f\n", 100 * sortableLongToDouble(cpuRatioAll.getQuantile(0.01))));
        builder.append(format("p05: %3.1f\n", 100 * sortableLongToDouble(cpuRatioAll.getQuantile(0.05))));
        builder.append(format("p10: %3.1f\n", 100 * sortableLongToDouble(cpuRatioAll.getQuantile(0.10))));
        builder.append(format("p25: %3.1f\n", 100 * sortableLongToDouble(cpuRatioAll.getQuantile(0.25))));
        builder.append(format("p50: %3.1f\n", 100 * sortableLongToDouble(cpuRatioAll.getQuantile(0.50))));
        builder.append(format("p75: %3.1f\n", 100 * sortableLongToDouble(cpuRatioAll.getQuantile(0.75))));
        builder.append(format("p90: %3.1f\n", 100 * sortableLongToDouble(cpuRatioAll.getQuantile(0.90))));
        builder.append(format("p99: %3.1f\n", 100 * sortableLongToDouble(cpuRatioAll.getQuantile(0.99))));
        builder.append(format("max: %3.1f\n", 100 * sortableLongToDouble(cpuRatioAll.getMax())));
        out.println(builder);
    }

    public static String formatBucket(double min, double max, QuantileDigest digest, long totalCount)
    {
        double p50 = sortableLongToDouble(digest.getQuantile(0.5));
        long count = (long) digest.getCount();
        double countRatio = 1.0 * count / totalCount;

        String maxValue = max == POSITIVE_INFINITY ? "inf" : format("%3.1f", max);

        return format("%3.1f - %s  (%4.2f) %6d  (%4.1f%%)", min, maxValue, p50, count, countRatio * 100);
    }

    private void printEvent(VerifierQueryEvent queryEvent)
    {
        out.println("----------");
        out.println("Name: " + queryEvent.getName());
        out.println("Schema (control): " + queryEvent.getControlSchema());
        out.println("Schema (test): " + queryEvent.getTestSchema());
        out.println("Valid: " + !queryEvent.isFailed());
        out.println("Query (test): " + queryEvent.getTestQuery());

        if (queryEvent.isFailed()) {
            out.println("\nError message:\n" + queryEvent.getErrorMessage());
        }
        else {
            out.println("Control Duration (secs): " + queryEvent.getControlWallTimeSecs());
            out.println("   Test Duration (secs): " + queryEvent.getTestWallTimeSecs());

            Optional<Double> cpuRatio = getCpuRatio(queryEvent);
            if (cpuRatio.isPresent()) {
                out.println("Control CPU (secs): " + queryEvent.getControlCpuTimeSecs());
                out.println("   Test CPU (secs): " + queryEvent.getTestCpuTimeSecs());
                out.println(format("         CPU Ratio: %.1f\n", (double) cpuRatio.get()));
            }
        }
        out.println("----------");
    }

    private Optional<Double> getCpuRatio(VerifierQueryEvent queryEvent)
    {
        Double controlCpuTime = queryEvent.getControlCpuTimeSecs();
        Double testCpuTime = queryEvent.getTestCpuTimeSecs();
        if (controlCpuTime == null || testCpuTime == null) {
            return Optional.empty();
        }
        if (controlCpuTime < regressionMinCpuTime.getValue(SECONDS)) {
            return Optional.empty();
        }

        double value = testCpuTime / controlCpuTime;

        // if both are 0.0, we had no speedup
        if (isNaN(value)) {
            value = 1.0;
        }

        return Optional.of(value);
    }

    /**
     * Converts a double value to a sortable long. The value is converted by getting their IEEE 754
     * floating-point bit layout. Some bits are swapped to be able to compare the result as long.
     */
    private static double sortableLongToDouble(long value)
    {
        value = value ^ (value >> 63) & Long.MAX_VALUE;
        return Double.longBitsToDouble(value);
    }

    /**
     * Converts a sortable long to double.
     *
     * @see #sortableLongToDouble(long)
     */
    private static long doubleToSortableLong(double value)
    {
        long bits = Double.doubleToRawLongBits(value);
        return bits ^ (bits >> 63) & Long.MAX_VALUE;
    }
}

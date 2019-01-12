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
package io.prestosql.benchmark.driver;

import io.airlift.units.Duration;

import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class BenchmarkQueryResult
{
    public enum Status
    {
        PASS, FAIL
    }

    private static final Stat FAIL_STAT = new Stat(new double[0]);

    public static BenchmarkQueryResult passResult(Suite suite, BenchmarkQuery benchmarkQuery, Stat wallTimeNanos, Stat processCpuTimeNanos, Stat queryCpuTimeNanos)
    {
        return new BenchmarkQueryResult(suite, benchmarkQuery, Status.PASS, Optional.empty(), wallTimeNanos, processCpuTimeNanos, queryCpuTimeNanos);
    }

    public static BenchmarkQueryResult failResult(Suite suite, BenchmarkQuery benchmarkQuery, String errorMessage)
    {
        return new BenchmarkQueryResult(suite, benchmarkQuery, Status.FAIL, Optional.of(errorMessage), FAIL_STAT, FAIL_STAT, FAIL_STAT);
    }

    private final Suite suite;
    private final BenchmarkQuery benchmarkQuery;
    private final Status status;
    private final Optional<String> errorMessage;
    private final Stat wallTimeNanos;
    private final Stat processCpuTimeNanos;
    private final Stat queryCpuTimeNanos;

    private BenchmarkQueryResult(
            Suite suite,
            BenchmarkQuery benchmarkQuery,
            Status status,
            Optional<String> errorMessage,
            Stat wallTimeNanos,
            Stat processCpuTimeNanos,
            Stat queryCpuTimeNanos)
    {
        this.suite = requireNonNull(suite, "suite is null");
        this.benchmarkQuery = requireNonNull(benchmarkQuery, "benchmarkQuery is null");
        this.status = requireNonNull(status, "status is null");
        this.errorMessage = requireNonNull(errorMessage, "errorMessage is null");
        this.wallTimeNanos = requireNonNull(wallTimeNanos, "wallTimeNanos is null");
        this.processCpuTimeNanos = requireNonNull(processCpuTimeNanos, "processCpuTimeNanos is null");
        this.queryCpuTimeNanos = requireNonNull(queryCpuTimeNanos, "queryCpuTimeNanos is null");
    }

    public Suite getSuite()
    {
        return suite;
    }

    public BenchmarkQuery getBenchmarkQuery()
    {
        return benchmarkQuery;
    }

    public Status getStatus()
    {
        return status;
    }

    public Optional<String> getErrorMessage()
    {
        return errorMessage;
    }

    public Stat getWallTimeNanos()
    {
        return wallTimeNanos;
    }

    public Stat getProcessCpuTimeNanos()
    {
        return processCpuTimeNanos;
    }

    public Stat getQueryCpuTimeNanos()
    {
        return queryCpuTimeNanos;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("suite", suite.getName())
                .add("benchmarkQuery", benchmarkQuery.getName())
                .add("status", status)
                .add("wallTimeMedian", new Duration(wallTimeNanos.getMedian(), NANOSECONDS).convertToMostSuccinctTimeUnit())
                .add("wallTimeMean", new Duration(wallTimeNanos.getMean(), NANOSECONDS).convertToMostSuccinctTimeUnit())
                .add("wallTimeStd", new Duration(wallTimeNanos.getStandardDeviation(), NANOSECONDS).convertToMostSuccinctTimeUnit())
                .add("processCpuTimeMedian", new Duration(processCpuTimeNanos.getMedian(), NANOSECONDS).convertToMostSuccinctTimeUnit())
                .add("processCpuTimeMean", new Duration(processCpuTimeNanos.getMean(), NANOSECONDS).convertToMostSuccinctTimeUnit())
                .add("processCpuTimeStd", new Duration(processCpuTimeNanos.getStandardDeviation(), NANOSECONDS).convertToMostSuccinctTimeUnit())
                .add("queryCpuTimeMedian", new Duration(queryCpuTimeNanos.getMedian(), NANOSECONDS).convertToMostSuccinctTimeUnit())
                .add("queryCpuTimeMean", new Duration(queryCpuTimeNanos.getMean(), NANOSECONDS).convertToMostSuccinctTimeUnit())
                .add("queryCpuTimeStd", new Duration(queryCpuTimeNanos.getStandardDeviation(), NANOSECONDS).convertToMostSuccinctTimeUnit())
                .add("error", errorMessage)
                .toString();
    }
}

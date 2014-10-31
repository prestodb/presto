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
package com.facebook.presto.benchmark.driver;

import com.google.common.base.MoreObjects;
import io.airlift.units.Duration;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class BenchmarkQueryResult
{
    private final Suite suite;
    private final BenchmarkQuery benchmarkQuery;
    private final Stat wallTimeNanos;
    private final Stat processCpuTimeNanos;
    private final Stat queryCpuTimeNanos;

    public BenchmarkQueryResult(Suite suite, BenchmarkQuery benchmarkQuery, Stat wallTimeNanos, Stat processCpuTimeNanos, Stat queryCpuTimeNanos)
    {
        this.suite = checkNotNull(suite, "suite is null");
        this.benchmarkQuery = checkNotNull(benchmarkQuery, "benchmarkQuery is null");
        this.wallTimeNanos = checkNotNull(wallTimeNanos, "wallTimeNanos is null");
        this.processCpuTimeNanos = checkNotNull(processCpuTimeNanos, "processCpuTimeNanos is null");
        this.queryCpuTimeNanos = checkNotNull(queryCpuTimeNanos, "queryCpuTimeNanos is null");
    }

    public Suite getSuite()
    {
        return suite;
    }

    public BenchmarkQuery getBenchmarkQuery()
    {
        return benchmarkQuery;
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
        return MoreObjects.toStringHelper(this)
                .add("suite", suite.getName())
                .add("benchmarkQuery", benchmarkQuery.getName())
                .add("wallTimeMedian", new Duration(wallTimeNanos.getMedian(), NANOSECONDS).convertToMostSuccinctTimeUnit())
                .add("wallTimeMean", new Duration(wallTimeNanos.getMean(), NANOSECONDS).convertToMostSuccinctTimeUnit())
                .add("wallTimeStd", new Duration(wallTimeNanos.getStandardDeviation(), NANOSECONDS).convertToMostSuccinctTimeUnit())
                .add("processCpuTimeMedian", new Duration(processCpuTimeNanos.getMedian(), NANOSECONDS).convertToMostSuccinctTimeUnit())
                .add("processCpuTimeMean", new Duration(processCpuTimeNanos.getMean(), NANOSECONDS).convertToMostSuccinctTimeUnit())
                .add("processCpuTimeStd", new Duration(processCpuTimeNanos.getStandardDeviation(), NANOSECONDS).convertToMostSuccinctTimeUnit())
                .add("queryCpuTimeMedian", new Duration(queryCpuTimeNanos.getMedian(), NANOSECONDS).convertToMostSuccinctTimeUnit())
                .add("queryCpuTimeMean", new Duration(queryCpuTimeNanos.getMean(), NANOSECONDS).convertToMostSuccinctTimeUnit())
                .add("queryCpuTimeStd", new Duration(queryCpuTimeNanos.getStandardDeviation(), NANOSECONDS).convertToMostSuccinctTimeUnit())
                .toString();
    }
}

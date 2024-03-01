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
package com.facebook.presto.verifier.resolver;

import com.facebook.presto.jdbc.QueryStats;
import com.facebook.presto.verifier.framework.ClusterConnectionException;
import com.facebook.presto.verifier.framework.PrestoQueryException;
import com.facebook.presto.verifier.prestoaction.QueryActionStats;
import org.testng.annotations.Test;

import java.net.SocketTimeoutException;
import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.EXCEEDED_GLOBAL_MEMORY_LIMIT;
import static com.facebook.presto.spi.StandardErrorCode.EXCEEDED_LOCAL_MEMORY_LIMIT;
import static com.facebook.presto.verifier.framework.QueryStage.TEST_MAIN;
import static com.facebook.presto.verifier.framework.QueryStage.TEST_SETUP;
import static com.facebook.presto.verifier.prestoaction.QueryActionStats.EMPTY_STATS;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertFalse;

public class AbstractTestPrestoQueryFailureResolver
{
    protected static final long CONTROL_CPU_TIME_MILLIS = 100000;
    protected static final long CONTROL_PEAK_TOTAL_MEMORY_BYTES = 600L * 1024 * 1024 * 1024;
    protected static final QueryStats CONTROL_QUERY_STATS = createQueryStats(CONTROL_CPU_TIME_MILLIS, CONTROL_PEAK_TOTAL_MEMORY_BYTES);

    private final FailureResolver failureResolver;

    public AbstractTestPrestoQueryFailureResolver(FailureResolver failureResolver)
    {
        this.failureResolver = requireNonNull(failureResolver, "failureResolver is null");
    }

    @Test
    public void testSetupFailure()
    {
        assertFalse(failureResolver.resolveQueryFailure(
                CONTROL_QUERY_STATS,
                new PrestoQueryException(
                        new RuntimeException(),
                        false,
                        TEST_SETUP,
                        Optional.of(EXCEEDED_GLOBAL_MEMORY_LIMIT),
                        createQueryActionStats(CONTROL_CPU_TIME_MILLIS, CONTROL_PEAK_TOTAL_MEMORY_BYTES / 2)),
                Optional.empty())
                .isPresent());
    }

    @Test
    public void testClusterConnectionFailure()
    {
        assertFalse(failureResolver.resolveQueryFailure(
                CONTROL_QUERY_STATS,
                new ClusterConnectionException(new SocketTimeoutException(), TEST_MAIN),
                Optional.empty())
                .isPresent());
    }

    @Test
    public void testNoQueryStats()
    {
        assertFalse(failureResolver.resolveQueryFailure(
                CONTROL_QUERY_STATS,
                new PrestoQueryException(
                        new RuntimeException(),
                        false,
                        TEST_MAIN,
                        Optional.of(EXCEEDED_GLOBAL_MEMORY_LIMIT),
                        EMPTY_STATS),
                Optional.empty())
                .isPresent());
    }

    @Test
    public void testUnrelatedErrorCode()
    {
        assertFalse(failureResolver.resolveQueryFailure(
                CONTROL_QUERY_STATS,
                new PrestoQueryException(
                        new RuntimeException(),
                        false,
                        TEST_MAIN,
                        Optional.of(EXCEEDED_LOCAL_MEMORY_LIMIT),
                        EMPTY_STATS),
                Optional.empty())
                .isPresent());
    }

    protected FailureResolver getFailureResolver()
    {
        return failureResolver;
    }

    protected static QueryStats createQueryStats(long cpuTimeMillis, long peakTotalMemoryBytes)
    {
        return new QueryStats("id", "", false, false, false, 1, 2, 3, 4, 5, cpuTimeMillis, 7, 8, 9, 10, 11, 12, 13, peakTotalMemoryBytes, 13, Optional.empty());
    }

    protected static QueryActionStats createQueryActionStats(long cpuTimeMillis, long peakTotalMemoryBytes)
    {
        return new QueryActionStats(Optional.of(createQueryStats(cpuTimeMillis, peakTotalMemoryBytes)), Optional.empty());
    }
}

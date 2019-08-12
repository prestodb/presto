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
import com.facebook.presto.verifier.framework.QueryException;
import org.testng.annotations.Test;

import java.net.SocketTimeoutException;
import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.EXCEEDED_GLOBAL_MEMORY_LIMIT;
import static com.facebook.presto.spi.StandardErrorCode.EXCEEDED_LOCAL_MEMORY_LIMIT;
import static com.facebook.presto.verifier.framework.QueryStage.TEST_MAIN;
import static com.facebook.presto.verifier.framework.QueryStage.TEST_SETUP;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertFalse;

public class AbstractTestPrestoQueryFailureResolver
{
    protected static final long CONTROL_CPU_TIME_MILLIS = 100000;
    protected static final long CONTROL_PEAK_MEMORY_BYTES = 600L * 1024 * 1024 * 1024;
    protected static final QueryStats CONTROL_QUERY_STATS = createQueryStats(CONTROL_CPU_TIME_MILLIS, CONTROL_PEAK_MEMORY_BYTES);

    private final FailureResolver failureResolver;

    public AbstractTestPrestoQueryFailureResolver(FailureResolver failureResolver)
    {
        this.failureResolver = requireNonNull(failureResolver, "failureResolver is null");
    }

    @Test
    public void testSetupFailure()
    {
        assertFalse(failureResolver.resolve(
                CONTROL_QUERY_STATS,
                QueryException.forPresto(
                        new RuntimeException(),
                        Optional.of(EXCEEDED_GLOBAL_MEMORY_LIMIT),
                        false,
                        Optional.of(createQueryStats(CONTROL_CPU_TIME_MILLIS, CONTROL_PEAK_MEMORY_BYTES / 2)),
                        TEST_SETUP))
                .isPresent());
    }

    @Test
    public void testClusterConnectionFailure()
    {
        assertFalse(failureResolver.resolve(
                CONTROL_QUERY_STATS,
                QueryException.forClusterConnection(new SocketTimeoutException(), TEST_MAIN))
                .isPresent());
    }

    @Test
    public void testNoQueryStats()
    {
        assertFalse(failureResolver.resolve(
                CONTROL_QUERY_STATS,
                QueryException.forPresto(
                        new RuntimeException(),
                        Optional.of(EXCEEDED_GLOBAL_MEMORY_LIMIT),
                        false,
                        Optional.empty(),
                        TEST_MAIN))
                .isPresent());
    }

    @Test
    public void testUnrelatedErrorCode()
    {
        assertFalse(failureResolver.resolve(
                CONTROL_QUERY_STATS,
                QueryException.forPresto(
                        new RuntimeException(),
                        Optional.of(EXCEEDED_LOCAL_MEMORY_LIMIT),
                        false,
                        Optional.empty(),
                        TEST_MAIN))
                .isPresent());
    }

    protected FailureResolver getFailureResolver()
    {
        return failureResolver;
    }

    protected static QueryStats createQueryStats(long cpuTimeMillls, long peakMemoryBytes)
    {
        return new QueryStats("id", "", false, false, 1, 2, 3, 4, 5, cpuTimeMillls, 7, 8, 9, 10, 11, peakMemoryBytes, Optional.empty());
    }
}

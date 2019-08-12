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

import com.facebook.presto.verifier.framework.QueryException;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.EXCEEDED_TIME_LIMIT;
import static com.facebook.presto.verifier.framework.QueryStage.TEST_MAIN;
import static org.testng.Assert.assertEquals;

public class TestExceededTimeLimitFailureResolver
        extends AbstractTestPrestoQueryFailureResolver
{
    public TestExceededTimeLimitFailureResolver()
    {
        super(new ExceededTimeLimitFailureResolver());
    }

    @Test
    public void testResolved()
    {
        assertEquals(
                getFailureResolver().resolve(
                        CONTROL_QUERY_STATS,
                        QueryException.forPresto(
                                new RuntimeException(),
                                Optional.of(EXCEEDED_TIME_LIMIT),
                                false,
                                Optional.of(createQueryStats(CONTROL_CPU_TIME_MILLIS / 2, CONTROL_PEAK_MEMORY_BYTES)),
                                TEST_MAIN)),
                Optional.of("Auto Resolved: Test cluster has less computing resource"));
    }
}

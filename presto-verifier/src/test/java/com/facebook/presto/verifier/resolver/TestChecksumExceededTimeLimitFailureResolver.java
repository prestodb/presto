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

import com.facebook.presto.verifier.framework.PrestoQueryException;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.EXCEEDED_TIME_LIMIT;
import static com.facebook.presto.verifier.framework.QueryStage.CONTROL_CHECKSUM;
import static org.testng.Assert.assertEquals;

public class TestChecksumExceededTimeLimitFailureResolver
        extends AbstractTestPrestoQueryFailureResolver
{
    public TestChecksumExceededTimeLimitFailureResolver()
    {
        super(new ChecksumExceededTimeLimitFailureResolver());
    }

    @Test
    public void testResolved()
    {
        assertEquals(
                getFailureResolver().resolveQueryFailure(
                        CONTROL_QUERY_STATS,
                        new PrestoQueryException(
                                new RuntimeException(),
                                false,
                                CONTROL_CHECKSUM,
                                Optional.of(EXCEEDED_TIME_LIMIT),
                                createQueryActionStats(CONTROL_CPU_TIME_MILLIS / 2, CONTROL_PEAK_TOTAL_MEMORY_BYTES)),
                        Optional.empty()),
                Optional.of("Time limit exceeded when running control checksum query"));
    }
}

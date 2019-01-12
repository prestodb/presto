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
package io.prestosql.plugin.raptor.legacy.metadata;

import com.google.common.collect.ImmutableSet;
import io.airlift.testing.TestingTicker;
import io.airlift.units.Duration;
import io.prestosql.spi.ErrorCodeSupplier;
import io.prestosql.spi.PrestoException;
import org.testng.annotations.Test;

import static io.prestosql.plugin.raptor.legacy.RaptorErrorCode.RAPTOR_REASSIGNMENT_DELAY;
import static io.prestosql.plugin.raptor.legacy.RaptorErrorCode.RAPTOR_REASSIGNMENT_THROTTLE;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestAssignmentLimiter
{
    @Test
    public void testLimiter()
    {
        TestingTicker ticker = new TestingTicker();
        AssignmentLimiter limiter = new AssignmentLimiter(
                ImmutableSet::of,
                ticker,
                new Duration(5, MINUTES),
                new Duration(10, MINUTES));

        // 4:00
        assertCheckFails(limiter, "A", RAPTOR_REASSIGNMENT_DELAY);

        // 4:01
        ticker.increment(1, MINUTES);
        assertCheckFails(limiter, "A", RAPTOR_REASSIGNMENT_DELAY);
        assertCheckFails(limiter, "B", RAPTOR_REASSIGNMENT_DELAY);

        // 4:05
        ticker.increment(4, MINUTES);
        limiter.checkAssignFrom("A");
        assertCheckFails(limiter, "B", RAPTOR_REASSIGNMENT_DELAY);

        // 4:06
        ticker.increment(1, MINUTES);
        assertCheckFails(limiter, "B", RAPTOR_REASSIGNMENT_THROTTLE);
        assertCheckFails(limiter, "C", RAPTOR_REASSIGNMENT_DELAY);

        // 4:14
        ticker.increment(8, MINUTES);
        assertCheckFails(limiter, "B", RAPTOR_REASSIGNMENT_THROTTLE);
        assertCheckFails(limiter, "C", RAPTOR_REASSIGNMENT_THROTTLE);

        // 4:15
        ticker.increment(1, MINUTES);
        limiter.checkAssignFrom("B");
        assertCheckFails(limiter, "C", RAPTOR_REASSIGNMENT_THROTTLE);

        // 4:24
        ticker.increment(9, MINUTES);
        assertCheckFails(limiter, "C", RAPTOR_REASSIGNMENT_THROTTLE);

        // 4:25
        ticker.increment(1, MINUTES);
        limiter.checkAssignFrom("A");

        // 4:30
        ticker.increment(5, MINUTES);
        limiter.checkAssignFrom("A");
        limiter.checkAssignFrom("B");
        limiter.checkAssignFrom("C");
    }

    private static void assertCheckFails(AssignmentLimiter limiter, String node, ErrorCodeSupplier expected)
    {
        try {
            limiter.checkAssignFrom(node);
            fail("expected exception");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), expected.toErrorCode());
        }
    }
}

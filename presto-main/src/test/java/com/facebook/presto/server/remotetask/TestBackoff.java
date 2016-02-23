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
package com.facebook.presto.server.remotetask;

import io.airlift.testing.TestingTicker;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestBackoff
{
    @Test
    public void testMaxFailureInterval()
    {
        TestingTicker ticker = new TestingTicker();
        Backoff backoff = new Backoff(new Duration(15, SECONDS), ticker, new Duration(10, MILLISECONDS));
        ticker.increment(10, MICROSECONDS);

        assertEquals(backoff.getFailureCount(), 0);
        assertEquals(backoff.getTimeSinceLastSuccess().roundTo(SECONDS), 0);

        ticker.increment(14, SECONDS);

        assertFalse(backoff.failure());
        assertEquals(backoff.getFailureCount(), 1);
        assertEquals(backoff.getTimeSinceLastSuccess().roundTo(SECONDS), 14);

        ticker.increment(1, SECONDS);

        assertTrue(backoff.failure());
        assertEquals(backoff.getFailureCount(), 2);
        assertEquals(backoff.getTimeSinceLastSuccess().roundTo(SECONDS), 15);
    }

    @Test
    public void testDelay()
    {
        // 1, 2, 4, 8
        TestingTicker ticker = new TestingTicker();
        Backoff backoff = new Backoff(new Duration(15, SECONDS), ticker,
                new Duration(0, SECONDS),
                new Duration(1, SECONDS),
                new Duration(2, SECONDS),
                new Duration(4, SECONDS),
                new Duration(8, SECONDS));

        assertEquals(backoff.getFailureCount(), 0);
        assertEquals(backoff.getTimeSinceLastSuccess().roundTo(SECONDS), 0);

        assertFalse(backoff.failure());
        assertEquals(backoff.getFailureCount(), 1);
        assertEquals(backoff.getTimeSinceLastSuccess().roundTo(SECONDS), 0);
        long backoffDelay = backoff.getBackoffDelayNanos();
        assertEquals(NANOSECONDS.toSeconds(backoffDelay), 0);

        ticker.increment(backoffDelay, NANOSECONDS);

        assertFalse(backoff.failure());
        assertEquals(backoff.getFailureCount(), 2);
        assertEquals(backoff.getTimeSinceLastSuccess().roundTo(SECONDS), 0);
        backoffDelay = backoff.getBackoffDelayNanos();
        assertEquals(NANOSECONDS.toSeconds(backoffDelay), 1);

        ticker.increment(backoffDelay, NANOSECONDS);

        assertFalse(backoff.failure());
        assertEquals(backoff.getFailureCount(), 3);
        assertEquals(backoff.getTimeSinceLastSuccess().roundTo(SECONDS), 1);
        backoffDelay = backoff.getBackoffDelayNanos();
        assertEquals(NANOSECONDS.toSeconds(backoffDelay), 2);

        ticker.increment(backoffDelay, NANOSECONDS);

        assertFalse(backoff.failure());
        assertEquals(backoff.getFailureCount(), 4);
        assertEquals(backoff.getTimeSinceLastSuccess().roundTo(SECONDS), 3);
        backoffDelay = backoff.getBackoffDelayNanos();
        assertEquals(NANOSECONDS.toSeconds(backoffDelay), 4);

        ticker.increment(backoffDelay, NANOSECONDS);

        assertFalse(backoff.failure());
        assertEquals(backoff.getFailureCount(), 5);
        assertEquals(backoff.getTimeSinceLastSuccess().roundTo(SECONDS), 7);
        backoffDelay = backoff.getBackoffDelayNanos();
        assertEquals(NANOSECONDS.toSeconds(backoffDelay), 8);

        ticker.increment(backoffDelay, NANOSECONDS);

        assertTrue(backoff.failure());
        assertEquals(backoff.getFailureCount(), 6);
        assertEquals(backoff.getTimeSinceLastSuccess().roundTo(SECONDS), 15);
        backoffDelay = backoff.getBackoffDelayNanos();
        assertEquals(NANOSECONDS.toSeconds(backoffDelay), 8);
    }
}

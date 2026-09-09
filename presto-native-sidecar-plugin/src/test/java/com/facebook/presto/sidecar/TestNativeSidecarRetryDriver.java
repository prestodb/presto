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
package com.facebook.presto.sidecar;

import com.facebook.airlift.http.client.ResponseTooLargeException;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.common.util.Backoff;
import com.facebook.presto.spi.PrestoException;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class TestNativeSidecarRetryDriver
{
    /**
     * Returns a Backoff that treats every failure as permanent after minTries by
     * using a ticker that advances by maxFailureInterval on each read, so no real
     * sleeping occurs.
     */
    private static Backoff instantFailingBackoff()
    {
        // Ticker that advances 10 seconds on every read — guarantees failure() returns
        // true after the very first failure interval check (failureCount >= minTries=1).
        Ticker advancingTicker = new Ticker()
        {
            private long nanos;

            @Override
            public long read()
            {
                nanos += SECONDS.toNanos(10);
                return nanos;
            }
        };
        return new Backoff(1, new Duration(1, MILLISECONDS), advancingTicker, ImmutableList.of(new Duration(0, MILLISECONDS)));
    }

    /**
     * Returns a Backoff that allows exactly {@code transientAttempts} transient failures
     * before permanently failing, using the same advancing-ticker trick.
     */
    private static Backoff backoffAllowingTransients(int transientAttempts)
    {
        // A ticker that reports zero until the Nth failure triggers the interval check.
        // We give a large interval so failures don't expire early, but minTries is set
        // so that permanent failure only triggers after transientAttempts+1 calls.
        return new Backoff(
                transientAttempts + 1,
                new Duration(1, SECONDS),
                Ticker.systemTicker(),
                ImmutableList.of(new Duration(0, MILLISECONDS)));
    }

    @Test
    public void testSuccessOnFirstAttemptReturnsResult()
    {
        Backoff backoff = instantFailingBackoff();
        String result = SidecarRetryDriver.executeWithRetry(
                () -> "ok",
                backoff,
                "test",
                new PrestoException(GENERIC_INTERNAL_ERROR, "should not be thrown"));
        assertEquals(result, "ok");
    }

    @Test
    public void testTransientErrorRetriedAndEventuallySucceeds()
    {
        // Fail twice with a RuntimeException, then succeed on the third attempt.
        AtomicInteger attempts = new AtomicInteger();
        Backoff backoff = backoffAllowingTransients(3);

        String result = SidecarRetryDriver.executeWithRetry(
                () -> {
                    if (attempts.incrementAndGet() < 3) {
                        throw new RuntimeException("transient failure " + attempts.get());
                    }
                    return "recovered";
                },
                backoff,
                "test",
                new PrestoException(GENERIC_INTERNAL_ERROR, "exhausted"));

        assertEquals(result, "recovered");
        assertEquals(attempts.get(), 3);
    }

    @Test
    public void testPrestoExceptionPropagatedImmediatelyWithoutRetry()
    {
        AtomicInteger attempts = new AtomicInteger();
        PrestoException originalException = new PrestoException(NOT_SUPPORTED, "definitive error");

        PrestoException thrown = expectThrows(PrestoException.class, () ->
                SidecarRetryDriver.executeWithRetry(
                        () -> {
                            attempts.incrementAndGet();
                            throw originalException;
                        },
                        instantFailingBackoff(),
                        "test",
                        new PrestoException(GENERIC_INTERNAL_ERROR, "wrapper")));

        assertSame(thrown, originalException);
        assertEquals(attempts.get(), 1, "PrestoException must not be retried");
    }

    @Test
    public void testNonRetryableHttpErrorNotRetried()
    {
        AtomicInteger attempts = new AtomicInteger();
        PrestoException wrapper = new PrestoException(GENERIC_INTERNAL_ERROR, "wrapper");

        PrestoException thrown = expectThrows(PrestoException.class, () ->
                SidecarRetryDriver.executeWithRetry(
                        () -> {
                            attempts.incrementAndGet();
                            throw new ResponseTooLargeException();
                        },
                        instantFailingBackoff(),
                        "test",
                        wrapper));

        assertSame(thrown, wrapper);
        assertEquals(attempts.get(), 1, "ResponseTooLargeException must not be retried");
        assertEquals(thrown.getSuppressed().length, 1);
        assertTrue(thrown.getSuppressed()[0] instanceof ResponseTooLargeException);
    }

    @Test
    public void testPermanentFailureAccumulatesSuppressedExceptions()
    {
        // Fail consistently — verify the wrapper is thrown with all transient failures suppressed.
        AtomicInteger attempts = new AtomicInteger();
        PrestoException wrapper = new PrestoException(GENERIC_INTERNAL_ERROR, "exhausted");

        PrestoException thrown = expectThrows(PrestoException.class, () ->
                SidecarRetryDriver.executeWithRetry(
                        () -> {
                            attempts.incrementAndGet();
                            throw new RuntimeException("transient " + attempts.get());
                        },
                        instantFailingBackoff(),
                        "test",
                        wrapper));

        assertSame(thrown, wrapper);
        assertEquals(thrown.getErrorCode(), GENERIC_INTERNAL_ERROR.toErrorCode());
        assertTrue(thrown.getSuppressed().length >= 1, "Expected suppressed exceptions to accumulate");
        assertTrue(thrown.getCause() instanceof RuntimeException);
    }
}

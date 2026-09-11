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

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
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
     * before permanently failing. Uses an advancing ticker (same as {@link #instantFailingBackoff})
     * so permanence is decided by minTries alone, with no dependence on wall time.
     */
    private static Backoff backoffAllowingTransients(int transientAttempts)
    {
        // Ticker advances by 10 s on every read, so failureDuration >= maxFailureInterval
        // is satisfied as soon as failureCount >= minTries (= transientAttempts + 1).
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
        return new Backoff(
                transientAttempts + 1,
                new Duration(1, SECONDS),
                advancingTicker,
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
                () -> new PrestoException(GENERIC_INTERNAL_ERROR, "should not be thrown"));
        assertEquals(result, "ok");
    }

    @Test
    public void testTransientErrorRetriedAndEventuallySucceeds()
    {
        // Fail twice with IOException, then succeed on the third attempt.
        AtomicInteger attempts = new AtomicInteger();
        Backoff backoff = backoffAllowingTransients(3);

        String result = SidecarRetryDriver.executeWithRetry(
                () -> {
                    if (attempts.incrementAndGet() < 3) {
                        throw new IOException("transient failure " + attempts.get());
                    }
                    return "recovered";
                },
                backoff,
                "test",
                () -> new PrestoException(GENERIC_INTERNAL_ERROR, "exhausted"));

        assertEquals(result, "recovered");
        assertEquals(attempts.get(), 3);
    }

    @Test
    public void testTransportExceptionsRetried()
    {
        // Test SocketTimeoutException and ConnectException wrapped in UncheckedIOException
        AtomicInteger attempts = new AtomicInteger();
        Backoff backoff = backoffAllowingTransients(3);

        String result = SidecarRetryDriver.executeWithRetry(
                () -> {
                    int count = attempts.incrementAndGet();
                    if (count == 1) {
                        throw new SocketTimeoutException("timeout");
                    }
                    if (count == 2) {
                        throw new UncheckedIOException(new ConnectException("connection refused"));
                    }
                    return "recovered";
                },
                backoff,
                "test",
                () -> new PrestoException(GENERIC_INTERNAL_ERROR, "exhausted"));

        assertEquals(result, "recovered");
        assertEquals(attempts.get(), 3);
    }

    @Test
    public void testNonRetryableRuntimeExceptionNotRetried()
    {
        AtomicInteger attempts = new AtomicInteger();

        PrestoException thrown = expectThrows(PrestoException.class, () ->
                SidecarRetryDriver.executeWithRetry(
                        () -> {
                            attempts.incrementAndGet();
                            throw new IllegalArgumentException("malformed json response");
                        },
                        instantFailingBackoff(),
                        "test",
                        () -> new PrestoException(GENERIC_INTERNAL_ERROR, "wrapper")));

        assertEquals(attempts.get(), 1, "Deterministic runtime exceptions must not be retried");
        assertEquals(thrown.getSuppressed().length, 1);
        assertTrue(thrown.getSuppressed()[0] instanceof IllegalArgumentException);
    }

    @Test
    public void testInterruptedIOExceptionNotRetried()
    {
        AtomicInteger attempts = new AtomicInteger();

        PrestoException thrown = expectThrows(PrestoException.class, () ->
                SidecarRetryDriver.executeWithRetry(
                        () -> {
                            attempts.incrementAndGet();
                            throw new InterruptedIOException("thread interrupted");
                        },
                        instantFailingBackoff(),
                        "test",
                        () -> new PrestoException(GENERIC_INTERNAL_ERROR, "wrapper")));

        assertEquals(attempts.get(), 1, "InterruptedIOException must not be retried");
        assertEquals(thrown.getSuppressed().length, 1);
        assertTrue(thrown.getSuppressed()[0] instanceof InterruptedIOException);
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
                        () -> new PrestoException(GENERIC_INTERNAL_ERROR, "wrapper")));

        assertSame(thrown, originalException);
        assertEquals(attempts.get(), 1, "PrestoException must not be retried");
    }

    @Test
    public void testNonRetryableHttpErrorNotRetried()
    {
        AtomicInteger attempts = new AtomicInteger();

        PrestoException thrown = expectThrows(PrestoException.class, () ->
                SidecarRetryDriver.executeWithRetry(
                        () -> {
                            attempts.incrementAndGet();
                            throw new ResponseTooLargeException();
                        },
                        instantFailingBackoff(),
                        "test",
                        () -> new PrestoException(GENERIC_INTERNAL_ERROR, "wrapper")));

        assertEquals(attempts.get(), 1, "ResponseTooLargeException must not be retried");
        assertEquals(thrown.getSuppressed().length, 1);
        assertTrue(thrown.getSuppressed()[0] instanceof ResponseTooLargeException);
    }

    @Test
    public void testPermanentFailureAttachesFirstTransientAsSuppressed()
    {
        AtomicInteger attempts = new AtomicInteger();

        PrestoException thrown = expectThrows(PrestoException.class, () ->
                SidecarRetryDriver.executeWithRetry(
                        () -> {
                            attempts.incrementAndGet();
                            throw new IOException("transient " + attempts.get());
                        },
                        instantFailingBackoff(),
                        "test",
                        () -> new PrestoException(GENERIC_INTERNAL_ERROR, "exhausted")));

        assertEquals(thrown.getSuppressed().length, 2);
        assertTrue(thrown.getSuppressed()[0] instanceof IOException);
        assertEquals(thrown.getSuppressed()[0].getMessage(), "transient 1", "first failure");
        assertTrue(thrown.getSuppressed()[1] instanceof IOException);
        assertEquals(thrown.getSuppressed()[1].getMessage(), "transient 2", "latest failure");
    }

    @Test
    public void testBoundedSuppressedHistoryFirstLatestAndDropSummary()
    {
        AtomicInteger attempts = new AtomicInteger();

        PrestoException thrown = expectThrows(PrestoException.class, () ->
                SidecarRetryDriver.executeWithRetry(
                        () -> {
                            attempts.incrementAndGet();
                            throw new IOException("transient " + attempts.get());
                        },
                        backoffAllowingTransients(4),
                        "test",
                        () -> new PrestoException(GENERIC_INTERNAL_ERROR, "exhausted")));

        Throwable[] suppressed = thrown.getSuppressed();
        assertEquals(suppressed.length, 3);
        assertTrue(suppressed[0] instanceof IOException);
        assertEquals(suppressed[0].getMessage(), "transient 1", "first failure");
        assertTrue(suppressed[1] instanceof RuntimeException);
        assertTrue(suppressed[1].getMessage().contains("3 intermediate failure(s) dropped"));
        assertTrue(suppressed[2] instanceof IOException);
        assertEquals(suppressed[2].getMessage(), "transient 5", "latest failure");
    }

    @Test
    public void testBoundedSuppressedHistoryTwoFailuresNoDropSummary()
    {
        AtomicInteger attempts = new AtomicInteger();

        PrestoException thrown = expectThrows(PrestoException.class, () ->
                SidecarRetryDriver.executeWithRetry(
                        () -> {
                            attempts.incrementAndGet();
                            throw new IOException("transient " + attempts.get());
                        },
                        backoffAllowingTransients(2),
                        "test",
                        () -> new PrestoException(GENERIC_INTERNAL_ERROR, "exhausted")));

        Throwable[] suppressed = thrown.getSuppressed();
        assertEquals(suppressed.length, 3);
        assertTrue(suppressed[0] instanceof IOException);
        assertEquals(suppressed[0].getMessage(), "transient 1", "first failure");
        assertTrue(suppressed[1] instanceof RuntimeException);
        assertTrue(suppressed[1].getMessage().contains("1 intermediate failure(s) dropped"));
        assertTrue(suppressed[2] instanceof IOException);
        assertEquals(suppressed[2].getMessage(), "transient 3", "latest failure");
    }
}

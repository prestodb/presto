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
package com.facebook.presto.util;

import com.facebook.airlift.units.Duration;
import com.facebook.presto.spi.PrestoException;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.net.SocketTimeoutException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class RetryRunner
{
    private final ScheduledExecutorService executor;
    private final int maxRetries;
    private final Duration minBackoff;
    private final Duration maxBackoff;
    private final double scale;
    private final Duration maxRetryTime;

    public RetryRunner(ScheduledExecutorService executor)
    {
        this(executor, 5, new Duration(100, MILLISECONDS), new Duration(30, SECONDS), 2.0,
                new Duration(1, MINUTES));
    }

    public RetryRunner(
            ScheduledExecutorService executor,
            int maxRetries,
            Duration minBackoff,
            Duration maxBackoff,
            double scale,
            Duration maxRetryTime)
    {
        this.executor = executor;
        this.maxRetries = maxRetries;
        this.minBackoff = minBackoff;
        this.maxBackoff = maxBackoff;
        this.scale = scale;
        this.maxRetryTime = maxRetryTime;
    }

    public <T> T runWithRetry(Supplier<T> attempt, boolean idempotent)
    {
        CompletableFuture<T> result = new CompletableFuture<>();
        long startNanos = System.nanoTime();
        long maxRetryNanos = maxRetryTime.roundTo(NANOSECONDS);

        class State
                implements Runnable
        {
            private int failures;

            @Override
            public void run()
            {
                if (result.isDone()) {
                    return;
                }

                executor.submit(() -> {
                    if (result.isDone()) {
                        return;
                    }
                    try {
                        result.complete(attempt.get());
                    }
                    catch (Throwable t) {
                        if (!shouldRetry(t, idempotent)) {
                            result.completeExceptionally(t);
                            return;
                        }
                        if (failures >= maxRetries) {
                            result.completeExceptionally(t);
                            return;
                        }

                        long elapsed = System.nanoTime() - startNanos;
                        if (elapsed >= maxRetryNanos) {
                            result.completeExceptionally(t);
                            return;
                        }

                        failures++;

                        Duration backoff = backoffDelay(failures);
                        long remainingNanos = maxRetryNanos - elapsed;
                        long delayMs = Math.min(backoff.toMillis(), NANOSECONDS.toMillis(remainingNanos));

                        executor.schedule(this, delayMs, MILLISECONDS);
                    }
                });
            }

            private Duration backoffDelay(int failureCount)
            {
                double scaled = minBackoff.toMillis() * Math.pow(scale, failureCount - 1);
                long delayMs = (long) Math.min(scaled, maxBackoff.toMillis());
                return new Duration(delayMs, MILLISECONDS);
            }
        }

        new State().run();

        try {
            return result.get();
        }
        catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new PrestoException(GENERIC_INTERNAL_ERROR, ie);
        }
        catch (ExecutionException e) {
            throwIfInstanceOf(e.getCause(), PrestoException.class);
            throwIfUnchecked(e.getCause());
            throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    private static boolean shouldRetry(Throwable t, boolean idempotent)
    {
        if (t instanceof InterruptedException) {
            Thread.currentThread().interrupt();
            return false;
        }

        if (t instanceof UncheckedIOException && t.getCause() != null) {
            t = t.getCause();
        }

        if (idempotent && t instanceof SocketTimeoutException) {
            return true;
        }

        if (t instanceof InterruptedIOException) {
            return false;
        }

        return idempotent && t instanceof IOException;
    }
}

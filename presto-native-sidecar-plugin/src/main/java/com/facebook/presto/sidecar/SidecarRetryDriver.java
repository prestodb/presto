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
import com.facebook.airlift.http.client.UnexpectedResponseException;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.util.Backoff;
import com.facebook.presto.spi.PrestoException;

import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Executes a sidecar HTTP call with exponential-backoff retry, using the same
 * {@link Backoff} mechanism that {@code PageBufferClient} / ExchangeClient uses.
 *
 * <p>On each transient failure the thread sleeps for the delay returned by
 * {@link Backoff#getBackoffDelayNanos()} and retries. The retry loop terminates
 * when {@link Backoff#failure()} returns {@code true} (i.e. the configured
 * {@code maxFailureInterval} has elapsed after at least {@code minTries} attempts).
 *
 * <p>The following exceptions are <em>never</em> retried:
 * <ul>
 *   <li>{@link PrestoException} — definitive server-side error, re-thrown immediately.</li>
 *   <li>{@link CancellationException} — query was cancelled; re-thrown immediately.</li>
 *   <li>{@link UnexpectedResponseException} — unexpected HTTP status (e.g. 4xx); retrying
 *       will not help and indicates a configuration or protocol bug.</li>
 *   <li>{@link ResponseTooLargeException} — response exceeded the configured size limit;
 *       not a transient condition.</li>
 *   <li>{@link InterruptedException} — the planning thread was interrupted (query cancelled);
 *       re-interrupts the thread and throws a {@link PrestoException}.</li>
 * </ul>
 *
 * <p>All other exceptions (transport errors, SSL failures, timeouts wrapped in
 * {@code UncheckedIOException}, etc.) are treated as transient and retried.
 * Each transient failure is collected and added as a suppressed exception to the
 * final {@code permanentFailureWrapper} so the full retry history is visible in
 * the stack trace.
 */
public final class SidecarRetryDriver
{
    private static final Logger log = Logger.get(SidecarRetryDriver.class);

    private SidecarRetryDriver() {}

    /**
     * @param operation              supplier that performs one attempt of the HTTP call and returns its result
     * @param backoff                a fresh {@link Backoff} instance scoped to this logical call
     * @param description            short description used in log/error messages (e.g. "session properties")
     * @param permanentFailureWrapper exception thrown when all retries are exhausted
     * @param <T>                    return type of the HTTP call
     * @return the result of the first successful {@code operation} invocation
     * @throws PrestoException if {@code operation} throws a {@link PrestoException} (propagated immediately),
     *                         if the thread is interrupted, or if all retries are exhausted
     */
    public static <T> T executeWithRetry(Callable<T> operation, Backoff backoff, String description, RuntimeException permanentFailureWrapper)
    {
        while (true) {
            backoff.startRequest();
            try {
                T result = operation.call();
                backoff.success();
                return result;
            }
            catch (PrestoException e) {
                // Definitive server-side error — do not retry.
                throw e;
            }
            catch (CancellationException e) {
                // Query was cancelled — do not retry, propagate as-is.
                throw e;
            }
            catch (UnexpectedResponseException | ResponseTooLargeException e) {
                // Non-transient HTTP-level error — retrying will not help.
                log.error(e, "Sidecar call for '%s' failed with non-retryable HTTP error", description);
                permanentFailureWrapper.addSuppressed(e);
                if (permanentFailureWrapper.getCause() == null) {
                    permanentFailureWrapper.initCause(e);
                }
                throw permanentFailureWrapper;
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new PrestoException(GENERIC_INTERNAL_ERROR,
                        format("Interrupted while waiting to retry sidecar call for '%s'", description), e);
            }
            catch (Exception e) {
                // Collect transient failures so the full retry history is visible in the final error.
                permanentFailureWrapper.addSuppressed(e);

                if (backoff.failure()) {
                    log.error(e, "Sidecar call for '%s' failed permanently after %s failures over %s",
                            description,
                            backoff.getFailureCount(),
                            backoff.getFailureDuration());
                    if (permanentFailureWrapper.getCause() == null) {
                        permanentFailureWrapper.initCause(e);
                    }
                    throw permanentFailureWrapper;
                }

                long delayNanos = backoff.getBackoffDelayNanos();
                log.debug(e, "Sidecar call for '%s' failed (attempt %d), retrying in %d ms",
                        description,
                        backoff.getFailureCount(),
                        NANOSECONDS.toMillis(delayNanos));

                if (delayNanos > 0) {
                    try {
                        NANOSECONDS.sleep(delayNanos);
                    }
                    catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new PrestoException(GENERIC_INTERNAL_ERROR,
                                format("Interrupted while waiting to retry sidecar call for '%s'", description), ie);
                    }
                }
            }
        }
    }
}

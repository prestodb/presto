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
package com.facebook.presto.connector.thrift.util;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class RetryDriver
{
    private static final Logger log = Logger.get(RetryDriver.class);
    private static final int DEFAULT_RETRY_ATTEMPTS = 10;
    private static final Duration DEFAULT_SLEEP_TIME = Duration.valueOf("1s");
    private static final Duration DEFAULT_MAX_RETRY_TIME = Duration.valueOf("30s");
    private static final double DEFAULT_SCALE_FACTOR = 2.0;

    private final int maxAttempts;
    private final Duration minSleepTime;
    private final Duration maxSleepTime;
    private final double scaleFactor;
    private final Duration maxRetryTime;
    private final Optional<Runnable> retryRunnable;
    private final Predicate<Exception> stopRetrying;
    private final Function<Exception, Exception> classifier;
    private final ListeningScheduledExecutorService retryExecutorService;

    private RetryDriver(
            int maxAttempts,
            Duration minSleepTime,
            Duration maxSleepTime,
            double scaleFactor,
            Duration maxRetryTime,
            Optional<Runnable> retryRunnable,
            Predicate<Exception> stopRetrying,
            Function<Exception, Exception> classifier,
            ListeningScheduledExecutorService retryExecutorService)
    {
        this.maxAttempts = maxAttempts;
        this.minSleepTime = minSleepTime;
        this.maxSleepTime = maxSleepTime;
        this.scaleFactor = scaleFactor;
        this.maxRetryTime = maxRetryTime;
        this.retryRunnable = retryRunnable;
        this.stopRetrying = stopRetrying;
        this.classifier = classifier;
        this.retryExecutorService = retryExecutorService;
    }

    private RetryDriver(ListeningScheduledExecutorService executor)
    {
        this(DEFAULT_RETRY_ATTEMPTS,
                DEFAULT_SLEEP_TIME,
                DEFAULT_SLEEP_TIME,
                DEFAULT_SCALE_FACTOR,
                DEFAULT_MAX_RETRY_TIME,
                Optional.empty(),
                e -> false,
                Function.identity(),
                executor);
    }

    public static RetryDriver retry(ListeningScheduledExecutorService executor)
    {
        return new RetryDriver(executor);
    }

    public final RetryDriver maxAttempts(int maxAttempts)
    {
        return new RetryDriver(maxAttempts, minSleepTime, maxSleepTime, scaleFactor, maxRetryTime, retryRunnable, stopRetrying, classifier, retryExecutorService);
    }

    public final RetryDriver exponentialBackoff(Duration minSleepTime, Duration maxSleepTime, Duration maxRetryTime, double scaleFactor)
    {
        return new RetryDriver(maxAttempts, minSleepTime, maxSleepTime, scaleFactor, maxRetryTime, retryRunnable, stopRetrying, classifier, retryExecutorService);
    }

    public final RetryDriver onRetry(Runnable retryRunnable)
    {
        return new RetryDriver(maxAttempts, minSleepTime, maxSleepTime, scaleFactor, maxRetryTime, Optional.ofNullable(retryRunnable), stopRetrying, classifier, retryExecutorService);
    }

    public RetryDriver stopRetryingWhen(Predicate<Exception> stopRetrying)
    {
        return new RetryDriver(maxAttempts, minSleepTime, maxSleepTime, scaleFactor, maxRetryTime, retryRunnable, stopRetrying, classifier, retryExecutorService);
    }

    public RetryDriver withClassifier(Function<Exception, Exception> classifier)
    {
        return new RetryDriver(maxAttempts, minSleepTime, maxSleepTime, scaleFactor, maxRetryTime, retryRunnable, stopRetrying, classifier, retryExecutorService);
    }

    /**
     * This is only for sync calls. For Callable that returns Future object, use runAsync instead.
     */
    public <V> V run(String callableName, Callable<V> callable)
    {
        requireNonNull(callableName, "callableName is null");
        requireNonNull(callable, "callable is null");
        RetryStatus retryStatus = new RetryStatus();

        while (true) {
            retryStatus.nextAttempt();
            try {
                return callable.call();
            }
            catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw propagate(ie);
            }
            catch (Exception e) {
                if (retryStatus.shouldStopRetry(e)) {
                    throw propagate(e);
                }

                log.warn(e, "Failed on executing %s with attempt %d, will retry.", callableName, retryStatus.getAttempts());
                try {
                    MILLISECONDS.sleep(retryStatus.getRetryDelayInMs());
                }
                catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw propagate(ie);
                }
            }
        }
    }

    public <V> ListenableFuture<V> runAsync(String callableName, Callable<ListenableFuture<V>> callable)
    {
        requireNonNull(callableName, "callableName is null");
        requireNonNull(callable, "callable is null");
        return runAsyncInternal(callableName, callable, new RetryStatus());
    }

    private <V> ListenableFuture<V> runAsyncInternal(String callableName, Callable<ListenableFuture<V>> callable, RetryStatus retryStatus)
    {
        retryStatus.nextAttempt();
        ListenableFuture<V> resultFuture;
        try {
            resultFuture = callable.call();
        }
        catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw propagate(ie);
        }
        catch (Exception e) {
            resultFuture = immediateFailedFuture(e);
        }

        return Futures.catchingAsync(resultFuture, Exception.class, e -> {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
                throw propagate(e);
            }
            else if (retryStatus.shouldStopRetry(e)) {
                throw propagate(e);
            }
            log.warn(e, "Failed on executing %s with attempt %d, will perform async retry.", callableName, retryStatus.getAttempts());
            return Futures.dereference(retryExecutorService.schedule(() -> runAsyncInternal(callableName, callable, retryStatus), retryStatus.getRetryDelayInMs(), MILLISECONDS));
        });
    }

    private RuntimeException propagate(Exception e)
    {
        Exception classified = classifier.apply(e);
        throwIfUnchecked(classified);
        throw new RuntimeException(classified);
    }

    private final class RetryStatus
    {
        private final long startTime;
        private final AtomicInteger attempts;

        RetryStatus()
        {
            this.startTime = System.nanoTime();
            this.attempts = new AtomicInteger(0);
        }

        long getRetryDelayInMs()
        {
            int delayInMs = (int) Math.min(minSleepTime.toMillis() * Math.pow(scaleFactor, attempts.get() - 1), maxSleepTime.toMillis());
            int jitter = ThreadLocalRandom.current().nextInt(Math.max(1, (int) (delayInMs * 0.1)));
            return delayInMs + jitter;
        }

        boolean shouldStopRetry(Exception e)
        {
            return stopRetrying.test(e) || attempts.get() >= maxAttempts || Duration.nanosSince(startTime).compareTo(maxRetryTime) >= 0;
        }

        int getAttempts()
        {
            return attempts.get();
        }

        void nextAttempt()
        {
            if (attempts.incrementAndGet() > 1) {
                retryRunnable.ifPresent(Runnable::run);
            }
        }
    }
}

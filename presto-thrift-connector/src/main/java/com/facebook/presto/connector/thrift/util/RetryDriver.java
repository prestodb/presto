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

import io.airlift.log.Logger;
import io.airlift.units.Duration;

import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

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

    private RetryDriver(
            int maxAttempts,
            Duration minSleepTime,
            Duration maxSleepTime,
            double scaleFactor,
            Duration maxRetryTime,
            Optional<Runnable> retryRunnable,
            Predicate<Exception> stopRetrying,
            Function<Exception, Exception> classifier)
    {
        this.maxAttempts = maxAttempts;
        this.minSleepTime = minSleepTime;
        this.maxSleepTime = maxSleepTime;
        this.scaleFactor = scaleFactor;
        this.maxRetryTime = maxRetryTime;
        this.retryRunnable = retryRunnable;
        this.stopRetrying = stopRetrying;
        this.classifier = classifier;
    }

    private RetryDriver()
    {
        this(DEFAULT_RETRY_ATTEMPTS,
                DEFAULT_SLEEP_TIME,
                DEFAULT_SLEEP_TIME,
                DEFAULT_SCALE_FACTOR,
                DEFAULT_MAX_RETRY_TIME,
                Optional.empty(),
                e -> false,
                Function.identity());
    }

    public static RetryDriver retry()
    {
        return new RetryDriver();
    }

    public final RetryDriver maxAttempts(int maxAttempts)
    {
        return new RetryDriver(maxAttempts, minSleepTime, maxSleepTime, scaleFactor, maxRetryTime, retryRunnable, stopRetrying, classifier);
    }

    public final RetryDriver exponentialBackoff(Duration minSleepTime, Duration maxSleepTime, Duration maxRetryTime, double scaleFactor)
    {
        return new RetryDriver(maxAttempts, minSleepTime, maxSleepTime, scaleFactor, maxRetryTime, retryRunnable, stopRetrying, classifier);
    }

    public final RetryDriver onRetry(Runnable retryRunnable)
    {
        return new RetryDriver(maxAttempts, minSleepTime, maxSleepTime, scaleFactor, maxRetryTime, Optional.ofNullable(retryRunnable), stopRetrying, classifier);
    }

    public RetryDriver stopRetryingWhen(Predicate<Exception> stopRetrying)
    {
        return new RetryDriver(maxAttempts, minSleepTime, maxSleepTime, scaleFactor, maxRetryTime, retryRunnable, stopRetrying, classifier);
    }

    public RetryDriver withClassifier(Function<Exception, Exception> classifier)
    {
        return new RetryDriver(maxAttempts, minSleepTime, maxSleepTime, scaleFactor, maxRetryTime, retryRunnable, stopRetrying, classifier);
    }

    public <V> V run(String callableName, Callable<V> callable)
    {
        requireNonNull(callableName, "callableName is null");
        requireNonNull(callable, "callable is null");

        long startTime = System.nanoTime();
        int attempt = 0;
        while (true) {
            attempt++;

            if (attempt > 1) {
                retryRunnable.ifPresent(Runnable::run);
            }

            try {
                return callable.call();
            }
            catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw propagate(ie);
            }
            catch (Exception e) {
                if (stopRetrying.test(e)) {
                    throw propagate(e);
                }
                if (attempt >= maxAttempts || Duration.nanosSince(startTime).compareTo(maxRetryTime) >= 0) {
                    throw propagate(e);
                }
                log.warn("Failed on executing %s with attempt %d, will retry. Exception: %s", callableName, attempt, e.getMessage());

                int delayInMs = (int) Math.min(minSleepTime.toMillis() * Math.pow(scaleFactor, attempt - 1), maxSleepTime.toMillis());
                int jitter = ThreadLocalRandom.current().nextInt(Math.max(1, (int) (delayInMs * 0.1)));
                try {
                    TimeUnit.MILLISECONDS.sleep(delayInMs + jitter);
                }
                catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw propagate(ie);
                }
            }
        }
    }

    private RuntimeException propagate(Exception e)
    {
        Exception classified = classifier.apply(e);
        throwIfUnchecked(classified);
        throw new RuntimeException(classified);
    }
}

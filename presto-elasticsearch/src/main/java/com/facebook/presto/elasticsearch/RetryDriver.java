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
package com.facebook.presto.elasticsearch;

import io.airlift.log.Logger;
import io.airlift.units.Duration;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class RetryDriver
{
    private static final Logger log = Logger.get(RetryDriver.class);
    private static final int DEFAULT_RETRY_ATTEMPTS = 5;
    private static final Duration DEFAULT_SLEEP_TIME = Duration.valueOf("1s");
    private static final Duration DEFAULT_MAX_RETRY_TIME = Duration.valueOf("10s");
    private static final double DEFAULT_SCALE_FACTOR = 2.0;

    private final int maxAttempts;
    private final Duration maxRetryTime;

    private RetryDriver(
            int maxAttempts,
            Duration maxRetryTime)
    {
        this.maxAttempts = maxAttempts;
        this.maxRetryTime = maxRetryTime;
    }

    private RetryDriver()
    {
        this(DEFAULT_RETRY_ATTEMPTS,
                DEFAULT_MAX_RETRY_TIME);
    }

    public static RetryDriver retry()
    {
        return new RetryDriver();
    }

    public final RetryDriver maxAttempts(int maxAttempts)
    {
        return new RetryDriver(maxAttempts, maxRetryTime);
    }

    public final RetryDriver exponentialBackoff(Duration maxRetryTime)
    {
        return new RetryDriver(maxAttempts, maxRetryTime);
    }

    public <V> V run(String callableName, Callable<V> callable)
            throws Exception
    {
        requireNonNull(callableName, "callableName is null");
        requireNonNull(callable, "callable is null");

        List<Throwable> suppressedExceptions = new ArrayList<>();
        long startTime = System.nanoTime();
        int attempt = 0;
        while (true) {
            attempt++;

            try {
                return callable.call();
            }
            catch (Exception e) {
                if (attempt >= maxAttempts || Duration.nanosSince(startTime).compareTo(maxRetryTime) >= 0) {
                    addSuppressed(e, suppressedExceptions);
                    throw e;
                }
                log.debug("Failed on executing %s with attempt %d, will retry. Exception: %s", callableName, attempt, e.getMessage());

                suppressedExceptions.add(e);
                int delayInMs = (int) Math.min(DEFAULT_SLEEP_TIME.toMillis() * Math.pow(DEFAULT_SCALE_FACTOR, attempt - 1), DEFAULT_SLEEP_TIME.toMillis());
                int jitter = ThreadLocalRandom.current().nextInt(Math.max(1, (int) (delayInMs * 0.1)));
                try {
                    MILLISECONDS.sleep(delayInMs + jitter);
                }
                catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(ie);
                }
            }
        }
    }

    private static void addSuppressed(Exception exception, List<Throwable> suppressedExceptions)
    {
        for (Throwable suppressedException : suppressedExceptions) {
            if (exception != suppressedException) {
                exception.addSuppressed(suppressedException);
            }
        }
    }
}

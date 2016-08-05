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
package com.facebook.presto.hive;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

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
    private final Function<Exception, Exception> exceptionMapper;
    private final List<Class<? extends Exception>> exceptionWhiteList;
    private final Optional<Runnable> retryRunnable;

    private RetryDriver(
            int maxAttempts,
            Duration minSleepTime,
            Duration maxSleepTime,
            double scaleFactor,
            Duration maxRetryTime,
            Function<Exception, Exception> exceptionMapper,
            List<Class<? extends Exception>> exceptionWhiteList,
            Optional<Runnable> retryRunnable)
    {
        this.maxAttempts = maxAttempts;
        this.minSleepTime = minSleepTime;
        this.maxSleepTime = maxSleepTime;
        this.scaleFactor = scaleFactor;
        this.maxRetryTime = maxRetryTime;
        this.exceptionMapper = exceptionMapper;
        this.exceptionWhiteList = exceptionWhiteList;
        this.retryRunnable = retryRunnable;
    }

    private RetryDriver()
    {
        this(DEFAULT_RETRY_ATTEMPTS,
                DEFAULT_SLEEP_TIME,
                DEFAULT_SLEEP_TIME,
                DEFAULT_SCALE_FACTOR,
                DEFAULT_MAX_RETRY_TIME,
                Function.identity(),
                ImmutableList.of(),
                Optional.empty());
    }

    public static RetryDriver retry()
    {
        return new RetryDriver();
    }

    public final RetryDriver maxAttempts(int maxAttempts)
    {
        return new RetryDriver(maxAttempts, minSleepTime, maxSleepTime, scaleFactor, maxRetryTime, exceptionMapper, exceptionWhiteList, retryRunnable);
    }

    public final RetryDriver exponentialBackoff(Duration minSleepTime, Duration maxSleepTime, Duration maxRetryTime, double scaleFactor)
    {
        return new RetryDriver(maxAttempts, minSleepTime, maxSleepTime, scaleFactor, maxRetryTime, exceptionMapper, exceptionWhiteList, retryRunnable);
    }

    public final RetryDriver onRetry(Runnable retryRunnable)
    {
        return new RetryDriver(maxAttempts, minSleepTime, maxSleepTime, scaleFactor, maxRetryTime, exceptionMapper, exceptionWhiteList, Optional.ofNullable(retryRunnable));
    }

    public final RetryDriver exceptionMapper(Function<Exception, Exception> exceptionMapper)
    {
        return new RetryDriver(maxAttempts, minSleepTime, maxSleepTime, scaleFactor, maxRetryTime, exceptionMapper, exceptionWhiteList, retryRunnable);
    }

    @SafeVarargs
    public final RetryDriver stopOn(Class<? extends Exception>... classes)
    {
        requireNonNull(classes, "classes is null");
        List<Class<? extends Exception>> exceptions = ImmutableList.<Class<? extends Exception>>builder()
                .addAll(exceptionWhiteList)
                .addAll(Arrays.asList(classes))
                .build();

        return new RetryDriver(maxAttempts, minSleepTime, maxSleepTime, scaleFactor, maxRetryTime, exceptionMapper, exceptions, retryRunnable);
    }

    public RetryDriver stopOnIllegalExceptions()
    {
        return stopOn(NullPointerException.class, IllegalStateException.class, IllegalArgumentException.class);
    }

    public <V> V run(String callableName, Callable<V> callable)
            throws Exception
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
            catch (Exception e) {
                e = exceptionMapper.apply(e);
                for (Class<? extends Exception> clazz : exceptionWhiteList) {
                    if (clazz.isInstance(e)) {
                        throw e;
                    }
                }
                if (attempt >= maxAttempts || Duration.nanosSince(startTime).compareTo(maxRetryTime) >= 0) {
                    throw e;
                }
                log.debug("Failed on executing %s with attempt %d, will retry. Exception: %s", callableName, attempt, e.getMessage());

                int delayInMs = (int) Math.min(minSleepTime.toMillis() * Math.pow(scaleFactor, attempt - 1), maxSleepTime.toMillis());
                try {
                    TimeUnit.MILLISECONDS.sleep(delayInMs);
                }
                catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw Throwables.propagate(ie);
                }
            }
        }
    }
}

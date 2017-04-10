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
package com.facebook.presto.cassandra;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

public class RetryDriver
{
    private static final Logger log = Logger.get(RetryDriver.class);
    private static final int DEFAULT_RETRY_ATTEMPTS = 10;
    private static final Duration DEFAULT_SLEEP_TIME = Duration.valueOf("1s");
    private static final Duration DEFAULT_MAX_RETRY_TIME = Duration.valueOf("30s");

    private final int maxRetryAttempts;
    private final Duration sleepTime;
    private final Duration maxRetryTime;
    private final List<Class<? extends Exception>> exceptionWhiteList;

    private RetryDriver(int maxRetryAttempts, Duration sleepTime, Duration maxRetryTime, List<Class<? extends Exception>> exceptionWhiteList)
    {
        this.maxRetryAttempts = maxRetryAttempts;
        this.sleepTime = sleepTime;
        this.maxRetryTime = maxRetryTime;
        this.exceptionWhiteList = exceptionWhiteList;
    }

    private RetryDriver()
    {
        this(DEFAULT_RETRY_ATTEMPTS, DEFAULT_SLEEP_TIME, DEFAULT_MAX_RETRY_TIME, ImmutableList.of());
    }

    public static RetryDriver retry()
    {
        return new RetryDriver();
    }

    @SafeVarargs
    public final RetryDriver stopOn(Class<? extends Exception>... classes)
    {
        requireNonNull(classes, "classes is null");
        List<Class<? extends Exception>> exceptions = ImmutableList.<Class<? extends Exception>>builder()
                .addAll(exceptionWhiteList)
                .addAll(Arrays.asList(classes))
                .build();

        return new RetryDriver(maxRetryAttempts, sleepTime, maxRetryTime, exceptions);
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
            try {
                return callable.call();
            }
            catch (Exception e) {
                for (Class<? extends Exception> clazz : exceptionWhiteList) {
                    if (clazz.isInstance(e)) {
                        throw e;
                    }
                }
                if (attempt >= maxRetryAttempts || Duration.nanosSince(startTime).compareTo(maxRetryTime) >= 0) {
                    throw e;
                }
                log.debug("Failed on executing %s with attempt %d, will retry. Exception: %s", callableName, attempt, e.getMessage());
                TimeUnit.MILLISECONDS.sleep(sleepTime.toMillis());
            }
        }
    }
}

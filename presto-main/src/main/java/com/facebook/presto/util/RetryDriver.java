package com.facebook.presto.util;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class RetryDriver
{
    private static final Logger log = Logger.get(RetryDriver.class);
    private static final String DEFAULT_CALLABLE_NAME = "<default>";
    private static final int DEFAULT_RETRY_ATTEMPTS = 10;
    private static final Duration DEFAULT_SLEEP_TIME = Duration.valueOf("1s");

    private final int maxRetryAttempts;
    private final Duration sleepTime;
    private final List<Class<? extends Exception>> exceptionWhitelist;

    private RetryDriver(int maxRetryAttempts, Duration sleepTime, List<Class<? extends Exception>> exceptionWhitelist)
    {
        this.maxRetryAttempts = maxRetryAttempts;
        this.sleepTime = sleepTime;
        this.exceptionWhitelist = exceptionWhitelist;

    }

    private RetryDriver()
    {
        this(DEFAULT_RETRY_ATTEMPTS, DEFAULT_SLEEP_TIME, ImmutableList.<Class<? extends Exception>>of());
    }

    public static RetryDriver retry()
    {
        return new RetryDriver();
    }

    public RetryDriver withMaxRetries(int maxRetryAttempts)
    {
        checkArgument(maxRetryAttempts > 0, "maxRetryAttempts must be greater than zero");
        return new RetryDriver(maxRetryAttempts, sleepTime, exceptionWhitelist);
    }

    public RetryDriver withSleep(Duration sleepTime)
    {
        return new RetryDriver(maxRetryAttempts, checkNotNull(sleepTime, "sleepTime is null"), exceptionWhitelist);
    }

    @SafeVarargs
    public final RetryDriver stopOn(Class<? extends Exception>... classes)
    {
        checkNotNull(classes, "classes is null");
        List<Class<? extends Exception>> exceptions = ImmutableList.<Class<? extends Exception>>builder()
                .addAll(exceptionWhitelist)
                .addAll(Arrays.asList(classes))
                .build();


        return new RetryDriver(maxRetryAttempts, sleepTime, exceptions);
    }

    public RetryDriver stopOnIllegalExceptions()
    {
        return stopOn(NullPointerException.class, IllegalStateException.class, IllegalArgumentException.class);
    }

    public <V> V run(Callable<V> callable)
            throws Exception
    {
        return run(DEFAULT_CALLABLE_NAME, callable);
    }

    public <V> V run(String callableName, Callable<V> callable)
            throws Exception
    {
        checkNotNull(callableName, "callableName is null");
        checkNotNull(callable, "callable is null");

        int attempt = 0;
        while (true) {
            attempt++;
            try {
                return callable.call();
            }
            catch (Exception e) {
                for (Class<? extends Exception> clazz : exceptionWhitelist) {
                    if (clazz.isInstance(e)) {
                        throw e;
                    }
                }
                if (attempt >= maxRetryAttempts) {
                    throw e;
                }
                else {
                    log.debug("Failed on executing %s with attempt %d, will retry. Exception: %s", callableName, attempt, e.getMessage());
                }
                TimeUnit.MILLISECONDS.sleep((long) sleepTime.toMillis());
            }
        }
    }

    public <V> V runUnchecked(Callable<V> callable)
    {
        return runUnchecked(DEFAULT_CALLABLE_NAME, callable);
    }

    public <V> V runUnchecked(String callableName, Callable<V> callable)
    {
        try {
            return run(callableName, callable);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}

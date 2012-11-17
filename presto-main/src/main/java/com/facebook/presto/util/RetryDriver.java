package com.facebook.presto.util;

import com.google.common.base.Throwables;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class RetryDriver
{
    private static final Logger log = Logger.get(RetryDriver.class);
    private static final int DEFAULT_RETRY_ATTEMPTS = 10;
    private static final Duration DEFAULT_SLEEP_DURATION = Duration.valueOf("1s");

    private RetryDriver()
    {
    }

    public static <V> V runWithRetryUnchecked(Callable<V> callable)
    {
        try {
            return runWithRetry(callable);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public static <V> V runWithRetry(Callable<V> callable)
            throws Exception
    {
        return runWithRetry(callable, "<default>");
    }

    public static <V> V runWithRetryUnchecked(Callable<V> callable, int maxRetryAttempts)
    {
        try {
            return runWithRetry(callable, maxRetryAttempts);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public static <V> V runWithRetry(Callable<V> callable, int maxRetryAttempts)
            throws Exception
    {
        return runWithRetry(callable, "<default>", maxRetryAttempts);
    }

    public static <V> V runWithRetryUnchecked(Callable<V> callable, String callableName)
    {
        try {
            return runWithRetry(callable, callableName);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public static <V> V runWithRetry(Callable<V> callable, String callableName)
            throws Exception
    {
        return runWithRetry(callable, callableName, DEFAULT_RETRY_ATTEMPTS);
    }

    public static <V> V runWithRetryUnchecked(Callable<V> callable, String callableName, int maxRetryAttempts)
    {
        try {
            return runWithRetry(callable, callableName, maxRetryAttempts);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public static <V> V runWithRetry(Callable<V> callable, String callableName, int maxRetryAttempts)
            throws Exception
    {
        return runWithRetry(callable, callableName, maxRetryAttempts, DEFAULT_SLEEP_DURATION);
    }

    public static <V> V runWithRetryUnchecked(Callable<V> callable, String callableName, int maxRetryAttempts, Duration duration)
    {
        try {
            return runWithRetry(callable, callableName, maxRetryAttempts, duration);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public static <V> V runWithRetry(Callable<V> callable, String callableName, int maxRetryAttempts, Duration duration)
            throws Exception
    {
        checkNotNull(callable, "callable is null");
        checkNotNull(callableName, "callableName is null");
        checkArgument(maxRetryAttempts > 0, "maxRetryAttempts must be greater than zero");
        checkNotNull(duration, "duration is null");

        int attempt = 0;
        while (true) {
            attempt++;
            try {
                return callable.call();
            }
            catch (Exception e) {
                if (attempt >= maxRetryAttempts) {
                    throw e;
                }
                else {
                    log.debug("Failed on executing %s with attempt %d, will retry. Exception: %s", callableName, attempt, e.getMessage());
                }
                TimeUnit.MILLISECONDS.sleep((long) duration.toMillis());
            }
        }
    }
}

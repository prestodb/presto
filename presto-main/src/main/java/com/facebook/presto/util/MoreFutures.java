package com.facebook.presto.util;

import com.google.common.base.Throwables;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Preconditions.checkNotNull;

public final class MoreFutures
{
    private MoreFutures()
    {
    }

    public static <T> T tryGetUnchecked(Future<T> future)
    {
        checkNotNull(future, "future is null");
        if (!future.isDone()) {
            return null;
        }

        try {
            return future.get(0, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Throwables.propagate(e);
        }
        catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause == null) {
                cause = e;
            }
            throw Throwables.propagate(cause);

        }
        catch (TimeoutException e) {
            // this mean that isDone() does not agree with get()
            // this should not happen for reasonable implementations of Future
            throw Throwables.propagate(e);
        }
    }
}

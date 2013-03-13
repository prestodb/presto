package com.facebook.presto.util;

import com.google.common.util.concurrent.ExecutionError;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.UncheckedExecutionException;

import java.util.concurrent.ExecutionException;

public final class FutureUtils
{
    private FutureUtils() {}

    /**
     * Wait for a future to complete. This method behaves similarly to
     * {@link Futures#getUnchecked}, with the difference that it wraps
     * {@link InterruptedException} in a {@link RuntimeException} rather
     * than waiting uninterruptibly for the future to complete.
     * <p/>
     * This method is useful in conjunction with {@link Futures#successfulAsList}
     * to wait for an entire collection of futures to complete.
     */
    public static <T> T waitForFuture(ListenableFuture<T> future)
    {
        try {
            return future.get();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for future", e);
        }
        catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Error) {
                throw new ExecutionError((Error) cause);
            }
            throw new UncheckedExecutionException(cause);
        }
    }

    public static <V> ListenableFuture<?> chainedCallback(ListenableFuture<V> future, final FutureCallback<? super V> callback)
    {
        final SettableFuture<?> done = SettableFuture.create();
        Futures.addCallback(future, new FutureCallback<V>()
        {
            @Override
            public void onSuccess(V result)
            {
                try {
                    callback.onSuccess(result);
                    done.set(null);
                }
                catch (Throwable e) {
                    done.setException(e);
                }
            }

            @Override
            public void onFailure(Throwable t)
            {
                try {
                    callback.onFailure(t);
                    done.set(null);
                }
                catch (Throwable e) {
                    done.setException(e);
                }
            }
        });
        return done;
    }
}

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
package com.facebook.presto.util;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.Duration;

import javax.annotation.Nullable;

import java.lang.ref.WeakReference;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
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

    public static <T> ListenableFuture<T> addTimeout(final ListenableFuture<T> future, final Callable<T> timeoutTask, Duration timeout, ScheduledExecutorService executorService)
    {
        // if the future is already complete, just return it
        if (future.isDone()) {
            return future;
        }

        // wrap the future, so we can set the result directly
        final SettableFuture<T> settableFuture = SettableFuture.create();

        // schedule a task to complete the future when the time expires
        final ScheduledFuture<?> timeoutTaskFuture = executorService.schedule(new TimeoutFutureTask<>(settableFuture, timeoutTask, future), timeout.toMillis(), TimeUnit.MILLISECONDS);

        // add a listener to the core future, which simply updates the settable future
        Futures.addCallback(future, new FutureCallback<T>() {
            @Override
            public void onSuccess(@Nullable T result)
            {
                settableFuture.set(result);
                timeoutTaskFuture.cancel(true);
            }

            @Override
            public void onFailure(Throwable t)
            {
                settableFuture.setException(t);
                timeoutTaskFuture.cancel(true);
            }
        }, executorService);

        return settableFuture;
    }

    private static class TimeoutFutureTask<T>
            implements Runnable
    {
        private final SettableFuture<T> settableFuture;
        private final Callable<T> timeoutTask;
        private final WeakReference<Future<T>> futureReference;

        public TimeoutFutureTask(SettableFuture<T> settableFuture, Callable<T> timeoutTask, ListenableFuture<T> future)
        {
            this.settableFuture = settableFuture;
            this.timeoutTask = timeoutTask;

            // the scheduled executor can hold on to the timeout task for a long time, and
            // the future can reference large expensive objects.  Since we are only interested
            // in canceling this future on a timeout, only hold a weak reference to the future
            this.futureReference = new WeakReference<Future<T>>(future);
        }

        @Override
        public void run()
        {
            if (settableFuture.isDone()) {
                return;
            }

            // run the timeout task and set the result into the future
            try {
                T result = timeoutTask.call();
                settableFuture.set(result);
            }
            catch (Throwable t) {
                settableFuture.setException(t);
            }

            // cancel the original future, if it still exists
            Future<T> future = futureReference.get();
            if (future != null) {
                future.cancel(true);
            }
        }
    }
}

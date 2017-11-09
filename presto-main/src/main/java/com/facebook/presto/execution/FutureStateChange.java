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
package com.facebook.presto.execution;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class FutureStateChange<T>
{
    // Use a separate future for each listener so canceled listeners can be removed
    @GuardedBy("listeners")
    private final Set<StateTrackingFuture<T>> listeners = new HashSet<>();

    public ListenableFuture<T> createNewListener()
    {
        StateTrackingFuture<T> listener = StateTrackingFuture.create();
        synchronized (listeners) {
            listeners.add(listener);
        }

        // remove the listener when the future completes
        listener.addListener(
                () -> {
                    synchronized (listeners) {
                        listeners.remove(listener);
                    }
                },
                directExecutor());

        return listener;
    }

    public void complete(T newState)
    {
        fireStateChange(newState, directExecutor());
    }

    public void complete(T newState, Executor executor)
    {
        fireStateChange(newState, executor);
    }

    private void fireStateChange(T newState, Executor executor)
    {
        requireNonNull(executor, "executor is null");
        Set<StateTrackingFuture<T>> futures;
        synchronized (listeners) {
            futures = ImmutableSet.copyOf(listeners);
            listeners.clear();
        }

        for (StateTrackingFuture<T> future : futures) {
            executor.execute(() -> future.set(newState));
        }
    }

    /**
     * Future cancellation can be expensive, because when a future is canceled
     * a CancellationException is created and thrown under the hood. To get rid
     * of the cancellation overhead this future implementation just cancels the
     * future with the same exception instance.
     */
    private static final class StateTrackingFuture<V>
            extends AbstractFuture<V>
    {
        private static final CancellationException EXCEPTION = new CancellationException("Future is canceled");
        private AtomicBoolean canceled = new AtomicBoolean();

        public static <V> StateTrackingFuture<V> create()
        {
            return new StateTrackingFuture<V>();
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning)
        {
            if (canceled.compareAndSet(false, true)) {
                setException(EXCEPTION);
                return true;
            }
            return false;
        }

        @Override
        protected boolean set(@Nullable V value)
        {
            return super.set(value);
        }

        @Override
        public boolean isCancelled()
        {
            return canceled.get();
        }
    }
}

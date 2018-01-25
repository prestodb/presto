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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class FutureStateChange<T>
{
    // Use a separate future for each listener so canceled futures can be removed
    @GuardedBy("this")
    private final Set<SettableFuture<T>> futures = new HashSet<>();

    @GuardedBy("this")
    private final Set<StateChangeFuture<T>> listeners = new HashSet<>();

    public void complete(T newState)
    {
        fireStateChange(newState, directExecutor());
    }

    public void complete(T newState, Executor executor)
    {
        fireStateChange(newState, executor);
    }

    @VisibleForTesting
    synchronized int getListenerCount()
    {
        return listeners.size();
    }

    private void fireStateChange(T newState, Executor executor)
    {
        requireNonNull(executor, "executor is null");
        Set<SettableFuture<T>> futures;
        Set<StateChangeFuture<T>> listeners;
        synchronized (this) {
            futures = ImmutableSet.copyOf(this.futures);
            this.futures.clear();
            listeners = ImmutableSet.copyOf(this.listeners);
            this.listeners.clear();
        }

        for (SettableFuture<T> future : futures) {
            executor.execute(() -> future.set(newState));
        }
        for (StateChangeFuture<T> listener : listeners) {
            executor.execute(() -> listener.set(newState));
        }
    }

    public ListenableFuture<T> newListenableFuture()
    {
        SettableFuture<T> future = SettableFuture.create();
        synchronized (FutureStateChange.this) {
            futures.add(future);
        }

        // remove the future when it transitions to done state
        future.addListener(
                () -> {
                    synchronized (FutureStateChange.this) {
                        futures.remove(future);
                    }
                },
                directExecutor());

        return future;
    }

    public StateChangeFuture<T> newStateChangeFuture()
    {
        StateChangeFuture<T> newListener = new StateChangeFuture<>(listener -> {
            synchronized (this) {
                listeners.remove(listener);
            }
        });
        synchronized (this) {
            listeners.add(newListener);
        }
        return newListener;
    }

    /**
     * This class is similar to ListenableFuture, but with a notable difference.
     * Cancellation will not be propagated to its callbacks (listener as in ListenableFuture).
     * <p>
     * This is distinctly different from wrapping a ListenableFuture with NonCancellationPropagatingFuture.
     * Say, you have `f1`, and you get `f2` by wrapping it with `NonCancellationPropagatingFuture`.
     * When you cancel `f2`, `f1` will not be cancelled. When you cancel `f1`, `f2` will be cancelled.
     * This class is the opposite. Even if you hold a reference to this object, there is no way you can observe its cancellation.
     * The only one that will be notified of its cancellation is whoever constructed it, to facilitate resource clean up.
     * <p>
     * We want to avoid notifying observers of the cancellation because it's expensive.
     * In AbstractFuture implementation, when future is canceled, at least two CancellationException is created.
     * Even if you managed to turn cancellation into a pre-constructed failure, you will only be able to avoid the first CancellationException.
     * The second CancellationException will become an ExecutionException.
     */
    public static class StateChangeFuture<T>
    {
        private final Consumer<StateChangeFuture> unsubscribe;

        private final List<Consumer<? super T>> callbacks = new ArrayList<>();
        private State state = State.PENDING;
        private T value;

        StateChangeFuture(Consumer<StateChangeFuture> unsubscribe)
        {
            this.unsubscribe = requireNonNull(unsubscribe, "unsubscribe is null");
        }

        /**
         * Add a {@link Consumer} which will be notified whenever the state changes.
         * The callback is NOT removed automatically when triggered, and may be executed again.
         */
        public void addCallback(Consumer<? super T> callback)
        {
            synchronized (this) {
                switch (state) {
                    case PENDING:
                        callbacks.add(callback);
                        return;
                    case COMPLETED:
                        break;
                    case CANCELLED:
                        return;
                }
            }
            // if state was originally COMPLETED
            callback.accept(value);
        }

        private void set(T value)
        {
            List<Consumer<? super T>> callbacks;
            synchronized (this) {
                switch (state) {
                    case PENDING:
                        this.state = State.COMPLETED;
                        this.value = value;
                        callbacks = ImmutableList.copyOf(this.callbacks);
                        break;
                    case COMPLETED:
                        return;
                    case CANCELLED:
                        return;
                    default:
                        throw new UnsupportedOperationException();
                }
            }
            // if state was originally PENDING
            unsubscribe.accept(this);
            for (Consumer<? super T> callback : callbacks) {
                callback.accept(value);
            }
        }

        private void cancel()
        {
            synchronized (this) {
                switch (state) {
                    case PENDING:
                        state = State.CANCELLED;
                        callbacks.clear();
                        break;
                    case COMPLETED:
                    case CANCELLED:
                        return;
                }
            }
            // if state was originally PENDING
            unsubscribe.accept(this);
        }

        public boolean isCompleted()
        {
            synchronized (this) {
                switch (state) {
                    case PENDING:
                        return false;
                    case COMPLETED:
                    case CANCELLED:
                        return true;
                    default:
                        throw new UnsupportedOperationException();
                }
            }
        }

        public static <T> StateChangeFuture<T> completedStateChangeFuture(T value)
        {
            StateChangeFuture<T> result = new StateChangeFuture<>(ignored -> {});
            result.set(value);
            return result;
        }

        /**
         * Wait for any in the listeners to fire, and cancel all others to free up resources
         */
        public static <T> ListenableFuture<T> getFirstCompleteAndCancelOthers(List<? extends StateChangeFuture<? extends T>> listeners)
        {
            SettableFuture<T> future = SettableFuture.create();

            Consumer<T> consumer = value -> {
                // Make sure consumer callback runs only once.
                // Otherwise, getFirstCompleteAndCancelOthers could be O(N^2) where N is #listeners.
                if (!future.set(value)) {
                    return;
                }
                // It is harmless if a listener is cancelled before this consumer is added to its callbacks.
                for (StateChangeFuture<? extends T> listener : listeners) {
                    listener.cancel();
                }
            };

            for (StateChangeFuture<? extends T> listener : listeners) {
                listener.addCallback(consumer);
            }

            return future;
        }

        private enum State
        {
            PENDING,
            COMPLETED,
            CANCELLED,
        }
    }
}

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

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executor;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Sets.newIdentityHashSet;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Simple state machine which holds a single value.  Callers can register for
 * state change events, and can wait for the state to change.
 */
@ThreadSafe
public class StateMachine<T>
{
    private static final Logger log = Logger.get(StateMachine.class);

    private final String name;
    private final Executor executor;

    @GuardedBy("this")
    private volatile T state;

    @GuardedBy("this")
    private final List<StateChangeListener<T>> stateChangeListeners = new ArrayList<>();

    @GuardedBy("this")
    private final Set<SettableFuture<T>> futureStateChanges = newIdentityHashSet();

    /**
     * Creates a state machine with the specified initial value
     *
     * @param name name of this state machine to use in debug statements
     * @param executor executor for firing state change events; must not be a same thread executor
     * @param initialState the initial value
     */
    public StateMachine(String name, Executor executor, T initialState)
    {
        this.name = checkNotNull(name, "name is null");
        this.executor = checkNotNull(executor, "executor is null");
        this.state = checkNotNull(initialState, "initialState is null");
    }

    public T get()
    {
        return state;
    }

    public ListenableFuture<T> getStateChange(T currentState)
    {
        checkState(!Thread.holdsLock(this), "Can not wait for state change while holding a lock on this");

        synchronized (this) {
            if (!Objects.equals(state, currentState)) {
                return Futures.immediateFuture(state);
            }

            SettableFuture<T> futureStateChange = SettableFuture.create();
            futureStateChanges.add(futureStateChange);
            Futures.addCallback(futureStateChange, new FutureCallback<T>() {
                @Override
                public void onSuccess(T result)
                {
                    // no-op. The futureStateChanges list is already cleared before fireStateChanged is called.
                }

                @Override
                public void onFailure(Throwable t)
                {
                    // Remove the Future early, in case it's cancelled.
                    synchronized (StateMachine.this) {
                        futureStateChanges.remove(futureStateChange);
                    }
                }
            });
            return futureStateChange;
        }
    }

    /**
     * Sets the state.
     * If the new state does not {@code .equals()} the current state, listeners and waiters will be notified.
     *
     * @return the old state
     */
    public T set(T newState)
    {
        checkState(!Thread.holdsLock(this), "Can not set state while holding a lock on this");
        checkNotNull(newState, "newState is null");

        T oldState;
        ImmutableList<SettableFuture<T>> futureStateChanges;
        ImmutableList<StateChangeListener<T>> stateChangeListeners;
        synchronized (this) {
            if (Objects.equals(state, newState)) {
                return state;
            }

            oldState = state;
            state = newState;

            futureStateChanges = ImmutableList.copyOf(this.futureStateChanges);
            this.futureStateChanges.clear();
            stateChangeListeners = ImmutableList.copyOf(this.stateChangeListeners);
            this.notifyAll();
        }

        fireStateChanged(newState, futureStateChanges, stateChangeListeners);
        return oldState;
    }

    /**
     * Sets the state if the current state satisfies the specified predicate.
     * If the new state does not {@code .equals()} the current state, listeners and waiters will be notified.
     *
     * @return the old state
     */
    public boolean setIf(T newValue, Predicate<T> predicate)
    {
        checkState(!Thread.holdsLock(this), "Can not set state while holding a lock on this");

        while (true) {
            // check if the current state passes the predicate
            T currentState = get();
            // do not call back while holding a lock on this
            if (!predicate.apply(currentState)) {
                return false;
            }

            // if state did not change while, checking the predicate, apply the new state
            if (compareAndSet(currentState, newValue)) {
                return true;
            }
        }
    }

    /**
     * Sets the state if the current state {@code .equals()} the specified expected state.
     * If the new state does not {@code .equals()} the current state, listeners and waiters will be notified.
     *
     * @return the old state
     */
    public boolean compareAndSet(T expectedState, T newState)
    {
        checkState(!Thread.holdsLock(this), "Can not set state while holding a lock on this");
        checkNotNull(expectedState, "expectedState is null");
        checkNotNull(newState, "newState is null");

        ImmutableList<SettableFuture<T>> futureStateChanges;
        ImmutableList<StateChangeListener<T>> stateChangeListeners;
        synchronized (this) {
            if (!Objects.equals(state, expectedState)) {
                return false;
            }

            if (Objects.equals(state, newState)) {
                // successfully changed to the same state, no need to notify
                return true;
            }

            state = newState;

            futureStateChanges = ImmutableList.copyOf(this.futureStateChanges);
            this.futureStateChanges.clear();
            stateChangeListeners = ImmutableList.copyOf(this.stateChangeListeners);
            this.notifyAll();
        }

        fireStateChanged(newState, futureStateChanges, stateChangeListeners);
        return true;
    }

    private void fireStateChanged(T newState, List<SettableFuture<T>> futureStateChanges, List<StateChangeListener<T>> stateChangeListeners)
    {
        checkState(!Thread.holdsLock(this), "Can not fire state change event while holding a lock on this");

        executor.execute(() -> {
            checkState(!Thread.holdsLock(this), "Can not notify while holding a lock on this");
            for (SettableFuture<T> futureStateChange : futureStateChanges) {
                try {
                    futureStateChange.set(newState);
                }
                catch (Throwable e) {
                    log.error(e, "Error setting future state for %s", name);
                }
            }
            for (StateChangeListener<T> stateChangeListener : stateChangeListeners) {
                try {
                    stateChangeListener.stateChanged(newState);
                }
                catch (Throwable e) {
                    log.error(e, "Error notifying state change listener for %s", name);
                }
            }
        });
    }

    /**
     * Adds a listener to be notified when the state instance changes according to {@code .equals()}.
     */
    public synchronized void addStateChangeListener(StateChangeListener<T> stateChangeListener)
    {
        stateChangeListeners.add(stateChangeListener);
    }

    /**
     * Wait for the state to not be {@code .equals()} to the specified current state.
     */
    public Duration waitForStateChange(T currentState, Duration maxWait)
            throws InterruptedException
    {
        checkState(!Thread.holdsLock(this), "Can not wait for state change while holding a lock on this");

        if (!Objects.equals(state, currentState)) {
            return maxWait;
        }

        // wait for task state to change
        long remainingNanos = maxWait.roundTo(NANOSECONDS);
        long start = System.nanoTime();
        long end = start + remainingNanos;

        synchronized (this) {
            while (remainingNanos > 0 && Objects.equals(state, currentState)) {
                // wait for timeout or notification
                NANOSECONDS.timedWait(this, remainingNanos);
                remainingNanos = end - System.nanoTime();
            }
        }
        if (remainingNanos < 0) {
            remainingNanos = 0;
        }
        return new Duration(remainingNanos, NANOSECONDS);
    }

    public interface StateChangeListener<T>
    {
        void stateChanged(T newValue);
    }

    @Override
    public String toString()
    {
        return String.valueOf(get());
    }
}

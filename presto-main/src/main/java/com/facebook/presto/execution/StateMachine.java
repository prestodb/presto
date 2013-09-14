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
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Simple state machine which holds a single value.  Callers can register for
 * state change events, and can wait for the state to change.
 */
public class StateMachine<T>
{
    private static final Logger log = Logger.get(StateMachine.class);

    private final String name;
    private final Executor executor;

    @Nullable
    @GuardedBy("this")
    private T state;

    @GuardedBy("this")
    private final List<StateChangeListener<T>> stateChangeListeners = new ArrayList<>();

    /**
     * Creates a state machine with the specified initial value
     *
     * @param name name of this state machine to use in debug statements
     * @param executor executor for firing state change events; must not be a same thread executor
     * @param initialState the initial value
     */
    public StateMachine(String name, Executor executor, @Nullable T initialState)
    {
        this.name = checkNotNull(name, "name is null");
        this.executor = checkNotNull(executor, "executor is null");
        this.state = initialState;
    }

    @Nullable
    public synchronized T get()
    {
        return state;
    }

    /**
     * Sets the state.
     * If the new state does not {@code ==} the current state, listeners and waiters will be notified.
     *
     * @return the old state
     */
    public T set(final T newState)
    {
        checkState(!Thread.holdsLock(this), "Can not set state while holding a lock on this");

        T oldState;
        ImmutableList<StateChangeListener<T>> stateChangeListeners;
        synchronized (this) {
            if (state == newState) {
                return state;
            }

            oldState = state;
            state = newState;

            stateChangeListeners = ImmutableList.copyOf(this.stateChangeListeners);
            this.notifyAll();
        }

        fireStateChanged(newState, stateChangeListeners);
        return oldState;
    }

    /**
     * Sets the state if the current state satisfies the specified predicate.
     * If the new state does not {@code ==} the current state, listeners and waiters will be notified.
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
     * Sets the state if the current state {@code ==} the specified expected state.
     * If the new state does not {@code ==} the current state, listeners and waiters will be notified.
     *
     * @return the old state
     */
    public boolean compareAndSet(T expectedState, T newState)
    {
        checkState(!Thread.holdsLock(this), "Can not set state while holding a lock on this");

        ImmutableList<StateChangeListener<T>> stateChangeListeners;
        synchronized (this) {
            if (state != expectedState) {
                return false;
            }

            if (state == newState) {
                // successfully changed to the same state, no need to notify
                return true;
            }

            state = newState;

            stateChangeListeners = ImmutableList.copyOf(this.stateChangeListeners);
            this.notifyAll();
        }

        fireStateChanged(newState, stateChangeListeners);
        return true;
    }

    private void fireStateChanged(final T newState, final ImmutableList<StateChangeListener<T>> stateChangeListeners)
    {
        executor.execute(new Runnable()
        {
            @Override
            public void run()
            {
                checkState(!Thread.holdsLock(StateMachine.this), "Can not notify while holding a lock on this");
                for (StateChangeListener<T> stateChangeListener : stateChangeListeners) {
                    try {
                        stateChangeListener.stateChanged(newState);
                    }
                    catch (Throwable e) {
                        log.error(e, "Error notifying state change listener for %s", name);
                    }
                }
            }
        });
    }

    /**
     * Adds a listener to be notified when the state instance changes according to {@code ==}.
     */
    public synchronized void addStateChangeListener(StateChangeListener<T> stateChangeListener)
    {
        stateChangeListeners.add(stateChangeListener);
    }

    /**
     * Wait for the state to not be {@code ==} to the specified current state.
     */
    public Duration waitForStateChange(T currentState, Duration maxWait)
            throws InterruptedException
    {
        checkState(!Thread.holdsLock(this), "Can not wait for state change while holding a lock on this");

        if (state != currentState) {
            return maxWait;
        }

        // wait for task state to change
        long remainingNanos = maxWait.roundTo(NANOSECONDS);
        long start = System.nanoTime();
        long end = start + remainingNanos;

        synchronized (this) {
            while (remainingNanos > 0 && state == currentState) {
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

    public static interface StateChangeListener<T>
    {
        public void stateChanged(T newValue);
    }

    @Override
    public String toString()
    {
        return String.valueOf(get());
    }
}

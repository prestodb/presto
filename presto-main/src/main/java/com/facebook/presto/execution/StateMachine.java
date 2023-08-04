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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.PrestoException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static com.facebook.presto.spi.StandardErrorCode.SERVER_SHUTTING_DOWN;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.util.Objects.requireNonNull;

/**
 * Simple state machine which holds a single state. Callers can register for state change events.
 */
@ThreadSafe
public class StateMachine<T>
{
    private static final Logger log = Logger.get(StateMachine.class);

    private final String name;
    private final Executor executor;
    private final Object lock = new Object();
    private final Set<T> terminalStates;

    @GuardedBy("lock")
    private volatile T state;

    @GuardedBy("lock")
    private final List<StateChangeListener<T>> stateChangeListeners = new ArrayList<>();

    private final AtomicReference<FutureStateChange<T>> futureStateChange = new AtomicReference<>(new FutureStateChange<>());

    /**
     * Creates a state machine with the specified initial state and no terminal states.
     *
     * @param name name of this state machine to use in debug statements
     * @param executor executor for firing state change events; must not be a same thread executor
     * @param initialState the initial state
     */
    public StateMachine(String name, Executor executor, T initialState)
    {
        this(name, executor, initialState, ImmutableSet.of());
    }

    /**
     * Creates a state machine with the specified initial state and terminal states.
     *
     * @param name name of this state machine to use in debug statements
     * @param executor executor for firing state change events; must not be a same thread executor
     * @param initialState the initial state
     * @param terminalStates the terminal states
     */
    public StateMachine(String name, Executor executor, T initialState, Iterable<T> terminalStates)
    {
        this.name = requireNonNull(name, "name is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.state = requireNonNull(initialState, "initialState is null");
        this.terminalStates = ImmutableSet.copyOf(requireNonNull(terminalStates, "terminalStates is null"));
    }

    // state changes are atomic and state is volatile, so a direct read is safe here
    @SuppressWarnings("FieldAccessNotGuarded")
    public T get()
    {
        return state;
    }

    /**
     * Sets the state.
     * If the new state does not {@code .equals()} the current state, listeners and waiters will be notified.
     *
     * @return the old state
     */
    public T set(T newState)
    {
        checkState(!Thread.holdsLock(lock), "Can not set state while holding the lock");
        requireNonNull(newState, "newState is null");

        T oldState;
        FutureStateChange<T> futureStateChange;
        ImmutableList<StateChangeListener<T>> stateChangeListeners;
        synchronized (lock) {
            if (state.equals(newState)) {
                return state;
            }

            checkState(!isTerminalState(state), "%s can not transition from %s to %s", name, state, newState);

            oldState = state;
            state = newState;

            futureStateChange = this.futureStateChange.getAndSet(new FutureStateChange<>());
            stateChangeListeners = ImmutableList.copyOf(this.stateChangeListeners);

            // if we are now in a terminal state, free the listeners since this will be the last notification
            if (isTerminalState(state)) {
                this.stateChangeListeners.clear();
            }
        }

        fireStateChanged(newState, futureStateChange, stateChangeListeners);
        return oldState;
    }

    /**
     * Sets the state if the current state satisfies the specified predicate.
     * If the new state does not {@code .equals()} the current state, listeners and waiters will be notified.
     *
     * @return true if the state is set
     */
    public boolean setIf(T newState, Predicate<T> predicate)
    {
        checkState(!Thread.holdsLock(lock), "Can not set state while holding the lock");
        requireNonNull(newState, "newState is null");

        while (true) {
            // check if the current state passes the predicate
            T currentState = get();

            // change to same state is not a change, and does not notify the notify listeners
            if (currentState.equals(newState)) {
                return false;
            }

            // do not call predicate while holding the lock
            if (!predicate.test(currentState)) {
                return false;
            }

            // if state did not change while, checking the predicate, apply the new state
            if (compareAndSet(currentState, newState)) {
                return true;
            }
        }
    }

    /**
     * Sets the state if the current state {@code .equals()} the specified expected state.
     * If the new state does not {@code .equals()} the current state, listeners and waiters will be notified.
     *
     * @return true if the state is set
     */
    public boolean compareAndSet(T expectedState, T newState)
    {
        checkState(!Thread.holdsLock(lock), "Can not set state while holding the lock");
        requireNonNull(expectedState, "expectedState is null");
        requireNonNull(newState, "newState is null");

        FutureStateChange<T> futureStateChange;
        ImmutableList<StateChangeListener<T>> stateChangeListeners;
        synchronized (lock) {
            if (!state.equals(expectedState)) {
                return false;
            }

            // change to same state is not a change, and does not notify the listeners
            if (state.equals(newState)) {
                return false;
            }

            checkState(!isTerminalState(state), "%s can not transition from %s to %s", name, state, newState);

            state = newState;

            futureStateChange = this.futureStateChange.getAndSet(new FutureStateChange<>());
            stateChangeListeners = ImmutableList.copyOf(this.stateChangeListeners);

            // if we are now in a terminal state, free the listeners since this will be the last notification
            if (isTerminalState(state)) {
                this.stateChangeListeners.clear();
            }
        }

        fireStateChanged(newState, futureStateChange, stateChangeListeners);
        return true;
    }

    private void fireStateChanged(T newState, FutureStateChange<T> futureStateChange, List<StateChangeListener<T>> stateChangeListeners)
    {
        checkState(!Thread.holdsLock(lock), "Can not fire state change event while holding the lock");
        requireNonNull(newState, "newState is null");

        // always fire listener callbacks from a different thread
        safeExecute(() -> {
            checkState(!Thread.holdsLock(lock), "Can not notify while holding the lock");
            try {
                futureStateChange.complete(newState);
            }
            catch (Throwable e) {
                log.error(e, "Error setting future state for %s", name);
            }
            for (StateChangeListener<T> stateChangeListener : stateChangeListeners) {
                fireStateChangedListener(newState, stateChangeListener);
            }
        });
    }

    private void fireStateChangedListener(T newState, StateChangeListener<T> stateChangeListener)
    {
        try {
            stateChangeListener.stateChanged(newState);
        }
        catch (Throwable e) {
            log.error(e, "Error notifying state change listener for %s", name);
        }
    }

    /**
     * Gets a future that completes when the state is no longer {@code .equals()} to {@code currentState)}.
     */
    public ListenableFuture<T> getStateChange(T currentState)
    {
        checkState(!Thread.holdsLock(lock), "Can not wait for state change while holding the lock");
        requireNonNull(currentState, "currentState is null");

        synchronized (lock) {
            // return a completed future if the state has already changed, or we are in a terminal state
            if (!state.equals(currentState) || isTerminalState(state)) {
                return immediateFuture(state);
            }

            return futureStateChange.get().createNewListener();
        }
    }

    /**
     * Adds a listener to be notified when the state instance changes according to {@code .equals()}.
     * Listener is always notified asynchronously using a dedicated notification thread pool so, care should
     * be taken to avoid leaking {@code this} when adding a listener in a constructor. Additionally, it is
     * possible notifications are observed out of order due to the asynchronous execution. The listener is
     * immediately notified immediately of the current state.
     */
    public void addStateChangeListener(StateChangeListener<T> stateChangeListener)
    {
        requireNonNull(stateChangeListener, "stateChangeListener is null");

        boolean inTerminalState;
        T currentState;
        synchronized (lock) {
            currentState = state;
            inTerminalState = isTerminalState(currentState);
            if (!inTerminalState) {
                stateChangeListeners.add(stateChangeListener);
            }
        }

        // fire state change listener with the current state
        // always fire listener callbacks from a different thread
        safeExecute(() -> stateChangeListener.stateChanged(currentState));
    }

    @VisibleForTesting
    boolean isTerminalState(T state)
    {
        return terminalStates.contains(state);
    }

    @VisibleForTesting
    List<StateChangeListener<T>> getStateChangeListeners()
    {
        synchronized (lock) {
            return ImmutableList.copyOf(stateChangeListeners);
        }
    }

    public interface StateChangeListener<T>
    {
        void stateChanged(T newState);
    }

    @Override
    public String toString()
    {
        return get().toString();
    }

    private void safeExecute(Runnable command)
    {
        try {
            executor.execute(command);
        }
        catch (RejectedExecutionException e) {
            if ((executor instanceof ExecutorService) && ((ExecutorService) executor).isShutdown()) {
                throw new PrestoException(SERVER_SHUTTING_DOWN, "Server is shutting down", e);
            }
            throw e;
        }
    }
}

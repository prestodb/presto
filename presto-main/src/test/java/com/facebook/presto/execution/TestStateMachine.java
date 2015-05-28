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

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.Duration;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestStateMachine
{
    private enum State
    {
        BREAKFAST, LUNCH, DINNER
    }

    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("test-%s"));

    @AfterClass
    public void tearDown()
            throws Exception
    {
        executor.shutdownNow();
    }

    @Test
    public void testNullState()
            throws Exception
    {
        try {
            new StateMachine<>("test", executor, null);
            fail("expected a NullPointerException");
        }
        catch (NullPointerException exception) {
        }

        StateMachine<State> stateMachine = new StateMachine<>("test", executor, State.BREAKFAST);

        assertNoStateChange(stateMachine, () -> {
            try {
                stateMachine.set(null);
                fail("expected a NullPointerException");
            }
            catch (NullPointerException exception) {
            }
        });

        assertNoStateChange(stateMachine, () -> {
            try {
                stateMachine.compareAndSet(State.BREAKFAST, null);
                fail("expected a NullPointerException");
            }
            catch (NullPointerException exception) {
            }
        });

        assertNoStateChange(stateMachine, () -> {
            try {
                stateMachine.compareAndSet(State.LUNCH, null);
                fail("expected a NullPointerException");
            }
            catch (NullPointerException exception) {
            }
        });

        assertNoStateChange(stateMachine, () -> {
            try {
                stateMachine.setIf(null, currentState -> true);
                fail("expected a NullPointerException");
            }
            catch (NullPointerException exception) {
            }
        });

        assertNoStateChange(stateMachine, () -> {
            try {
                stateMachine.setIf(null, currentState -> false);
                fail("expected a NullPointerException");
            }
            catch (NullPointerException exception) {
            }
        });
    }

    @Test
    public void testSet()
            throws Exception
    {
        StateMachine<State> stateMachine = new StateMachine<>("test", executor, State.BREAKFAST);
        assertEquals(stateMachine.get(), State.BREAKFAST);

        assertNoStateChange(stateMachine, () -> assertEquals(stateMachine.set(State.BREAKFAST), State.BREAKFAST));

        assertStateChange(stateMachine, () -> assertEquals(stateMachine.set(State.LUNCH), State.BREAKFAST), State.LUNCH);

        assertStateChange(stateMachine, () -> assertEquals(stateMachine.set(State.BREAKFAST), State.LUNCH), State.BREAKFAST);
    }

    @Test
    public void testCompareAndSet()
            throws Exception
    {
        StateMachine<State> stateMachine = new StateMachine<>("test", executor, State.BREAKFAST);
        assertEquals(stateMachine.get(), State.BREAKFAST);

        // no match with new state
        assertNoStateChange(stateMachine, () -> stateMachine.compareAndSet(State.DINNER, State.LUNCH));

        // match with new state
        assertStateChange(stateMachine,
                () -> stateMachine.compareAndSet(State.BREAKFAST, State.LUNCH),
                State.LUNCH);

        // no match with same state
        assertNoStateChange(stateMachine, () -> stateMachine.compareAndSet(State.BREAKFAST, State.LUNCH));

        // match with same state
        assertNoStateChange(stateMachine, () -> stateMachine.compareAndSet(State.LUNCH, State.LUNCH));
    }

    @Test
    public void testSetIf()
            throws Exception
    {
        StateMachine<State> stateMachine = new StateMachine<>("test", executor, State.BREAKFAST);
        assertEquals(stateMachine.get(), State.BREAKFAST);

        // false predicate with new state
        assertNoStateChange(stateMachine,
                () -> assertFalse(stateMachine.setIf(State.LUNCH, currentState -> {
                    assertEquals(currentState, State.BREAKFAST);
                    return false;
                })));

        // true predicate with new state
        assertStateChange(stateMachine,
                () -> assertTrue(stateMachine.setIf(State.LUNCH, currentState -> {
                    assertEquals(currentState, State.BREAKFAST);
                    return true;
                })),
                State.LUNCH);

        // false predicate with same state
        assertNoStateChange(stateMachine,
                () -> assertFalse(stateMachine.setIf(State.LUNCH, currentState -> {
                    assertEquals(currentState, State.LUNCH);
                    return false;
                })));

        // true predicate with same state
        assertNoStateChange(stateMachine,
                () -> assertFalse(stateMachine.setIf(State.LUNCH, currentState -> {
                    assertEquals(currentState, State.LUNCH);
                    return true;
                })));
    }

    private void assertStateChange(StateMachine<State> stateMachine, StateChanger stateChange, State expectedState)
            throws Exception
    {
        State initialState = stateMachine.get();
        ListenableFuture<State> futureChange = stateMachine.getStateChange(initialState);

        SettableFuture<State> listenerChange = SettableFuture.create();
        stateMachine.addStateChangeListener(listenerChange::set);

        Future<State> waitChange = executor.submit(() -> {
            try {
                stateMachine.waitForStateChange(initialState, new Duration(10, SECONDS));
                return stateMachine.get();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw Throwables.propagate(e);
            }
            catch (Exception e) {
                throw Throwables.propagate(e);
            }
        });

        stateChange.run();

        assertEquals(stateMachine.get(), expectedState);

        assertEquals(futureChange.get(1, SECONDS), expectedState);
        assertEquals(listenerChange.get(1, SECONDS), expectedState);
        assertEquals(waitChange.get(1, SECONDS), expectedState);
    }

    private void assertNoStateChange(StateMachine<State> stateMachine, StateChanger stateChange)
            throws Exception
    {
        State initialState = stateMachine.get();
        ListenableFuture<State> futureChange = stateMachine.getStateChange(initialState);

        SettableFuture<State> listenerChange = SettableFuture.create();
        stateMachine.addStateChangeListener(listenerChange::set);

        Future<State> waitChange = executor.submit(() -> {
            try {
                stateMachine.waitForStateChange(initialState, new Duration(10, SECONDS));
                return stateMachine.get();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw Throwables.propagate(e);
            }
        });

        stateChange.run();

        assertEquals(stateMachine.get(), initialState);

        try {
            // none of the futures should finish, but there is no way to prove that
            // the state change is not happening (the changes happen in another thread),
            // so we wait a short time for nothing to happen.
            futureChange.get(50, MILLISECONDS);
        }
        catch (InterruptedException e) {
            throw e;
        }
        catch (Exception ignored) {
        }

        assertFalse(futureChange.isDone());
        futureChange.cancel(true);
        assertFalse(listenerChange.isDone());
        listenerChange.cancel(true);
        assertFalse(waitChange.isDone());
        waitChange.cancel(true);
    }

    private interface StateChanger
    {
        void run()
                throws Exception;
    }
}

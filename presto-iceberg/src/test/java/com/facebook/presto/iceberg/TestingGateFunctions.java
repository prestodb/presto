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
package com.facebook.presto.iceberg;

import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

/**
 * Provides a latch-based synchronization mechanism exposed as a SQL scalar function ({@code await_gate(VARCHAR) -> BOOLEAN})
 * for deterministically coordinating concurrent SQL queries in tests.
 * <p>
 * <b>Goal:</b>
 * Testing concurrency, optimistic concurrency control (such as Iceberg snapshot isolation), and query retry behavior
 * requires deterministic control over when concurrent queries read metadata, process rows, and commit changes.
 * {@code TestingGateFunctions} allows a test to pause a SQL query in the middle of row evaluation—after query planning,
 * transaction creation, and table snapshot resolution have already completed, but before the query finishes execution
 * and commits—so that the test thread can perform conflicting or concurrent actions at a precise point in the query lifecycle.
 * <p>
 * <b>How It Works:</b>
 * <ul>
 *   <li>{@link TestingGatePlugin} registers the non-deterministic SQL scalar function {@code await_gate(VARCHAR) -> BOOLEAN}.
 *       Because it is marked {@code deterministic = false}, the Presto optimizer does not constant-fold or eliminate it
 *       during planning; instead, worker threads evaluate it at runtime while processing rows.</li>
 *   <li>Each named {@link Gate} is backed by two {@link CountDownLatch} instances:
 *       {@code arrived} (signaled when a query first evaluates {@code await_gate} for that gate) and
 *       {@code released} (awaited by the query thread until the test explicitly releases the gate).</li>
 *   <li>Always returning {@code true}, {@code await_gate('gate_name')} can be conjoined onto any {@code WHERE} predicate
 *       without altering which rows match.</li>
 *   <li>Once {@link Gate#release()} is called—or if the gate has been removed via {@link #removeGate(String)}—subsequent
 *       evaluations of {@code await_gate} for that gate name return {@code true} immediately without blocking. This ensures
 *       that queries scanning multiple rows or queries automatically retried by Presto after a commit conflict do not block
 *       a second time.</li>
 * </ul>
 * <p>
 * <b>How to use:</b>
 * <ol>
 *   <li>Register {@link TestingGatePlugin} on the {@code QueryRunner}:
 *       <pre>{@code
 *       queryRunner.installPlugin(new TestingGateFunctions.TestingGatePlugin());
 *       }</pre>
 *   </li>
 *   <li>Create a uniquely named {@link Gate} before submitting the query to be paused:
 *       <pre>{@code
 *       String gateName = "my_test_gate";
 *       TestingGateFunctions.Gate gate = TestingGateFunctions.createGate(gateName);
 *       }</pre>
 *   </li>
 *   <li>Submit the SQL statement asynchronously (for example, on an {@code ExecutorService}) with {@code await_gate}
 *       in its {@code WHERE} clause or projection:
 *       <pre>{@code
 *       Future<?> queryFuture = executor.submit(() ->
 *               queryRunner.execute(session, "UPDATE my_table SET x = x + 1 WHERE id = 1 AND await_gate('my_test_gate')"));
 *       }</pre>
 *   </li>
 *   <li>Wait for the query to reach the gate during execution:
 *       <pre>{@code
 *       gate.awaitArrival(30, TimeUnit.SECONDS);
 *       }</pre>
 *   </li>
 *   <li>Perform the concurrent action while the first query is guaranteed to be in-flight (e.g., execute a conflicting
 *       DML/DDL statement, inspect coordinator/query state, test lock contention, or trigger query cancellation).</li>
 *   <li>Release and remove the gate in {@code finally} blocks, then await the background query result:
 *       <pre>{@code
 *       try {
 *           gate.awaitArrival(30, TimeUnit.SECONDS);
 *           // execute concurrent operations here
 *       }
 *       finally {
 *           gate.release();
 *           TestingGateFunctions.removeGate(gateName);
 *       }
 *       queryFuture.get();
 *       }</pre>
 *   </li>
 * </ol>
 */
public final class TestingGateFunctions
{
    private static final Map<String, Gate> GATES = new ConcurrentHashMap<>();

    private TestingGateFunctions() {}

    /**
     * Creates and registers a new {@link Gate} with the specified unique name.
     *
     * @param name unique identifier referenced by {@code await_gate('<name>')} in SQL
     * @return the newly created {@link Gate}
     * @throws IllegalStateException if a gate with the same name is already registered
     */
    public static Gate createGate(String name)
    {
        Gate gate = new Gate(name);
        if (GATES.putIfAbsent(name, gate) != null) {
            throw new IllegalStateException("Gate already exists: " + name);
        }
        return gate;
    }

    /**
     * Unregisters the gate with the specified name so its resources can be reclaimed and the name can be reused.
     * If a query evaluates {@code await_gate} for a removed gate, the function returns {@code true} immediately.
     *
     * @param name name of the gate to remove
     */
    public static void removeGate(String name)
    {
        GATES.remove(name);
    }

    /**
     * SQL scalar function {@code await_gate(VARCHAR) -> BOOLEAN} that notifies the named {@link Gate} of arrival
     * and blocks the executing worker thread until the gate is released. If no gate is registered under {@code name},
     * returns {@code true} immediately without blocking.
     *
     * @param name the gate name passed from SQL
     * @return always {@code true} so it can be used transparently in boolean predicates
     */
    @ScalarFunction(value = "await_gate", deterministic = false)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean awaitGate(@SqlType(StandardTypes.VARCHAR) Slice name)
    {
        Gate gate = GATES.get(name.toStringUtf8());
        if (gate != null) {
            gate.arriveAndAwait();
        }
        return true;
    }

    /**
     * A two-way synchronization barrier between a SQL query thread executing {@code await_gate} and a test control thread.
     */
    public static final class Gate
    {
        private final String name;
        private final CountDownLatch arrived = new CountDownLatch(1);
        private final CountDownLatch released = new CountDownLatch(1);

        private Gate(String name)
        {
            this.name = requireNonNull(name, "name is null");
        }

        /**
         * Signals that a SQL query thread has reached this gate by counting down {@code arrived},
         * and then blocks the calling thread for up to 60 seconds until {@link #release()} is invoked.
         * If the gate has already been released, this method returns immediately without blocking.
         *
         * @throws IllegalStateException if the gate is not released within 60 seconds
         * @throws RuntimeException wrapping {@link InterruptedException} if the thread is interrupted while waiting
         */
        private void arriveAndAwait()
        {
            arrived.countDown();
            try {
                if (!released.await(60, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Timed out waiting for gate " + name + " to be released");
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        /**
         * Blocks the calling test thread until a SQL query evaluates {@code await_gate} for this gate's name.
         *
         * @param timeout maximum time to wait for a query to reach the gate
         * @param unit time unit of the {@code timeout} argument
         * @throws InterruptedException if the current thread is interrupted while waiting
         * @throws IllegalStateException if the timeout elapses before any query reaches the gate
         */
        public void awaitArrival(long timeout, TimeUnit unit)
                throws InterruptedException
        {
            if (!arrived.await(timeout, unit)) {
                throw new IllegalStateException("Timed out waiting for gate " + name + " to be reached");
            }
        }

        /**
         * Releases this gate, unblocking any SQL query threads currently waiting in {@code await_gate} and allowing
         * any subsequent calls on this gate to pass through immediately.
         */
        public void release()
        {
            released.countDown();
        }
    }

    /**
     * Presto {@link Plugin} that registers {@link TestingGateFunctions} so {@code await_gate} can be invoked in SQL queries.
     */
    public static final class TestingGatePlugin
            implements Plugin
    {
        @Override
        public Set<Class<?>> getFunctions()
        {
            return ImmutableSet.of(TestingGateFunctions.class);
        }
    }
}

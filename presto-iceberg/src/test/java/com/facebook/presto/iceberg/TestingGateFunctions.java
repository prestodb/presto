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

public final class TestingGateFunctions
{
    private static final Map<String, Gate> GATES = new ConcurrentHashMap<>();

    private TestingGateFunctions() {}

    public static Gate createGate(String name)
    {
        Gate gate = new Gate(name);
        if (GATES.putIfAbsent(name, gate) != null) {
            throw new IllegalStateException("Gate already exists: " + name);
        }
        return gate;
    }

    public static void removeGate(String name)
    {
        GATES.remove(name);
    }

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

    public static final class Gate
    {
        private final String name;
        private final CountDownLatch arrived = new CountDownLatch(1);
        private final CountDownLatch released = new CountDownLatch(1);

        private Gate(String name)
        {
            this.name = requireNonNull(name, "name is null");
        }

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

        public void awaitArrival(long timeout, TimeUnit unit)
                throws InterruptedException
        {
            if (!arrived.await(timeout, unit)) {
                throw new IllegalStateException("Timed out waiting for gate " + name + " to be reached");
            }
        }

        public void release()
        {
            released.countDown();
        }
    }

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

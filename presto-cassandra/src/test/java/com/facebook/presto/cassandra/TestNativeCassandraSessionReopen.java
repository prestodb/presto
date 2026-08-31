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
package com.facebook.presto.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.facebook.airlift.units.Duration;
import org.testng.annotations.Test;

import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.facebook.airlift.json.JsonCodec.listJsonCodec;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;

/**
 * Regression test for the {@link ReopeningSession} wiring inside {@link NativeCassandraSession}.
 * <p>
 * {@code NativeCassandraSession} must resolve the live session through {@link ReopeningSession}
 * on every operation so that a closed delegate is transparently reopened. A previous version
 * memoized the supplier, which pinned the first session forever and silently defeated reopening
 * (every subsequent query would keep using the closed session). This test asserts that once the
 * underlying session is closed, the next operation triggers the supplier again and a fresh,
 * non-closed session is used.
 */
public class TestNativeCassandraSessionReopen
{
    @Test
    public void testClosedSessionIsReopenedAndNotPinned()
    {
        AtomicInteger sessionsCreated = new AtomicInteger();
        // Tracks the most recently created proxy session so the test can close it out-of-band.
        AtomicReference<AtomicBoolean> currentClosedFlag = new AtomicReference<>();
        AtomicReference<CqlSession> currentSession = new AtomicReference<>();

        Supplier<CqlSession> sessionSupplier = () -> {
            sessionsCreated.incrementAndGet();
            AtomicBoolean closed = new AtomicBoolean(false);
            CqlSession session = newProxySession(closed);
            currentClosedFlag.set(closed);
            currentSession.set(session);
            return session;
        };

        ReopeningSession reopeningSession = new ReopeningSession(sessionSupplier);
        NativeCassandraSession cassandraSession = new NativeCassandraSession(
                "test-connector",
                listJsonCodec(ExtraColumnMetadata.class),
                reopeningSession,
                new Duration(1, MINUTES),
                false,
                8192);

        // First operation creates the initial session.
        cassandraSession.execute("SELECT 1");
        assertEquals(sessionsCreated.get(), 1, "expected exactly one session after the first operation");
        CqlSession firstSession = currentSession.get();

        // Simulate the underlying session being closed out-of-band (transient outage, driver close).
        currentClosedFlag.get().set(true);

        // Next operation must reopen rather than reuse the closed (or a memoized) session.
        cassandraSession.execute("SELECT 1");
        assertEquals(sessionsCreated.get(), 2, "closed session must be reopened, not pinned/memoized");
        assertNotSame(currentSession.get(), firstSession, "a fresh session must be used after reopening");
    }

    /**
     * Builds a dynamic-proxy {@link CqlSession} whose {@code isClosed()} reflects the supplied flag.
     * Every other method returns a default value (null/false), which is sufficient for the code paths
     * exercised by {@code execute(String, Object...)} and {@code ReopeningSession.get()}.
     */
    private static CqlSession newProxySession(AtomicBoolean closed)
    {
        return (CqlSession) Proxy.newProxyInstance(
                CqlSession.class.getClassLoader(),
                new Class<?>[] {CqlSession.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "isClosed":
                            return closed.get();
                        case "toString":
                            return "ProxyCqlSession";
                        case "hashCode":
                            return System.identityHashCode(proxy);
                        case "equals":
                            return proxy == args[0];
                        default:
                            // execute(...) -> ResultSet is consumed but ignored by the test; null is fine.
                            return null;
                    }
                });
    }
}

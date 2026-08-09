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
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.facebook.airlift.units.Duration;
import org.testng.annotations.Test;

import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static com.facebook.airlift.json.JsonCodec.listJsonCodec;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.testng.Assert.assertEquals;

/**
 * Regression test for {@link NativeCassandraSession#getCassandraVersion()} (review item M1).
 * <p>
 * The version must be resolved from the driver's cached node metadata (no CQL round-trip) and cached
 * for the lifetime of the session. A previous version ran {@code SELECT release_version FROM
 * system.local} on every call, including the hot path in {@code getPartitions()}.
 */
public class TestNativeCassandraSessionVersion
{
    @Test
    public void testVersionReadFromNodeMetadataAndCached()
    {
        AtomicInteger executeCount = new AtomicInteger();
        AtomicInteger getNodesCount = new AtomicInteger();

        Node node = newProxyNode(Version.parse("3.11.10"));
        Metadata metadata = newProxyMetadata(node, getNodesCount);
        CqlSession cqlSession = newProxySession(metadata, executeCount);

        Supplier<CqlSession> supplier = () -> cqlSession;
        NativeCassandraSession cassandraSession = new NativeCassandraSession(
                "test-connector",
                listJsonCodec(ExtraColumnMetadata.class),
                new ReopeningSession(supplier),
                new Duration(1, MINUTES),
                false,
                8192);

        assertEquals(cassandraSession.getCassandraVersion(), "3.11.10");
        // Repeated calls must not re-resolve and must never fall back to a CQL query.
        assertEquals(cassandraSession.getCassandraVersion(), "3.11.10");
        assertEquals(cassandraSession.getCassandraVersion(), "3.11.10");

        assertEquals(getNodesCount.get(), 1, "version must be resolved from metadata exactly once (cached)");
        assertEquals(executeCount.get(), 0, "no system.local query should be issued when metadata exposes the version");
    }

    private static CqlSession newProxySession(Metadata metadata, AtomicInteger executeCount)
    {
        return (CqlSession) Proxy.newProxyInstance(
                CqlSession.class.getClassLoader(),
                new Class<?>[] {CqlSession.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "getMetadata":
                            return metadata;
                        case "execute":
                            executeCount.incrementAndGet();
                            return null;
                        case "isClosed":
                            return false;
                        case "toString":
                            return "ProxyCqlSession";
                        case "hashCode":
                            return System.identityHashCode(proxy);
                        case "equals":
                            return proxy == args[0];
                        default:
                            return null;
                    }
                });
    }

    private static Metadata newProxyMetadata(Node node, AtomicInteger getNodesCount)
    {
        return (Metadata) Proxy.newProxyInstance(
                Metadata.class.getClassLoader(),
                new Class<?>[] {Metadata.class},
                (proxy, method, args) -> {
                    if ("getNodes".equals(method.getName())) {
                        getNodesCount.incrementAndGet();
                        return Collections.singletonMap(UUID.randomUUID(), node);
                    }
                    return defaultObjectMethod(proxy, method, args);
                });
    }

    private static Node newProxyNode(Version version)
    {
        return (Node) Proxy.newProxyInstance(
                Node.class.getClassLoader(),
                new Class<?>[] {Node.class},
                (proxy, method, args) -> {
                    if ("getCassandraVersion".equals(method.getName())) {
                        return version;
                    }
                    return defaultObjectMethod(proxy, method, args);
                });
    }

    private static Object defaultObjectMethod(Object proxy, java.lang.reflect.Method method, Object[] args)
    {
        switch (method.getName()) {
            case "toString":
                return "Proxy(" + method.getDeclaringClass().getSimpleName() + ")";
            case "hashCode":
                return System.identityHashCode(proxy);
            case "equals":
                return proxy == args[0];
            default:
                return null;
        }
    }
}

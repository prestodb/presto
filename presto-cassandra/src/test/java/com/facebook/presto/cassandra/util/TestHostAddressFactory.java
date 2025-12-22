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
package com.facebook.presto.cassandra.util;

import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.api.core.metadata.token.TokenMap;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.facebook.presto.spi.HostAddress;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.testng.Assert.assertEquals;

public class TestHostAddressFactory
{
    // Mock Node implementation for testing - only implements getEndPoint() which is what the test needs
    private static class MockNode implements Node
    {
        private final InetSocketAddress address;

        MockNode(InetSocketAddress address)
        {
            this.address = address;
        }

        @Override
        public EndPoint getEndPoint()
        {
            return new EndPoint()
            {
                @Override
                public InetSocketAddress resolve()
                {
                    return address;
                }

                @Override
                public String asMetricPrefix()
                {
                    return address.toString();
                }
            };
        }

        // Stub implementations for all other Node interface methods
        // These are not used by the test but required by the interface
        @Override public NodeState getState() { throw new UnsupportedOperationException(); }
        @Override public NodeDistance getDistance() { throw new UnsupportedOperationException(); }
        @Override public Version getCassandraVersion() { throw new UnsupportedOperationException(); }
        @Override public Version getDriverVersion() { throw new UnsupportedOperationException(); }
        @Override public Optional<TokenMap> getTokenMap() { throw new UnsupportedOperationException(); }
        @Override public Set<TokenRange> getReplicas(String keyspace, TokenRange tokenRange) { throw new UnsupportedOperationException(); }
        @Override public Set<TokenRange> getReplicas(String keyspace, ByteBuffer partitionKey) { throw new UnsupportedOperationException(); }
        @Override public String getDatacenter() { throw new UnsupportedOperationException(); }
        @Override public String getRack() { throw new UnsupportedOperationException(); }
        @Override public Map<String, Object> getExtras() { throw new UnsupportedOperationException(); }
        @Override public Optional<Instant> getUpSince() { throw new UnsupportedOperationException(); }
        @Override public Optional<Instant> getDownSince() { throw new UnsupportedOperationException(); }
        @Override public int getOpenConnections() { throw new UnsupportedOperationException(); }
        @Override public UUID getHostId() { throw new UnsupportedOperationException(); }
        @Override public boolean isReconnecting() { throw new UnsupportedOperationException(); }
        @Override public Optional<InetSocketAddress> getListenAddress() { throw new UnsupportedOperationException(); }
        @Override public Optional<InetSocketAddress> getBroadcastAddress() { throw new UnsupportedOperationException(); }
        @Override public long getUpSinceMillis() { throw new UnsupportedOperationException(); }
        @Override public Optional<InetSocketAddress> getBroadcastRpcAddress() { throw new UnsupportedOperationException(); }
        @Override public UUID getSchemaVersion() { throw new UnsupportedOperationException(); }
    }

    @Test
    public void testToHostAddressList()
            throws Exception
    {
        Set<Node> nodes = ImmutableSet.of(
                new MockNode(new InetSocketAddress(
                        InetAddress.getByAddress(new byte[] {
                                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16
                        }),
                        3000)),
                new MockNode(new InetSocketAddress(InetAddress.getByAddress(new byte[] {1, 2, 3, 4}), 3000)));

        HostAddressFactory hostAddressFactory = new HostAddressFactory();
        List<HostAddress> list = hostAddressFactory.toHostAddressList(nodes);

        assertEquals(list.toString(), "[[102:304:506:708:90a:b0c:d0e:f10], 1.2.3.4]");
    }
}

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
package com.datastax.driver.core;

import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;

import java.net.InetSocketAddress;
import java.util.Optional;

/**
 * Test utility class for creating mock Host/Node objects.
 * This is a compatibility shim for tests that used the old driver's Host class.
 */
public class TestHost
{
    private final InetSocketAddress address;

    public TestHost(InetSocketAddress address)
    {
        this.address = address;
    }

    public InetSocketAddress getSocketAddress()
    {
        return address;
    }

    /**
     * Converts to a Node for the new driver.
     * Note: This creates a minimal mock node for testing purposes.
     */
    public Node toNode()
    {
        // Return a mock Node implementation for testing
        return new MockNode(address);
    }

    private static class MockNode
            implements Node
    {
        private final InetSocketAddress address;

        MockNode(InetSocketAddress address)
        {
            this.address = address;
        }

        @Override
        public EndPoint getEndPoint()
        {
            return new EndPoint() {
                @Override
                public InetSocketAddress resolve()
                {
                    return address;
                }

                @Override
                public String asMetricPrefix()
                {
                    return address.getHostString() + ":" + address.getPort();
                }
            };
        }

        @Override
        public Optional<InetSocketAddress> getBroadcastAddress()
        {
            return Optional.of(address);
        }

        @Override
        public Optional<InetSocketAddress> getListenAddress()
        {
            return Optional.of(address);
        }

        @Override
        public String getDatacenter()
        {
            return "datacenter1";
        }

        @Override
        public String getRack()
        {
            return "rack1";
        }

        @Override
        public Version getCassandraVersion()
        {
            return null;
        }

        @Override
        public java.util.Map<String, Object> getExtras()
        {
            return java.util.Collections.emptyMap();
        }

        @Override
        public java.util.UUID getHostId()
        {
            return java.util.UUID.randomUUID();
        }

        @Override
        public java.util.UUID getSchemaVersion()
        {
            return java.util.UUID.randomUUID();
        }

        @Override
        public NodeState getState()
        {
            return NodeState.UP;
        }

        @Override
        public long getUpSinceMillis()
        {
            return System.currentTimeMillis();
        }

        @Override
        public int getOpenConnections()
        {
            return 1;
        }

        @Override
        public boolean isReconnecting()
        {
            return false;
        }

        @Override
        public Optional<InetSocketAddress> getBroadcastRpcAddress()
        {
            return Optional.of(address);
        }

        @Override
        public NodeDistance getDistance()
        {
            return NodeDistance.LOCAL;
        }
    }
}

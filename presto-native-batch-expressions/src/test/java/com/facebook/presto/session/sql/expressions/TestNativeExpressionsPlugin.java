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
package com.facebook.presto.session.sql.expressions;

import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.NodePoolType;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.RowExpressionSerde;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.sql.planner.RowExpressionInterpreterServiceFactory;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Set;

import static com.google.common.collect.Iterables.getOnlyElement;

public class TestNativeExpressionsPlugin
{
    @Test
    public void testLoadPlugin()
    {
        Plugin plugin = new NativeExpressionsPlugin();
        Iterable<RowExpressionInterpreterServiceFactory> serviceFactories = plugin.getRowExpressionInterpreterServiceFactories();
        RowExpressionInterpreterServiceFactory factory = getOnlyElement(serviceFactories);
        factory.createInterpreter(ImmutableMap.of(), new TestingNodeManager(), new TestingRowExpressionSerde());
    }

    // TODO: clean this up
    private static class TestingNodeManager
            implements NodeManager
    {
        @Override
        public Set<Node> getAllNodes()
        {
            return null;
        }

        @Override
        public Set<Node> getWorkerNodes()
        {
            return null;
        }

        @Override
        public Node getCurrentNode()
        {
            return new Node() {
                @Override
                public String getHost()
                {
                    return null;
                }

                @Override
                public HostAddress getHostAndPort()
                {
                    return null;
                }

                @Override
                public URI getHttpUri()
                {
                    return null;
                }

                @Override
                public String getNodeIdentifier()
                {
                    return null;
                }

                @Override
                public String getVersion()
                {
                    return null;
                }

                @Override
                public boolean isCoordinator()
                {
                    return false;
                }

                @Override
                public boolean isResourceManager()
                {
                    return false;
                }

                @Override
                public boolean isCatalogServer()
                {
                    return false;
                }

                @Override
                public boolean isCoordinatorSidecar()
                {
                    return false;
                }

                @Override
                public NodePoolType getPoolType()
                {
                    return null;
                }
            };
        }

        @Override
        public Node getSidecarNode()
        {
            return null;
        }

        @Override
        public String getEnvironment()
        {
            return null;
        }
    }

    // TODO: @tdm
    private class TestingRowExpressionSerde
            implements RowExpressionSerde
    {
        @Override
        public String serialize(RowExpression expression)
        {
            return "";
        }

        @Override
        public RowExpression deserialize(String value)
        {
            return null;
        }
    }
}

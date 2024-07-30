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
package com.facebook.presto.sidecar;

import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.NodePoolType;

import java.net.URI;
import java.util.Set;

class UnimplementedNodeManager
        implements NodeManager
{
    @Override
    public Set<Node> getAllNodes()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Node> getWorkerNodes()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Node getCurrentNode()
    {
        return new Node()
        {
            @Override
            public String getHost()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public HostAddress getHostAndPort()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public URI getHttpUri()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public String getNodeIdentifier()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public String getVersion()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean isCoordinator()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean isResourceManager()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean isCatalogServer()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean isCoordinatorSidecar()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public NodePoolType getPoolType()
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public Node getSidecarNode()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getEnvironment()
    {
        throw new UnsupportedOperationException();
    }
}

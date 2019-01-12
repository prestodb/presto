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
package com.facebook.presto.connector;

import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

import static java.util.Objects.requireNonNull;

public class ConnectorAwareNodeManager
        implements NodeManager
{
    private final InternalNodeManager nodeManager;
    private final String environment;
    private final ConnectorId connectorId;

    public ConnectorAwareNodeManager(InternalNodeManager nodeManager, String environment, ConnectorId connectorId)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.environment = requireNonNull(environment, "environment is null");
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
    }

    @Override
    public Set<Node> getAllNodes()
    {
        return ImmutableSet.<Node>builder()
                .addAll(getWorkerNodes())
                .addAll(nodeManager.getCoordinators())
                .build();
    }

    @Override
    public Set<Node> getWorkerNodes()
    {
        return nodeManager.getActiveConnectorNodes(connectorId);
    }

    @Override
    public Node getCurrentNode()
    {
        return nodeManager.getCurrentNode();
    }

    @Override
    public String getEnvironment()
    {
        return environment;
    }
}

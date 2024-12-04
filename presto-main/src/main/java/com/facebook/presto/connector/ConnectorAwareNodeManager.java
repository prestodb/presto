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

import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import java.util.Random;
import java.util.Set;

import static com.facebook.presto.spi.StandardErrorCode.NO_CPP_SIDECARS;
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
                .addAll(nodeManager.getCoordinatorSidecars())
                .build();
    }

    @Override
    public Set<Node> getWorkerNodes()
    {
        return ImmutableSet.copyOf(nodeManager.getActiveConnectorNodes(connectorId));
    }

    @Override
    public Node getCurrentNode()
    {
        return nodeManager.getCurrentNode();
    }

    @Override
    public Node getSidecarNode()
    {
        Set<InternalNode> coordinatorSidecars = nodeManager.getCoordinatorSidecars();
        if (coordinatorSidecars.isEmpty()) {
            throw new PrestoException(NO_CPP_SIDECARS, "Expected exactly one coordinator sidecar, but found none");
        }
        return Iterables.get(coordinatorSidecars, new Random().nextInt(coordinatorSidecars.size()));
    }

    @Override
    public String getEnvironment()
    {
        return environment;
    }
}

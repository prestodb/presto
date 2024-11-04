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
package com.facebook.presto.nodeManager;

import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;

import java.util.Random;
import java.util.Set;

import static com.facebook.presto.spi.StandardErrorCode.NO_CPP_SIDECARS;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

/**
 * This class simplifies managing Presto's cluster nodes,
 * focusing on active workers and coordinators without tying to specific connectors.
 */
public class PluginNodeManager
        implements NodeManager
{
    private final InternalNodeManager nodeManager;
    private final String environment;

    @Inject
    public PluginNodeManager(InternalNodeManager nodeManager)
    {
        this.nodeManager = nodeManager;
        this.environment = "test";
    }

    public PluginNodeManager(InternalNodeManager nodeManager, String environment)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.environment = requireNonNull(environment, "environment is null");
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
        //Retrieves all active worker nodes, excluding coordinators, resource managers, and catalog servers.
        return nodeManager.getAllNodes().getActiveNodes().stream()
                .filter(node -> !node.isResourceManager() && !node.isCoordinator() && !node.isCatalogServer())
                .collect(toImmutableSet());
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

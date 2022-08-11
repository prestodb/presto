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
package com.facebook.presto.resourcemanager.cpu;

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.json.Codec;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.smile.SmileCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.execution.LocationFactory;
import com.facebook.presto.execution.scheduler.NodeSchedulerConfig;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.server.InternalCommunicationConfig;
import com.facebook.presto.server.ServerConfig;
import com.google.common.collect.ImmutableSet;
import org.weakref.jmx.MBeanExporter;

import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.spi.NodeState.ACTIVE;
import static com.facebook.presto.spi.NodeState.SHUTTING_DOWN;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.difference;
import static java.util.Objects.requireNonNull;

public class ClusterCPUManager
{
    private static final Logger log = Logger.get(ClusterCPUManager.class);

    private final InternalNodeManager nodeManager;
    private final LocationFactory locationFactory;
    private final HttpClient httpClient;
    private final MBeanExporter exporter;
    private final Codec<CPUInfo> cpuInfoCodec;
    private final boolean enabled;
    private final boolean isWorkScheduledOnCoordinator;
    private final boolean isBinaryTransportEnabled;

    @GuardedBy("this")
    private final Map<String, RemoteNodeCPU> nodes = new HashMap<>();

    @Inject
    public ClusterCPUManager(
            HttpClient httpClient,
            InternalNodeManager nodeManager,
            LocationFactory locationFactory,
            MBeanExporter exporter,
            JsonCodec<CPUInfo> cpuInfoJsonCodec,
            SmileCodec<CPUInfo> cpuInfoSmileCodec,
            ServerConfig serverConfig,
            NodeSchedulerConfig schedulerConfig,
            InternalCommunicationConfig communicationConfig)
    {
        requireNonNull(serverConfig, "serverConfig is null");
        requireNonNull(schedulerConfig, "schedulerConfig is null");
        requireNonNull(communicationConfig, "communicationConfig is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.locationFactory = requireNonNull(locationFactory, "locationFactory is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.exporter = requireNonNull(exporter, "exporter is null");
        this.enabled = serverConfig.isCoordinator();
        this.isWorkScheduledOnCoordinator = schedulerConfig.isIncludeCoordinator();
        this.isBinaryTransportEnabled = communicationConfig.isBinaryTransportEnabled();
        if (this.isBinaryTransportEnabled) {
            this.cpuInfoCodec = requireNonNull(cpuInfoSmileCodec, "cpuInfoSmileCodec is null");
        }
        else {
            this.cpuInfoCodec = requireNonNull(cpuInfoJsonCodec, "cpuInfoJsonCodec is null");
        }
    }

    public synchronized void refresh()
    {
        if (!enabled) {
            return;
        }
        updateNodes();
    }

    private synchronized void updateNodes()
    {
        ImmutableSet.Builder<InternalNode> builder = ImmutableSet.builder();
        Set<InternalNode> aliveNodes = builder
                .addAll(nodeManager.getNodes(ACTIVE))
                .addAll(nodeManager.getNodes(SHUTTING_DOWN))
                .build();

        ImmutableSet<String> aliveNodeIds = aliveNodes.stream()
                .map(InternalNode::getNodeIdentifier)
                .collect(toImmutableSet());

        // Remove nodes that don't exist anymore
        // Make a copy to materialize the set difference
        Set<String> deadNodes = ImmutableSet.copyOf(difference(nodes.keySet(), aliveNodeIds));
        nodes.keySet().removeAll(deadNodes);

        // Add new nodes
        for (InternalNode node : aliveNodes) {
            if (!nodes.containsKey(node.getNodeIdentifier())) {
                nodes.put(
                        node.getNodeIdentifier(),
                        new RemoteNodeCPU(
                                node,
                                httpClient,
                                cpuInfoCodec,
                                locationFactory.createCPUInfoLocation(node),
                                isBinaryTransportEnabled));
            }
        }

        // If work isn't scheduled on the coordinator (the current node) there is no point
        // in polling or updating (when moving queries to the reserved pool) its cpu information
        if (!isWorkScheduledOnCoordinator) {
            nodes.remove(nodeManager.getCurrentNode().getNodeIdentifier());
        }

        // Schedule refresh
        for (RemoteNodeCPU node : nodes.values()) {
            node.asyncRefresh();
        }
    }

    public synchronized Map<String, Optional<CPUInfo>> getWorkerCPUInfo()
    {
        Map<String, Optional<CPUInfo>> cpuInfo = new HashMap<>();
        for (Map.Entry<String, RemoteNodeCPU> entry : nodes.entrySet()) {
            // workerId is of the form "node_identifier [node_host]"
            String workerId = entry.getKey() + " [" + entry.getValue().getNode().getHost() + "]";
            cpuInfo.put(workerId, entry.getValue().getCPUInfo());
        }
        return cpuInfo;
    }
}

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
package com.facebook.presto.execution;

import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import com.google.common.net.InetAddresses;
import org.weakref.jmx.Managed;

import javax.inject.Inject;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class NodeScheduler
{
    private final NodeManager nodeManager;
    private final AtomicLong scheduleLocal = new AtomicLong();
    private final AtomicLong scheduleRack = new AtomicLong();
    private final AtomicLong scheduleRandom = new AtomicLong();
    private final int minCandidates;
    private final boolean locationAwareScheduling;
    private final boolean includeCoordinator;
    private final int maxSplitsPerNode;
    private final int maxSplitsPerNodePerTaskWhenFull;
    private final NodeTaskMap nodeTaskMap;

    @Inject
    public NodeScheduler(NodeManager nodeManager, NodeSchedulerConfig config, NodeTaskMap nodeTaskMap)
    {
        this.nodeManager = nodeManager;
        this.minCandidates = config.getMinCandidates();
        this.locationAwareScheduling = config.isLocationAwareSchedulingEnabled();
        this.includeCoordinator = config.isIncludeCoordinator();
        this.maxSplitsPerNode = config.getMaxSplitsPerNode();
        this.maxSplitsPerNodePerTaskWhenFull = config.getMaxPendingSplitsPerNodePerTask();
        this.nodeTaskMap = checkNotNull(nodeTaskMap, "nodeTaskMap is null");
        checkArgument(maxSplitsPerNode > maxSplitsPerNodePerTaskWhenFull, "maxSplitsPerNode must be > maxSplitsPerNodePerTaskWhenFull");
    }

    @Managed
    public long getScheduleLocal()
    {
        return scheduleLocal.get();
    }

    @Managed
    public long getScheduleRack()
    {
        return scheduleRack.get();
    }

    @Managed
    public long getScheduleRandom()
    {
        return scheduleRandom.get();
    }

    @Managed
    public void reset()
    {
        scheduleLocal.set(0);
        scheduleRack.set(0);
        scheduleRandom.set(0);
    }

    public NodeSelector createNodeSelector(final String dataSourceName, Map<Node, RemoteTask> taskMap)
    {
        // this supplier is thread-safe. TODO: this logic should probably move to the scheduler since the choice of which node to run in should be
        // done as close to when the the split is about to be scheduled
        Supplier<NodeMap> nodeMap = Suppliers.memoizeWithExpiration(new Supplier<NodeMap>()
        {
            @Override
            public NodeMap get()
            {
                ImmutableSetMultimap.Builder<HostAddress, Node> byHostAndPort = ImmutableSetMultimap.builder();
                ImmutableSetMultimap.Builder<InetAddress, Node> byHost = ImmutableSetMultimap.builder();
                ImmutableSetMultimap.Builder<Rack, Node> byRack = ImmutableSetMultimap.builder();

                Set<Node> nodes;
                if (dataSourceName != null) {
                    nodes = nodeManager.getActiveDatasourceNodes(dataSourceName);
                }
                else {
                    nodes = FluentIterable.from(nodeManager.getActiveNodes()).filter(new Predicate<Node>()
                    {
                        @Override
                        public boolean apply(Node node)
                        {
                            // TODO: This only filters out the coordinator if it's the current node, which does not work if we have multiple coordinators.
                            // Instead we should have the coordinator announce that it's a coordinator in service discovery.
                            return includeCoordinator || !nodeManager.getCurrentNode().getNodeIdentifier().equals(node.getNodeIdentifier());
                        }
                    }).toSet();
                }

                for (Node node : nodes) {
                    try {
                        byHostAndPort.put(node.getHostAndPort(), node);

                        InetAddress host = InetAddress.getByName(node.getHttpUri().getHost());
                        byHost.put(host, node);

                        byRack.put(Rack.of(host), node);
                    }
                    catch (UnknownHostException e) {
                        // ignore
                    }
                }

                return new NodeMap(byHostAndPort.build(), byHost.build(), byRack.build());
            }
        }, 5, TimeUnit.SECONDS);

        return new NodeSelector(nodeMap, taskMap);
    }

    public class NodeSelector
    {
        private final AtomicReference<Supplier<NodeMap>> nodeMap;
        private final Map<Node, RemoteTask> taskMap;

        public NodeSelector(Supplier<NodeMap> nodeMap, Map<Node, RemoteTask> taskMap)
        {
            this.nodeMap = new AtomicReference<>(nodeMap);
            this.taskMap = taskMap;
        }

        public void lockDownNodes()
        {
            nodeMap.set(Suppliers.ofInstance(nodeMap.get().get()));
        }

        public List<Node> allNodes()
        {
            return ImmutableList.copyOf(nodeMap.get().get().getNodesByHostAndPort().values());
        }

        public Node selectCurrentNode()
        {
            // TODO: this is a hack to force scheduling on the coordinator
            return nodeManager.getCurrentNode();
        }

        public List<Node> selectRandomNodes(int limit)
        {
            checkArgument(limit > 0, "limit must be at least 1");

            return ImmutableList.copyOf(FluentIterable.from(lazyShuffle(nodeMap.get().get().getNodesByHostAndPort().values())).limit(limit));
        }

        /**
         * Identifies the nodes for runnning the specified splits.
         *
         * @param splits the splits that need to be assigned to nodes
         *
         * @return a multimap from node to splits only for splits for which we could identify a node to schedule on.
         *      If we cannot find an assignment for a split, it is not included in the map.
         */
        public Multimap<Node, Split> computeAssignments(Set<Split> splits)
        {
            Multimap<Node, Split> assignment = HashMultimap.create();
            Map<Node, Integer> assignmentCount = new HashMap<>();

            // maintain a temporary local cache of partitioned splits on the node
            Map<Node, Integer> splitCountByNode = new HashMap<>();

            for (Split split : splits) {
                List<Node> candidateNodes;
                if (locationAwareScheduling) {
                    candidateNodes = selectCandidateNodes(nodeMap.get().get(), split);
                }
                else {
                    candidateNodes = selectRandomNodes(minCandidates);
                }
                checkCondition(!candidateNodes.isEmpty(), NO_NODES_AVAILABLE, "No nodes available to run query");

                // compute and cache number of splits currently assigned to each node
                for (Node node : candidateNodes) {
                    if (!splitCountByNode.containsKey(node)) {
                        splitCountByNode.put(node, nodeTaskMap.getPartitionedSplitsOnNode(node));
                    }
                }
                Node chosenNode = null;
                int min = Integer.MAX_VALUE;

                for (Node node : candidateNodes) {
                    int assignedSplitCount = assignmentCount.containsKey(node) ? assignmentCount.get(node) : 0;
                    int totalSplitCount = assignedSplitCount + splitCountByNode.get(node);

                    if (totalSplitCount < min && totalSplitCount < maxSplitsPerNode) {
                        chosenNode = node;
                        min = totalSplitCount;
                    }
                }
                if (chosenNode == null) {
                    for (Node node : candidateNodes) {
                        int assignedSplitCount = assignmentCount.containsKey(node) ? assignmentCount.get(node) : 0;
                        RemoteTask remoteTask = taskMap.get(node);
                        int queuedSplitCount = remoteTask == null ? 0 : remoteTask.getQueuedPartitionedSplitCount();
                        int totalSplitCount = queuedSplitCount + assignedSplitCount;
                        if (totalSplitCount < min && totalSplitCount < maxSplitsPerNodePerTaskWhenFull) {
                            chosenNode = node;
                            min = totalSplitCount;
                        }
                    }
                }
                if (chosenNode != null) {
                    assignment.put(chosenNode, split);
                    int count = assignmentCount.containsKey(chosenNode) ? assignmentCount.get(chosenNode) : 0;
                    assignmentCount.put(chosenNode, count + 1);
                }
            }
            return assignment;
        }

        private List<Node> selectCandidateNodes(NodeMap nodeMap, Split split)
        {
            Set<Node> chosen = new LinkedHashSet<>(minCandidates);

            // first look for nodes that match the hint
            for (HostAddress hint : split.getAddresses()) {
                for (Node node : nodeMap.getNodesByHostAndPort().get(hint)) {
                    if (chosen.add(node)) {
                        scheduleLocal.incrementAndGet();
                    }
                }

                InetAddress address;
                try {
                    address = hint.toInetAddress();
                }
                catch (UnknownHostException e) {
                    // skip addresses that don't resolve
                    continue;
                }

                // consider a split with a host hint without a port as being accessible
                // by all nodes in that host
                if (!hint.hasPort() || split.isRemotelyAccessible()) {
                    for (Node node : nodeMap.getNodesByHost().get(address)) {
                        if (chosen.add(node)) {
                            scheduleLocal.incrementAndGet();
                        }
                    }
                }
            }

            // add nodes in same rack, if below the minimum count
            if (split.isRemotelyAccessible() && chosen.size() < minCandidates) {
                for (HostAddress hint : split.getAddresses()) {
                    InetAddress address;
                    try {
                        address = hint.toInetAddress();
                    }
                    catch (UnknownHostException e) {
                        // skip addresses that don't resolve
                        continue;
                    }
                    for (Node node : nodeMap.getNodesByRack().get(Rack.of(address))) {
                        if (chosen.add(node)) {
                            scheduleRack.incrementAndGet();
                        }
                        if (chosen.size() == minCandidates) {
                            break;
                        }
                    }
                    if (chosen.size() == minCandidates) {
                        break;
                    }
                }
            }

            // add some random nodes if below the minimum count
            if (split.isRemotelyAccessible()) {
                if (chosen.size() < minCandidates) {
                    for (Node node : lazyShuffle(nodeMap.getNodesByHost().values())) {
                        if (chosen.add(node)) {
                            scheduleRandom.incrementAndGet();
                        }

                        if (chosen.size() == minCandidates) {
                            break;
                        }
                    }
                }
            }

            return ImmutableList.copyOf(chosen);
        }
    }

    private static <T> Iterable<T> lazyShuffle(final Iterable<T> iterable)
    {
        return new Iterable<T>()
        {
            @Override
            public Iterator<T> iterator()
            {
                return new AbstractIterator<T>()
                {
                    List<T> list = Lists.newArrayList(iterable);
                    int limit = list.size();

                    @Override
                    protected T computeNext()
                    {
                        if (limit == 0) {
                            return endOfData();
                        }

                        int position = ThreadLocalRandom.current().nextInt(limit);

                        T result = list.get(position);
                        list.set(position, list.get(limit - 1));
                        limit--;

                        return result;
                    }
                };
            }
        };
    }

    private static class NodeMap
    {
        private final SetMultimap<HostAddress, Node> nodesByHostAndPort;
        private final SetMultimap<InetAddress, Node> nodesByHost;
        private final SetMultimap<Rack, Node> nodesByRack;

        public NodeMap(SetMultimap<HostAddress, Node> nodesByHostAndPort, SetMultimap<InetAddress, Node> nodesByHost, SetMultimap<Rack, Node> nodesByRack)
        {
            this.nodesByHostAndPort = nodesByHostAndPort;
            this.nodesByHost = nodesByHost;
            this.nodesByRack = nodesByRack;
        }

        private SetMultimap<HostAddress, Node> getNodesByHostAndPort()
        {
            return nodesByHostAndPort;
        }

        public SetMultimap<InetAddress, Node> getNodesByHost()
        {
            return nodesByHost;
        }

        public SetMultimap<Rack, Node> getNodesByRack()
        {
            return nodesByRack;
        }
    }

    private static class Rack
    {
        private int id;

        public static Rack of(InetAddress address)
        {
            // TODO: this needs to be pluggable
            int id = InetAddresses.coerceToInteger(address) & 0xFF_FF_FF_00;
            return new Rack(id);
        }

        private Rack(int id)
        {
            this.id = id;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Rack rack = (Rack) o;

            if (id != rack.id) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode()
        {
            return id;
        }
    }
}

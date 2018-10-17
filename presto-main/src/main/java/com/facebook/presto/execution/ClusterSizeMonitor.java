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

import com.facebook.presto.execution.scheduler.NodeSchedulerConfig;
import com.facebook.presto.metadata.AllNodes;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.Duration;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;

import static com.facebook.presto.spi.StandardErrorCode.SERVER_STARTING_UP;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.airlift.units.Duration.nanosSince;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class ClusterSizeMonitor
{
    private final InternalNodeManager nodeManager;
    private final boolean includeCoordinator;
    private final int minCount;
    private final Duration maxWait;
    private final ScheduledExecutorService executor;

    private final long createNanos = System.nanoTime();

    private final Consumer<AllNodes> listener = this::updateAllNodes;

    @GuardedBy("this")
    private int currentCount;

    @GuardedBy("this")
    private final List<SettableFuture<?>> futures = new ArrayList<>();

    @GuardedBy("this")
    private boolean minimumWorkerRequirementMet;

    @Inject
    public ClusterSizeMonitor(InternalNodeManager nodeManager, NodeSchedulerConfig nodeSchedulerConfig, QueryManagerConfig queryManagerConfig)
    {
        this(
                nodeManager,
                requireNonNull(nodeSchedulerConfig, "nodeSchedulerConfig is null").isIncludeCoordinator(),
                requireNonNull(queryManagerConfig, "queryManagerConfig is null").getInitializationRequiredWorkers(),
                queryManagerConfig.getInitializationTimeout());
    }

    public ClusterSizeMonitor(InternalNodeManager nodeManager, boolean includeCoordinator, int minCount, Duration maxWait)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.includeCoordinator = includeCoordinator;
        checkArgument(minCount >= 0, "minCount is negative");
        this.minCount = minCount;
        this.maxWait = requireNonNull(maxWait, "maxWait is null");
        this.executor = newSingleThreadScheduledExecutor(threadsNamed("node-monitor-%s"));
    }

    @PostConstruct
    public void start()
    {
        nodeManager.addNodeChangeListener(listener);
        updateAllNodes(nodeManager.getAllNodes());
    }

    @PreDestroy
    public void stop()
    {
        nodeManager.removeNodeChangeListener(listener);
    }

    public synchronized void verifyInitialMinimumWorkersRequirement()
    {
        if (minimumWorkerRequirementMet) {
            return;
        }

        if (currentCount < minCount && nanosSince(createNanos).compareTo(maxWait) < 0) {
            throw new PrestoException(
                    SERVER_STARTING_UP,
                    format("Cluster is still initializing, there are insufficient active worker nodes (%s) to run query", currentCount));
        }
        minimumWorkerRequirementMet = true;
    }

    private synchronized void updateAllNodes(AllNodes allNodes)
    {
        if (includeCoordinator) {
            currentCount = allNodes.getActiveNodes().size();
        }
        else {
            currentCount = Sets.difference(allNodes.getActiveNodes(), allNodes.getActiveCoordinators()).size();
        }
        if (currentCount >= minCount) {
            ImmutableList<SettableFuture<?>> listeners = ImmutableList.copyOf(futures);
            futures.clear();
            executor.submit(() -> listeners.forEach(listener -> listener.set(null)));
        }
    }
}

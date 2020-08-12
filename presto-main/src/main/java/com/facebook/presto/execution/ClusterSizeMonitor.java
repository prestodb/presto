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
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.Duration;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Consumer;

import static com.facebook.airlift.concurrent.Threads.threadsNamed;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class ClusterSizeMonitor
{
    private final InternalNodeManager nodeManager;
    private final boolean includeCoordinator;
    private final int executionMinCount;
    private final Duration executionMaxWait;
    private final int minCoordinatorCount;
    private final Duration maxCoordinatorWait;
    private final ScheduledExecutorService executor;

    private final Consumer<AllNodes> listener = this::updateAllNodes;

    @GuardedBy("this")
    private int currentCount;

    @GuardedBy("this")
    private int currentCoordinatorCount;

    @GuardedBy("this")
    private final List<SettableFuture<?>> workerSizeFutures = new ArrayList<>();

    @GuardedBy("this")
    private final List<SettableFuture<?>> coordinatorSizeFutures = new ArrayList<>();

    @Inject
    public ClusterSizeMonitor(InternalNodeManager nodeManager, NodeSchedulerConfig nodeSchedulerConfig, QueryManagerConfig queryManagerConfig)
    {
        this(
                nodeManager,
                requireNonNull(nodeSchedulerConfig, "nodeSchedulerConfig is null").isIncludeCoordinator(),
                requireNonNull(queryManagerConfig, "queryManagerConfig is null").getRequiredWorkers(),
                queryManagerConfig.getRequiredWorkersMaxWait(),
                requireNonNull(queryManagerConfig, "queryManagerConfig is null").getRequiredCoordinators(),
                queryManagerConfig.getRequiredCoordinatorsMaxWait());
    }

    public ClusterSizeMonitor(
            InternalNodeManager nodeManager,
            boolean includeCoordinator,
            int executionMinCount,
            Duration executionMaxWait,
            int minCoordinatorCount,
            Duration maxCoordinatorWait)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.includeCoordinator = includeCoordinator;
        checkArgument(executionMinCount >= 0, "executionMinCount is negative");
        this.executionMinCount = executionMinCount;
        this.executionMaxWait = requireNonNull(executionMaxWait, "executionMaxWait is null");
        checkArgument(minCoordinatorCount >= 0, "minCoordinatorCount is negative");
        this.minCoordinatorCount = minCoordinatorCount;
        this.maxCoordinatorWait = requireNonNull(maxCoordinatorWait, "maxCoordinatorWait is null");
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

    /**
     * Returns a listener that completes when the minimum number of workers for the cluster has been met.
     * Note: caller should not add a listener using the direct executor, as this can delay the
     * notifications for other listeners.
     */
    public synchronized ListenableFuture<?> waitForMinimumWorkers()
    {
        if (currentCount >= executionMinCount) {
            return immediateFuture(null);
        }

        SettableFuture<?> future = SettableFuture.create();
        workerSizeFutures.add(future);

        // if future does not finish in wait period, complete with an exception
        ScheduledFuture<?> timeoutTask = executor.schedule(
                () -> {
                    synchronized (this) {
                        future.setException(new PrestoException(
                                GENERIC_INSUFFICIENT_RESOURCES,
                                format("Insufficient active worker nodes. Waited %s for at least %s workers, but only %s workers are active", executionMaxWait, executionMinCount, currentCount)));
                    }
                },
                executionMaxWait.toMillis(),
                MILLISECONDS);

        // remove future if finished (e.g., canceled, timed out)
        future.addListener(() -> {
            timeoutTask.cancel(true);
            removeFuture(future);
        }, executor);

        return future;
    }

    public synchronized ListenableFuture<?> waitForMinimumCoordinators()
    {
        if (currentCoordinatorCount >= minCoordinatorCount) {
            return immediateFuture(null);
        }

        SettableFuture<?> future = SettableFuture.create();
        coordinatorSizeFutures.add(future);

        // if future does not finish in wait period, complete with an exception
        ScheduledFuture<?> timeoutTask = executor.schedule(
                () -> {
                    synchronized (this) {
                        future.setException(new PrestoException(
                                GENERIC_INSUFFICIENT_RESOURCES,
                                format("Insufficient active coordinator nodes. Waited %s for at least %s coordinators, but only %s coordinators are active", executionMaxWait, 2, currentCoordinatorCount)));
                    }
                },
                maxCoordinatorWait.toMillis(),
                MILLISECONDS);

        // remove future if finished (e.g., canceled, timed out)
        future.addListener(() -> {
            timeoutTask.cancel(true);
            removeFuture(future);
        }, executor);

        return future;
    }

    private synchronized void removeFuture(SettableFuture<?> future)
    {
        workerSizeFutures.remove(future);
    }

    private synchronized void updateAllNodes(AllNodes allNodes)
    {
        currentCount = 0;
        currentCoordinatorCount = 0;
        allNodes.getActiveNodes().forEach(node -> {
            if (node.isCoordinator()) {
                currentCoordinatorCount += 1;
                if (!includeCoordinator) {
                    return;
                }
            }
            currentCount += 1;
        });
        if (currentCount >= executionMinCount) {
            ImmutableList<SettableFuture<?>> listeners = ImmutableList.copyOf(workerSizeFutures);
            workerSizeFutures.clear();
            executor.submit(() -> listeners.forEach(listener -> listener.set(null)));
        }
        if (currentCoordinatorCount >= minCoordinatorCount) {
            ImmutableList<SettableFuture<?>> listeners = ImmutableList.copyOf(coordinatorSizeFutures);
            coordinatorSizeFutures.clear();
            executor.submit(() -> listeners.forEach(listener -> listener.set(null)));
        }
    }
}

package com.facebook.presto.resourcemanager;

import com.facebook.drift.client.DriftClient;
import com.facebook.presto.server.NodeStatus;
import com.facebook.presto.util.PeriodicTaskExecutor;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

public class ClusterNodeManagerService
{
    private final DriftClient<ResourceManagerClient> resourceManagerClient;
    private final ScheduledExecutorService executorService;
    private final long nodeFetchIntervalMillis = 1_000;

    private final AtomicReference<Map<String, NodeStatus>> nodes;
    private PeriodicTaskExecutor nodeUpdater;

    @Inject
    public ClusterNodeManagerService(
            @ForResourceManager DriftClient<ResourceManagerClient> resourceManagerClient,
            @ForResourceManager ScheduledExecutorService executorService,
            ResourceManagerConfig resourceManagerConfig)
    {
        this.resourceManagerClient = requireNonNull(resourceManagerClient, "resourceManagerClient is null");
        this.executorService = requireNonNull(executorService, "executorService is null");
//        this.memoryPoolFetchIntervalMillis = requireNonNull(resourceManagerConfig, "resourceManagerConfig is null").getMemoryPoolFetchInterval().toMillis();

        this.nodes = new AtomicReference<>();
    }

    @PostConstruct
    public void init()
    {
        this.nodeUpdater = new PeriodicTaskExecutor(nodeFetchIntervalMillis, executorService, () -> nodes.set(retrieveNodes()));
    }

    @PreDestroy
    public void stop()
    {
        if (nodeUpdater != null) {
            nodeUpdater.stop();
        }
    }

    public Map<String, NodeStatus> getNodes()
    {
        return nodes.get();
    }

    private Map<String, NodeStatus> retrieveNodes()
    {
        return resourceManagerClient.get().getNodeStatuses();
    }
}

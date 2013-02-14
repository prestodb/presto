package com.facebook.presto.importer;

import com.facebook.presto.metadata.Node;
import com.facebook.presto.metadata.NodeManager;
import com.google.common.collect.Sets;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.util.Threads.threadsNamed;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.Uninterruptibles.putUninterruptibly;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class NodeWorkerQueue
{
    private static final int tasksPerNode = 32;

    private final ScheduledExecutorService nodeRefreshExecutor = newSingleThreadScheduledExecutor(threadsNamed("node-refresh-%s"));
    private final BlockingQueue<Node> nodeWorkers = new LinkedBlockingDeque<>();
    private final Set<Node> nodes = new HashSet<>();
    private final NodeManager nodeManager;

    @Inject
    public NodeWorkerQueue(NodeManager nodeManager)
    {
        this.nodeManager = checkNotNull(nodeManager, "nodeManager is null");
    }

    @PostConstruct
    public void start()
    {
        nodeRefreshExecutor.scheduleWithFixedDelay(new NodeRefreshJob(), 0, 10, TimeUnit.SECONDS);
    }

    @PreDestroy
    public void stop()
    {
        nodeRefreshExecutor.shutdownNow();
    }

    public Node acquireNodeWorker()
            throws InterruptedException
    {
        return nodeWorkers.take();
    }

    public void releaseNodeWorker(Node worker)
    {
        putUninterruptibly(nodeWorkers, worker);
    }

    private class NodeRefreshJob
            implements Runnable
    {
        @Override
        public void run()
        {
            // TODO: handle nodes changing their URI
            // TODO: handle nodes disappearing
            Set<Node> activeNodes = nodeManager.getActiveDatasourceNodes("native");
            Set<Node> newNodes = Sets.difference(activeNodes, nodes);

            // add node workers in round-robin node order
            for (int i = 0; i < tasksPerNode; i++) {
                for (Node node : newNodes) {
                    putUninterruptibly(nodeWorkers, node);
                }
            }
        }
    }
}

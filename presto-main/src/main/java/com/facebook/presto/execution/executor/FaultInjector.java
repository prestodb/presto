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
package com.facebook.presto.execution.executor;

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.node.NodeInfo;
import com.facebook.presto.server.GracefulShutdownHandler;
import com.facebook.presto.server.ServerConfig;
import com.facebook.presto.util.PeriodicTaskExecutor;
import io.airlift.units.Duration;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newScheduledThreadPool;

public class FaultInjector
{
    private final NodeInfo nodeInfo;
    private final ServerConfig serverConfig;
    private final GracefulShutdownHandler shutdownHandler;
    private final ScheduledExecutorService refreshExecutor = newScheduledThreadPool(2, daemonThreadsNamed("FaultInjector"));
    private final PeriodicTaskExecutor gracefulShutdownExecutor;
    private Duration resourceGroupRunTimeInfoRefreshInterval = new Duration(100, TimeUnit.MILLISECONDS);
    private final TaskExecutor taskExecutor;
    private static final Logger log = Logger.get(FaultInjector.class);
    //try with one node going down, multiple node going down at the same time may have some corner case issues to be fixed
    private final AtomicBoolean isShutDown = new AtomicBoolean(false);

    @Inject
    public FaultInjector(NodeInfo nodeInfo, ServerConfig serverConfig, GracefulShutdownHandler shutdownHandler, TaskExecutor taskExecutor)
    {
        this.nodeInfo = nodeInfo;
        this.shutdownHandler = shutdownHandler;
        this.gracefulShutdownExecutor = new PeriodicTaskExecutor(resourceGroupRunTimeInfoRefreshInterval.toMillis(), refreshExecutor, this::refreshShutdownCriteria);
        this.taskExecutor = taskExecutor;
        this.serverConfig = serverConfig;
    }

//    @PostConstruct
    public void start()
    {
        gracefulShutdownExecutor.start();
    }

    @PreDestroy
    public void destroy()
    {
        refreshExecutor.shutdownNow();
        gracefulShutdownExecutor.stop();
    }

    private void refreshShutdownCriteria()
    {
        if (!this.serverConfig.getPoolType().isPresent() || !ServerConfig.WORKER_POOL_TYPE_LEAF.equals(this.serverConfig.getPoolType().get())) {
            return;
        }

        //don't launch shutdown handle other nodes
//        if (!nodeInfo.getNodeId().startsWith("node-2")) {
//            return;
//        }
        if (isShutDown.get()) {
            return;
        }
        int queuedSplit = taskExecutor.getTaskList().stream().mapToInt(taskHandle -> taskHandle.getQueuedSplitSize()).sum();
        int runningLeafSplit = taskExecutor.getTaskList().stream().mapToInt(taskHandle -> taskHandle.getRunningLeafSplits()).sum();
        if (queuedSplit > 5 && runningLeafSplit > 0 && !isShutDown.get()) {
            log.warn("Shutting down node - %s", nodeInfo.getNodeId());
            shutdownHandler.requestShutdown();
            isShutDown.set(true);
        }
    }
}

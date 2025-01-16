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
package com.facebook.presto.event;

import com.facebook.presto.dispatcher.DispatchManager;
import com.facebook.presto.server.BasicQueryInfo;
import io.airlift.units.Duration;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class QueryProgressMonitor
{
    private final AtomicLong monotonicallyIncreasingEventId = new AtomicLong();

    private final QueryMonitor queryMonitor;
    private final DispatchManager dispatchManager;
    private final Duration queryProgressPublishInterval;

    @GuardedBy("this")
    private ScheduledExecutorService queryProgressMonitorExecutor;

    @Inject
    public QueryProgressMonitor(
            QueryMonitor queryMonitor,
            DispatchManager dispatchManager,
            QueryMonitorConfig queryMonitorConfig)
    {
        this.queryMonitor = requireNonNull(queryMonitor, "queryMonitor is null");
        this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
        this.queryProgressPublishInterval = requireNonNull(queryMonitorConfig, "queryMonitorConfig is null").getQueryProgressPublishInterval();
    }

    @PostConstruct
    public synchronized void start()
    {
        if (queryProgressPublishInterval.getValue() > 0) {
            if (queryProgressMonitorExecutor == null) {
                queryProgressMonitorExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("query-progress-monitor-executor"));
            }

            queryProgressMonitorExecutor.scheduleWithFixedDelay(
                    this::publishQueryProgressEvent,
                    (long) queryProgressPublishInterval.getValue(),
                    (long) queryProgressPublishInterval.getValue(),
                    queryProgressPublishInterval.getUnit());
        }
    }

    @PreDestroy
    public synchronized void stop()
    {
        if (queryProgressMonitorExecutor != null) {
            queryProgressMonitorExecutor.shutdown();
        }
    }

    private void publishQueryProgressEvent()
    {
        for (BasicQueryInfo basicQueryInfo : dispatchManager.getQueries()) {
            if (!basicQueryInfo.getState().isDone()) {
                queryMonitor.publishQueryProgressEvent(monotonicallyIncreasingEventId.incrementAndGet(), basicQueryInfo);
            }
        }
    }
}

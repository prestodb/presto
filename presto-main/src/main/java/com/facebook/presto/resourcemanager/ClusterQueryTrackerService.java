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
package com.facebook.presto.resourcemanager;

import com.facebook.drift.client.DriftClient;
import com.facebook.presto.util.PeriodicTaskExecutor;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;

public class ClusterQueryTrackerService
{
    private final DriftClient<ResourceManagerClient> resourceManagerClient;
    private final ScheduledExecutorService executorService;
    private final long runningTaskCountFetchIntervalMillis;
    private AtomicInteger runningTaskCount;
    private final PeriodicTaskExecutor runningTaskCountUpdater;

    @Inject
    public ClusterQueryTrackerService(
            @ForResourceManager DriftClient<ResourceManagerClient> resourceManagerClient,
            @ForResourceManager ScheduledExecutorService executorService,
            ResourceManagerConfig resourceManagerConfig)
    {
        this.resourceManagerClient = requireNonNull(resourceManagerClient, "resourceManagerClient is null");
        this.executorService = requireNonNull(executorService, "executorService is null");
        this.runningTaskCountFetchIntervalMillis = requireNonNull(resourceManagerConfig, "resourceManagerConfig is null").getRunningTaskCountFetchInterval().toMillis();
        this.runningTaskCount = new AtomicInteger(0);
        this.runningTaskCountUpdater = new PeriodicTaskExecutor(runningTaskCountFetchIntervalMillis, executorService, () -> updateRunningTaskCount());
    }

    @PostConstruct
    public void init()
    {
        runningTaskCountUpdater.start();
    }

    @PreDestroy
    public void stop()
    {
        runningTaskCountUpdater.stop();
    }

    public int getRunningTaskCount()
    {
        return runningTaskCount.get();
    }

    private void updateRunningTaskCount()
    {
        this.runningTaskCount.set(resourceManagerClient.get().getRunningTaskCount());
    }
}

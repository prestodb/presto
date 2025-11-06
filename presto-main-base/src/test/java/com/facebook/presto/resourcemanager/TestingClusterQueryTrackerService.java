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
import com.facebook.presto.server.InternalCommunicationConfig;

import java.util.concurrent.ScheduledExecutorService;

public class TestingClusterQueryTrackerService
        extends ClusterQueryTrackerService
{
    DriftClient<ResourceManagerClient> resourceManagerClient;
    int runningTaskCount;

    public TestingClusterQueryTrackerService(DriftClient<ResourceManagerClient> resourceManagerClient, HttpResourceManagerClient httpResourceManagerClient, ScheduledExecutorService executorService, ResourceManagerConfig resourceManagerConfig, InternalCommunicationConfig internalCommunicationConfig, int runningTaskCount)
    {
        super(resourceManagerClient, httpResourceManagerClient, executorService, resourceManagerConfig, internalCommunicationConfig);
        this.runningTaskCount = runningTaskCount;
    }

    @Override
    public int getRunningTaskCount()
    {
        return runningTaskCount;
    }
}

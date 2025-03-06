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
package com.facebook.presto.router.cluster;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;

import javax.validation.constraints.NotNull;

import java.time.Duration;

public class RemoteStateConfig
{
    private Duration clusterUnhealthyTimeout;
    private Duration pollingInterval;

    @Config("router.remote-state.cluster-unhealthy-timeout")
    @ConfigDescription("The amount of that a cluster must remain unresponsive to health checks in order to be deemed \"unhealthy\"")
    public RemoteStateConfig setClusterUnhealthyTimeout(String clusterUnhealthyTimeout)
    {
        this.clusterUnhealthyTimeout = Duration.parse(clusterUnhealthyTimeout);
        return this;
    }

    public void setClusterUnhealthyTimeout(Duration timeToUnhealthy)
    {
        this.clusterUnhealthyTimeout = timeToUnhealthy;
    }

    @NotNull
    public Duration getClusterUnhealthyTimeout()
    {
        return this.clusterUnhealthyTimeout;
    }

    @Config("router.remote-state.polling-interval")
    @ConfigDescription("The amount of time between attached cluster health checks. This value being lower than cluster-unhealthy-timeout can lead to unexpected behavior")
    public RemoteStateConfig setPollingInterval(String pollingInterval)
    {
        this.pollingInterval = Duration.parse(pollingInterval);
        return this;
    }

    public void setPollingInterval(Duration pollingInterval)
    {
        this.pollingInterval = pollingInterval;
    }

    @NotNull
    public Duration getPollingInterval()
    {
        return this.pollingInterval;
    }
}

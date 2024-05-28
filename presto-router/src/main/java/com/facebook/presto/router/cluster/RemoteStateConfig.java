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
import io.airlift.units.Duration;

import javax.validation.constraints.NotNull;

import static java.util.concurrent.TimeUnit.SECONDS;

public class RemoteStateConfig
{
    private Duration clusterUnhealthyTimeout = new Duration(30, SECONDS);
    private Duration pollingInterval = new Duration(5, SECONDS);

    @Config("router.remote-state.cluster-unhealthy-timeout")
    @ConfigDescription("The amount of that a cluster must remain unresponsive to health checks in order to be deemed \"unhealthy\"")
    public RemoteStateConfig setClusterUnhealthyTimeout(Duration clusterUnhealthyTimeout)
    {
        this.clusterUnhealthyTimeout = clusterUnhealthyTimeout;
        return this;
    }

    @NotNull
    public Duration getClusterUnhealthyTimeout()
    {
        return this.clusterUnhealthyTimeout;
    }

    @Config("router.remote-state.polling-interval")
    @ConfigDescription("The amount of time between attached cluster health checks. This value being lower than cluster-unhealthy-timeout can lead to unexpected behavior")
    public RemoteStateConfig setPollingInterval(Duration pollingInterval)
    {
        this.pollingInterval = pollingInterval;
        return this;
    }

    @NotNull
    public Duration getPollingInterval()
    {
        return this.pollingInterval;
    }
}

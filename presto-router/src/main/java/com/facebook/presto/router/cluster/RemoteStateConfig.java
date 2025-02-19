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
    private Duration timeToUnhealthy;

    @Config("router.remote-state.cluster-unhealthy-timeout")
    @ConfigDescription("The amount of time in seconds that a cluster must remain unresponsive to health checks in order to be deemed \"unhealthy\"")
    public RemoteStateConfig setTimeToUnhealthy(String timeToUnhealthy)
    {
        this.timeToUnhealthy = Duration.parse(timeToUnhealthy);
        return this;
    }

    public RemoteStateConfig setTimeToUnhealthy(Duration timeToUnhealthy)
    {
        this.timeToUnhealthy = timeToUnhealthy;
        return this;
    }

    @NotNull
    public Duration getTimeToUnhealthy()
    {
        return this.timeToUnhealthy;
    }
}

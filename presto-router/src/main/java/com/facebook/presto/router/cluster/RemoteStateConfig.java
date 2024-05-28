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

public class RemoteStateConfig
{
    private long secondsToUnhealthy;

    @Config("router.remote-state.cluster-unhealthy-timeout")
    @ConfigDescription("The amount of time in seconds that a cluster must remain unresponsive to health checks in order to be deemed \"unhealthy\"")
    public RemoteStateConfig setSecondsToUnhealthy(long secondsToUnhealthy)
    {
        this.secondsToUnhealthy = secondsToUnhealthy;
        return this;
    }

    @NotNull
    public long getSecondsToUnhealthy()
    {
        return this.secondsToUnhealthy;
    }
}

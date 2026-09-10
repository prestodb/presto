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
package com.facebook.presto.sidecar;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.facebook.airlift.units.Duration;
import com.facebook.airlift.units.MinDuration;
import jakarta.validation.constraints.NotNull;

import static java.util.concurrent.TimeUnit.SECONDS;

// Shared retry configuration for sidecar HTTP clients; used by SidecarRetryDriver.
public class SidecarRetryConfig
{
    public static final String CONFIG_PREFIX = "sidecar.retry";

    private Duration maxFailureInterval = new Duration(30, SECONDS);

    @NotNull
    @MinDuration("1ms")
    public Duration getMaxFailureInterval()
    {
        return maxFailureInterval;
    }

    @Config("max-failure-interval")
    @ConfigDescription("Maximum duration to keep retrying transient sidecar HTTP failures before giving up; this value directly caps planning-path latency for per-query sidecar calls (expression optimization, plan validation)")
    public SidecarRetryConfig setMaxFailureInterval(Duration maxFailureInterval)
    {
        this.maxFailureInterval = maxFailureInterval;
        return this;
    }
}

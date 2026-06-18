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
package com.facebook.presto.sidecar.functionNamespace;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.facebook.airlift.units.Duration;

import static java.util.concurrent.TimeUnit.MINUTES;

public class NativeFunctionNamespaceManagerConfig
{
    private int sidecarNumRetries = 8;
    private Duration sidecarRetryDelay = new Duration(1, MINUTES);

    public int getSidecarNumRetries()
    {
        return sidecarNumRetries;
    }

    @Config("sidecar.num-retries")
    @ConfigDescription("Max times to retry fetching sidecar node")
    public NativeFunctionNamespaceManagerConfig setSidecarNumRetries(int sidecarNumRetries)
    {
        this.sidecarNumRetries = sidecarNumRetries;
        return this;
    }

    public Duration getSidecarRetryDelay()
    {
        return sidecarRetryDelay;
    }

    @Config("sidecar.retry-delay")
    @ConfigDescription("Cooldown period to retry when fetching sidecar node fails")
    public NativeFunctionNamespaceManagerConfig setSidecarRetryDelay(Duration sidecarRetryDelay)
    {
        this.sidecarRetryDelay = sidecarRetryDelay;
        return this;
    }
}

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
package com.facebook.presto.builtin.tools;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;

import java.time.Duration;

public class NativeSidecarRegistryToolConfig
{
    private int nativeSidecarRegistryToolNumRetries = 8;
    private long nativeSidecarRegistryToolRetryDelayMs = Duration.ofMinutes(1).toMillis();

    public int getNativeSidecarRegistryToolNumRetries()
    {
        return nativeSidecarRegistryToolNumRetries;
    }

    @Config("native-sidecar-registry-tool.num-retries")
    @ConfigDescription("Max times to retry fetching sidecar node")
    public NativeSidecarRegistryToolConfig setNativeSidecarRegistryToolNumRetries(int nativeSidecarRegistryToolNumRetries)
    {
        this.nativeSidecarRegistryToolNumRetries = nativeSidecarRegistryToolNumRetries;
        return this;
    }

    public long getNativeSidecarRegistryToolRetryDelayMs()
    {
        return nativeSidecarRegistryToolRetryDelayMs;
    }

    @Config("native-sidecar-registry-tool.retry-delay-ms")
    @ConfigDescription("Cooldown period to retry when fetching sidecar node fails")
    public NativeSidecarRegistryToolConfig setNativeSidecarRegistryToolRetryDelayMs(long nativeSidecarRegistryToolRetryDelayMs)
    {
        this.nativeSidecarRegistryToolRetryDelayMs = nativeSidecarRegistryToolRetryDelayMs;
        return this;
    }
}

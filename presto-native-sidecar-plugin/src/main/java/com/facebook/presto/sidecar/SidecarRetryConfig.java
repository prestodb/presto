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

import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * Shared retry configuration for all HTTP clients that communicate with the native sidecar.
 * <p>
 * The retry mechanism mirrors the {@code Backoff}-based approach used in
 * {@code PageBufferClient} / ExchangeClient: on each transient failure the caller
 * sleeps for an exponentially-increasing delay before retrying, and gives up only
 * after failures have continued for longer than {@code maxFailureInterval}.
 */
public class SidecarRetryConfig
{
    public static final String CONFIG_PREFIX = "sidecar.retry";

    private Duration maxFailureInterval = new Duration(1, MINUTES);

    public Duration getMaxFailureInterval()
    {
        return maxFailureInterval;
    }

    @Config("max-failure-interval")
    @ConfigDescription("Maximum duration to keep retrying transient sidecar HTTP failures before giving up")
    public SidecarRetryConfig setMaxFailureInterval(Duration maxFailureInterval)
    {
        this.maxFailureInterval = maxFailureInterval;
        return this;
    }
}

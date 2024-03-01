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
package com.facebook.presto.failureDetector;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.DecimalMax;
import javax.validation.constraints.DecimalMin;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

public class FailureDetectorConfig
{
    private boolean enabled = true;
    private double failureRatioThreshold = 0.1; // ~6secs of failures, given default setting of heartbeatInterval = 500ms
    private Duration heartbeatInterval = new Duration(500, TimeUnit.MILLISECONDS);
    private Duration warmupInterval = new Duration(5, TimeUnit.SECONDS);
    private Duration expirationGraceInterval = new Duration(10, TimeUnit.MINUTES);
    private int exponentialDecaySeconds = 60;

    @NotNull
    public Duration getExpirationGraceInterval()
    {
        return expirationGraceInterval;
    }

    @Config("failure-detector.expiration-grace-interval")
    @ConfigDescription("How long to wait before 'forgetting' a service after it disappears from discovery")
    public FailureDetectorConfig setExpirationGraceInterval(Duration expirationGraceInterval)
    {
        this.expirationGraceInterval = expirationGraceInterval;
        return this;
    }

    public boolean isEnabled()
    {
        return enabled;
    }

    @Config("failure-detector.enabled")
    public FailureDetectorConfig setEnabled(boolean enabled)
    {
        this.enabled = enabled;
        return this;
    }

    @NotNull
    public Duration getWarmupInterval()
    {
        return warmupInterval;
    }

    @Config("failure-detector.warmup-interval")
    @ConfigDescription("How long to wait after transitioning to success before considering a service alive")
    public FailureDetectorConfig setWarmupInterval(Duration warmupInterval)
    {
        this.warmupInterval = warmupInterval;
        return this;
    }

    @MinDuration("1ms")
    @NotNull
    public Duration getHeartbeatInterval()
    {
        return heartbeatInterval;
    }

    @Config("failure-detector.heartbeat-interval")
    public FailureDetectorConfig setHeartbeatInterval(Duration interval)
    {
        this.heartbeatInterval = interval;
        return this;
    }

    @DecimalMin("0.0")
    @DecimalMax("1.0")
    public double getFailureRatioThreshold()
    {
        return failureRatioThreshold;
    }

    @Config("failure-detector.threshold")
    public FailureDetectorConfig setFailureRatioThreshold(double threshold)
    {
        this.failureRatioThreshold = threshold;
        return this;
    }

    @Min(1)
    public int getExponentialDecaySeconds()
    {
        return exponentialDecaySeconds;
    }

    @Config("failure-detector.exponential-decay-seconds")
    public FailureDetectorConfig setExponentialDecaySeconds(int exponentialDecaySeconds)
    {
        this.exponentialDecaySeconds = exponentialDecaySeconds;
        return this;
    }
}

package com.facebook.presto.server;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.DecimalMax;
import javax.validation.constraints.DecimalMin;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.concurrent.TimeUnit;

public class FailureDetectorConfiguration
{
    private boolean enabled = true;
    private double failureRatioThreshold = 0.01; // 1% failure rate
    private Duration hearbeatInterval = new Duration(500, TimeUnit.MILLISECONDS);
    private Duration warmupInterval = new Duration(5, TimeUnit.SECONDS);

    public boolean isEnabled()
    {
        return enabled;
    }

    @Config("failure-detector.enabled")
    public FailureDetectorConfiguration setEnabled(boolean enabled)
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
    @ConfigDescription("How long to wait before considering a new node alive")
    public FailureDetectorConfiguration setWarmupInterval(Duration warmupInterval)
    {
        this.warmupInterval = warmupInterval;
        return this;
    }

    @MinDuration("1ms")
    @NotNull
    public Duration getHearbeatInterval()
    {
        return hearbeatInterval;
    }

    @Config("failure-detector.heartbeat-interval")
    public FailureDetectorConfiguration setHearbeatInterval(Duration interval)
    {
        this.hearbeatInterval = interval;
        return this;
    }

    @DecimalMin("0.0")
    @DecimalMax("1.0")
    public double getFailureRatioThreshold()
    {
        return failureRatioThreshold;
    }

    @Config("failure-detector.threshold")
    public FailureDetectorConfiguration setFailureRatioThreshold(double threshold)
    {
        this.failureRatioThreshold = threshold;
        return this;
    }
}

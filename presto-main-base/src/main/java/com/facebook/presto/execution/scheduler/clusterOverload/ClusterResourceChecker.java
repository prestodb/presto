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
package com.facebook.presto.execution.scheduler.clusterOverload;

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.stats.CounterStat;
import com.facebook.airlift.stats.TimeStat;
import com.facebook.presto.execution.ClusterOverloadConfig;
import com.facebook.presto.metadata.InternalNodeManager;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Objects.requireNonNull;

/**
 * Provides methods to check if more queries can be run on the cluster
 * based on various resource constraints.
 */
@Singleton
@ThreadSafe
public class ClusterResourceChecker
{
    private static final Logger log = Logger.get(ClusterResourceChecker.class);

    private final NodeOverloadPolicy nodeOverloadPolicy;
    private final ClusterOverloadConfig config;
    private final AtomicBoolean cachedOverloadState = new AtomicBoolean(false);
    private final AtomicLong lastCheckTimeMillis = new AtomicLong(0);
    private final CounterStat overloadDetectionCount = new CounterStat();
    private final TimeStat timeSinceLastCheck = new TimeStat();
    private final AtomicLong overloadStartTimeMillis = new AtomicLong(0);

    @Inject
    public ClusterResourceChecker(NodeOverloadPolicy nodeOverloadPolicy, ClusterOverloadConfig config)
    {
        this.nodeOverloadPolicy = requireNonNull(nodeOverloadPolicy, "nodeOverloadPolicy is null");
        this.config = requireNonNull(config, "config is null");
    }

    /**
     * Checks if more queries can be run on the cluster based on overload.
     * Uses a cached result for a configurable time period to avoid frequent expensive checks.
     *
     * @param nodeManager The node manager to get cluster wide worker node information
     * @return true if more queries can be run, false otherwise
     */
    public boolean canRunMoreOnCluster(InternalNodeManager nodeManager)
    {
        // If throttling is disabled, always allow more queries
        if (!isClusterOverloadThrottlingEnabled()) {
            return true;
        }

        long currentTimeMillis = System.currentTimeMillis();
        long lastCheckTime = lastCheckTimeMillis.get();

        // If cache is still valid, use the cached value
        if (currentTimeMillis - lastCheckTime <= config.getOverloadCheckCacheTtlMillis()) {
            return !cachedOverloadState.get();
        }

        // Cache is expired, need to update it - use synchronized block to prevent multiple threads
        // from performing the expensive check simultaneously
        synchronized (this) {
            // Re-check the time since another thread might have updated while we were waiting
            currentTimeMillis = System.currentTimeMillis();
            lastCheckTime = lastCheckTimeMillis.get();
            if (currentTimeMillis - lastCheckTime <= config.getOverloadCheckCacheTtlMillis()) {
                // Another thread updated the cache while we were waiting
                return !cachedOverloadState.get();
            }

            // Record time since last check
            if (lastCheckTime > 0) {
                timeSinceLastCheck.add(currentTimeMillis - lastCheckTime, java.util.concurrent.TimeUnit.MILLISECONDS);
            }

            // Perform the actual check
            boolean isOverloaded = nodeOverloadPolicy.isClusterOverloaded(nodeManager);
            boolean wasOverloaded = cachedOverloadState.getAndSet(isOverloaded);
            lastCheckTimeMillis.set(currentTimeMillis);

            // Track overload state changes
            if (isOverloaded && !wasOverloaded) {
                overloadDetectionCount.update(1);
                overloadStartTimeMillis.set(currentTimeMillis);
                log.info("Cluster entered overloaded state");
            }
            else if (!isOverloaded && wasOverloaded) {
                log.info("Cluster exited overloaded state after %d ms", currentTimeMillis - overloadStartTimeMillis.get());
                overloadStartTimeMillis.set(0);
            }

            log.debug("Updated cluster overload state: %s (cache TTL: %d ms)", isOverloaded ? "OVERLOADED" : "NOT OVERLOADED", config.getOverloadCheckCacheTtlMillis());
            return !isOverloaded;
        }
    }

    /**
     * Returns whether cluster overload throttling is enabled.
     * When disabled, canRunMoreOnCluster will always return true regardless of cluster state.
     *
     * @return true if throttling is enabled, false otherwise
     */
    @Managed
    public boolean isClusterOverloadThrottlingEnabled()
    {
        return config.isClusterOverloadThrottlingEnabled();
    }

    /**
     * Returns whether the cluster is currently in an overloaded state.
     * This is exposed as a JMX metric for monitoring.
     *
     * @return true if the cluster is overloaded, false otherwise
     */
    @Managed
    public boolean isClusterOverloaded()
    {
        return cachedOverloadState.get();
    }

    /**
     * Returns the number of times the cluster has entered an overloaded state.
     *
     * @return counter of overload detections
     */
    @Managed
    @Nested
    public CounterStat getOverloadDetectionCount()
    {
        return overloadDetectionCount;
    }

    /**
     * Returns statistics about the time between overload checks.
     *
     * @return time statistics for overload checks
     */
    @Managed
    @Nested
    public TimeStat getTimeSinceLastCheck()
    {
        return timeSinceLastCheck;
    }

    /**
     * Returns the duration in milliseconds that the cluster has been in an overloaded state.
     * Returns 0 if the cluster is not currently overloaded.
     *
     * @return duration in milliseconds of current overload state
     */
    @Managed
    public long getOverloadDurationMillis()
    {
        long startTime = overloadStartTimeMillis.get();
        if (startTime == 0 || !cachedOverloadState.get()) {
            return 0;
        }
        return System.currentTimeMillis() - startTime;
    }
}

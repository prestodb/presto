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
import com.google.errorprone.annotations.ThreadSafe;
import com.google.inject.Singleton;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.airlift.concurrent.Threads.threadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

/**
 * Provides methods to check if more queries can be run on the cluster
 * based on various resource constraints.
 */
@Singleton
@ThreadSafe
public class ClusterResourceChecker
{
    private static final Logger log = Logger.get(ClusterResourceChecker.class);

    private final ClusterOverloadPolicy clusterOverloadPolicy;
    private final ClusterOverloadConfig config;
    private final AtomicBoolean cachedOverloadState = new AtomicBoolean(false);
    private final AtomicLong lastCheckTimeMillis = new AtomicLong(0);
    private final CounterStat overloadDetectionCount = new CounterStat();
    private final TimeStat timeSinceLastCheck = new TimeStat();
    private final AtomicLong overloadStartTimeMillis = new AtomicLong(0);
    private final CopyOnWriteArrayList<ClusterOverloadStateListener> listeners = new CopyOnWriteArrayList<>();

    private final ScheduledExecutorService overloadCheckerExecutor;
    private final InternalNodeManager nodeManager;

    @Inject
    public ClusterResourceChecker(ClusterOverloadPolicy clusterOverloadPolicy, ClusterOverloadConfig config, InternalNodeManager nodeManager)
    {
        this.clusterOverloadPolicy = requireNonNull(clusterOverloadPolicy, "clusterOverloadPolicy is null");
        this.config = requireNonNull(config, "config is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.overloadCheckerExecutor = newSingleThreadScheduledExecutor(threadsNamed("cluster-overload-checker-%s"));
    }

    @PostConstruct
    public void start()
    {
        if (config.isClusterOverloadThrottlingEnabled()) {
            long checkIntervalMillis = Math.max(1000, config.getOverloadCheckCacheTtlInSecs() * 1000L);
            overloadCheckerExecutor.scheduleWithFixedDelay(() -> {
                try {
                    performPeriodicOverloadCheck();
                }
                catch (Exception e) {
                    log.error(e, "Error polling cluster overload state");
                }
            }, checkIntervalMillis, checkIntervalMillis, TimeUnit.MILLISECONDS);
            log.info("Started periodic cluster overload checker with interval: %d milliseconds", checkIntervalMillis);
            // Perform initial check
            performPeriodicOverloadCheck();
        }
    }

    @PreDestroy
    public void stop()
    {
        overloadCheckerExecutor.shutdownNow();
    }

    /**
     * Registers a listener to be notified when the cluster exits the overloaded state.
     *
     * @param listener the listener to register
     */
    public void addListener(ClusterOverloadStateListener listener)
    {
        requireNonNull(listener, "listener is null");
        listeners.add(listener);
    }

    /**
     * Removes a previously registered listener.
     *
     * @param listener the listener to remove
     */
    public void removeListener(ClusterOverloadStateListener listener)
    {
        listeners.remove(listener);
    }

    /**
     * Performs a periodic check of cluster overload state.
     * This method is called by the periodic task when throttling is enabled.
     * Updates JMX metrics and notifies listeners when cluster exits overloaded state.
     */
    private void performPeriodicOverloadCheck()
    {
        try {
            long currentTimeMillis = System.currentTimeMillis();
            long lastCheckTime = lastCheckTimeMillis.get();

            if (lastCheckTime > 0) {
                timeSinceLastCheck.add(currentTimeMillis - lastCheckTime, TimeUnit.MILLISECONDS);
            }

            boolean isOverloaded = clusterOverloadPolicy.isClusterOverloaded(nodeManager);
            synchronized (this) {
                boolean wasOverloaded = cachedOverloadState.getAndSet(isOverloaded);
                lastCheckTimeMillis.set(currentTimeMillis);

                if (isOverloaded && !wasOverloaded) {
                    overloadDetectionCount.update(1);
                    overloadStartTimeMillis.set(currentTimeMillis);
                    log.info("Cluster entered overloaded state via periodic check");
                }
                else if (!isOverloaded && wasOverloaded) {
                    long overloadDuration = currentTimeMillis - overloadStartTimeMillis.get();
                    log.info("Cluster exited overloaded state after %d ms via periodic check", overloadDuration);
                    overloadStartTimeMillis.set(0);
                    // Notify listeners that cluster exited overload state
                    notifyClusterExitedOverloadedState();
                }
            }

            log.debug("Periodic overload check completed: %s", isOverloaded ? "OVERLOADED" : "NOT OVERLOADED");
        }
        catch (Exception e) {
            log.error(e, "Error during periodic cluster overload check");
        }
    }

    /**
     * Returns the current overload state of the cluster.
     * @return true if cluster is overloaded, false otherwise
     */
    public boolean isClusterCurrentlyOverloaded()
    {
        if (!config.isClusterOverloadThrottlingEnabled()) {
            return false;
        }

        return cachedOverloadState.get();
    }

    /**
     * Notifies all registered listeners that the cluster has exited the overloaded state.
     */
    private void notifyClusterExitedOverloadedState()
    {
        for (ClusterOverloadStateListener listener : listeners) {
            listener.onClusterExitedOverloadedState();
        }
    }

    /**
     * Returns whether cluster overload throttling is enabled.
     * When disabled, the cluster overload check will be bypassed.
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
     * Note: This method reads two atomic fields but doesn't need synchronization because:
     * 1. Single writer (periodic task) ensures consistent updates
     * 2. Atomic fields provide memory visibility guarantees
     * 3. Slight inconsistency in edge cases is acceptable for monitoring metrics
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

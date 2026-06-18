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
package com.facebook.presto.server.remotetask;

import com.facebook.airlift.stats.DistributionStat;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;
import reactor.netty.resources.ConnectionPoolMetrics;
import reactor.netty.resources.ConnectionProvider;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Singleton
public class HttpClientConnectionPoolStats
        implements ConnectionProvider.MeterRegistrar
{
    private final ConcurrentHashMap<String, ConnectionPoolMetrics> poolMetrics = new ConcurrentHashMap<>();

    private final DistributionStat activeConnections = new DistributionStat();
    private final DistributionStat totalConnections = new DistributionStat();
    private final DistributionStat idleConnections = new DistributionStat();
    private final DistributionStat pendingAcquires = new DistributionStat();
    private final DistributionStat maxConnections = new DistributionStat();
    private final DistributionStat maxPendingAcquires = new DistributionStat();

    @Inject
    public HttpClientConnectionPoolStats()
    {
        scheduleStatsExport();
    }

    @Override
    public void registerMetrics(String poolName, String id, SocketAddress remoteAddress, ConnectionPoolMetrics metrics)
    {
        poolMetrics.put(createPoolKey(poolName, remoteAddress), metrics);
    }

    private static String createPoolKey(String poolName, SocketAddress remoteAddress)
    {
        return poolName + ":" + formatSocketAddress(remoteAddress);
    }

    private static String formatSocketAddress(SocketAddress socketAddress)
    {
        if (socketAddress != null) {
            if (socketAddress instanceof InetSocketAddress) {
                InetSocketAddress address = (InetSocketAddress) socketAddress;
                return address.getHostString().replace(".", "_");
            }
            else {
                return socketAddress.toString().replace(".", "_");
            }
        }
        return "UNKNOWN";
    }

    private void scheduleStatsExport()
    {
        Executors.newSingleThreadScheduledExecutor()
                .scheduleAtFixedRate(
                        () -> {
                            for (ConnectionPoolMetrics metrics : poolMetrics.values()) {
                                activeConnections.add(metrics.acquiredSize());
                                totalConnections.add(metrics.allocatedSize());
                                idleConnections.add(metrics.idleSize());
                                pendingAcquires.add(metrics.pendingAcquireSize());
                                maxConnections.add(metrics.maxAllocatedSize());
                                maxPendingAcquires.add(metrics.maxPendingAcquireSize());
                            }
                        },
                        0,
                        1,
                        TimeUnit.SECONDS);
    }

    @Managed
    @Nested
    public DistributionStat getActiveConnections()
    {
        return activeConnections;
    }

    @Managed
    @Nested
    public DistributionStat getTotalConnections()
    {
        return totalConnections;
    }

    @Managed
    @Nested
    public DistributionStat getIdleConnections()
    {
        return idleConnections;
    }

    @Managed
    @Nested
    public DistributionStat getPendingAcquires()
    {
        return pendingAcquires;
    }

    @Managed
    @Nested
    public DistributionStat getMaxConnections()
    {
        return maxConnections;
    }

    @Managed
    @Nested
    public DistributionStat getMaxPendingAcquires()
    {
        return maxPendingAcquires;
    }
}

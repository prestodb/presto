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

package com.facebook.presto.router.scheduler;

import com.facebook.presto.spi.router.ClusterInfo;
import com.facebook.presto.spi.router.RouterRequestInfo;
import com.facebook.presto.spi.router.Scheduler;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;

public class TestMetricsBasedScheduler
{
    private final Map<URI, ClusterInfo> clusterInfos = new HashMap<>();

    private static class MockRemoteClusterInfo
            implements ClusterInfo
    {
        private final long runningQueries;
        private final long queuedQueries;

        MockRemoteClusterInfo(long runningQueries, long queuedQueries)
        {
            this.runningQueries = runningQueries;
            this.queuedQueries = queuedQueries;
        }

        @Override
        public long getRunningQueries()
        {
            return runningQueries;
        }

        @Override
        public long getQueuedQueries()
        {
            return queuedQueries;
        }

        @Override
        public long getBlockedQueries()
        {
            return 0;
        }

        @Override
        public long getActiveWorkers()
        {
            return 0;
        }

        @Override
        public long getRunningDrivers()
        {
            return 0;
        }
    }

    @Test
    public void testMetricsBasedScheduler()
            throws Exception
    {
        Scheduler scheduler = new MetricsBasedScheduler();

        URI uri1 = new URI("192.168.0.1");
        URI uri2 = new URI("192.168.0.2");
        URI uri3 = new URI("192.168.0.3");

        clusterInfos.put(uri1, new MockRemoteClusterInfo(10, 10));
        clusterInfos.put(uri2, new MockRemoteClusterInfo(20, 20));
        clusterInfos.put(uri3, new MockRemoteClusterInfo(30, 30));
        scheduler.setClusterInfos(clusterInfos);
        URI target = scheduler.getDestination(new RouterRequestInfo("test")).orElseThrow(AssertionError::new);
        assertEquals(target, uri1);

        clusterInfos.put(uri1, new MockRemoteClusterInfo(20, 20));
        clusterInfos.put(uri2, new MockRemoteClusterInfo(10, 10));
        clusterInfos.put(uri3, new MockRemoteClusterInfo(30, 30));
        scheduler.setClusterInfos(clusterInfos);
        target = scheduler.getDestination(new RouterRequestInfo("test")).orElseThrow(AssertionError::new);
        assertEquals(target, uri2);

        clusterInfos.put(uri1, new MockRemoteClusterInfo(20, 20));
        clusterInfos.put(uri2, new MockRemoteClusterInfo(30, 30));
        clusterInfos.put(uri3, new MockRemoteClusterInfo(10, 10));
        scheduler.setClusterInfos(clusterInfos);
        target = scheduler.getDestination(new RouterRequestInfo("test")).orElseThrow(AssertionError::new);
        assertEquals(target, uri3);

        scheduler.setClusterInfos(new HashMap<>());
        target = scheduler.getDestination(new RouterRequestInfo("test")).orElse(new URI("invalid"));
        assertEquals(target, new URI("invalid"));
    }
}

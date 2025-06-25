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

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Comparator.comparingLong;

/**
 * Metrics based scheduler uses coordinator and/or worker metrics for scheduling decisions.
 */
public class MetricsBasedScheduler
        implements Scheduler
{
    private Map<URI, ClusterInfo> clusterInfos;

    /**
     * Returns the destination cluster URI with the fewest number of active and queued queries.
     */
    @Override
    public Optional<URI> getDestination(RouterRequestInfo routerRequestInfo)
    {
        if (clusterInfos != null && !clusterInfos.isEmpty()) {
            //Cluster with the fewest number of queries will be returned
            return clusterInfos.entrySet().stream()
                    .min(comparingLong(entry -> entry.getValue().getRunningQueries() + entry.getValue().getQueuedQueries()))
                    .map(Map.Entry::getKey);
        }
        return Optional.empty();
    }

    @Override
    public void setCandidates(List<URI> candidates) {}

    @Override
    public void setClusterInfos(Map<URI, ClusterInfo> clusterInfos)
    {
        this.clusterInfos = clusterInfos;
    }
}

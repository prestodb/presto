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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.router.ClusterInfo;
import com.facebook.presto.spi.router.Scheduler;

import java.net.URI;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

//Metrics based scheduler uses coordinator and/or worker metrics for scheduling decisions
public class MetricsBasedScheduler
        implements Scheduler
{
    private static final Logger log = Logger.get(MetricsBasedScheduler.class);
    Map<URI, ClusterInfo> clusterInfos;

    @Override
    public Optional<URI> getDestination(String user, String query)
    {
        try {
            if (clusterInfos != null && !clusterInfos.isEmpty()) {
                //Cluster with lowest number of queries will be returned
                return clusterInfos.entrySet().stream()
                        .min(Comparator.comparingLong(entry -> entry.getValue().getRunningQueries() + entry.getValue().getQueuedQueries()))
                        .map(Map.Entry::getKey);
            }
            return Optional.empty();
        }
        catch (IllegalArgumentException e) {
            log.warn(e, "Error getting destination for user " + user);
            return Optional.empty();
        }
    }

    @Override
    public void setCandidates(List<URI> candidates) {}

    @Override
    public void setClusterInfos(Map<URI, ClusterInfo> clusterInfos)
    {
        this.clusterInfos = clusterInfos;
    }
}

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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

//Metrics based scheduler uses coordinator and/or worker metrics for scheduling decisions
public class MetricsBasedScheduler
        implements Scheduler
{
    private static final Logger log = Logger.get(MetricsBasedScheduler.class);
    Map<URI, ClusterInfo> clusterInfos;

    //Cluster with lowest number of queries will be on top of the queue
    List<ClusterQueryInfo> clusterQueue = new ArrayList<>();

    private List<URI> candidates;

    private class ClusterQueryInfo
    {
        URI clusterUri;
        long runningQueries;
        long queuedQueries;
        long totalQueries;

        ClusterQueryInfo(URI clusterUri, long runningQueries, long queuedQueries)
        {
            this.clusterUri = clusterUri;
            this.runningQueries = runningQueries;
            this.queuedQueries = queuedQueries;
            totalQueries = (long) (runningQueries + queuedQueries);
            log.info("Cluster URI : " + clusterUri + ", runningQueries : " + runningQueries + ", queuedQueries : " + queuedQueries + ", totalQueries : " + totalQueries);
        }
    }

    @Override
    public Optional<URI> getDestination(String user, String query)
    {
        try {
            if (clusterInfos != null && !clusterInfos.isEmpty()) {
                clusterQueue.clear();
                clusterQueue.addAll(clusterInfos.keySet().stream()
                        .map(uri -> new ClusterQueryInfo(uri, clusterInfos.get(uri).getRunningQueries(), clusterInfos.get(uri).getQueuedQueries()))
                        .collect(Collectors.toList()));
                clusterQueue.sort((x, y) -> (int) (x.totalQueries - y.totalQueries));
                return Optional.of(clusterQueue.get(0).clusterUri);
            }
            return Optional.empty();
        }
        catch (IllegalArgumentException e) {
            log.warn(e, "Error getting destination for user " + user);
            return Optional.empty();
        }
    }

    @Override
    public void setCandidates(List<URI> candidates)
    {
        this.candidates = candidates;
    }

    @Override
    public void setClusterInfos(Map<URI, ClusterInfo> clusterInfos)
    {
        this.clusterInfos = clusterInfos;
    }
}

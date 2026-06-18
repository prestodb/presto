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
import com.facebook.presto.spi.router.RouterRequestInfo;
import com.facebook.presto.spi.router.Scheduler;

import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Collections.nCopies;

/**
* As the weighted round-robin scheduler, similar to the normal round-robin method,
* keeps the selected index as a state for scheduling, the candidates and weights
* shall not be modified after they are assigned.
* This design indicates that the weighted round-robin scheduler can only be used
* when the candidates and weights are always consistent. Otherwise, users need to
* create a new scheduler object to pick up the change.
 */
public class WeightedRoundRobinScheduler
        implements Scheduler
{
    private List<URI> candidates;
    private Map<URI, Integer> weights;
    private List<URI> serverList;

    private final Map<String, Integer> candidateIndexByGroup = new HashMap<>();
    private String candidateGroupName;
    private static final Logger log = Logger.get(WeightedRoundRobinScheduler.class);

    @Override
    public Optional<URI> getDestination(RouterRequestInfo routerRequestInfo)
    {
        int serverIndex = 0;

        checkArgument(candidates.size() == weights.size());

        if (serverList == null || weights.values().stream().mapToInt(Integer::intValue).sum() != serverList.size()) {
            this.generateServerList();
        }

        if (!candidateIndexByGroup.containsKey(candidateGroupName)) {
            candidateIndexByGroup.put(candidateGroupName, 0);
        }
        else {
            serverIndex = candidateIndexByGroup.get(candidateGroupName) + 1;
        }
        if (serverIndex >= serverList.size()) {
            serverIndex = 0;
        }
        candidateIndexByGroup.put(candidateGroupName, serverIndex);

        //If server list is empty (servers got filtered out due to 0 weight)
        //select the first candidate from candidate list
        if (serverList.isEmpty() && !candidates.isEmpty()) {
            return Optional.of(candidates.get(0));
        }

        return Optional.of(serverList.get(serverIndex));
    }

    @Override
    public void setCandidates(List<URI> candidates)
    {
        this.candidates = candidates;
    }

    public List<URI> getCandidates()
    {
        return candidates;
    }

    @Override
    public void setWeights(Map<URI, Integer> weights)
    {
        // Only keeps the first given `weights` due to maintaining the
        // selected index for weighted round-robin.
        if (this.weights == null) {
            this.weights = weights;
        }
    }

    public Map<URI, Integer> getWeights()
    {
        return weights;
    }

    private void generateServerList()
    {
        checkArgument(!candidates.isEmpty(), "Server candidates should not be empty");

        serverList = weights.keySet().stream()
                .map(uri -> nCopies(weights.get(uri), uri))
                .flatMap(Collection::stream)
                .collect(toImmutableList());
    }

    @Override
    public void setCandidateGroupName(String candidateGroupName)
    {
        this.candidateGroupName = candidateGroupName;
    }
}

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
package com.facebook.presto.router.cluster;

import com.facebook.presto.router.RouterConfig;
import com.facebook.presto.router.scheduler.Scheduler;
import com.facebook.presto.router.scheduler.SchedulerFactory;
import com.facebook.presto.router.scheduler.SchedulerType;
import com.facebook.presto.router.spec.GroupSpec;
import com.facebook.presto.router.spec.RouterSpec;
import com.facebook.presto.router.spec.SelectorRuleSpec;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.router.RouterUtil.parseRouterConfig;
import static com.facebook.presto.router.scheduler.SchedulerType.WEIGHTED_RANDOM_CHOICE;
import static com.facebook.presto.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.stream.Collectors.toMap;

public class ClusterManager
{
    private final Map<String, GroupSpec> groups;
    private final List<SelectorRuleSpec> groupSelectors;
    private final SchedulerType schedulerType;
    private final Scheduler scheduler;
    private final HashMap<String, HashMap<URI, Integer>> serverWeights = new HashMap<>();

    @Inject
    public ClusterManager(RouterConfig config)
    {
        RouterSpec routerSpec = parseRouterConfig(config)
                .orElseThrow(() -> new PrestoException(CONFIGURATION_INVALID, "Failed to load router config"));

        this.groups = ImmutableMap.copyOf(routerSpec.getGroups().stream().collect(toMap(GroupSpec::getName, group -> group)));
        this.groupSelectors = ImmutableList.copyOf(routerSpec.getSelectors());
        this.schedulerType = routerSpec.getSchedulerType();
        this.scheduler = new SchedulerFactory(routerSpec.getSchedulerType()).create();

        this.initializeServerWeights();
    }

    public List<URI> getAllClusters()
    {
        return groups.values().stream()
                .flatMap(groupSpec -> groupSpec.getMembers().stream())
                .collect(toImmutableList());
    }

    public Optional<URI> getDestination(RequestInfo requestInfo)
    {
        Optional<String> target = matchGroup(requestInfo);
        if (!target.isPresent()) {
            return Optional.empty();
        }

        checkArgument(groups.containsKey(target.get()));
        GroupSpec groupSpec = groups.get(target.get());
        scheduler.setCandidates(groupSpec.getMembers());
        if (schedulerType == WEIGHTED_RANDOM_CHOICE) {
            scheduler.setWeights(serverWeights.get(groupSpec.getName()));
        }

        return scheduler.getDestination(requestInfo.getUser());
    }

    private Optional<String> matchGroup(RequestInfo requestInfo)
    {
        return groupSelectors.stream()
                .map(s -> s.match(requestInfo))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();
    }

    private void initializeServerWeights()
    {
        groups.forEach((name, groupSpec) -> {
            List<URI> members = groupSpec.getMembers();
            List<Integer> weights = groupSpec.getWeights();
            serverWeights.put(name, new HashMap<>());
            for (int i = 0; i < members.size(); i++) {
                serverWeights.get(name).put(members.get(i), weights.get(i));
            }
        });
    }
}

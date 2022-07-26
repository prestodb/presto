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
package com.facebook.presto.router.spec;

import com.facebook.presto.router.scheduler.SchedulerType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.router.scheduler.SchedulerType.RANDOM_CHOICE;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class RouterSpec
{
    private final List<GroupSpec> groups;
    private final List<SelectorRuleSpec> selectors;
    private final Optional<SchedulerType> schedulerType;

    @JsonCreator
    public RouterSpec(
            @JsonProperty("groups") List<GroupSpec> groups,
            @JsonProperty("selectors") List<SelectorRuleSpec> selectors,
            @JsonProperty("scheduler") Optional<SchedulerType> schedulerType)
    {
        this.groups = ImmutableList.copyOf(requireNonNull(groups, "groups is null"));
        this.selectors = ImmutableList.copyOf(requireNonNull(selectors, "selectors is null"));
        this.schedulerType = requireNonNull(schedulerType, "scheduleType is null");

        // make sure no duplicate names in group definition
        checkArgument(groups.stream()
                .map(GroupSpec::getName)
                .allMatch(new HashSet<>()::add));
    }

    @JsonProperty
    public List<GroupSpec> getGroups()
    {
        return groups;
    }

    @JsonProperty
    public List<SelectorRuleSpec> getSelectors()
    {
        return selectors;
    }

    @JsonProperty
    public SchedulerType getSchedulerType()
    {
        return schedulerType.orElse(RANDOM_CHOICE);
    }
}

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
package com.facebook.presto.resourceGroups;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class ManagerSpec
{
    private final List<ResourceGroupSpec> rootGroups;
    private final List<SelectorSpec> selectors;
    private final Optional<Duration> cpuQuotaPeriod;

    @JsonCreator
    public ManagerSpec(
            @JsonProperty("rootGroups") List<ResourceGroupSpec> rootGroups,
            @JsonProperty("selectors") List<SelectorSpec> selectors,
            @JsonProperty("cpuQuotaPeriod") Optional<Duration> cpuQuotaPeriod)
    {
        this.rootGroups = ImmutableList.copyOf(requireNonNull(rootGroups, "rootGroups is null"));
        this.selectors = ImmutableList.copyOf(requireNonNull(selectors, "selectors is null"));
        this.cpuQuotaPeriod = requireNonNull(cpuQuotaPeriod, "cpuQuotaPeriod is null");
        Set<ResourceGroupNameTemplate> names = new HashSet<>();
        for (ResourceGroupSpec group : rootGroups) {
            checkArgument(!names.contains(group.getName()), "Duplicated root group: %s", group.getName());
            names.add(group.getName());
        }
    }

    public List<ResourceGroupSpec> getRootGroups()
    {
        return rootGroups;
    }

    public List<SelectorSpec> getSelectors()
    {
        return selectors;
    }

    public Optional<Duration> getCpuQuotaPeriod()
    {
        return cpuQuotaPeriod;
    }
}

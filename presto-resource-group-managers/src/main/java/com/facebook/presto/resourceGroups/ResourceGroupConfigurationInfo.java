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
package com.facebook.presto.resourceGroups.systemtables;

import com.facebook.presto.resourceGroups.ResourceGroupIdTemplate;
import com.facebook.presto.resourceGroups.ResourceGroupSpec;
import com.facebook.presto.resourceGroups.SelectorSpec;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

public class ResourceGroupConfigurationInfo
{
    private final AtomicReference<List<SelectorSpec>> selectorSpecs = new AtomicReference<>(ImmutableList.of());
    private final AtomicReference<Optional<Duration>> cpuQuotaPeriod = new AtomicReference<>(Optional.empty());
    private final AtomicReference<Map<ResourceGroupIdTemplate, ResourceGroupSpec>> resourceGroupSpecs = new AtomicReference<>(ImmutableMap.of());
    private final Map<ResourceGroupId, ResourceGroupIdTemplate> configuredGroups = new ConcurrentHashMap<>();
    public Map<ResourceGroupIdTemplate, ResourceGroupSpec> getResourceGroupSpecs()
    {
        return resourceGroupSpecs.get();
    }

    public List<SelectorSpec> getSelectorSpecs()
    {
        return selectorSpecs.get();
    }

    public Optional<Duration> getCpuQuotaPeriod()
    {
        return cpuQuotaPeriod.get();
    }

    public Map<ResourceGroupId, ResourceGroupIdTemplate> getConfiguredGroups()
    {
        return ImmutableMap.copyOf(configuredGroups);
    }

    public void setRootGroupSpecs(List<ResourceGroupSpec> rootGroupSpecs)
    {
        extractResourceGroupSpecs(rootGroupSpecs);
    }

    public void setSelectorSpecs(List<SelectorSpec> selectorSpecs)
    {
        this.selectorSpecs.set(selectorSpecs);
    }

    public void setCpuQuotaPeriod(Optional<Duration> cpuQuotaPeriod)
    {
        this.cpuQuotaPeriod.set(cpuQuotaPeriod);
    }

    public void addGroup(ResourceGroupId resourceGroupId, ResourceGroupIdTemplate templateId)
    {
        configuredGroups.put(resourceGroupId, templateId);
    }

    private void extractResourceGroupSpecs(List<ResourceGroupSpec> rootGroupSpecs)
    {
        ImmutableMap.Builder<ResourceGroupIdTemplate, ResourceGroupSpec> builder = ImmutableMap.builder();
        for (ResourceGroupSpec spec : rootGroupSpecs) {
            extractFromRootGroupSpec(spec, builder);
        }
        resourceGroupSpecs.set(builder.build());
    }

    // Traverse the root group and add all specs to the map builder
    private void extractFromRootGroupSpec(
            ResourceGroupSpec rootGroup,
            ImmutableMap.Builder<ResourceGroupIdTemplate, ResourceGroupSpec> builder)
    {
        requireNonNull(rootGroup, "rootGroup is null");
        LinkedList<Entry> entries = new LinkedList<>();
        // Create the root entry
        entries.add(Entry.fromRootGroup(rootGroup));
        while (!entries.isEmpty()) {
            Entry entry = entries.poll();
            builder.put(entry.id, entry.spec);
            for (ResourceGroupSpec spec : entry.spec.getSubGroups()) {
                // Create child entries by prepending the parent template id
                entries.add(Entry.withParent(entry.id, spec));
            }
        }
    }

    // Utility class to store the full id and spec
    private static class Entry
    {
        public final ResourceGroupIdTemplate id;
        public final ResourceGroupSpec spec;
        public Entry(ResourceGroupIdTemplate id, ResourceGroupSpec spec)
        {
            this.id = id;
            this.spec = spec;
        }

        public static Entry fromRootGroup(ResourceGroupSpec rootGroup)
        {
            return new Entry(
                    new ResourceGroupIdTemplate(rootGroup.getName().toString()),
                    rootGroup);
        }

        public static Entry withParent(ResourceGroupIdTemplate parentId, ResourceGroupSpec spec)
        {
            return new Entry(
                    ResourceGroupIdTemplate.forSubGroupNamed(parentId, spec.getName().toString()),
                    spec);
        }
    }
}

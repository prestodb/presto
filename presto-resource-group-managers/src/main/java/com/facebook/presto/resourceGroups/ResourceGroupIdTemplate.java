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

import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ResourceGroupIdTemplate
{
    private final List<ResourceGroupNameTemplate> segments;

    @JsonCreator
    public ResourceGroupIdTemplate(String fullId)
    {
        List<String> segments = Splitter.on(".").splitToList(requireNonNull(fullId, "fullId is null"));
        checkArgument(!segments.isEmpty(), "Resource group id is empty");
        this.segments = segments.stream()
                .map(ResourceGroupNameTemplate::new)
                .collect(Collectors.toList());
    }

    public static ResourceGroupIdTemplate forSubGroupNamed(ResourceGroupIdTemplate parent, String name)
    {
        return new ResourceGroupIdTemplate(format("%s.%s", requireNonNull(parent, "parent is null"), requireNonNull(name, "name is null")));
    }

    public static ResourceGroupIdTemplate fromSegments(List<ResourceGroupNameTemplate> segments)
    {
        return new ResourceGroupIdTemplate(String.join(".", segments.stream().map(ResourceGroupNameTemplate::toString).collect(Collectors.toList())));
    }

    public ResourceGroupId expandTemplate(VariableMap context)
    {
        ResourceGroupId id = null;
        for (ResourceGroupNameTemplate segment : segments) {
            String expanded = segment.expandTemplate(context);
            if (id == null) {
                id = new ResourceGroupId(expanded);
            }
            else {
                id = new ResourceGroupId(id, expanded);
            }
        }
        return id;
    }

    public OptionalInt getFirstDynamicSegment()
    {
        return IntStream.range(0, segments.size())
                .filter(i -> segments.get(i).hasVariables())
                .findFirst();
    }

    public List<ResourceGroupNameTemplate> getSegments()
    {
        return ImmutableList.copyOf(segments);
    }

    public Set<String> getVariableNames()
    {
        return segments.stream()
                .flatMap(s -> s.getVariableNames().stream())
                .collect(toImmutableSet());
    }

    @Override
    public String toString()
    {
        return Joiner.on(".").join(segments);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ResourceGroupIdTemplate that = (ResourceGroupIdTemplate) o;
        return Objects.equals(segments, that.segments);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(segments);
    }
}

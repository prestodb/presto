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
package com.facebook.presto.spi.resourceGroups;

import com.fasterxml.jackson.annotation.JsonValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public final class ResourceGroupId
{
    private final List<String> segments;

    public ResourceGroupId(String name)
    {
        this(singletonList(requireNonNull(name, "name is null")));
    }

    public ResourceGroupId(ResourceGroupId parent, String name)
    {
        this(append(requireNonNull(parent, "parent is null").segments, requireNonNull(name, "name is null")));
    }

    private static List<String> append(List<String> list, String element)
    {
        List<String> result = new ArrayList<>(list);
        result.add(element);
        return result;
    }

    private ResourceGroupId(List<String> segments)
    {
        checkArgument(!segments.isEmpty(), "Resource group id is empty");
        for (String segment : segments) {
            checkArgument(!segment.isEmpty(), "Empty segment in resource group id");
        }
        this.segments = segments;
    }

    public String getLastSegment()
    {
        return segments.get(segments.size() - 1);
    }

    public List<String> getSegments()
    {
        return segments;
    }

    public ResourceGroupId getRoot()
    {
        return new ResourceGroupId(segments.get(0));
    }

    public Optional<ResourceGroupId> getParent()
    {
        if (segments.size() == 1) {
            return Optional.empty();
        }
        return Optional.of(new ResourceGroupId(segments.subList(0, segments.size() - 1)));
    }

    public boolean isAncestorOf(ResourceGroupId descendant)
    {
        List<String> descendantSegments = descendant.getSegments();
        if (segments.size() >= descendantSegments.size()) {
            return false;
        }
        return descendantSegments.subList(0, segments.size()).equals(segments);
    }

    private static void checkArgument(boolean argument, String format, Object... args)
    {
        if (!argument) {
            throw new IllegalArgumentException(format(format, args));
        }
    }

    @Override
    @JsonValue
    public String toString()
    {
        return segments.stream()
                .collect(joining("."));
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
        ResourceGroupId that = (ResourceGroupId) o;
        return Objects.equals(segments, that.segments);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(segments);
    }
}

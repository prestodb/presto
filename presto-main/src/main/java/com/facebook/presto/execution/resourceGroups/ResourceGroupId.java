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
package com.facebook.presto.execution.resourceGroups;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class ResourceGroupId
{
    private final List<String> segments;

    public ResourceGroupId(String name)
    {
        this(ImmutableList.of(requireNonNull(name, "name is null")));
    }

    public ResourceGroupId(ResourceGroupId parent, String name)
    {
        this(ImmutableList.<String>builder().addAll(requireNonNull(parent, "parent is null").segments).add(requireNonNull(name, "name is null")).build());
    }

    private ResourceGroupId(List<String> segments)
    {
        checkArgument(!segments.isEmpty(), "Resource group id is empty");
        for (String segment : segments) {
            checkArgument(!segment.isEmpty(), "Empty segment in resource group id");
            checkArgument(segment.indexOf('.') < 0, "Invalid resource group id. '%s' contains a '.'", Joiner.on(".").join(segments));
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

    public Optional<ResourceGroupId> getParent()
    {
        if (segments.size() == 1) {
            return Optional.empty();
        }
        return Optional.of(new ResourceGroupId(segments.subList(0, segments.size() - 1)));
    }

    public static ResourceGroupId fromString(String value)
    {
        requireNonNull(value, "value is null");
        return new ResourceGroupId(Splitter.on(".").splitToList(value));
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
        ResourceGroupId that = (ResourceGroupId) o;
        return Objects.equals(segments, that.segments);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(segments);
    }
}

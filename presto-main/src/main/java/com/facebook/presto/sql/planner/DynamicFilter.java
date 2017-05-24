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
package com.facebook.presto.sql.planner;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class DynamicFilter
{
    private final String source;
    private final Map<Symbol, Symbol> mappings;

    public static DynamicFilter createEmptyDynamicFilter()
    {
        return new DynamicFilter(UUID.randomUUID().toString(), ImmutableMap.of());
    }

    @JsonCreator
    public DynamicFilter(@JsonProperty("source") String source, @JsonProperty("mappings") Map<Symbol, Symbol> mappings)
    {
        this.source = requireNonNull(source, "source is null");
        this.mappings = ImmutableMap.copyOf(requireNonNull(mappings, "mappings is null"));
    }

    public String getSource()
    {
        return source;
    }

    public Map<Symbol, Symbol> getMappings()
    {
        return mappings;
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
        DynamicFilter that = (DynamicFilter) o;
        return Objects.equals(source, that.source) &&
                Objects.equals(mappings, that.mappings);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(source, mappings);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("source", source)
                .add("mappings", mappings)
                .toString();
    }
}

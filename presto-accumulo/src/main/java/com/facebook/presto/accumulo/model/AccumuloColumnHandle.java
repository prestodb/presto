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
package com.facebook.presto.accumulo.model;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class AccumuloColumnHandle
        implements ColumnHandle, Comparable<AccumuloColumnHandle>
{
    private final boolean indexed;
    private final Optional<String> family;
    private final Optional<String> qualifier;
    private final Type type;
    private final String comment;
    private final String name;
    private final int ordinal;

    @JsonCreator
    public AccumuloColumnHandle(
            @JsonProperty("name") String name,
            @JsonProperty("family") Optional<String> family,
            @JsonProperty("qualifier") Optional<String> qualifier,
            @JsonProperty("type") Type type,
            @JsonProperty("ordinal") int ordinal,
            @JsonProperty("comment") String comment,
            @JsonProperty("indexed") boolean indexed)
    {
        this.name = requireNonNull(name, "name is null");
        this.family = requireNonNull(family, "family is null");
        this.qualifier = requireNonNull(qualifier, "qualifier is null");
        this.type = requireNonNull(type, "type is null");
        this.ordinal = requireNonNull(ordinal, "type is null");
        checkArgument(ordinal >= 0, "ordinal must be >= zero");

        this.comment = requireNonNull(comment, "comment is null");
        this.indexed = requireNonNull(indexed, "indexed is null");
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public Optional<String> getFamily()
    {
        return family;
    }

    @JsonProperty
    public Optional<String> getQualifier()
    {
        return qualifier;
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public int getOrdinal()
    {
        return ordinal;
    }

    @JsonProperty
    public String getComment()
    {
        return comment;
    }

    @JsonIgnore
    public ColumnMetadata getColumnMetadata()
    {
        return new ColumnMetadata(name, type, comment, false);
    }

    @JsonProperty
    public boolean isIndexed()
    {
        return indexed;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(indexed, name, family, qualifier, type, ordinal, comment);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }

        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        AccumuloColumnHandle other = (AccumuloColumnHandle) obj;
        return Objects.equals(this.indexed, other.indexed)
                && Objects.equals(this.name, other.name)
                && Objects.equals(this.family, other.family)
                && Objects.equals(this.qualifier, other.qualifier)
                && Objects.equals(this.type, other.type)
                && Objects.equals(this.ordinal, other.ordinal)
                && Objects.equals(this.comment, other.comment);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("columnFamily", family.orElse(null))
                .add("columnQualifier", qualifier.orElse(null))
                .add("type", type)
                .add("ordinal", ordinal)
                .add("comment", comment)
                .add("indexed", indexed)
                .toString();
    }

    @Override
    public int compareTo(AccumuloColumnHandle obj)
    {
        return Integer.compare(this.getOrdinal(), obj.getOrdinal());
    }
}

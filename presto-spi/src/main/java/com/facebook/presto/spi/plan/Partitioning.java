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
package com.facebook.presto.spi.plan;

import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

@Immutable
public final class Partitioning
{
    private final PartitioningHandle handle;
    private final List<RowExpression> arguments;

    public Partitioning(PartitioningHandle handle, List<RowExpression> arguments)
    {
        this.handle = requireNonNull(handle, "handle is null");
        this.arguments = unmodifiableList(requireNonNull(arguments, "arguments is null"));
    }

    public static <T extends RowExpression> Partitioning create(PartitioningHandle handle, Collection<T> columns)
    {
        return new Partitioning(handle, columns.stream()
                .map(RowExpression.class::cast)
                .collect(collectingAndThen(toList(), Collections::unmodifiableList)));
    }

    // Factory method for JSON serde only!
    @JsonCreator
    public static Partitioning jsonCreate(
            @JsonProperty("handle") PartitioningHandle handle,
            @JsonProperty("arguments") List<RowExpression> arguments)
    {
        return new Partitioning(handle, arguments);
    }

    @JsonProperty
    public PartitioningHandle getHandle()
    {
        return handle;
    }

    @JsonProperty
    public List<RowExpression> getArguments()
    {
        return arguments;
    }

    public Set<VariableReferenceExpression> getVariableReferences()
    {
        return arguments.stream()
                .filter(VariableReferenceExpression.class::isInstance)
                .map(VariableReferenceExpression.class::cast)
                .collect(collectingAndThen(toSet(), Collections::unmodifiableSet));
    }

    public Partitioning withAlternativePartitioningHandle(PartitioningHandle partitioningHandle)
    {
        return new Partitioning(partitioningHandle, this.arguments);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(handle, arguments);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final Partitioning other = (Partitioning) obj;
        return Objects.equals(this.handle, other.handle) &&
                Objects.equals(this.arguments, other.arguments);
    }

    @Override
    public String toString()
    {
        String sb = "Partitioning{" + "handle=" + handle +
                ", arguments=" + arguments +
                '}';
        return sb;
    }
}

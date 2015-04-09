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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.spi.LocalProperty;
import com.facebook.presto.sql.planner.Symbol;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

class PreferredProperties
{
    private final boolean hasPartitioningRequirements;
    private final Optional<Set<Symbol>> partitioningColumns;
    private final List<LocalProperty<Symbol>> localProperties;

    public PreferredProperties(
            boolean hasPartitioningRequirements,
            Optional<Set<Symbol>> partitioningColumns,
            List<? extends LocalProperty<Symbol>> localProperties)
    {
        requireNonNull(partitioningColumns, "partitioningColumns is null");
        requireNonNull(localProperties, "localProperties is null");

        this.hasPartitioningRequirements = hasPartitioningRequirements;
        this.partitioningColumns = partitioningColumns;
        this.localProperties = ImmutableList.copyOf(localProperties);
    }

    public static PreferredProperties any()
    {
        return new PreferredProperties(false, Optional.<Set<Symbol>>empty(), ImmutableList.of());
    }

    public static PreferredProperties unpartitioned()
    {
        return new PreferredProperties(true, Optional.of(ImmutableSet.of()), ImmutableList.of());
    }

    public static PreferredProperties partitioned(Set<Symbol> columns)
    {
        return new PreferredProperties(true, Optional.of(ImmutableSet.copyOf(columns)), ImmutableList.of());
    }

    public static PreferredProperties partitioned()
    {
        return new PreferredProperties(true, Optional.<Set<Symbol>>empty(), ImmutableList.of());
    }

    public static PreferredProperties partitionedWithLocal(Set<Symbol> columns, List<? extends LocalProperty<Symbol>> localProperties)
    {
        return new PreferredProperties(true, Optional.of(ImmutableSet.copyOf(columns)), ImmutableList.copyOf(localProperties));
    }

    public boolean hasPartitioningRequirements()
    {
        return hasPartitioningRequirements;
    }

    public Optional<Set<Symbol>> getPartitioningColumns()
    {
        return partitioningColumns;
    }

    public List<LocalProperty<Symbol>> getLocalProperties()
    {
        return localProperties;
    }
}

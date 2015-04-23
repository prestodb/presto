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

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

class PreferredProperties
{
    private final Optional<PartitioningPreferences> partitioningRequirements;
    private final List<LocalProperty<Symbol>> localProperties;

    public PreferredProperties(
            Optional<PartitioningPreferences> partitioningRequirements,
            List<? extends LocalProperty<Symbol>> localProperties)
    {
        requireNonNull(partitioningRequirements, "partitioningRequirements is null");
        requireNonNull(localProperties, "localProperties is null");

        this.partitioningRequirements = partitioningRequirements;
        this.localProperties = ImmutableList.copyOf(localProperties);
    }

    public static PreferredProperties any()
    {
        return new PreferredProperties(Optional.empty(), ImmutableList.of());
    }

    public static PreferredProperties unpartitioned()
    {
        return new PreferredProperties(Optional.of(PartitioningPreferences.unpartitioned()), ImmutableList.of());
    }

    public static PreferredProperties partitioned(Set<Symbol> columns)
    {
        return new PreferredProperties(Optional.of(PartitioningPreferences.partitioned(columns)), ImmutableList.of());
    }

    public static PreferredProperties partitioned()
    {
        return new PreferredProperties(Optional.of(PartitioningPreferences.partitioned()), ImmutableList.of());
    }

    public static PreferredProperties partitionedWithLocal(Set<Symbol> columns, List<? extends LocalProperty<Symbol>> localProperties)
    {
        return new PreferredProperties(Optional.of(PartitioningPreferences.partitioned(columns)), ImmutableList.copyOf(localProperties));
    }

    public static PreferredProperties unpartitionedWithLocal(List<? extends LocalProperty<Symbol>> localProperties)
    {
        return new PreferredProperties(Optional.of(PartitioningPreferences.unpartitioned()), ImmutableList.copyOf(localProperties));
    }

    public static PreferredProperties local(List<? extends LocalProperty<Symbol>> localProperties)
    {
        return new PreferredProperties(Optional.empty(), ImmutableList.copyOf(localProperties));
    }

    public Optional<PartitioningPreferences> getPartitioningProperties()
    {
        return partitioningRequirements;
    }

    public List<LocalProperty<Symbol>> getLocalProperties()
    {
        return localProperties;
    }

    public static class PartitioningPreferences
    {
        private final boolean partitioned;
        private final Optional<Set<Symbol>> partitioningColumns;

        private PartitioningPreferences(boolean partitioned, Optional<Set<Symbol>> partitioningColumns)
        {
            requireNonNull(partitioningColumns, "partitioningColumns is null");
            checkArgument(partitioned || (partitioningColumns.isPresent() && partitioningColumns.get().isEmpty()), "unpartitioned implies partitioned on the empty set");

            this.partitioned = partitioned;
            this.partitioningColumns = partitioningColumns.map(ImmutableSet::copyOf);
        }

        public static PartitioningPreferences unpartitioned()
        {
            return new PartitioningPreferences(false, Optional.<Set<Symbol>>of(ImmutableSet.of()));
        }

        public static PartitioningPreferences partitioned(Set<Symbol> columns)
        {
            return new PartitioningPreferences(true, Optional.of(columns));
        }

        public static PartitioningPreferences partitioned()
        {
            return new PartitioningPreferences(true, Optional.empty());
        }

        public boolean isPartitioned()
        {
            return partitioned;
        }

        public Optional<Set<Symbol>> getPartitioningColumns()
        {
            return partitioningColumns;
        }
    }
}

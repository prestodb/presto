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
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

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

    public static PreferredProperties hashPartitioned(List<Symbol> columns)
    {
        return new PreferredProperties(Optional.of(PartitioningPreferences.hashPartitioned(columns)), ImmutableList.of());
    }

    public static PreferredProperties hashPartitionedWithLocal(List<Symbol> columns, List<? extends LocalProperty<Symbol>> localProperties)
    {
        return new PreferredProperties(Optional.of(PartitioningPreferences.hashPartitioned(columns)), ImmutableList.copyOf(localProperties));
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

    /**
     * @param preferred PreferredProperties in terms of the parent symbols
     * @param translations output to input translations for symbols that can be translated
     * @return translated PreferredProperties
     */
    public static PreferredProperties translate(PreferredProperties preferred, Map<Symbol, Symbol> translations)
    {
        List<LocalProperty<Symbol>> localProperties = LocalProperties.translate(preferred.getLocalProperties(), column -> Optional.ofNullable(translations.get(column)));

        if (preferred.getPartitioningProperties().isPresent()) {
            PartitioningPreferences partitioning = preferred.getPartitioningProperties().get();
            if (partitioning.isHashPartitioned()) {
                List<Symbol> hashingSymbols = partitioning.getHashPartitioningColumns().get();
                if (translations.keySet().containsAll(hashingSymbols)) {
                    List<Symbol> translated = partitioning.getHashPartitioningColumns().get().stream()
                            .map(translations::get)
                            .collect(toList());
                    return hashPartitionedWithLocal(translated, localProperties);
                }
            }
            if (partitioning.isPartitioned()) {
                // check if we can satisfy any partitioning requirements
                Set<Symbol> symbols = partitioning.getPartitioningColumns().get();
                Set<Symbol> translated = symbols.stream()
                        .filter(symbol -> translations.keySet().contains(symbol))
                        .map(translations::get)
                        .collect(toSet());

                if (!translated.isEmpty()) {
                    return partitionedWithLocal(translated, localProperties);
                }
            }
            else {
                return unpartitionedWithLocal(localProperties);
            }
        }
        return local(localProperties);
    }

    public static PreferredProperties derivePreferences(
            PreferredProperties parentProperties,
            Set<Symbol> partitioningColumns,
            List<LocalProperty<Symbol>> localProperties)
    {
        return derivePreferences(parentProperties, partitioningColumns, Optional.empty(), localProperties);
    }

    /**
     * Derive current node's preferred properties based on parent's preferences
     * @param parentProperties Parent's preferences (translated)
     * @param partitioningColumns partitioning columns of current node
     * @param hashingColumns hashing columns of current node
     * @param localProperties local properties of current node
     * @return PreferredProperties for current node
     */
    public static PreferredProperties derivePreferences(
            PreferredProperties parentProperties,
            Set<Symbol> partitioningColumns,
            Optional<List<Symbol>> hashingColumns,
            List<LocalProperty<Symbol>> localProperties)
    {
        if (hashingColumns.isPresent()) {
            checkState(partitioningColumns.equals(ImmutableSet.copyOf(hashingColumns.get())), "hashingColumns and partitioningColumns must be the same");
        }

        List<LocalProperty<Symbol>> local = ImmutableList.<LocalProperty<Symbol>>builder()
                .addAll(localProperties)
                .addAll(parentProperties.getLocalProperties())
                .build();

        // Check we need to be hash partitioned
        if (hashingColumns.isPresent()) {
            return hashPartitionedWithLocal(hashingColumns.get(), local);
        }

        if (parentProperties.getPartitioningProperties().isPresent()) {
            PartitioningPreferences parentPartitioning = parentProperties.getPartitioningProperties().get();
            // If parent's hash partitioning satisfies our partitioning, use parent's hash partitioning
            if (parentPartitioning.isHashPartitioned() && partitioningColumns.equals(ImmutableSet.copyOf(parentPartitioning.getHashPartitioningColumns().get()))) {
                List<Symbol> hashingSymbols = parentPartitioning.getHashPartitioningColumns().get();
                return hashPartitionedWithLocal(hashingSymbols, local);
            }

            // if the child plan is partitioned by the common columns between our requirements and our parent's, it can satisfy both in one shot
            if (parentPartitioning.isPartitioned()) {
                Set<Symbol> parentPartitioningColumns = parentPartitioning.getPartitioningColumns().get();
                Set<Symbol> common = Sets.intersection(partitioningColumns, parentPartitioningColumns);

                // If we find common partitioning columns, use them, else use child's partitioning columns
                if (!common.isEmpty()) {
                    return partitionedWithLocal(common, local);
                }
                return partitionedWithLocal(partitioningColumns, local);
            }
        }
        return partitionedWithLocal(partitioningColumns, local);
    }

    public static class PartitioningPreferences
    {
        private final boolean partitioned;
        private final Optional<Set<Symbol>> partitioningColumns;
        private final Optional<List<Symbol>> hashingColumns;

        private PartitioningPreferences(boolean partitioned, Optional<Set<Symbol>> partitioningColumns, Optional<List<Symbol>> hashingColumns)
        {
            requireNonNull(partitioningColumns, "partitioningColumns is null");
            checkArgument(partitioned || (partitioningColumns.isPresent() && partitioningColumns.get().isEmpty()), "unpartitioned implies partitioned on the empty set");
            if (hashingColumns.isPresent()) {
                checkArgument(partitioningColumns.isPresent(), "partitioningColumns not present");
                checkArgument(partitioningColumns.get().containsAll(hashingColumns.get()), "partitioningColumns does not include hashingColumns");
            }

            this.partitioned = partitioned;
            this.partitioningColumns = partitioningColumns.map(ImmutableSet::copyOf);
            this.hashingColumns = hashingColumns.map(ImmutableList::copyOf);
        }

        public static PartitioningPreferences unpartitioned()
        {
            return new PartitioningPreferences(false, Optional.<Set<Symbol>>of(ImmutableSet.of()), Optional.empty());
        }

        public static PartitioningPreferences partitioned(Set<Symbol> columns)
        {
            return new PartitioningPreferences(true, Optional.of(columns), Optional.empty());
        }

        public static PartitioningPreferences partitioned()
        {
            return new PartitioningPreferences(true, Optional.empty(), Optional.empty());
        }

        public static PartitioningPreferences hashPartitioned(List<Symbol> columns)
        {
            return new PartitioningPreferences(true, Optional.of(ImmutableSet.copyOf(columns)), Optional.of(ImmutableList.copyOf(columns)));
        }

        public boolean isPartitioned()
        {
            return partitioned;
        }

        public Optional<Set<Symbol>> getPartitioningColumns()
        {
            return partitioningColumns;
        }

        public boolean isHashPartitioned()
        {
            return hashingColumns.isPresent();
        }

        public Optional<List<Symbol>> getHashPartitioningColumns()
        {
            return hashingColumns;
        }
    }
}

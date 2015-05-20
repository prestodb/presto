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
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableSet;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

class PreferredProperties
{
    private final Optional<Global> globalProperties;
    private final List<LocalProperty<Symbol>> localProperties;

    private PreferredProperties(
            Optional<Global> globalProperties,
            List<? extends LocalProperty<Symbol>> localProperties)
    {
        requireNonNull(globalProperties, "globalProperties is null");
        requireNonNull(localProperties, "localProperties is null");

        this.globalProperties = globalProperties;
        this.localProperties = ImmutableList.copyOf(localProperties);
    }

    public static PreferredProperties any()
    {
        return builder().build();
    }

    public static PreferredProperties undistributed()
    {
        return builder()
                .global(Global.undistributed())
                .build();
    }

    public static PreferredProperties partitioned(Set<Symbol> columns)
    {
        return builder()
                .global(Global.distributed(Partitioning.partitioned(columns)))
                .build();
    }

    public static PreferredProperties distributed()
    {
        return builder()
                .global(Global.distributed())
                .build();
    }

    public static PreferredProperties hashPartitioned(List<Symbol> columns)
    {
        return builder()
                .global(Global.distributed(Partitioning.hashPartitioned(columns)))
                .build();
    }

    public static PreferredProperties hashPartitionedWithLocal(List<Symbol> columns, List<? extends LocalProperty<Symbol>> localProperties)
    {
        return builder()
                .global(Global.distributed(Partitioning.hashPartitioned(columns)))
                .local(localProperties)
                .build();
    }

    public static PreferredProperties partitionedWithLocal(Set<Symbol> columns, List<? extends LocalProperty<Symbol>> localProperties)
    {
        return builder()
                .global(Global.distributed(Partitioning.partitioned(columns)))
                .local(localProperties)
                .build();
    }

    public static PreferredProperties undistributedWithLocal(List<? extends LocalProperty<Symbol>> localProperties)
    {
        return builder()
                .global(Global.undistributed())
                .local(localProperties)
                .build();
    }

    public static PreferredProperties local(List<? extends LocalProperty<Symbol>> localProperties)
    {
        return builder()
                .local(localProperties)
                .build();
    }

    public Optional<Global> getGlobalProperties()
    {
        return globalProperties;
    }

    public List<LocalProperty<Symbol>> getLocalProperties()
    {
        return localProperties;
    }

    public PreferredProperties translate(Function<Symbol, Optional<Symbol>> translator)
    {
        Optional<Global> newGlobalProperties = globalProperties.map(global -> global.translate(translator));
        List<LocalProperty<Symbol>> newLocalProperties = LocalProperties.translate(localProperties, translator);
        return new PreferredProperties(newGlobalProperties, newLocalProperties);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private Optional<Global> globalProperties = Optional.empty();
        private List<LocalProperty<Symbol>> localProperties = ImmutableList.of();

        public Builder global(Global globalProperties)
        {
            this.globalProperties = Optional.of(globalProperties);
            return this;
        }

        public Builder global(PreferredProperties other)
        {
            this.globalProperties = other.globalProperties;
            return this;
        }

        public Builder local(List<? extends LocalProperty<Symbol>> localProperties)
        {
            this.localProperties = ImmutableList.copyOf(localProperties);
            return this;
        }

        public Builder local(PreferredProperties other)
        {
            this.localProperties = ImmutableList.copyOf(other.localProperties);
            return this;
        }

        public PreferredProperties build()
        {
            return new PreferredProperties(globalProperties, localProperties);
        }
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
     *
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

        if (parentProperties.getGlobalProperties().isPresent()) {
            Global global = parentProperties.getGlobalProperties().get();
            if (global.getPartitioningProperties().isPresent()) {
                Partitioning partitioning = global.getPartitioningProperties().get();

                // If parent's hash partitioning satisfies our partitioning, use parent's hash partitioning
                if (partitioning.getHashingOrder().isPresent() && partitioningColumns.equals(ImmutableSet.copyOf(partitioning.getHashingOrder().get()))) {
                    List<Symbol> hashingSymbols = partitioning.getHashingOrder().get();
                    return hashPartitionedWithLocal(hashingSymbols, local);
                }

                // if the child plan is partitioned by the common columns between our requirements and our parent's, it can satisfy both in one shot
                Set<Symbol> parentPartitioningColumns = partitioning.getPartitioningColumns();
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

    @Immutable
    public static final class Global
    {
        private final boolean distributed;
        private final Optional<Partitioning> partitioningProperties; // if missing => partitioned with some unknown scheme

        private Global(boolean distributed, Optional<Partitioning> partitioningProperties)
        {
            this.distributed = distributed;
            this.partitioningProperties = Objects.requireNonNull(partitioningProperties, "partitioningProperties is null");
        }

        public static Global undistributed()
        {
            return new Global(false, Optional.of(Partitioning.singlePartition()));
        }

        public static Global distributed(Optional<Partitioning> partitioningProperties)
        {
            return new Global(true, partitioningProperties);
        }

        public static Global distributed()
        {
            return distributed(Optional.<Partitioning>empty());
        }

        public static Global distributed(Partitioning partitioning)
        {
            return distributed(Optional.of(partitioning));
        }

        public boolean isDistributed()
        {
            return distributed;
        }

        public Optional<Partitioning> getPartitioningProperties()
        {
            return partitioningProperties;
        }

        public boolean isHashPartitioned()
        {
            return partitioningProperties.isPresent() && partitioningProperties.get().getHashingOrder().isPresent();
        }

        public Global translate(Function<Symbol, Optional<Symbol>> translator)
        {
            if (!isDistributed()) {
                return this;
            }
            return distributed(partitioningProperties.flatMap(properties -> properties.translate(translator)));
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(distributed, partitioningProperties);
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
            final Global other = (Global) obj;
            return Objects.equals(this.distributed, other.distributed)
                    && Objects.equals(this.partitioningProperties, other.partitioningProperties);
        }

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper(this)
                    .add("distributed", distributed)
                    .add("partitioningProperties", partitioningProperties)
                    .toString();
        }
    }

    @Immutable
    public static final class Partitioning
    {
        private final Set<Symbol> partitioningColumns;
        private final Optional<List<Symbol>> hashingOrder; // If populated, this list will contain the same symbols as partitioningColumns

        private Partitioning(Set<Symbol> partitioningColumns, Optional<List<Symbol>> hashingOrder)
        {
            this.partitioningColumns = ImmutableSet.copyOf(Objects.requireNonNull(partitioningColumns, "partitioningColumns is null"));
            this.hashingOrder = Objects.requireNonNull(hashingOrder, "hashingOrder is null").map(ImmutableList::copyOf);
        }

        public static Partitioning hashPartitioned(List<Symbol> columns)
        {
            return new Partitioning(ImmutableSet.copyOf(columns), Optional.of(columns));
        }

        public static Partitioning partitioned(Set<Symbol> columns)
        {
            return new Partitioning(columns, Optional.<List<Symbol>>empty());
        }

        public static Partitioning singlePartition()
        {
            return partitioned(ImmutableSet.of());
        }

        public Set<Symbol> getPartitioningColumns()
        {
            return partitioningColumns;
        }

        public Optional<List<Symbol>> getHashingOrder()
        {
            return hashingOrder;
        }

        public Optional<Partitioning> translate(Function<Symbol, Optional<Symbol>> translator)
        {
            Set<Symbol> newPartitioningColumns = partitioningColumns.stream()
                    .map(translator)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(toImmutableSet());

            // If nothing can be translated, then we won't have any partitioning preferences
            if (newPartitioningColumns.isEmpty()) {
                return Optional.empty();
            }

            Optional<List<Symbol>> newHashingOrder = hashingOrder.flatMap(columns -> {
                ImmutableList.Builder<Symbol> builder = ImmutableList.builder();
                for (Symbol column : columns) {
                    Optional<Symbol> translated = translator.apply(column);
                    if (!translated.isPresent()) {
                        return Optional.empty();
                    }
                    builder.add(translated.get());
                }
                return Optional.of(builder.build());
            });

            return Optional.of(new Partitioning(newPartitioningColumns, newHashingOrder));
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(partitioningColumns, hashingOrder);
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
            return Objects.equals(this.partitioningColumns, other.partitioningColumns)
                    && Objects.equals(this.hashingOrder, other.hashingOrder);
        }

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper(this)
                    .add("partitioningColumns", partitioningColumns)
                    .add("hashingOrder", hashingOrder)
                    .toString();
        }
    }
}

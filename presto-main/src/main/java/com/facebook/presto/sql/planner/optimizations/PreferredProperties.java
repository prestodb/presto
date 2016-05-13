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
                .global(Global.distributed(PartitioningProperties.partitioned(columns)))
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
                .global(Global.distributed(PartitioningProperties.hashPartitioned(columns)))
                .build();
    }

    public static PreferredProperties hashPartitionedWithNullsReplicated(List<Symbol> columns)
    {
        return builder()
                .global(Global.distributed(PartitioningProperties.hashPartitioned(columns).withNullsReplicated(true)))
                .build();
    }

    public static PreferredProperties partitionedWithLocal(Set<Symbol> columns, List<? extends LocalProperty<Symbol>> localProperties)
    {
        return builder()
                .global(Global.distributed(PartitioningProperties.partitioned(columns)))
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

    public PreferredProperties mergeWithParent(PreferredProperties parent)
    {
        List<LocalProperty<Symbol>> newLocal = ImmutableList.<LocalProperty<Symbol>>builder()
                .addAll(localProperties)
                .addAll(parent.getLocalProperties())
                .build();

        Builder builder = builder()
                .local(newLocal);

        if (globalProperties.isPresent()) {
            Global currentGlobal = globalProperties.get();
            Global newGlobal = parent.getGlobalProperties()
                    .map(currentGlobal::mergeWithParent)
                    .orElse(currentGlobal);
            builder.global(newGlobal);
        }
        else {
            builder.global(parent.getGlobalProperties());
        }

        return builder.build();
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

        public Builder global(Optional<Global> globalProperties)
        {
            this.globalProperties = globalProperties;
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

    @Immutable
    public static final class Global
    {
        private final boolean distributed;
        private final Optional<PartitioningProperties> partitioningProperties; // if missing => partitioned with some unknown scheme

        private Global(boolean distributed, Optional<PartitioningProperties> partitioningProperties)
        {
            this.distributed = distributed;
            this.partitioningProperties = Objects.requireNonNull(partitioningProperties, "partitioningProperties is null");
        }

        public static Global undistributed()
        {
            return new Global(false, Optional.of(PartitioningProperties.singlePartition()));
        }

        public static Global distributed(Optional<PartitioningProperties> partitioningProperties)
        {
            return new Global(true, partitioningProperties);
        }

        public static Global distributed()
        {
            return distributed(Optional.<PartitioningProperties>empty());
        }

        public static Global distributed(PartitioningProperties partitioning)
        {
            return distributed(Optional.of(partitioning));
        }

        public boolean isDistributed()
        {
            return distributed;
        }

        public Optional<PartitioningProperties> getPartitioningProperties()
        {
            return partitioningProperties;
        }

        public Global mergeWithParent(Global parent)
        {
            if (distributed != parent.distributed) {
                return this;
            }
            if (!partitioningProperties.isPresent()) {
                return parent;
            }
            if (!parent.partitioningProperties.isPresent()) {
                return this;
            }
            return new Global(distributed, Optional.of(partitioningProperties.get().mergeWithParent(parent.partitioningProperties.get())));
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
    public static final class PartitioningProperties
    {
        private final Set<Symbol> partitioningColumns;
        private final Optional<List<Symbol>> hashingOrder; // If populated, this list will contain the same symbols as partitioningColumns
        private final boolean nullsReplicated;

        private PartitioningProperties(Set<Symbol> partitioningColumns, Optional<List<Symbol>> hashingOrder, boolean nullsReplicated)
        {
            this.partitioningColumns = ImmutableSet.copyOf(Objects.requireNonNull(partitioningColumns, "partitioningColumns is null"));
            this.hashingOrder = Objects.requireNonNull(hashingOrder, "hashingOrder is null").map(ImmutableList::copyOf);
            this.nullsReplicated = nullsReplicated;
        }

        public PartitioningProperties withNullsReplicated(boolean nullsReplicated)
        {
            return new PartitioningProperties(partitioningColumns, hashingOrder, nullsReplicated);
        }

        public static PartitioningProperties hashPartitioned(List<Symbol> columns)
        {
            return new PartitioningProperties(ImmutableSet.copyOf(columns), Optional.of(columns), false);
        }

        public static PartitioningProperties partitioned(Set<Symbol> columns)
        {
            return new PartitioningProperties(columns, Optional.<List<Symbol>>empty(), false);
        }

        public static PartitioningProperties singlePartition()
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

        public boolean isNullsReplicated()
        {
            return nullsReplicated;
        }

        public PartitioningProperties mergeWithParent(PartitioningProperties parent)
        {
            // Non-negotiable if we require a specific hashing order
            if (hashingOrder.isPresent()) {
                return this;
            }

            // Partitioning with different null replication cannot be compared
            if (nullsReplicated != parent.nullsReplicated) {
                return this;
            }

            if (parent.hashingOrder.isPresent()) {
                // If the parent has a hashing requirement, propagate parent only if the parent's hashing order satisfies our partitioning preference.
                // Otherwise, ignore the parent since the parent will have to repartition anyways.
                return partitioningColumns.containsAll(parent.partitioningColumns) ? parent : this;
            }

            // Otherwise partition on any common columns if available
            Set<Symbol> common = Sets.intersection(partitioningColumns, parent.partitioningColumns);
            return common.isEmpty() ? this : partitioned(common).withNullsReplicated(nullsReplicated);
        }

        public Optional<PartitioningProperties> translate(Function<Symbol, Optional<Symbol>> translator)
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

            return Optional.of(new PartitioningProperties(newPartitioningColumns, newHashingOrder, nullsReplicated));
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(partitioningColumns, hashingOrder, nullsReplicated);
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
            final PartitioningProperties other = (PartitioningProperties) obj;
            return Objects.equals(this.partitioningColumns, other.partitioningColumns)
                    && Objects.equals(this.hashingOrder, other.hashingOrder)
                    && this.nullsReplicated == other.nullsReplicated;
        }

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper(this)
                    .add("partitioningColumns", partitioningColumns)
                    .add("hashingOrder", hashingOrder)
                    .add("nullsReplicated", nullsReplicated)
                    .toString();
        }
    }
}

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
import com.facebook.presto.sql.planner.Partitioning;
import com.facebook.presto.sql.planner.Symbol;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.MoreObjects.toStringHelper;
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
                .global(Global.distributed(PreferredPartitioning.partitioned(columns)))
                .build();
    }

    public static PreferredProperties partitionedWithNullsReplicated(Set<Symbol> columns)
    {
        return builder()
                .global(Global.distributed(PreferredPartitioning.partitioned(columns).withNullsReplicated(true)))
                .build();
    }

    public static PreferredProperties distributed()
    {
        return builder()
                .global(Global.distributed())
                .build();
    }

    public static PreferredProperties partitioned(Partitioning partitioning)
    {
        return builder()
                .global(Global.distributed(PreferredPartitioning.partitioned(partitioning)))
                .build();
    }

    public static PreferredProperties partitionedWithNullsReplicated(Partitioning partitioning)
    {
        return builder()
                .global(Global.distributed(PreferredPartitioning.partitioned(partitioning).withNullsReplicated(true)))
                .build();
    }

    public static PreferredProperties partitionedWithLocal(Set<Symbol> columns, List<? extends LocalProperty<Symbol>> localProperties)
    {
        return builder()
                .global(Global.distributed(PreferredPartitioning.partitioned(columns)))
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
        private final Optional<PreferredPartitioning> partitioningProperties; // if missing => partitioned with some unknown scheme

        private Global(boolean distributed, Optional<PreferredPartitioning> partitioningProperties)
        {
            this.distributed = distributed;
            this.partitioningProperties = requireNonNull(partitioningProperties, "partitioningProperties is null");
        }

        public static Global undistributed()
        {
            return new Global(false, Optional.of(PreferredPartitioning.singlePartition()));
        }

        public static Global distributed(Optional<PreferredPartitioning> partitioningProperties)
        {
            return new Global(true, partitioningProperties);
        }

        public static Global distributed()
        {
            return distributed(Optional.<PreferredPartitioning>empty());
        }

        public static Global distributed(PreferredPartitioning partitioning)
        {
            return distributed(Optional.of(partitioning));
        }

        public boolean isDistributed()
        {
            return distributed;
        }

        public Optional<PreferredPartitioning> getPartitioningProperties()
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
            return toStringHelper(this)
                    .add("distributed", distributed)
                    .add("partitioningProperties", partitioningProperties)
                    .toString();
        }
    }
}

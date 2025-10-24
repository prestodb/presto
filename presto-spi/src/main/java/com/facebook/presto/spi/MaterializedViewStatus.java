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
package com.facebook.presto.spi;

import com.facebook.presto.common.predicate.TupleDomain;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.MaterializedViewStatus.MaterializedViewState.FULLY_MATERIALIZED;
import static com.facebook.presto.spi.MaterializedViewStatus.MaterializedViewState.NOT_MATERIALIZED;
import static com.facebook.presto.spi.MaterializedViewStatus.MaterializedViewState.PARTIALLY_MATERIALIZED;
import static com.facebook.presto.spi.MaterializedViewStatus.MaterializedViewState.TOO_MANY_PARTITIONS_MISSING;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

public class MaterializedViewStatus
{
    // ========================================
    // NEW SECTION (RFC-0017)
    // ========================================

    public static class StaleDataConstraints
    {
        private final Map<SchemaTableName, List<TupleDomain<String>>> dataDisjuncts;
        private final Map<SchemaTableName, TupleDomain<String>> conjunctiveConstraints;

        public StaleDataConstraints(
                Map<SchemaTableName, List<TupleDomain<String>>> dataDisjuncts,
                Map<SchemaTableName, TupleDomain<String>> conjunctiveConstraints)
        {
            this.dataDisjuncts = unmodifiableMap(new HashMap<>(requireNonNull(dataDisjuncts, "dataDisjuncts is null")));
            this.conjunctiveConstraints = unmodifiableMap(new HashMap<>(requireNonNull(conjunctiveConstraints, "conjunctiveConstraints is null")));
        }

        public Map<SchemaTableName, List<TupleDomain<String>>> getDataDisjuncts()
        {
            return dataDisjuncts;
        }

        public Map<SchemaTableName, TupleDomain<String>> getConjunctiveConstraints()
        {
            return conjunctiveConstraints;
        }

        public TupleDomain<String> getUnifiedConstraintForTable(SchemaTableName tableName)
        {
            List<TupleDomain<String>> disjuncts = dataDisjuncts.getOrDefault(tableName, new ArrayList<>());
            TupleDomain<String> conjunctive = conjunctiveConstraints.getOrDefault(tableName, TupleDomain.all());

            if (disjuncts.isEmpty()) {
                return conjunctive;
            }

            TupleDomain<String> disjunctResult = TupleDomain.columnWiseUnion(disjuncts);
            return disjunctResult.intersect(conjunctive);
        }

        public boolean isEmpty()
        {
            return dataDisjuncts.isEmpty() && conjunctiveConstraints.isEmpty();
        }
    }

    private final boolean fullyMaterialized;
    private final Optional<StaleDataConstraints> staleDataConstraints;

    public static MaterializedViewStatus fullyMaterialized()
    {
        return new MaterializedViewStatus(true, Optional.empty());
    }

    public static MaterializedViewStatus withStaleConstraints(StaleDataConstraints staleDataConstraints)
    {
        requireNonNull(staleDataConstraints, "staleDataConstraints is null");
        return new MaterializedViewStatus(false, Optional.of(staleDataConstraints));
    }

    public MaterializedViewStatus(boolean fullyMaterialized, Optional<StaleDataConstraints> staleDataConstraints)
    {
        this.fullyMaterialized = fullyMaterialized;
        this.staleDataConstraints = requireNonNull(staleDataConstraints, "staleDataConstraints is null");
        this.materializedViewState = fullyMaterialized ? FULLY_MATERIALIZED : PARTIALLY_MATERIALIZED;
        this.partitionsFromBaseTables = emptyMap();
    }

    public boolean isFullyMaterialized()
    {
        return fullyMaterialized;
    }

    public Optional<StaleDataConstraints> getStaleDataConstraints()
    {
        return staleDataConstraints;
    }

    // ========================================
    // EXISTING SECTION (Backward Compatibility)
    // ========================================

    public enum MaterializedViewState
    {
        NOT_MATERIALIZED,
        TOO_MANY_PARTITIONS_MISSING,
        PARTIALLY_MATERIALIZED,
        FULLY_MATERIALIZED
    }

    @Deprecated
    public static class MaterializedDataPredicates
    {
        private final List<TupleDomain<String>> predicateDisjuncts;
        private final List<String> columnNames;

        public MaterializedDataPredicates(List<TupleDomain<String>> predicateDisjuncts, List<String> keys)
        {
            this.predicateDisjuncts = unmodifiableList(new ArrayList<>(requireNonNull(predicateDisjuncts, "partitionSpecs is null")));
            this.columnNames = unmodifiableList(new ArrayList<>(requireNonNull(keys, "keys is null")));
        }

        public boolean isEmpty()
        {
            return predicateDisjuncts.isEmpty();
        }

        public List<TupleDomain<String>> getPredicateDisjuncts()
        {
            return predicateDisjuncts;
        }

        public List<String> getColumnNames()
        {
            return columnNames;
        }
    }

    @Deprecated
    private final MaterializedViewState materializedViewState;
    @Deprecated
    private final Map<SchemaTableName, MaterializedDataPredicates> partitionsFromBaseTables;

    @Deprecated
    public MaterializedViewStatus(MaterializedViewState materializedViewState)
    {
        this(materializedViewState, emptyMap());
    }

    @Deprecated
    public MaterializedViewStatus(MaterializedViewState materializedViewState, Map<SchemaTableName, MaterializedDataPredicates> partitionsFromBaseTables)
    {
        this.materializedViewState = requireNonNull(materializedViewState, "materializedViewState is null");
        this.partitionsFromBaseTables = requireNonNull(partitionsFromBaseTables, "partitionsFromBaseTables is null");
        this.fullyMaterialized = (materializedViewState == FULLY_MATERIALIZED);
        this.staleDataConstraints = Optional.empty();
    }

    @Deprecated
    public MaterializedViewState getMaterializedViewState()
    {
        return materializedViewState;
    }

    @Deprecated
    public boolean isNotMaterialized()
    {
        return materializedViewState == NOT_MATERIALIZED;
    }

    @Deprecated
    public boolean isTooManyPartitionsMissing()
    {
        return materializedViewState == TOO_MANY_PARTITIONS_MISSING;
    }

    @Deprecated
    public boolean isPartiallyMaterialized()
    {
        return materializedViewState == PARTIALLY_MATERIALIZED;
    }

    @Deprecated
    public Map<SchemaTableName, MaterializedDataPredicates> getPartitionsFromBaseTables()
    {
        return partitionsFromBaseTables;
    }
}

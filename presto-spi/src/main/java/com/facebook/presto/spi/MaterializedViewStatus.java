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
import java.util.List;
import java.util.Map;

import static com.facebook.presto.spi.MaterializedViewStatus.MaterializedViewState.FULLY_MATERIALIZED;
import static com.facebook.presto.spi.MaterializedViewStatus.MaterializedViewState.NOT_MATERIALIZED;
import static com.facebook.presto.spi.MaterializedViewStatus.MaterializedViewState.PARTIALLY_MATERIALIZED;
import static com.facebook.presto.spi.MaterializedViewStatus.MaterializedViewState.TOO_MANY_PARTITIONS_MISSING;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

public class MaterializedViewStatus
{
    public enum MaterializedViewState
    {
        NOT_MATERIALIZED,
        TOO_MANY_PARTITIONS_MISSING,
        PARTIALLY_MATERIALIZED,
        FULLY_MATERIALIZED
    }

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

    private final MaterializedViewState materializedViewState;
    private final Map<SchemaTableName, MaterializedDataPredicates> partitionsFromBaseTables;

    public MaterializedViewStatus(MaterializedViewState materializedViewState)
    {
        this(materializedViewState, emptyMap());
    }

    public MaterializedViewStatus(MaterializedViewState materializedViewState, Map<SchemaTableName, MaterializedDataPredicates> partitionsFromBaseTables)
    {
        this.materializedViewState = requireNonNull(materializedViewState, "materializedViewState is null");
        this.partitionsFromBaseTables = requireNonNull(partitionsFromBaseTables, "partitionsFromBaseTables is null");
    }

    public MaterializedViewState getMaterializedViewState()
    {
        return materializedViewState;
    }

    public boolean isFullyMaterialized()
    {
        return materializedViewState == FULLY_MATERIALIZED;
    }

    public boolean isNotMaterialized()
    {
        return materializedViewState == NOT_MATERIALIZED;
    }

    public boolean isTooManyPartitionsMissing()
    {
        return materializedViewState == TOO_MANY_PARTITIONS_MISSING;
    }

    public boolean isPartiallyMaterialized()
    {
        return materializedViewState == PARTIALLY_MATERIALIZED;
    }

    public Map<SchemaTableName, MaterializedDataPredicates> getPartitionsFromBaseTables()
    {
        return partitionsFromBaseTables;
    }
}

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

package com.facebook.presto.hudi;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.hudi.HudiMetadata.fromPartitionColumns;
import static com.facebook.presto.hudi.HudiMetadata.toMetastoreContext;
import static java.util.Objects.requireNonNull;

public class HudiPartitionManager
{
    private final TypeManager typeManager;

    @Inject
    public HudiPartitionManager(TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    public List<String> getEffectivePartitions(
            ConnectorSession connectorSession,
            ExtendedHiveMetastore metastore,
            String schemaName,
            String tableName,
            TupleDomain<ColumnHandle> tupleDomain)
    {
        MetastoreContext metastoreContext = toMetastoreContext(connectorSession);
        Optional<Table> table = metastore.getTable(metastoreContext, schemaName, tableName);
        Verify.verify(table.isPresent());
        List<Column> partitionColumns = table.get().getPartitionColumns();
        if (partitionColumns.isEmpty()) {
            return ImmutableList.of("");
        }

        Map<Column, Domain> partitionPredicate = new HashMap<>();
        Map<ColumnHandle, Domain> domains = tupleDomain.getDomains().orElse(ImmutableMap.of());

        List<HudiColumnHandle> hudiColumnHandles = fromPartitionColumns(partitionColumns);

        for (int i = 0; i < hudiColumnHandles.size(); i++) {
            HudiColumnHandle column = hudiColumnHandles.get(i);
            Column partitionColumn = partitionColumns.get(i);
            if (domains.containsKey(column)) {
                partitionPredicate.put(partitionColumn, domains.get(column));
            }
            else {
                partitionPredicate.put(partitionColumn, Domain.all(column.getHiveType().getType(typeManager)));
            }
        }
        return metastore.getPartitionNamesByFilter(metastoreContext, schemaName, tableName, partitionPredicate);
    }
}

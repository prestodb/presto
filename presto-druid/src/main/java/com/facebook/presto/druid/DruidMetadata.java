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
package com.facebook.presto.druid;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.druid.DruidClient.RemoteTableObject;
import com.facebook.presto.druid.ingestion.DruidIngestionTableHandle;
import com.facebook.presto.druid.metadata.DruidColumnInfo;
import com.facebook.presto.druid.metadata.DruidColumnType;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorNewTableLayout;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.facebook.presto.spi.statistics.ComputedStatistics;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import jakarta.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class DruidMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(DruidMetadata.class);

    private final DruidClient druidClient;

    @Inject
    public DruidMetadata(DruidClient druidClient)
    {
        this.druidClient = requireNonNull(druidClient, "druidClient is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return druidClient.getSchemas();
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName)
    {
        String remoteTableName = druidClient.toRemoteTable(schemaTableName)
                .map(RemoteTableObject::getOnlyRemoteTableName)
                .orElse(schemaTableName.getTableName());

        return druidClient.getTables().stream()
                .filter(name -> name.equals(remoteTableName))
                .map(name -> new DruidTableHandle(druidClient.getSchema(), remoteTableName, Optional.empty()))
                .findFirst()
                .orElse(null);
    }

    @Override
    public ConnectorTableLayoutResult getTableLayoutForConstraint(
            ConnectorSession session,
            ConnectorTableHandle table,
            Constraint<ColumnHandle> constraint,
            Optional<Set<ColumnHandle>> desiredColumns)
    {
        DruidTableHandle handle = (DruidTableHandle) table;
        ConnectorTableLayout layout = new ConnectorTableLayout(new DruidTableLayoutHandle(handle, constraint.getSummary()));
        return new ConnectorTableLayoutResult(layout, constraint.getSummary());
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(handle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        DruidTableHandle druidTable = (DruidTableHandle) tableHandle;
        List<ColumnMetadata> columns = druidClient.getColumnDataType(druidTable.getTableName()).stream()
                .map(column -> toColumnMetadata(session, column))
                .collect(toImmutableList());

        return new ConnectorTableMetadata(druidTable.toSchemaTableName(), columns);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
        for (String table : druidClient.getTables()) {
            // Ignore ambiguous tables
            boolean isAmbiguous = druidClient.toRemoteTable(new SchemaTableName(druidClient.getSchema(), table))
                    .filter(RemoteTableObject::isAmbiguous)
                    .isPresent();

            if (!isAmbiguous) {
                tableNames.add(new SchemaTableName(druidClient.getSchema(), table));
            }
            else {
                log.debug("Filtered out [%s.%s] from list of tables due to ambiguous name", druidClient.getSchema(), table);
            }
        }

        return tableNames.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        DruidTableHandle druidTable = (DruidTableHandle) tableHandle;
        return druidClient.getColumnDataType(druidTable.getTableName()).stream()
                .collect(toImmutableMap(DruidColumnInfo::getColumnName, column -> toColumnHandle(column)));
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(session, getTableHandle(session, tableName));
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((DruidColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        DruidTableHandle druidTableHandle = (DruidTableHandle) tableHandle;
        List<DruidColumnInfo> columns = druidClient.getColumnDataType(druidTableHandle.getTableName());
        return new DruidIngestionTableHandle(druidTableHandle.getSchemaName(), druidTableHandle.getTableName(), columns);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return Optional.empty();
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        return new DruidIngestionTableHandle(
                tableMetadata.getTable().getSchemaName(),
                tableMetadata.getTable().getTableName(),
                tableMetadata.getColumns().stream()
                        .map(column -> new DruidColumnInfo(column.getName(), DruidColumnType.fromPrestoType(column.getType())))
                        .collect(Collectors.toList()));
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return Optional.empty();
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getTableName() == null) {
            return listTables(session, Optional.ofNullable(prefix.getSchemaName()));
        }
        return ImmutableList.of(prefix.toSchemaTableName());
    }

    private ColumnMetadata toColumnMetadata(ConnectorSession session, DruidColumnInfo column)
    {
        return ColumnMetadata.builder()
                .setName(normalizeIdentifier(session, column.getColumnName()))
                .setType(column.getDataType().getPrestoType())
                .build();
    }

    private static ColumnHandle toColumnHandle(DruidColumnInfo column)
    {
        return new DruidColumnHandle(column.getColumnName(), column.getDataType().getPrestoType());
    }
}

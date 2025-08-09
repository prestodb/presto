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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveColumnConverterProvider;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.facebook.presto.hive.HiveColumnHandle.MAX_PARTITION_KEY_COLUMN_INDEX;
import static com.facebook.presto.hudi.HudiColumnHandle.ColumnType.PARTITION_KEY;
import static com.facebook.presto.hudi.HudiColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hudi.HudiErrorCode.HUDI_UNKNOWN_TABLE_TYPE;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class HudiMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(HudiMetadata.class);

    private final ExtendedHiveMetastore metastore;
    private final HdfsEnvironment hdfsEnvironment;
    private final TypeManager typeManager;

    public HudiMetadata(
            ExtendedHiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager)
    {
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return metastore.getAllDatabases(toMetastoreContext(session));
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        Optional<Table> hiveTable = metastore.getTable(toMetastoreContext(session), tableName.getSchemaName(), tableName.getTableName());
        if (!hiveTable.isPresent()) {
            return null;
        }

        Table table = hiveTable.get();
        String inputFormat = table.getStorage().getStorageFormat().getInputFormat();
        HudiTableType hudiTableType = HudiTableType.fromInputFormat(inputFormat);

        if (hudiTableType == HudiTableType.UNKNOWN) {
            throw new PrestoException(HUDI_UNKNOWN_TABLE_TYPE, "Unknown table type " + inputFormat);
        }

        return new HudiTableHandle(
                table.getDatabaseName(),
                table.getTableName(),
                table.getStorage().getLocation(),
                hudiTableType);
    }

    @Override
    public Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        // TODO: support hive flavour system tables
        return Optional.empty();
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle tableHandle, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        HudiTableHandle handle = (HudiTableHandle) tableHandle;
        Table table = getTable(session, tableHandle);
        List<HudiColumnHandle> partitionColumns = getPartitionColumnHandles(table);
        List<HudiColumnHandle> dataColumns = getDataColumnHandles(table);
        ConnectorTableLayout layout = new ConnectorTableLayout(new HudiTableLayoutHandle(
                handle,
                dataColumns,
                partitionColumns,
                table.getParameters(),
                constraint.getSummary()));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(handle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        return getTableMetadata(session, ((HudiTableHandle) table).getSchemaTableName());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        MetastoreContext metastoreContext = toMetastoreContext(session);
        return metastore
                .getAllTables(metastoreContext, schemaName.get())
                .orElseGet(() -> metastore.getAllDatabases(metastoreContext))
                .stream()
                .map(table -> new SchemaTableName(schemaName.get(), table))
                .collect(toList());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        Table table = getTable(session, tableHandle);
        return allColumnHandles(table).collect(toImmutableMap(HudiColumnHandle::getName, Function.identity()));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((HudiColumnHandle) columnHandle).toColumnMetadata(typeManager);
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        List<SchemaTableName> tables = prefix.getTableName() != null ? singletonList(prefix.toSchemaTableName()) : listTables(session, Optional.ofNullable(prefix.getSchemaName()));

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName table : tables) {
            try {
                columns.put(table, getTableMetadata(session, table).getColumns());
            }
            catch (TableNotFoundException e) {
                log.warn(String.format("table disappeared during listing operation: %s", e.getMessage()));
            }
        }
        return columns.build();
    }

    public ExtendedHiveMetastore getMetastore()
    {
        return metastore;
    }

    private Table getTable(ConnectorSession connectorSession, ConnectorTableHandle tableHandle)
    {
        MetastoreContext metastoreContext = toMetastoreContext(connectorSession);
        HudiTableHandle handle = (HudiTableHandle) tableHandle;
        Optional<Table> table = metastore.getTable(metastoreContext, handle.getSchemaName(), handle.getTableName());
        checkArgument(table.isPresent());
        return table.get();
    }

    private ConnectorTableMetadata getTableMetadata(ConnectorSession session, SchemaTableName tableName)
    {
        Table table = metastore.getTable(
                toMetastoreContext(session),
                tableName.getSchemaName(),
                tableName.getTableName()).orElseThrow(() -> new TableNotFoundException(tableName));

        List<ColumnMetadata> columnMetadatas = allColumnHandles(table)
                .map(columnHandle -> columnHandle.toColumnMetadata(typeManager, normalizeIdentifier(session, columnHandle.getName())))
                .collect(toList());
        return new ConnectorTableMetadata(tableName, columnMetadatas);
    }

    private Stream<HudiColumnHandle> allColumnHandles(Table table)
    {
        return Stream.concat(getDataColumnHandles(table).stream(), getPartitionColumnHandles(table).stream());
    }

    private List<HudiColumnHandle> getDataColumnHandles(Table table)
    {
        return fromDataColumns(table.getDataColumns());
    }

    private List<HudiColumnHandle> getPartitionColumnHandles(Table table)
    {
        return fromPartitionColumns(table.getPartitionColumns());
    }

    static List<HudiColumnHandle> fromPartitionColumns(List<Column> partitionColumns)
    {
        ImmutableList.Builder<HudiColumnHandle> builder = ImmutableList.builderWithExpectedSize(partitionColumns.size());
        int id = MAX_PARTITION_KEY_COLUMN_INDEX;
        for (Column column : partitionColumns) {
            HiveType hiveType = column.getType();
            if (!hiveType.isSupportedType()) {
                throw new PrestoException(NOT_SUPPORTED, String.format("Partition key type %s not supported", hiveType));
            }
            builder.add(fromPartitionColumn(id, column));
            id--;
        }
        return builder.build();
    }

    static List<HudiColumnHandle> fromDataColumns(List<Column> dataColumns)
    {
        ImmutableList.Builder<HudiColumnHandle> builder = ImmutableList.builder();
        int id = 0;
        for (Column column : dataColumns) {
            HiveType hiveType = column.getType();
            if (hiveType.isSupportedType()) {
                builder.add(fromDataColumn(id, column));
            }
            id++;
        }
        return builder.build();
    }

    public static MetastoreContext toMetastoreContext(ConnectorSession session)
    {
        return new MetastoreContext(session.getIdentity(), session.getQueryId(), session.getClientInfo(), session.getClientTags(), session.getSource(), Optional.empty(), false, HiveColumnConverterProvider.DEFAULT_COLUMN_CONVERTER_PROVIDER, session.getWarningCollector(), session.getRuntimeStats());
    }

    private static HudiColumnHandle fromDataColumn(int index, Column column)
    {
        return new HudiColumnHandle(index, column.getName(), column.getType(), column.getComment(), REGULAR);
    }

    private static HudiColumnHandle fromPartitionColumn(int index, Column column)
    {
        return new HudiColumnHandle(index, column.getName(), column.getType(), column.getComment(), PARTITION_KEY);
    }
}

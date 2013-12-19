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
package com.facebook.presto.metadata;

import com.facebook.presto.connector.informationSchema.InformationSchemaMetadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.OutputTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.inject.Singleton;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.facebook.presto.metadata.MetadataUtil.checkCatalogName;
import static com.facebook.presto.metadata.MetadataUtil.checkColumnName;
import static com.facebook.presto.metadata.QualifiedTableName.convertFromSchemaTableName;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.transform;

@Singleton
public class MetadataManager
        implements Metadata
{
    public static final String INTERNAL_CONNECTOR_ID = "$internal";

    // Note this must be a list to assure dual is always checked first
    private final CopyOnWriteArrayList<ConnectorMetadataEntry> internalSchemas = new CopyOnWriteArrayList<>();
    private final ConcurrentMap<String, ConnectorMetadataEntry> connectors = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ConnectorMetadataEntry> informationSchemas = new ConcurrentHashMap<>();
    private final FunctionRegistry functions = new FunctionRegistry();

    public void addConnectorMetadata(String connectorId, String catalogName, ConnectorMetadata connectorMetadata)
    {
        ConnectorMetadataEntry entry = new ConnectorMetadataEntry(connectorId, connectorMetadata);
        checkState(connectors.putIfAbsent(catalogName, entry) == null, "Catalog '%s' is already registered", catalogName);

        informationSchemas.put(catalogName, new ConnectorMetadataEntry(INTERNAL_CONNECTOR_ID, new InformationSchemaMetadata(catalogName)));
    }

    public void addInternalSchemaMetadata(String connectorId, ConnectorMetadata connectorMetadata)
    {
        checkNotNull(connectorId, "connectorId is null");
        checkNotNull(connectorMetadata, "connectorMetadata is null");

        internalSchemas.add(new ConnectorMetadataEntry(connectorId, connectorMetadata));
    }

    @Override
    public FunctionInfo getFunction(QualifiedName name, List<Type> parameterTypes)
    {
        return functions.get(name, parameterTypes);
    }

    @Override
    public FunctionInfo getFunction(FunctionHandle handle)
    {
        return functions.get(handle);
    }

    @Override
    public boolean isAggregationFunction(QualifiedName name)
    {
        return functions.isAggregationFunction(name);
    }

    @Override
    public List<FunctionInfo> listFunctions()
    {
        return functions.list();
    }

    @Override
    public List<String> listSchemaNames(String catalogName)
    {
        checkCatalogName(catalogName);
        ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
        for (ConnectorMetadataEntry entry : allConnectorsFor(catalogName)) {
            schemaNames.addAll(entry.getMetadata().listSchemaNames());
        }
        return ImmutableList.copyOf(schemaNames.build());
    }

    @Override
    public Optional<TableHandle> getTableHandle(QualifiedTableName table)
    {
        checkNotNull(table, "table is null");

        SchemaTableName tableName = table.asSchemaTableName();
        for (ConnectorMetadataEntry entry : allConnectorsFor(table.getCatalogName())) {
            TableHandle tableHandle = entry.getMetadata().getTableHandle(tableName);
            if (tableHandle != null) {
                return Optional.of(tableHandle);
            }
        }
        return Optional.absent();
    }

    @Override
    public TableMetadata getTableMetadata(TableHandle tableHandle)
    {
        ConnectorTableMetadata tableMetadata = lookupConnectorFor(tableHandle).getMetadata().getTableMetadata(tableHandle);
        return new TableMetadata(getConnectorId(tableHandle), tableMetadata);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(TableHandle tableHandle)
    {
        return lookupConnectorFor(tableHandle).getMetadata().getColumnHandles(tableHandle);
    }

    @Override
    public ColumnMetadata getColumnMetadata(TableHandle tableHandle, ColumnHandle columnHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkNotNull(columnHandle, "columnHandle is null");

        return lookupConnectorFor(tableHandle).getMetadata().getColumnMetadata(tableHandle, columnHandle);
    }

    @Override
    public List<QualifiedTableName> listTables(QualifiedTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");

        String schemaNameOrNull = prefix.getSchemaName().orNull();
        LinkedHashSet<QualifiedTableName> tables = new LinkedHashSet<>();
        for (ConnectorMetadataEntry entry : allConnectorsFor(prefix.getCatalogName())) {
            for (QualifiedTableName tableName : transform(entry.getMetadata().listTables(schemaNameOrNull), convertFromSchemaTableName(prefix.getCatalogName()))) {
                tables.add(tableName);
            }
        }
        return ImmutableList.copyOf(tables);
    }

    @Override
    public Optional<ColumnHandle> getColumnHandle(TableHandle tableHandle, String columnName)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkColumnName(columnName);

        return Optional.fromNullable(lookupConnectorFor(tableHandle).getMetadata().getColumnHandle(tableHandle, columnName));
    }

    @Override
    public Map<QualifiedTableName, List<ColumnMetadata>> listTableColumns(QualifiedTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");

        LinkedHashMap<QualifiedTableName, List<ColumnMetadata>> tableColumns = new LinkedHashMap<>();
        for (ConnectorMetadataEntry connectorMetadata : allConnectorsFor(prefix.getCatalogName())) {
            for (Entry<SchemaTableName, List<ColumnMetadata>> entry : connectorMetadata.getMetadata().listTableColumns(prefix.asSchemaTablePrefix()).entrySet()) {
                QualifiedTableName tableName = new QualifiedTableName(prefix.getCatalogName(), entry.getKey().getSchemaName(), entry.getKey().getTableName());
                if (!tableColumns.containsKey(tableName)) {
                    tableColumns.put(tableName, entry.getValue());
                }
            }
        }
        return ImmutableMap.copyOf(tableColumns);
    }

    @Override
    public TableHandle createTable(String catalogName, TableMetadata tableMetadata)
    {
        ConnectorMetadataEntry connectorMetadata = connectors.get(catalogName);
        checkArgument(connectorMetadata != null, "Catalog %s does not exist", catalogName);
        return connectorMetadata.getMetadata().createTable(tableMetadata.getMetadata());
    }

    @Override
    public void dropTable(TableHandle tableHandle)
    {
        lookupConnectorFor(tableHandle).getMetadata().dropTable(tableHandle);
    }

    @Override
    public OutputTableHandle beginCreateTable(String catalogName, TableMetadata tableMetadata)
    {
        ConnectorMetadataEntry connectorMetadata = connectors.get(catalogName);
        checkArgument(connectorMetadata != null, "Catalog %s does not exist", catalogName);
        return connectorMetadata.getMetadata().beginCreateTable(tableMetadata.getMetadata());
    }

    @Override
    public void commitCreateTable(OutputTableHandle tableHandle, Collection<String> fragments)
    {
        lookupConnectorFor(tableHandle).getMetadata().commitCreateTable(tableHandle, fragments);
    }

    @Override
    public String getConnectorId(TableHandle tableHandle)
    {
        return lookupConnectorFor(tableHandle).getConnectorId();
    }

    @Override
    public Optional<TableHandle> getTableHandle(String connectorId, SchemaTableName tableName)
    {
        // use catalog name in place of connector id
        ConnectorMetadataEntry entry = connectors.get(connectorId);
        if (entry == null) {
            return Optional.absent();
        }
        return Optional.fromNullable(entry.getMetadata().getTableHandle(tableName));
    }

    @Override
    public Map<String, String> getCatalogNames()
    {
        ImmutableMap.Builder<String, String> catalogsMap = ImmutableMap.builder();
        for(Map.Entry<String, ConnectorMetadataEntry> entry : connectors.entrySet()) {
            catalogsMap.put(entry.getKey(), entry.getValue().getConnectorId());
        }
        return catalogsMap.build();
    }

    private List<ConnectorMetadataEntry> allConnectorsFor(String catalogName)
    {
        ImmutableList.Builder<ConnectorMetadataEntry> builder = ImmutableList.builder();
        builder.addAll(internalSchemas);
        ConnectorMetadataEntry connector = connectors.get(catalogName);
        if (connector != null) {
            builder.add(connector);
        }
        ConnectorMetadataEntry informationSchema = informationSchemas.get(catalogName);
        if (informationSchema != null) {
            builder.add(informationSchema);
        }
        return builder.build();
    }

    private ConnectorMetadataEntry lookupConnectorFor(TableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");

        for (Entry<String, ConnectorMetadataEntry> entry : informationSchemas.entrySet()) {
            if (entry.getValue().getMetadata().canHandle(tableHandle)) {
                return entry.getValue();
            }
        }

        for (ConnectorMetadataEntry entry : internalSchemas) {
            if (entry.getMetadata().canHandle(tableHandle)) {
                return entry;

            }
        }

        for (Entry<String, ConnectorMetadataEntry> entry : connectors.entrySet()) {
            if (entry.getValue().getMetadata().canHandle(tableHandle)) {
                return entry.getValue();
            }
        }

        throw new IllegalArgumentException("Table %s does not exist: " + tableHandle);
    }

    private ConnectorMetadataEntry lookupConnectorFor(OutputTableHandle tableHandle)
    {
        for (Entry<String, ConnectorMetadataEntry> entry : connectors.entrySet()) {
            if (entry.getValue().getMetadata().canHandle(tableHandle)) {
                return entry.getValue();
            }
        }
        throw new IllegalArgumentException("No connector for output table handle: " + tableHandle);
    }

    private static class ConnectorMetadataEntry
    {
        private final String connectorId;
        private final ConnectorMetadata metadata;

        private ConnectorMetadataEntry(String connectorId, ConnectorMetadata metadata)
        {
            Preconditions.checkNotNull(connectorId, "connectorId is null");
            Preconditions.checkNotNull(metadata, "metadata is null");

            this.connectorId = connectorId;
            this.metadata = metadata;
        }

        private String getConnectorId()
        {
            return connectorId;
        }

        private ConnectorMetadata getMetadata()
        {
            return metadata;
        }
    }
}

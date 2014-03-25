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

import com.facebook.presto.metadata.OperatorInfo.OperatorType;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import javax.inject.Inject;
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

import static com.facebook.presto.metadata.ColumnHandle.fromConnectorHandle;
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
    // Note this must be a list to assure dual is always checked first
    private final CopyOnWriteArrayList<ConnectorMetadataEntry> internalSchemas = new CopyOnWriteArrayList<>();
    private final ConcurrentMap<String, ConnectorMetadataEntry> connectors = new ConcurrentHashMap<>();
    private final FunctionRegistry functions;
    private final TypeManager typeManager;

    public MetadataManager()
    {
        this(new FeaturesConfig(), new TypeRegistry());
    }

    @Inject
    public MetadataManager(FeaturesConfig featuresConfig, TypeManager typeManager)
    {
        functions = new FunctionRegistry(featuresConfig.isExperimentalSyntaxEnabled());
        this.typeManager = checkNotNull(typeManager, "types is null");
    }

    public void addConnectorMetadata(String connectorId, String catalogName, ConnectorMetadata connectorMetadata)
    {
        ConnectorMetadataEntry entry = new ConnectorMetadataEntry(connectorId, connectorMetadata);
        checkState(connectors.putIfAbsent(catalogName, entry) == null, "Catalog '%s' is already registered", catalogName);
    }

    public void addInternalSchemaMetadata(String connectorId, ConnectorMetadata connectorMetadata)
    {
        checkNotNull(connectorId, "connectorId is null");
        checkNotNull(connectorMetadata, "connectorMetadata is null");

        internalSchemas.add(new ConnectorMetadataEntry(connectorId, connectorMetadata));
    }

    @Override
    public Type getType(String typeName)
    {
        return typeManager.getType(typeName);
    }

    @Override
    public FunctionInfo resolveFunction(QualifiedName name, List<? extends Type> parameterTypes, boolean approximate)
    {
        return functions.resolveFunction(name, parameterTypes, approximate);
    }

    @Override
    public FunctionInfo getExactFunction(Signature handle)
    {
        return functions.getExactFunction(handle);
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
    public void addFunctions(List<FunctionInfo> functionInfos)
    {
        functions.addFunctions(functionInfos, ImmutableList.<OperatorInfo>of());
    }

    @Override
    public OperatorInfo resolveOperator(OperatorType operatorType, List<? extends Type> argumentTypes)
            throws OperatorNotFoundException
    {
        return functions.resolveOperator(operatorType, argumentTypes);
    }

    @Override
    public OperatorInfo getExactOperator(OperatorType operatorType, Type returnType, List<? extends Type> argumentTypes)
            throws OperatorNotFoundException
    {
        return functions.getExactOperator(operatorType, argumentTypes, returnType);
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
            ConnectorTableHandle tableHandle = entry.getMetadata().getTableHandle(tableName);
            if (tableHandle != null) {
                return Optional.of(new TableHandle(entry.getConnectorId(), tableHandle));
            }
        }
        return Optional.absent();
    }

    @Override
    public TableMetadata getTableMetadata(TableHandle tableHandle)
    {
        ConnectorTableMetadata tableMetadata = lookupConnectorFor(tableHandle).getTableMetadata(tableHandle.getConnectorHandle());

        return new TableMetadata(tableHandle.getConnectorId(), tableMetadata);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(TableHandle tableHandle)
    {
        Map<String, ConnectorColumnHandle> columns = lookupConnectorFor(tableHandle).getColumnHandles(tableHandle.getConnectorHandle());

        return Maps.transformValues(columns, fromConnectorHandle(tableHandle.getConnectorId()));
    }

    @Override
    public ColumnMetadata getColumnMetadata(TableHandle tableHandle, ColumnHandle columnHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkNotNull(columnHandle, "columnHandle is null");

        return lookupConnectorFor(tableHandle).getColumnMetadata(tableHandle.getConnectorHandle(), columnHandle.getConnectorHandle());
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

        ConnectorColumnHandle handle = lookupConnectorFor(tableHandle).getColumnHandle(tableHandle.getConnectorHandle(), columnName);

        if (handle == null) {
            return Optional.absent();
        }

        return Optional.of(new ColumnHandle(tableHandle.getConnectorId(), handle));
    }

    @Override
    public Optional<ColumnHandle> getSampleWeightColumnHandle(TableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        ConnectorColumnHandle handle = lookupConnectorFor(tableHandle).getSampleWeightColumnHandle(tableHandle.getConnectorHandle());

        if (handle == null) {
            return Optional.absent();
        }

        return Optional.of(new ColumnHandle(tableHandle.getConnectorId(), handle));
    }

    @Override
    public boolean canCreateSampledTables(String catalogName)
    {
        ConnectorMetadataEntry connectorMetadata = connectors.get(catalogName);
        checkArgument(connectorMetadata != null, "Catalog %s does not exist", catalogName);
        return connectorMetadata.getMetadata().canCreateSampledTables();
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

        ConnectorTableHandle handle = connectorMetadata.getMetadata().createTable(tableMetadata.getMetadata());
        return new TableHandle(connectorMetadata.getConnectorId(), handle);
    }

    @Override
    public void dropTable(TableHandle tableHandle)
    {
        lookupConnectorFor(tableHandle).dropTable(tableHandle.getConnectorHandle());
    }

    @Override
    public OutputTableHandle beginCreateTable(String catalogName, TableMetadata tableMetadata)
    {
        ConnectorMetadataEntry connectorMetadata = connectors.get(catalogName);
        checkArgument(connectorMetadata != null, "Catalog %s does not exist", catalogName);
        ConnectorOutputTableHandle handle = connectorMetadata.getMetadata().beginCreateTable(tableMetadata.getMetadata());
        return new OutputTableHandle(connectorMetadata.getConnectorId(), handle);
    }

    @Override
    public void commitCreateTable(OutputTableHandle tableHandle, Collection<String> fragments)
    {
        lookupConnectorFor(tableHandle)
                .getMetadata()
                .commitCreateTable(tableHandle.getConnectorHandle(), fragments);
    }

    @Override
    public Map<String, String> getCatalogNames()
    {
        ImmutableMap.Builder<String, String> catalogsMap = ImmutableMap.builder();
        for (Map.Entry<String, ConnectorMetadataEntry> entry : connectors.entrySet()) {
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
        return builder.build();
    }

    private ConnectorMetadata lookupConnectorFor(TableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");

        for (ConnectorMetadataEntry entry : internalSchemas) {
            if (entry.getMetadata().canHandle(tableHandle.getConnectorHandle())) {
                return entry.getMetadata();
            }
        }

        for (Entry<String, ConnectorMetadataEntry> entry : connectors.entrySet()) {
            if (entry.getValue().getMetadata().canHandle(tableHandle.getConnectorHandle())) {
                return entry.getValue().getMetadata();
            }
        }

        throw new IllegalArgumentException("Table %s does not exist: " + tableHandle);
    }

    private ConnectorMetadataEntry lookupConnectorFor(OutputTableHandle tableHandle)
    {
        for (Entry<String, ConnectorMetadataEntry> entry : connectors.entrySet()) {
            if (entry.getValue().getMetadata().canHandle(tableHandle.getConnectorHandle())) {
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

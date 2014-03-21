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
import com.facebook.presto.spi.ConnectorMetadataProvider;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.OutputTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.Session;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;

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
    private final FunctionRegistry functions;

    @Inject
    public MetadataManager(FeaturesConfig featuresConfig)
    {
        functions = new FunctionRegistry(featuresConfig.isExperimentalSyntaxEnabled());
    }

    public void addConnectorMetadataProvider(String connectorId, String catalogName, ConnectorMetadataProvider connectorMetadataProvider)
    {
        ConnectorMetadataEntry entry = new ConnectorMetadataEntry(connectorId, connectorMetadataProvider);
        checkState(connectors.putIfAbsent(catalogName, entry) == null, "Catalog '%s' is already registered", catalogName);

        informationSchemas.put(catalogName, new ConnectorMetadataEntry(INTERNAL_CONNECTOR_ID, new InformationSchemaMetadata(catalogName)));
    }

    public void addInternalSchemaMetadataProvider(String connectorId, ConnectorMetadataProvider connectorMetadataProvider)
    {
        checkNotNull(connectorId, "connectorId is null");
        checkNotNull(connectorMetadataProvider, "connectorMetadata is null");

        internalSchemas.add(new ConnectorMetadataEntry(connectorId, connectorMetadataProvider));
    }

    @Override
    public FunctionInfo getFunction(QualifiedName name, List<Type> parameterTypes, boolean approximate)
    {
        return functions.get(name, parameterTypes, approximate);
    }

    @Override
    public FunctionInfo getFunction(Signature handle)
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
    public void addFunctions(List<FunctionInfo> functionInfos)
    {
        functions.addFunctions(functionInfos);
    }

    @Override
    public List<String> listSchemaNames(Session session, String catalogName)
    {
        checkCatalogName(catalogName);
        ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
        for (ConnectorMetadataEntry entry : allConnectorsFor(catalogName)) {
            schemaNames.addAll(entry.getMetadata(session).listSchemaNames());
        }
        return ImmutableList.copyOf(schemaNames.build());
    }

    @Override
    public Optional<TableHandle> getTableHandle(Session session, QualifiedTableName table)
    {
        checkNotNull(table, "table is null");

        SchemaTableName tableName = table.asSchemaTableName();
        for (ConnectorMetadataEntry entry : allConnectorsFor(table.getCatalogName())) {
            TableHandle tableHandle = entry.getMetadata(session).getTableHandle(tableName);
            if (tableHandle != null) {
                return Optional.of(tableHandle);
            }
        }
        return Optional.absent();
    }

    @Override
    public TableMetadata getTableMetadata(Session session, TableHandle tableHandle)
    {
        ConnectorTableMetadata tableMetadata = lookupConnectorFor(session, tableHandle).getMetadata(session).getTableMetadata(tableHandle);
        return new TableMetadata(lookupConnectorFor(session, tableHandle).getConnectorId(), tableMetadata);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(Session session, TableHandle tableHandle)
    {
        return lookupConnectorFor(session, tableHandle).getMetadata(session).getColumnHandles(tableHandle);
    }

    @Override
    public ColumnMetadata getColumnMetadata(Session session, TableHandle tableHandle, ColumnHandle columnHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkNotNull(columnHandle, "columnHandle is null");

        return lookupConnectorFor(session, tableHandle).getMetadata(session).getColumnMetadata(tableHandle, columnHandle);
    }

    @Override
    public List<QualifiedTableName> listTables(Session session, QualifiedTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");

        String schemaNameOrNull = prefix.getSchemaName().orNull();
        LinkedHashSet<QualifiedTableName> tables = new LinkedHashSet<>();
        for (ConnectorMetadataEntry entry : allConnectorsFor(prefix.getCatalogName())) {
            for (QualifiedTableName tableName : transform(entry.getMetadata(session).listTables(schemaNameOrNull), convertFromSchemaTableName(prefix.getCatalogName()))) {
                tables.add(tableName);
            }
        }
        return ImmutableList.copyOf(tables);
    }

    @Override
    public Optional<ColumnHandle> getColumnHandle(Session session, TableHandle tableHandle, String columnName)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkColumnName(columnName);

        return Optional.fromNullable(lookupConnectorFor(session, tableHandle).getMetadata(session).getColumnHandle(tableHandle, columnName));
    }

    @Override
    public Optional<ColumnHandle> getSampleWeightColumnHandle(Session session, TableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        return Optional.fromNullable(lookupConnectorFor(session, tableHandle).getMetadata(session).getSampleWeightColumnHandle(tableHandle));
    }

    @Override
    public boolean canCreateSampledTables(Session session, String catalogName)
    {
        ConnectorMetadataEntry connectorMetadata = connectors.get(catalogName);
        checkArgument(connectorMetadata != null, "Catalog %s does not exist", catalogName);
        return connectorMetadata.getMetadata(session).canCreateSampledTables();
    }

    @Override
    public Map<QualifiedTableName, List<ColumnMetadata>> listTableColumns(Session session, QualifiedTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");

        LinkedHashMap<QualifiedTableName, List<ColumnMetadata>> tableColumns = new LinkedHashMap<>();
        for (ConnectorMetadataEntry connectorMetadata : allConnectorsFor(prefix.getCatalogName())) {
            for (Entry<SchemaTableName, List<ColumnMetadata>> entry : connectorMetadata.getMetadata(session).listTableColumns(prefix.asSchemaTablePrefix()).entrySet()) {
                QualifiedTableName tableName = new QualifiedTableName(prefix.getCatalogName(), entry.getKey().getSchemaName(), entry.getKey().getTableName());
                if (!tableColumns.containsKey(tableName)) {
                    tableColumns.put(tableName, entry.getValue());
                }
            }
        }
        return ImmutableMap.copyOf(tableColumns);
    }

    @Override
    public TableHandle createTable(Session session, String catalogName, TableMetadata tableMetadata)
    {
        ConnectorMetadataEntry connectorMetadata = connectors.get(catalogName);
        checkArgument(connectorMetadata != null, "Catalog %s does not exist", catalogName);
        return connectorMetadata.getMetadata(session).createTable(tableMetadata.getMetadata());
    }

    @Override
    public void dropTable(Session session, TableHandle tableHandle)
    {
        lookupConnectorFor(session, tableHandle).getMetadata(session).dropTable(tableHandle);
    }

    @Override
    public OutputTableHandle beginCreateTable(Session session, String catalogName, TableMetadata tableMetadata)
    {
        ConnectorMetadataEntry connectorMetadata = connectors.get(catalogName);
        checkArgument(connectorMetadata != null, "Catalog %s does not exist", catalogName);
        return connectorMetadata.getMetadata(session).beginCreateTable(tableMetadata.getMetadata());
    }

    @Override
    public void commitCreateTable(Session session, OutputTableHandle tableHandle, Collection<String> fragments)
    {
        lookupConnectorFor(session, tableHandle).getMetadata(session).commitCreateTable(tableHandle, fragments);
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
        ConnectorMetadataEntry informationSchema = informationSchemas.get(catalogName);
        if (informationSchema != null) {
            builder.add(informationSchema);
        }
        return builder.build();
    }

    private ConnectorMetadataEntry lookupConnectorFor(Session session, TableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");

        for (Entry<String, ConnectorMetadataEntry> entry : informationSchemas.entrySet()) {
            if (entry.getValue().getMetadata(session).canHandle(tableHandle)) {
                return entry.getValue();
            }
        }

        for (ConnectorMetadataEntry entry : internalSchemas) {
            if (entry.getMetadata(session).canHandle(tableHandle)) {
                return entry;
            }
        }

        for (Entry<String, ConnectorMetadataEntry> entry : connectors.entrySet()) {
            if (entry.getValue().getMetadata(session).canHandle(tableHandle)) {
                return entry.getValue();
            }
        }

        throw new IllegalArgumentException("Table %s does not exist: " + tableHandle);
    }

    private ConnectorMetadataEntry lookupConnectorFor(Session session, OutputTableHandle tableHandle)
    {
        for (Entry<String, ConnectorMetadataEntry> entry : connectors.entrySet()) {
            if (entry.getValue().getMetadata(session).canHandle(tableHandle)) {
                return entry.getValue();
            }
        }
        throw new IllegalArgumentException("No connector for output table handle: " + tableHandle);
    }

    private static class ConnectorMetadataEntry
    {
        private final String connectorId;
        private final ConnectorMetadataProvider metadataProvider;

        private ConnectorMetadataEntry(String connectorId, ConnectorMetadataProvider metadataProvider)
        {
            Preconditions.checkNotNull(connectorId, "connectorId is null");
            Preconditions.checkNotNull(metadataProvider, "metadataProvider is null");

            this.connectorId = connectorId;
            this.metadataProvider = metadataProvider;
        }

        private String getConnectorId()
        {
            return connectorId;
        }

        private ConnectorMetadata getMetadata(Session session)
        {
            checkState(metadataProvider.canHandle(session),
                    "Cannot handle session User = " + (session == null ? "none" : session.getUser()));
            return metadataProvider.getMetadata(session);
        }
    }
}

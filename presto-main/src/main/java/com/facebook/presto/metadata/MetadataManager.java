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
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.type.TypeDeserializer;
import com.facebook.presto.type.TypeRegistry;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.facebook.presto.metadata.ColumnHandle.fromConnectorHandle;
import static com.facebook.presto.metadata.MetadataUtil.checkCatalogName;
import static com.facebook.presto.metadata.MetadataUtil.checkColumnName;
import static com.facebook.presto.metadata.QualifiedTableName.convertFromSchemaTableName;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_VIEW;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;

@Singleton
public class MetadataManager
        implements Metadata
{
    private final Set<String> globalConnectors = Sets.newConcurrentHashSet();
    private final ConcurrentMap<String, ConnectorMetadataEntry> informationSchemasByCatalog = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ConnectorMetadataEntry> connectorsByCatalog = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ConnectorMetadata> connectorsById = new ConcurrentHashMap<>();
    private final FunctionRegistry functions;
    private final TypeManager typeManager;
    private final JsonCodec<ViewDefinition> viewCodec;

    public MetadataManager()
    {
        this(new FeaturesConfig(), new TypeRegistry());
    }

    public MetadataManager(FeaturesConfig featuresConfig, TypeManager typeManager)
    {
        this(featuresConfig, typeManager, createTestingViewCodec());
    }

    @Inject
    public MetadataManager(FeaturesConfig featuresConfig, TypeManager typeManager, JsonCodec<ViewDefinition> viewCodec)
    {
        functions = new FunctionRegistry(typeManager, featuresConfig.isExperimentalSyntaxEnabled());
        this.typeManager = checkNotNull(typeManager, "types is null");
        this.viewCodec = checkNotNull(viewCodec, "viewCodec is null");
    }

    public synchronized void addConnectorMetadata(String connectorId, String catalogName, ConnectorMetadata connectorMetadata)
    {
        checkNotNull(connectorId, "connectorId is null");
        checkNotNull(catalogName, "catalogName is null");
        checkNotNull(connectorMetadata, "connectorMetadata is null");

        checkArgument(!connectorsByCatalog.containsKey(catalogName), "Catalog '%s' is already registered", catalogName);
        checkArgument(!connectorsById.containsKey(connectorId), "Connector '%s' is already registered", connectorId);

        ConnectorMetadataEntry entry = new ConnectorMetadataEntry(connectorId, connectorMetadata);

        connectorsById.put(connectorId, connectorMetadata);
        connectorsByCatalog.put(catalogName, entry);
    }

    public synchronized void addInformationSchemaMetadata(String connectorId, String catalogName, InformationSchemaMetadata metadata)
    {
        checkNotNull(connectorId, "connectorId is null");
        checkNotNull(catalogName, "catalogName is null");
        checkNotNull(metadata, "metadata is null");

        checkArgument(!connectorsById.containsKey(connectorId), "Connector '%s' is already registered", connectorId);
        checkArgument(!informationSchemasByCatalog.containsKey(catalogName), "Information schema for catalog '%s' is already registered", catalogName);

        connectorsById.put(connectorId, metadata);
        informationSchemasByCatalog.put(catalogName, new ConnectorMetadataEntry(connectorId, metadata));
    }

    public synchronized void addGlobalSchemaMetadata(String connectorId, ConnectorMetadata connectorMetadata)
    {
        checkNotNull(connectorId, "connectorId is null");
        checkNotNull(connectorMetadata, "connectorMetadata is null");

        checkArgument(!globalConnectors.contains(connectorId), "Global connector '%s' is already registered", connectorId);
        checkArgument(!connectorsById.containsKey(connectorId), "Connector '%s' is already registered", connectorId);

        connectorsById.put(connectorId, connectorMetadata);
        globalConnectors.add(connectorId);
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
        functions.addFunctions(functionInfos, ImmutableMultimap.<OperatorType, FunctionInfo>of());
    }

    @Override
    public void addOperators(Multimap<OperatorType, FunctionInfo> operators)
    {
        functions.addFunctions(ImmutableList.<FunctionInfo>of(), operators);
    }

    @Override
    public FunctionInfo resolveOperator(OperatorType operatorType, List<? extends Type> argumentTypes)
            throws OperatorNotFoundException
    {
        return functions.resolveOperator(operatorType, argumentTypes);
    }

    @Override
    public FunctionInfo getExactOperator(OperatorType operatorType, Type returnType, List<? extends Type> argumentTypes)
            throws OperatorNotFoundException
    {
        return functions.getExactOperator(operatorType, argumentTypes, returnType);
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session, String catalogName)
    {
        checkCatalogName(catalogName);
        ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
        for (ConnectorMetadataEntry entry : allConnectorsFor(catalogName)) {
            schemaNames.addAll(entry.getMetadata().listSchemaNames(session));
        }
        return ImmutableList.copyOf(schemaNames.build());
    }

    @Override
    public Optional<TableHandle> getTableHandle(ConnectorSession session, QualifiedTableName table)
    {
        checkNotNull(table, "table is null");

        SchemaTableName tableName = table.asSchemaTableName();
        for (ConnectorMetadataEntry entry : allConnectorsFor(table.getCatalogName())) {
            ConnectorTableHandle tableHandle = entry.getMetadata().getTableHandle(session, tableName);
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
    public List<QualifiedTableName> listTables(ConnectorSession session, QualifiedTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");

        String schemaNameOrNull = prefix.getSchemaName().orNull();
        Set<QualifiedTableName> tables = new LinkedHashSet<>();
        for (ConnectorMetadataEntry entry : allConnectorsFor(prefix.getCatalogName())) {
            for (QualifiedTableName tableName : transform(entry.getMetadata().listTables(session, schemaNameOrNull), convertFromSchemaTableName(prefix.getCatalogName()))) {
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
    public boolean canCreateSampledTables(ConnectorSession session, String catalogName)
    {
        ConnectorMetadataEntry connectorMetadata = connectorsByCatalog.get(catalogName);
        checkArgument(connectorMetadata != null, "Catalog %s does not exist", catalogName);
        return connectorMetadata.getMetadata().canCreateSampledTables(session);
    }

    @Override
    public Map<QualifiedTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, QualifiedTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");

        Map<QualifiedTableName, List<ColumnMetadata>> tableColumns = new LinkedHashMap<>();
        for (ConnectorMetadataEntry connectorMetadata : allConnectorsFor(prefix.getCatalogName())) {
            for (Entry<SchemaTableName, List<ColumnMetadata>> entry : connectorMetadata.getMetadata().listTableColumns(session, prefix.asSchemaTablePrefix()).entrySet()) {
                QualifiedTableName tableName = new QualifiedTableName(prefix.getCatalogName(), entry.getKey().getSchemaName(), entry.getKey().getTableName());
                if (!tableColumns.containsKey(tableName)) {
                    tableColumns.put(tableName, entry.getValue());
                }
            }
        }
        return ImmutableMap.copyOf(tableColumns);
    }

    @Override
    public TableHandle createTable(ConnectorSession session, String catalogName, TableMetadata tableMetadata)
    {
        ConnectorMetadataEntry connectorMetadata = connectorsByCatalog.get(catalogName);
        checkArgument(connectorMetadata != null, "Catalog %s does not exist", catalogName);

        ConnectorTableHandle handle = connectorMetadata.getMetadata().createTable(session, tableMetadata.getMetadata());
        return new TableHandle(connectorMetadata.getConnectorId(), handle);
    }

    @Override
    public void dropTable(TableHandle tableHandle)
    {
        lookupConnectorFor(tableHandle).dropTable(tableHandle.getConnectorHandle());
    }

    @Override
    public OutputTableHandle beginCreateTable(ConnectorSession session, String catalogName, TableMetadata tableMetadata)
    {
        ConnectorMetadataEntry connectorMetadata = connectorsByCatalog.get(catalogName);
        checkArgument(connectorMetadata != null, "Catalog %s does not exist", catalogName);
        ConnectorOutputTableHandle handle = connectorMetadata.getMetadata().beginCreateTable(session, tableMetadata.getMetadata());
        return new OutputTableHandle(connectorMetadata.getConnectorId(), handle);
    }

    @Override
    public void commitCreateTable(OutputTableHandle tableHandle, Collection<String> fragments)
    {
        lookupConnectorFor(tableHandle).commitCreateTable(tableHandle.getConnectorHandle(), fragments);
    }

    @Override
    public Map<String, String> getCatalogNames()
    {
        ImmutableMap.Builder<String, String> catalogsMap = ImmutableMap.builder();
        for (Map.Entry<String, ConnectorMetadataEntry> entry : connectorsByCatalog.entrySet()) {
            catalogsMap.put(entry.getKey(), entry.getValue().getConnectorId());
        }
        return catalogsMap.build();
    }

    @Override
    public Optional<ViewDefinition> getView(ConnectorSession session, QualifiedTableName viewName)
    {
        SchemaTablePrefix prefix = viewName.asSchemaTableName().toSchemaTablePrefix();
        for (ConnectorMetadataEntry entry : allConnectorsFor(viewName.getCatalogName())) {
            Map<SchemaTableName, String> views = entry.getMetadata().getViews(session, prefix);
            String view = views.get(viewName.asSchemaTableName());
            if (view != null) {
                try {
                    return Optional.of(viewCodec.fromJson(view));
                }
                catch (IllegalArgumentException e) {
                    throw new PrestoException(INVALID_VIEW.toErrorCode(), "Invalid view JSON: " + view, e);
                }
            }
        }
        return Optional.absent();
    }

    @Override
    public void createView(ConnectorSession session, QualifiedTableName viewName, String viewData, boolean replace)
    {
        ConnectorMetadataEntry connectorMetadata = connectorsByCatalog.get(viewName.getCatalogName());
        checkArgument(connectorMetadata != null, "Catalog %s does not exist", viewName.getCatalogName());
        connectorMetadata.getMetadata().createView(session, viewName.asSchemaTableName(), viewData, replace);
    }

    @Override
    public void dropView(ConnectorSession session, QualifiedTableName viewName)
    {
        ConnectorMetadataEntry connectorMetadata = connectorsByCatalog.get(viewName.getCatalogName());
        checkArgument(connectorMetadata != null, "Catalog %s does not exist", viewName.getCatalogName());
        connectorMetadata.getMetadata().dropView(session, viewName.asSchemaTableName());
    }

    private List<ConnectorMetadataEntry> allConnectorsFor(String catalogName)
    {
        ImmutableList.Builder<ConnectorMetadataEntry> builder = ImmutableList.builder();

        for (String connectorId : globalConnectors) {
            builder.add(new ConnectorMetadataEntry(connectorId, connectorsById.get(connectorId)));
        }

        ConnectorMetadataEntry entry = informationSchemasByCatalog.get(catalogName);
        if (entry != null) {
            builder.add(entry);
        }

        ConnectorMetadataEntry connector = connectorsByCatalog.get(catalogName);
        if (connector != null) {
            builder.add(connector);
        }

        return builder.build();
    }

    private ConnectorMetadata lookupConnectorFor(TableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");

        ConnectorMetadata result = connectorsById.get(tableHandle.getConnectorId());
        checkArgument(result != null, "No connector for table handle: %s", tableHandle.getConnectorId());

        return result;
    }

    private ConnectorMetadata lookupConnectorFor(OutputTableHandle tableHandle)
    {
        ConnectorMetadata metadata = connectorsById.get(tableHandle.getConnectorId());
        checkArgument(metadata != null, "No connector for output table handle: %s", tableHandle.getConnectorId());
        return metadata;
    }

    private static class ConnectorMetadataEntry
    {
        private final String connectorId;
        private final ConnectorMetadata metadata;

        private ConnectorMetadataEntry(String connectorId, ConnectorMetadata metadata)
        {
            this.connectorId =  checkNotNull(connectorId, "connectorId is null");
            this.metadata = checkNotNull(metadata, "metadata is null");
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

    private static JsonCodec<ViewDefinition> createTestingViewCodec()
    {
        ObjectMapperProvider provider = new ObjectMapperProvider();
        provider.setJsonDeserializers(ImmutableMap.<Class<?>, JsonDeserializer<?>>of(Type.class, new TypeDeserializer(new TypeRegistry())));
        return new JsonCodecFactory(provider).jsonCodec(ViewDefinition.class);
    }
}

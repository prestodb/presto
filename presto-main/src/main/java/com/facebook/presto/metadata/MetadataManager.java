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

import com.facebook.presto.Session;
import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.connector.informationSchema.InformationSchemaMetadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.type.TypeDeserializer;
import com.facebook.presto.type.TypeRegistry;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.Slice;

import javax.inject.Inject;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;

import static com.facebook.presto.metadata.MetadataUtil.checkCatalogName;
import static com.facebook.presto.metadata.QualifiedTableName.convertFromSchemaTableName;
import static com.facebook.presto.metadata.TableLayout.fromConnectorLayout;
import static com.facebook.presto.metadata.ViewDefinition.ViewColumn;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_VIEW;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.StandardErrorCode.SYNTAX_ERROR;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;
import static java.lang.String.format;

public class MetadataManager
        implements Metadata
{
    private static final String INFORMATION_SCHEMA_NAME = "information_schema";

    private final ConcurrentMap<String, ConnectorMetadataEntry> informationSchemasByCatalog = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ConnectorMetadataEntry> systemTablesByCatalog = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ConnectorMetadataEntry> connectorsByCatalog = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ConnectorMetadata> connectorsById = new ConcurrentHashMap<>();
    private final FunctionRegistry functions;
    private final TypeManager typeManager;
    private final JsonCodec<ViewDefinition> viewCodec;
    private final SplitManager splitManager;
    private final BlockEncodingSerde blockEncodingSerde;

    public MetadataManager(FeaturesConfig featuresConfig, TypeManager typeManager, SplitManager splitManager, BlockEncodingSerde blockEncodingSerde)
    {
        this(featuresConfig, typeManager, createTestingViewCodec(), splitManager, blockEncodingSerde);
    }

    @Inject
    public MetadataManager(FeaturesConfig featuresConfig, TypeManager typeManager, JsonCodec<ViewDefinition> viewCodec, SplitManager splitManager, BlockEncodingSerde blockEncodingSerde)
    {
        functions = new FunctionRegistry(typeManager, blockEncodingSerde, featuresConfig.isExperimentalSyntaxEnabled());
        this.typeManager = checkNotNull(typeManager, "types is null");
        this.viewCodec = checkNotNull(viewCodec, "viewCodec is null");
        this.splitManager = checkNotNull(splitManager, "splitManager is null");
        this.blockEncodingSerde = checkNotNull(blockEncodingSerde, "blockEncodingSerde is null");
    }

    public static MetadataManager createTestMetadataManager()
    {
        FeaturesConfig featuresConfig = new FeaturesConfig();
        TypeManager typeManager = new TypeRegistry();
        SplitManager splitManager = new SplitManager();
        BlockEncodingSerde blockEncodingSerde = new BlockEncodingManager(typeManager);
        return new MetadataManager(featuresConfig, typeManager, splitManager, blockEncodingSerde);
    }

    public synchronized void addConnectorMetadata(String connectorId, String catalogName, ConnectorMetadata connectorMetadata)
    {
        checkMetadataArguments(connectorId, catalogName, connectorMetadata);
        checkArgument(!connectorsByCatalog.containsKey(catalogName), "Catalog '%s' is already registered", catalogName);

        connectorsById.put(connectorId, connectorMetadata);
        connectorsByCatalog.put(catalogName, new ConnectorMetadataEntry(connectorId, connectorMetadata));
    }

    public synchronized void addInformationSchemaMetadata(String connectorId, String catalogName, InformationSchemaMetadata metadata)
    {
        checkMetadataArguments(connectorId, catalogName, metadata);
        checkArgument(!informationSchemasByCatalog.containsKey(catalogName), "Information schema for catalog '%s' is already registered", catalogName);

        connectorsById.put(connectorId, metadata);
        informationSchemasByCatalog.put(catalogName, new ConnectorMetadataEntry(connectorId, metadata));
    }

    public synchronized void addSystemTablesMetadata(String connectorId, String catalogName, ConnectorMetadata metadata)
    {
        checkMetadataArguments(connectorId, catalogName, metadata);
        checkArgument(!systemTablesByCatalog.containsKey(catalogName), "System tables for catalog '%s' are already registered", catalogName);

        connectorsById.put(connectorId, metadata);
        systemTablesByCatalog.put(catalogName, new ConnectorMetadataEntry(connectorId, metadata));
    }

    private void checkMetadataArguments(String connectorId, String catalogName, ConnectorMetadata metadata)
    {
        checkNotNull(connectorId, "connectorId is null");
        checkNotNull(catalogName, "catalogName is null");
        checkNotNull(metadata, "metadata is null");
        checkArgument(!connectorsById.containsKey(connectorId), "Connector '%s' is already registered", connectorId);
    }

    @Override
    public Type getType(TypeSignature signature)
    {
        return typeManager.getType(signature);
    }

    @Override
    public FunctionInfo resolveFunction(QualifiedName name, List<TypeSignature> parameterTypes, boolean approximate)
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
    public List<ParametricFunction> listFunctions()
    {
        return functions.list();
    }

    @Override
    public void addFunctions(List<? extends ParametricFunction> functionInfos)
    {
        functions.addFunctions(functionInfos);
    }

    @Override
    public FunctionInfo resolveOperator(OperatorType operatorType, List<? extends Type> argumentTypes)
            throws OperatorNotFoundException
    {
        return functions.resolveOperator(operatorType, argumentTypes);
    }

    @Override
    public List<String> listSchemaNames(Session session, String catalogName)
    {
        checkCatalogName(catalogName);
        ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
        for (ConnectorMetadataEntry entry : allConnectorsFor(catalogName)) {
            schemaNames.addAll(entry.getMetadata().listSchemaNames(session.toConnectorSession(entry.getCatalog())));
        }
        return ImmutableList.copyOf(schemaNames.build());
    }

    @Override
    public Optional<TableHandle> getTableHandle(Session session, QualifiedTableName table)
    {
        checkNotNull(table, "table is null");

        ConnectorMetadataEntry entry = getConnectorFor(table);
        if (entry != null) {
            ConnectorMetadata metadata = entry.getMetadata();

            ConnectorTableHandle tableHandle = metadata.getTableHandle(session.toConnectorSession(entry.getCatalog()), table.asSchemaTableName());

            if (tableHandle != null) {
                return Optional.of(new TableHandle(entry.getConnectorId(), tableHandle));
            }
        }
        return Optional.empty();
    }

    @Override
    public List<TableLayoutResult> getLayouts(TableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        if (constraint.getSummary().isNone()) {
            return ImmutableList.of();
        }

        TupleDomain<ColumnHandle> summary = constraint.getSummary();
        String connectorId = table.getConnectorId();
        ConnectorTableHandle connectorTable = table.getConnectorHandle();
        Predicate<Map<ColumnHandle, ?>> predicate = constraint.predicate();

        List<ConnectorTableLayoutResult> layouts;
        try {
            ConnectorMetadata metadata = getConnectorMetadata(connectorId);
            layouts = metadata.getTableLayouts(connectorTable, new Constraint<>(summary, predicate::test), desiredColumns);
        }
        catch (UnsupportedOperationException e) {
            ConnectorSplitManager connectorSplitManager = splitManager.getConnectorSplitManager(connectorId);
            ConnectorPartitionResult result = connectorSplitManager.getPartitions(connectorTable, summary);

            List<ConnectorPartition> partitions = result.getPartitions().stream()
                    .filter(partition -> predicate.test(partition.getTupleDomain().extractFixedValues()))
                    .collect(toImmutableList());

            List<TupleDomain<ColumnHandle>> partitionDomains = partitions.stream()
                    .map(ConnectorPartition::getTupleDomain)
                    .collect(toImmutableList());

            TupleDomain<ColumnHandle> effectivePredicate = TupleDomain.none();
            if (!partitionDomains.isEmpty()) {
                effectivePredicate = TupleDomain.columnWiseUnion(partitionDomains);
            }

            ConnectorTableLayout layout = new ConnectorTableLayout(new LegacyTableLayoutHandle(connectorTable, partitions), Optional.empty(), effectivePredicate, Optional.empty(), Optional.of(partitionDomains), ImmutableList.of());
            layouts = ImmutableList.of(new ConnectorTableLayoutResult(layout, result.getUndeterminedTupleDomain()));
        }

        return layouts.stream()
                .map(entry -> new TableLayoutResult(fromConnectorLayout(connectorId, entry.getTableLayout()), entry.getUnenforcedConstraint()))
                .collect(toImmutableList());
    }

    public TableLayout getLayout(TableLayoutHandle handle)
    {
        if (handle.getConnectorHandle() instanceof LegacyTableLayoutHandle) {
            LegacyTableLayoutHandle legacyHandle = (LegacyTableLayoutHandle) handle.getConnectorHandle();
            List<TupleDomain<ColumnHandle>> partitionDomains = legacyHandle.getPartitions().stream()
                    .map(ConnectorPartition::getTupleDomain)
                    .collect(toImmutableList());

            TupleDomain<ColumnHandle> predicate = TupleDomain.none();
            if (!partitionDomains.isEmpty()) {
                predicate = TupleDomain.columnWiseUnion(partitionDomains);
            }
            return new TableLayout(handle, new ConnectorTableLayout(legacyHandle, Optional.empty(), predicate, Optional.empty(), Optional.of(partitionDomains), ImmutableList.of()));
        }

        String connectorId = handle.getConnectorId();
        ConnectorMetadata metadata = getConnectorMetadata(connectorId);
        return fromConnectorLayout(connectorId, metadata.getTableLayout(handle.getConnectorHandle()));
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
        return lookupConnectorFor(tableHandle).getColumnHandles(tableHandle.getConnectorHandle());
    }

    @Override
    public ColumnMetadata getColumnMetadata(TableHandle tableHandle, ColumnHandle columnHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkNotNull(columnHandle, "columnHandle is null");

        return lookupConnectorFor(tableHandle).getColumnMetadata(tableHandle.getConnectorHandle(), columnHandle);
    }

    @Override
    public List<QualifiedTableName> listTables(Session session, QualifiedTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");

        String schemaNameOrNull = prefix.getSchemaName().orElse(null);
        Set<QualifiedTableName> tables = new LinkedHashSet<>();
        for (ConnectorMetadataEntry entry : allConnectorsFor(prefix.getCatalogName())) {
            ConnectorSession connectorSession = session.toConnectorSession(entry.getCatalog());
            for (QualifiedTableName tableName : transform(entry.getMetadata().listTables(connectorSession, schemaNameOrNull), convertFromSchemaTableName(prefix.getCatalogName()))) {
                tables.add(tableName);
            }
        }
        return ImmutableList.copyOf(tables);
    }

    @Override
    public Optional<ColumnHandle> getSampleWeightColumnHandle(TableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        ColumnHandle handle = lookupConnectorFor(tableHandle).getSampleWeightColumnHandle(tableHandle.getConnectorHandle());

        return Optional.ofNullable(handle);
    }

    @Override
    public boolean canCreateSampledTables(Session session, String catalogName)
    {
        ConnectorMetadataEntry connectorMetadata = connectorsByCatalog.get(catalogName);
        checkArgument(connectorMetadata != null, "Catalog %s does not exist", catalogName);
        return connectorMetadata.getMetadata().canCreateSampledTables(session.toConnectorSession(connectorMetadata.getCatalog()));
    }

    @Override
    public Map<QualifiedTableName, List<ColumnMetadata>> listTableColumns(Session session, QualifiedTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");
        SchemaTablePrefix tablePrefix = prefix.asSchemaTablePrefix();

        Map<QualifiedTableName, List<ColumnMetadata>> tableColumns = new HashMap<>();
        for (ConnectorMetadataEntry connectorMetadata : allConnectorsFor(prefix.getCatalogName())) {
            ConnectorMetadata metadata = connectorMetadata.getMetadata();

            ConnectorSession connectorSession = session.toConnectorSession(connectorMetadata.getCatalog());
            for (Entry<SchemaTableName, List<ColumnMetadata>> entry : metadata.listTableColumns(connectorSession, tablePrefix).entrySet()) {
                QualifiedTableName tableName = new QualifiedTableName(
                        prefix.getCatalogName(),
                        entry.getKey().getSchemaName(),
                        entry.getKey().getTableName());
                tableColumns.put(tableName, entry.getValue());
            }

            // if table and view names overlap, the view wins
            for (Entry<SchemaTableName, String> entry : metadata.getViews(connectorSession, tablePrefix).entrySet()) {
                QualifiedTableName tableName = new QualifiedTableName(
                        prefix.getCatalogName(),
                        entry.getKey().getSchemaName(),
                        entry.getKey().getTableName());

                int ordinalPosition = 0;
                ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
                for (ViewColumn column : deserializeView(entry.getValue()).getColumns()) {
                    columns.add(new ColumnMetadata(column.getName(), column.getType(), ordinalPosition, false));
                    ordinalPosition++;
                }

                tableColumns.put(tableName, columns.build());
            }
        }
        return ImmutableMap.copyOf(tableColumns);
    }

    @Override
    public void createTable(Session session, String catalogName, TableMetadata tableMetadata)
    {
        ConnectorMetadataEntry connectorMetadata = connectorsByCatalog.get(catalogName);
        checkArgument(connectorMetadata != null, "Catalog %s does not exist", catalogName);

        connectorMetadata.getMetadata().createTable(session.toConnectorSession(connectorMetadata.getCatalog()), tableMetadata.getMetadata());
    }

    @Override
    public void renameTable(TableHandle tableHandle, QualifiedTableName newTableName)
    {
        String catalogName = newTableName.getCatalogName();
        ConnectorMetadataEntry target = connectorsByCatalog.get(catalogName);
        if (target == null) {
            throw new PrestoException(NOT_FOUND, format("Target catalog '%s' does not exist", catalogName));
        }
        if (!tableHandle.getConnectorId().equals(target.getConnectorId())) {
            throw new PrestoException(SYNTAX_ERROR, "Cannot rename tables across catalogs");
        }

        lookupConnectorFor(tableHandle).renameTable(tableHandle.getConnectorHandle(), newTableName.asSchemaTableName());
    }

    @Override
    public void renameColumn(TableHandle tableHandle, ColumnHandle source, String target)
    {
        lookupConnectorFor(tableHandle).renameColumn(tableHandle.getConnectorHandle(), source, target);
    }

    @Override
    public void dropTable(TableHandle tableHandle)
    {
        lookupConnectorFor(tableHandle).dropTable(tableHandle.getConnectorHandle());
    }

    @Override
    public OutputTableHandle beginCreateTable(Session session, String catalogName, TableMetadata tableMetadata)
    {
        ConnectorMetadataEntry connectorMetadata = connectorsByCatalog.get(catalogName);
        checkArgument(connectorMetadata != null, "Catalog %s does not exist", catalogName);
        ConnectorSession connectorSession = session.toConnectorSession(connectorMetadata.getCatalog());
        ConnectorOutputTableHandle handle = connectorMetadata.getMetadata().beginCreateTable(connectorSession, tableMetadata.getMetadata());
        return new OutputTableHandle(connectorMetadata.getConnectorId(), handle);
    }

    @Override
    public void commitCreateTable(OutputTableHandle tableHandle, Collection<Slice> fragments)
    {
        lookupConnectorFor(tableHandle).commitCreateTable(tableHandle.getConnectorHandle(), fragments);
    }

    @Override
    public void rollbackCreateTable(OutputTableHandle tableHandle)
    {
        lookupConnectorFor(tableHandle).rollbackCreateTable(tableHandle.getConnectorHandle());
    }

    @Override
    public InsertTableHandle beginInsert(Session session, TableHandle tableHandle)
    {
        // assume connectorId and catalog are the same
        ConnectorSession connectorSession = session.toConnectorSession(tableHandle.getConnectorId());
        ConnectorInsertTableHandle handle = lookupConnectorFor(tableHandle).beginInsert(connectorSession, tableHandle.getConnectorHandle());
        return new InsertTableHandle(tableHandle.getConnectorId(), handle);
    }

    @Override
    public void commitInsert(InsertTableHandle tableHandle, Collection<Slice> fragments)
    {
        lookupConnectorFor(tableHandle).commitInsert(tableHandle.getConnectorHandle(), fragments);
    }

    @Override
    public void rollbackInsert(InsertTableHandle tableHandle)
    {
        lookupConnectorFor(tableHandle).rollbackInsert(tableHandle.getConnectorHandle());
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
    public List<QualifiedTableName> listViews(Session session, QualifiedTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");

        String schemaNameOrNull = prefix.getSchemaName().orElse(null);
        Set<QualifiedTableName> views = new LinkedHashSet<>();
        for (ConnectorMetadataEntry entry : allConnectorsFor(prefix.getCatalogName())) {
            ConnectorSession connectorSession = session.toConnectorSession(entry.getCatalog());
            for (QualifiedTableName tableName : transform(entry.getMetadata().listViews(connectorSession, schemaNameOrNull), convertFromSchemaTableName(prefix.getCatalogName()))) {
                views.add(tableName);
            }
        }
        return ImmutableList.copyOf(views);
    }

    @Override
    public Map<QualifiedTableName, ViewDefinition> getViews(Session session, QualifiedTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");
        SchemaTablePrefix tablePrefix = prefix.asSchemaTablePrefix();

        Map<QualifiedTableName, ViewDefinition> views = new LinkedHashMap<>();
        for (ConnectorMetadataEntry metadata : allConnectorsFor(prefix.getCatalogName())) {
            ConnectorSession connectorSession = session.toConnectorSession(metadata.getCatalog());
            for (Entry<SchemaTableName, String> entry : metadata.getMetadata().getViews(connectorSession, tablePrefix).entrySet()) {
                QualifiedTableName viewName = new QualifiedTableName(
                        prefix.getCatalogName(),
                        entry.getKey().getSchemaName(),
                        entry.getKey().getTableName());
                views.put(viewName, deserializeView(entry.getValue()));
            }
        }
        return ImmutableMap.copyOf(views);
    }

    @Override
    public Optional<ViewDefinition> getView(Session session, QualifiedTableName viewName)
    {
        ConnectorMetadataEntry entry = getConnectorFor(viewName);
        if (entry != null) {
            SchemaTablePrefix prefix = viewName.asSchemaTableName().toSchemaTablePrefix();
            Map<SchemaTableName, String> views = entry.getMetadata().getViews(session.toConnectorSession(entry.getCatalog()), prefix);
            String view = views.get(viewName.asSchemaTableName());
            if (view != null) {
                return Optional.of(deserializeView(view));
            }
        }
        return Optional.empty();
    }

    @Override
    public void createView(Session session, QualifiedTableName viewName, String viewData, boolean replace)
    {
        ConnectorMetadataEntry connectorMetadata = connectorsByCatalog.get(viewName.getCatalogName());
        checkArgument(connectorMetadata != null, "Catalog %s does not exist", viewName.getCatalogName());
        connectorMetadata.getMetadata().createView(session.toConnectorSession(connectorMetadata.getCatalog()), viewName.asSchemaTableName(), viewData, replace);
    }

    @Override
    public void dropView(Session session, QualifiedTableName viewName)
    {
        ConnectorMetadataEntry connectorMetadata = connectorsByCatalog.get(viewName.getCatalogName());
        checkArgument(connectorMetadata != null, "Catalog %s does not exist", viewName.getCatalogName());
        connectorMetadata.getMetadata().dropView(session.toConnectorSession(connectorMetadata.getCatalog()), viewName.asSchemaTableName());
    }

    @Override
    public FunctionRegistry getFunctionRegistry()
    {
        return functions;
    }

    @Override
    public TypeManager getTypeManager()
    {
        return typeManager;
    }

    @Override
    public BlockEncodingSerde getBlockEncodingSerde()
    {
        return blockEncodingSerde;
    }

    private ViewDefinition deserializeView(String data)
    {
        try {
            return viewCodec.fromJson(data);
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_VIEW, "Invalid view JSON: " + data, e);
        }
    }

    private List<ConnectorMetadataEntry> allConnectorsFor(String catalogName)
    {
        ImmutableList.Builder<ConnectorMetadataEntry> builder = ImmutableList.builder();

        ConnectorMetadataEntry entry = informationSchemasByCatalog.get(catalogName);
        if (entry != null) {
            builder.add(entry);
        }

        ConnectorMetadataEntry systemTables = systemTablesByCatalog.get(catalogName);
        if (systemTables != null) {
            builder.add(systemTables);
        }

        ConnectorMetadataEntry connector = connectorsByCatalog.get(catalogName);
        if (connector != null) {
            builder.add(connector);
        }

        return builder.build();
    }

    private ConnectorMetadataEntry getConnectorFor(QualifiedTableName name)
    {
        String catalog = name.getCatalogName();
        String schema = name.getSchemaName();

        if (schema.equals(INFORMATION_SCHEMA_NAME)) {
            return informationSchemasByCatalog.get(catalog);
        }

        ConnectorMetadataEntry entry = systemTablesByCatalog.get(catalog);
        if ((entry != null) && (entry.getMetadata().getTableHandle(null, name.asSchemaTableName()) != null)) {
            return entry;
        }

        return connectorsByCatalog.get(catalog);
    }

    private ConnectorMetadata lookupConnectorFor(TableHandle tableHandle)
    {
        return getConnectorMetadata(tableHandle.getConnectorId());
    }

    private ConnectorMetadata lookupConnectorFor(OutputTableHandle tableHandle)
    {
        return getConnectorMetadata(tableHandle.getConnectorId());
    }

    private ConnectorMetadata lookupConnectorFor(InsertTableHandle tableHandle)
    {
        return getConnectorMetadata(tableHandle.getConnectorId());
    }

    private ConnectorMetadata getConnectorMetadata(String connectorId)
    {
        ConnectorMetadata result = connectorsById.get(connectorId);
        checkArgument(result != null, "No connector for connector ID: %s", connectorId);
        return result;
    }

    private static class ConnectorMetadataEntry
    {
        private final String connectorId;
        private final ConnectorMetadata metadata;

        private ConnectorMetadataEntry(String connectorId, ConnectorMetadata metadata)
        {
            this.connectorId = checkNotNull(connectorId, "connectorId is null");
            this.metadata = checkNotNull(metadata, "metadata is null");
        }

        private String getConnectorId()
        {
            return connectorId;
        }

        private String getCatalog()
        {
            // assume connectorId and catalog are the same
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

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
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorNewTableLayout;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorResolvedIndex;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.ConnectorViewDefinition;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.security.Privilege;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.transaction.TransactionId;
import com.facebook.presto.transaction.TransactionManager;
import com.facebook.presto.type.TypeDeserializer;
import com.facebook.presto.type.TypeRegistry;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.google.common.base.Joiner;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.Slice;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;

import static com.facebook.presto.metadata.MetadataUtil.checkCatalogName;
import static com.facebook.presto.metadata.OperatorType.BETWEEN;
import static com.facebook.presto.metadata.OperatorType.EQUAL;
import static com.facebook.presto.metadata.OperatorType.GREATER_THAN;
import static com.facebook.presto.metadata.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.metadata.OperatorType.HASH_CODE;
import static com.facebook.presto.metadata.OperatorType.LESS_THAN;
import static com.facebook.presto.metadata.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.metadata.OperatorType.NOT_EQUAL;
import static com.facebook.presto.metadata.QualifiedObjectName.convertFromSchemaTableName;
import static com.facebook.presto.metadata.TableLayout.fromConnectorLayout;
import static com.facebook.presto.metadata.ViewDefinition.ViewColumn;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_VIEW;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.StandardErrorCode.SYNTAX_ERROR;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.transaction.TransactionManager.createTestTransactionManager;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.transform;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class MetadataManager
        implements Metadata
{
    private static final String INFORMATION_SCHEMA_NAME = "information_schema";

    private final ConcurrentMap<String, ConnectorEntry> informationSchemasByCatalog = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ConnectorEntry> systemTablesByCatalog = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ConnectorEntry> connectorsByCatalog = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ConnectorEntry> connectorsById = new ConcurrentHashMap<>();
    private final FunctionRegistry functions;
    private final ProcedureRegistry procedures;
    private final TypeManager typeManager;
    private final JsonCodec<ViewDefinition> viewCodec;
    private final BlockEncodingSerde blockEncodingSerde;
    private final SessionPropertyManager sessionPropertyManager;
    private final TablePropertyManager tablePropertyManager;
    private final TransactionManager transactionManager;

    public MetadataManager(FeaturesConfig featuresConfig,
            TypeManager typeManager,
            BlockEncodingSerde blockEncodingSerde,
            SessionPropertyManager sessionPropertyManager,
            TablePropertyManager tablePropertyManager,
            TransactionManager transactionManager)
    {
        this(featuresConfig, typeManager, createTestingViewCodec(), blockEncodingSerde, sessionPropertyManager, tablePropertyManager, transactionManager);
    }

    @Inject
    public MetadataManager(FeaturesConfig featuresConfig,
            TypeManager typeManager,
            JsonCodec<ViewDefinition> viewCodec,
            BlockEncodingSerde blockEncodingSerde,
            SessionPropertyManager sessionPropertyManager,
            TablePropertyManager tablePropertyManager,
            TransactionManager transactionManager)
    {
        functions = new FunctionRegistry(typeManager, blockEncodingSerde, featuresConfig);
        procedures = new ProcedureRegistry();
        this.typeManager = requireNonNull(typeManager, "types is null");
        this.viewCodec = requireNonNull(viewCodec, "viewCodec is null");
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.tablePropertyManager = requireNonNull(tablePropertyManager, "tablePropertyManager is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");

        verifyComparableOrderableContract();
    }

    public static MetadataManager createTestMetadataManager()
    {
        FeaturesConfig featuresConfig = new FeaturesConfig();
        TypeManager typeManager = new TypeRegistry();
        SessionPropertyManager sessionPropertyManager = new SessionPropertyManager();
        BlockEncodingSerde blockEncodingSerde = new BlockEncodingManager(typeManager);
        TransactionManager transactionManager = createTestTransactionManager();
        return new MetadataManager(featuresConfig, typeManager, blockEncodingSerde, sessionPropertyManager, new TablePropertyManager(), transactionManager);
    }

    public synchronized void registerConnectorCatalog(String connectorId, String catalogName)
    {
        checkMetadataArguments(connectorId, catalogName);
        checkArgument(!connectorsByCatalog.containsKey(catalogName), "Catalog '%s' is already registered", catalogName);

        ConnectorEntry connectorEntry = new ConnectorEntry(connectorId, catalogName);
        connectorsById.put(connectorId, connectorEntry);
        connectorsByCatalog.put(catalogName, connectorEntry);
    }

    public synchronized void registerInformationSchemaCatalog(String connectorId, String catalogName)
    {
        checkMetadataArguments(connectorId, catalogName);
        checkArgument(!informationSchemasByCatalog.containsKey(catalogName), "Information schema for catalog '%s' is already registered", catalogName);

        ConnectorEntry connectorEntry = new ConnectorEntry(connectorId, catalogName);
        connectorsById.put(connectorId, connectorEntry);
        informationSchemasByCatalog.put(catalogName, connectorEntry);
    }

    public synchronized void registerSystemTablesCatalog(String connectorId, String catalogName)
    {
        checkMetadataArguments(connectorId, catalogName);
        checkArgument(!systemTablesByCatalog.containsKey(catalogName), "System tables for catalog '%s' are already registered", catalogName);

        ConnectorEntry connectorEntry = new ConnectorEntry(connectorId, catalogName);
        connectorsById.put(connectorId, connectorEntry);
        systemTablesByCatalog.put(catalogName, connectorEntry);
    }

    private void checkMetadataArguments(String connectorId, String catalogName)
    {
        requireNonNull(connectorId, "connectorId is null");
        requireNonNull(catalogName, "catalogName is null");
        checkArgument(!connectorsById.containsKey(connectorId), "Connector '%s' is already registered", connectorId);
    }

    @Override
    public final void verifyComparableOrderableContract()
    {
        Multimap<Type, OperatorType> missingOperators = HashMultimap.create();
        for (Type type : typeManager.getTypes()) {
            if (type.isComparable()) {
                if (!functions.canResolveOperator(HASH_CODE, BIGINT, ImmutableList.of(type))) {
                    missingOperators.put(type, HASH_CODE);
                }
                if (!functions.canResolveOperator(EQUAL, BOOLEAN, ImmutableList.of(type, type))) {
                    missingOperators.put(type, EQUAL);
                }
                if (!functions.canResolveOperator(NOT_EQUAL, BOOLEAN, ImmutableList.of(type, type))) {
                    missingOperators.put(type, NOT_EQUAL);
                }
            }
            if (type.isOrderable()) {
                for (OperatorType operator : ImmutableList.of(LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL)) {
                    if (!functions.canResolveOperator(operator, BOOLEAN, ImmutableList.of(type, type))) {
                        missingOperators.put(type, operator);
                    }
                }
                if (!functions.canResolveOperator(BETWEEN, BOOLEAN, ImmutableList.of(type, type, type))) {
                    missingOperators.put(type, BETWEEN);
                }
            }
        }
        // TODO: verify the parametric types too
        if (!missingOperators.isEmpty()) {
            List<String> messages = new ArrayList<>();
            for (Type type : missingOperators.keySet()) {
                messages.add(format("%s missing for %s", missingOperators.get(type), type));
            }
            throw new IllegalStateException(Joiner.on(", ").join(messages));
        }
    }

    @Override
    public Type getType(TypeSignature signature)
    {
        return typeManager.getType(signature);
    }

    @Override
    public boolean isAggregationFunction(QualifiedName name)
    {
        // TODO: transactional when FunctionRegistry is made transactional
        return functions.isAggregationFunction(name);
    }

    @Override
    public List<SqlFunction> listFunctions()
    {
        // TODO: transactional when FunctionRegistry is made transactional
        return functions.list();
    }

    @Override
    public void addFunctions(List<? extends SqlFunction> functionInfos)
    {
        // TODO: transactional when FunctionRegistry is made transactional
        functions.addFunctions(functionInfos);
    }

    @Override
    public List<String> listSchemaNames(Session session, String catalogName)
    {
        checkCatalogName(catalogName);
        ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
        for (ConnectorEntry entry : allConnectorsFor(catalogName)) {
            ConnectorMetadata metadata = entry.getMetadata(session);
            schemaNames.addAll(metadata.listSchemaNames(session.toConnectorSession(entry.getCatalog())));
        }
        return ImmutableList.copyOf(schemaNames.build());
    }

    @Override
    public Optional<TableHandle> getTableHandle(Session session, QualifiedObjectName table)
    {
        requireNonNull(table, "table is null");

        ConnectorEntry entry = getConnectorFor(session, table);
        if (entry != null) {
            ConnectorMetadata metadata = entry.getMetadata(session);

            ConnectorTableHandle tableHandle = metadata.getTableHandle(session.toConnectorSession(entry.getCatalog()), table.asSchemaTableName());

            if (tableHandle != null) {
                return Optional.of(new TableHandle(entry.getConnectorId(), tableHandle));
            }
        }
        return Optional.empty();
    }

    @Override
    public List<TableLayoutResult> getLayouts(Session session, TableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        if (constraint.getSummary().isNone()) {
            return ImmutableList.of();
        }

        TupleDomain<ColumnHandle> summary = constraint.getSummary();
        String connectorId = table.getConnectorId();
        ConnectorTableHandle connectorTable = table.getConnectorHandle();
        Predicate<Map<ColumnHandle, NullableValue>> predicate = constraint.predicate();

        ConnectorEntry entry = getConnectorMetadata(connectorId);
        ConnectorMetadata metadata = entry.getMetadata(session);
        ConnectorTransactionHandle transaction = entry.getTransactionHandle(session);
        ConnectorSession connectorSession = session.toConnectorSession(entry.getCatalog());
        List<ConnectorTableLayoutResult> layouts = metadata.getTableLayouts(connectorSession, connectorTable, new Constraint<>(summary, predicate::test), desiredColumns);

        return layouts.stream()
                .map(layout -> new TableLayoutResult(fromConnectorLayout(connectorId, transaction, layout.getTableLayout()), layout.getUnenforcedConstraint()))
                .collect(toImmutableList());
    }

    @Override
    public TableLayout getLayout(Session session, TableLayoutHandle handle)
    {
        String connectorId = handle.getConnectorId();
        ConnectorEntry entry = getConnectorMetadata(connectorId);
        ConnectorMetadata metadata = entry.getMetadata(session);
        ConnectorTransactionHandle transaction = entry.getTransactionHandle(session);
        return fromConnectorLayout(connectorId, transaction, metadata.getTableLayout(session.toConnectorSession(entry.getCatalog()), handle.getConnectorHandle()));
    }

    @Override
    public TableMetadata getTableMetadata(Session session, TableHandle tableHandle)
    {
        ConnectorEntry entry = lookupConnectorFor(tableHandle);
        ConnectorMetadata metadata = entry.getMetadata(session);
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(session.toConnectorSession(entry.getCatalog()), tableHandle.getConnectorHandle());

        return new TableMetadata(tableHandle.getConnectorId(), tableMetadata);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(Session session, TableHandle tableHandle)
    {
        ConnectorEntry entry = lookupConnectorFor(tableHandle);
        ConnectorMetadata metadata = entry.getMetadata(session);
        Map<String, ColumnHandle> handles = metadata.getColumnHandles(session.toConnectorSession(entry.getCatalog()), tableHandle.getConnectorHandle());

        ImmutableMap.Builder<String, ColumnHandle> map = ImmutableMap.builder();
        for (Entry<String, ColumnHandle> mapEntry : handles.entrySet()) {
            map.put(mapEntry.getKey().toLowerCase(ENGLISH), mapEntry.getValue());
        }
        return map.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(Session session, TableHandle tableHandle, ColumnHandle columnHandle)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        requireNonNull(columnHandle, "columnHandle is null");

        ConnectorEntry entry = lookupConnectorFor(tableHandle);
        ConnectorMetadata metadata = entry.getMetadata(session);
        return metadata.getColumnMetadata(session.toConnectorSession(entry.getCatalog()), tableHandle.getConnectorHandle(), columnHandle);
    }

    @Override
    public List<QualifiedObjectName> listTables(Session session, QualifiedTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");

        String schemaNameOrNull = prefix.getSchemaName().orElse(null);
        Set<QualifiedObjectName> tables = new LinkedHashSet<>();
        for (ConnectorEntry entry : allConnectorsFor(prefix.getCatalogName())) {
            ConnectorMetadata metadata = entry.getMetadata(session);
            ConnectorSession connectorSession = session.toConnectorSession(entry.getCatalog());
            for (QualifiedObjectName tableName : transform(metadata.listTables(connectorSession, schemaNameOrNull), convertFromSchemaTableName(prefix.getCatalogName()))) {
                tables.add(tableName);
            }
        }
        return ImmutableList.copyOf(tables);
    }

    @Override
    public Optional<ColumnHandle> getSampleWeightColumnHandle(Session session, TableHandle tableHandle)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        ConnectorEntry entry = lookupConnectorFor(tableHandle);
        ConnectorMetadata metadata = entry.getMetadata(session);
        ColumnHandle handle = metadata.getSampleWeightColumnHandle(session.toConnectorSession(entry.getCatalog()), tableHandle.getConnectorHandle());

        return Optional.ofNullable(handle);
    }

    @Override
    public boolean canCreateSampledTables(Session session, String catalogName)
    {
        ConnectorEntry entry = connectorsByCatalog.get(catalogName);
        checkArgument(entry != null, "Catalog %s does not exist", catalogName);
        ConnectorMetadata metadata = entry.getMetadata(session);
        return metadata.canCreateSampledTables(session.toConnectorSession(entry.getCatalog()));
    }

    @Override
    public Map<QualifiedObjectName, List<ColumnMetadata>> listTableColumns(Session session, QualifiedTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        SchemaTablePrefix tablePrefix = prefix.asSchemaTablePrefix();

        Map<QualifiedObjectName, List<ColumnMetadata>> tableColumns = new HashMap<>();
        for (ConnectorEntry connectorEntry : allConnectorsFor(prefix.getCatalogName())) {
            ConnectorMetadata metadata = connectorEntry.getMetadata(session);

            ConnectorSession connectorSession = session.toConnectorSession(connectorEntry.getCatalog());
            for (Entry<SchemaTableName, List<ColumnMetadata>> entry : metadata.listTableColumns(connectorSession, tablePrefix).entrySet()) {
                QualifiedObjectName tableName = new QualifiedObjectName(
                        prefix.getCatalogName(),
                        entry.getKey().getSchemaName(),
                        entry.getKey().getTableName());
                tableColumns.put(tableName, entry.getValue());
            }

            // if table and view names overlap, the view wins
            for (Entry<SchemaTableName, ConnectorViewDefinition> entry : metadata.getViews(connectorSession, tablePrefix).entrySet()) {
                QualifiedObjectName tableName = new QualifiedObjectName(
                        prefix.getCatalogName(),
                        entry.getKey().getSchemaName(),
                        entry.getKey().getTableName());

                ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
                for (ViewColumn column : deserializeView(entry.getValue().getViewData()).getColumns()) {
                    columns.add(new ColumnMetadata(column.getName(), column.getType()));
                }

                tableColumns.put(tableName, columns.build());
            }
        }
        return ImmutableMap.copyOf(tableColumns);
    }

    @Override
    public void createTable(Session session, String catalogName, TableMetadata tableMetadata)
    {
        ConnectorEntry entry = connectorsByCatalog.get(catalogName);
        checkArgument(entry != null, "Catalog %s does not exist", catalogName);
        ConnectorMetadata metadata = entry.getMetadataForWrite(session);
        metadata.createTable(session.toConnectorSession(entry.getCatalog()), tableMetadata.getMetadata());
    }

    @Override
    public void renameTable(Session session, TableHandle tableHandle, QualifiedObjectName newTableName)
    {
        String catalogName = newTableName.getCatalogName();
        ConnectorEntry target = connectorsByCatalog.get(catalogName);
        if (target == null) {
            throw new PrestoException(NOT_FOUND, format("Target catalog '%s' does not exist", catalogName));
        }
        if (!tableHandle.getConnectorId().equals(target.getConnectorId())) {
            throw new PrestoException(SYNTAX_ERROR, "Cannot rename tables across catalogs");
        }

        ConnectorEntry entry = lookupConnectorFor(tableHandle);
        ConnectorMetadata metadata = entry.getMetadataForWrite(session);
        metadata.renameTable(session.toConnectorSession(entry.getCatalog()), tableHandle.getConnectorHandle(), newTableName.asSchemaTableName());
    }

    @Override
    public void renameColumn(Session session, TableHandle tableHandle, ColumnHandle source, String target)
    {
        ConnectorEntry entry = lookupConnectorFor(tableHandle);
        ConnectorMetadata metadata = entry.getMetadataForWrite(session);
        metadata.renameColumn(session.toConnectorSession(entry.getCatalog()), tableHandle.getConnectorHandle(), source, target.toLowerCase(ENGLISH));
    }

    @Override
    public void addColumn(Session session, TableHandle tableHandle, ColumnMetadata column)
    {
        ConnectorEntry entry = lookupConnectorFor(tableHandle);
        ConnectorMetadata metadata = entry.getMetadataForWrite(session);
        metadata.addColumn(session.toConnectorSession(entry.getCatalog()), tableHandle.getConnectorHandle(), column);
    }

    @Override
    public void dropTable(Session session, TableHandle tableHandle)
    {
        ConnectorEntry entry = lookupConnectorFor(tableHandle);
        ConnectorMetadata metadata = entry.getMetadataForWrite(session);
        metadata.dropTable(session.toConnectorSession(entry.getCatalog()), tableHandle.getConnectorHandle());
    }

    @Override
    public Optional<NewTableLayout> getInsertLayout(Session session, TableHandle table)
    {
        ConnectorEntry entry = getConnectorMetadata(table.getConnectorId());

        Optional<ConnectorNewTableLayout> insertLayout = entry.getMetadataForWrite(session).getInsertLayout(session.toConnectorSession(entry.getCatalog()), table.getConnectorHandle());
        if (!insertLayout.isPresent()) {
            return Optional.empty();
        }

        return Optional.of(new NewTableLayout(entry.getConnectorId(), entry.getTransactionHandle(session), insertLayout.get()));
    }

    @Override
    public Optional<NewTableLayout> getNewTableLayout(Session session, String catalogName, TableMetadata tableMetadata)
    {
        ConnectorEntry entry = connectorsByCatalog.get(catalogName);
        checkArgument(entry != null, "Catalog %s does not exist", catalogName);
        ConnectorMetadata metadata = entry.getMetadataForWrite(session);
        ConnectorTransactionHandle transactionHandle = entry.getTransactionHandle(session);
        ConnectorSession connectorSession = session.toConnectorSession(entry.getCatalog());
        return metadata.getNewTableLayout(connectorSession, tableMetadata.getMetadata())
                .map(layout -> new NewTableLayout(entry.getConnectorId(), transactionHandle, layout));
    }

    @Override
    public OutputTableHandle beginCreateTable(Session session, String catalogName, TableMetadata tableMetadata, Optional<NewTableLayout> layout)
    {
        ConnectorEntry entry = connectorsByCatalog.get(catalogName);
        checkArgument(entry != null, "Catalog %s does not exist", catalogName);
        ConnectorMetadata metadata = entry.getMetadataForWrite(session);
        ConnectorTransactionHandle transactionHandle = entry.getTransactionHandle(session);
        ConnectorSession connectorSession = session.toConnectorSession(entry.getCatalog());
        ConnectorOutputTableHandle handle = metadata.beginCreateTable(connectorSession, tableMetadata.getMetadata(), layout.map(NewTableLayout::getLayout));
        return new OutputTableHandle(entry.getConnectorId(), transactionHandle, handle);
    }

    @Override
    public void finishCreateTable(Session session, OutputTableHandle tableHandle, Collection<Slice> fragments)
    {
        ConnectorEntry entry = lookupConnectorFor(tableHandle);
        ConnectorMetadata metadata = entry.getMetadata(session);
        metadata.finishCreateTable(session.toConnectorSession(entry.getCatalog()), tableHandle.getConnectorHandle(), fragments);
    }

    @Override
    public InsertTableHandle beginInsert(Session session, TableHandle tableHandle)
    {
        ConnectorEntry entry = lookupConnectorFor(tableHandle);
        ConnectorMetadata metadata = entry.getMetadataForWrite(session);
        ConnectorTransactionHandle transactionHandle = entry.getTransactionHandle(session);
        ConnectorInsertTableHandle handle = metadata.beginInsert(session.toConnectorSession(entry.getCatalog()), tableHandle.getConnectorHandle());
        return new InsertTableHandle(tableHandle.getConnectorId(), transactionHandle, handle);
    }

    @Override
    public void finishInsert(Session session, InsertTableHandle tableHandle, Collection<Slice> fragments)
    {
        ConnectorEntry entry = lookupConnectorFor(tableHandle);
        ConnectorMetadata metadata = entry.getMetadata(session);
        metadata.finishInsert(session.toConnectorSession(entry.getCatalog()), tableHandle.getConnectorHandle(), fragments);
    }

    @Override
    public ColumnHandle getUpdateRowIdColumnHandle(Session session, TableHandle tableHandle)
    {
        ConnectorEntry entry = lookupConnectorFor(tableHandle);
        ConnectorMetadata metadata = entry.getMetadata(session);
        return metadata.getUpdateRowIdColumnHandle(session.toConnectorSession(entry.getCatalog()), tableHandle.getConnectorHandle());
    }

    @Override
    public boolean supportsMetadataDelete(Session session, TableHandle tableHandle, TableLayoutHandle tableLayoutHandle)
    {
        ConnectorEntry entry = lookupConnectorFor(tableHandle);
        ConnectorMetadata metadata = entry.getMetadata(session);
        return metadata.supportsMetadataDelete(
                session.toConnectorSession(entry.getCatalog()),
                tableHandle.getConnectorHandle(),
                tableLayoutHandle.getConnectorHandle());
    }

    @Override
    public OptionalLong metadataDelete(Session session, TableHandle tableHandle, TableLayoutHandle tableLayoutHandle)
    {
        ConnectorEntry entry = lookupConnectorFor(tableHandle);
        ConnectorMetadata metadata = entry.getMetadataForWrite(session);
        return metadata.metadataDelete(session.toConnectorSession(entry.getCatalog()), tableHandle.getConnectorHandle(), tableLayoutHandle.getConnectorHandle());
    }

    @Override
    public TableHandle beginDelete(Session session, TableHandle tableHandle)
    {
        ConnectorEntry entry = lookupConnectorFor(tableHandle);
        ConnectorMetadata metadata = entry.getMetadataForWrite(session);
        ConnectorTableHandle newHandle = metadata.beginDelete(session.toConnectorSession(entry.getCatalog()), tableHandle.getConnectorHandle());
        return new TableHandle(tableHandle.getConnectorId(), newHandle);
    }

    @Override
    public void finishDelete(Session session, TableHandle tableHandle, Collection<Slice> fragments)
    {
        ConnectorEntry entry = lookupConnectorFor(tableHandle);
        ConnectorMetadata metadata = entry.getMetadata(session);
        metadata.finishDelete(session.toConnectorSession(entry.getCatalog()), tableHandle.getConnectorHandle(), fragments);
    }

    @Override
    public Map<String, String> getCatalogNames()
    {
        ImmutableMap.Builder<String, String> catalogsMap = ImmutableMap.builder();
        for (Map.Entry<String, ConnectorEntry> entry : connectorsByCatalog.entrySet()) {
            catalogsMap.put(entry.getKey(), entry.getValue().getConnectorId());
        }
        return catalogsMap.build();
    }

    @Override
    public List<QualifiedObjectName> listViews(Session session, QualifiedTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");

        String schemaNameOrNull = prefix.getSchemaName().orElse(null);
        Set<QualifiedObjectName> views = new LinkedHashSet<>();
        for (ConnectorEntry entry : allConnectorsFor(prefix.getCatalogName())) {
            ConnectorMetadata metadata = entry.getMetadata(session);
            ConnectorSession connectorSession = session.toConnectorSession(entry.getCatalog());
            for (QualifiedObjectName tableName : transform(metadata.listViews(connectorSession, schemaNameOrNull), convertFromSchemaTableName(prefix.getCatalogName()))) {
                views.add(tableName);
            }
        }
        return ImmutableList.copyOf(views);
    }

    @Override
    public Map<QualifiedObjectName, ViewDefinition> getViews(Session session, QualifiedTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        SchemaTablePrefix tablePrefix = prefix.asSchemaTablePrefix();

        Map<QualifiedObjectName, ViewDefinition> views = new LinkedHashMap<>();
        for (ConnectorEntry connectorEntry : allConnectorsFor(prefix.getCatalogName())) {
            ConnectorMetadata metadata = connectorEntry.getMetadata(session);
            ConnectorSession connectorSession = session.toConnectorSession(connectorEntry.getCatalog());
            for (Entry<SchemaTableName, ConnectorViewDefinition> entry : metadata.getViews(connectorSession, tablePrefix).entrySet()) {
                QualifiedObjectName viewName = new QualifiedObjectName(
                        prefix.getCatalogName(),
                        entry.getKey().getSchemaName(),
                        entry.getKey().getTableName());
                views.put(viewName, deserializeView(entry.getValue().getViewData()));
            }
        }
        return ImmutableMap.copyOf(views);
    }

    @Override
    public Optional<ViewDefinition> getView(Session session, QualifiedObjectName viewName)
    {
        ConnectorEntry entry = getConnectorFor(session, viewName);
        if (entry != null) {
            ConnectorMetadata metadata = entry.getMetadata(session);
            Map<SchemaTableName, ConnectorViewDefinition> views = metadata.getViews(
                    session.toConnectorSession(entry.getCatalog()),
                    viewName.asSchemaTableName().toSchemaTablePrefix());
            ConnectorViewDefinition view = views.get(viewName.asSchemaTableName());
            if (view != null) {
                return Optional.of(deserializeView(view.getViewData()));
            }
        }
        return Optional.empty();
    }

    @Override
    public void createView(Session session, QualifiedObjectName viewName, String viewData, boolean replace)
    {
        ConnectorEntry entry = connectorsByCatalog.get(viewName.getCatalogName());
        checkArgument(entry != null, "Catalog %s does not exist", viewName.getCatalogName());
        ConnectorMetadata metadata = entry.getMetadataForWrite(session);
        metadata.createView(session.toConnectorSession(entry.getCatalog()), viewName.asSchemaTableName(), viewData, replace);
    }

    @Override
    public void dropView(Session session, QualifiedObjectName viewName)
    {
        ConnectorEntry entry = connectorsByCatalog.get(viewName.getCatalogName());
        checkArgument(entry != null, "Catalog %s does not exist", viewName.getCatalogName());
        ConnectorMetadata metadata = entry.getMetadataForWrite(session);
        metadata.dropView(session.toConnectorSession(entry.getCatalog()), viewName.asSchemaTableName());
    }

    @Override
    public Optional<ResolvedIndex> resolveIndex(Session session, TableHandle tableHandle, Set<ColumnHandle> indexableColumns, Set<ColumnHandle> outputColumns, TupleDomain<ColumnHandle> tupleDomain)
    {
        ConnectorEntry entry = lookupConnectorFor(tableHandle);
        ConnectorMetadata metadata = entry.getMetadata(session);
        ConnectorTransactionHandle transaction = entry.getTransactionHandle(session);
        ConnectorSession connectorSession = session.toConnectorSession(entry.getCatalog());
        Optional<ConnectorResolvedIndex> resolvedIndex = metadata.resolveIndex(connectorSession, tableHandle.getConnectorHandle(), indexableColumns, outputColumns, tupleDomain);
        return resolvedIndex.map(resolved -> new ResolvedIndex(tableHandle.getConnectorId(), transaction, resolved));
    }

    @Override
    public void grantTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, String grantee, boolean grantOption)
    {
        ConnectorEntry entry = connectorsByCatalog.get(tableName.getCatalogName());
        checkArgument(entry != null, "Catalog %s does not exist", tableName.getCatalogName());
        ConnectorMetadata metadata = entry.getMetadata(session);
        metadata.grantTablePrivileges(session.toConnectorSession(entry.getCatalog()), tableName.asSchemaTableName(), privileges, grantee, grantOption);
    }

    @Override
    public void revokeTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, String grantee, boolean grantOption)
    {
        ConnectorEntry entry = connectorsByCatalog.get(tableName.getCatalogName());
        checkArgument(entry != null, "Catalog %s does not exist", tableName.getCatalogName());
        ConnectorMetadata metadata = entry.getMetadata(session);
        metadata.revokeTablePrivileges(session.toConnectorSession(entry.getCatalog()), tableName.asSchemaTableName(), privileges, grantee, grantOption);
    }

    @Override
    public FunctionRegistry getFunctionRegistry()
    {
        // TODO: transactional when FunctionRegistry is made transactional
        return functions;
    }

    @Override
    public ProcedureRegistry getProcedureRegistry()
    {
        return procedures;
    }

    @Override
    public TypeManager getTypeManager()
    {
        // TODO: make this transactional when we allow user defined types
        return typeManager;
    }

    @Override
    public BlockEncodingSerde getBlockEncodingSerde()
    {
        return blockEncodingSerde;
    }

    @Override
    public SessionPropertyManager getSessionPropertyManager()
    {
        return sessionPropertyManager;
    }

    @Override
    public TablePropertyManager getTablePropertyManager()
    {
        return tablePropertyManager;
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

    private List<ConnectorEntry> allConnectorsFor(String catalogName)
    {
        ImmutableList.Builder<ConnectorEntry> builder = ImmutableList.builder();

        ConnectorEntry entry = informationSchemasByCatalog.get(catalogName);
        if (entry != null) {
            builder.add(entry);
        }

        ConnectorEntry systemTables = systemTablesByCatalog.get(catalogName);
        if (systemTables != null) {
            builder.add(systemTables);
        }

        ConnectorEntry connector = connectorsByCatalog.get(catalogName);
        if (connector != null) {
            builder.add(connector);
        }

        return builder.build();
    }

    private ConnectorEntry getConnectorFor(Session session, QualifiedObjectName name)
    {
        String catalog = name.getCatalogName();
        String schema = name.getSchemaName();

        if (schema.equals(INFORMATION_SCHEMA_NAME)) {
            return informationSchemasByCatalog.get(catalog);
        }

        ConnectorEntry entry = systemTablesByCatalog.get(catalog);
        if ((entry != null) && (entry.getMetadata(session).getTableHandle(null, name.asSchemaTableName()) != null)) {
            return entry;
        }

        return connectorsByCatalog.get(catalog);
    }

    private ConnectorEntry lookupConnectorFor(TableHandle tableHandle)
    {
        return getConnectorMetadata(tableHandle.getConnectorId());
    }

    private ConnectorEntry lookupConnectorFor(OutputTableHandle tableHandle)
    {
        return getConnectorMetadata(tableHandle.getConnectorId());
    }

    private ConnectorEntry lookupConnectorFor(InsertTableHandle tableHandle)
    {
        return getConnectorMetadata(tableHandle.getConnectorId());
    }

    private ConnectorEntry getConnectorMetadata(String connectorId)
    {
        ConnectorEntry result = connectorsById.get(connectorId);
        checkArgument(result != null, "No connector for connector ID: %s", connectorId);
        return result;
    }

    private class ConnectorEntry
    {
        private final String connectorId;
        private final String catalog;

        private ConnectorEntry(String connectorId, String catalog)
        {
            this.connectorId = requireNonNull(connectorId, "connectorId is null");
            this.catalog = requireNonNull(catalog, "catalog is null");
        }

        private String getConnectorId()
        {
            return connectorId;
        }

        private String getCatalog()
        {
            return catalog;
        }

        public ConnectorMetadata getMetadata(Session session)
        {
            return transactionManager.getMetadata(session.getRequiredTransactionId(), connectorId);
        }

        public ConnectorMetadata getMetadataForWrite(Session session)
        {
            TransactionId transactionId = session.getRequiredTransactionId();
            ConnectorMetadata metadata = transactionManager.getMetadata(transactionId, connectorId);
            transactionManager.checkConnectorWrite(transactionId, connectorId);
            return metadata;
        }

        public ConnectorTransactionHandle getTransactionHandle(Session session)
        {
            return transactionManager.getConnectorTransaction(session.getRequiredTransactionId(), connectorId);
        }
    }

    private static JsonCodec<ViewDefinition> createTestingViewCodec()
    {
        ObjectMapperProvider provider = new ObjectMapperProvider();
        provider.setJsonDeserializers(ImmutableMap.<Class<?>, JsonDeserializer<?>>of(Type.class, new TypeDeserializer(new TypeRegistry())));
        return new JsonCodecFactory(provider).jsonCodec(ViewDefinition.class);
    }
}

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
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.spi.CatalogSchemaName;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnIdentity;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorNewTableLayout;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorResolvedIndex;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.ConnectorViewDefinition;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableIdentity;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.security.Privilege;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.transaction.TransactionManager;
import com.facebook.presto.type.TypeDeserializer;
import com.facebook.presto.type.TypeRegistry;
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

import static com.facebook.presto.metadata.QualifiedObjectName.convertFromSchemaTableName;
import static com.facebook.presto.metadata.TableLayout.fromConnectorLayout;
import static com.facebook.presto.metadata.ViewDefinition.ViewColumn;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_VIEW;
import static com.facebook.presto.spi.StandardErrorCode.SYNTAX_ERROR;
import static com.facebook.presto.spi.function.OperatorType.BETWEEN;
import static com.facebook.presto.spi.function.OperatorType.EQUAL;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.HASH_CODE;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.NOT_EQUAL;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.transaction.TransactionManager.createTestTransactionManager;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class MetadataManager
        implements Metadata
{
    private final FunctionRegistry functions;
    private final ProcedureRegistry procedures;
    private final TypeManager typeManager;
    private final JsonCodec<ViewDefinition> viewCodec;
    private final BlockEncodingSerde blockEncodingSerde;
    private final SessionPropertyManager sessionPropertyManager;
    private final SchemaPropertyManager schemaPropertyManager;
    private final TablePropertyManager tablePropertyManager;
    private final TransactionManager transactionManager;

    public MetadataManager(FeaturesConfig featuresConfig,
            TypeManager typeManager,
            BlockEncodingSerde blockEncodingSerde,
            SessionPropertyManager sessionPropertyManager,
            SchemaPropertyManager schemaPropertyManager,
            TablePropertyManager tablePropertyManager,
            TransactionManager transactionManager)
    {
        this(featuresConfig,
                typeManager,
                createTestingViewCodec(),
                blockEncodingSerde,
                sessionPropertyManager,
                schemaPropertyManager,
                tablePropertyManager,
                transactionManager);
    }

    @Inject
    public MetadataManager(FeaturesConfig featuresConfig,
            TypeManager typeManager,
            JsonCodec<ViewDefinition> viewCodec,
            BlockEncodingSerde blockEncodingSerde,
            SessionPropertyManager sessionPropertyManager,
            SchemaPropertyManager schemaPropertyManager,
            TablePropertyManager tablePropertyManager,
            TransactionManager transactionManager)
    {
        functions = new FunctionRegistry(typeManager, blockEncodingSerde, featuresConfig);
        procedures = new ProcedureRegistry();
        this.typeManager = requireNonNull(typeManager, "types is null");
        this.viewCodec = requireNonNull(viewCodec, "viewCodec is null");
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.schemaPropertyManager = requireNonNull(schemaPropertyManager, "schemaPropertyManager is null");
        this.tablePropertyManager = requireNonNull(tablePropertyManager, "tablePropertyManager is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");

        verifyComparableOrderableContract();
    }

    public static MetadataManager createTestMetadataManager()
    {
        return createTestMetadataManager(new CatalogManager());
    }

    public static MetadataManager createTestMetadataManager(CatalogManager catalogManager)
    {
        TypeManager typeManager = new TypeRegistry();
        return new MetadataManager(
                new FeaturesConfig(),
                typeManager,
                new BlockEncodingManager(typeManager),
                new SessionPropertyManager(),
                new SchemaPropertyManager(),
                new TablePropertyManager(),
                createTestTransactionManager(catalogManager));
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
    public boolean schemaExists(Session session, CatalogSchemaName schema)
    {
        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, schema.getCatalogName());
        if (!catalog.isPresent()) {
            return false;
        }
        CatalogMetadata catalogMetadata = catalog.get();
        ConnectorSession connectorSession = session.toConnectorSession(catalogMetadata.getConnectorId());
        return catalogMetadata.listConnectorIds().stream()
                .map(catalogMetadata::getMetadataFor)
                .anyMatch(metadata -> metadata.schemaExists(connectorSession, schema.getSchemaName()));
    }

    @Override
    public List<String> listSchemaNames(Session session, String catalogName)
    {
        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, catalogName);

        ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
        if (catalog.isPresent()) {
            CatalogMetadata catalogMetadata = catalog.get();
            ConnectorSession connectorSession = session.toConnectorSession(catalogMetadata.getConnectorId());
            for (ConnectorId connectorId : catalogMetadata.listConnectorIds()) {
                ConnectorMetadata metadata = catalogMetadata.getMetadataFor(connectorId);
                schemaNames.addAll(metadata.listSchemaNames(connectorSession));
            }
        }
        return ImmutableList.copyOf(schemaNames.build());
    }

    @Override
    public Optional<TableHandle> getTableHandle(Session session, QualifiedObjectName table)
    {
        requireNonNull(table, "table is null");

        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, table.getCatalogName());
        if (catalog.isPresent()) {
            CatalogMetadata catalogMetadata = catalog.get();
            ConnectorId connectorId = catalogMetadata.getConnectorId(table);
            ConnectorMetadata metadata = catalogMetadata.getMetadataFor(connectorId);

            ConnectorTableHandle tableHandle = metadata.getTableHandle(session.toConnectorSession(connectorId), table.asSchemaTableName());
            if (tableHandle != null) {
                return Optional.of(new TableHandle(connectorId, tableHandle));
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

        ConnectorId connectorId = table.getConnectorId();
        ConnectorTableHandle connectorTable = table.getConnectorHandle();

        CatalogMetadata catalogMetadata = getCatalogMetadata(session, connectorId);
        ConnectorMetadata metadata = catalogMetadata.getMetadataFor(connectorId);
        ConnectorTransactionHandle transaction = catalogMetadata.getTransactionHandleFor(connectorId);
        ConnectorSession connectorSession = session.toConnectorSession(connectorId);
        List<ConnectorTableLayoutResult> layouts = metadata.getTableLayouts(connectorSession, connectorTable, constraint, desiredColumns);

        return layouts.stream()
                .map(layout -> new TableLayoutResult(fromConnectorLayout(connectorId, transaction, layout.getTableLayout()), layout.getUnenforcedConstraint()))
                .collect(toImmutableList());
    }

    @Override
    public TableLayout getLayout(Session session, TableLayoutHandle handle)
    {
        ConnectorId connectorId = handle.getConnectorId();
        CatalogMetadata catalogMetadata = getCatalogMetadata(session, connectorId);
        ConnectorMetadata metadata = catalogMetadata.getMetadataFor(connectorId);
        ConnectorTransactionHandle transaction = catalogMetadata.getTransactionHandleFor(connectorId);
        return fromConnectorLayout(connectorId, transaction, metadata.getTableLayout(session.toConnectorSession(connectorId), handle.getConnectorHandle()));
    }

    @Override
    public Optional<Object> getInfo(Session session, TableLayoutHandle handle)
    {
        ConnectorId connectorId = handle.getConnectorId();
        ConnectorMetadata metadata = getMetadata(session, connectorId);
        ConnectorTableLayout tableLayout = metadata.getTableLayout(session.toConnectorSession(connectorId), handle.getConnectorHandle());
        return metadata.getInfo(tableLayout.getHandle());
    }

    @Override
    public TableMetadata getTableMetadata(Session session, TableHandle tableHandle)
    {
        ConnectorId connectorId = tableHandle.getConnectorId();
        ConnectorMetadata metadata = getMetadata(session, connectorId);
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(session.toConnectorSession(connectorId), tableHandle.getConnectorHandle());

        return new TableMetadata(connectorId, tableMetadata);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(Session session, TableHandle tableHandle)
    {
        ConnectorId connectorId = tableHandle.getConnectorId();
        ConnectorMetadata metadata = getMetadata(session, connectorId);
        Map<String, ColumnHandle> handles = metadata.getColumnHandles(session.toConnectorSession(connectorId), tableHandle.getConnectorHandle());

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

        ConnectorId connectorId = tableHandle.getConnectorId();
        ConnectorMetadata metadata = getMetadata(session, connectorId);
        return metadata.getColumnMetadata(session.toConnectorSession(connectorId), tableHandle.getConnectorHandle(), columnHandle);
    }

    @Override
    public List<QualifiedObjectName> listTables(Session session, QualifiedTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");

        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, prefix.getCatalogName());
        Set<QualifiedObjectName> tables = new LinkedHashSet<>();
        if (catalog.isPresent()) {
            CatalogMetadata catalogMetadata = catalog.get();

            String schemaNameOrNull = prefix.getSchemaName().orElse(null);
            for (ConnectorId connectorId : catalogMetadata.listConnectorIds()) {
                ConnectorMetadata metadata = catalogMetadata.getMetadataFor(connectorId);
                ConnectorSession connectorSession = session.toConnectorSession(connectorId);
                metadata.listTables(connectorSession, schemaNameOrNull).stream()
                        .map(convertFromSchemaTableName(prefix.getCatalogName())::apply)
                        .forEach(tables::add);
            }
        }
        return ImmutableList.copyOf(tables);
    }

    @Override
    public Map<QualifiedObjectName, List<ColumnMetadata>> listTableColumns(Session session, QualifiedTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");

        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, prefix.getCatalogName());
        Map<QualifiedObjectName, List<ColumnMetadata>> tableColumns = new HashMap<>();
        if (catalog.isPresent()) {
            CatalogMetadata catalogMetadata = catalog.get();

            SchemaTablePrefix tablePrefix = prefix.asSchemaTablePrefix();
            for (ConnectorId connectorId : catalogMetadata.listConnectorIds()) {
                ConnectorMetadata metadata = catalogMetadata.getMetadataFor(connectorId);

                ConnectorSession connectorSession = session.toConnectorSession(connectorId);
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
        }
        return ImmutableMap.copyOf(tableColumns);
    }

    @Override
    public void createSchema(Session session, CatalogSchemaName schema, Map<String, Object> properties)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, schema.getCatalogName());
        ConnectorId connectorId = catalogMetadata.getConnectorId();
        ConnectorMetadata metadata = catalogMetadata.getMetadata();
        metadata.createSchema(session.toConnectorSession(connectorId), schema.getSchemaName(), properties);
    }

    @Override
    public void dropSchema(Session session, CatalogSchemaName schema)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, schema.getCatalogName());
        ConnectorId connectorId = catalogMetadata.getConnectorId();
        ConnectorMetadata metadata = catalogMetadata.getMetadata();
        metadata.dropSchema(session.toConnectorSession(connectorId), schema.getSchemaName());
    }

    @Override
    public void renameSchema(Session session, CatalogSchemaName source, String target)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, source.getCatalogName());
        ConnectorId connectorId = catalogMetadata.getConnectorId();
        ConnectorMetadata metadata = catalogMetadata.getMetadata();
        metadata.renameSchema(session.toConnectorSession(connectorId), source.getSchemaName(), target);
    }

    @Override
    public TableIdentity getTableIdentity(Session session, TableHandle tableHandle)
    {
        ConnectorMetadata metadata = getMetadata(session, tableHandle.getConnectorId());
        return metadata.getTableIdentity(tableHandle.getConnectorHandle());
    }

    @Override
    public TableIdentity deserializeTableIdentity(Session session, String catalogName, byte[] bytes)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
        return catalogMetadata.getMetadata().deserializeTableIdentity(bytes);
    }

    @Override
    public ColumnIdentity getColumnIdentity(Session session, TableHandle tableHandle, ColumnHandle columnHandle)
    {
        ConnectorMetadata metadata = getMetadata(session, tableHandle.getConnectorId());
        return metadata.getColumnIdentity(columnHandle);
    }

    @Override
    public ColumnIdentity deserializeColumnIdentity(Session session, String catalogName, byte[] bytes)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
        return catalogMetadata.getMetadata().deserializeColumnIdentity(bytes);
    }

    @Override
    public void createTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
        ConnectorId connectorId = catalogMetadata.getConnectorId();
        ConnectorMetadata metadata = catalogMetadata.getMetadata();
        metadata.createTable(session.toConnectorSession(connectorId), tableMetadata);
    }

    @Override
    public void renameTable(Session session, TableHandle tableHandle, QualifiedObjectName newTableName)
    {
        String catalogName = newTableName.getCatalogName();
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
        ConnectorId connectorId = catalogMetadata.getConnectorId();
        if (!tableHandle.getConnectorId().equals(connectorId)) {
            throw new PrestoException(SYNTAX_ERROR, "Cannot rename tables across catalogs");
        }

        ConnectorMetadata metadata = catalogMetadata.getMetadata();
        metadata.renameTable(session.toConnectorSession(connectorId), tableHandle.getConnectorHandle(), newTableName.asSchemaTableName());
    }

    @Override
    public void renameColumn(Session session, TableHandle tableHandle, ColumnHandle source, String target)
    {
        ConnectorId connectorId = tableHandle.getConnectorId();
        ConnectorMetadata metadata = getMetadataForWrite(session, connectorId);
        metadata.renameColumn(session.toConnectorSession(connectorId), tableHandle.getConnectorHandle(), source, target.toLowerCase(ENGLISH));
    }

    @Override
    public void addColumn(Session session, TableHandle tableHandle, ColumnMetadata column)
    {
        ConnectorId connectorId = tableHandle.getConnectorId();
        ConnectorMetadata metadata = getMetadataForWrite(session, connectorId);
        metadata.addColumn(session.toConnectorSession(connectorId), tableHandle.getConnectorHandle(), column);
    }

    @Override
    public void dropTable(Session session, TableHandle tableHandle)
    {
        ConnectorId connectorId = tableHandle.getConnectorId();
        ConnectorMetadata metadata = getMetadataForWrite(session, connectorId);
        metadata.dropTable(session.toConnectorSession(connectorId), tableHandle.getConnectorHandle());
    }

    @Override
    public Optional<NewTableLayout> getInsertLayout(Session session, TableHandle table)
    {
        ConnectorId connectorId = table.getConnectorId();
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, connectorId);
        ConnectorMetadata metadata = catalogMetadata.getMetadata();

        Optional<ConnectorNewTableLayout> insertLayout = metadata.getInsertLayout(session.toConnectorSession(connectorId), table.getConnectorHandle());
        if (!insertLayout.isPresent()) {
            return Optional.empty();
        }

        return Optional.of(new NewTableLayout(connectorId, catalogMetadata.getTransactionHandleFor(connectorId), insertLayout.get()));
    }

    @Override
    public Optional<NewTableLayout> getNewTableLayout(Session session, String catalogName, ConnectorTableMetadata tableMetadata)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
        ConnectorId connectorId = catalogMetadata.getConnectorId();
        ConnectorMetadata metadata = catalogMetadata.getMetadata();

        ConnectorTransactionHandle transactionHandle = catalogMetadata.getTransactionHandleFor(connectorId);
        ConnectorSession connectorSession = session.toConnectorSession(connectorId);
        return metadata.getNewTableLayout(connectorSession, tableMetadata)
                .map(layout -> new NewTableLayout(connectorId, transactionHandle, layout));
    }

    @Override
    public OutputTableHandle beginCreateTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata, Optional<NewTableLayout> layout)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
        ConnectorId connectorId = catalogMetadata.getConnectorId();
        ConnectorMetadata metadata = catalogMetadata.getMetadata();

        ConnectorTransactionHandle transactionHandle = catalogMetadata.getTransactionHandleFor(connectorId);
        ConnectorSession connectorSession = session.toConnectorSession(connectorId);
        ConnectorOutputTableHandle handle = metadata.beginCreateTable(connectorSession, tableMetadata, layout.map(NewTableLayout::getLayout));
        return new OutputTableHandle(connectorId, transactionHandle, handle);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(Session session, OutputTableHandle tableHandle, Collection<Slice> fragments)
    {
        ConnectorId connectorId = tableHandle.getConnectorId();
        ConnectorMetadata metadata = getMetadata(session, connectorId);
        return metadata.finishCreateTable(session.toConnectorSession(connectorId), tableHandle.getConnectorHandle(), fragments);
    }

    @Override
    public InsertTableHandle beginInsert(Session session, TableHandle tableHandle)
    {
        ConnectorId connectorId = tableHandle.getConnectorId();
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, connectorId);
        ConnectorMetadata metadata = catalogMetadata.getMetadata();
        ConnectorTransactionHandle transactionHandle = catalogMetadata.getTransactionHandleFor(connectorId);
        ConnectorInsertTableHandle handle = metadata.beginInsert(session.toConnectorSession(connectorId), tableHandle.getConnectorHandle());
        return new InsertTableHandle(tableHandle.getConnectorId(), transactionHandle, handle);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(Session session, InsertTableHandle tableHandle, Collection<Slice> fragments)
    {
        ConnectorId connectorId = tableHandle.getConnectorId();
        ConnectorMetadata metadata = getMetadata(session, connectorId);
        return metadata.finishInsert(session.toConnectorSession(connectorId), tableHandle.getConnectorHandle(), fragments);
    }

    @Override
    public ColumnHandle getUpdateRowIdColumnHandle(Session session, TableHandle tableHandle)
    {
        ConnectorId connectorId = tableHandle.getConnectorId();
        ConnectorMetadata metadata = getMetadata(session, connectorId);
        return metadata.getUpdateRowIdColumnHandle(session.toConnectorSession(connectorId), tableHandle.getConnectorHandle());
    }

    @Override
    public boolean supportsMetadataDelete(Session session, TableHandle tableHandle, TableLayoutHandle tableLayoutHandle)
    {
        ConnectorId connectorId = tableHandle.getConnectorId();
        ConnectorMetadata metadata = getMetadata(session, connectorId);
        return metadata.supportsMetadataDelete(
                session.toConnectorSession(connectorId),
                tableHandle.getConnectorHandle(),
                tableLayoutHandle.getConnectorHandle());
    }

    @Override
    public OptionalLong metadataDelete(Session session, TableHandle tableHandle, TableLayoutHandle tableLayoutHandle)
    {
        ConnectorId connectorId = tableHandle.getConnectorId();
        ConnectorMetadata metadata = getMetadataForWrite(session, connectorId);
        return metadata.metadataDelete(session.toConnectorSession(connectorId), tableHandle.getConnectorHandle(), tableLayoutHandle.getConnectorHandle());
    }

    @Override
    public TableHandle beginDelete(Session session, TableHandle tableHandle)
    {
        ConnectorId connectorId = tableHandle.getConnectorId();
        ConnectorMetadata metadata = getMetadataForWrite(session, connectorId);
        ConnectorTableHandle newHandle = metadata.beginDelete(session.toConnectorSession(connectorId), tableHandle.getConnectorHandle());
        return new TableHandle(tableHandle.getConnectorId(), newHandle);
    }

    @Override
    public void finishDelete(Session session, TableHandle tableHandle, Collection<Slice> fragments)
    {
        ConnectorId connectorId = tableHandle.getConnectorId();
        ConnectorMetadata metadata = getMetadata(session, connectorId);
        metadata.finishDelete(session.toConnectorSession(connectorId), tableHandle.getConnectorHandle(), fragments);
    }

    @Override
    public Optional<ConnectorId> getCatalogHandle(Session session, String catalogName)
    {
        return transactionManager.getOptionalCatalogMetadata(session.getRequiredTransactionId(), catalogName).map(CatalogMetadata::getConnectorId);
    }

    @Override
    public Map<String, ConnectorId> getCatalogNames(Session session)
    {
        return transactionManager.getCatalogNames(session.getRequiredTransactionId());
    }

    @Override
    public List<QualifiedObjectName> listViews(Session session, QualifiedTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");

        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, prefix.getCatalogName());

        Set<QualifiedObjectName> views = new LinkedHashSet<>();
        if (catalog.isPresent()) {
            CatalogMetadata catalogMetadata = catalog.get();

            String schemaNameOrNull = prefix.getSchemaName().orElse(null);
            for (ConnectorId connectorId : catalogMetadata.listConnectorIds()) {
                ConnectorMetadata metadata = catalogMetadata.getMetadataFor(connectorId);
                ConnectorSession connectorSession = session.toConnectorSession(connectorId);
                metadata.listViews(connectorSession, schemaNameOrNull).stream()
                        .map(convertFromSchemaTableName(prefix.getCatalogName())::apply)
                        .forEach(views::add);
            }
        }
        return ImmutableList.copyOf(views);
    }

    @Override
    public Map<QualifiedObjectName, ViewDefinition> getViews(Session session, QualifiedTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");

        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, prefix.getCatalogName());

        Map<QualifiedObjectName, ViewDefinition> views = new LinkedHashMap<>();
        if (catalog.isPresent()) {
            CatalogMetadata catalogMetadata = catalog.get();

            SchemaTablePrefix tablePrefix = prefix.asSchemaTablePrefix();
            for (ConnectorId connectorId : catalogMetadata.listConnectorIds()) {
                ConnectorMetadata metadata = catalogMetadata.getMetadataFor(connectorId);
                ConnectorSession connectorSession = session.toConnectorSession(connectorId);
                for (Entry<SchemaTableName, ConnectorViewDefinition> entry : metadata.getViews(connectorSession, tablePrefix).entrySet()) {
                    QualifiedObjectName viewName = new QualifiedObjectName(
                            prefix.getCatalogName(),
                            entry.getKey().getSchemaName(),
                            entry.getKey().getTableName());
                    views.put(viewName, deserializeView(entry.getValue().getViewData()));
                }
            }
        }
        return ImmutableMap.copyOf(views);
    }

    @Override
    public Optional<ViewDefinition> getView(Session session, QualifiedObjectName viewName)
    {
        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, viewName.getCatalogName());
        if (catalog.isPresent()) {
            CatalogMetadata catalogMetadata = catalog.get();
            ConnectorId connectorId = catalogMetadata.getConnectorId(viewName);
            ConnectorMetadata metadata = catalogMetadata.getMetadataFor(connectorId);

            Map<SchemaTableName, ConnectorViewDefinition> views = metadata.getViews(
                    session.toConnectorSession(connectorId),
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
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, viewName.getCatalogName());
        ConnectorId connectorId = catalogMetadata.getConnectorId();
        ConnectorMetadata metadata = catalogMetadata.getMetadata();

        metadata.createView(session.toConnectorSession(connectorId), viewName.asSchemaTableName(), viewData, replace);
    }

    @Override
    public void dropView(Session session, QualifiedObjectName viewName)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, viewName.getCatalogName());
        ConnectorId connectorId = catalogMetadata.getConnectorId();
        ConnectorMetadata metadata = catalogMetadata.getMetadata();

        metadata.dropView(session.toConnectorSession(connectorId), viewName.asSchemaTableName());
    }

    @Override
    public Optional<ResolvedIndex> resolveIndex(Session session, TableHandle tableHandle, Set<ColumnHandle> indexableColumns, Set<ColumnHandle> outputColumns, TupleDomain<ColumnHandle> tupleDomain)
    {
        ConnectorId connectorId = tableHandle.getConnectorId();
        CatalogMetadata catalogMetadata = getCatalogMetadata(session, connectorId);
        ConnectorMetadata metadata = catalogMetadata.getMetadataFor(connectorId);
        ConnectorTransactionHandle transaction = catalogMetadata.getTransactionHandleFor(connectorId);
        ConnectorSession connectorSession = session.toConnectorSession(connectorId);
        Optional<ConnectorResolvedIndex> resolvedIndex = metadata.resolveIndex(connectorSession, tableHandle.getConnectorHandle(), indexableColumns, outputColumns, tupleDomain);
        return resolvedIndex.map(resolved -> new ResolvedIndex(tableHandle.getConnectorId(), transaction, resolved));
    }

    @Override
    public void grantTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, String grantee, boolean grantOption)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, tableName.getCatalogName());
        ConnectorId connectorId = catalogMetadata.getConnectorId();
        ConnectorMetadata metadata = catalogMetadata.getMetadata();

        metadata.grantTablePrivileges(session.toConnectorSession(connectorId), tableName.asSchemaTableName(), privileges, grantee, grantOption);
    }

    @Override
    public void revokeTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, String grantee, boolean grantOption)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, tableName.getCatalogName());
        ConnectorId connectorId = catalogMetadata.getConnectorId();
        ConnectorMetadata metadata = catalogMetadata.getMetadata();

        metadata.revokeTablePrivileges(session.toConnectorSession(connectorId), tableName.asSchemaTableName(), privileges, grantee, grantOption);
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
    public SchemaPropertyManager getSchemaPropertyManager()
    {
        return schemaPropertyManager;
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

    private Optional<CatalogMetadata> getOptionalCatalogMetadata(Session session, String catalogName)
    {
        return transactionManager.getOptionalCatalogMetadata(session.getRequiredTransactionId(), catalogName);
    }

    private CatalogMetadata getCatalogMetadata(Session session, ConnectorId connectorId)
    {
        return transactionManager.getCatalogMetadata(session.getRequiredTransactionId(), connectorId);
    }

    private CatalogMetadata getCatalogMetadataForWrite(Session session, String catalogName)
    {
        return transactionManager.getCatalogMetadataForWrite(session.getRequiredTransactionId(), catalogName);
    }

    private CatalogMetadata getCatalogMetadataForWrite(Session session, ConnectorId connectorId)
    {
        return transactionManager.getCatalogMetadataForWrite(session.getRequiredTransactionId(), connectorId);
    }

    private ConnectorMetadata getMetadata(Session session, ConnectorId connectorId)
    {
        return getCatalogMetadata(session, connectorId).getMetadataFor(connectorId);
    }

    private ConnectorMetadata getMetadataForWrite(Session session, ConnectorId connectorId)
    {
        return getCatalogMetadataForWrite(session, connectorId).getMetadata();
    }

    private static JsonCodec<ViewDefinition> createTestingViewCodec()
    {
        ObjectMapperProvider provider = new ObjectMapperProvider();
        provider.setJsonDeserializers(ImmutableMap.of(Type.class, new TypeDeserializer(new TypeRegistry())));
        return new JsonCodecFactory(provider).jsonCodec(ViewDefinition.class);
    }
}

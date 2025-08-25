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
package com.facebook.presto.iceberg;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.NodeVersion;
import com.facebook.presto.hive.TableAlreadyExistsException;
import com.facebook.presto.iceberg.statistics.StatisticsFileCache;
import com.facebook.presto.iceberg.util.IcebergPrestoModelConverters;
import com.facebook.presto.spi.ConnectorNewTableLayout;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.ConnectorViewDefinition;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.FilterStatsCalculatorService;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ReplaceSortOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewBuilder;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;

import static com.facebook.presto.iceberg.CatalogType.HADOOP;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_COMMIT_ERROR;
import static com.facebook.presto.iceberg.IcebergSessionProperties.getCompressionCodec;
import static com.facebook.presto.iceberg.IcebergTableProperties.getPartitioning;
import static com.facebook.presto.iceberg.IcebergTableProperties.getSortOrder;
import static com.facebook.presto.iceberg.IcebergTableProperties.getTableLocation;
import static com.facebook.presto.iceberg.IcebergTableType.DATA;
import static com.facebook.presto.iceberg.IcebergUtil.VIEW_OWNER;
import static com.facebook.presto.iceberg.IcebergUtil.createIcebergViewProperties;
import static com.facebook.presto.iceberg.IcebergUtil.getColumns;
import static com.facebook.presto.iceberg.IcebergUtil.getNativeIcebergTable;
import static com.facebook.presto.iceberg.IcebergUtil.getNativeIcebergView;
import static com.facebook.presto.iceberg.IcebergUtil.populateTableProperties;
import static com.facebook.presto.iceberg.PartitionFields.parsePartitionFields;
import static com.facebook.presto.iceberg.PartitionSpecConverter.toPrestoPartitionSpec;
import static com.facebook.presto.iceberg.SchemaConverter.toPrestoSchema;
import static com.facebook.presto.iceberg.SortFieldUtils.parseSortFields;
import static com.facebook.presto.iceberg.util.IcebergPrestoModelConverters.toIcebergNamespace;
import static com.facebook.presto.iceberg.util.IcebergPrestoModelConverters.toIcebergTableIdentifier;
import static com.facebook.presto.iceberg.util.IcebergPrestoModelConverters.toPrestoSchemaName;
import static com.facebook.presto.iceberg.util.IcebergPrestoModelConverters.toPrestoSchemaTableName;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.StandardErrorCode.SCHEMA_NOT_EMPTY;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.iceberg.NullOrder.NULLS_FIRST;
import static org.apache.iceberg.NullOrder.NULLS_LAST;

public class IcebergNativeMetadata
        extends IcebergAbstractMetadata
{
    private static final String VIEW_DIALECT = "presto";

    private final Optional<String> warehouseDataDir;
    private final IcebergNativeCatalogFactory catalogFactory;
    private final CatalogType catalogType;
    private final ConcurrentMap<SchemaTableName, View> icebergViews = new ConcurrentHashMap<>();

    public IcebergNativeMetadata(
            IcebergNativeCatalogFactory catalogFactory,
            TypeManager typeManager,
            StandardFunctionResolution functionResolution,
            RowExpressionService rowExpressionService,
            JsonCodec<CommitTaskData> commitTaskCodec,
            CatalogType catalogType,
            NodeVersion nodeVersion,
            FilterStatsCalculatorService filterStatsCalculatorService,
            StatisticsFileCache statisticsFileCache,
            IcebergTableProperties tableProperties)
    {
        super(typeManager, functionResolution, rowExpressionService, commitTaskCodec, nodeVersion, filterStatsCalculatorService, statisticsFileCache, tableProperties);
        this.catalogFactory = requireNonNull(catalogFactory, "catalogFactory is null");
        this.catalogType = requireNonNull(catalogType, "catalogType is null");
        this.warehouseDataDir = Optional.ofNullable(catalogFactory.getCatalogWarehouseDataDir());
    }

    @Override
    protected Table getRawIcebergTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        return getNativeIcebergTable(catalogFactory, session, schemaTableName);
    }

    @Override
    protected View getIcebergView(ConnectorSession session, SchemaTableName schemaTableName)
    {
        return icebergViews.computeIfAbsent(
                schemaTableName,
                ignored -> getNativeIcebergView(catalogFactory, session, schemaTableName));
    }

    @Override
    protected boolean tableExists(ConnectorSession session, SchemaTableName schemaTableName)
    {
        IcebergTableName name = IcebergTableName.from(schemaTableName.getTableName());
        try {
            getIcebergTable(session, new SchemaTableName(schemaTableName.getSchemaName(), name.getTableName()));
        }
        catch (NoSuchTableException e) {
            // return null to throw
            return false;
        }
        return true;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        SupportsNamespaces supportsNamespaces = catalogFactory.getNamespaces(session);
        if (catalogFactory.isNestedNamespaceEnabled()) {
            return listNestedNamespaces(supportsNamespaces, Namespace.empty());
        }

        return supportsNamespaces.listNamespaces()
                .stream()
                .map(IcebergPrestoModelConverters::toPrestoSchemaName)
                .collect(toList());
    }

    private List<String> listNestedNamespaces(SupportsNamespaces supportsNamespaces, Namespace parentNamespace)
    {
        return supportsNamespaces.listNamespaces(parentNamespace)
                .stream()
                .flatMap(childNamespace -> Stream.concat(
                        Stream.of(toPrestoSchemaName(childNamespace)),
                        listNestedNamespaces(supportsNamespaces, childNamespace).stream()))
                .collect(toList());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        if (schemaName.isPresent() && INFORMATION_SCHEMA.equals(schemaName.get())) {
            return listSchemaNames(session).stream()
                    .map(schema -> new SchemaTableName(INFORMATION_SCHEMA, schema))
                    .collect(toList());
        }

        try {
            return catalogFactory.getCatalog(session).listTables(toIcebergNamespace(schemaName, catalogFactory.isNestedNamespaceEnabled()))
                    .stream()
                    .map(tableIdentifier -> toPrestoSchemaTableName(tableIdentifier, catalogFactory.isNestedNamespaceEnabled()))
                    .collect(toImmutableList());
        }
        catch (NoSuchNamespaceException e) {
            return ImmutableList.of();
        }
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties)
    {
        catalogFactory.getNamespaces(session).createNamespace(toIcebergNamespace(Optional.of(schemaName), catalogFactory.isNestedNamespaceEnabled()),
                properties.entrySet().stream()
                        .collect(toMap(Map.Entry::getKey, e -> e.getValue().toString())));
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        try {
            catalogFactory.getNamespaces(session).dropNamespace(toIcebergNamespace(Optional.of(schemaName), catalogFactory.isNestedNamespaceEnabled()));
        }
        catch (NamespaceNotEmptyException e) {
            throw new PrestoException(SCHEMA_NOT_EMPTY, "Schema not empty: " + schemaName);
        }
    }

    @Override
    public void renameSchema(ConnectorSession session, String source, String target)
    {
        throw new PrestoException(NOT_SUPPORTED, format("Iceberg %s catalog does not support rename namespace", catalogType.name()));
    }

    @Override
    public void createView(ConnectorSession session, ConnectorTableMetadata viewMetadata, String viewData, boolean replace)
    {
        Catalog catalog = catalogFactory.getCatalog(session);
        if (!(catalog instanceof ViewCatalog)) {
            throw new PrestoException(NOT_SUPPORTED, "This connector does not support creating views");
        }
        Schema schema = toIcebergSchema(viewMetadata.getColumns());
        ViewBuilder viewBuilder = ((ViewCatalog) catalog).buildView(toIcebergTableIdentifier(viewMetadata.getTable(), catalogFactory.isNestedNamespaceEnabled()))
                .withSchema(schema)
                .withDefaultNamespace(toIcebergNamespace(Optional.ofNullable(viewMetadata.getTable().getSchemaName()), catalogFactory.isNestedNamespaceEnabled()))
                .withQuery(VIEW_DIALECT, viewData)
                .withProperties(createIcebergViewProperties(session, nodeVersion.toString()));
        if (replace) {
            viewBuilder.createOrReplace();
        }
        else {
            viewBuilder.create();
        }
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName)
    {
        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
        Catalog catalog = catalogFactory.getCatalog(session);
        if (catalog instanceof ViewCatalog) {
            for (String schema : listSchemas(session, schemaName.orElse(null))) {
                try {
                    for (TableIdentifier tableIdentifier : ((ViewCatalog) catalog).listViews(
                            toIcebergNamespace(Optional.ofNullable(schema), catalogFactory.isNestedNamespaceEnabled()))) {
                        tableNames.add(new SchemaTableName(schema, tableIdentifier.name()));
                    }
                }
                catch (NoSuchNamespaceException e) {
                    // ignore
                }
            }
        }
        return tableNames.build();
    }

    private List<String> listSchemas(ConnectorSession session, String schemaNameOrNull)
    {
        if (schemaNameOrNull == null) {
            return listSchemaNames(session);
        }
        return ImmutableList.of(schemaNameOrNull);
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        ImmutableMap.Builder<SchemaTableName, ConnectorViewDefinition> views = ImmutableMap.builder();
        Catalog catalog = catalogFactory.getCatalog(session);
        if (catalog instanceof ViewCatalog) {
            List<SchemaTableName> tableNames;
            if (prefix.getSchemaName() != null && prefix.getTableName() != null) {
                tableNames = ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
            }
            else {
                tableNames = listViews(session, Optional.ofNullable(prefix.getSchemaName()));
            }

            for (SchemaTableName schemaTableName : tableNames) {
                try {
                    TableIdentifier viewIdentifier = toIcebergTableIdentifier(schemaTableName, catalogFactory.isNestedNamespaceEnabled());
                    if (((ViewCatalog) catalog).viewExists(viewIdentifier)) {
                        View view = ((ViewCatalog) catalog).loadView(viewIdentifier);
                        verifyAndPopulateViews(view, schemaTableName, view.sqlFor(VIEW_DIALECT).sql(), views);
                    }
                }
                catch (IllegalArgumentException e) {
                    // Ignore illegal view names
                }
            }
        }
        return views.build();
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        Catalog catalog = catalogFactory.getCatalog(session);
        if (!(catalog instanceof ViewCatalog)) {
            throw new PrestoException(NOT_SUPPORTED, "This connector does not support dropping views");
        }
        ((ViewCatalog) catalog).dropView(toIcebergTableIdentifier(viewName, catalogFactory.isNestedNamespaceEnabled()));
    }

    @Override
    public void renameView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        Catalog catalog = catalogFactory.getCatalog(session);
        if (!(catalog instanceof ViewCatalog)) {
            throw new PrestoException(NOT_SUPPORTED, "This connector does not support renaming views");
        }
        ((ViewCatalog) catalog).renameView(
                toIcebergTableIdentifier(source, catalogFactory.isNestedNamespaceEnabled()),
                toIcebergTableIdentifier(target, catalogFactory.isNestedNamespaceEnabled()));
    }

    private void verifyAndPopulateViews(View view, SchemaTableName schemaTableName, String viewData, ImmutableMap.Builder<SchemaTableName, ConnectorViewDefinition> views)
    {
        views.put(schemaTableName, new ConnectorViewDefinition(
                schemaTableName,
                Optional.ofNullable(view.properties().get(VIEW_OWNER)),
                viewData));
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();

        Schema schema = toIcebergSchema(tableMetadata.getColumns());

        PartitionSpec partitionSpec = parsePartitionFields(schema, getPartitioning(tableMetadata.getProperties()));
        FileFormat fileFormat = tableProperties.getFileFormat(session, tableMetadata.getProperties());

        try {
            TableIdentifier tableIdentifier = toIcebergTableIdentifier(schemaTableName, catalogFactory.isNestedNamespaceEnabled());
            String targetPath = getTableLocation(tableMetadata.getProperties());
            if (!isNullOrEmpty(targetPath)) {
                transaction = catalogFactory.getCatalog(session).newCreateTableTransaction(
                        tableIdentifier,
                        schema,
                        partitionSpec,
                        targetPath,
                        populateTableProperties(this, tableMetadata, tableProperties, fileFormat, session));
            }
            else {
                transaction = catalogFactory.getCatalog(session).newCreateTableTransaction(
                        tableIdentifier,
                        schema,
                        partitionSpec,
                        populateTableProperties(this, tableMetadata, tableProperties, fileFormat, session));
            }
        }
        catch (AlreadyExistsException e) {
            throw new TableAlreadyExistsException(schemaTableName);
        }

        Table icebergTable = transaction.table();
        ReplaceSortOrder replaceSortOrder = transaction.replaceSortOrder();
        SortOrder sortOrder = parseSortFields(schema, getSortOrder(tableMetadata.getProperties()));
        List<SortField> sortFields = getSupportedSortFields(icebergTable.schema(), sortOrder);
        for (SortField sortField : sortFields) {
            if (sortField.getSortOrder().isAscending()) {
                replaceSortOrder.asc(schema.findColumnName(sortField.getSourceColumnId()), sortField.getSortOrder().isNullsFirst() ? NULLS_FIRST : NULLS_LAST);
            }
            else {
                replaceSortOrder.desc(schema.findColumnName(sortField.getSourceColumnId()), sortField.getSortOrder().isNullsFirst() ? NULLS_FIRST : NULLS_LAST);
            }
        }

        try {
            replaceSortOrder.commit();
        }
        catch (RuntimeException e) {
            throw new PrestoException(ICEBERG_COMMIT_ERROR, "Failed to set the sorted_by property", e);
        }

        return new IcebergOutputTableHandle(
                schemaName,
                new IcebergTableName(tableName, DATA, Optional.empty(), Optional.empty()),
                toPrestoSchema(icebergTable.schema(), typeManager),
                toPrestoPartitionSpec(icebergTable.spec(), typeManager),
                getColumns(icebergTable.schema(), icebergTable.spec(), typeManager),
                icebergTable.location(),
                fileFormat,
                getCompressionCodec(session),
                icebergTable.properties(),
                sortFields);
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle icebergTableHandle = (IcebergTableHandle) tableHandle;
        verify(icebergTableHandle.getIcebergTableName().getTableType() == DATA, "only the data table can be dropped");
        TableIdentifier tableIdentifier = toIcebergTableIdentifier(icebergTableHandle.getSchemaTableName(), catalogFactory.isNestedNamespaceEnabled());
        catalogFactory.getCatalog(session).dropTable(tableIdentifier);
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTable)
    {
        IcebergTableHandle icebergTableHandle = (IcebergTableHandle) tableHandle;
        verify(icebergTableHandle.getIcebergTableName().getTableType() == DATA, "only the data table can be renamed");
        TableIdentifier from = toIcebergTableIdentifier(icebergTableHandle.getSchemaTableName(), catalogFactory.isNestedNamespaceEnabled());
        TableIdentifier to = toIcebergTableIdentifier(newTable, catalogFactory.isNestedNamespaceEnabled());
        catalogFactory.getCatalog(session).renameTable(from, to);
    }

    @Override
    public void registerTable(ConnectorSession clientSession, SchemaTableName schemaTableName, Path metadataLocation)
    {
        catalogFactory.getCatalog(clientSession).registerTable(toIcebergTableIdentifier(schemaTableName, catalogFactory.isNestedNamespaceEnabled()), metadataLocation.toString());
    }

    @Override
    public void unregisterTable(ConnectorSession clientSession, SchemaTableName schemaTableName)
    {
        catalogFactory.getCatalog(clientSession).dropTable(toIcebergTableIdentifier(schemaTableName, catalogFactory.isNestedNamespaceEnabled()), false);
    }

    protected Optional<String> getDataLocationBasedOnWarehouseDataDir(SchemaTableName schemaTableName)
    {
        if (!catalogType.equals(HADOOP)) {
            return Optional.empty();
        }
        return warehouseDataDir.map(base -> base + schemaTableName.getSchemaName() + "/" + schemaTableName.getTableName());
    }
}

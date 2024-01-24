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
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveColumnConverterProvider;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveTypeTranslator;
import com.facebook.presto.hive.NodeVersion;
import com.facebook.presto.hive.TableAlreadyExistsException;
import com.facebook.presto.hive.ViewAlreadyExistsException;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.Database;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.HivePrivilegeInfo;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.hive.metastore.PartitionStatistics;
import com.facebook.presto.hive.metastore.PrestoTableType;
import com.facebook.presto.hive.metastore.PrincipalPrivileges;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.iceberg.util.HiveStatisticsMergeStrategy;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorNewTableLayout;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.ConnectorViewDefinition;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.ViewNotFoundException;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.FilterStatsCalculatorService;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.spi.statistics.ColumnStatisticMetadata;
import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.facebook.presto.spi.statistics.ComputedStatistics;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.TableStatisticType;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.facebook.presto.spi.statistics.TableStatisticsMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.io.InputFile;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;

import static com.facebook.presto.hive.HiveStatisticsUtil.createPartitionStatistics;
import static com.facebook.presto.hive.HiveStatisticsUtil.updatePartitionStatistics;
import static com.facebook.presto.hive.HiveUtil.decodeViewData;
import static com.facebook.presto.hive.HiveUtil.encodeViewData;
import static com.facebook.presto.hive.HiveUtil.hiveColumnHandles;
import static com.facebook.presto.hive.metastore.HivePrivilegeInfo.HivePrivilege.DELETE;
import static com.facebook.presto.hive.metastore.HivePrivilegeInfo.HivePrivilege.INSERT;
import static com.facebook.presto.hive.metastore.HivePrivilegeInfo.HivePrivilege.SELECT;
import static com.facebook.presto.hive.metastore.HivePrivilegeInfo.HivePrivilege.UPDATE;
import static com.facebook.presto.hive.metastore.MetastoreUtil.TABLE_COMMENT;
import static com.facebook.presto.hive.metastore.MetastoreUtil.buildInitialPrivilegeSet;
import static com.facebook.presto.hive.metastore.MetastoreUtil.checkIfNullView;
import static com.facebook.presto.hive.metastore.MetastoreUtil.createTableObjectForViewCreation;
import static com.facebook.presto.hive.metastore.MetastoreUtil.getMetastoreHeaders;
import static com.facebook.presto.hive.metastore.MetastoreUtil.isPrestoView;
import static com.facebook.presto.hive.metastore.MetastoreUtil.isUserDefinedTypeEncodingEnabled;
import static com.facebook.presto.hive.metastore.MetastoreUtil.verifyAndPopulateViews;
import static com.facebook.presto.hive.metastore.Statistics.createComputedStatisticsToPartitionMap;
import static com.facebook.presto.iceberg.HiveTableOperations.STORAGE_FORMAT;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static com.facebook.presto.iceberg.IcebergSchemaProperties.getSchemaLocation;
import static com.facebook.presto.iceberg.IcebergSessionProperties.getHiveStatisticsMergeStrategy;
import static com.facebook.presto.iceberg.IcebergTableProperties.getFileFormat;
import static com.facebook.presto.iceberg.IcebergTableProperties.getFormatVersion;
import static com.facebook.presto.iceberg.IcebergTableProperties.getPartitioning;
import static com.facebook.presto.iceberg.IcebergTableProperties.getTableLocation;
import static com.facebook.presto.iceberg.IcebergTableType.DATA;
import static com.facebook.presto.iceberg.IcebergUtil.createIcebergViewProperties;
import static com.facebook.presto.iceberg.IcebergUtil.getColumns;
import static com.facebook.presto.iceberg.IcebergUtil.getHiveIcebergTable;
import static com.facebook.presto.iceberg.IcebergUtil.isIcebergTable;
import static com.facebook.presto.iceberg.IcebergUtil.toHiveColumns;
import static com.facebook.presto.iceberg.IcebergUtil.tryGetProperties;
import static com.facebook.presto.iceberg.PartitionFields.parsePartitionFields;
import static com.facebook.presto.iceberg.util.StatisticsUtil.mergeHiveStatistics;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_SCHEMA_PROPERTY;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.StandardErrorCode.SCHEMA_NOT_EMPTY;
import static com.facebook.presto.spi.security.PrincipalType.USER;
import static com.facebook.presto.spi.statistics.TableStatisticType.ROW_COUNT;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.TableMetadata.newTableMetadata;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.FORMAT_VERSION;
import static org.apache.iceberg.TableProperties.OBJECT_STORE_PATH;
import static org.apache.iceberg.TableProperties.WRITE_DATA_LOCATION;
import static org.apache.iceberg.TableProperties.WRITE_METADATA_LOCATION;
import static org.apache.iceberg.Transactions.createTableTransaction;

public class IcebergHiveMetadata
        extends IcebergAbstractMetadata
{
    private final ExtendedHiveMetastore metastore;
    private final HdfsEnvironment hdfsEnvironment;
    private final DateTimeZone timeZone = DateTimeZone.forTimeZone(TimeZone.getTimeZone(ZoneId.of(TimeZone.getDefault().getID())));

    private final FilterStatsCalculatorService filterStatsCalculatorService;

    public IcebergHiveMetadata(
            ExtendedHiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            StandardFunctionResolution functionResolution,
            RowExpressionService rowExpressionService,
            JsonCodec<CommitTaskData> commitTaskCodec,
            NodeVersion nodeVersion,
            FilterStatsCalculatorService filterStatsCalculatorService)
    {
        super(typeManager, functionResolution, rowExpressionService, commitTaskCodec, nodeVersion);
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.filterStatsCalculatorService = requireNonNull(filterStatsCalculatorService, "filterStatsCalculatorService is null");
    }

    public ExtendedHiveMetastore getMetastore()
    {
        return metastore;
    }

    @Override
    protected org.apache.iceberg.Table getIcebergTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        return getHiveIcebergTable(metastore, hdfsEnvironment, session, schemaTableName);
    }

    @Override
    protected boolean tableExists(ConnectorSession session, SchemaTableName schemaTableName)
    {
        IcebergTableName name = IcebergTableName.from(schemaTableName.getTableName());
        Optional<Table> hiveTable = metastore.getTable(getMetastoreContext(session), schemaTableName.getSchemaName(), name.getTableName());
        if (!hiveTable.isPresent()) {
            return false;
        }
        if (!isIcebergTable(hiveTable.get())) {
            throw new UnknownTableTypeException(schemaTableName);
        }
        return true;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return metastore.getAllDatabases(getMetastoreContext(session));
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        MetastoreContext metastoreContext = getMetastoreContext(session);
        // If schema name is not present, list tables from all schemas
        List<String> schemaNames = schemaName
                .map(ImmutableList::of)
                .orElseGet(() -> ImmutableList.copyOf(listSchemaNames(session)));
        return schemaNames.stream()
                .flatMap(schema -> metastore
                        .getAllTables(metastoreContext, schema)
                        .orElseGet(() -> metastore.getAllDatabases(metastoreContext))
                        .stream()
                        .map(table -> new SchemaTableName(schema, table)))
                .collect(toImmutableList());
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties)
    {
        Optional<String> location = getSchemaLocation(properties).map(uri -> {
            try {
                hdfsEnvironment.getFileSystem(new HdfsContext(session, schemaName), new Path(uri));
            }
            catch (IOException | IllegalArgumentException e) {
                throw new PrestoException(INVALID_SCHEMA_PROPERTY, "Invalid location URI: " + uri, e);
            }
            return uri;
        });

        Database database = Database.builder()
                .setDatabaseName(schemaName)
                .setLocation(location)
                .setOwnerType(USER)
                .setOwnerName(session.getUser())
                .build();

        MetastoreContext metastoreContext = getMetastoreContext(session);
        metastore.createDatabase(metastoreContext, database);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        // basic sanity check to provide a better error message
        if (!listTables(session, Optional.of(schemaName)).isEmpty() ||
                !listViews(session, Optional.of(schemaName)).isEmpty()) {
            throw new PrestoException(SCHEMA_NOT_EMPTY, "Schema not empty: " + schemaName);
        }
        MetastoreContext metastoreContext = getMetastoreContext(session);
        metastore.dropDatabase(metastoreContext, schemaName);
    }

    @Override
    public void renameSchema(ConnectorSession session, String source, String target)
    {
        MetastoreContext metastoreContext = getMetastoreContext(session);
        metastore.renameDatabase(metastoreContext, source, target);
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();

        Schema schema = toIcebergSchema(tableMetadata.getColumns());

        PartitionSpec partitionSpec = parsePartitionFields(schema, getPartitioning(tableMetadata.getProperties()));

        MetastoreContext metastoreContext = getMetastoreContext(session);
        Database database = metastore.getDatabase(metastoreContext, schemaName)
                .orElseThrow(() -> new SchemaNotFoundException(schemaName));

        HdfsContext hdfsContext = new HdfsContext(session, schemaName, tableName);
        String targetPath = getTableLocation(tableMetadata.getProperties());
        if (targetPath == null) {
            Optional<String> location = database.getLocation();
            if (!location.isPresent() || location.get().isEmpty()) {
                throw new PrestoException(NOT_SUPPORTED, "Database " + schemaName + " location is not set");
            }

            Path databasePath = new Path(location.get());
            Path resultPath = new Path(databasePath, tableName);
            targetPath = resultPath.toString();
        }

        TableOperations operations = new HiveTableOperations(
                metastore,
                getMetastoreContext(session),
                hdfsEnvironment,
                hdfsContext,
                schemaName,
                tableName,
                session.getUser(),
                targetPath);
        if (operations.current() != null) {
            throw new TableAlreadyExistsException(schemaTableName);
        }

        ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.builderWithExpectedSize(3);
        FileFormat fileFormat = getFileFormat(tableMetadata.getProperties());
        propertiesBuilder.put(DEFAULT_FILE_FORMAT, fileFormat.toString());
        if (tableMetadata.getComment().isPresent()) {
            propertiesBuilder.put(TABLE_COMMENT, tableMetadata.getComment().get());
        }

        String formatVersion = getFormatVersion(tableMetadata.getProperties());
        if (formatVersion != null) {
            propertiesBuilder.put(FORMAT_VERSION, formatVersion);
        }

        TableMetadata metadata = newTableMetadata(schema, partitionSpec, targetPath, propertiesBuilder.build());

        transaction = createTableTransaction(tableName, operations, metadata);

        return new IcebergWritableTableHandle(
                schemaName,
                new IcebergTableName(tableName, DATA, Optional.empty(), Optional.empty()),
                SchemaParser.toJson(metadata.schema()),
                PartitionSpecParser.toJson(metadata.spec()),
                getColumns(metadata.schema(), metadata.spec(), typeManager),
                targetPath,
                fileFormat,
                metadata.properties());
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;
        verify(handle.getIcebergTableName().getTableType() == DATA, "only the data table can be dropped");
        // TODO: support path override in Iceberg table creation
        org.apache.iceberg.Table table = getIcebergTable(session, handle.getSchemaTableName());
        Optional<Map<String, String>> tableProperties = tryGetProperties(table);
        if (tableProperties.isPresent()) {
            if (tableProperties.get().containsKey(OBJECT_STORE_PATH) ||
                    tableProperties.get().containsKey("write.folder-storage.path") || // Removed from Iceberg as of 0.14.0, but preserved for backward compatibility
                    tableProperties.get().containsKey(WRITE_METADATA_LOCATION) ||
                    tableProperties.get().containsKey(WRITE_DATA_LOCATION)) {
                throw new PrestoException(NOT_SUPPORTED, "Table " + handle.getSchemaTableName() + " contains Iceberg path override properties and cannot be dropped from Presto");
            }
        }
        metastore.dropTable(getMetastoreContext(session), handle.getSchemaName(), handle.getIcebergTableName().getTableName(), true);
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTable)
    {
        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;
        verify(handle.getIcebergTableName().getTableType() == DATA, "only the data table can be renamed");
        metastore.renameTable(getMetastoreContext(session), handle.getSchemaName(), handle.getIcebergTableName().getTableName(), newTable.getSchemaName(), newTable.getTableName());
    }

    @Override
    public void createView(ConnectorSession session, ConnectorTableMetadata viewMetadata, String viewData, boolean replace)
    {
        MetastoreContext metastoreContext = getMetastoreContext(session);
        SchemaTableName viewName = viewMetadata.getTable();
        Table table = createTableObjectForViewCreation(
                session,
                viewMetadata,
                createIcebergViewProperties(session, nodeVersion.toString()),
                new HiveTypeTranslator(),
                metastoreContext,
                encodeViewData(viewData));
        PrincipalPrivileges principalPrivileges = buildInitialPrivilegeSet(session.getUser());

        Optional<Table> existing = metastore.getTable(metastoreContext, viewName.getSchemaName(), viewName.getTableName());
        if (existing.isPresent()) {
            if (!replace || !isPrestoView(existing.get())) {
                throw new ViewAlreadyExistsException(viewName);
            }

            metastore.replaceTable(metastoreContext, viewName.getSchemaName(), viewName.getTableName(), table, principalPrivileges);
            return;
        }

        try {
            metastore.createTable(metastoreContext, table, principalPrivileges);
        }
        catch (TableAlreadyExistsException e) {
            throw new ViewAlreadyExistsException(e.getTableName());
        }
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName)
    {
        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
        MetastoreContext metastoreContext = getMetastoreContext(session);
        for (String schema : listSchemas(session, schemaName.orElse(null))) {
            for (String tableName : metastore.getAllViews(metastoreContext, schema).orElse(Collections.emptyList())) {
                tableNames.add(new SchemaTableName(schema, tableName));
            }
        }
        return tableNames.build();
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        ImmutableMap.Builder<SchemaTableName, ConnectorViewDefinition> views = ImmutableMap.builder();
        List<SchemaTableName> tableNames;
        if (prefix.getTableName() != null) {
            tableNames = ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
        }
        else {
            tableNames = listViews(session, Optional.of(prefix.getSchemaName()));
        }
        MetastoreContext metastoreContext = getMetastoreContext(session);
        for (SchemaTableName schemaTableName : tableNames) {
            Optional<Table> table = metastore.getTable(metastoreContext, schemaTableName.getSchemaName(), schemaTableName.getTableName());
            if (table.isPresent() && isPrestoView(table.get())) {
                verifyAndPopulateViews(table.get(), schemaTableName, decodeViewData(table.get().getViewOriginalText().get()), views);
            }
        }
        return views.build();
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        ConnectorViewDefinition view = getViews(session, viewName.toSchemaTablePrefix()).get(viewName);
        checkIfNullView(view, viewName);

        try {
            metastore.dropTable(
                    getMetastoreContext(session),
                    viewName.getSchemaName(),
                    viewName.getTableName(),
                    true);
        }
        catch (TableNotFoundException e) {
            throw new ViewNotFoundException(e.getTableName());
        }
    }

    private MetastoreContext getMetastoreContext(ConnectorSession session)
    {
        return new MetastoreContext(session.getIdentity(), session.getQueryId(), session.getClientInfo(), session.getSource(), getMetastoreHeaders(session), isUserDefinedTypeEncodingEnabled(session), HiveColumnConverterProvider.DEFAULT_COLUMN_CONVERTER_PROVIDER, session.getWarningCollector());
    }

    private List<String> listSchemas(ConnectorSession session, String schemaNameOrNull)
    {
        if (schemaNameOrNull == null) {
            return listSchemaNames(session);
        }
        return ImmutableList.of(schemaNameOrNull);
    }

    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<ConnectorTableLayoutHandle> tableLayoutHandle, List<ColumnHandle> columnHandles, Constraint<ColumnHandle> constraint)
    {
        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;
        org.apache.iceberg.Table icebergTable = getHiveIcebergTable(metastore, hdfsEnvironment, session, handle.getSchemaTableName());
        TableStatistics icebergStatistics = TableStatisticsMaker.getTableStatistics(session, constraint, handle, icebergTable, columnHandles.stream().map(IcebergColumnHandle.class::cast).collect(Collectors.toList()));
        HiveStatisticsMergeStrategy mergeStrategy = getHiveStatisticsMergeStrategy(session);
        return tableLayoutHandle.map(IcebergTableLayoutHandle.class::cast).map(layoutHandle -> {
            TupleDomain<ColumnHandle> domainPredicate = layoutHandle.getDomainPredicate()
                    .transform(subfield -> isEntireColumn(subfield) ? subfield.getRootName() : null)
                    .transform(layoutHandle.getPredicateColumns()::get)
                    .transform(ColumnHandle.class::cast);

            TupleDomain<VariableReferenceExpression> predicate = domainPredicate.transform(icebergLayout -> {
                IcebergColumnHandle columnHandle = (IcebergColumnHandle) icebergLayout;
                return new VariableReferenceExpression(Optional.empty(), columnHandle.getName(), columnHandle.getType());
            });
            RowExpression translatedPredicate = rowExpressionService.getDomainTranslator().toPredicate(predicate);
            PartitionStatistics hiveStatistics = metastore.getTableStatistics(getMetastoreContext(session), handle.getSchemaName(), handle.getIcebergTableName().getTableName());
            TableStatistics mergedStatistics = mergeHiveStatistics(icebergStatistics, hiveStatistics, mergeStrategy, icebergTable.spec());
            TableStatistics.Builder filteredStatsBuilder = TableStatistics.builder()
                    .setRowCount(mergedStatistics.getRowCount());
            double totalSize = 0;
            for (ColumnHandle colHandle : columnHandles) {
                IcebergColumnHandle icebergHandle = (IcebergColumnHandle) colHandle;
                if (mergedStatistics.getColumnStatistics().containsKey(icebergHandle)) {
                    ColumnStatistics stats = mergedStatistics.getColumnStatistics().get(icebergHandle);
                    filteredStatsBuilder.setColumnStatistics(icebergHandle, stats);
                    if (!stats.getDataSize().isUnknown()) {
                        totalSize += stats.getDataSize().getValue();
                    }
                }
            }
            filteredStatsBuilder.setTotalSize(Estimate.of(totalSize));
            return filterStatsCalculatorService.filterStats(
                    filteredStatsBuilder.build(),
                    translatedPredicate,
                    session,
                    columnHandles.stream().map(IcebergColumnHandle.class::cast).collect(toImmutableMap(
                            col -> col,
                            IcebergColumnHandle::getName)),
                    columnHandles.stream().map(IcebergColumnHandle.class::cast).collect(toImmutableMap(
                            IcebergColumnHandle::getName,
                            IcebergColumnHandle::getType)));
        }).orElseGet(() -> {
            if (!mergeStrategy.equals(HiveStatisticsMergeStrategy.NONE)) {
                PartitionStatistics hiveStats = metastore.getTableStatistics(getMetastoreContext(session), handle.getSchemaName(), handle.getIcebergTableName().getTableName());
                return mergeHiveStatistics(icebergStatistics, hiveStats, mergeStrategy, icebergTable.spec());
            }
            return icebergStatistics;
        });
    }

    @Override
    public IcebergTableHandle getTableHandleForStatisticsCollection(ConnectorSession session, SchemaTableName tableName, Map<String, Object> analyzeProperties)
    {
        return getTableHandle(session, tableName);
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadata(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        Set<ColumnStatisticMetadata> columnStatistics = tableMetadata.getColumns().stream()
                .filter(column -> !column.isHidden())
                .flatMap(meta -> metastore.getSupportedColumnStatistics(getMetastoreContext(session), meta.getType())
                        .stream()
                        .map(statType -> statType.getColumnStatisticMetadata(meta.getName())))
                .collect(toImmutableSet());

        Set<TableStatisticType> tableStatistics = ImmutableSet.of(ROW_COUNT);
        return new TableStatisticsMetadata(columnStatistics, tableStatistics, Collections.emptyList());
    }

    @Override
    public ConnectorTableHandle beginStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return tableHandle;
    }

    @Override
    public void finishStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<ComputedStatistics> computedStatistics)
    {
        IcebergTableHandle icebergTableHandle = (IcebergTableHandle) tableHandle;
        MetastoreContext metastoreContext = getMetastoreContext(session);
        Table table = metastore.getTable(metastoreContext, icebergTableHandle.getSchemaTableName().getSchemaName(), icebergTableHandle.getSchemaTableName().getTableName())
                .orElseThrow(() -> new TableNotFoundException(icebergTableHandle.getSchemaTableName()));

        List<Column> partitionColumns = table.getPartitionColumns();
        List<String> partitionColumnNames = partitionColumns.stream()
                .map(Column::getName)
                .collect(toImmutableList());
        List<HiveColumnHandle> hiveColumnHandles = hiveColumnHandles(table);
        Map<String, Type> columnTypes = hiveColumnHandles.stream()
                .filter(columnHandle -> !columnHandle.isHidden())
                .collect(toImmutableMap(HiveColumnHandle::getName, column -> column.getHiveType().getType(typeManager)));

        Map<List<String>, ComputedStatistics> computedStatisticsMap = createComputedStatisticsToPartitionMap(computedStatistics, partitionColumnNames, columnTypes);

        // commit analyze to unpartitioned table
        PartitionStatistics tableStatistics = createPartitionStatistics(session, columnTypes, computedStatisticsMap.get(ImmutableList.<String>of()), timeZone);
        metastore.updateTableStatistics(metastoreContext,
                table.getDatabaseName(),
                table.getTableName(),
                oldStats -> updatePartitionStatistics(oldStats, tableStatistics));
    }

    @Override
    public void registerTable(ConnectorSession clientSession, SchemaTableName schemaTableName, Path metadataLocation)
    {
        String tableLocation = metadataLocation.getName();
        HdfsContext hdfsContext = new HdfsContext(
                clientSession,
                schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                tableLocation,
                true);

        InputFile inputFile = new HdfsInputFile(metadataLocation, hdfsEnvironment, hdfsContext);
        TableMetadata tableMetadata;
        try {
            tableMetadata = TableMetadataParser.read(new HdfsFileIO(hdfsEnvironment, hdfsContext), inputFile);
        }
        catch (Exception e) {
            throw new PrestoException(ICEBERG_INVALID_METADATA, String.format("Unable to read metadata file %s", metadataLocation), e);
        }

        Table.Builder builder = Table.builder()
                .setDatabaseName(schemaTableName.getSchemaName())
                .setTableName(schemaTableName.getTableName())
                .setOwner(clientSession.getUser())
                .setDataColumns(toHiveColumns(tableMetadata.schema().columns()))
                .setTableType(PrestoTableType.EXTERNAL_TABLE)
                .withStorage(storage -> storage.setLocation(tableMetadata.location()))
                .withStorage(storage -> storage.setStorageFormat(STORAGE_FORMAT))
                .setParameter("EXTERNAL", "TRUE")
                .setParameter(BaseMetastoreTableOperations.TABLE_TYPE_PROP, BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE.toUpperCase(ENGLISH))
                .setParameter(BaseMetastoreTableOperations.METADATA_LOCATION_PROP, tableMetadata.metadataFileLocation());
        Table table = builder.build();

        PrestoPrincipal owner = new PrestoPrincipal(USER, table.getOwner());
        PrincipalPrivileges privileges = new PrincipalPrivileges(
                ImmutableMultimap.<String, HivePrivilegeInfo>builder()
                        .put(table.getOwner(), new HivePrivilegeInfo(SELECT, true, owner, owner))
                        .put(table.getOwner(), new HivePrivilegeInfo(INSERT, true, owner, owner))
                        .put(table.getOwner(), new HivePrivilegeInfo(UPDATE, true, owner, owner))
                        .put(table.getOwner(), new HivePrivilegeInfo(DELETE, true, owner, owner))
                        .build(),
                ImmutableMultimap.of());

        MetastoreContext metastoreContext = getMetastoreContext(clientSession);
        metastore.createTable(metastoreContext, table, privileges);
    }

    @Override
    public void unregisterTable(ConnectorSession clientSession, SchemaTableName schemaTableName)
    {
        MetastoreContext metastoreContext = getMetastoreContext(clientSession);
        metastore.dropTableFromMetastore(metastoreContext, schemaTableName.getSchemaName(), schemaTableName.getTableName());
    }
}

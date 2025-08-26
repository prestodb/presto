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
import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveColumnConverterProvider;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveTypeTranslator;
import com.facebook.presto.hive.NodeVersion;
import com.facebook.presto.hive.TableAlreadyExistsException;
import com.facebook.presto.hive.UnknownTableTypeException;
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
import com.facebook.presto.iceberg.statistics.StatisticsFileCache;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorNewTableLayout;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSystemConfig;
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
import com.facebook.presto.spi.relation.RowExpressionService;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.spi.statistics.ColumnStatisticMetadata;
import com.facebook.presto.spi.statistics.ColumnStatisticType;
import com.facebook.presto.spi.statistics.ComputedStatistics;
import com.facebook.presto.spi.statistics.TableStatisticType;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.facebook.presto.spi.statistics.TableStatisticsMetadata;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.MetricsModes.None;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.view.View;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static com.facebook.presto.hive.HiveStatisticsUtil.createPartitionStatistics;
import static com.facebook.presto.hive.HiveStatisticsUtil.updatePartitionStatistics;
import static com.facebook.presto.hive.HiveUtil.decodeViewData;
import static com.facebook.presto.hive.HiveUtil.encodeViewData;
import static com.facebook.presto.hive.HiveUtil.hiveColumnHandles;
import static com.facebook.presto.hive.SchemaProperties.getDatabaseProperties;
import static com.facebook.presto.hive.SchemaProperties.getLocation;
import static com.facebook.presto.hive.metastore.HivePrivilegeInfo.HivePrivilege.DELETE;
import static com.facebook.presto.hive.metastore.HivePrivilegeInfo.HivePrivilege.INSERT;
import static com.facebook.presto.hive.metastore.HivePrivilegeInfo.HivePrivilege.SELECT;
import static com.facebook.presto.hive.metastore.HivePrivilegeInfo.HivePrivilege.UPDATE;
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
import static com.facebook.presto.iceberg.IcebergSessionProperties.getCompressionCodec;
import static com.facebook.presto.iceberg.IcebergSessionProperties.getHiveStatisticsMergeStrategy;
import static com.facebook.presto.iceberg.IcebergTableProperties.getPartitioning;
import static com.facebook.presto.iceberg.IcebergTableProperties.getSortOrder;
import static com.facebook.presto.iceberg.IcebergTableProperties.getTableLocation;
import static com.facebook.presto.iceberg.IcebergTableType.DATA;
import static com.facebook.presto.iceberg.IcebergUtil.createIcebergViewProperties;
import static com.facebook.presto.iceberg.IcebergUtil.getColumns;
import static com.facebook.presto.iceberg.IcebergUtil.getHiveIcebergTable;
import static com.facebook.presto.iceberg.IcebergUtil.isIcebergTable;
import static com.facebook.presto.iceberg.IcebergUtil.populateTableProperties;
import static com.facebook.presto.iceberg.IcebergUtil.toHiveColumns;
import static com.facebook.presto.iceberg.IcebergUtil.tryGetProperties;
import static com.facebook.presto.iceberg.PartitionFields.parsePartitionFields;
import static com.facebook.presto.iceberg.PartitionSpecConverter.toPrestoPartitionSpec;
import static com.facebook.presto.iceberg.SchemaConverter.toPrestoSchema;
import static com.facebook.presto.iceberg.SortFieldUtils.parseSortFields;
import static com.facebook.presto.iceberg.util.StatisticsUtil.calculateBaseTableStatistics;
import static com.facebook.presto.iceberg.util.StatisticsUtil.calculateStatisticsConsideringLayout;
import static com.facebook.presto.iceberg.util.StatisticsUtil.mergeHiveStatistics;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_SCHEMA_PROPERTY;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.StandardErrorCode.SCHEMA_NOT_EMPTY;
import static com.facebook.presto.spi.security.PrincipalType.USER;
import static com.facebook.presto.spi.statistics.TableStatisticType.ROW_COUNT;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Collections.emptyList;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.TableMetadata.newTableMetadata;
import static org.apache.iceberg.TableProperties.OBJECT_STORE_PATH;
import static org.apache.iceberg.TableProperties.WRITE_DATA_LOCATION;
import static org.apache.iceberg.TableProperties.WRITE_METADATA_LOCATION;
import static org.apache.iceberg.Transactions.createTableTransaction;

public class IcebergHiveMetadata
        extends IcebergAbstractMetadata
{
    public static final int MAXIMUM_PER_QUERY_TABLE_CACHE_SIZE = 1000;

    private final IcebergCatalogName catalogName;
    private final ExtendedHiveMetastore metastore;
    private final HdfsEnvironment hdfsEnvironment;
    private final DateTimeZone timeZone = DateTimeZone.forTimeZone(TimeZone.getTimeZone(ZoneId.of(TimeZone.getDefault().getID())));
    private final IcebergHiveTableOperationsConfig hiveTableOperationsConfig;
    private final ConnectorSystemConfig connectorSystemConfig;
    private final Cache<SchemaTableName, Optional<Table>> tableCache;
    private final ManifestFileCache manifestFileCache;

    public IcebergHiveMetadata(
            IcebergCatalogName catalogName,
            ExtendedHiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            StandardFunctionResolution functionResolution,
            RowExpressionService rowExpressionService,
            JsonCodec<CommitTaskData> commitTaskCodec,
            NodeVersion nodeVersion,
            FilterStatsCalculatorService filterStatsCalculatorService,
            IcebergHiveTableOperationsConfig hiveTableOperationsConfig,
            StatisticsFileCache statisticsFileCache,
            ManifestFileCache manifestFileCache,
            IcebergTableProperties tableProperties,
            ConnectorSystemConfig connectorSystemConfig)
    {
        super(typeManager, functionResolution, rowExpressionService, commitTaskCodec, nodeVersion, filterStatsCalculatorService, statisticsFileCache, tableProperties);
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.hiveTableOperationsConfig = requireNonNull(hiveTableOperationsConfig, "hiveTableOperationsConfig is null");
        this.tableCache = CacheBuilder.newBuilder().maximumSize(MAXIMUM_PER_QUERY_TABLE_CACHE_SIZE).build();
        this.manifestFileCache = requireNonNull(manifestFileCache, "manifestFileCache is null");
        this.connectorSystemConfig = requireNonNull(connectorSystemConfig, "connectorSystemConfig is null");
    }

    public ExtendedHiveMetastore getMetastore()
    {
        return metastore;
    }

    @VisibleForTesting
    public ManifestFileCache getManifestFileCache()
    {
        return manifestFileCache;
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName)
    {
        Optional<Database> database = metastore.getDatabase(getMetastoreContext(session), schemaName);
        return database.isPresent();
    }

    @Override
    protected org.apache.iceberg.Table getRawIcebergTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        return getHiveIcebergTable(metastore, hdfsEnvironment, hiveTableOperationsConfig, manifestFileCache, session, catalogName, schemaTableName);
    }

    @Override
    protected View getIcebergView(ConnectorSession session, SchemaTableName schemaTableName)
    {
        throw new PrestoException(NOT_SUPPORTED, "Iceberg Hive catalog does not support native Iceberg views.");
    }

    @Override
    protected boolean tableExists(ConnectorSession session, SchemaTableName schemaTableName)
    {
        Optional<Table> hiveTable = getHiveTable(session, schemaTableName);
        if (!hiveTable.isPresent()) {
            return false;
        }
        if (!isIcebergTable(hiveTable.get())) {
            throw new UnknownTableTypeException("Not an Iceberg table: " + schemaTableName);
        }
        return true;
    }

    private Optional<Table> getHiveTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        IcebergTableName name = IcebergTableName.from(schemaTableName.getTableName());
        try {
            return tableCache.get(schemaTableName, () ->
                    metastore.getTable(getMetastoreContext(session), schemaTableName.getSchemaName(), name.getTableName()));
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), PrestoException.class);
            throw e;
        }
        catch (ExecutionException e) {
            throw new RuntimeException("Unexpected checked exception by cache load from metastore", e);
        }
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return metastore.getAllDatabases(getMetastoreContext(session));
    }

    @Override
    public Map<String, Object> getSchemaProperties(ConnectorSession session, CatalogSchemaName schemaName)
    {
        MetastoreContext metastoreContext = getMetastoreContext(session);
        Optional<Database> database = metastore.getDatabase(metastoreContext, schemaName.getSchemaName());
        if (database.isPresent()) {
            return getDatabaseProperties(database.get());
        }
        throw new SchemaNotFoundException(schemaName.getSchemaName());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        MetastoreContext metastoreContext = getMetastoreContext(session);
        if (schemaName.isPresent() && INFORMATION_SCHEMA.equals(schemaName.get())) {
            return metastore.getAllDatabases(metastoreContext)
                    .stream()
                    .map(table -> new SchemaTableName(INFORMATION_SCHEMA, table))
                    .collect(toImmutableList());
        }
        // If schema name is not present, list tables from all schemas
        List<String> schemaNames = schemaName
                .map(ImmutableList::of)
                .orElseGet(() -> ImmutableList.copyOf(listSchemaNames(session)));
        return schemaNames.stream()
                .flatMap(schema -> metastore
                        .getAllTables(metastoreContext, schema)
                        .orElseGet(() -> ImmutableList.of())
                        .stream()
                        .map(table -> new SchemaTableName(schema, table)))
                .collect(toImmutableList());
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties)
    {
        Optional<String> location = getLocation(properties).map(uri -> {
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
                hiveTableOperationsConfig,
                manifestFileCache,
                schemaName,
                tableName,
                session.getUser(),
                targetPath);
        if (operations.current() != null) {
            throw new TableAlreadyExistsException(schemaTableName);
        }
        SortOrder sortOrder = parseSortFields(schema, getSortOrder(tableMetadata.getProperties()));
        FileFormat fileFormat = tableProperties.getFileFormat(session, tableMetadata.getProperties());
        TableMetadata metadata = newTableMetadata(schema, partitionSpec, sortOrder, targetPath, populateTableProperties(this, tableMetadata, tableProperties, fileFormat, session));
        transaction = createTableTransaction(tableName, operations, metadata);

        return new IcebergOutputTableHandle(
                schemaName,
                new IcebergTableName(tableName, DATA, Optional.empty(), Optional.empty()),
                toPrestoSchema(metadata.schema(), typeManager),
                toPrestoPartitionSpec(metadata.spec(), typeManager),
                getColumns(metadata.schema(), metadata.spec(), typeManager),
                targetPath,
                fileFormat,
                getCompressionCodec(session),
                metadata.properties(),
                getSupportedSortFields(metadata.schema(), sortOrder));
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

        Optional<Table> existing = getHiveTable(session, viewName);
        if (existing.isPresent()) {
            if (!replace || !isPrestoView(existing.get())) {
                throw new ViewAlreadyExistsException(viewName);
            }

            metastore.replaceTable(metastoreContext, viewName.getSchemaName(), viewName.getTableName(), table, principalPrivileges);
            return;
        }

        try {
            metastore.createTable(metastoreContext, table, principalPrivileges, emptyList());
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
            for (String tableName : metastore.getAllViews(metastoreContext, schema).orElse(emptyList())) {
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
        if (prefix.getSchemaName() != null && prefix.getTableName() != null) {
            tableNames = ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
        }
        else {
            tableNames = listViews(session, Optional.ofNullable(prefix.getSchemaName()));
        }
        for (SchemaTableName schemaTableName : tableNames) {
            Optional<Table> table = getHiveTable(session, schemaTableName);
            if (table.isPresent() && isPrestoView(table.get())) {
                verifyAndPopulateViews(table.get(), schemaTableName, decodeViewData(table.get().getViewOriginalText().get()), views);
            }
        }
        return views.build();
    }

    @Override
    public void renameView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        // Not checking if source view exists as this is already done in RenameViewTask
        metastore.renameTable(getMetastoreContext(session), source.getSchemaName(), source.getTableName(), target.getSchemaName(), target.getTableName());
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
        return new MetastoreContext(session.getIdentity(), session.getQueryId(), session.getClientInfo(), session.getClientTags(), session.getSource(), getMetastoreHeaders(session), isUserDefinedTypeEncodingEnabled(session), HiveColumnConverterProvider.DEFAULT_COLUMN_CONVERTER_PROVIDER, session.getWarningCollector(), session.getRuntimeStats());
    }

    private List<String> listSchemas(ConnectorSession session, String schemaNameOrNull)
    {
        if (schemaNameOrNull == null) {
            return listSchemaNames(session);
        }
        return ImmutableList.of(schemaNameOrNull);
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<ConnectorTableLayoutHandle> tableLayoutHandle, List<ColumnHandle> columnHandles, Constraint<ColumnHandle> constraint)
    {
        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;
        org.apache.iceberg.Table icebergTable = getIcebergTable(session, handle.getSchemaTableName());
        TableStatistics icebergStatistics = calculateBaseTableStatistics(this, typeManager, session, statisticsFileCache, (IcebergTableHandle) tableHandle, tableLayoutHandle, columnHandles, constraint);
        EnumSet<ColumnStatisticType> mergeFlags = getHiveStatisticsMergeStrategy(session);
        TableStatistics mergedStatistics = Optional.of(mergeFlags)
                .filter(set -> !set.isEmpty())
                .map(flags -> {
                    PartitionStatistics hiveStatistics = metastore.getTableStatistics(getMetastoreContext(session), handle.getSchemaName(), handle.getIcebergTableName().getTableName());
                    return mergeHiveStatistics(icebergStatistics, hiveStatistics, mergeFlags, icebergTable.spec());
                })
                .orElse(icebergStatistics);
        return calculateStatisticsConsideringLayout(filterStatsCalculatorService, rowExpressionService, mergedStatistics, session, tableLayoutHandle);
    }

    @Override
    public IcebergTableHandle getTableHandleForStatisticsCollection(ConnectorSession session, SchemaTableName tableName, Map<String, Object> analyzeProperties)
    {
        return getTableHandle(session, tableName);
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadata(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        org.apache.iceberg.Table icebergTable = getIcebergTable(session, tableMetadata.getTable());
        Set<ColumnStatisticMetadata> hiveColumnStatistics = getHiveSupportedColumnStatistics(session, icebergTable, tableMetadata);
        Set<ColumnStatisticMetadata> supportedStatistics = ImmutableSet.<ColumnStatisticMetadata>builder()
                .addAll(hiveColumnStatistics)
                // iceberg table-supported statistics
                .addAll(!connectorSystemConfig.isNativeExecution() ?
                        super.getStatisticsCollectionMetadata(session, tableMetadata).getColumnStatistics() : ImmutableSet.of())
                .build();
        Set<TableStatisticType> tableStatistics = ImmutableSet.of(ROW_COUNT);
        return new TableStatisticsMetadata(supportedStatistics, tableStatistics, emptyList());
    }

    private Set<ColumnStatisticMetadata> getHiveSupportedColumnStatistics(ConnectorSession session, org.apache.iceberg.Table table, ConnectorTableMetadata tableMetadata)
    {
        MetricsConfig metricsConfig = MetricsConfig.forTable(table);
        return tableMetadata.getColumns().stream()
                .filter(column -> !column.isHidden())
                .filter(column -> metricsConfig.columnMode(column.getName()) != None.get())
                .flatMap(meta -> {
                    try {
                        return metastore.getSupportedColumnStatistics(getMetastoreContext(session), meta.getType())
                                .stream()
                                .map(statType -> statType.getColumnStatisticMetadata(meta.getName()));
                    }
                    // thrown in the case the type isn't supported by HMS statistics
                    catch (IllegalArgumentException e) {
                        return Stream.empty();
                    }
                })
                .collect(toImmutableSet());
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
        Table table = getHiveTable(session, icebergTableHandle.getSchemaTableName())
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
        ConnectorTableMetadata metadata = getTableMetadata(session, tableHandle);
        org.apache.iceberg.Table icebergTable = getIcebergTable(session, icebergTableHandle.getSchemaTableName());
        Set<ColumnStatisticMetadata> hiveSupportedStatistics = getHiveSupportedColumnStatistics(session, icebergTable, metadata);
        PartitionStatistics tableStatistics = createPartitionStatistics(
                session,
                columnTypes,
                computedStatisticsMap.get(ImmutableList.<String>of()),
                hiveSupportedStatistics,
                timeZone);
        metastore.updateTableStatistics(metastoreContext,
                table.getDatabaseName(),
                table.getTableName(),
                oldStats -> updatePartitionStatistics(oldStats, tableStatistics));

        Set<ColumnStatisticMetadata> icebergSupportedStatistics = super.getStatisticsCollectionMetadata(session, metadata).getColumnStatistics();
        Collection<ComputedStatistics> icebergComputedStatistics = computedStatistics.stream().map(stat -> {
            ComputedStatistics.Builder builder = ComputedStatistics.builder(stat.getGroupingColumns(), stat.getGroupingValues());
            stat.getTableStatistics()
                    .forEach(builder::addTableStatistic);
            stat.getColumnStatistics().entrySet().stream()
                    .filter(entry -> icebergSupportedStatistics.contains(entry.getKey()))
                    .forEach(entry -> builder.addColumnStatistic(entry.getKey(), entry.getValue()));
            return builder.build();
        }).collect(toImmutableList());
        super.finishStatisticsCollection(session, tableHandle, icebergComputedStatistics);
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
            tableMetadata = TableMetadataParser.read(new HdfsFileIO(manifestFileCache, hdfsEnvironment, hdfsContext), inputFile);
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
        metastore.createTable(metastoreContext, table, privileges, emptyList());
    }

    @Override
    public void unregisterTable(ConnectorSession clientSession, SchemaTableName schemaTableName)
    {
        MetastoreContext metastoreContext = getMetastoreContext(clientSession);
        metastore.dropTableFromMetastore(metastoreContext, schemaTableName.getSchemaName(), schemaTableName.getTableName());
    }
}

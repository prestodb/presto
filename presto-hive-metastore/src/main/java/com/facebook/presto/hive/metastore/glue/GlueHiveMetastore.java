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
package com.facebook.presto.hive.metastore.glue;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.metrics.RequestMetricCollector;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.glue.AWSGlueAsync;
import com.amazonaws.services.glue.AWSGlueAsyncClientBuilder;
import com.amazonaws.services.glue.model.AlreadyExistsException;
import com.amazonaws.services.glue.model.BatchCreatePartitionRequest;
import com.amazonaws.services.glue.model.BatchCreatePartitionResult;
import com.amazonaws.services.glue.model.BatchGetPartitionRequest;
import com.amazonaws.services.glue.model.BatchGetPartitionResult;
import com.amazonaws.services.glue.model.CreateDatabaseRequest;
import com.amazonaws.services.glue.model.CreateTableRequest;
import com.amazonaws.services.glue.model.DatabaseInput;
import com.amazonaws.services.glue.model.DeleteDatabaseRequest;
import com.amazonaws.services.glue.model.DeletePartitionRequest;
import com.amazonaws.services.glue.model.DeleteTableRequest;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.ErrorDetail;
import com.amazonaws.services.glue.model.GetDatabaseRequest;
import com.amazonaws.services.glue.model.GetDatabaseResult;
import com.amazonaws.services.glue.model.GetDatabasesRequest;
import com.amazonaws.services.glue.model.GetDatabasesResult;
import com.amazonaws.services.glue.model.GetPartitionRequest;
import com.amazonaws.services.glue.model.GetPartitionResult;
import com.amazonaws.services.glue.model.GetPartitionsRequest;
import com.amazonaws.services.glue.model.GetPartitionsResult;
import com.amazonaws.services.glue.model.GetTableRequest;
import com.amazonaws.services.glue.model.GetTableResult;
import com.amazonaws.services.glue.model.GetTablesRequest;
import com.amazonaws.services.glue.model.GetTablesResult;
import com.amazonaws.services.glue.model.PartitionError;
import com.amazonaws.services.glue.model.PartitionInput;
import com.amazonaws.services.glue.model.PartitionValueList;
import com.amazonaws.services.glue.model.Segment;
import com.amazonaws.services.glue.model.TableInput;
import com.amazonaws.services.glue.model.UpdateDatabaseRequest;
import com.amazonaws.services.glue.model.UpdatePartitionRequest;
import com.amazonaws.services.glue.model.UpdateTableRequest;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.PartitionNotFoundException;
import com.facebook.presto.hive.SchemaAlreadyExistsException;
import com.facebook.presto.hive.TableAlreadyExistsException;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.Database;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.HivePrivilegeInfo;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.hive.metastore.MetastoreUtil;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.PartitionNameWithVersion;
import com.facebook.presto.hive.metastore.PartitionStatistics;
import com.facebook.presto.hive.metastore.PartitionWithStatistics;
import com.facebook.presto.hive.metastore.PrincipalPrivileges;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.hive.metastore.glue.converter.GlueInputConverter;
import com.facebook.presto.hive.metastore.glue.converter.GlueToPrestoConverter;
import com.facebook.presto.hive.metastore.glue.converter.GlueToPrestoConverter.GluePartitionConverter;
import com.facebook.presto.spi.ColumnNotFoundException;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.security.ConnectorIdentity;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.spi.security.RoleGrant;
import com.facebook.presto.spi.statistics.ColumnStatisticType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.airlift.units.Duration;
import org.apache.hadoop.fs.Path;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.function.Function;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_PARTITION_DROPPED_DURING_QUERY;
import static com.facebook.presto.hive.metastore.MetastoreUtil.convertPredicateToParts;
import static com.facebook.presto.hive.metastore.MetastoreUtil.createDirectory;
import static com.facebook.presto.hive.metastore.MetastoreUtil.getHiveBasicStatistics;
import static com.facebook.presto.hive.metastore.MetastoreUtil.makePartName;
import static com.facebook.presto.hive.metastore.MetastoreUtil.toPartitionValues;
import static com.facebook.presto.hive.metastore.MetastoreUtil.updateStatisticsParameters;
import static com.facebook.presto.hive.metastore.MetastoreUtil.verifyCanDropColumn;
import static com.facebook.presto.hive.metastore.PrestoTableType.MANAGED_TABLE;
import static com.facebook.presto.hive.metastore.PrestoTableType.VIRTUAL_VIEW;
import static com.facebook.presto.hive.metastore.glue.GlueExpressionUtil.buildGlueExpression;
import static com.facebook.presto.hive.metastore.glue.converter.GlueInputConverter.convertColumn;
import static com.facebook.presto.hive.metastore.glue.converter.GlueInputConverter.toTableInput;
import static com.facebook.presto.hive.metastore.glue.converter.GlueToPrestoConverter.mappedCopy;
import static com.facebook.presto.spi.StandardErrorCode.ALREADY_EXISTS;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.security.PrincipalType.USER;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Comparators.lexicographical;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;
import static java.util.function.UnaryOperator.identity;
import static java.util.stream.Collectors.toMap;

public class GlueHiveMetastore
        implements ExtendedHiveMetastore
{
    private static final Logger log = Logger.get(GlueHiveMetastore.class);

    private static final String PUBLIC_ROLE_NAME = "public";
    private static final String DEFAULT_METASTORE_USER = "presto";
    private static final String WILDCARD_EXPRESSION = "";
    private static final int BATCH_GET_PARTITION_MAX_PAGE_SIZE = 1000;
    private static final int BATCH_CREATE_PARTITION_MAX_PAGE_SIZE = 100;
    private static final int AWS_GLUE_GET_PARTITIONS_MAX_RESULTS = 1000;
    private static final Comparator<Partition> PARTITION_COMPARATOR = comparing(Partition::getValues, lexicographical(String.CASE_INSENSITIVE_ORDER));

    private final GlueMetastoreStats stats = new GlueMetastoreStats();
    private final HdfsEnvironment hdfsEnvironment;
    private final HdfsContext hdfsContext;
    private final AWSGlueAsync glueClient;
    private final Optional<String> defaultDir;
    private final String catalogId;
    private final int partitionSegments;
    private final Executor executor;

    @Inject
    public GlueHiveMetastore(
            HdfsEnvironment hdfsEnvironment,
            GlueHiveMetastoreConfig glueConfig,
            @ForGlueHiveMetastore Executor executor)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.hdfsContext = new HdfsContext(new ConnectorIdentity(DEFAULT_METASTORE_USER, Optional.empty(), Optional.empty()));
        this.glueClient = createAsyncGlueClient(requireNonNull(glueConfig, "glueConfig is null"), stats.newRequestMetricsCollector());
        this.defaultDir = glueConfig.getDefaultWarehouseDir();
        this.catalogId = glueConfig.getCatalogId().orElse(null);
        this.partitionSegments = glueConfig.getPartitionSegments();
        this.executor = requireNonNull(executor, "executor is null");
    }

    private static AWSGlueAsync createAsyncGlueClient(GlueHiveMetastoreConfig config, RequestMetricCollector metricsCollector)
    {
        ClientConfiguration clientConfig = new ClientConfiguration()
                .withMaxConnections(config.getMaxGlueConnections())
                .withMaxErrorRetry(config.getMaxGlueErrorRetries());
        AWSGlueAsyncClientBuilder asyncGlueClientBuilder = AWSGlueAsyncClientBuilder.standard()
                .withMetricsCollector(metricsCollector)
                .withClientConfiguration(clientConfig);

        if (config.getGlueEndpointUrl().isPresent()) {
            checkArgument(config.getGlueRegion().isPresent(), "Glue region must be set when Glue endpoint URL is set");
            asyncGlueClientBuilder.setEndpointConfiguration(new EndpointConfiguration(
                    config.getGlueEndpointUrl().get(),
                    config.getGlueRegion().get()));
        }
        else if (config.getGlueRegion().isPresent()) {
            asyncGlueClientBuilder.setRegion(config.getGlueRegion().get());
        }
        else if (config.getPinGlueClientToCurrentRegion()) {
            Region currentRegion = Regions.getCurrentRegion();
            if (currentRegion != null) {
                asyncGlueClientBuilder.setRegion(currentRegion.getName());
            }
        }

        if (config.getAwsAccessKey().isPresent() && config.getAwsSecretKey().isPresent()) {
            AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(
                    new BasicAWSCredentials(config.getAwsAccessKey().get(), config.getAwsSecretKey().get()));
            asyncGlueClientBuilder.setCredentials(credentialsProvider);
        }
        else if (config.getIamRole().isPresent()) {
            AWSCredentialsProvider credentialsProvider = new STSAssumeRoleSessionCredentialsProvider
                    .Builder(config.getIamRole().get(), "roleSessionName")
                    .build();
            asyncGlueClientBuilder.setCredentials(credentialsProvider);
        }

        return asyncGlueClientBuilder.build();
    }

    @Managed
    @Flatten
    public GlueMetastoreStats getStats()
    {
        return stats;
    }

    @Override
    public Optional<Database> getDatabase(MetastoreContext metastoreContext, String databaseName)
    {
        return stats.getGetDatabase().record(() -> {
            try {
                GetDatabaseResult result = glueClient.getDatabase(new GetDatabaseRequest().withCatalogId(catalogId).withName(databaseName));
                return Optional.of(GlueToPrestoConverter.convertDatabase(result.getDatabase()));
            }
            catch (EntityNotFoundException e) {
                return Optional.empty();
            }
            catch (AmazonServiceException e) {
                throw new PrestoException(HIVE_METASTORE_ERROR, e);
            }
        });
    }

    @Override
    public List<String> getAllDatabases(MetastoreContext metastoreContext)
    {
        try {
            List<String> databaseNames = new ArrayList<>();
            GetDatabasesRequest request = new GetDatabasesRequest().withCatalogId(catalogId);
            do {
                GetDatabasesResult result = stats.getGetDatabases().record(() -> glueClient.getDatabases(request));
                request.setNextToken(result.getNextToken());
                result.getDatabaseList().forEach(database -> databaseNames.add(database.getName()));
            }
            while (request.getNextToken() != null);

            return databaseNames;
        }
        catch (AmazonServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Optional<Table> getTable(MetastoreContext metastoreContext, String databaseName, String tableName)
    {
        return getGlueTable(databaseName, tableName).map(table -> GlueToPrestoConverter.convertTable(table, databaseName));
    }

    private com.amazonaws.services.glue.model.Table getGlueTableOrElseThrow(String databaseName, String tableName)
    {
        return getGlueTable(databaseName, tableName)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));
    }

    private Optional<com.amazonaws.services.glue.model.Table> getGlueTable(String databaseName, String tableName)
    {
        return stats.getGetTable().record(() -> {
            try {
                GetTableResult result = glueClient.getTable(new GetTableRequest()
                        .withCatalogId(catalogId)
                        .withDatabaseName(databaseName)
                        .withName(tableName));
                return Optional.of(result.getTable());
            }
            catch (EntityNotFoundException e) {
                return Optional.empty();
            }
            catch (AmazonServiceException e) {
                throw new PrestoException(HIVE_METASTORE_ERROR, e);
            }
        });
    }

    @Override
    public Set<ColumnStatisticType> getSupportedColumnStatistics(MetastoreContext metastoreContext, Type type)
    {
        return ImmutableSet.of();
    }

    private Table getTableOrElseThrow(MetastoreContext metastoreContext, String databaseName, String tableName)
    {
        return getTable(metastoreContext, databaseName, tableName)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));
    }

    @Override
    public PartitionStatistics getTableStatistics(MetastoreContext metastoreContext, String databaseName, String tableName)
    {
        Table table = getTable(metastoreContext, databaseName, tableName)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));
        return new PartitionStatistics(getHiveBasicStatistics(table.getParameters()), ImmutableMap.of());
    }

    @Override
    public Map<String, PartitionStatistics> getPartitionStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, Set<String> partitionNames)
    {
        ImmutableMap.Builder<String, PartitionStatistics> result = ImmutableMap.builder();
        getPartitionsByNames(metastoreContext, databaseName, tableName, ImmutableList.copyOf(partitionNames)).forEach((partitionName, optionalPartition) -> {
            Partition partition = optionalPartition.orElseThrow(() ->
                    new PartitionNotFoundException(new SchemaTableName(databaseName, tableName), toPartitionValues(partitionName)));
            PartitionStatistics partitionStatistics = new PartitionStatistics(getHiveBasicStatistics(partition.getParameters()), ImmutableMap.of());
            result.put(partitionName, partitionStatistics);
        });
        return result.build();
    }

    @Override
    public void updateTableStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        PartitionStatistics currentStatistics = getTableStatistics(metastoreContext, databaseName, tableName);
        PartitionStatistics updatedStatistics = update.apply(currentStatistics);
        if (!updatedStatistics.getColumnStatistics().isEmpty()) {
            throw new PrestoException(NOT_SUPPORTED, "Glue metastore does not support column level statistics");
        }

        Table table = getTableOrElseThrow(metastoreContext, databaseName, tableName);

        try {
            TableInput tableInput = GlueInputConverter.convertTable(table);
            tableInput.setParameters(updateStatisticsParameters(table.getParameters(), updatedStatistics.getBasicStatistics()));
            UpdateTableRequest request = new UpdateTableRequest()
                    .withCatalogId(catalogId)
                    .withDatabaseName(databaseName)
                    .withTableInput(tableInput);
            stats.getUpdateTable().record(() -> glueClient.updateTable(request));
        }
        catch (EntityNotFoundException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (AmazonServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public void updatePartitionStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, String partitionName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        PartitionStatistics currentStatistics = getPartitionStatistics(metastoreContext, databaseName, tableName, ImmutableSet.of(partitionName)).get(partitionName);
        if (currentStatistics == null) {
            throw new PrestoException(HIVE_PARTITION_DROPPED_DURING_QUERY, "Statistics result does not contain entry for partition: " + partitionName);
        }
        PartitionStatistics updatedStatistics = update.apply(currentStatistics);
        if (!updatedStatistics.getColumnStatistics().isEmpty()) {
            throw new PrestoException(NOT_SUPPORTED, "Glue metastore does not support column level statistics");
        }

        List<String> partitionValues = toPartitionValues(partitionName);
        Partition partition = getPartition(metastoreContext, databaseName, tableName, partitionValues)
                .orElseThrow(() -> new PartitionNotFoundException(new SchemaTableName(databaseName, tableName), partitionValues));
        try {
            PartitionInput partitionInput = GlueInputConverter.convertPartition(partition);
            partitionInput.setParameters(updateStatisticsParameters(partition.getParameters(), updatedStatistics.getBasicStatistics()));
            stats.getUpdatePartition().record(() -> glueClient.updatePartition(new UpdatePartitionRequest()
                    .withCatalogId(catalogId)
                    .withDatabaseName(databaseName)
                    .withTableName(tableName)
                    .withPartitionValueList(partition.getValues())
                    .withPartitionInput(partitionInput)));
        }
        catch (EntityNotFoundException e) {
            throw new PartitionNotFoundException(new SchemaTableName(databaseName, tableName), partitionValues);
        }
        catch (AmazonServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Optional<List<String>> getAllTables(MetastoreContext metastoreContext, String databaseName)
    {
        try {
            List<String> tableNames = new ArrayList<>();
            GetTablesRequest request = new GetTablesRequest().withCatalogId(catalogId).withDatabaseName(databaseName);
            do {
                GetTablesResult result = stats.getGetTables().record(() -> glueClient.getTables(request));
                request.setNextToken(result.getNextToken());
                result.getTableList().forEach(table -> tableNames.add(table.getName()));
            }
            while (request.getNextToken() != null);

            return Optional.of(tableNames);
        }
        catch (EntityNotFoundException e) {
            // database does not exist
            return Optional.empty();
        }
        catch (AmazonServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Optional<List<String>> getAllViews(MetastoreContext metastoreContext, String databaseName)
    {
        try {
            List<String> views = new ArrayList<>();
            GetTablesRequest request = new GetTablesRequest().withCatalogId(catalogId).withDatabaseName(databaseName);

            do {
                GetTablesResult result = stats.getGetTables().record(() -> glueClient.getTables(request));
                request.setNextToken(result.getNextToken());
                result.getTableList().stream()
                        .filter(table -> VIRTUAL_VIEW.name().equals(table.getTableType()))
                        .forEach(table -> views.add(table.getName()));
            }
            while (request.getNextToken() != null);

            return Optional.of(views);
        }
        catch (EntityNotFoundException e) {
            // database does not exist
            return Optional.empty();
        }
        catch (AmazonServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public void createDatabase(MetastoreContext metastoreContext, Database database)
    {
        if (!database.getLocation().isPresent() && defaultDir.isPresent()) {
            String databaseLocation = new Path(defaultDir.get(), database.getDatabaseName()).toString();
            database = Database.builder(database)
                    .setLocation(Optional.of(databaseLocation))
                    .build();
        }

        try {
            DatabaseInput databaseInput = GlueInputConverter.convertDatabase(database);
            stats.getCreateDatabase().record(() -> glueClient.createDatabase(new CreateDatabaseRequest().withCatalogId(catalogId).withDatabaseInput(databaseInput)));
        }
        catch (AlreadyExistsException e) {
            throw new SchemaAlreadyExistsException(database.getDatabaseName());
        }
        catch (AmazonServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }

        if (database.getLocation().isPresent()) {
            createDirectory(hdfsContext, hdfsEnvironment, new Path(database.getLocation().get()));
        }
    }

    @Override
    public void dropDatabase(MetastoreContext metastoreContext, String databaseName)
    {
        try {
            stats.getDeleteDatabase().record(() -> glueClient.deleteDatabase(new DeleteDatabaseRequest().withCatalogId(catalogId).withName(databaseName)));
        }
        catch (EntityNotFoundException e) {
            throw new SchemaNotFoundException(databaseName);
        }
        catch (AmazonServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public void renameDatabase(MetastoreContext metastoreContext, String databaseName, String newDatabaseName)
    {
        try {
            Database database = getDatabase(metastoreContext, databaseName).orElseThrow(() -> new SchemaNotFoundException(databaseName));
            DatabaseInput renamedDatabase = GlueInputConverter.convertDatabase(database).withName(newDatabaseName);
            stats.getUpdateDatabase().record(() -> glueClient.updateDatabase(new UpdateDatabaseRequest()
                    .withCatalogId(catalogId)
                    .withName(databaseName)
                    .withDatabaseInput(renamedDatabase)));
        }
        catch (AmazonServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public void createTable(MetastoreContext metastoreContext, Table table, PrincipalPrivileges principalPrivileges)
    {
        try {
            TableInput input = GlueInputConverter.convertTable(table);
            stats.getCreateTable().record(() -> glueClient.createTable(new CreateTableRequest()
                    .withCatalogId(catalogId)
                    .withDatabaseName(table.getDatabaseName())
                    .withTableInput(input)));
        }
        catch (AlreadyExistsException e) {
            throw new TableAlreadyExistsException(new SchemaTableName(table.getDatabaseName(), table.getTableName()));
        }
        catch (EntityNotFoundException e) {
            throw new SchemaNotFoundException(table.getDatabaseName());
        }
        catch (AmazonServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public void dropTable(MetastoreContext metastoreContext, String databaseName, String tableName, boolean deleteData)
    {
        Table table = getTableOrElseThrow(metastoreContext, databaseName, tableName);

        try {
            stats.getDeleteTable().record(() -> glueClient.deleteTable(new DeleteTableRequest()
                    .withCatalogId(catalogId)
                    .withDatabaseName(databaseName)
                    .withName(tableName)));
        }
        catch (AmazonServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }

        String tableLocation = table.getStorage().getLocation();
        if (deleteData && isManagedTable(table) && !isNullOrEmpty(tableLocation)) {
            deleteDir(hdfsContext, hdfsEnvironment, new Path(tableLocation), true);
        }
    }

    private static boolean isManagedTable(Table table)
    {
        return table.getTableType().equals(MANAGED_TABLE);
    }

    private static void deleteDir(HdfsContext context, HdfsEnvironment hdfsEnvironment, Path path, boolean recursive)
    {
        try {
            hdfsEnvironment.getFileSystem(context, path).delete(path, recursive);
        }
        catch (Exception e) {
            // don't fail if unable to delete path
            log.warn(e, "Failed to delete path: " + path.toString());
        }
    }

    @Override
    public void replaceTable(MetastoreContext metastoreContext, String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges)
    {
        try {
            TableInput newTableInput = GlueInputConverter.convertTable(newTable);
            stats.getUpdateTable().record(() -> glueClient.updateTable(new UpdateTableRequest()
                    .withCatalogId(catalogId)
                    .withDatabaseName(databaseName)
                    .withTableInput(newTableInput)));
        }
        catch (EntityNotFoundException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (AmazonServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public void renameTable(MetastoreContext metastoreContext, String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        throw new PrestoException(NOT_SUPPORTED, "Table rename is not yet supported by Glue service");
    }

    @Override
    public void addColumn(MetastoreContext metastoreContext, String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        com.amazonaws.services.glue.model.Table table = getGlueTableOrElseThrow(databaseName, tableName);
        ImmutableList.Builder<com.amazonaws.services.glue.model.Column> newDataColumns = ImmutableList.builder();
        newDataColumns.addAll(table.getStorageDescriptor().getColumns());
        newDataColumns.add(convertColumn(new Column(columnName, columnType, Optional.ofNullable(columnComment))));
        table.getStorageDescriptor().setColumns(newDataColumns.build());
        replaceGlueTable(databaseName, tableName, table);
    }

    @Override
    public void renameColumn(MetastoreContext metastoreContext, String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        com.amazonaws.services.glue.model.Table table = getGlueTableOrElseThrow(databaseName, tableName);
        if (table.getPartitionKeys() != null && table.getPartitionKeys().stream().anyMatch(c -> c.getName().equals(oldColumnName))) {
            throw new PrestoException(NOT_SUPPORTED, "Renaming partition columns is not supported");
        }
        ImmutableList.Builder<com.amazonaws.services.glue.model.Column> newDataColumns = ImmutableList.builder();
        for (com.amazonaws.services.glue.model.Column column : table.getStorageDescriptor().getColumns()) {
            if (column.getName().equals(oldColumnName)) {
                newDataColumns.add(new com.amazonaws.services.glue.model.Column()
                        .withName(newColumnName)
                        .withType(column.getType())
                        .withComment(column.getComment()));
            }
            else {
                newDataColumns.add(column);
            }
        }
        table.getStorageDescriptor().setColumns(newDataColumns.build());
        replaceGlueTable(databaseName, tableName, table);
    }

    @Override
    public void dropColumn(MetastoreContext metastoreContext, String databaseName, String tableName, String columnName)
    {
        verifyCanDropColumn(this, metastoreContext, databaseName, tableName, columnName);
        com.amazonaws.services.glue.model.Table table = getGlueTableOrElseThrow(databaseName, tableName);

        ImmutableList.Builder<com.amazonaws.services.glue.model.Column> newDataColumns = ImmutableList.builder();
        boolean found = false;
        for (com.amazonaws.services.glue.model.Column column : table.getStorageDescriptor().getColumns()) {
            if (column.getName().equals(columnName)) {
                found = true;
            }
            else {
                newDataColumns.add(column);
            }
        }

        if (!found) {
            SchemaTableName name = new SchemaTableName(databaseName, tableName);
            throw new ColumnNotFoundException(name, columnName);
        }

        table.getStorageDescriptor().setColumns(newDataColumns.build());
        replaceGlueTable(databaseName, tableName, table);
    }

    private void replaceGlueTable(String databaseName, String tableName, com.amazonaws.services.glue.model.Table newTable)
    {
        try {
            stats.getUpdateTable().record(() -> glueClient.updateTable(new UpdateTableRequest()
                    .withCatalogId(catalogId)
                    .withDatabaseName(databaseName)
                    .withTableInput(toTableInput(newTable))));
        }
        catch (EntityNotFoundException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (AmazonServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Optional<Partition> getPartition(MetastoreContext metastoreContext, String databaseName, String tableName, List<String> partitionValues)
    {
        return stats.getGetPartition().record(() -> {
            try {
                GetPartitionResult result = glueClient.getPartition(new GetPartitionRequest()
                        .withCatalogId(catalogId)
                        .withDatabaseName(databaseName)
                        .withTableName(tableName)
                        .withPartitionValues(partitionValues));
                return Optional.of(new GluePartitionConverter(databaseName, tableName).apply(result.getPartition()));
            }
            catch (EntityNotFoundException e) {
                return Optional.empty();
            }
            catch (AmazonServiceException e) {
                throw new PrestoException(HIVE_METASTORE_ERROR, e);
            }
        });
    }

    @Override
    public Optional<List<String>> getPartitionNames(MetastoreContext metastoreContext, String databaseName, String tableName)
    {
        Table table = getTableOrElseThrow(metastoreContext, databaseName, tableName);
        List<Partition> partitions = getPartitions(databaseName, tableName, WILDCARD_EXPRESSION);
        return Optional.of(buildPartitionNames(table.getPartitionColumns(), partitions));
    }

    /**
     * <pre>
     * Ex: Partition keys = ['a', 'b', 'c']
     *     Valid partition values:
     *     ['1','2','3'] or
     *     ['', '2', '']
     * </pre>
     *
     * @param partitionPredicates Full or partial list of partition values to filter on. Keys without filter will be empty strings.
     * @return a list of partition names.
     */
    @Override
    public List<String> getPartitionNamesByFilter(
            MetastoreContext metastoreContext,
            String databaseName,
            String tableName,
            Map<Column, Domain> partitionPredicates)
    {
        Table table = getTableOrElseThrow(metastoreContext, databaseName, tableName);
        List<String> parts = convertPredicateToParts(partitionPredicates);
        String expression = buildGlueExpression(table.getPartitionColumns(), parts);
        List<Partition> partitions = getPartitions(databaseName, tableName, expression);
        return buildPartitionNames(table.getPartitionColumns(), partitions);
    }

    @Override
    public List<PartitionNameWithVersion> getPartitionNamesWithVersionByFilter(
            MetastoreContext metastoreContext,
            String databaseName,
            String tableName,
            Map<Column, Domain> partitionPredicates)
    {
        throw new UnsupportedOperationException();
    }

    private List<Partition> getPartitions(String databaseName, String tableName, String expression)
    {
        if (partitionSegments == 1) {
            return getPartitions(databaseName, tableName, expression, null);
        }

        // Do parallel partition fetch.
        CompletionService<List<Partition>> completionService = new ExecutorCompletionService<>(executor);
        for (int i = 0; i < partitionSegments; i++) {
            Segment segment = new Segment().withSegmentNumber(i).withTotalSegments(partitionSegments);
            completionService.submit(() -> getPartitions(databaseName, tableName, expression, segment));
        }

        List<Partition> partitions = new ArrayList<>();
        try {
            for (int i = 0; i < partitionSegments; i++) {
                Future<List<Partition>> futurePartitions = completionService.take();
                partitions.addAll(futurePartitions.get());
            }
        }
        catch (ExecutionException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new PrestoException(HIVE_METASTORE_ERROR, "Failed to fetch partitions from Glue Data Catalog", e);
        }

        partitions.sort(PARTITION_COMPARATOR);
        return partitions;
    }

    private List<Partition> getPartitions(String databaseName, String tableName, String expression, @Nullable Segment segment)
    {
        try {
            GluePartitionConverter converter = new GluePartitionConverter(databaseName, tableName);
            ArrayList<Partition> partitions = new ArrayList<>();
            GetPartitionsRequest request = new GetPartitionsRequest()
                    .withCatalogId(catalogId)
                    .withDatabaseName(databaseName)
                    .withTableName(tableName)
                    .withExpression(expression)
                    .withSegment(segment)
                    .withMaxResults(AWS_GLUE_GET_PARTITIONS_MAX_RESULTS);

            do {
                GetPartitionsResult result = stats.getGetPartitions().record(() -> glueClient.getPartitions(request));
                request.setNextToken(result.getNextToken());
                partitions.ensureCapacity(partitions.size() + result.getPartitions().size());
                result.getPartitions().stream()
                        .map(converter)
                        .forEach(partitions::add);
            }
            while (request.getNextToken() != null);

            return partitions;
        }
        catch (AmazonServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    private static List<String> buildPartitionNames(List<Column> partitionColumns, List<Partition> partitions)
    {
        return mappedCopy(partitions, partition -> makePartName(partitionColumns, partition.getValues()));
    }

    /**
     * <pre>
     * Ex: Partition keys = ['a', 'b']
     *     Partition names = ['a=1/b=2', 'a=2/b=2']
     * </pre>
     *
     * @param partitionNames List of full partition names
     * @return Mapping of partition name to partition object
     */
    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(MetastoreContext metastoreContext, String databaseName, String tableName, List<String> partitionNames)
    {
        requireNonNull(partitionNames, "partitionNames is null");
        if (partitionNames.isEmpty()) {
            return ImmutableMap.of();
        }

        List<Partition> partitions = batchGetPartition(databaseName, tableName, partitionNames);

        Map<String, List<String>> partitionNameToPartitionValuesMap = partitionNames.stream()
                .collect(toMap(identity(), MetastoreUtil::toPartitionValues));
        Map<List<String>, Partition> partitionValuesToPartitionMap = partitions.stream()
                .collect(toMap(Partition::getValues, identity()));

        ImmutableMap.Builder<String, Optional<Partition>> resultBuilder = ImmutableMap.builder();
        for (Entry<String, List<String>> entry : partitionNameToPartitionValuesMap.entrySet()) {
            Partition partition = partitionValuesToPartitionMap.get(entry.getValue());
            resultBuilder.put(entry.getKey(), Optional.ofNullable(partition));
        }
        return resultBuilder.build();
    }

    private List<Partition> batchGetPartition(String databaseName, String tableName, List<String> partitionNames)
    {
        try {
            List<Future<BatchGetPartitionResult>> batchGetPartitionFutures = new ArrayList<>();

            for (List<String> partitionNamesBatch : Lists.partition(partitionNames, BATCH_GET_PARTITION_MAX_PAGE_SIZE)) {
                List<PartitionValueList> partitionValuesBatch = mappedCopy(partitionNamesBatch, partitionName -> new PartitionValueList().withValues(toPartitionValues(partitionName)));
                batchGetPartitionFutures.add(glueClient.batchGetPartitionAsync(new BatchGetPartitionRequest()
                        .withCatalogId(catalogId)
                        .withDatabaseName(databaseName)
                        .withTableName(tableName)
                        .withPartitionsToGet(partitionValuesBatch), stats.getBatchGetPartitions().metricsAsyncHandler()));
            }

            GluePartitionConverter converter = new GluePartitionConverter(databaseName, tableName);
            ImmutableList.Builder<Partition> resultsBuilder = ImmutableList.builderWithExpectedSize(partitionNames.size());
            for (Future<BatchGetPartitionResult> future : batchGetPartitionFutures) {
                future.get().getPartitions().stream()
                        .map(converter)
                        .forEach(resultsBuilder::add);
            }

            return resultsBuilder.build();
        }
        catch (AmazonServiceException | InterruptedException | ExecutionException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public void addPartitions(MetastoreContext metastoreContext, String databaseName, String tableName, List<PartitionWithStatistics> partitions)
    {
        try {
            List<Future<BatchCreatePartitionResult>> futures = new ArrayList<>();

            for (List<PartitionWithStatistics> partitionBatch : Lists.partition(partitions, BATCH_CREATE_PARTITION_MAX_PAGE_SIZE)) {
                List<PartitionInput> partitionInputs = mappedCopy(partitionBatch, GlueInputConverter::convertPartition);
                futures.add(glueClient.batchCreatePartitionAsync(new BatchCreatePartitionRequest()
                        .withCatalogId(catalogId)
                        .withDatabaseName(databaseName)
                        .withTableName(tableName)
                        .withPartitionInputList(partitionInputs), stats.getBatchCreatePartitions().metricsAsyncHandler()));
            }

            for (Future<BatchCreatePartitionResult> future : futures) {
                BatchCreatePartitionResult result = future.get();
                propagatePartitionErrorToPrestoException(databaseName, tableName, result.getErrors());
            }
        }
        catch (AmazonServiceException | InterruptedException | ExecutionException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    private static void propagatePartitionErrorToPrestoException(String databaseName, String tableName, List<PartitionError> partitionErrors)
    {
        if (partitionErrors != null && !partitionErrors.isEmpty()) {
            ErrorDetail errorDetail = partitionErrors.get(0).getErrorDetail();
            String glueExceptionCode = errorDetail.getErrorCode();

            switch (glueExceptionCode) {
                case "AlreadyExistsException":
                    throw new PrestoException(ALREADY_EXISTS, errorDetail.getErrorMessage());
                case "EntityNotFoundException":
                    throw new TableNotFoundException(new SchemaTableName(databaseName, tableName), errorDetail.getErrorMessage());
                default:
                    throw new PrestoException(HIVE_METASTORE_ERROR, errorDetail.getErrorCode() + ": " + errorDetail.getErrorMessage());
            }
        }
    }

    @Override
    public void dropPartition(MetastoreContext metastoreContext, String databaseName, String tableName, List<String> parts, boolean deleteData)
    {
        Table table = getTableOrElseThrow(metastoreContext, databaseName, tableName);
        Partition partition = getPartition(metastoreContext, databaseName, tableName, parts)
                .orElseThrow(() -> new PartitionNotFoundException(new SchemaTableName(databaseName, tableName), parts));

        try {
            stats.getDeletePartition().record(() -> glueClient.deletePartition(new DeletePartitionRequest()
                    .withCatalogId(catalogId)
                    .withDatabaseName(databaseName)
                    .withTableName(tableName)
                    .withPartitionValues(parts)));
        }
        catch (AmazonServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }

        String partLocation = partition.getStorage().getLocation();
        if (deleteData && isManagedTable(table) && !isNullOrEmpty(partLocation)) {
            deleteDir(hdfsContext, hdfsEnvironment, new Path(partLocation), true);
        }
    }

    @Override
    public void alterPartition(MetastoreContext metastoreContext, String databaseName, String tableName, PartitionWithStatistics partition)
    {
        try {
            PartitionInput newPartition = GlueInputConverter.convertPartition(partition);
            stats.getUpdatePartition().record(() -> glueClient.updatePartition(new UpdatePartitionRequest()
                    .withCatalogId(catalogId)
                    .withDatabaseName(databaseName)
                    .withTableName(tableName)
                    .withPartitionInput(newPartition)
                    .withPartitionValueList(partition.getPartition().getValues())));
        }
        catch (EntityNotFoundException e) {
            throw new PartitionNotFoundException(new SchemaTableName(databaseName, tableName), partition.getPartition().getValues());
        }
        catch (AmazonServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public void createRole(MetastoreContext metastoreContext, String role, String grantor)
    {
        throw new PrestoException(NOT_SUPPORTED, "createRole is not supported by Glue");
    }

    @Override
    public void dropRole(MetastoreContext metastoreContext, String role)
    {
        throw new PrestoException(NOT_SUPPORTED, "dropRole is not supported by Glue");
    }

    @Override
    public Set<String> listRoles(MetastoreContext metastoreContext)
    {
        return ImmutableSet.of(PUBLIC_ROLE_NAME);
    }

    @Override
    public void grantRoles(MetastoreContext metastoreContext, Set<String> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, PrestoPrincipal grantor)
    {
        throw new PrestoException(NOT_SUPPORTED, "grantRoles is not supported by Glue");
    }

    @Override
    public void revokeRoles(MetastoreContext metastoreContext, Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, PrestoPrincipal grantor)
    {
        throw new PrestoException(NOT_SUPPORTED, "revokeRoles is not supported by Glue");
    }

    @Override
    public Set<RoleGrant> listRoleGrants(MetastoreContext metastoreContext, PrestoPrincipal principal)
    {
        if (principal.getType() == USER) {
            return ImmutableSet.of(new RoleGrant(principal, PUBLIC_ROLE_NAME, false));
        }
        return ImmutableSet.of();
    }

    @Override
    public void grantTablePrivileges(MetastoreContext metastoreContext, String databaseName, String tableName, PrestoPrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {
        throw new PrestoException(NOT_SUPPORTED, "grantTablePrivileges is not supported by Glue");
    }

    @Override
    public void revokeTablePrivileges(MetastoreContext metastoreContext, String databaseName, String tableName, PrestoPrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {
        throw new PrestoException(NOT_SUPPORTED, "revokeTablePrivileges is not supported by Glue");
    }

    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(MetastoreContext metastoreContext, String databaseName, String tableName, PrestoPrincipal principal)
    {
        throw new PrestoException(NOT_SUPPORTED, "listTablePrivileges is not supported by Glue");
    }

    @Override
    public void setPartitionLeases(MetastoreContext metastoreContext, String databaseName, String tableName, Map<String, String> partitionNameToLocation, Duration leaseDuration)
    {
        throw new PrestoException(NOT_SUPPORTED, "setPartitionLeases is not supported by Glue");
    }
}

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

import com.facebook.airlift.concurrent.MoreFutures;
import com.facebook.airlift.log.Logger;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.PartitionNameWithVersion;
import com.facebook.presto.hive.PartitionNotFoundException;
import com.facebook.presto.hive.SchemaAlreadyExistsException;
import com.facebook.presto.hive.TableAlreadyExistsException;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.Database;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.HiveColumnStatistics;
import com.facebook.presto.hive.metastore.HivePrivilegeInfo;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.hive.metastore.MetastoreOperationResult;
import com.facebook.presto.hive.metastore.MetastoreUtil;
import com.facebook.presto.hive.metastore.Partition;
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
import com.facebook.presto.spi.constraints.TableConstraint;
import com.facebook.presto.spi.security.ConnectorIdentity;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.spi.security.RoleGrant;
import com.facebook.presto.spi.statistics.ColumnStatisticType;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import jakarta.annotation.Nullable;
import jakarta.inject.Inject;
import org.apache.hadoop.fs.Path;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.awscore.retry.AwsRetryStrategy;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.metrics.MetricPublisher;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.retries.StandardRetryStrategy;
import software.amazon.awssdk.services.glue.GlueAsyncClient;
import software.amazon.awssdk.services.glue.GlueAsyncClientBuilder;
import software.amazon.awssdk.services.glue.model.AlreadyExistsException;
import software.amazon.awssdk.services.glue.model.BatchCreatePartitionRequest;
import software.amazon.awssdk.services.glue.model.BatchCreatePartitionResponse;
import software.amazon.awssdk.services.glue.model.BatchGetPartitionRequest;
import software.amazon.awssdk.services.glue.model.BatchGetPartitionResponse;
import software.amazon.awssdk.services.glue.model.BatchUpdatePartitionRequest;
import software.amazon.awssdk.services.glue.model.BatchUpdatePartitionRequestEntry;
import software.amazon.awssdk.services.glue.model.BatchUpdatePartitionResponse;
import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.DeleteDatabaseRequest;
import software.amazon.awssdk.services.glue.model.DeletePartitionRequest;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.ErrorDetail;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetDatabaseResponse;
import software.amazon.awssdk.services.glue.model.GetDatabasesRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionResponse;
import software.amazon.awssdk.services.glue.model.GetPartitionsRequest;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;
import software.amazon.awssdk.services.glue.model.GlueException;
import software.amazon.awssdk.services.glue.model.GlueResponse;
import software.amazon.awssdk.services.glue.model.PartitionError;
import software.amazon.awssdk.services.glue.model.PartitionInput;
import software.amazon.awssdk.services.glue.model.PartitionValueList;
import software.amazon.awssdk.services.glue.model.Segment;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.TableInput;
import software.amazon.awssdk.services.glue.model.UpdateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.UpdatePartitionRequest;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static com.facebook.presto.hive.metastore.MetastoreOperationResult.EMPTY_RESULT;
import static com.facebook.presto.hive.metastore.MetastoreUtil.createDirectory;
import static com.facebook.presto.hive.metastore.MetastoreUtil.deleteDirectoryRecursively;
import static com.facebook.presto.hive.metastore.MetastoreUtil.getHiveBasicStatistics;
import static com.facebook.presto.hive.metastore.MetastoreUtil.getPartitionNamesWithEmptyVersion;
import static com.facebook.presto.hive.metastore.MetastoreUtil.isManagedTable;
import static com.facebook.presto.hive.metastore.MetastoreUtil.makePartName;
import static com.facebook.presto.hive.metastore.MetastoreUtil.makePartitionName;
import static com.facebook.presto.hive.metastore.MetastoreUtil.toPartitionValues;
import static com.facebook.presto.hive.metastore.MetastoreUtil.updateStatisticsParameters;
import static com.facebook.presto.hive.metastore.MetastoreUtil.verifyCanDropColumn;
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
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Maps.immutableEntry;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.function.UnaryOperator.identity;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toMap;

public class GlueHiveMetastore
        implements ExtendedHiveMetastore
{
    private static final Logger log = Logger.get(GlueHiveMetastore.class);
    private static final String PUBLIC_ROLE_NAME = "public";
    private static final String DEFAULT_METASTORE_USER = "presto";
    private static final String WILDCARD_EXPRESSION = "";

    // This is the total number of partitions allowed to process in a big batch chunk which splits multiple smaller batch of partitions allowed by BATCH_CREATE_PARTITION_MAX_PAGE_SIZE
    // Here's an example diagram on how async batches are handled for Create Partition:
    // |--------BATCH_CREATE_PARTITION_MAX_PAGE_SIZE------------| ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    // | p0, p1, p2 .....................................   p99 |
    // |--------------------------------------------------------|
    // | p0, p1, p2 .....................................   p99 |
    // |--------------------------------------------------------|
    // BATCH_PARTITION_COMMIT_TOTAL_SIZE / BATCH_CREATE_PARTITION_MAX_PAGE_SIZE ..... (10k/100=100 batches)
    // |--------------------------------------------------------|++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    // | p0, p1, p2 .....................................   p99 |
    // |--------------------------------------------------------|
    // | p0, p1, p2 .....................................   p99 |
    // |--------------------------------------------------------|.......... (100 batches)
    // |--------------------------------------------------------|++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    private static final int BATCH_PARTITION_COMMIT_TOTAL_SIZE = 10000;
    private static final int BATCH_GET_PARTITION_MAX_PAGE_SIZE = 1000;
    // this is the total number of partitions allowed per batch that glue metastore can process to create partitions
    private static final int BATCH_CREATE_PARTITION_MAX_PAGE_SIZE = 100;
    private static final int BATCH_UPDATE_PARTITION_MAX_PAGE_SIZE = 100;
    private static final int AWS_GLUE_GET_PARTITIONS_MAX_RESULTS = 1000;
    private static final Comparator<Partition> PARTITION_COMPARATOR = comparing(Partition::getValues, lexicographical(String.CASE_INSENSITIVE_ORDER));

    private final GlueMetastoreStats stats = new GlueMetastoreStats();
    private final GlueColumnStatisticsProvider columnStatisticsProvider;
    private final boolean enableColumnStatistics;
    private final HdfsEnvironment hdfsEnvironment;
    private final HdfsContext hdfsContext;
    private final GlueAsyncClient glueClient;
    private final Optional<String> defaultDir;
    private final String catalogId;
    private final int partitionSegments;
    private final Executor partitionsReadExecutor;

    @Inject
    public GlueHiveMetastore(
            HdfsEnvironment hdfsEnvironment,
            GlueHiveMetastoreConfig glueConfig,
            @ForGlueHiveMetastore Executor partitionsReadExecutor,
            @ForGlueColumnStatisticsRead Executor statisticsReadExecutor,
            @ForGlueColumnStatisticsWrite Executor statisticsWriteExecutor)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.hdfsContext = new HdfsContext(new ConnectorIdentity(DEFAULT_METASTORE_USER, Optional.empty(), Optional.empty()));
        this.glueClient = createAsyncGlueClient(requireNonNull(glueConfig, "glueConfig is null"), stats.newRequestMetricPublisher());
        this.defaultDir = glueConfig.getDefaultWarehouseDir();
        this.catalogId = glueConfig.getCatalogId().orElse(null);
        this.partitionSegments = glueConfig.getPartitionSegments();
        this.partitionsReadExecutor = requireNonNull(partitionsReadExecutor, "partitionsReadExecutor is null");
        this.enableColumnStatistics = glueConfig.isColumnStatisticsEnabled();
        if (this.enableColumnStatistics) {
            this.columnStatisticsProvider = new DefaultGlueColumnStatisticsProvider(glueClient, catalogId, statisticsReadExecutor, statisticsWriteExecutor, stats);
        }
        else {
            this.columnStatisticsProvider = new DisabledGlueColumnStatisticsProvider();
        }
    }

    private static GlueAsyncClient createAsyncGlueClient(GlueHiveMetastoreConfig config, MetricPublisher metricPublisher)
    {
        NettyNioAsyncHttpClient.Builder nettyBuilder = NettyNioAsyncHttpClient.builder()
                .maxConcurrency(config.getMaxGlueConnections());

        StandardRetryStrategy strategy = AwsRetryStrategy.standardRetryStrategy()
                .toBuilder()
                .maxAttempts(config.getMaxGlueErrorRetries())
                .build();

        ClientOverrideConfiguration.Builder overrideConfigBuilder = ClientOverrideConfiguration.builder()
                .retryStrategy(strategy)
                .addMetricPublisher(metricPublisher);

        GlueAsyncClientBuilder glueAsyncClientBuilder = GlueAsyncClient.builder()
                .httpClientBuilder(nettyBuilder)
                .overrideConfiguration(overrideConfigBuilder.build());

        if (config.getGlueEndpointUrl().isPresent()) {
            checkArgument(config.getGlueRegion().isPresent(), "Glue region must be set when Glue endpoint URL is set");
            glueAsyncClientBuilder
                    .endpointOverride(URI.create(config.getGlueEndpointUrl().get()))
                    .region(Region.of(config.getGlueRegion().get()));
        }
        else if (config.getGlueRegion().isPresent()) {
            glueAsyncClientBuilder.region(Region.of(config.getGlueRegion().get()));
        }

        AwsCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();
        if (config.getAwsAccessKey().isPresent() && config.getAwsSecretKey().isPresent()) {
            credentialsProvider = StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(config.getAwsAccessKey().get(), config.getAwsSecretKey().get()));
        }
        else if (config.getIamRole().isPresent()) {
            StsClientBuilder stsClientBuilder = StsClient.builder()
                    .credentialsProvider(DefaultCredentialsProvider.create());

            if (config.getGlueStsEndpointUrl().isPresent()) {
                checkArgument(config.getGlueStsRegion().isPresent(), "Glue STS region must be set when Glue STS endpoint URL is set");
                stsClientBuilder
                        .endpointOverride(URI.create(config.getGlueStsEndpointUrl().get()))
                        .region(Region.of(config.getGlueStsRegion().get()));
            }
            else if (config.getGlueStsRegion().isPresent()) {
                stsClientBuilder.region(Region.of(config.getGlueStsRegion().get()));
            }

            credentialsProvider = StsAssumeRoleCredentialsProvider.builder()
                    .refreshRequest(() -> AssumeRoleRequest.builder()
                            .roleArn(config.getIamRole().get())
                            .roleSessionName("presto-session").build())
                    .stsClient(stsClientBuilder.build())
                    .build();
        }

        glueAsyncClientBuilder.credentialsProvider(credentialsProvider);

        return glueAsyncClientBuilder.build();
    }

    @Managed
    @Flatten
    public GlueMetastoreStats getStats()
    {
        return stats;
    }

    // For Glue metastore there's an upper bound limit on 100 partitions per batch.
    // Here's the reference: https://docs.aws.amazon.com/glue/latest/webapi/API_BatchCreatePartition.html
    @Override
    public int getPartitionCommitBatchSize()
    {
        return BATCH_PARTITION_COMMIT_TOTAL_SIZE;
    }

    @Override
    public Optional<Database> getDatabase(MetastoreContext metastoreContext, String databaseName)
    {
        try {
            GetDatabaseResponse response = awsSyncRequest(glueClient::getDatabase,
                    GetDatabaseRequest.builder().catalogId(catalogId).name(databaseName).build(),
                    stats.getGetDatabase());

            return Optional.of(GlueToPrestoConverter.convertDatabase(response.database()));
        }
        catch (EntityNotFoundException e) {
            return Optional.empty();
        }
        catch (AwsServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public List<String> getAllDatabases(MetastoreContext metastoreContext)
    {
        try {
            ImmutableList.Builder<String> databaseNames = ImmutableList.builder();

            awsSyncPaginatedRequest(
                    glueClient.getDatabasesPaginator(GetDatabasesRequest.builder().catalogId(catalogId).build()),
                    getDatabasesResponse -> getDatabasesResponse.databaseList().forEach(database -> databaseNames.add(database.name())),
                    stats.getGetDatabases());

            return databaseNames.build();
        }
        catch (AwsServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Optional<Table> getTable(MetastoreContext metastoreContext, String databaseName, String tableName)
    {
        return getGlueTable(databaseName, tableName).map(table -> GlueToPrestoConverter.convertTable(table, databaseName));
    }

    private software.amazon.awssdk.services.glue.model.Table getGlueTableOrElseThrow(String databaseName, String tableName)
    {
        return getGlueTable(databaseName, tableName)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));
    }

    private Optional<software.amazon.awssdk.services.glue.model.Table> getGlueTable(String databaseName, String tableName)
    {
        try {
            GetTableResponse response = awsSyncRequest(
                    glueClient::getTable,
                    GetTableRequest.builder().catalogId(catalogId).databaseName(databaseName).name(tableName).build(),
                    stats.getGetTable());

            return Optional.of(response.table());
        }
        catch (EntityNotFoundException e) {
            return Optional.empty();
        }
        catch (AwsServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Set<ColumnStatisticType> getSupportedColumnStatistics(MetastoreContext metastoreContext, Type type)
    {
        return columnStatisticsProvider.getSupportedColumnStatistics(type);
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
        return new PartitionStatistics(getHiveBasicStatistics(table.getParameters()), columnStatisticsProvider.getTableColumnStatistics(table));
    }

    @Override
    public Map<String, PartitionStatistics> getPartitionStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, Set<String> partitionNames)
    {
        Table table = getTable(metastoreContext, databaseName, tableName)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));
        return getPartitionStatistics(metastoreContext, getExistingPartitionsByNames(metastoreContext, databaseName, tableName, ImmutableList.copyOf(getPartitionNamesWithEmptyVersion(partitionNames))))
                .entrySet()
                .stream()
                .collect(toImmutableMap(
                        entry -> makePartitionName(table, entry.getKey()),
                        Entry::getValue));
    }

    private List<Partition> getExistingPartitionsByNames(MetastoreContext metastoreContext, String databaseName, String tableName, List<PartitionNameWithVersion> partitionNames)
    {
        Map<String, Partition> partitions = getPartitionsByNames(metastoreContext, databaseName, tableName, partitionNames).entrySet().stream()
                .map(entry -> immutableEntry(entry.getKey(), entry.getValue().orElseThrow(() ->
                        new PartitionNotFoundException(new SchemaTableName(databaseName, tableName), toPartitionValues(entry.getKey())))))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

        return (MetastoreUtil.getPartitionNames(partitionNames)).stream()
                .map(partitions::get)
                .collect(toImmutableList());
    }

    private Map<Partition, PartitionStatistics> getPartitionStatistics(MetastoreContext metastoreContext, Collection<Partition> partitions)
    {
        return columnStatisticsProvider.getPartitionColumnStatistics(partitions).entrySet().stream()
                .collect(toImmutableMap(
                        Entry::getKey,
                        entry -> new PartitionStatistics(getHiveBasicStatistics(entry.getKey().getParameters()), entry.getValue())));
    }

    @Override
    public void updateTableStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        PartitionStatistics currentStatistics = getTableStatistics(metastoreContext, databaseName, tableName);
        PartitionStatistics updatedStatistics = update.apply(currentStatistics);

        Table table = getTableOrElseThrow(metastoreContext, databaseName, tableName);

        try {
            TableInput tableInput = GlueInputConverter.convertTable(table);
            final Map<String, String> statisticsParameters =
                    updateStatisticsParameters(table.getParameters(), updatedStatistics.getBasicStatistics());
            table = Table.builder(table).setParameters(statisticsParameters).build();

            awsSyncRequest(
                    glueClient::updateTable,
                    UpdateTableRequest.builder()
                            .catalogId(catalogId)
                            .databaseName(databaseName)
                            .tableInput(tableInput.toBuilder().parameters(statisticsParameters).build())
                            .build(),
                    stats.getUpdateTable());

            columnStatisticsProvider.updateTableColumnStatistics(table, updatedStatistics.getColumnStatistics());
        }
        catch (EntityNotFoundException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (AwsServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public void updatePartitionStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, Map<String, Function<PartitionStatistics, PartitionStatistics>> updates)
    {
        Iterables.partition(updates.entrySet(), BATCH_CREATE_PARTITION_MAX_PAGE_SIZE).forEach(partitionUpdates ->
                updatePartitionStatisticsBatch(metastoreContext, databaseName, tableName, partitionUpdates.stream().collect(toImmutableMap(Entry::getKey, Entry::getValue))));
    }

    private void updatePartitionStatisticsBatch(MetastoreContext metastoreContext, String databaseName, String tableName, Map<String, Function<PartitionStatistics, PartitionStatistics>> updates)
    {
        ImmutableList.Builder<BatchUpdatePartitionRequestEntry> partitionUpdateRequests = ImmutableList.builder();
        ImmutableSet.Builder<GlueColumnStatisticsProvider.PartitionStatisticsUpdate> columnStatisticsUpdates = ImmutableSet.builder();

        Map<List<String>, String> partitionValuesToName = updates.keySet().stream()
                .collect(toImmutableMap(MetastoreUtil::toPartitionValues, identity()));

        List<Partition> partitions = batchGetPartition(databaseName, tableName, ImmutableList.copyOf(updates.keySet()));
        Map<Partition, Map<String, HiveColumnStatistics>> statisticsPerPartition = columnStatisticsProvider.getPartitionColumnStatistics(partitions);

        statisticsPerPartition.forEach((partition, columnStatistics) -> {
            Function<PartitionStatistics, PartitionStatistics> update = updates.get(partitionValuesToName.get(partition.getValues()));

            PartitionStatistics currentStatistics = new PartitionStatistics(getHiveBasicStatistics(partition.getParameters()), columnStatistics);
            PartitionStatistics updatedStatistics = update.apply(currentStatistics);

            Map<String, String> updatedStatisticsParameters = updateStatisticsParameters(partition.getParameters(), updatedStatistics.getBasicStatistics());

            partition = Partition.builder(partition).setParameters(updatedStatisticsParameters).build();
            Map<String, HiveColumnStatistics> updatedColumnStatistics = updatedStatistics.getColumnStatistics();

            PartitionInput partitionInput = GlueInputConverter.convertPartition(partition);
            partitionUpdateRequests.add(
                    BatchUpdatePartitionRequestEntry.builder()
                            .partitionValueList(partition.getValues())
                            .partitionInput(partitionInput.toBuilder().parameters(partition.getParameters()).build())
                            .build());
            columnStatisticsUpdates.add(new GlueColumnStatisticsProvider.PartitionStatisticsUpdate(partition, updatedColumnStatistics));
        });

        List<List<BatchUpdatePartitionRequestEntry>> partitionUpdateRequestsPartitioned = Lists.partition(partitionUpdateRequests.build(), BATCH_UPDATE_PARTITION_MAX_PAGE_SIZE);
        List<CompletableFuture<BatchUpdatePartitionResponse>> partitionUpdateRequestsFutures = new ArrayList<>();

        partitionUpdateRequestsPartitioned.forEach(partitionUpdateRequestsPartition -> {
            GlueStatsAsyncHandler glueStatsAsyncHandler = new GlueStatsAsyncHandler(stats.getBatchUpdatePartitions());

            partitionUpdateRequestsFutures.add(glueClient.batchUpdatePartition(
                    BatchUpdatePartitionRequest.builder()
                            .catalogId(catalogId)
                            .databaseName(databaseName)
                            .tableName(tableName)
                            .entries(partitionUpdateRequestsPartition)
                            .build())
                    .whenCompleteAsync((response, exception) -> {
                        if (response != null) {
                            glueStatsAsyncHandler.onSuccess(response);
                        }
                        else if (exception != null) {
                            glueStatsAsyncHandler.onError(exception);
                        }
                    }));
        });

        try {
            columnStatisticsProvider.updatePartitionStatistics(columnStatisticsUpdates.build());
            partitionUpdateRequestsFutures.forEach(MoreFutures::getFutureValue);
        }
        catch (AwsServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Optional<List<String>> getAllTables(MetastoreContext metastoreContext, String databaseName)
    {
        try {
            ImmutableList.Builder<String> tableNames = ImmutableList.builder();

            awsSyncPaginatedRequest(
                    glueClient.getTablesPaginator(GetTablesRequest.builder().catalogId(catalogId).databaseName(databaseName).build()),
                    getTablesResponse -> {
                        getTablesResponse.tableList().stream()
                                .map(software.amazon.awssdk.services.glue.model.Table::name)
                                .forEach(tableNames::add);
                    },
                    stats.getGetTables());

            return Optional.of(tableNames.build());
        }
        catch (EntityNotFoundException e) {
            // database does not exist
            return Optional.empty();
        }
        catch (AwsServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Optional<List<String>> getAllViews(MetastoreContext metastoreContext, String databaseName)
    {
        try {
            ImmutableList.Builder<String> viewNames = ImmutableList.builder();

            awsSyncPaginatedRequest(
                    glueClient.getTablesPaginator(GetTablesRequest.builder().catalogId(catalogId).databaseName(databaseName).build()),
                    getTablesResponse -> {
                        getTablesResponse.tableList().stream()
                                .filter(table -> VIRTUAL_VIEW.name().equals(table.tableType()))
                                .map(software.amazon.awssdk.services.glue.model.Table::name)
                                .forEach(viewNames::add);
                    },
                    stats.getGetTables());

            return Optional.of(viewNames.build());
        }
        catch (EntityNotFoundException e) {
            // database does not exist
            return Optional.empty();
        }
        catch (AwsServiceException e) {
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
            awsSyncRequest(
                    glueClient::createDatabase,
                    CreateDatabaseRequest.builder().catalogId(catalogId).databaseInput(databaseInput).build(),
                    stats.getCreateDatabase());
        }
        catch (AlreadyExistsException e) {
            throw new SchemaAlreadyExistsException(database.getDatabaseName());
        }
        catch (AwsServiceException e) {
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
            awsSyncRequest(
                    glueClient::deleteDatabase,
                    DeleteDatabaseRequest.builder().catalogId(catalogId).name(databaseName).build(),
                    stats.getDeleteDatabase());
        }
        catch (EntityNotFoundException e) {
            throw new SchemaNotFoundException(databaseName);
        }
        catch (AwsServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public void renameDatabase(MetastoreContext metastoreContext, String databaseName, String newDatabaseName)
    {
        try {
            Database database = getDatabase(metastoreContext, databaseName).orElseThrow(() -> new SchemaNotFoundException(databaseName));
            DatabaseInput renamedDatabase = GlueInputConverter.convertDatabase(database);

            awsSyncRequest(
                    glueClient::updateDatabase,
                    UpdateDatabaseRequest.builder()
                            .catalogId(catalogId)
                            .name(databaseName)
                            .databaseInput(renamedDatabase.toBuilder().name(newDatabaseName).build())
                            .build(),
                    stats.getUpdateDatabase());
        }
        catch (AwsServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public MetastoreOperationResult createTable(MetastoreContext metastoreContext, Table table, PrincipalPrivileges principalPrivileges, List<TableConstraint<String>> constraints)
    {
        if (constraints != null & !constraints.isEmpty()) {
            throw new PrestoException(NOT_SUPPORTED, "Glue metastore does not support table constraints");
        }

        try {
            TableInput input = GlueInputConverter.convertTable(table);
            awsSyncRequest(
                    glueClient::createTable,
                    CreateTableRequest.builder()
                            .catalogId(catalogId)
                            .databaseName(table.getDatabaseName())
                            .tableInput(input)
                            .build(),
                    stats.getCreateTable());
        }
        catch (AlreadyExistsException e) {
            throw new TableAlreadyExistsException(new SchemaTableName(table.getDatabaseName(), table.getTableName()));
        }
        catch (EntityNotFoundException e) {
            throw new SchemaNotFoundException(table.getDatabaseName());
        }
        catch (AwsServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }

        return EMPTY_RESULT;
    }

    @Override
    public void dropTable(MetastoreContext metastoreContext, String databaseName, String tableName, boolean deleteData)
    {
        Table table = getTableOrElseThrow(metastoreContext, databaseName, tableName);

        try {
            awsSyncRequest(
                    glueClient::deleteTable,
                    DeleteTableRequest.builder()
                            .catalogId(catalogId)
                            .databaseName(databaseName)
                            .name(tableName)
                            .build(),
                    stats.getDeleteTable());
        }
        catch (AwsServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }

        String tableLocation = table.getStorage().getLocation();
        if (deleteData && isManagedTable(table.getTableType().name()) && !isNullOrEmpty(tableLocation)) {
            deleteDirectoryRecursively(hdfsContext, hdfsEnvironment, new Path(tableLocation), true);
        }
    }

    @Override
    public MetastoreOperationResult replaceTable(MetastoreContext metastoreContext, String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges)
    {
        try {
            TableInput newTableInput = GlueInputConverter.convertTable(newTable);

            awsSyncRequest(
                    glueClient::updateTable,
                    UpdateTableRequest.builder()
                            .catalogId(catalogId)
                            .databaseName(databaseName)
                            .tableInput(newTableInput)
                            .build(),
                    stats.getUpdateTable());

            return EMPTY_RESULT;
        }
        catch (EntityNotFoundException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (AwsServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    public MetastoreOperationResult persistTable(MetastoreContext metastoreContext, String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges, Supplier<PartitionStatistics> update, Map<String, String> additionalParameters)
    {
        PartitionStatistics updatedStatistics = update.get();
        if (!updatedStatistics.getColumnStatistics().isEmpty()) {
            throw new PrestoException(NOT_SUPPORTED, "Glue metastore does not support column level statistics");
        }
        try {
            TableInput newTableInput = GlueInputConverter.convertTable(newTable);
            final Map<String, String> statisticsParameters =
                    updateStatisticsParameters(newTableInput.parameters(), updatedStatistics.getBasicStatistics());

            awsSyncRequest(
                    glueClient::updateTable,
                    UpdateTableRequest.builder()
                            .catalogId(catalogId)
                            .databaseName(databaseName)
                            .tableInput(newTableInput.toBuilder().parameters(statisticsParameters).build())
                            .build(),
                    stats.getUpdateTable());

            return EMPTY_RESULT;
        }
        catch (EntityNotFoundException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (AwsServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public MetastoreOperationResult renameTable(MetastoreContext metastoreContext, String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        throw new PrestoException(NOT_SUPPORTED, "Table rename is not yet supported by Glue service");
    }

    @Override
    public MetastoreOperationResult addColumn(MetastoreContext metastoreContext, String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        software.amazon.awssdk.services.glue.model.Table table = getGlueTableOrElseThrow(databaseName, tableName);
        ImmutableList.Builder<software.amazon.awssdk.services.glue.model.Column> newDataColumns = ImmutableList.builder();
        newDataColumns.addAll(table.storageDescriptor().columns());
        newDataColumns.add(convertColumn(new Column(columnName, columnType, Optional.ofNullable(columnComment), Optional.empty())));
        StorageDescriptor newStorageDescriptor = table.storageDescriptor().toBuilder().columns(newDataColumns.build()).build();
        replaceGlueTable(databaseName, tableName, table.toBuilder().storageDescriptor(newStorageDescriptor).build());
        return EMPTY_RESULT;
    }

    @Override
    public MetastoreOperationResult renameColumn(MetastoreContext metastoreContext, String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        software.amazon.awssdk.services.glue.model.Table table = getGlueTableOrElseThrow(databaseName, tableName);
        if (table.partitionKeys() != null && table.partitionKeys().stream().anyMatch(c -> c.name().equals(oldColumnName))) {
            throw new PrestoException(NOT_SUPPORTED, "Renaming partition columns is not supported");
        }
        ImmutableList.Builder<software.amazon.awssdk.services.glue.model.Column> newDataColumns = ImmutableList.builder();
        for (software.amazon.awssdk.services.glue.model.Column column : table.storageDescriptor().columns()) {
            if (column.name().equals(oldColumnName)) {
                newDataColumns.add(software.amazon.awssdk.services.glue.model.Column.builder()
                        .name(newColumnName)
                        .type(column.type())
                        .comment(column.comment())
                        .build());
            }
            else {
                newDataColumns.add(column);
            }
        }

        StorageDescriptor newStorageDescriptor = table.storageDescriptor().toBuilder().columns(newDataColumns.build()).build();
        replaceGlueTable(databaseName, tableName, table.toBuilder().storageDescriptor(newStorageDescriptor).build());
        return EMPTY_RESULT;
    }

    @Override
    public MetastoreOperationResult dropColumn(MetastoreContext metastoreContext, String databaseName, String tableName, String columnName)
    {
        verifyCanDropColumn(this, metastoreContext, databaseName, tableName, columnName);
        software.amazon.awssdk.services.glue.model.Table table = getGlueTableOrElseThrow(databaseName, tableName);

        ImmutableList.Builder<software.amazon.awssdk.services.glue.model.Column> newDataColumns = ImmutableList.builder();
        boolean found = false;
        for (software.amazon.awssdk.services.glue.model.Column column : table.storageDescriptor().columns()) {
            if (column.name().equals(columnName)) {
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

        StorageDescriptor newStorageDescriptor = table.storageDescriptor().toBuilder().columns(newDataColumns.build()).build();
        replaceGlueTable(databaseName, tableName, table.toBuilder().storageDescriptor(newStorageDescriptor).build());

        return EMPTY_RESULT;
    }

    private void replaceGlueTable(String databaseName, String tableName, software.amazon.awssdk.services.glue.model.Table newTable)
    {
        try {
            awsSyncRequest(
                    glueClient::updateTable,
                    UpdateTableRequest.builder()
                            .catalogId(catalogId)
                            .databaseName(databaseName)
                            .tableInput(toTableInput(newTable))
                            .build(),
                    stats.getUpdateTable());
        }
        catch (EntityNotFoundException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (AwsServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Optional<Partition> getPartition(MetastoreContext metastoreContext, String databaseName, String tableName, List<String> partitionValues)
    {
        try {
            GetPartitionResponse response = awsSyncRequest(
                    glueClient::getPartition,
                    GetPartitionRequest.builder()
                            .catalogId(catalogId)
                            .databaseName(databaseName)
                            .tableName(tableName)
                            .partitionValues(partitionValues)
                            .build(),
                    stats.getGetPartition());

            return Optional.of(new GluePartitionConverter(databaseName, tableName).apply(response.partition()));
        }
        catch (EntityNotFoundException e) {
            return Optional.empty();
        }
        catch (AwsServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Optional<List<PartitionNameWithVersion>> getPartitionNames(MetastoreContext metastoreContext, String databaseName, String tableName)
    {
        Table table = getTableOrElseThrow(metastoreContext, databaseName, tableName);
        List<Partition> partitions = getPartitions(databaseName, tableName, WILDCARD_EXPRESSION);
        return Optional.of(getPartitionNamesWithEmptyVersion(buildPartitionNames(table.getPartitionColumns(), partitions)));
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
    public List<PartitionNameWithVersion> getPartitionNamesByFilter(
            MetastoreContext metastoreContext,
            String databaseName,
            String tableName,
            Map<Column, Domain> partitionPredicates)
    {
        Table table = getTableOrElseThrow(metastoreContext, databaseName, tableName);
        String expression = buildGlueExpression(partitionPredicates);
        List<Partition> partitions = getPartitions(databaseName, tableName, expression);
        return getPartitionNamesWithEmptyVersion(buildPartitionNames(table.getPartitionColumns(), partitions));
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
        CompletionService<List<Partition>> completionService = new ExecutorCompletionService<>(partitionsReadExecutor);
        for (int i = 0; i < partitionSegments; i++) {
            Segment segment = Segment.builder().segmentNumber(i).totalSegments(partitionSegments).build();
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

            ImmutableList.Builder<Partition> partitionBuilder = ImmutableList.builder();

            GetPartitionsRequest partitionsRequest = GetPartitionsRequest.builder()
                    .catalogId(catalogId)
                    .databaseName(databaseName)
                    .tableName(tableName)
                    .expression(expression)
                    .segment(segment)
                    .maxResults(AWS_GLUE_GET_PARTITIONS_MAX_RESULTS)
                    .build();

            awsSyncPaginatedRequest(
                    glueClient.getPartitionsPaginator(partitionsRequest),
                    getPartitionsResponse -> {
                        getPartitionsResponse.partitions().stream()
                                .map(converter)
                                .forEach(partitionBuilder::add);
                    },
                    stats.getGetPartitions());

            return partitionBuilder.build();
        }
        catch (AwsServiceException e) {
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
     * @param partitionNamesWithVersion List of full partition names
     * @return Mapping of partition name to partition object
     */
    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(MetastoreContext metastoreContext, String databaseName, String tableName, List<PartitionNameWithVersion> partitionNamesWithVersion)
    {
        requireNonNull(partitionNamesWithVersion, "partitionNames is null");
        if (partitionNamesWithVersion.isEmpty()) {
            return ImmutableMap.of();
        }

        List<String> partitionNames = MetastoreUtil.getPartitionNames(partitionNamesWithVersion);
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
        List<CompletableFuture<BatchGetPartitionResponse>> batchGetPartitionFutures = new ArrayList<>();
        try {
            List<PartitionValueList> pendingPartitions = partitionNames.stream()
                    .map(partitionName -> PartitionValueList.builder().values(toPartitionValues(partitionName)).build())
                    .collect(toCollection(ArrayList::new));

            ImmutableList.Builder<Partition> resultsBuilder = ImmutableList.builderWithExpectedSize(partitionNames.size());

            GluePartitionConverter converter = new GluePartitionConverter(databaseName, tableName);

            while (!pendingPartitions.isEmpty()) {
                for (List<PartitionValueList> partitions : Lists.partition(pendingPartitions, BATCH_GET_PARTITION_MAX_PAGE_SIZE)) {
                    GlueStatsAsyncHandler glueStatsAsyncHandler = new GlueStatsAsyncHandler(stats.getBatchGetPartitions());

                    batchGetPartitionFutures.add(glueClient.batchGetPartition(BatchGetPartitionRequest.builder()
                                    .catalogId(catalogId)
                                    .databaseName(databaseName)
                                    .tableName(tableName)
                                    .partitionsToGet(partitions)
                                    .build())
                            .whenCompleteAsync((response, exception) -> {
                                if (response != null) {
                                    glueStatsAsyncHandler.onSuccess(response);
                                }
                                else if (exception != null) {
                                    glueStatsAsyncHandler.onError(exception);
                                }
                            }));
                }
                pendingPartitions.clear();

                for (Future<BatchGetPartitionResponse> futureResponse : batchGetPartitionFutures) {
                    BatchGetPartitionResponse batchGetPartitionResponse = futureResponse.get();
                    List<software.amazon.awssdk.services.glue.model.Partition> partitions = batchGetPartitionResponse.partitions();
                    List<PartitionValueList> unprocessedKeys = batchGetPartitionResponse.unprocessedKeys();

                    if (partitions.isEmpty() && unprocessedKeys.isEmpty()) {
                        log.warn("Empty unprocessedKeys for non-empty BatchGetPartitionRequest and empty partitions result");
                    }

                    partitions.stream()
                            .map(converter)
                            .forEach(resultsBuilder::add);
                    pendingPartitions.addAll(unprocessedKeys);
                }
                batchGetPartitionFutures.clear();
            }

            return resultsBuilder.build();
        }
        catch (AwsServiceException | InterruptedException | ExecutionException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        finally {
            // Ensure any futures still running are canceled in case of failure
            batchGetPartitionFutures.forEach(future -> future.cancel(true));
        }
    }

    @Override
    public MetastoreOperationResult addPartitions(MetastoreContext metastoreContext, String databaseName, String tableName, List<PartitionWithStatistics> partitions)
    {
        try {
            List<CompletableFuture<BatchCreatePartitionResponse>> futures = new ArrayList<>();

            for (List<PartitionWithStatistics> partitionBatch : Lists.partition(partitions, BATCH_CREATE_PARTITION_MAX_PAGE_SIZE)) {
                List<PartitionInput> partitionInputs = mappedCopy(partitionBatch, GlueInputConverter::convertPartition);

                GlueStatsAsyncHandler asyncHandler = new GlueStatsAsyncHandler(stats.getBatchCreatePartitions());

                futures.add(glueClient.batchCreatePartition(BatchCreatePartitionRequest.builder()
                                .catalogId(catalogId)
                                .databaseName(databaseName)
                                .tableName(tableName)
                                .partitionInputList(partitionInputs)
                                .build())
                        .whenCompleteAsync((response, exception) -> {
                            if (response != null) {
                                asyncHandler.onSuccess(response);
                            }
                            else if (exception != null) {
                                asyncHandler.onError(exception);
                            }
                        }));
            }

            for (Future<BatchCreatePartitionResponse> future : futures) {
                BatchCreatePartitionResponse result = future.get();
                propagatePartitionErrorToPrestoException(databaseName, tableName, result.errors());
            }

            Set<GlueColumnStatisticsProvider.PartitionStatisticsUpdate> updates = partitions.stream()
                    .map(partitionWithStatistics -> new GlueColumnStatisticsProvider.PartitionStatisticsUpdate(
                            partitionWithStatistics.getPartition(),
                            partitionWithStatistics.getStatistics().getColumnStatistics()))
                    .collect(toImmutableSet());
            columnStatisticsProvider.updatePartitionStatistics(updates);

            return EMPTY_RESULT;
        }
        catch (AwsServiceException | InterruptedException | ExecutionException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    private static void propagatePartitionErrorToPrestoException(String databaseName, String tableName, List<PartitionError> partitionErrors)
    {
        if (partitionErrors != null && !partitionErrors.isEmpty()) {
            ErrorDetail errorDetail = partitionErrors.get(0).errorDetail();
            String glueExceptionCode = errorDetail.errorCode();

            switch (glueExceptionCode) {
                case "AlreadyExistsException":
                    throw new PrestoException(ALREADY_EXISTS, errorDetail.errorMessage());
                case "EntityNotFoundException":
                    throw new TableNotFoundException(new SchemaTableName(databaseName, tableName), errorDetail.errorMessage());
                default:
                    throw new PrestoException(HIVE_METASTORE_ERROR, errorDetail.errorCode() + ": " + errorDetail.errorMessage());
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
            awsSyncRequest(
                    glueClient::deletePartition,
                    DeletePartitionRequest.builder()
                            .catalogId(catalogId)
                            .databaseName(databaseName)
                            .tableName(tableName)
                            .partitionValues(parts)
                            .build(),
                    stats.getDeletePartition());
        }
        catch (AwsServiceException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }

        String partLocation = partition.getStorage().getLocation();
        if (deleteData && isManagedTable(table.getTableType().name()) && !isNullOrEmpty(partLocation)) {
            deleteDirectoryRecursively(hdfsContext, hdfsEnvironment, new Path(partLocation), true);
        }
    }

    @Override
    public MetastoreOperationResult alterPartition(MetastoreContext metastoreContext, String databaseName, String tableName, PartitionWithStatistics partition)
    {
        try {
            PartitionInput newPartition = GlueInputConverter.convertPartition(partition);

            awsSyncRequest(
                    glueClient::updatePartition,
                    UpdatePartitionRequest.builder()
                            .catalogId(catalogId)
                            .databaseName(databaseName)
                            .tableName(tableName)
                            .partitionInput(newPartition)
                            .partitionValueList(partition.getPartition().getValues())
                            .build(),
                    stats.getUpdatePartition());

            columnStatisticsProvider.updatePartitionStatistics(partition.getPartition(), partition.getStatistics().getColumnStatistics());

            return EMPTY_RESULT;
        }
        catch (EntityNotFoundException e) {
            throw new PartitionNotFoundException(new SchemaTableName(databaseName, tableName), partition.getPartition().getValues());
        }
        catch (AwsServiceException e) {
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

    public Optional<Long> lock(MetastoreContext metastoreContext, String databaseName, String tableName)
    {
        return Optional.empty();
    }

    public void unlock(MetastoreContext metastoreContext, long lockId)
    {
        //No-op
    }

    @Override
    public MetastoreOperationResult dropConstraint(MetastoreContext metastoreContext, String databaseName, String tableName, String constraintName)
    {
        throw new PrestoException(NOT_SUPPORTED, "dropConstraint is not supported by Glue");
    }

    @Override
    public MetastoreOperationResult addConstraint(MetastoreContext metastoreContext, String databaseName, String tableName, TableConstraint<String> tableConstraint)
    {
        throw new PrestoException(NOT_SUPPORTED, "addConstraint is not supported by Glue");
    }

    public static <R, T> T awsSyncRequest(
            Function<R, CompletableFuture<T>> submission,
            R request,
            GlueCatalogApiStats stats)
    {
        requireNonNull(submission, "submission is null");
        requireNonNull(request, "request is null");

        try {
            if (stats != null) {
                return stats.record(() -> submission.apply(request).join());
            }

            return submission.apply(request).join();
        }
        catch (CompletionException e) {
            if (e.getCause() instanceof GlueException) {
                throw (GlueException) e.getCause();
            }
            throw new PrestoException(HIVE_METASTORE_ERROR, e.getCause());
        }
    }

    private static <T> void awsSyncPaginatedRequest(
            SdkPublisher<T> paginator,
            Consumer<T> resultConsumer,
            GlueCatalogApiStats stats)
    {
        requireNonNull(paginator, "paginator is null");
        requireNonNull(resultConsumer, "resultConsumer is null");

        // Single join point so exception handling is consistent, and stats (when present)
        // cover the full wall-clock time of the paginated request including completion.
        Runnable paginationTask = () -> paginator.subscribe(resultConsumer).join();

        try {
            if (stats != null) {
                stats.record(() -> {
                    paginationTask.run();
                    return null;
                });
            }
            else {
                paginationTask.run();
            }
        }
        catch (CompletionException e) {
            if (e.getCause() instanceof GlueException) {
                throw (GlueException) e.getCause();
            }
            throw new PrestoException(HIVE_METASTORE_ERROR, e.getCause());
        }
    }

    static class GlueStatsAsyncHandler
    {
        private final GlueCatalogApiStats stats;
        private final Stopwatch stopwatch;

        public GlueStatsAsyncHandler(GlueCatalogApiStats stats)
        {
            this.stats = requireNonNull(stats, "stats is null");
            this.stopwatch = Stopwatch.createStarted();
        }

        public void onError(Throwable e)
        {
            stats.recordAsync(stopwatch.elapsed(NANOSECONDS), true);
        }

        public void onSuccess(GlueResponse response)
        {
            stats.recordAsync(stopwatch.elapsed(NANOSECONDS), false);
        }
    }
}

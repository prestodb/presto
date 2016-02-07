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
package com.facebook.presto.hive;

import com.facebook.presto.GroupByHashPageIndexerFactory;
import com.facebook.presto.hadoop.HadoopFileStatus;
import com.facebook.presto.hive.metastore.CachingHiveMetastore;
import com.facebook.presto.hive.metastore.HiveMetastore;
import com.facebook.presto.hive.orc.OrcPageSource;
import com.facebook.presto.hive.parquet.ParquetHiveRecordCursor;
import com.facebook.presto.hive.rcfile.RcFilePageSource;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.ConnectorViewDefinition;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.DiscretePredicates;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordPageSource;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.ViewNotFoundException;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.predicate.ValueSet;
import com.facebook.presto.spi.type.SqlDate;
import com.facebook.presto.spi.type.SqlTimestamp;
import com.facebook.presto.spi.type.SqlVarbinary;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.TestingConnectorSession;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.MapType;
import com.facebook.presto.type.TypeRegistry;
import com.facebook.presto.util.ImmutableCollectors;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.net.HostAndPort;
import com.google.common.primitives.Ints;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.units.Duration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_PARTITION_VALUE;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static com.facebook.presto.hive.HiveMetadata.convertToPredicate;
import static com.facebook.presto.hive.HiveStorageFormat.DWRF;
import static com.facebook.presto.hive.HiveStorageFormat.ORC;
import static com.facebook.presto.hive.HiveStorageFormat.PARQUET;
import static com.facebook.presto.hive.HiveStorageFormat.RCBINARY;
import static com.facebook.presto.hive.HiveStorageFormat.RCTEXT;
import static com.facebook.presto.hive.HiveStorageFormat.SEQUENCEFILE;
import static com.facebook.presto.hive.HiveStorageFormat.TEXTFILE;
import static com.facebook.presto.hive.HiveTableProperties.PARTITIONED_BY_PROPERTY;
import static com.facebook.presto.hive.HiveTableProperties.STORAGE_FORMAT_PROPERTY;
import static com.facebook.presto.hive.HiveTestUtils.SESSION;
import static com.facebook.presto.hive.HiveTestUtils.TYPE_MANAGER;
import static com.facebook.presto.hive.HiveTestUtils.getDefaultHiveDataStreamFactories;
import static com.facebook.presto.hive.HiveTestUtils.getDefaultHiveRecordCursorProvider;
import static com.facebook.presto.hive.HiveTestUtils.getTypes;
import static com.facebook.presto.hive.HiveType.HIVE_INT;
import static com.facebook.presto.hive.HiveType.HIVE_STRING;
import static com.facebook.presto.hive.HiveUtil.annotateColumnComment;
import static com.facebook.presto.hive.util.Types.checkType;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static com.facebook.presto.spi.type.StandardTypes.ARRAY;
import static com.facebook.presto.spi.type.StandardTypes.MAP;
import static com.facebook.presto.spi.type.StandardTypes.ROW;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.MaterializedResult.materializeSourceDataStream;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.uniqueIndex;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(groups = "hive")
public abstract class AbstractTestHiveClient
{
    protected static final String INVALID_DATABASE = "totally_invalid_database_name";
    protected static final String INVALID_TABLE = "totally_invalid_table_name";
    protected static final String INVALID_COLUMN = "totally_invalid_column_name";

    private static final Type ARRAY_TYPE = TYPE_MANAGER.getParameterizedType(ARRAY, ImmutableList.of(VARCHAR.getTypeSignature()), ImmutableList.of());
    private static final Type MAP_TYPE = TYPE_MANAGER.getParameterizedType(MAP, ImmutableList.of(VARCHAR.getTypeSignature(), BIGINT.getTypeSignature()), ImmutableList.of());
    private static final Type ROW_TYPE = TYPE_MANAGER.getParameterizedType(
            ROW,
            ImmutableList.of(VARCHAR.getTypeSignature(), BIGINT.getTypeSignature(), BOOLEAN.getTypeSignature()),
            ImmutableList.of("f_string", "f_bigint", "f_boolean"));

    private static final List<ColumnMetadata> CREATE_TABLE_COLUMNS = ImmutableList.<ColumnMetadata>builder()
            .add(new ColumnMetadata("id", BIGINT))
            .add(new ColumnMetadata("t_string", VARCHAR))
            .add(new ColumnMetadata("t_bigint", BIGINT))
            .add(new ColumnMetadata("t_double", DOUBLE))
            .add(new ColumnMetadata("t_boolean", BOOLEAN))
            .add(new ColumnMetadata("t_array", ARRAY_TYPE))
            .add(new ColumnMetadata("t_map", MAP_TYPE))
            .add(new ColumnMetadata("t_row", ROW_TYPE))
            .build();

    private static final MaterializedResult CREATE_TABLE_DATA = MaterializedResult.resultBuilder(SESSION, BIGINT, VARCHAR, BIGINT, DOUBLE, BOOLEAN, ARRAY_TYPE, MAP_TYPE, ROW_TYPE)
            .row(1, "hello", 123, 43.5, true, ImmutableList.of("apple", "banana"), ImmutableMap.of("one", 1L, "two", 2L), ImmutableList.of("true", 1, true))
            .row(2, null, null, null, null, null, null, null)
            .row(3, "bye", 456, 98.1, false, ImmutableList.of("ape", "bear"), ImmutableMap.of("three", 3L, "four", 4L), ImmutableList.of("false", 0, false))
            .build();

    private static final List<ColumnMetadata> CREATE_TABLE_COLUMNS_PARTITIONED = ImmutableList.<ColumnMetadata>builder()
            .addAll(CREATE_TABLE_COLUMNS)
            .add(new ColumnMetadata("ds", VARCHAR))
            .build();

    private static final MaterializedResult CREATE_TABLE_PARTITIONED_DATA = new MaterializedResult(
            CREATE_TABLE_DATA.getMaterializedRows().stream()
                    .map(row -> new MaterializedRow(row.getPrecision(), newArrayList(concat(row.getFields(), ImmutableList.of("2015-07-0" + row.getField(0))))))
                    .collect(toList()),
            ImmutableList.<Type>builder()
                    .addAll(CREATE_TABLE_DATA.getTypes())
                    .add(VARCHAR)
                    .build());

    private static final MaterializedResult CREATE_TABLE_PARTITIONED_DATA_2ND = MaterializedResult.resultBuilder(SESSION, BIGINT, VARCHAR, BIGINT, DOUBLE, BOOLEAN, ARRAY_TYPE, MAP_TYPE, ROW_TYPE, VARCHAR)
            .row(4, "hello", 123, 43.5, true, ImmutableList.of("apple", "banana"), ImmutableMap.of("one", 1L, "two", 2L), ImmutableList.of("true", 1, true), "2015-07-04")
            .row(5, null, null, null, null, null, null, null, "2015-07-04")
            .row(6, "bye", 456, 98.1, false, ImmutableList.of("ape", "bear"), ImmutableMap.of("three", 3L, "four", 4L), ImmutableList.of("false", 0, false), "2015-07-04")
            .build();

    protected Set<HiveStorageFormat> createTableFormats = ImmutableSet.copyOf(HiveStorageFormat.values());

    protected String clientId;
    protected String database;
    protected SchemaTableName tablePartitionFormat;
    protected SchemaTableName tableUnpartitioned;
    protected SchemaTableName tableOffline;
    protected SchemaTableName tableOfflinePartition;
    protected SchemaTableName view;
    protected SchemaTableName invalidTable;
    protected SchemaTableName tableBucketedStringInt;
    protected SchemaTableName tableBucketedBigintBoolean;
    protected SchemaTableName tableBucketedDoubleFloat;
    protected SchemaTableName tablePartitionSchemaChange;
    protected SchemaTableName tablePartitionSchemaChangeNonCanonical;

    protected SchemaTableName temporaryCreateTable;
    protected SchemaTableName temporaryCreateRollbackTable;
    protected SchemaTableName temporaryCreateSampledTable;
    protected SchemaTableName temporaryCreateEmptyTable;
    protected SchemaTableName temporaryInsertTable;
    protected SchemaTableName temporaryInsertIntoNewPartitionTable;
    protected SchemaTableName temporaryInsertIntoExistingPartitionTable;
    protected SchemaTableName temporaryInsertUnsupportedWriteType;
    protected SchemaTableName temporaryMetadataDeleteTable;
    protected SchemaTableName temporaryRenameTableOld;
    protected SchemaTableName temporaryRenameTableNew;
    protected SchemaTableName temporaryCreateView;

    protected String invalidClientId;
    protected ConnectorTableHandle invalidTableHandle;

    protected ColumnHandle dsColumn;
    protected ColumnHandle fileFormatColumn;
    protected ColumnHandle dummyColumn;
    protected ColumnHandle intColumn;
    protected ColumnHandle invalidColumnHandle;

    protected int partitionCount;
    protected TupleDomain<ColumnHandle> tupleDomain;
    protected ConnectorTableLayout tableLayout;
    protected ConnectorTableLayout unpartitionedTableLayout;
    protected ConnectorTableLayoutHandle invalidTableLayoutHandle;
    protected ConnectorTableLayoutHandle emptyTableLayoutHandle;

    protected DateTimeZone timeZone;

    protected HdfsEnvironment hdfsEnvironment;
    protected LocationService locationService;

    protected HiveMetadataFactory metadataFactory;
    protected HiveMetastore metastoreClient;
    protected ConnectorSplitManager splitManager;
    protected ConnectorPageSourceProvider pageSourceProvider;
    protected ConnectorPageSinkProvider pageSinkProvider;
    protected ExecutorService executor;

    protected SchemaTableName insertTableDestination;
    protected SchemaTableName insertTablePartitionedDestination;

    @BeforeClass
    public void setupClass()
            throws Exception
    {
        executor = newCachedThreadPool(daemonThreadsNamed("hive-%s"));
    }

    @AfterClass
    public void tearDown()
            throws Exception
    {
        if (executor != null) {
            executor.shutdownNow();
            executor = null;
        }
    }

    protected void setupHive(String connectorId, String databaseName, String timeZoneId)
    {
        clientId = connectorId;
        database = databaseName;
        tablePartitionFormat = new SchemaTableName(database, "presto_test_partition_format");
        tableUnpartitioned = new SchemaTableName(database, "presto_test_unpartitioned");
        tableOffline = new SchemaTableName(database, "presto_test_offline");
        tableOfflinePartition = new SchemaTableName(database, "presto_test_offline_partition");
        view = new SchemaTableName(database, "presto_test_view");
        invalidTable = new SchemaTableName(database, INVALID_TABLE);
        tableBucketedStringInt = new SchemaTableName(database, "presto_test_bucketed_by_string_int");
        tableBucketedBigintBoolean = new SchemaTableName(database, "presto_test_bucketed_by_bigint_boolean");
        tableBucketedDoubleFloat = new SchemaTableName(database, "presto_test_bucketed_by_double_float");
        tablePartitionSchemaChange = new SchemaTableName(database, "presto_test_partition_schema_change");
        tablePartitionSchemaChangeNonCanonical = new SchemaTableName(database, "presto_test_partition_schema_change_non_canonical");

        temporaryCreateTable = new SchemaTableName(database, "tmp_presto_test_create_" + randomName());
        temporaryCreateRollbackTable = new SchemaTableName(database, "tmp_presto_test_create_" + randomName());
        temporaryCreateSampledTable = new SchemaTableName(database, "tmp_presto_test_create_" + randomName());
        temporaryCreateEmptyTable = new SchemaTableName(database, "tmp_presto_test_create_" + randomName());
        temporaryInsertTable = new SchemaTableName(database, "tmp_presto_test_insert_" + randomName());
        temporaryInsertIntoExistingPartitionTable = new SchemaTableName(database, "tmp_presto_test_insert_exsting_partitioned_" + randomName());
        temporaryInsertIntoNewPartitionTable = new SchemaTableName(database, "tmp_presto_test_insert_new_partitioned_" + randomName());
        temporaryInsertUnsupportedWriteType = new SchemaTableName(database, "tmp_presto_test_insert_unsupported_type_" + randomName());
        temporaryMetadataDeleteTable = new SchemaTableName(database, "tmp_presto_test_metadata_delete_" + randomName());
        temporaryRenameTableOld = new SchemaTableName(database, "tmp_presto_test_rename_" + randomName());
        temporaryRenameTableNew = new SchemaTableName(database, "tmp_presto_test_rename_" + randomName());
        temporaryCreateView = new SchemaTableName(database, "tmp_presto_test_create_" + randomName());

        invalidClientId = "hive";
        invalidTableHandle = new HiveTableHandle(invalidClientId, database, INVALID_TABLE);
        invalidTableLayoutHandle = new HiveTableLayoutHandle(invalidClientId,
                ImmutableList.of(),
                ImmutableList.of(new HivePartition(invalidTable, TupleDomain.all(), "unknown", ImmutableMap.of(), Optional.empty())),
                TupleDomain.all());
        emptyTableLayoutHandle = new HiveTableLayoutHandle(invalidClientId, ImmutableList.of(), ImmutableList.of(), TupleDomain.none());

        dsColumn = new HiveColumnHandle(connectorId, "ds", HIVE_STRING, parseTypeSignature(StandardTypes.VARCHAR), -1, true);
        fileFormatColumn = new HiveColumnHandle(connectorId, "file_format", HIVE_STRING, parseTypeSignature(StandardTypes.VARCHAR), -1, true);
        dummyColumn = new HiveColumnHandle(connectorId, "dummy", HIVE_INT, parseTypeSignature(StandardTypes.BIGINT), -1, true);
        intColumn = new HiveColumnHandle(connectorId, "t_int", HIVE_INT, parseTypeSignature(StandardTypes.BIGINT), -1, true);
        invalidColumnHandle = new HiveColumnHandle(connectorId, INVALID_COLUMN, HIVE_STRING, parseTypeSignature(StandardTypes.VARCHAR), 0, false);

        insertTableDestination = new SchemaTableName(database, "presto_insert_destination");
        insertTablePartitionedDestination = new SchemaTableName(database, "presto_insert_destination_partitioned");

        List<ColumnHandle> partitionColumns = ImmutableList.of(dsColumn, fileFormatColumn, dummyColumn);
        List<HivePartition> partitions = ImmutableList.<HivePartition>builder()
                .add(new HivePartition(tablePartitionFormat,
                        TupleDomain.<HiveColumnHandle>all(),
                        "ds=2012-12-29/file_format=textfile/dummy=1",
                        ImmutableMap.<ColumnHandle, NullableValue>builder()
                                .put(dsColumn, NullableValue.of(VARCHAR, utf8Slice("2012-12-29")))
                                .put(fileFormatColumn, NullableValue.of(VARCHAR, utf8Slice("textfile")))
                                .put(dummyColumn, NullableValue.of(BIGINT, 1L))
                                .build(),
                        Optional.empty()))
                .add(new HivePartition(tablePartitionFormat,
                        TupleDomain.<HiveColumnHandle>all(),
                        "ds=2012-12-29/file_format=sequencefile/dummy=2",
                        ImmutableMap.<ColumnHandle, NullableValue>builder()
                                .put(dsColumn, NullableValue.of(VARCHAR, utf8Slice("2012-12-29")))
                                .put(fileFormatColumn, NullableValue.of(VARCHAR, utf8Slice("sequencefile")))
                                .put(dummyColumn, NullableValue.of(BIGINT, 2L))
                                .build(),
                        Optional.empty()))
                .add(new HivePartition(tablePartitionFormat,
                        TupleDomain.<HiveColumnHandle>all(),
                        "ds=2012-12-29/file_format=rctext/dummy=3",
                        ImmutableMap.<ColumnHandle, NullableValue>builder()
                                .put(dsColumn, NullableValue.of(VARCHAR, utf8Slice("2012-12-29")))
                                .put(fileFormatColumn, NullableValue.of(VARCHAR, utf8Slice("rctext")))
                                .put(dummyColumn, NullableValue.of(BIGINT, 3L))
                                .build(),
                        Optional.empty()))
                .add(new HivePartition(tablePartitionFormat,
                        TupleDomain.<HiveColumnHandle>all(),
                        "ds=2012-12-29/file_format=rcbinary/dummy=4",
                        ImmutableMap.<ColumnHandle, NullableValue>builder()
                                .put(dsColumn, NullableValue.of(VARCHAR, utf8Slice("2012-12-29")))
                                .put(fileFormatColumn, NullableValue.of(VARCHAR, utf8Slice("rcbinary")))
                                .put(dummyColumn, NullableValue.of(BIGINT, 4L))
                                .build(),
                        Optional.empty()))
                .build();
        partitionCount = partitions.size();
        tupleDomain = TupleDomain.fromFixedValues(ImmutableMap.of(dsColumn, NullableValue.of(VARCHAR, utf8Slice("2012-12-29"))));
        tableLayout = new ConnectorTableLayout(
                new HiveTableLayoutHandle(clientId, partitionColumns, partitions, tupleDomain),
                Optional.empty(),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        dsColumn, Domain.create(ValueSet.ofRanges(Range.equal(VARCHAR, utf8Slice("2012-12-29"))), false),
                        fileFormatColumn, Domain.create(ValueSet.ofRanges(Range.equal(VARCHAR, utf8Slice("textfile")), Range.equal(VARCHAR, utf8Slice("sequencefile")), Range.equal(VARCHAR, utf8Slice("rctext")), Range.equal(VARCHAR, utf8Slice("rcbinary"))), false),
                        dummyColumn, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L), Range.equal(BIGINT, 3L), Range.equal(BIGINT, 4L)), false))),
                Optional.empty(),
                Optional.empty(),
                Optional.of(new DiscretePredicates(partitionColumns, ImmutableList.of(
                        TupleDomain.withColumnDomains(ImmutableMap.of(
                                dsColumn, Domain.create(ValueSet.ofRanges(Range.equal(VARCHAR, utf8Slice("2012-12-29"))), false),
                                fileFormatColumn, Domain.create(ValueSet.ofRanges(Range.equal(VARCHAR, utf8Slice("textfile"))), false),
                                dummyColumn, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L)), false))),
                        TupleDomain.withColumnDomains(ImmutableMap.of(
                                dsColumn, Domain.create(ValueSet.ofRanges(Range.equal(VARCHAR, utf8Slice("2012-12-29"))), false),
                                fileFormatColumn, Domain.create(ValueSet.ofRanges(Range.equal(VARCHAR, utf8Slice("sequencefile"))), false),
                                dummyColumn, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 2L)), false))),
                        TupleDomain.withColumnDomains(ImmutableMap.of(
                                dsColumn, Domain.create(ValueSet.ofRanges(Range.equal(VARCHAR, utf8Slice("2012-12-29"))), false),
                                fileFormatColumn, Domain.create(ValueSet.ofRanges(Range.equal(VARCHAR, utf8Slice("rctext"))), false),
                                dummyColumn, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 3L)), false))),
                        TupleDomain.withColumnDomains(ImmutableMap.of(
                                dsColumn, Domain.create(ValueSet.ofRanges(Range.equal(VARCHAR, utf8Slice("2012-12-29"))), false),
                                fileFormatColumn, Domain.create(ValueSet.ofRanges(Range.equal(VARCHAR, utf8Slice("rcbinary"))), false),
                                dummyColumn, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 4L)), false)))
                ))),
                ImmutableList.of());
        List<HivePartition> unpartitionedPartitions = ImmutableList.of(new HivePartition(tableUnpartitioned, TupleDomain.all()));
        unpartitionedTableLayout = new ConnectorTableLayout(new HiveTableLayoutHandle(clientId, ImmutableList.of(), unpartitionedPartitions, TupleDomain.all()));
        timeZone = DateTimeZone.forTimeZone(TimeZone.getTimeZone(timeZoneId));
    }

    protected final void setup(String host, int port, String databaseName, String timeZone)
    {
        setup(host, port, databaseName, timeZone, "hive-test", 100, 50);
    }

    protected final void setup(String host, int port, String databaseName, String timeZoneId, String connectorName, int maxOutstandingSplits, int maxThreads)
    {
        setupHive(connectorName, databaseName, timeZoneId);

        HiveClientConfig hiveClientConfig = new HiveClientConfig();
        hiveClientConfig.setTimeZone(timeZoneId);
        String proxy = System.getProperty("hive.metastore.thrift.client.socks-proxy");
        if (proxy != null) {
            hiveClientConfig.setMetastoreSocksProxy(HostAndPort.fromString(proxy));
        }

        HiveCluster hiveCluster = new TestingHiveCluster(hiveClientConfig, host, port);
        metastoreClient = new CachingHiveMetastore(hiveCluster, executor, Duration.valueOf("1m"), Duration.valueOf("15s"));
        HiveConnectorId connectorId = new HiveConnectorId(connectorName);
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationUpdater(hiveClientConfig));

        hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, hiveClientConfig);
        locationService = new HiveLocationService(metastoreClient, hdfsEnvironment);
        TypeManager typeManager = new TypeRegistry();
        JsonCodec<PartitionUpdate> partitionUpdateCodec = JsonCodec.jsonCodec(PartitionUpdate.class);
        metadataFactory = new HiveMetadataFactory(
                connectorId,
                metastoreClient,
                hdfsEnvironment,
                new HivePartitionManager(connectorId, hiveClientConfig),
                timeZone,
                10,
                true,
                true,
                true,
                true,
                true,
                true,
                HiveStorageFormat.RCBINARY,
                typeManager,
                locationService,
                new TableParameterCodec(),
                partitionUpdateCodec,
                newFixedThreadPool(2));
        splitManager = new HiveSplitManager(
                connectorId,
                metastoreClient,
                new NamenodeStats(),
                hdfsEnvironment,
                new HadoopDirectoryLister(),
                newDirectExecutorService(),
                maxOutstandingSplits,
                hiveClientConfig.getMinPartitionBatchSize(),
                hiveClientConfig.getMaxPartitionBatchSize(),
                hiveClientConfig.getMaxSplitSize(),
                hiveClientConfig.getMaxInitialSplitSize(),
                hiveClientConfig.getMaxInitialSplits(),
                false
        );
        pageSinkProvider = new HivePageSinkProvider(hdfsEnvironment, metastoreClient, new GroupByHashPageIndexerFactory(), typeManager, new HiveClientConfig(), locationService, partitionUpdateCodec);
        pageSourceProvider = new HivePageSourceProvider(hiveClientConfig, hdfsEnvironment, getDefaultHiveRecordCursorProvider(hiveClientConfig), getDefaultHiveDataStreamFactories(hiveClientConfig), TYPE_MANAGER);
    }

    protected ConnectorSession newSession()
    {
        return new TestingConnectorSession(new HiveSessionProperties(new HiveClientConfig()).getSessionProperties());
    }

    protected ConnectorTransactionHandle newTransaction()
    {
        return new HiveTransactionHandle();
    }

    protected ConnectorMetadata newMetadata()
    {
        return metadataFactory.create();
    }

    protected void rollback(ConnectorMetadata metadata)
    {
        ((HiveMetadata) metadata).rollback();
    }

    @Test
    public void testGetDatabaseNames()
            throws Exception
    {
        List<String> databases = newMetadata().listSchemaNames(newSession());
        assertTrue(databases.contains(database));
    }

    @Test
    public void testGetTableNames()
            throws Exception
    {
        List<SchemaTableName> tables = newMetadata().listTables(newSession(), database);
        assertTrue(tables.contains(tablePartitionFormat));
        assertTrue(tables.contains(tableUnpartitioned));
    }

    @Test
    public void testGetAllTableNames()
            throws Exception
    {
        List<SchemaTableName> tables = newMetadata().listTables(newSession(), null);
        assertTrue(tables.contains(tablePartitionFormat));
        assertTrue(tables.contains(tableUnpartitioned));
    }

    @Test
    public void testGetAllTableColumns()
    {
        Map<SchemaTableName, List<ColumnMetadata>> allColumns = newMetadata().listTableColumns(newSession(), new SchemaTablePrefix());
        assertTrue(allColumns.containsKey(tablePartitionFormat));
        assertTrue(allColumns.containsKey(tableUnpartitioned));
    }

    @Test
    public void testGetAllTableColumnsInSchema()
    {
        Map<SchemaTableName, List<ColumnMetadata>> allColumns = newMetadata().listTableColumns(newSession(), new SchemaTablePrefix(database));
        assertTrue(allColumns.containsKey(tablePartitionFormat));
        assertTrue(allColumns.containsKey(tableUnpartitioned));
    }

    @Test
    public void testListUnknownSchema()
    {
        ConnectorMetadata metadata = newMetadata();
        ConnectorSession session = newSession();
        assertNull(metadata.getTableHandle(session, new SchemaTableName(INVALID_DATABASE, INVALID_TABLE)));
        assertEquals(metadata.listTables(session, INVALID_DATABASE), ImmutableList.of());
        assertEquals(metadata.listTableColumns(session, new SchemaTablePrefix(INVALID_DATABASE, INVALID_TABLE)), ImmutableMap.of());
        assertEquals(metadata.listViews(session, INVALID_DATABASE), ImmutableList.of());
        assertEquals(metadata.getViews(session, new SchemaTablePrefix(INVALID_DATABASE, INVALID_TABLE)), ImmutableMap.of());
    }

    @Test
    public void testGetPartitions()
            throws Exception
    {
        ConnectorMetadata metadata = newMetadata();
        ConnectorTableHandle tableHandle = getTableHandle(metadata, tablePartitionFormat);
        List<ConnectorTableLayoutResult> tableLayoutResults = metadata.getTableLayouts(newSession(), tableHandle, new Constraint<>(TupleDomain.all(), bindings -> true), Optional.empty());
        assertExpectedTableLayout(getOnlyElement(tableLayoutResults).getTableLayout(), tableLayout);
    }

    @Test
    public void testGetPartitionsWithBindings()
            throws Exception
    {
        ConnectorMetadata metadata = newMetadata();
        ConnectorTableHandle tableHandle = getTableHandle(metadata, tablePartitionFormat);
        List<ConnectorTableLayoutResult> tableLayoutResults = metadata.getTableLayouts(newSession(), tableHandle, new Constraint<>(TupleDomain.withColumnDomains(ImmutableMap.of(intColumn, Domain.singleValue(BIGINT, 5L))), bindings -> true), Optional.empty());
        assertExpectedTableLayout(getOnlyElement(tableLayoutResults).getTableLayout(), tableLayout);
    }

    @Test(expectedExceptions = TableNotFoundException.class)
    public void testGetPartitionsException()
            throws Exception
    {
        newMetadata().getTableLayouts(newSession(), invalidTableHandle, new Constraint<>(TupleDomain.all(), bindings -> true), Optional.empty());
    }

    @Test
    public void testGetPartitionNames()
            throws Exception
    {
        ConnectorMetadata metadata = newMetadata();
        ConnectorTableHandle tableHandle = getTableHandle(metadata, tablePartitionFormat);
        List<ConnectorTableLayoutResult> tableLayoutResults = metadata.getTableLayouts(newSession(), tableHandle, new Constraint<>(TupleDomain.all(), bindings -> true), Optional.empty());
        assertExpectedTableLayout(getOnlyElement(tableLayoutResults).getTableLayout(), tableLayout);
    }

    protected void assertExpectedTableLayout(ConnectorTableLayout actualTableLayout, ConnectorTableLayout expectedTableLayout)
    {
        assertExpectedTableLayoutHandle(actualTableLayout.getHandle(), expectedTableLayout.getHandle());
        assertEquals(actualTableLayout.getPredicate(), expectedTableLayout.getPredicate());
        assertEquals(actualTableLayout.getDiscretePredicates().isPresent(), expectedTableLayout.getDiscretePredicates().isPresent());
        actualTableLayout.getDiscretePredicates().ifPresent(actual -> {
            DiscretePredicates expected = expectedTableLayout.getDiscretePredicates().get();
            assertEquals(actual.getColumns(), expected.getColumns());
            assertEqualsIgnoreOrder(actual.getPredicates(), expected.getPredicates());
        });
        assertEquals(actualTableLayout.getStreamPartitioningColumns(), expectedTableLayout.getStreamPartitioningColumns());
        assertEquals(actualTableLayout.getLocalProperties(), expectedTableLayout.getLocalProperties());
    }

    protected void assertExpectedTableLayoutHandle(ConnectorTableLayoutHandle actualTableLayoutHandle, ConnectorTableLayoutHandle expectedTableLayoutHandle)
    {
        assertInstanceOf(actualTableLayoutHandle, HiveTableLayoutHandle.class);
        assertInstanceOf(expectedTableLayoutHandle, HiveTableLayoutHandle.class);
        HiveTableLayoutHandle actual = (HiveTableLayoutHandle) actualTableLayoutHandle;
        HiveTableLayoutHandle expected = (HiveTableLayoutHandle) expectedTableLayoutHandle;
        assertEquals(actual.getClientId(), expected.getClientId());
        assertExpectedPartitions(actual.getPartitions().get(), expected.getPartitions().get());
    }

    protected void assertExpectedPartitions(List<?> actualPartitions, Iterable<?> expectedPartitions)
    {
        Map<String, ?> actualById = uniqueIndex(actualPartitions, actualPartition -> ((HivePartition) actualPartition).getPartitionId());
        for (Object expected : expectedPartitions) {
            assertInstanceOf(expected, HivePartition.class);
            HivePartition expectedPartition = (HivePartition) expected;

            Object actual = actualById.get(expectedPartition.getPartitionId());
            assertEquals(actual, expected);
            assertInstanceOf(actual, HivePartition.class);
            HivePartition actualPartition = (HivePartition) actual;

            assertNotNull(actualPartition, "partition " + expectedPartition.getPartitionId());
            assertEquals(actualPartition.getPartitionId(), expectedPartition.getPartitionId());
            assertEquals(actualPartition.getKeys(), expectedPartition.getKeys());
            assertEquals(actualPartition.getTableName(), expectedPartition.getTableName());
            assertEquals(actualPartition.getBucket(), expectedPartition.getBucket());
            assertEquals(actualPartition.getTupleDomain(), expectedPartition.getTupleDomain());
        }
    }

    @Test
    public void testGetPartitionNamesUnpartitioned()
            throws Exception
    {
        ConnectorMetadata metadata = newMetadata();
        ConnectorTableHandle tableHandle = getTableHandle(metadata, tableUnpartitioned);
        List<ConnectorTableLayoutResult> tableLayoutResults = metadata.getTableLayouts(newSession(), tableHandle, new Constraint<>(TupleDomain.all(), bindings -> true), Optional.empty());
        assertEquals(getAllPartitions(getOnlyElement(tableLayoutResults).getTableLayout().getHandle()).size(), 1);
        assertExpectedTableLayout(getOnlyElement(tableLayoutResults).getTableLayout(), unpartitionedTableLayout);
    }

    @Test(expectedExceptions = TableNotFoundException.class)
    public void testGetPartitionNamesException()
            throws Exception
    {
        newMetadata().getTableLayouts(newSession(), invalidTableHandle, new Constraint<>(TupleDomain.all(), bindings -> true), Optional.empty());
    }

    @SuppressWarnings({"ValueOfIncrementOrDecrementUsed", "UnusedAssignment"})
    @Test
    public void testGetTableSchemaPartitionFormat()
            throws Exception
    {
        ConnectorMetadata metadata = newMetadata();
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(newSession(), getTableHandle(metadata, tablePartitionFormat));
        Map<String, ColumnMetadata> map = uniqueIndex(tableMetadata.getColumns(), ColumnMetadata::getName);

        assertPrimitiveField(map, "t_string", VARCHAR, false);
        assertPrimitiveField(map, "t_tinyint", BIGINT, false);
        assertPrimitiveField(map, "t_smallint", BIGINT, false);
        assertPrimitiveField(map, "t_int", BIGINT, false);
        assertPrimitiveField(map, "t_bigint", BIGINT, false);
        assertPrimitiveField(map, "t_float", DOUBLE, false);
        assertPrimitiveField(map, "t_double", DOUBLE, false);
        assertPrimitiveField(map, "t_boolean", BOOLEAN, false);
        assertPrimitiveField(map, "ds", VARCHAR, true);
        assertPrimitiveField(map, "file_format", VARCHAR, true);
        assertPrimitiveField(map, "dummy", BIGINT, true);
    }

    @Test
    public void testGetTableSchemaUnpartitioned()
            throws Exception
    {
        ConnectorMetadata metadata = newMetadata();
        ConnectorTableHandle tableHandle = getTableHandle(metadata, tableUnpartitioned);
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(newSession(), tableHandle);
        Map<String, ColumnMetadata> map = uniqueIndex(tableMetadata.getColumns(), ColumnMetadata::getName);

        assertPrimitiveField(map, "t_string", VARCHAR, false);
        assertPrimitiveField(map, "t_tinyint", BIGINT, false);
    }

    @Test
    public void testGetTableSchemaOffline()
            throws Exception
    {
        ConnectorMetadata metadata = newMetadata();
        ConnectorTableHandle tableHandle = getTableHandle(metadata, tableOffline);
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(newSession(), tableHandle);
        Map<String, ColumnMetadata> map = uniqueIndex(tableMetadata.getColumns(), ColumnMetadata::getName);

        assertPrimitiveField(map, "t_string", VARCHAR, false);
    }

    @Test
    public void testGetTableSchemaOfflinePartition()
            throws Exception
    {
        ConnectorMetadata metadata = newMetadata();
        ConnectorTableHandle tableHandle = getTableHandle(metadata, tableOfflinePartition);
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(newSession(), tableHandle);
        Map<String, ColumnMetadata> map = uniqueIndex(tableMetadata.getColumns(), ColumnMetadata::getName);

        assertPrimitiveField(map, "t_string", VARCHAR, false);
    }

    @Test
    public void testGetTableSchemaException()
            throws Exception
    {
        assertNull(newMetadata().getTableHandle(newSession(), invalidTable));
    }

    @Test
    public void testGetPartitionSplitsBatch()
            throws Exception
    {
        ConnectorMetadata metadata = newMetadata();
        ConnectorSession session = newSession();

        ConnectorTableHandle tableHandle = getTableHandle(metadata, tablePartitionFormat);
        List<ConnectorTableLayoutResult> tableLayoutResults = metadata.getTableLayouts(session, tableHandle, new Constraint<>(TupleDomain.all(), bindings -> true), Optional.empty());
        ConnectorSplitSource splitSource = splitManager.getSplits(newTransaction(), session, getOnlyElement(tableLayoutResults).getTableLayout().getHandle());

        assertEquals(getSplitCount(splitSource), partitionCount);
    }

    @Test
    public void testGetPartitionSplitsBatchUnpartitioned()
            throws Exception
    {
        ConnectorMetadata metadata = newMetadata();
        ConnectorSession session = newSession();

        ConnectorTableHandle tableHandle = getTableHandle(metadata, tableUnpartitioned);
        List<ConnectorTableLayoutResult> tableLayoutResults = metadata.getTableLayouts(session, tableHandle, new Constraint<>(TupleDomain.all(), bindings -> true), Optional.empty());
        ConnectorSplitSource splitSource = splitManager.getSplits(newTransaction(), session, getOnlyElement(tableLayoutResults).getTableLayout().getHandle());

        assertEquals(getSplitCount(splitSource), 1);
    }

    @Test(expectedExceptions = TableNotFoundException.class)
    public void testGetPartitionSplitsBatchInvalidTable()
            throws Exception
    {
        splitManager.getSplits(newTransaction(), newSession(), invalidTableLayoutHandle);
    }

    @Test
    public void testGetPartitionSplitsEmpty()
            throws Exception
    {
        ConnectorSplitSource splitSource = splitManager.getSplits(newTransaction(), newSession(), emptyTableLayoutHandle);
        // fetch full list
        getSplitCount(splitSource);
    }

    @Test
    public void testGetPartitionTableOffline()
            throws Exception
    {
        ConnectorMetadata metadata = newMetadata();
        ConnectorTableHandle tableHandle = getTableHandle(metadata, tableOffline);
        try {
            metadata.getTableLayouts(newSession(), tableHandle, new Constraint<>(TupleDomain.all(), bindings -> true), Optional.empty());
            fail("expected TableOfflineException");
        }
        catch (TableOfflineException e) {
            assertEquals(e.getTableName(), tableOffline);
        }
    }

    @Test
    public void testGetPartitionSplitsTableOfflinePartition()
            throws Exception
    {
        ConnectorMetadata metadata = newMetadata();
        ConnectorSession session = newSession();

        ConnectorTableHandle tableHandle = getTableHandle(metadata, tableOfflinePartition);
        assertNotNull(tableHandle);

        ColumnHandle dsColumn = metadata.getColumnHandles(session, tableHandle).get("ds");
        assertNotNull(dsColumn);

        Domain domain = Domain.singleValue(VARCHAR, utf8Slice("2012-12-30"));
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(dsColumn, domain));
        List<ConnectorTableLayoutResult> tableLayoutResults = metadata.getTableLayouts(session, tableHandle, new Constraint<>(tupleDomain, bindings -> true), Optional.empty());
        try {
            getSplitCount(splitManager.getSplits(newTransaction(), session, getOnlyElement(tableLayoutResults).getTableLayout().getHandle()));
            fail("Expected PartitionOfflineException");
        }
        catch (PartitionOfflineException e) {
            assertEquals(e.getTableName(), tableOfflinePartition);
            assertEquals(e.getPartition(), "ds=2012-12-30");
        }
    }

    @Test
    public void testBucketedTableStringInt()
            throws Exception
    {
        ConnectorMetadata metadata = newMetadata();
        ConnectorSession session = newSession();

        ConnectorTableHandle tableHandle = getTableHandle(metadata, tableBucketedStringInt);
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(session, tableHandle).values());
        Map<String, Integer> columnIndex = indexColumns(columnHandles);

        assertTableIsBucketed(tableHandle);

        String testString = "test";
        Long testInt = 13L;
        Long testSmallint = 12L;

        // Reverse the order of bindings as compared to bucketing order
        ImmutableMap<ColumnHandle, NullableValue> bindings = ImmutableMap.<ColumnHandle, NullableValue>builder()
                .put(columnHandles.get(columnIndex.get("t_int")), NullableValue.of(BIGINT, testInt))
                .put(columnHandles.get(columnIndex.get("t_string")), NullableValue.of(VARCHAR, utf8Slice(testString)))
                .put(columnHandles.get(columnIndex.get("t_smallint")), NullableValue.of(BIGINT, testSmallint))
                .build();

        MaterializedResult result = readTable(tableHandle, columnHandles, session, TupleDomain.fromFixedValues(bindings), OptionalInt.of(1), Optional.empty());

        boolean rowFound = false;
        for (MaterializedRow row : result) {
            if (testString.equals(row.getField(columnIndex.get("t_string"))) &&
                    testInt.equals(row.getField(columnIndex.get("t_int"))) &&
                    testSmallint.equals(row.getField(columnIndex.get("t_smallint")))) {
                rowFound = true;
            }
        }
        assertTrue(rowFound);
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testBucketedTableBigintBoolean()
            throws Exception
    {
        ConnectorMetadata metadata = newMetadata();
        ConnectorSession session = newSession();

        ConnectorTableHandle tableHandle = getTableHandle(metadata, tableBucketedBigintBoolean);
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(session, tableHandle).values());
        Map<String, Integer> columnIndex = indexColumns(columnHandles);

        assertTableIsBucketed(tableHandle);

        String testString = "test";
        Long testBigint = 89L;
        Boolean testBoolean = true;

        ImmutableMap<ColumnHandle, NullableValue> bindings = ImmutableMap.<ColumnHandle, NullableValue>builder()
                .put(columnHandles.get(columnIndex.get("t_string")), NullableValue.of(VARCHAR, utf8Slice(testString)))
                .put(columnHandles.get(columnIndex.get("t_bigint")), NullableValue.of(BIGINT, testBigint))
                .put(columnHandles.get(columnIndex.get("t_boolean")), NullableValue.of(BOOLEAN, testBoolean))
                .build();

        MaterializedResult result = readTable(tableHandle, columnHandles, session, TupleDomain.fromFixedValues(bindings), OptionalInt.of(1), Optional.empty());

        boolean rowFound = false;
        for (MaterializedRow row : result) {
            if (testString.equals(row.getField(columnIndex.get("t_string"))) &&
                    testBigint.equals(row.getField(columnIndex.get("t_bigint"))) &&
                    testBoolean.equals(row.getField(columnIndex.get("t_boolean")))) {
                rowFound = true;
                break;
            }
        }
        assertTrue(rowFound);
    }

    @Test
    public void testBucketedTableDoubleFloat()
            throws Exception
    {
        ConnectorMetadata metadata = newMetadata();
        ConnectorSession session = newSession();

        ConnectorTableHandle tableHandle = getTableHandle(metadata, tableBucketedDoubleFloat);
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(session, tableHandle).values());
        Map<String, Integer> columnIndex = indexColumns(columnHandles);

        assertTableIsBucketed(tableHandle);

        ImmutableMap<ColumnHandle, NullableValue> bindings = ImmutableMap.<ColumnHandle, NullableValue>builder()
                .put(columnHandles.get(columnIndex.get("t_float")), NullableValue.of(DOUBLE, 87.1))
                .put(columnHandles.get(columnIndex.get("t_double")), NullableValue.of(DOUBLE, 88.2))
                .build();

        // floats and doubles are not supported, so we should see all splits
        MaterializedResult result = readTable(tableHandle, columnHandles, session, TupleDomain.fromFixedValues(bindings), OptionalInt.of(32), Optional.empty());
        assertEquals(result.getRowCount(), 100);
    }

    private void assertTableIsBucketed(ConnectorTableHandle tableHandle)
            throws Exception
    {
        // the bucketed test tables should have exactly 32 splits
        List<ConnectorSplit> splits = getAllSplits(tableHandle, TupleDomain.all());
        assertEquals(splits.size(), 32);

        // verify all paths are unique
        Set<String> paths = new HashSet<>();
        for (ConnectorSplit split : splits) {
            assertTrue(paths.add(((HiveSplit) split).getPath()));
        }
    }

    @Test
    public void testGetRecords()
            throws Exception
    {
        ConnectorMetadata metadata = newMetadata();
        ConnectorSession session = newSession();

        ConnectorTableHandle tableHandle = getTableHandle(metadata, tablePartitionFormat);
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(session, tableHandle);
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(session, tableHandle).values());
        Map<String, Integer> columnIndex = indexColumns(columnHandles);

        List<ConnectorSplit> splits = getAllSplits(tableHandle, TupleDomain.all());
        assertEquals(splits.size(), partitionCount);
        for (ConnectorSplit split : splits) {
            HiveSplit hiveSplit = (HiveSplit) split;

            List<HivePartitionKey> partitionKeys = hiveSplit.getPartitionKeys();
            String ds = partitionKeys.get(0).getValue();
            String fileFormat = partitionKeys.get(1).getValue();
            HiveStorageFormat fileType = HiveStorageFormat.valueOf(fileFormat.toUpperCase());
            long dummyPartition = Long.parseLong(partitionKeys.get(2).getValue());

            long rowNumber = 0;
            long completedBytes = 0;
            try (ConnectorPageSource pageSource = pageSourceProvider.createPageSource(newTransaction(), session, hiveSplit, columnHandles)) {
                MaterializedResult result = materializeSourceDataStream(session, pageSource, getTypes(columnHandles));

                assertPageSourceType(pageSource, fileType);

                for (MaterializedRow row : result) {
                    try {
                        assertValueTypes(row, tableMetadata.getColumns());
                    }
                    catch (RuntimeException e) {
                        throw new RuntimeException("row " + rowNumber, e);
                    }

                    rowNumber++;

                    if (rowNumber % 19 == 0) {
                        assertNull(row.getField(columnIndex.get("t_string")));
                    }
                    else if (rowNumber % 19 == 1) {
                        assertEquals(row.getField(columnIndex.get("t_string")), "");
                    }
                    else {
                        assertEquals(row.getField(columnIndex.get("t_string")), "test");
                    }

                    assertEquals(row.getField(columnIndex.get("t_tinyint")), 1 + rowNumber);
                    assertEquals(row.getField(columnIndex.get("t_smallint")), 2 + rowNumber);
                    assertEquals(row.getField(columnIndex.get("t_int")), 3 + rowNumber);

                    if (rowNumber % 13 == 0) {
                        assertNull(row.getField(columnIndex.get("t_bigint")));
                    }
                    else {
                        assertEquals(row.getField(columnIndex.get("t_bigint")), 4 + rowNumber);
                    }

                    assertEquals((Double) row.getField(columnIndex.get("t_float")), 5.1 + rowNumber, 0.001);
                    assertEquals(row.getField(columnIndex.get("t_double")), 6.2 + rowNumber);

                    if (rowNumber % 3 == 2) {
                        assertNull(row.getField(columnIndex.get("t_boolean")));
                    }
                    else {
                        assertEquals(row.getField(columnIndex.get("t_boolean")), rowNumber % 3 != 0);
                    }

                    assertEquals(row.getField(columnIndex.get("ds")), ds);
                    assertEquals(row.getField(columnIndex.get("file_format")), fileFormat);
                    assertEquals(row.getField(columnIndex.get("dummy")), dummyPartition);

                    long newCompletedBytes = pageSource.getCompletedBytes();
                    assertTrue(newCompletedBytes >= completedBytes);
                    assertTrue(newCompletedBytes <= hiveSplit.getLength());
                    completedBytes = newCompletedBytes;
                }

                assertTrue(completedBytes <= hiveSplit.getLength());
                assertEquals(rowNumber, 100);
            }
        }
    }

    @Test
    public void testGetPartialRecords()
            throws Exception
    {
        ConnectorMetadata metadata = newMetadata();
        ConnectorSession session = newSession();

        ConnectorTableHandle tableHandle = getTableHandle(metadata, tablePartitionFormat);
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(session, tableHandle).values());
        Map<String, Integer> columnIndex = indexColumns(columnHandles);

        List<ConnectorSplit> splits = getAllSplits(tableHandle, TupleDomain.all());
        assertEquals(splits.size(), partitionCount);
        for (ConnectorSplit split : splits) {
            HiveSplit hiveSplit = (HiveSplit) split;

            List<HivePartitionKey> partitionKeys = hiveSplit.getPartitionKeys();
            String ds = partitionKeys.get(0).getValue();
            String fileFormat = partitionKeys.get(1).getValue();
            HiveStorageFormat fileType = HiveStorageFormat.valueOf(fileFormat.toUpperCase());
            long dummyPartition = Long.parseLong(partitionKeys.get(2).getValue());

            long rowNumber = 0;
            try (ConnectorPageSource pageSource = pageSourceProvider.createPageSource(newTransaction(), session, hiveSplit, columnHandles)) {
                assertPageSourceType(pageSource, fileType);
                MaterializedResult result = materializeSourceDataStream(session, pageSource, getTypes(columnHandles));
                for (MaterializedRow row : result) {
                    rowNumber++;

                    assertEquals(row.getField(columnIndex.get("t_double")), 6.2 + rowNumber);
                    assertEquals(row.getField(columnIndex.get("ds")), ds);
                    assertEquals(row.getField(columnIndex.get("file_format")), fileFormat);
                    assertEquals(row.getField(columnIndex.get("dummy")), dummyPartition);
                }
            }
            assertEquals(rowNumber, 100);
        }
    }

    @Test
    public void testGetRecordsUnpartitioned()
            throws Exception
    {
        ConnectorMetadata metadata = newMetadata();
        ConnectorSession session = newSession();

        ConnectorTableHandle tableHandle = getTableHandle(metadata, tableUnpartitioned);
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(session, tableHandle).values());
        Map<String, Integer> columnIndex = indexColumns(columnHandles);

        List<ConnectorSplit> splits = getAllSplits(tableHandle, TupleDomain.all());
        assertEquals(splits.size(), 1);

        for (ConnectorSplit split : splits) {
            HiveSplit hiveSplit = (HiveSplit) split;

            assertEquals(hiveSplit.getPartitionKeys(), ImmutableList.of());

            long rowNumber = 0;
            try (ConnectorPageSource pageSource = pageSourceProvider.createPageSource(newTransaction(), session, split, columnHandles)) {
                assertPageSourceType(pageSource, TEXTFILE);
                MaterializedResult result = materializeSourceDataStream(session, pageSource, getTypes(columnHandles));

                assertEquals(pageSource.getTotalBytes(), hiveSplit.getLength());
                for (MaterializedRow row : result) {
                    rowNumber++;

                    if (rowNumber % 19 == 0) {
                        assertNull(row.getField(columnIndex.get("t_string")));
                    }
                    else if (rowNumber % 19 == 1) {
                        assertEquals(row.getField(columnIndex.get("t_string")), "");
                    }
                    else {
                        assertEquals(row.getField(columnIndex.get("t_string")), "unpartitioned");
                    }

                    assertEquals(row.getField(columnIndex.get("t_tinyint")), 1 + rowNumber);
                }
            }
            assertEquals(rowNumber, 100);
        }
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*" + INVALID_COLUMN + ".*")
    public void testGetRecordsInvalidColumn()
            throws Exception
    {
        ConnectorTableHandle table = getTableHandle(newMetadata(), tableUnpartitioned);
        readTable(table, ImmutableList.of(invalidColumnHandle), newSession(), TupleDomain.all(), OptionalInt.empty(), Optional.empty());
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = ".*The column 't_data' in table '.*\\.presto_test_partition_schema_change' is declared as type 'bigint', but partition 'ds=2012-12-29' declared column 't_data' as type 'string'.")
    public void testPartitionSchemaMismatch()
            throws Exception
    {
        ConnectorTableHandle table = getTableHandle(newMetadata(), tablePartitionSchemaChange);
        readTable(table, ImmutableList.of(dsColumn), newSession(), TupleDomain.all(), OptionalInt.empty(), Optional.empty());
    }

    @Test
    public void testPartitionSchemaNonCanonical()
            throws Exception
    {
        ConnectorSession session = newSession();
        ConnectorMetadata metadata = newMetadata();
        ConnectorTransactionHandle transaction = newTransaction();

        ConnectorTableHandle table = getTableHandle(metadata, tablePartitionSchemaChangeNonCanonical);
        ColumnHandle column = metadata.getColumnHandles(session, table).get("t_boolean");
        assertNotNull(column);
        List<ConnectorTableLayoutResult> tableLayoutResults = metadata.getTableLayouts(session, table, new Constraint<>(TupleDomain.fromFixedValues(ImmutableMap.of(column, NullableValue.of(BOOLEAN, false))), bindings -> true), Optional.empty());
        ConnectorTableLayoutHandle layoutHandle = getOnlyElement(tableLayoutResults).getTableLayout().getHandle();
        assertEquals(getAllPartitions(layoutHandle).size(), 1);
        assertEquals(getPartitionId(getAllPartitions(layoutHandle).get(0)), "t_boolean=0");

        ConnectorSplitSource splitSource = splitManager.getSplits(transaction, session, layoutHandle);
        ConnectorSplit split = getOnlyElement(getAllSplits(splitSource));

        ImmutableList<ColumnHandle> columnHandles = ImmutableList.of(column);
        try (ConnectorPageSource ignored = pageSourceProvider.createPageSource(transaction, session, split, columnHandles)) {
            // TODO coercion of non-canonical values should be supported
            fail("expected exception");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), HIVE_INVALID_PARTITION_VALUE.toErrorCode());
        }
    }

    @Test
    public void testTypesTextFile()
            throws Exception
    {
        assertGetRecords("presto_test_types_textfile", TEXTFILE);
    }

    @Test
    public void testTypesSequenceFile()
            throws Exception
    {
        assertGetRecords("presto_test_types_sequencefile", SEQUENCEFILE);
    }

    @Test
    public void testTypesRcText()
            throws Exception
    {
        assertGetRecords("presto_test_types_rctext", RCTEXT);
    }

    @Test
    public void testTypesRcTextRecordCursor()
            throws Exception
    {
        ConnectorSession session = newSession();
        ConnectorMetadata metadata = newMetadata();

        if (metadata.getTableHandle(session, new SchemaTableName(database, "presto_test_types_rctext")) == null) {
            return;
        }

        ConnectorTableHandle tableHandle = getTableHandle(metadata, new SchemaTableName(database, "presto_test_types_rctext"));
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(session, tableHandle);
        HiveSplit hiveSplit = getHiveSplit(tableHandle);
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(session, tableHandle).values());

        ConnectorPageSourceProvider pageSourceProvider = new HivePageSourceProvider(
                new HiveClientConfig().setTimeZone(timeZone.getID()),
                hdfsEnvironment,
                ImmutableSet.<HiveRecordCursorProvider>of(new ColumnarTextHiveRecordCursorProvider()),
                ImmutableSet.<HivePageSourceFactory>of(),
                TYPE_MANAGER);

        ConnectorPageSource pageSource = pageSourceProvider.createPageSource(newTransaction(), session, hiveSplit, columnHandles);
        assertGetRecords(RCTEXT, tableMetadata, hiveSplit, pageSource, columnHandles);
    }

    @Test
    public void testTypesRcBinary()
            throws Exception
    {
        assertGetRecords("presto_test_types_rcbinary", RCBINARY);
    }

    @Test
    public void testTypesRcBinaryRecordCursor()
            throws Exception
    {
        ConnectorSession session = newSession();
        ConnectorMetadata metadata = newMetadata();

        if (metadata.getTableHandle(session, new SchemaTableName(database, "presto_test_types_rcbinary")) == null) {
            return;
        }

        ConnectorTableHandle tableHandle = getTableHandle(metadata, new SchemaTableName(database, "presto_test_types_rcbinary"));
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(session, tableHandle);
        HiveSplit hiveSplit = getHiveSplit(tableHandle);
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(session, tableHandle).values());

        ConnectorPageSourceProvider pageSourceProvider = new HivePageSourceProvider(
                new HiveClientConfig().setTimeZone(timeZone.getID()),
                hdfsEnvironment,
                ImmutableSet.<HiveRecordCursorProvider>of(new ColumnarBinaryHiveRecordCursorProvider()),
                ImmutableSet.<HivePageSourceFactory>of(),
                TYPE_MANAGER);

        ConnectorPageSource pageSource = pageSourceProvider.createPageSource(newTransaction(), session, hiveSplit, columnHandles);
        assertGetRecords(RCBINARY, tableMetadata, hiveSplit, pageSource, columnHandles);
    }

    @Test
    public void testTypesOrc()
            throws Exception
    {
        assertGetRecordsOptional("presto_test_types_orc", ORC);
    }

    @Test
    public void testTypesParquet()
            throws Exception
    {
        assertGetRecordsOptional("presto_test_types_parquet", PARQUET);
    }

    @Test
    public void testTypesDwrf()
            throws Exception
    {
        assertGetRecordsOptional("presto_test_types_dwrf", DWRF);
    }

    @Test
    public void testHiveViewsAreNotSupported()
            throws Exception
    {
        try {
            getTableHandle(newMetadata(), view);
            fail("Expected HiveViewNotSupportedException");
        }
        catch (HiveViewNotSupportedException e) {
            assertEquals(e.getTableName(), view);
        }
    }

    @Test
    public void testHiveViewsHaveNoColumns()
            throws Exception
    {
        assertEquals(newMetadata().listTableColumns(newSession(), new SchemaTablePrefix(view.getSchemaName(), view.getTableName())), ImmutableMap.of());
    }

    @Test
    public void testRenameTable()
    {
        try {
            createDummyTable(temporaryRenameTableOld);
            ConnectorSession session = newSession();
            ConnectorMetadata metadata = newMetadata();

            metadata.renameTable(session, getTableHandle(metadata, temporaryRenameTableOld), temporaryRenameTableNew);

            assertNull(metadata.getTableHandle(session, temporaryRenameTableOld));
            assertNotNull(metadata.getTableHandle(session, temporaryRenameTableNew));
        }
        finally {
            dropTable(temporaryRenameTableOld);
            dropTable(temporaryRenameTableNew);
        }
    }

    @Test
    public void testTableCreation()
            throws Exception
    {
        for (HiveStorageFormat storageFormat : createTableFormats) {
            try {
                doCreateTable(temporaryCreateTable, storageFormat);
            }
            finally {
                dropTable(temporaryCreateTable);
            }
        }
    }

    @Test
    public void testTableCreationRollback()
            throws Exception
    {
        try {
            ConnectorSession session = newSession();
            ConnectorTransactionHandle transaction = newTransaction();
            ConnectorMetadata metadata = newMetadata();

            // begin creating the table
            ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(temporaryCreateRollbackTable, CREATE_TABLE_COLUMNS, createTableProperties(RCBINARY), session.getUser());

            ConnectorOutputTableHandle outputHandle = metadata.beginCreateTable(session, tableMetadata, Optional.empty());

            // write the data
            ConnectorPageSink sink = pageSinkProvider.createPageSink(transaction, session, outputHandle);
            sink.appendPage(CREATE_TABLE_DATA.toPage(), null);
            sink.finish();

            // verify we have data files
            assertFalse(listAllDataFiles(outputHandle).isEmpty());

            // rollback the table
            rollback(metadata);

            // verify all files have been deleted
            assertTrue(listAllDataFiles(outputHandle).isEmpty());

            // verify table is not in the metastore
            assertNull(metadata.getTableHandle(session, temporaryCreateRollbackTable));
        }
        finally {
            dropTable(temporaryCreateRollbackTable);
        }
    }

    @Test
    public void testInsert()
            throws Exception
    {
        for (HiveStorageFormat storageFormat : createTableFormats) {
            try {
                doInsert(storageFormat, temporaryInsertTable);
            }
            finally {
                dropTable(temporaryInsertTable);
            }
        }
    }

    @Test
    public void testInsertIntoNewPartition()
            throws Exception
    {
        for (HiveStorageFormat storageFormat : createTableFormats) {
            try {
                doInsertIntoNewPartition(storageFormat, temporaryInsertIntoNewPartitionTable);
            }
            finally {
                dropTable(temporaryInsertIntoNewPartitionTable);
            }
        }
    }

    @Test
    public void testInsertIntoExistingPartition()
            throws Exception
    {
        for (HiveStorageFormat storageFormat : createTableFormats) {
            try {
                doInsertIntoExistingPartition(storageFormat, temporaryInsertIntoExistingPartitionTable);
            }
            finally {
                dropTable(temporaryInsertIntoExistingPartitionTable);
            }
        }
    }

    @Test
    public void testInsertUnsupportedWriteType()
            throws Exception
    {
        try {
            doInsertUnsupportedWriteType(ORC, temporaryInsertUnsupportedWriteType);
        }
        finally {
            dropTable(temporaryInsertUnsupportedWriteType);
        }
    }

    @Test
    public void testMetadataDelete()
            throws Exception
    {
        for (HiveStorageFormat storageFormat : createTableFormats) {
            try {
                doMetadataDelete(storageFormat, temporaryMetadataDeleteTable);
            }
            finally {
                dropTable(temporaryMetadataDeleteTable);
            }
        }
    }

    @Test
    public void testSampledTableCreation()
            throws Exception
    {
        try {
            doCreateSampledTable(temporaryCreateSampledTable);
        }
        finally {
            dropTable(temporaryCreateSampledTable);
        }
    }

    @Test
    public void testEmptyTableCreation()
            throws Exception
    {
        for (HiveStorageFormat storageFormat : createTableFormats) {
            try {
                doCreateEmptyTable(temporaryCreateEmptyTable, storageFormat, CREATE_TABLE_COLUMNS);
            }
            finally {
                dropTable(temporaryCreateEmptyTable);
            }
        }
    }

    @Test
    public void testViewCreation()
    {
        try {
            verifyViewCreation();
        }
        finally {
            try {
                newMetadata().dropView(newSession(), temporaryCreateView);
            }
            catch (RuntimeException e) {
                // this usually occurs because the view was not created
            }
        }
    }

    @Test
    public void testCreateTableUnsupportedType()
    {
        for (HiveStorageFormat storageFormat : createTableFormats) {
            try {
                ConnectorSession session = newSession();
                List<ColumnMetadata> columns = ImmutableList.of(new ColumnMetadata("dummy", HYPER_LOG_LOG));
                ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(invalidTable, columns, createTableProperties(storageFormat), session.getUser());
                newMetadata().beginCreateTable(session, tableMetadata, Optional.empty());
                fail("create table with unsupported type should fail for storage format " + storageFormat);
            }
            catch (PrestoException e) {
                assertEquals(e.getErrorCode(), NOT_SUPPORTED.toErrorCode());
            }
        }
    }

    private void createDummyTable(SchemaTableName tableName)
    {
        ConnectorSession session = newSession();
        ConnectorMetadata metadata = newMetadata();

        List<ColumnMetadata> columns = ImmutableList.of(new ColumnMetadata("dummy", VARCHAR));
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(tableName, columns, createTableProperties(TEXTFILE), session.getUser());
        ConnectorOutputTableHandle handle = metadata.beginCreateTable(session, tableMetadata, Optional.empty());
        metadata.finishCreateTable(session, handle, ImmutableList.of());
    }

    private void verifyViewCreation()
    {
        // replace works for new view
        doCreateView(temporaryCreateView, true);

        // replace works for existing view
        doCreateView(temporaryCreateView, true);

        // create fails for existing view
        try {
            doCreateView(temporaryCreateView, false);
            fail("create existing should fail");
        }
        catch (ViewAlreadyExistsException e) {
            assertEquals(e.getViewName(), temporaryCreateView);
        }

        ConnectorMetadata metadata = newMetadata();

        // drop works when view exists
        metadata.dropView(newSession(), temporaryCreateView);
        assertEquals(metadata.getViews(newSession(), temporaryCreateView.toSchemaTablePrefix()).size(), 0);
        assertFalse(metadata.listViews(newSession(), temporaryCreateView.getSchemaName()).contains(temporaryCreateView));

        // drop fails when view does not exist
        try {
            metadata.dropView(newSession(), temporaryCreateView);
            fail("drop non-existing should fail");
        }
        catch (ViewNotFoundException e) {
            assertEquals(e.getViewName(), temporaryCreateView);
        }

        // create works for new view
        doCreateView(temporaryCreateView, false);
    }

    private void doCreateView(SchemaTableName viewName, boolean replace)
    {
        String viewData = "test data";
        ConnectorMetadata metadata = newMetadata();

        metadata.createView(newSession(), viewName, viewData, replace);

        Map<SchemaTableName, ConnectorViewDefinition> views = metadata.getViews(newSession(), viewName.toSchemaTablePrefix());
        assertEquals(views.size(), 1);
        assertEquals(views.get(viewName).getViewData(), viewData);

        assertTrue(metadata.listViews(newSession(), viewName.getSchemaName()).contains(viewName));
    }

    protected void doCreateSampledTable(SchemaTableName tableName)
            throws Exception
    {
        ConnectorSession session = newSession();
        ConnectorTransactionHandle transaction = newTransaction();
        ConnectorMetadata metadata = newMetadata();

        // begin creating the table
        List<ColumnMetadata> columns = ImmutableList.<ColumnMetadata>builder()
                .add(new ColumnMetadata("sales", BIGINT))
                .build();

        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(tableName, columns, createTableProperties(RCBINARY), session.getUser(), true);
        ConnectorOutputTableHandle outputHandle = metadata.beginCreateTable(session, tableMetadata, Optional.empty());

        // write the records
        ConnectorPageSink sink = pageSinkProvider.createPageSink(transaction, session, outputHandle);

        BlockBuilder sampleBlockBuilder = BIGINT.createBlockBuilder(new BlockBuilderStatus(), 3);
        BlockBuilder dataBlockBuilder = BIGINT.createBlockBuilder(new BlockBuilderStatus(), 3);

        BIGINT.writeLong(sampleBlockBuilder, 8);
        BIGINT.writeLong(dataBlockBuilder, 2);

        BIGINT.writeLong(sampleBlockBuilder, 5);
        BIGINT.writeLong(dataBlockBuilder, 3);

        BIGINT.writeLong(sampleBlockBuilder, 7);
        BIGINT.writeLong(dataBlockBuilder, 4);

        sink.appendPage(new Page(dataBlockBuilder.build()), sampleBlockBuilder.build());

        Collection<Slice> fragments = sink.finish();

        // commit the table
        metadata.finishCreateTable(session, outputHandle, fragments);

        // load the new table
        ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);
        List<ColumnHandle> columnHandles = ImmutableList.<ColumnHandle>builder()
                .addAll(metadata.getColumnHandles(session, tableHandle).values())
                .add(metadata.getSampleWeightColumnHandle(session, tableHandle))
                .build();
        assertEquals(columnHandles.size(), 2);

        // verify the metadata
        tableMetadata = metadata.getTableMetadata(session, getTableHandle(metadata, tableName));
        assertEquals(tableMetadata.getOwner(), session.getUser());

        Map<String, ColumnMetadata> columnMap = uniqueIndex(tableMetadata.getColumns(), ColumnMetadata::getName);
        assertEquals(columnMap.size(), 1);

        assertPrimitiveField(columnMap, "sales", BIGINT, false);

        // verify the data
        List<ConnectorTableLayoutResult> tableLayoutResults = metadata.getTableLayouts(session, tableHandle, new Constraint<>(TupleDomain.all(), bindings -> true), Optional.empty());
        ConnectorTableLayoutHandle layoutHandle = getOnlyElement(tableLayoutResults).getTableLayout().getHandle();
        assertEquals(getAllPartitions(layoutHandle).size(), 1);
        ConnectorSplitSource splitSource = splitManager.getSplits(transaction, session, layoutHandle);
        ConnectorSplit split = getOnlyElement(getAllSplits(splitSource));

        try (ConnectorPageSource pageSource = pageSourceProvider.createPageSource(transaction, session, split, columnHandles)) {
            assertPageSourceType(pageSource, RCBINARY);
            MaterializedResult result = materializeSourceDataStream(session, pageSource, getTypes(columnHandles));
            assertEquals(result.getRowCount(), 3);

            MaterializedRow row;

            row = result.getMaterializedRows().get(0);
            assertEquals(row.getField(0), 2L);
            assertEquals(row.getField(1), 8L);

            row = result.getMaterializedRows().get(1);
            assertEquals(row.getField(0), 3L);
            assertEquals(row.getField(1), 5L);

            row = result.getMaterializedRows().get(2);
            assertEquals(row.getField(0), 4L);
            assertEquals(row.getField(1), 7L);
        }
    }

    protected void doCreateTable(SchemaTableName tableName, HiveStorageFormat storageFormat)
            throws Exception
    {
        ConnectorSession session = newSession();
        ConnectorTransactionHandle transaction = newTransaction();
        ConnectorMetadata metadata = newMetadata();

        // begin creating the table
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(tableName, CREATE_TABLE_COLUMNS, createTableProperties(storageFormat), session.getUser());

        ConnectorOutputTableHandle outputHandle = metadata.beginCreateTable(session, tableMetadata, Optional.empty());

        // write the data
        ConnectorPageSink sink = pageSinkProvider.createPageSink(transaction, session, outputHandle);
        sink.appendPage(CREATE_TABLE_DATA.toPage(), null);
        Collection<Slice> fragments = sink.finish();

        // verify all new files start with the unique prefix
        for (String filePath : listAllDataFiles(outputHandle)) {
            assertTrue(new Path(filePath).getName().startsWith(getFilePrefix(outputHandle)));
        }

        // commit the table
        metadata.finishCreateTable(session, outputHandle, fragments);

        // load the new table
        ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(session, tableHandle).values());

        // verify the metadata
        tableMetadata = metadata.getTableMetadata(session, getTableHandle(metadata, tableName));
        assertEquals(tableMetadata.getOwner(), session.getUser());
        assertEquals(tableMetadata.getColumns(), CREATE_TABLE_COLUMNS);

        // verify the data
        MaterializedResult result = readTable(tableHandle, columnHandles, session, TupleDomain.all(), OptionalInt.empty(), Optional.of(storageFormat));
        assertEqualsIgnoreOrder(result.getMaterializedRows(), CREATE_TABLE_DATA.getMaterializedRows());
    }

    protected void doCreateEmptyTable(SchemaTableName tableName, HiveStorageFormat storageFormat, List<ColumnMetadata> createTableColumns)
            throws Exception
    {
        ConnectorSession session = newSession();
        ConnectorMetadata metadata = newMetadata();

        List<String> partitionedBy = createTableColumns.stream()
                .filter(column -> column.getName().equals("ds"))
                .map(ColumnMetadata::getName)
                .collect(toList());
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(tableName, createTableColumns, createTableProperties(storageFormat, partitionedBy), session.getUser());

        metadata.createTable(session, tableMetadata);

        // load the new table
        ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);

        // verify the metadata
        tableMetadata = metadata.getTableMetadata(session, getTableHandle(metadata, tableName));
        assertEquals(tableMetadata.getOwner(), session.getUser());

        List<ColumnMetadata> expectedColumns = createTableColumns.stream()
                .map(column -> new ColumnMetadata(
                        column.getName(),
                        column.getType(),
                        annotateColumnComment(column.getComment(), partitionedBy.contains(column.getName())),
                        false))
                .collect(toList());
        assertEquals(tableMetadata.getColumns(), expectedColumns);

        // verify table format
        Table table = getMetastoreClient(tableName.getSchemaName()).getTable(tableName.getSchemaName(), tableName.getTableName()).get();
        if (!table.getSd().getInputFormat().equals(storageFormat.getInputFormat())) {
            assertEquals(table.getSd().getInputFormat(), storageFormat.getInputFormat());
        }

        // verify the table is empty
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(session, tableHandle).values());
        MaterializedResult result = readTable(tableHandle, columnHandles, session, TupleDomain.all(), OptionalInt.empty(), Optional.of(storageFormat));
        assertEquals(result.getRowCount(), 0);
    }

    private void doInsert(HiveStorageFormat storageFormat, SchemaTableName tableName)
            throws Exception
    {
        // creating the table
        doCreateEmptyTable(tableName, storageFormat, CREATE_TABLE_COLUMNS);

        MaterializedResult.Builder resultBuilder = MaterializedResult.resultBuilder(SESSION, CREATE_TABLE_DATA.getTypes());
        for (int i = 0; i < 3; i++) {
            ConnectorSession session = newSession();
            ConnectorTransactionHandle transaction = newTransaction();
            ConnectorMetadata metadata = newMetadata();

            // begin the insert
            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);
            ConnectorInsertTableHandle insertTableHandle = metadata.beginInsert(session, tableHandle);

            ConnectorPageSink sink = pageSinkProvider.createPageSink(transaction, session, insertTableHandle);

            // write data
            sink.appendPage(CREATE_TABLE_DATA.toPage(), null);
            Collection<Slice> fragments = sink.finish();

            // commit the insert
            metadata.finishInsert(session, insertTableHandle, fragments);

            // load the new table
            List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(session, tableHandle).values());

            // verify the metadata
            ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(session, getTableHandle(metadata, tableName));
            assertEquals(tableMetadata.getOwner(), session.getUser());
            assertEquals(tableMetadata.getColumns(), CREATE_TABLE_COLUMNS);

            // verify the data
            resultBuilder.rows(CREATE_TABLE_DATA.getMaterializedRows());
            MaterializedResult result = readTable(tableHandle, columnHandles, session, TupleDomain.<ColumnHandle>all(), OptionalInt.empty(), Optional.empty());
            assertEqualsIgnoreOrder(result.getMaterializedRows(), resultBuilder.build().getMaterializedRows());
        }

        // test rollback
        Set<String> existingFiles = listAllDataFiles(tableName.getSchemaName(), tableName.getTableName());
        assertFalse(existingFiles.isEmpty());

        ConnectorSession session = newSession();
        ConnectorTransactionHandle transaction = newTransaction();
        ConnectorMetadata metadata = newMetadata();
        ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(session, tableHandle).values());

        // "stage" insert data
        ConnectorInsertTableHandle insertTableHandle = metadata.beginInsert(session, tableHandle);
        ConnectorPageSink sink = pageSinkProvider.createPageSink(transaction, session, insertTableHandle);
        sink.appendPage(CREATE_TABLE_DATA.toPage(), null);
        sink.appendPage(CREATE_TABLE_DATA.toPage(), null);
        sink.finish();

        // verify we did not modify the table directory
        assertEquals(listAllDataFiles(tableName.getSchemaName(), tableName.getTableName()), existingFiles);

        // verify all temp files start with the unique prefix
        Set<String> tempFiles = listAllDataFiles(insertTableHandle);
        assertTrue(!tempFiles.isEmpty());
        for (String filePath : tempFiles) {
            assertTrue(new Path(filePath).getName().startsWith(getFilePrefix(insertTableHandle)));
        }

        // rollback insert
        rollback(metadata);

        // verify the data is unchanged
        MaterializedResult result = readTable(tableHandle, columnHandles, session, TupleDomain.<ColumnHandle>all(), OptionalInt.empty(), Optional.empty());
        assertEqualsIgnoreOrder(result.getMaterializedRows(), resultBuilder.build().getMaterializedRows());

        // verify we did not modify the table directory
        assertEquals(listAllDataFiles(tableName.getSchemaName(), tableName.getTableName()), existingFiles);

        // verify temp directory is empty
        assertTrue(listAllDataFiles(insertTableHandle).isEmpty());
    }

    // These are protected so extensions to the hive connector can replace the handle classes
    protected String getFilePrefix(ConnectorOutputTableHandle outputTableHandle)
    {
        return ((HiveOutputTableHandle) outputTableHandle).getFilePrefix();
    }

    protected String getFilePrefix(ConnectorInsertTableHandle insertTableHandle)
    {
        return ((HiveInsertTableHandle) insertTableHandle).getFilePrefix();
    }

    protected Set<String> listAllDataFiles(ConnectorOutputTableHandle tableHandle)
            throws IOException
    {
        HiveOutputTableHandle hiveOutputTableHandle = (HiveOutputTableHandle) tableHandle;
        Path writePath = new Path(getLocationService(hiveOutputTableHandle.getSchemaName()).writePathRoot(hiveOutputTableHandle.getLocationHandle()).get().toString());
        return listAllDataFiles(writePath);
    }

    protected Set<String> listAllDataFiles(ConnectorInsertTableHandle tableHandle)
            throws IOException
    {
        HiveInsertTableHandle hiveInsertTableHandle = (HiveInsertTableHandle) tableHandle;
        Path writePath = new Path(getLocationService(hiveInsertTableHandle.getSchemaName()).writePathRoot(hiveInsertTableHandle.getLocationHandle()).get().toString());
        return listAllDataFiles(writePath);
    }

    protected Set<String> listAllDataFiles(String schemaName, String tableName)
            throws IOException
    {
        Set<String> existingFiles = new HashSet<>();
        for (String location : listAllDataPaths(getMetastoreClient(schemaName), schemaName, tableName)) {
            existingFiles.addAll(listAllDataFiles(new Path(location)));
        }
        return existingFiles;
    }

    public static List<String> listAllDataPaths(HiveMetastore metastore, String schemaName, String tableName)
    {
        ImmutableList.Builder<String> locations = ImmutableList.builder();
        Table table = metastore.getTable(schemaName, tableName).get();
        if (table.getSd().getLocation() != null) {
            // For unpartitioned table, there should be nothing directly under this directory.
            // But including this location in the set makes the directory content assert more
            // extensive, which is desirable.
            locations.add(table.getSd().getLocation());
        }

        Optional<List<String>> partitionNames = metastore.getPartitionNames(schemaName, tableName);
        if (partitionNames.isPresent()) {
            metastore.getPartitionsByNames(schemaName, tableName, partitionNames.get()).get().values().stream()
                    .map(partition -> partition.getSd().getLocation())
                    .filter(location -> !location.startsWith(table.getSd().getLocation()))
                    .forEach(locations::add);
        }

        return locations.build();
    }

    protected Set<String> listAllDataFiles(Path path)
            throws IOException
    {
        Set<String> result = new HashSet<>();
        FileSystem fileSystem = hdfsEnvironment.getFileSystem(path);
        if (fileSystem.exists(path)) {
            for (FileStatus fileStatus : fileSystem.listStatus(path)) {
                if (HadoopFileStatus.isFile(fileStatus)) {
                    result.add(fileStatus.getPath().toString());
                }
                else if (HadoopFileStatus.isDirectory(fileStatus)) {
                    result.addAll(listAllDataFiles(fileStatus.getPath()));
                }
            }
        }
        return result;
    }

    private void doInsertIntoNewPartition(HiveStorageFormat storageFormat, SchemaTableName tableName)
            throws Exception
    {
        // creating the table
        doCreateEmptyTable(tableName, storageFormat, CREATE_TABLE_COLUMNS_PARTITIONED);

        ConnectorMetadata metadata = newMetadata();
        ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);

        // insert the data
        insertData(tableHandle, CREATE_TABLE_PARTITIONED_DATA, newSession());

        // verify partitions were created
        List<String> partitionNames = getMetastoreClient(tableName.getSchemaName()).getPartitionNames(tableName.getSchemaName(), tableName.getTableName())
                .orElseThrow(() -> new PrestoException(HIVE_METASTORE_ERROR, "Partition metadata not available"));
        assertEqualsIgnoreOrder(partitionNames, CREATE_TABLE_PARTITIONED_DATA.getMaterializedRows().stream()
                .map(row -> "ds=" + row.getField(CREATE_TABLE_PARTITIONED_DATA.getTypes().size() - 1))
                .collect(toList()));

        // load the new table
        ConnectorSession session = newSession();
        metadata = newMetadata();
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(session, tableHandle).values());

        // verify the data
        MaterializedResult result = readTable(tableHandle, columnHandles, session, TupleDomain.all(), OptionalInt.empty(), Optional.of(storageFormat));
        assertEqualsIgnoreOrder(result.getMaterializedRows(), CREATE_TABLE_PARTITIONED_DATA.getMaterializedRows());

        // test rollback
        Set<String> existingFiles = listAllDataFiles(tableName.getSchemaName(), tableName.getTableName());
        assertFalse(existingFiles.isEmpty());

        session = newSession();
        ConnectorTransactionHandle transaction = newTransaction();
        metadata = newMetadata();

        // "stage" insert data
        ConnectorInsertTableHandle insertTableHandle = metadata.beginInsert(session, tableHandle);
        ConnectorPageSink sink = pageSinkProvider.createPageSink(transaction, session, insertTableHandle);
        sink.appendPage(CREATE_TABLE_PARTITIONED_DATA_2ND.toPage(), null);
        sink.finish();

        // verify we did not modify the table directory
        assertEquals(listAllDataFiles(tableName.getSchemaName(), tableName.getTableName()), existingFiles);

        // verify all temp files start with the unique prefix
        Set<String> tempFiles = listAllDataFiles(insertTableHandle);
        assertTrue(!tempFiles.isEmpty());
        for (String filePath : tempFiles) {
            assertTrue(new Path(filePath).getName().startsWith(getFilePrefix(insertTableHandle)));
        }

        // rollback insert
        rollback(metadata);

        // verify the data is unchanged
        result = readTable(tableHandle, columnHandles, newSession(), TupleDomain.<ColumnHandle>all(), OptionalInt.empty(), Optional.empty());
        assertEqualsIgnoreOrder(result.getMaterializedRows(), CREATE_TABLE_PARTITIONED_DATA.getMaterializedRows());

        // verify we did not modify the table directory
        assertEquals(listAllDataFiles(tableName.getSchemaName(), tableName.getTableName()), existingFiles);

        // verify temp directory is empty
        assertTrue(listAllDataFiles(insertTableHandle).isEmpty());
    }

    private void doInsertUnsupportedWriteType(HiveStorageFormat storageFormat, SchemaTableName tableName)
            throws Exception
    {
        List<FieldSchema> columns = ImmutableList.of(new FieldSchema("dummy", "int", null));
        List<FieldSchema> partitionColumns = ImmutableList.of(new FieldSchema("name", "string", null));

        createEmptyTable(tableName, storageFormat, columns, partitionColumns);

        ConnectorMetadata metadata = newMetadata();
        ConnectorSession session = newSession();
        ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);
        try {
            metadata.beginInsert(session, tableHandle);
            fail("expected failure");
        }
        catch (PrestoException e) {
            assertEquals(e.getMessage(), "Inserting into Hive table with column type int not supported");
        }
    }

    private void doInsertIntoExistingPartition(HiveStorageFormat storageFormat, SchemaTableName tableName)
            throws Exception
    {
        // creating the table
        doCreateEmptyTable(tableName, storageFormat, CREATE_TABLE_COLUMNS_PARTITIONED);

        MaterializedResult.Builder resultBuilder = MaterializedResult.resultBuilder(SESSION, CREATE_TABLE_PARTITIONED_DATA.getTypes());
        for (int i = 0; i < 3; i++) {
            ConnectorMetadata metadata = newMetadata();
            ConnectorSession session = newSession();

            ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);

            // insert the data
            insertData(tableHandle, CREATE_TABLE_PARTITIONED_DATA, newSession());

            // verify partitions were created
            List<String> partitionNames = getMetastoreClient(tableName.getSchemaName()).getPartitionNames(tableName.getSchemaName(), tableName.getTableName())
                    .orElseThrow(() -> new PrestoException(HIVE_METASTORE_ERROR, "Partition metadata not available"));
            assertEqualsIgnoreOrder(partitionNames, CREATE_TABLE_PARTITIONED_DATA.getMaterializedRows().stream()
                    .map(row -> "ds=" + row.getField(CREATE_TABLE_PARTITIONED_DATA.getTypes().size() - 1))
                    .collect(toList()));

            // load the new table
            List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(session, tableHandle).values());

            // verify the data
            resultBuilder.rows(CREATE_TABLE_PARTITIONED_DATA.getMaterializedRows());
            MaterializedResult result = readTable(tableHandle, columnHandles, session, TupleDomain.all(), OptionalInt.empty(), Optional.of(storageFormat));
            assertEqualsIgnoreOrder(result.getMaterializedRows(), resultBuilder.build().getMaterializedRows());
        }

        // test rollback
        Set<String> existingFiles = listAllDataFiles(tableName.getSchemaName(), tableName.getTableName());
        assertFalse(existingFiles.isEmpty());

        ConnectorMetadata metadata = newMetadata();
        ConnectorTransactionHandle transaction = newTransaction();
        ConnectorSession session = newSession();
        ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(session, tableHandle).values());

        // "stage" insert data
        ConnectorInsertTableHandle insertTableHandle = metadata.beginInsert(session, tableHandle);
        ConnectorPageSink sink = pageSinkProvider.createPageSink(transaction, session, insertTableHandle);
        sink.appendPage(CREATE_TABLE_PARTITIONED_DATA.toPage(), null);
        sink.appendPage(CREATE_TABLE_PARTITIONED_DATA.toPage(), null);
        sink.finish();

        // verify we did not modify the table directory
        assertEquals(listAllDataFiles(tableName.getSchemaName(), tableName.getTableName()), existingFiles);

        // verify all temp files start with the unique prefix
        Set<String> tempFiles = listAllDataFiles(insertTableHandle);
        assertTrue(!tempFiles.isEmpty());
        for (String filePath : tempFiles) {
            assertTrue(new Path(filePath).getName().startsWith(getFilePrefix(insertTableHandle)));
        }

        // rollback insert
        rollback(metadata);

        // verify the data is unchanged
        MaterializedResult result = readTable(tableHandle, columnHandles, newSession(), TupleDomain.<ColumnHandle>all(), OptionalInt.empty(), Optional.empty());
        assertEqualsIgnoreOrder(result.getMaterializedRows(), resultBuilder.build().getMaterializedRows());

        // verify we did not modify the table directory
        assertEquals(listAllDataFiles(tableName.getSchemaName(), tableName.getTableName()), existingFiles);

        // verify temp directory is empty
        assertTrue(listAllDataFiles(insertTableHandle).isEmpty());
    }

    private void insertData(ConnectorTableHandle tableHandle, MaterializedResult data, ConnectorSession session)
    {
        ConnectorMetadata metadata = newMetadata();
        ConnectorTransactionHandle transaction = newTransaction();
        ConnectorInsertTableHandle insertTableHandle = metadata.beginInsert(session, tableHandle);

        ConnectorPageSink sink = pageSinkProvider.createPageSink(transaction, session, insertTableHandle);

        // write data
        sink.appendPage(data.toPage(), null);
        Collection<Slice> fragments = sink.finish();

        // commit the insert
        metadata.finishInsert(session, insertTableHandle, fragments);
    }

    private void doMetadataDelete(HiveStorageFormat storageFormat, SchemaTableName tableName)
            throws Exception
    {
        // creating the table
        doCreateEmptyTable(tableName, storageFormat, CREATE_TABLE_COLUMNS_PARTITIONED);

        // verify table directory is empty
        Set<String> initialFiles = listAllDataFiles(tableName.getSchemaName(), tableName.getTableName());
        assertTrue(initialFiles.isEmpty());

        MaterializedResult.Builder expectedResultBuilder = MaterializedResult.resultBuilder(SESSION, CREATE_TABLE_PARTITIONED_DATA.getTypes());
        expectedResultBuilder.rows(CREATE_TABLE_PARTITIONED_DATA.getMaterializedRows());

        ConnectorSession session = newSession();
        ConnectorMetadata metadata = newMetadata();

        ConnectorTableHandle tableHandle = getTableHandle(metadata, tableName);
        insertData(tableHandle, CREATE_TABLE_PARTITIONED_DATA, newSession());

        // verify partitions were created
        List<String> partitionNames = getMetastoreClient(tableName.getSchemaName()).getPartitionNames(tableName.getSchemaName(), tableName.getTableName())
                .orElseThrow(() -> new PrestoException(HIVE_METASTORE_ERROR, "Partition metadata not available"));
        assertEqualsIgnoreOrder(partitionNames, CREATE_TABLE_PARTITIONED_DATA.getMaterializedRows().stream()
                .map(row -> "ds=" + row.getField(CREATE_TABLE_PARTITIONED_DATA.getTypes().size() - 1))
                .collect(toList()));

        // verify table directory is not empty
        Set<String> filesAfterInsert = listAllDataFiles(tableName.getSchemaName(), tableName.getTableName());
        assertFalse(filesAfterInsert.isEmpty());

        // verify the data
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(session, tableHandle).values());
        MaterializedResult result = readTable(tableHandle, columnHandles, session, TupleDomain.all(), OptionalInt.empty(), Optional.of(storageFormat));
        assertEqualsIgnoreOrder(result.getMaterializedRows(), expectedResultBuilder.build().getMaterializedRows());

        // get ds column handle
        Map<String, HiveColumnHandle> columnHandleMap = columnHandles.stream()
                .map(columnHandle -> (HiveColumnHandle) columnHandle)
                .collect(Collectors.toMap(HiveColumnHandle::getName, Function.identity()));
        HiveColumnHandle dsColumnHandle = columnHandleMap.get("ds");
        int dsColumnOrdinalPosition = columnHandles.indexOf(dsColumnHandle);

        // delete ds=2015-07-03
        session = newSession();
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.fromFixedValues(ImmutableMap.of(dsColumnHandle, NullableValue.of(VARCHAR, utf8Slice("2015-07-03"))));
        Constraint<ColumnHandle> constraint = new Constraint<>(tupleDomain, convertToPredicate(tupleDomain));
        List<ConnectorTableLayoutResult> tableLayoutResults = metadata.getTableLayouts(session, tableHandle, constraint, Optional.empty());
        ConnectorTableLayoutHandle tableLayoutHandle = Iterables.getOnlyElement(tableLayoutResults).getTableLayout().getHandle();
        metadata.metadataDelete(session, tableHandle, tableLayoutHandle);
        // verify the data
        session = newSession();
        ImmutableList<MaterializedRow> expectedRows = expectedResultBuilder.build().getMaterializedRows().stream()
                .filter(row -> !"2015-07-03".equals(row.getField(dsColumnOrdinalPosition)))
                .collect(ImmutableCollectors.toImmutableList());
        MaterializedResult actualAfterDelete = readTable(tableHandle, columnHandles, session, TupleDomain.all(), OptionalInt.empty(), Optional.of(storageFormat));
        assertEqualsIgnoreOrder(actualAfterDelete.getMaterializedRows(), expectedRows);

        // delete ds=2015-07-01 and 2015-07-02
        session = newSession();
        TupleDomain<ColumnHandle> tupleDomain2 = TupleDomain.withColumnDomains(
                ImmutableMap.of(dsColumnHandle, Domain.create(ValueSet.ofRanges(Range.range(VARCHAR, utf8Slice("2015-07-01"), true, utf8Slice("2015-07-02"), true)), false)));
        Constraint<ColumnHandle> constraint2 = new Constraint<>(tupleDomain2, convertToPredicate(tupleDomain2));
        List<ConnectorTableLayoutResult> tableLayoutResults2 = metadata.getTableLayouts(session, tableHandle, constraint2, Optional.empty());
        ConnectorTableLayoutHandle tableLayoutHandle2 = Iterables.getOnlyElement(tableLayoutResults2).getTableLayout().getHandle();
        metadata.metadataDelete(session, tableHandle, tableLayoutHandle2);
        // verify the data
        session = newSession();
        MaterializedResult actualAfterDelete2 = readTable(tableHandle, columnHandles, session, TupleDomain.all(), OptionalInt.empty(), Optional.of(storageFormat));
        assertEqualsIgnoreOrder(actualAfterDelete2.getMaterializedRows(), ImmutableList.of());

        // verify table directory is empty
        Set<String> filesAfterDelete = listAllDataFiles(tableName.getSchemaName(), tableName.getTableName());
        assertTrue(filesAfterDelete.isEmpty());
    }

    protected void assertGetRecordsOptional(String tableName, HiveStorageFormat hiveStorageFormat)
            throws Exception
    {
        if (newMetadata().getTableHandle(newSession(), new SchemaTableName(database, tableName)) != null) {
            assertGetRecords(tableName, hiveStorageFormat);
        }
    }

    protected void assertGetRecords(String tableName, HiveStorageFormat hiveStorageFormat)
            throws Exception
    {
        ConnectorSession session = newSession();
        ConnectorTransactionHandle transaction = newTransaction();
        ConnectorMetadata metadata = newMetadata();

        ConnectorTableHandle tableHandle = getTableHandle(metadata, new SchemaTableName(database, tableName));
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(session, tableHandle);
        HiveSplit hiveSplit = getHiveSplit(tableHandle);

        List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(session, tableHandle).values());

        ConnectorPageSource pageSource = pageSourceProvider.createPageSource(transaction, session, hiveSplit, columnHandles);
        assertGetRecords(hiveStorageFormat, tableMetadata, hiveSplit, pageSource, columnHandles);
    }

    protected HiveSplit getHiveSplit(ConnectorTableHandle tableHandle)
            throws InterruptedException
    {
        List<ConnectorSplit> splits = getAllSplits(tableHandle, TupleDomain.all());
        assertEquals(splits.size(), 1);
        return checkType(getOnlyElement(splits), HiveSplit.class, "split");
    }

    protected void assertGetRecords(
            HiveStorageFormat hiveStorageFormat,
            ConnectorTableMetadata tableMetadata,
            HiveSplit hiveSplit,
            ConnectorPageSource pageSource,
            List<? extends ColumnHandle> columnHandles)
            throws IOException
    {
        try {
            MaterializedResult result = materializeSourceDataStream(newSession(), pageSource, getTypes(columnHandles));

            assertPageSourceType(pageSource, hiveStorageFormat);

            ImmutableMap<String, Integer> columnIndex = indexColumns(tableMetadata);

            long rowNumber = 0;
            long completedBytes = 0;
            for (MaterializedRow row : result) {
                try {
                    assertValueTypes(row, tableMetadata.getColumns());
                }
                catch (RuntimeException e) {
                    throw new RuntimeException("row " + rowNumber, e);
                }

                rowNumber++;
                Integer index;

                // STRING
                index = columnIndex.get("t_string");
                if ((rowNumber % 19) == 0) {
                    assertNull(row.getField(index));
                }
                else {
                    assertEquals(row.getField(index), ((rowNumber % 19) == 1) ? "" : "test");
                }

                // NUMBERS
                assertEquals(row.getField(columnIndex.get("t_tinyint")), 1 + rowNumber);
                assertEquals(row.getField(columnIndex.get("t_smallint")), 2 + rowNumber);
                assertEquals(row.getField(columnIndex.get("t_int")), 3 + rowNumber);

                index = columnIndex.get("t_bigint");
                if ((rowNumber % 13) == 0) {
                    assertNull(row.getField(index));
                }
                else {
                    assertEquals(row.getField(index), 4 + rowNumber);
                }

                assertEquals((Double) row.getField(columnIndex.get("t_float")), 5.1 + rowNumber, 0.001);
                assertEquals(row.getField(columnIndex.get("t_double")), 6.2 + rowNumber);

                // BOOLEAN
                index = columnIndex.get("t_boolean");
                if ((rowNumber % 3) == 2) {
                    assertNull(row.getField(index));
                }
                else {
                    assertEquals(row.getField(index), (rowNumber % 3) != 0);
                }

                // TIMESTAMP
                index = columnIndex.get("t_timestamp");
                if (index != null) {
                    if ((rowNumber % 17) == 0) {
                        assertNull(row.getField(index));
                    }
                    else {
                        SqlTimestamp expected = new SqlTimestamp(new DateTime(2011, 5, 6, 7, 8, 9, 123, timeZone).getMillis(), UTC_KEY);
                        assertEquals(row.getField(index), expected);
                    }
                }

                // BINARY
                index = columnIndex.get("t_binary");
                if (index != null) {
                    if ((rowNumber % 23) == 0) {
                        assertNull(row.getField(index));
                    }
                    else {
                        assertEquals(row.getField(index), new SqlVarbinary("test binary".getBytes(UTF_8)));
                    }
                }

                // DATE
                index = columnIndex.get("t_date");
                if (index != null) {
                    if ((rowNumber % 37) == 0) {
                        assertNull(row.getField(index));
                    }
                    else {
                        SqlDate expected = new SqlDate(Ints.checkedCast(TimeUnit.MILLISECONDS.toDays(new DateTime(2013, 8, 9, 0, 0, 0, DateTimeZone.UTC).getMillis())));
                        assertEquals(row.getField(index), expected);
                    }
                }

                /* TODO: enable these tests when the types are supported
                // VARCHAR(50)
                index = columnIndex.get("t_varchar");
                if (index != null) {
                    if ((rowNumber % 39) == 0) {
                        assertTrue(cursor.isNull(index));
                    }
                    else {
                        String stringValue = cursor.getSlice(index).toStringUtf8();
                        assertEquals(stringValue, ((rowNumber % 39) == 1) ? "" : "test varchar");
                    }
                }

                // CHAR(25)
                index = columnIndex.get("t_char");
                if (index != null) {
                    if ((rowNumber % 41) == 0) {
                        assertTrue(cursor.isNull(index));
                    }
                    else {
                        String stringValue = cursor.getSlice(index).toStringUtf8();
                        assertEquals(stringValue, ((rowNumber % 41) == 1) ? "" : "test char");
                    }
                }
                */

                // MAP<STRING, STRING>
                index = columnIndex.get("t_map");
                if (index != null) {
                    if ((rowNumber % 27) == 0) {
                        assertNull(row.getField(index));
                    }
                    else {
                        assertEquals(row.getField(index), ImmutableMap.of("test key", "test value"));
                    }
                }

                // ARRAY<STRING>
                index = columnIndex.get("t_array_string");
                if (index != null) {
                    if ((rowNumber % 29) == 0) {
                        assertNull(row.getField(index));
                    }
                    else {
                        assertEquals(row.getField(index), ImmutableList.of("abc", "xyz", "data"));
                    }
                }

                // ARRAY<STRUCT<s_string: STRING, s_double:DOUBLE>>
                index = columnIndex.get("t_array_struct");
                if (index != null) {
                    if ((rowNumber % 31) == 0) {
                        assertNull(row.getField(index));
                    }
                    else {
                        List<Object> expected1 = ImmutableList.<Object>of("test abc", 0.1);
                        List<Object> expected2 = ImmutableList.<Object>of("test xyz", 0.2);
                        assertEquals(row.getField(index), ImmutableList.of(expected1, expected2));
                    }
                }

                // MAP<INT, ARRAY<STRUCT<s_string: STRING, s_double:DOUBLE>>>
                index = columnIndex.get("t_complex");
                if (index != null) {
                    if ((rowNumber % 33) == 0) {
                        assertNull(row.getField(index));
                    }
                    else {
                        List<Object> expected1 = ImmutableList.<Object>of("test abc", 0.1);
                        List<Object> expected2 = ImmutableList.<Object>of("test xyz", 0.2);
                        assertEquals(row.getField(index), ImmutableMap.of(1L, ImmutableList.of(expected1, expected2)));
                    }
                }

                // NEW COLUMN
                assertNull(row.getField(columnIndex.get("new_column")));

                long newCompletedBytes = pageSource.getCompletedBytes();
                assertTrue(newCompletedBytes >= completedBytes);
                assertTrue(newCompletedBytes <= hiveSplit.getLength());
                completedBytes = newCompletedBytes;
            }

            assertTrue(completedBytes <= hiveSplit.getLength());
            assertEquals(rowNumber, 100);
        }
        finally {
            pageSource.close();
        }
    }

    protected void dropTable(SchemaTableName table)
    {
        try {
            ConnectorMetadata metadata = newMetadata();
            ConnectorSession session = newSession();

            ConnectorTableHandle handle = metadata.getTableHandle(session, table);
            if (handle == null) {
                return;
            }

            metadata.dropTable(session, handle);
            try {
                // todo I have no idea why this is needed... maybe there is a propagation delay in the metastore?
                metadata.dropTable(session, handle);
                fail("expected NotFoundException");
            }
            catch (TableNotFoundException expected) {
            }
        }
        catch (Exception e) {
            Logger.get(getClass()).warn(e, "failed to drop table");
        }
    }

    protected ConnectorTableHandle getTableHandle(ConnectorMetadata metadata, SchemaTableName tableName)
    {
        ConnectorTableHandle handle = metadata.getTableHandle(newSession(), tableName);
        checkArgument(handle != null, "table not found: %s", tableName);
        return handle;
    }

    private MaterializedResult readTable(
            ConnectorTableHandle tableHandle,
            List<ColumnHandle> columnHandles,
            ConnectorSession session,
            TupleDomain<ColumnHandle> tupleDomain,
            OptionalInt expectedSplitCount,
            Optional<HiveStorageFormat> expectedStorageFormat)
            throws Exception
    {
        ConnectorMetadata metadata = newMetadata();
        ConnectorTransactionHandle transactionHandle = newTransaction();

        List<ConnectorTableLayoutResult> tableLayoutResults = metadata.getTableLayouts(session, tableHandle, new Constraint<>(tupleDomain, bindings -> true), Optional.empty());
        ConnectorTableLayoutHandle layoutHandle = getOnlyElement(tableLayoutResults).getTableLayout().getHandle();
        List<ConnectorSplit> splits = getAllSplits(splitManager.getSplits(transactionHandle, session, layoutHandle));
        if (expectedSplitCount.isPresent()) {
            assertEquals(splits.size(), expectedSplitCount.getAsInt());
        }

        ImmutableList.Builder<MaterializedRow> allRows = ImmutableList.builder();
        for (ConnectorSplit split : splits) {
            try (ConnectorPageSource pageSource = pageSourceProvider.createPageSource(transactionHandle, session, split, columnHandles)) {
                if (expectedStorageFormat.isPresent()) {
                    assertPageSourceType(pageSource, expectedStorageFormat.get());
                }
                MaterializedResult result = materializeSourceDataStream(session, pageSource, getTypes(columnHandles));
                allRows.addAll(result.getMaterializedRows());
            }
        }
        return new MaterializedResult(allRows.build(), getTypes(columnHandles));
    }

    public HiveMetastore getMetastoreClient(String namespace)
    {
        return metastoreClient;
    }

    public LocationService getLocationService(String namespace)
    {
        return locationService;
    }

    protected static int getSplitCount(ConnectorSplitSource splitSource)
            throws InterruptedException
    {
        int splitCount = 0;
        while (!splitSource.isFinished()) {
            List<ConnectorSplit> batch = getFutureValue(splitSource.getNextBatch(1000));
            splitCount += batch.size();
        }
        return splitCount;
    }

    private List<ConnectorSplit> getAllSplits(ConnectorTableHandle tableHandle, TupleDomain<ColumnHandle> tupleDomain)
            throws InterruptedException
    {
        ConnectorSession session = newSession();
        List<ConnectorTableLayoutResult> tableLayoutResults = newMetadata().getTableLayouts(session, tableHandle, new Constraint<>(tupleDomain, bindings -> true), Optional.empty());
        ConnectorTableLayoutHandle layoutHandle = getOnlyElement(tableLayoutResults).getTableLayout().getHandle();
        return getAllSplits(splitManager.getSplits(newTransaction(), session, layoutHandle));
    }

    protected static List<ConnectorSplit> getAllSplits(ConnectorSplitSource splitSource)
            throws InterruptedException
    {
        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
        while (!splitSource.isFinished()) {
            List<ConnectorSplit> batch = getFutureValue(splitSource.getNextBatch(1000));
            splits.addAll(batch);
        }
        return splits.build();
    }

    protected List<?> getAllPartitions(ConnectorTableLayoutHandle layoutHandle)
    {
        return ((HiveTableLayoutHandle) layoutHandle).getPartitions().get();
    }

    protected String getPartitionId(Object partition)
    {
        return ((HivePartition) partition).getPartitionId();
    }

    protected static void assertPageSourceType(ConnectorPageSource pageSource, HiveStorageFormat hiveStorageFormat)
    {
        if (pageSource instanceof RecordPageSource) {
            assertInstanceOf(((RecordPageSource) pageSource).getCursor(), recordCursorType(hiveStorageFormat), hiveStorageFormat.name());
        }
        else {
            assertInstanceOf(pageSource, pageSourceType(hiveStorageFormat), hiveStorageFormat.name());
        }
    }

    private static Class<? extends HiveRecordCursor> recordCursorType(HiveStorageFormat hiveStorageFormat)
    {
        switch (hiveStorageFormat) {
            case RCTEXT:
                return ColumnarTextHiveRecordCursor.class;
            case RCBINARY:
                return ColumnarBinaryHiveRecordCursor.class;
            case PARQUET:
                return ParquetHiveRecordCursor.class;
        }
        return GenericHiveRecordCursor.class;
    }

    private static Class<? extends ConnectorPageSource> pageSourceType(HiveStorageFormat hiveStorageFormat)
    {
        switch (hiveStorageFormat) {
            case RCTEXT:
            case RCBINARY:
                return RcFilePageSource.class;
            case ORC:
            case DWRF:
                return OrcPageSource.class;
            default:
                throw new AssertionError("Filed type " + hiveStorageFormat + " does not use a page source");
        }
    }

    private static void assertValueTypes(MaterializedRow row, List<ColumnMetadata> schema)
    {
        for (int columnIndex = 0; columnIndex < schema.size(); columnIndex++) {
            ColumnMetadata column = schema.get(columnIndex);
            Object value = row.getField(columnIndex);
            if (value != null) {
                if (BOOLEAN.equals(column.getType())) {
                    assertInstanceOf(value, Boolean.class);
                }
                else if (BIGINT.equals(column.getType())) {
                    assertInstanceOf(value, Long.class);
                }
                else if (DOUBLE.equals(column.getType())) {
                    assertInstanceOf(value, Double.class);
                }
                else if (VARCHAR.equals(column.getType())) {
                    assertInstanceOf(value, String.class);
                }
                else if (VARBINARY.equals(column.getType())) {
                    assertInstanceOf(value, SqlVarbinary.class);
                }
                else if (TIMESTAMP.equals(column.getType())) {
                    assertInstanceOf(value, SqlTimestamp.class);
                }
                else if (DATE.equals(column.getType())) {
                    assertInstanceOf(value, SqlDate.class);
                }
                else if (column.getType() instanceof ArrayType) {
                    assertInstanceOf(value, List.class);
                }
                else if (column.getType() instanceof MapType) {
                    assertInstanceOf(value, Map.class);
                }
                else {
                    fail("Unknown primitive type " + columnIndex);
                }
            }
        }
    }

    private static void assertPrimitiveField(Map<String, ColumnMetadata> map, String name, Type type, boolean partitionKey)
    {
        assertTrue(map.containsKey(name));
        ColumnMetadata column = map.get(name);
        assertEquals(column.getType(), type, name);
        assertEquals(column.getComment(), annotateColumnComment(null, partitionKey));
    }

    protected static ImmutableMap<String, Integer> indexColumns(List<ColumnHandle> columnHandles)
    {
        ImmutableMap.Builder<String, Integer> index = ImmutableMap.builder();
        int i = 0;
        for (ColumnHandle columnHandle : columnHandles) {
            HiveColumnHandle hiveColumnHandle = checkType(columnHandle, HiveColumnHandle.class, "columnHandle");
            index.put(hiveColumnHandle.getName(), i);
            i++;
        }
        return index.build();
    }

    protected static ImmutableMap<String, Integer> indexColumns(ConnectorTableMetadata tableMetadata)
    {
        ImmutableMap.Builder<String, Integer> index = ImmutableMap.builder();
        int i = 0;
        for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
            index.put(columnMetadata.getName(), i);
            i++;
        }
        return index.build();
    }

    protected static String randomName()
    {
        return UUID.randomUUID().toString().toLowerCase(ENGLISH).replace("-", "");
    }

    private static Map<String, Object> createTableProperties(HiveStorageFormat storageFormat)
    {
        return createTableProperties(storageFormat, ImmutableList.of());
    }

    private static Map<String, Object> createTableProperties(HiveStorageFormat storageFormat, Iterable<String> parititonedBy)
    {
        return ImmutableMap.<String, Object>builder()
                .put(STORAGE_FORMAT_PROPERTY, storageFormat)
                .put(PARTITIONED_BY_PROPERTY, ImmutableList.copyOf(parititonedBy))
                .build();
    }

    protected void createEmptyTable(SchemaTableName schemaTableName, HiveStorageFormat hiveStorageFormat, List<FieldSchema> columns, List<FieldSchema> partitionColumns)
            throws Exception
    {
        ConnectorSession session = newSession();

        String tableOwner = session.getUser();
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();

        LocationService locationService = getLocationService(schemaName);
        LocationHandle locationHandle = locationService.forNewTable(session.getQueryId(), schemaName, tableName);
        Path targetPath = locationService.targetPathRoot(locationHandle);
        HiveWriteUtils.createDirectory(hdfsEnvironment, targetPath);

        SerDeInfo serdeInfo = new SerDeInfo();
        serdeInfo.setName(tableName);
        serdeInfo.setSerializationLib(hiveStorageFormat.getSerDe());
        serdeInfo.setParameters(ImmutableMap.of());

        StorageDescriptor sd = new StorageDescriptor();
        sd.setLocation(targetPath.toString());
        sd.setCols(columns);
        sd.setSerdeInfo(serdeInfo);
        sd.setInputFormat(hiveStorageFormat.getInputFormat());
        sd.setOutputFormat(hiveStorageFormat.getOutputFormat());
        sd.setParameters(ImmutableMap.of());

        Table table = new Table();
        table.setDbName(schemaName);
        table.setTableName(tableName);
        table.setOwner(tableOwner);
        table.setTableType(TableType.MANAGED_TABLE.toString());
        table.setParameters(ImmutableMap.of());
        table.setPartitionKeys(partitionColumns);
        table.setSd(sd);

        getMetastoreClient(schemaName).createTable(table);
    }
}

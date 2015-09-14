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
import com.facebook.presto.hive.orc.DwrfHiveRecordCursor;
import com.facebook.presto.hive.orc.DwrfRecordCursorProvider;
import com.facebook.presto.hive.orc.OrcHiveRecordCursor;
import com.facebook.presto.hive.orc.OrcPageSource;
import com.facebook.presto.hive.orc.OrcRecordCursorProvider;
import com.facebook.presto.hive.parquet.ParquetHiveRecordCursor;
import com.facebook.presto.hive.rcfile.RcFilePageSource;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorPageSinkProvider;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorPageSourceProvider;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.ConnectorViewDefinition;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.Domain;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.Range;
import com.facebook.presto.spi.RecordPageSource;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.SerializableNativeValue;
import com.facebook.presto.spi.SortedRangeSet;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.ViewNotFoundException;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.SqlDate;
import com.facebook.presto.spi.type.SqlTimestamp;
import com.facebook.presto.spi.type.SqlVarbinary;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde2.ReaderWriterProfiler;
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
import static com.facebook.presto.hive.HiveTestUtils.DEFAULT_HIVE_DATA_STREAM_FACTORIES;
import static com.facebook.presto.hive.HiveTestUtils.DEFAULT_HIVE_RECORD_CURSOR_PROVIDER;
import static com.facebook.presto.hive.HiveTestUtils.SESSION;
import static com.facebook.presto.hive.HiveTestUtils.TYPE_MANAGER;
import static com.facebook.presto.hive.HiveTestUtils.getTypes;
import static com.facebook.presto.hive.HiveType.HIVE_INT;
import static com.facebook.presto.hive.HiveType.HIVE_STRING;
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
            .add(new ColumnMetadata("id", BIGINT, false))
            .add(new ColumnMetadata("t_string", VARCHAR, false))
            .add(new ColumnMetadata("t_bigint", BIGINT, false))
            .add(new ColumnMetadata("t_double", DOUBLE, false))
            .add(new ColumnMetadata("t_boolean", BOOLEAN, false))
            .add(new ColumnMetadata("t_array", ARRAY_TYPE, false))
            .add(new ColumnMetadata("t_map", MAP_TYPE, false))
            .add(new ColumnMetadata("t_row", ROW_TYPE, false))
            .build();

    private static final MaterializedResult CREATE_TABLE_DATA = MaterializedResult.resultBuilder(SESSION, BIGINT, VARCHAR, BIGINT, DOUBLE, BOOLEAN, ARRAY_TYPE, MAP_TYPE, ROW_TYPE)
            .row(1, "hello", 123, 43.5, true, ImmutableList.of("apple", "banana"), ImmutableMap.of("one", 1L, "two", 2L), ImmutableList.of("true", 1, true))
            .row(2, null, null, null, null, null, null, null)
            .row(3, "bye", 456, 98.1, false, ImmutableList.of("ape", "bear"), ImmutableMap.of("three", 3L, "four", 4L), ImmutableList.of("false", 0, false))
            .build();

    private static final List<ColumnMetadata> CREATE_TABLE_COLUMNS_PARTITIONED = ImmutableList.<ColumnMetadata>builder()
            .addAll(CREATE_TABLE_COLUMNS)
            .add(new ColumnMetadata("ds", VARCHAR, true))
            .build();

    private static final MaterializedResult CREATE_TABLE_PARTITIONED_DATA = new MaterializedResult(
            CREATE_TABLE_DATA.getMaterializedRows().stream()
                    .map(row -> new MaterializedRow(row.getPrecision(), newArrayList(concat(row.getFields(), ImmutableList.of("2015-07-0" + row.getField(0))))))
                    .collect(toList()),
            ImmutableList.<Type>builder()
                    .addAll(CREATE_TABLE_DATA.getTypes())
                    .add(VARCHAR)
                    .build());

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
    protected SchemaTableName temporaryCreateSampledTable;
    protected SchemaTableName temporaryCreateEmptyTable;
    protected SchemaTableName temporaryInsertTable;
    protected SchemaTableName temporaryInsertPartitionedTable;
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

    protected ConnectorMetadata metadata;
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
        temporaryCreateSampledTable = new SchemaTableName(database, "tmp_presto_test_create_" + randomName());
        temporaryCreateEmptyTable = new SchemaTableName(database, "tmp_presto_test_create_" + randomName());
        temporaryInsertTable = new SchemaTableName(database, "tmp_presto_test_insert_" + randomName());
        temporaryInsertPartitionedTable = new SchemaTableName(database, "tmp_presto_test_insert_partitioned_" + randomName());
        temporaryMetadataDeleteTable = new SchemaTableName(database, "tmp_presto_test_metadata_delete_" + randomName());
        temporaryRenameTableOld = new SchemaTableName(database, "tmp_presto_test_rename_" + randomName());
        temporaryRenameTableNew = new SchemaTableName(database, "tmp_presto_test_rename_" + randomName());
        temporaryCreateView = new SchemaTableName(database, "tmp_presto_test_create_" + randomName());

        invalidClientId = "hive";
        invalidTableHandle = new HiveTableHandle(invalidClientId, database, INVALID_TABLE);
        invalidTableLayoutHandle = new HiveTableLayoutHandle(invalidClientId,
                ImmutableList.of(new HivePartition(invalidTable, TupleDomain.all(), "unknown", ImmutableMap.of(), Optional.empty())),
                TupleDomain.all());
        emptyTableLayoutHandle = new HiveTableLayoutHandle(invalidClientId, ImmutableList.of(), TupleDomain.none());

        dsColumn = new HiveColumnHandle(connectorId, "ds", HIVE_STRING, parseTypeSignature(StandardTypes.VARCHAR), -1, true);
        fileFormatColumn = new HiveColumnHandle(connectorId, "file_format", HIVE_STRING, parseTypeSignature(StandardTypes.VARCHAR), -1, true);
        dummyColumn = new HiveColumnHandle(connectorId, "dummy", HIVE_INT, parseTypeSignature(StandardTypes.BIGINT), -1, true);
        intColumn = new HiveColumnHandle(connectorId, "t_int", HIVE_INT, parseTypeSignature(StandardTypes.BIGINT), -1, true);
        invalidColumnHandle = new HiveColumnHandle(connectorId, INVALID_COLUMN, HIVE_STRING, parseTypeSignature(StandardTypes.VARCHAR), 0, false);

        insertTableDestination = new SchemaTableName(database, "presto_insert_destination");
        insertTablePartitionedDestination = new SchemaTableName(database, "presto_insert_destination_partitioned");

        List<HivePartition> partitions = ImmutableList.<HivePartition>builder()
                .add(new HivePartition(tablePartitionFormat,
                        TupleDomain.<HiveColumnHandle>all(),
                        "ds=2012-12-29/file_format=textfile/dummy=1",
                        ImmutableMap.<ColumnHandle, SerializableNativeValue>builder()
                                .put(dsColumn, new SerializableNativeValue(Slice.class, utf8Slice("2012-12-29")))
                                .put(fileFormatColumn, new SerializableNativeValue(Slice.class, utf8Slice("textfile")))
                                .put(dummyColumn, new SerializableNativeValue(Long.class, 1L))
                                .build(),
                        Optional.empty()))
                .add(new HivePartition(tablePartitionFormat,
                        TupleDomain.<HiveColumnHandle>all(),
                        "ds=2012-12-29/file_format=sequencefile/dummy=2",
                        ImmutableMap.<ColumnHandle, SerializableNativeValue>builder()
                                .put(dsColumn, new SerializableNativeValue(Slice.class, utf8Slice("2012-12-29")))
                                .put(fileFormatColumn, new SerializableNativeValue(Slice.class, utf8Slice("sequencefile")))
                                .put(dummyColumn, new SerializableNativeValue(Long.class, 2L))
                                .build(),
                        Optional.empty()))
                .add(new HivePartition(tablePartitionFormat,
                        TupleDomain.<HiveColumnHandle>all(),
                        "ds=2012-12-29/file_format=rctext/dummy=3",
                        ImmutableMap.<ColumnHandle, SerializableNativeValue>builder()
                                .put(dsColumn, new SerializableNativeValue(Slice.class, utf8Slice("2012-12-29")))
                                .put(fileFormatColumn, new SerializableNativeValue(Slice.class, utf8Slice("rctext")))
                                .put(dummyColumn, new SerializableNativeValue(Long.class, 3L))
                                .build(),
                        Optional.empty()))
                .add(new HivePartition(tablePartitionFormat,
                        TupleDomain.<HiveColumnHandle>all(),
                        "ds=2012-12-29/file_format=rcbinary/dummy=4",
                        ImmutableMap.<ColumnHandle, SerializableNativeValue>builder()
                                .put(dsColumn, new SerializableNativeValue(Slice.class, utf8Slice("2012-12-29")))
                                .put(fileFormatColumn, new SerializableNativeValue(Slice.class, utf8Slice("rcbinary")))
                                .put(dummyColumn, new SerializableNativeValue(Long.class, 4L))
                                .build(),
                        Optional.empty()))
                .build();
        partitionCount = partitions.size();
        tupleDomain = TupleDomain.withFixedValues(ImmutableMap.of(dsColumn, "2012-12-29"));
        tableLayout = new ConnectorTableLayout(
                new HiveTableLayoutHandle(clientId, partitions, tupleDomain),
                Optional.empty(),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        dsColumn, Domain.create(SortedRangeSet.of(Range.equal(utf8Slice("2012-12-29"))), false),
                        fileFormatColumn, Domain.create(SortedRangeSet.of(Range.equal(utf8Slice("textfile")), Range.equal(utf8Slice("sequencefile")), Range.equal(utf8Slice("rctext")), Range.equal(utf8Slice("rcbinary"))), false),
                        dummyColumn, Domain.create(SortedRangeSet.of(Range.equal(1L), Range.equal(2L), Range.equal(3L), Range.equal(4L)), false))),
                Optional.empty(),
                Optional.of(ImmutableList.of(
                        TupleDomain.withColumnDomains(ImmutableMap.of(
                                dsColumn, Domain.create(SortedRangeSet.of(Range.equal(utf8Slice("2012-12-29"))), false),
                                fileFormatColumn, Domain.create(SortedRangeSet.of(Range.equal(utf8Slice("textfile"))), false),
                                dummyColumn, Domain.create(SortedRangeSet.of(Range.equal(1L)), false))),
                        TupleDomain.withColumnDomains(ImmutableMap.of(
                                dsColumn, Domain.create(SortedRangeSet.of(Range.equal(utf8Slice("2012-12-29"))), false),
                                fileFormatColumn, Domain.create(SortedRangeSet.of(Range.equal(utf8Slice("sequencefile"))), false),
                                dummyColumn, Domain.create(SortedRangeSet.of(Range.equal(2L)), false))),
                        TupleDomain.withColumnDomains(ImmutableMap.of(
                                dsColumn, Domain.create(SortedRangeSet.of(Range.equal(utf8Slice("2012-12-29"))), false),
                                fileFormatColumn, Domain.create(SortedRangeSet.of(Range.equal(utf8Slice("rctext"))), false),
                                dummyColumn, Domain.create(SortedRangeSet.of(Range.equal(3L)), false))),
                        TupleDomain.withColumnDomains(ImmutableMap.of(
                                dsColumn, Domain.create(SortedRangeSet.of(Range.equal(utf8Slice("2012-12-29"))), false),
                                fileFormatColumn, Domain.create(SortedRangeSet.of(Range.equal(utf8Slice("rcbinary"))), false),
                                dummyColumn, Domain.create(SortedRangeSet.of(Range.equal(4L)), false)))
                )),
                ImmutableList.of());
        List<HivePartition> unpartitionedPartitions = ImmutableList.of(new HivePartition(tableUnpartitioned, TupleDomain.<HiveColumnHandle>all()));
        unpartitionedTableLayout = new ConnectorTableLayout(
                new HiveTableLayoutHandle(clientId, unpartitionedPartitions, TupleDomain.all()),
                Optional.empty(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.of(ImmutableList.of(TupleDomain.all())),
                ImmutableList.of());
        timeZone = DateTimeZone.forTimeZone(TimeZone.getTimeZone(timeZoneId));
    }

    protected void setup(String host, int port, String databaseName, String timeZone)
    {
        setup(host, port, databaseName, timeZone, "hive-test", 100, 50);
    }

    protected void setup(String host, int port, String databaseName, String timeZoneId, String connectorName, int maxOutstandingSplits, int maxThreads)
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
        TypeRegistry typeManager = new TypeRegistry();
        JsonCodec<PartitionUpdate> partitionUpdateCodec = JsonCodec.jsonCodec(PartitionUpdate.class);
        metadata = new HiveMetadata(
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
                typeManager,
                partitionUpdateCodec);
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
        pageSinkProvider = new HivePageSinkProvider(hdfsEnvironment, metastoreClient, new GroupByHashPageIndexerFactory(), typeManager, new HiveClientConfig(), partitionUpdateCodec);
        pageSourceProvider = new HivePageSourceProvider(hiveClientConfig, hdfsEnvironment, DEFAULT_HIVE_RECORD_CURSOR_PROVIDER, DEFAULT_HIVE_DATA_STREAM_FACTORIES, TYPE_MANAGER);
    }

    @Test
    public void testGetDatabaseNames()
            throws Exception
    {
        List<String> databases = metadata.listSchemaNames(SESSION);
        assertTrue(databases.contains(database));
    }

    @Test
    public void testGetTableNames()
            throws Exception
    {
        List<SchemaTableName> tables = metadata.listTables(SESSION, database);
        assertTrue(tables.contains(tablePartitionFormat));
        assertTrue(tables.contains(tableUnpartitioned));
    }

    @Test
    public void testGetAllTableNames()
            throws Exception
    {
        List<SchemaTableName> tables = metadata.listTables(SESSION, null);
        assertTrue(tables.contains(tablePartitionFormat));
        assertTrue(tables.contains(tableUnpartitioned));
    }

    @Test
    public void testGetAllTableColumns()
    {
        Map<SchemaTableName, List<ColumnMetadata>> allColumns = metadata.listTableColumns(SESSION, new SchemaTablePrefix());
        assertTrue(allColumns.containsKey(tablePartitionFormat));
        assertTrue(allColumns.containsKey(tableUnpartitioned));
    }

    @Test
    public void testGetAllTableColumnsInSchema()
    {
        Map<SchemaTableName, List<ColumnMetadata>> allColumns = metadata.listTableColumns(SESSION, new SchemaTablePrefix(database));
        assertTrue(allColumns.containsKey(tablePartitionFormat));
        assertTrue(allColumns.containsKey(tableUnpartitioned));
    }

    @Test
    public void testListUnknownSchema()
    {
        assertNull(metadata.getTableHandle(SESSION, new SchemaTableName(INVALID_DATABASE, INVALID_TABLE)));
        assertEquals(metadata.listTables(SESSION, INVALID_DATABASE), ImmutableList.of());
        assertEquals(metadata.listTableColumns(SESSION, new SchemaTablePrefix(INVALID_DATABASE, INVALID_TABLE)), ImmutableMap.of());
        assertEquals(metadata.listViews(SESSION, INVALID_DATABASE), ImmutableList.of());
        assertEquals(metadata.getViews(SESSION, new SchemaTablePrefix(INVALID_DATABASE, INVALID_TABLE)), ImmutableMap.of());
    }

    @Test
    public void testGetPartitions()
            throws Exception
    {
        ConnectorTableHandle tableHandle = getTableHandle(tablePartitionFormat);
        List<ConnectorTableLayoutResult> tableLayoutResults = metadata.getTableLayouts(SESSION, tableHandle, new Constraint<>(TupleDomain.all(), bindings -> true), Optional.empty());
        assertExpectedTableLayout(getOnlyElement(tableLayoutResults).getTableLayout(), tableLayout);
    }

    @Test
    public void testGetPartitionsWithBindings()
            throws Exception
    {
        ConnectorTableHandle tableHandle = getTableHandle(tablePartitionFormat);
        List<ConnectorTableLayoutResult> tableLayoutResults = metadata.getTableLayouts(SESSION, tableHandle, new Constraint<>(TupleDomain.withColumnDomains(ImmutableMap.of(intColumn, Domain.singleValue(5L))), bindings -> true), Optional.empty());
        assertExpectedTableLayout(getOnlyElement(tableLayoutResults).getTableLayout(), tableLayout);
    }

    @Test(expectedExceptions = TableNotFoundException.class)
    public void testGetPartitionsException()
            throws Exception
    {
        metadata.getTableLayouts(SESSION, invalidTableHandle, new Constraint<>(TupleDomain.all(), bindings -> true), Optional.empty());
    }

    @Test
    public void testGetPartitionNames()
            throws Exception
    {
        ConnectorTableHandle tableHandle = getTableHandle(tablePartitionFormat);
        List<ConnectorTableLayoutResult> tableLayoutResults = metadata.getTableLayouts(SESSION, tableHandle, new Constraint<>(TupleDomain.all(), bindings -> true), Optional.empty());
        assertExpectedTableLayout(getOnlyElement(tableLayoutResults).getTableLayout(), tableLayout);
    }

    protected void assertExpectedTableLayout(ConnectorTableLayout actualTableLayout, ConnectorTableLayout expectedTableLayout)
    {
        assertExpectedTableLayoutHandle(actualTableLayout.getHandle(), expectedTableLayout.getHandle());
        assertEquals(actualTableLayout.getPredicate(), expectedTableLayout.getPredicate());
        assertEquals(actualTableLayout.getDiscretePredicates().isPresent(), expectedTableLayout.getDiscretePredicates().isPresent());
        actualTableLayout.getDiscretePredicates().ifPresent(actual -> assertEqualsIgnoreOrder(actual, expectedTableLayout.getDiscretePredicates().get()));
        assertEquals(actualTableLayout.getPartitioningColumns(), expectedTableLayout.getPartitioningColumns());
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
        ConnectorTableHandle tableHandle = getTableHandle(tableUnpartitioned);
        List<ConnectorTableLayoutResult> tableLayoutResults = metadata.getTableLayouts(SESSION, tableHandle, new Constraint<>(TupleDomain.all(), bindings -> true), Optional.empty());
        assertEquals(getAllPartitions(getOnlyElement(tableLayoutResults).getTableLayout().getHandle()).size(), 1);
        assertExpectedTableLayout(getOnlyElement(tableLayoutResults).getTableLayout(), unpartitionedTableLayout);
    }

    @Test(expectedExceptions = TableNotFoundException.class)
    public void testGetPartitionNamesException()
            throws Exception
    {
        metadata.getTableLayouts(SESSION, invalidTableHandle, new Constraint<>(TupleDomain.all(), bindings -> true), Optional.empty());
    }

    @SuppressWarnings({"ValueOfIncrementOrDecrementUsed", "UnusedAssignment"})
    @Test
    public void testGetTableSchemaPartitionFormat()
            throws Exception
    {
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(SESSION, getTableHandle(tablePartitionFormat));
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
        ConnectorTableHandle tableHandle = getTableHandle(tableUnpartitioned);
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(SESSION, tableHandle);
        Map<String, ColumnMetadata> map = uniqueIndex(tableMetadata.getColumns(), ColumnMetadata::getName);

        assertPrimitiveField(map, "t_string", VARCHAR, false);
        assertPrimitiveField(map, "t_tinyint", BIGINT, false);
    }

    @Test
    public void testGetTableSchemaOffline()
            throws Exception
    {
        ConnectorTableHandle tableHandle = getTableHandle(tableOffline);
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(SESSION, tableHandle);
        Map<String, ColumnMetadata> map = uniqueIndex(tableMetadata.getColumns(), ColumnMetadata::getName);

        assertPrimitiveField(map, "t_string", VARCHAR, false);
    }

    @Test
    public void testGetTableSchemaOfflinePartition()
            throws Exception
    {
        ConnectorTableHandle tableHandle = getTableHandle(tableOfflinePartition);
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(SESSION, tableHandle);
        Map<String, ColumnMetadata> map = uniqueIndex(tableMetadata.getColumns(), ColumnMetadata::getName);

        assertPrimitiveField(map, "t_string", VARCHAR, false);
    }

    @Test
    public void testGetTableSchemaException()
            throws Exception
    {
        assertNull(metadata.getTableHandle(SESSION, invalidTable));
    }

    @Test
    public void testGetPartitionSplitsBatch()
            throws Exception
    {
        ConnectorTableHandle tableHandle = getTableHandle(tablePartitionFormat);
        List<ConnectorTableLayoutResult> tableLayoutResults = metadata.getTableLayouts(SESSION, tableHandle, new Constraint<>(TupleDomain.all(), bindings -> true), Optional.empty());
        ConnectorSplitSource splitSource = splitManager.getSplits(SESSION, getOnlyElement(tableLayoutResults).getTableLayout().getHandle());

        assertEquals(getSplitCount(splitSource), partitionCount);
    }

    @Test
    public void testGetPartitionSplitsBatchUnpartitioned()
            throws Exception
    {
        ConnectorTableHandle tableHandle = getTableHandle(tableUnpartitioned);
        List<ConnectorTableLayoutResult> tableLayoutResults = metadata.getTableLayouts(SESSION, tableHandle, new Constraint<>(TupleDomain.all(), bindings -> true), Optional.empty());
        ConnectorSplitSource splitSource = splitManager.getSplits(SESSION, getOnlyElement(tableLayoutResults).getTableLayout().getHandle());

        assertEquals(getSplitCount(splitSource), 1);
    }

    @Test(expectedExceptions = TableNotFoundException.class)
    public void testGetPartitionSplitsBatchInvalidTable()
            throws Exception
    {
        splitManager.getSplits(SESSION, invalidTableLayoutHandle);
    }

    @Test
    public void testGetPartitionSplitsEmpty()
            throws Exception
    {
        ConnectorSplitSource splitSource = splitManager.getSplits(SESSION, emptyTableLayoutHandle);
        // fetch full list
        getSplitCount(splitSource);
    }

    @Test
    public void testGetPartitionTableOffline()
            throws Exception
    {
        ConnectorTableHandle tableHandle = getTableHandle(tableOffline);
        try {
            metadata.getTableLayouts(SESSION, tableHandle, new Constraint<>(TupleDomain.all(), bindings -> true), Optional.empty());
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
        ConnectorTableHandle tableHandle = getTableHandle(tableOfflinePartition);
        assertNotNull(tableHandle);

        ColumnHandle dsColumn = metadata.getColumnHandles(SESSION, tableHandle).get("ds");
        assertNotNull(dsColumn);

        Domain domain = Domain.singleValue(utf8Slice("2012-12-30"));
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(dsColumn, domain));
        List<ConnectorTableLayoutResult> tableLayoutResults = metadata.getTableLayouts(SESSION, tableHandle, new Constraint<>(tupleDomain, bindings -> true), Optional.empty());
        try {
            getSplitCount(splitManager.getSplits(SESSION, getOnlyElement(tableLayoutResults).getTableLayout().getHandle()));
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
        ConnectorTableHandle tableHandle = getTableHandle(tableBucketedStringInt);
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(SESSION, tableHandle).values());
        Map<String, Integer> columnIndex = indexColumns(columnHandles);

        assertTableIsBucketed(tableHandle);

        String testString = "test";
        Long testInt = 13L;
        Long testSmallint = 12L;

        // Reverse the order of bindings as compared to bucketing order
        ImmutableMap<ColumnHandle, SerializableNativeValue> bindings = ImmutableMap.<ColumnHandle, SerializableNativeValue>builder()
                .put(columnHandles.get(columnIndex.get("t_int")), new SerializableNativeValue(Long.class, testInt))
                .put(columnHandles.get(columnIndex.get("t_string")), new SerializableNativeValue(Slice.class, utf8Slice(testString)))
                .put(columnHandles.get(columnIndex.get("t_smallint")), new SerializableNativeValue(Long.class, testSmallint))
                .build();

        MaterializedResult result = readTable(tableHandle, columnHandles, SESSION, TupleDomain.withNullableFixedValues(bindings), OptionalInt.of(1), Optional.empty());

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
        ConnectorTableHandle tableHandle = getTableHandle(tableBucketedBigintBoolean);
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(SESSION, tableHandle).values());
        Map<String, Integer> columnIndex = indexColumns(columnHandles);

        assertTableIsBucketed(tableHandle);

        String testString = "test";
        Long testBigint = 89L;
        Boolean testBoolean = true;

        ImmutableMap<ColumnHandle, SerializableNativeValue> bindings = ImmutableMap.<ColumnHandle, SerializableNativeValue>builder()
                .put(columnHandles.get(columnIndex.get("t_string")), new SerializableNativeValue(Slice.class, utf8Slice(testString)))
                .put(columnHandles.get(columnIndex.get("t_bigint")), new SerializableNativeValue(Long.class, testBigint))
                .put(columnHandles.get(columnIndex.get("t_boolean")), new SerializableNativeValue(Boolean.class, testBoolean))
                .build();

        MaterializedResult result = readTable(tableHandle, columnHandles, SESSION, TupleDomain.withNullableFixedValues(bindings), OptionalInt.of(1), Optional.empty());

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
        ConnectorTableHandle tableHandle = getTableHandle(tableBucketedDoubleFloat);
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(SESSION, tableHandle).values());
        Map<String, Integer> columnIndex = indexColumns(columnHandles);

        assertTableIsBucketed(tableHandle);

        ImmutableMap<ColumnHandle, SerializableNativeValue> bindings = ImmutableMap.<ColumnHandle, SerializableNativeValue>builder()
                .put(columnHandles.get(columnIndex.get("t_float")), new SerializableNativeValue(Double.class, 87.1))
                .put(columnHandles.get(columnIndex.get("t_double")), new SerializableNativeValue(Double.class, 88.2))
                .build();

        // floats and doubles are not supported, so we should see all splits
        MaterializedResult result = readTable(tableHandle, columnHandles, SESSION, TupleDomain.withNullableFixedValues(bindings), OptionalInt.of(32), Optional.empty());
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
        ConnectorTableHandle tableHandle = getTableHandle(tablePartitionFormat);
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(SESSION, tableHandle);
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(SESSION, tableHandle).values());
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
            try (ConnectorPageSource pageSource = pageSourceProvider.createPageSource(SESSION, hiveSplit, columnHandles)) {
                MaterializedResult result = materializeSourceDataStream(SESSION, pageSource, getTypes(columnHandles));

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
        ConnectorTableHandle tableHandle = getTableHandle(tablePartitionFormat);
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(SESSION, tableHandle).values());
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
            try (ConnectorPageSource pageSource = pageSourceProvider.createPageSource(SESSION, hiveSplit, columnHandles)) {
                assertPageSourceType(pageSource, fileType);
                MaterializedResult result = materializeSourceDataStream(SESSION, pageSource, getTypes(columnHandles));
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
        ConnectorTableHandle tableHandle = getTableHandle(tableUnpartitioned);
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(SESSION, tableHandle).values());
        Map<String, Integer> columnIndex = indexColumns(columnHandles);

        List<ConnectorSplit> splits = getAllSplits(tableHandle, TupleDomain.all());
        assertEquals(splits.size(), 1);

        for (ConnectorSplit split : splits) {
            HiveSplit hiveSplit = (HiveSplit) split;

            assertEquals(hiveSplit.getPartitionKeys(), ImmutableList.of());

            long rowNumber = 0;
            try (ConnectorPageSource pageSource = pageSourceProvider.createPageSource(SESSION, split, columnHandles)) {
                assertPageSourceType(pageSource, TEXTFILE);
                MaterializedResult result = materializeSourceDataStream(SESSION, pageSource, getTypes(columnHandles));

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
        ConnectorTableHandle table = getTableHandle(tableUnpartitioned);
        readTable(table, ImmutableList.of(invalidColumnHandle), SESSION, TupleDomain.all(), OptionalInt.empty(), Optional.empty());
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = ".*The column 't_data' in table '.*\\.presto_test_partition_schema_change' is declared as type 'bigint', but partition 'ds=2012-12-29' declared column 't_data' as type 'string'.")
    public void testPartitionSchemaMismatch()
            throws Exception
    {
        ConnectorTableHandle table = getTableHandle(tablePartitionSchemaChange);
        readTable(table, ImmutableList.of(dsColumn), SESSION, TupleDomain.all(), OptionalInt.empty(), Optional.empty());
    }

    @Test
    public void testPartitionSchemaNonCanonical()
            throws Exception
    {
        ConnectorTableHandle table = getTableHandle(tablePartitionSchemaChangeNonCanonical);
        ColumnHandle column = metadata.getColumnHandles(SESSION, table).get("t_boolean");
        assertNotNull(column);
        List<ConnectorTableLayoutResult> tableLayoutResults = metadata.getTableLayouts(SESSION, table, new Constraint<>(TupleDomain.withFixedValues(ImmutableMap.of(column, false)), bindings -> true), Optional.empty());
        ConnectorTableLayoutHandle layoutHandle = getOnlyElement(tableLayoutResults).getTableLayout().getHandle();
        assertEquals(getAllPartitions(layoutHandle).size(), 1);
        assertEquals(getPartitionId(getAllPartitions(layoutHandle).get(0)), "t_boolean=0");

        ConnectorSplitSource splitSource = splitManager.getSplits(SESSION, layoutHandle);
        ConnectorSplit split = getOnlyElement(getAllSplits(splitSource));

        ImmutableList<ColumnHandle> columnHandles = ImmutableList.of(column);
        try (ConnectorPageSource ignored = pageSourceProvider.createPageSource(SESSION, split, columnHandles)) {
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
        if (metadata.getTableHandle(SESSION, new SchemaTableName(database, "presto_test_types_rctext")) == null) {
            return;
        }

        ConnectorTableHandle tableHandle = getTableHandle(new SchemaTableName(database, "presto_test_types_rctext"));
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(SESSION, tableHandle);
        HiveSplit hiveSplit = getHiveSplit(tableHandle);
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(SESSION, tableHandle).values());

        ConnectorPageSourceProvider pageSourceProvider = new HivePageSourceProvider(
                new HiveClientConfig().setTimeZone(timeZone.getID()),
                hdfsEnvironment,
                ImmutableSet.<HiveRecordCursorProvider>of(new ColumnarTextHiveRecordCursorProvider()),
                ImmutableSet.<HivePageSourceFactory>of(),
                TYPE_MANAGER);

        ConnectorPageSource pageSource = pageSourceProvider.createPageSource(SESSION, hiveSplit, columnHandles);
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
        if (metadata.getTableHandle(SESSION, new SchemaTableName(database, "presto_test_types_rcbinary")) == null) {
            return;
        }

        ConnectorTableHandle tableHandle = getTableHandle(new SchemaTableName(database, "presto_test_types_rcbinary"));
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(SESSION, tableHandle);
        HiveSplit hiveSplit = getHiveSplit(tableHandle);
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(SESSION, tableHandle).values());

        ConnectorPageSourceProvider pageSourceProvider = new HivePageSourceProvider(
                new HiveClientConfig().setTimeZone(timeZone.getID()),
                hdfsEnvironment,
                ImmutableSet.<HiveRecordCursorProvider>of(new ColumnarBinaryHiveRecordCursorProvider()),
                ImmutableSet.<HivePageSourceFactory>of(),
                TYPE_MANAGER);

        ConnectorPageSource pageSource = pageSourceProvider.createPageSource(SESSION, hiveSplit, columnHandles);
        assertGetRecords(RCBINARY, tableMetadata, hiveSplit, pageSource, columnHandles);
    }

    @Test
    public void testTypesOrc()
            throws Exception
    {
        assertGetRecordsOptional("presto_test_types_orc", ORC);
    }

    @Test
    public void testTypesOrcRecordCursor()
            throws Exception
    {
        if (metadata.getTableHandle(SESSION, new SchemaTableName(database, "presto_test_types_orc")) == null) {
            return;
        }

        ConnectorTableHandle tableHandle = getTableHandle(new SchemaTableName(database, "presto_test_types_orc"));
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(SESSION, tableHandle);
        HiveSplit hiveSplit = getHiveSplit(tableHandle);
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(SESSION, tableHandle).values());

        ConnectorPageSourceProvider pageSourceProvider = new HivePageSourceProvider(
                new HiveClientConfig().setTimeZone(timeZone.getID()),
                hdfsEnvironment,
                ImmutableSet.<HiveRecordCursorProvider>of(new OrcRecordCursorProvider()),
                ImmutableSet.<HivePageSourceFactory>of(),
                TYPE_MANAGER);

        ConnectorPageSource pageSource = pageSourceProvider.createPageSource(SESSION, hiveSplit, columnHandles);
        assertGetRecords(ORC, tableMetadata, hiveSplit, pageSource, columnHandles);
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
    public void testTypesDwrfRecordCursor()
            throws Exception
    {
        if (metadata.getTableHandle(SESSION, new SchemaTableName(database, "presto_test_types_dwrf")) == null) {
            return;
        }

        ConnectorTableHandle tableHandle = getTableHandle(new SchemaTableName(database, "presto_test_types_dwrf"));
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(SESSION, tableHandle);
        HiveSplit hiveSplit = getHiveSplit(tableHandle);
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(SESSION, tableHandle).values());

        ReaderWriterProfiler.setProfilerOptions(new Configuration());

        ConnectorPageSourceProvider pageSourceProvider = new HivePageSourceProvider(
                new HiveClientConfig().setTimeZone(timeZone.getID()),
                hdfsEnvironment,
                ImmutableSet.<HiveRecordCursorProvider>of(new DwrfRecordCursorProvider()),
                ImmutableSet.<HivePageSourceFactory>of(),
                TYPE_MANAGER);

        ConnectorPageSource pageSource = pageSourceProvider.createPageSource(SESSION, hiveSplit, columnHandles);
        assertGetRecords(DWRF, tableMetadata, hiveSplit, pageSource, columnHandles);
    }

    @Test
    public void testHiveViewsAreNotSupported()
            throws Exception
    {
        try {
            getTableHandle(view);
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
        assertEquals(metadata.listTableColumns(SESSION, new SchemaTablePrefix(view.getSchemaName(), view.getTableName())), ImmutableMap.of());
    }

    @Test
    public void testRenameTable()
    {
        try {
            createDummyTable(temporaryRenameTableOld);

            metadata.renameTable(SESSION, getTableHandle(temporaryRenameTableOld), temporaryRenameTableNew);

            assertNull(metadata.getTableHandle(SESSION, temporaryRenameTableOld));
            assertNotNull(metadata.getTableHandle(SESSION, temporaryRenameTableNew));
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
            // begin creating the table
            ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(temporaryCreateTable, CREATE_TABLE_COLUMNS, createTableProperties(RCBINARY), SESSION.getUser());

            ConnectorOutputTableHandle outputHandle = metadata.beginCreateTable(SESSION, tableMetadata);

            // write the data
            ConnectorPageSink sink = pageSinkProvider.createPageSink(SESSION, outputHandle);
            sink.appendPage(CREATE_TABLE_DATA.toPage(), null);
            sink.commit();

            // verify we have data files
            assertFalse(listAllDataFiles(outputHandle).isEmpty());

            // rollback the table
            metadata.rollbackCreateTable(SESSION, outputHandle);

            // verify all files have been deleted
            assertTrue(listAllDataFiles(outputHandle).isEmpty());

            // verify table is not in the metastore
            assertNull(metadata.getTableHandle(SESSION, temporaryCreateTable));
        }
        finally {
            dropTable(temporaryCreateTable);
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
    public void testInsertPartitioned()
            throws Exception
    {
        for (HiveStorageFormat storageFormat : createTableFormats) {
            try {
                doInsertPartitioned(storageFormat, temporaryInsertPartitionedTable);
            }
            finally {
                dropTable(temporaryInsertPartitionedTable);
            }
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
                metadata.dropView(SESSION, temporaryCreateView);
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
                List<ColumnMetadata> columns = ImmutableList.of(new ColumnMetadata("dummy", HYPER_LOG_LOG, false));
                ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(invalidTable, columns, createTableProperties(storageFormat), SESSION.getUser());
                metadata.beginCreateTable(SESSION, tableMetadata);
                fail("create table with unsupported type should fail for storage format " + storageFormat);
            }
            catch (PrestoException e) {
                assertEquals(e.getErrorCode(), NOT_SUPPORTED.toErrorCode());
            }
        }
    }

    private void createDummyTable(SchemaTableName tableName)
    {
        List<ColumnMetadata> columns = ImmutableList.of(new ColumnMetadata("dummy", VARCHAR, false));
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(tableName, columns, createTableProperties(TEXTFILE), SESSION.getUser());
        ConnectorOutputTableHandle handle = metadata.beginCreateTable(SESSION, tableMetadata);
        metadata.commitCreateTable(SESSION, handle, ImmutableList.of());
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

        // drop works when view exists
        metadata.dropView(SESSION, temporaryCreateView);
        assertEquals(metadata.getViews(SESSION, temporaryCreateView.toSchemaTablePrefix()).size(), 0);
        assertFalse(metadata.listViews(SESSION, temporaryCreateView.getSchemaName()).contains(temporaryCreateView));

        // drop fails when view does not exist
        try {
            metadata.dropView(SESSION, temporaryCreateView);
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

        metadata.createView(SESSION, viewName, viewData, replace);

        Map<SchemaTableName, ConnectorViewDefinition> views = metadata.getViews(SESSION, viewName.toSchemaTablePrefix());
        assertEquals(views.size(), 1);
        assertEquals(views.get(viewName).getViewData(), viewData);

        assertTrue(metadata.listViews(SESSION, viewName.getSchemaName()).contains(viewName));
    }

    protected void doCreateSampledTable(SchemaTableName tableName)
            throws Exception
    {
        // begin creating the table
        List<ColumnMetadata> columns = ImmutableList.<ColumnMetadata>builder()
                .add(new ColumnMetadata("sales", BIGINT, false))
                .build();

        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(tableName, columns, createTableProperties(RCBINARY), SESSION.getUser(), true);
        ConnectorOutputTableHandle outputHandle = metadata.beginCreateTable(SESSION, tableMetadata);

        // write the records
        ConnectorPageSink sink = pageSinkProvider.createPageSink(SESSION, outputHandle);

        BlockBuilder sampleBlockBuilder = BIGINT.createBlockBuilder(new BlockBuilderStatus(), 3);
        BlockBuilder dataBlockBuilder = BIGINT.createBlockBuilder(new BlockBuilderStatus(), 3);

        BIGINT.writeLong(sampleBlockBuilder, 8);
        BIGINT.writeLong(dataBlockBuilder, 2);

        BIGINT.writeLong(sampleBlockBuilder, 5);
        BIGINT.writeLong(dataBlockBuilder, 3);

        BIGINT.writeLong(sampleBlockBuilder, 7);
        BIGINT.writeLong(dataBlockBuilder, 4);

        sink.appendPage(new Page(dataBlockBuilder.build()), sampleBlockBuilder.build());

        Collection<Slice> fragments = sink.commit();

        // commit the table
        metadata.commitCreateTable(SESSION, outputHandle, fragments);

        // load the new table
        ConnectorTableHandle tableHandle = getTableHandle(tableName);
        List<ColumnHandle> columnHandles = ImmutableList.<ColumnHandle>builder()
                .addAll(metadata.getColumnHandles(SESSION, tableHandle).values())
                .add(metadata.getSampleWeightColumnHandle(SESSION, tableHandle))
                .build();
        assertEquals(columnHandles.size(), 2);

        // verify the metadata
        tableMetadata = metadata.getTableMetadata(SESSION, getTableHandle(tableName));
        assertEquals(tableMetadata.getOwner(), SESSION.getUser());

        Map<String, ColumnMetadata> columnMap = uniqueIndex(tableMetadata.getColumns(), ColumnMetadata::getName);
        assertEquals(columnMap.size(), 1);

        assertPrimitiveField(columnMap, "sales", BIGINT, false);

        // verify the data
        List<ConnectorTableLayoutResult> tableLayoutResults = metadata.getTableLayouts(SESSION, tableHandle, new Constraint<>(TupleDomain.all(), bindings -> true), Optional.empty());
        ConnectorTableLayoutHandle layoutHandle = getOnlyElement(tableLayoutResults).getTableLayout().getHandle();
        assertEquals(getAllPartitions(layoutHandle).size(), 1);
        ConnectorSplitSource splitSource = splitManager.getSplits(SESSION, layoutHandle);
        ConnectorSplit split = getOnlyElement(getAllSplits(splitSource));

        try (ConnectorPageSource pageSource = pageSourceProvider.createPageSource(SESSION, split, columnHandles)) {
            assertPageSourceType(pageSource, RCBINARY);
            MaterializedResult result = materializeSourceDataStream(SESSION, pageSource, getTypes(columnHandles));
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
        // begin creating the table
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(tableName, CREATE_TABLE_COLUMNS, createTableProperties(storageFormat), SESSION.getUser());

        ConnectorOutputTableHandle outputHandle = metadata.beginCreateTable(SESSION, tableMetadata);

        // write the data
        ConnectorPageSink sink = pageSinkProvider.createPageSink(SESSION, outputHandle);
        sink.appendPage(CREATE_TABLE_DATA.toPage(), null);
        Collection<Slice> fragments = sink.commit();

        // verify all new files start with the unique prefix
        for (String filePath : listAllDataFiles(outputHandle)) {
            assertTrue(new Path(filePath).getName().startsWith(getFilePrefix(outputHandle)));
        }

        // commit the table
        metadata.commitCreateTable(SESSION, outputHandle, fragments);

        // load the new table
        ConnectorTableHandle tableHandle = getTableHandle(tableName);
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(SESSION, tableHandle).values());

        // verify the metadata
        tableMetadata = metadata.getTableMetadata(SESSION, getTableHandle(tableName));
        assertEquals(tableMetadata.getOwner(), SESSION.getUser());
        assertEquals(tableMetadata.getColumns(), CREATE_TABLE_COLUMNS);

        // verify the data
        MaterializedResult result = readTable(tableHandle, columnHandles, SESSION, TupleDomain.all(), OptionalInt.empty(), Optional.of(storageFormat));
        assertEqualsIgnoreOrder(result.getMaterializedRows(), CREATE_TABLE_DATA.getMaterializedRows());
    }

    protected void doCreateEmptyTable(SchemaTableName tableName, HiveStorageFormat storageFormat, List<ColumnMetadata> createTableColumns)
            throws Exception
    {
        List<String> partitionedBy = createTableColumns.stream()
                .filter(ColumnMetadata::isPartitionKey)
                .map(ColumnMetadata::getName)
                .collect(toList());
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(tableName, createTableColumns, createTableProperties(storageFormat, partitionedBy), SESSION.getUser());

        metadata.createTable(SESSION, tableMetadata);

        // load the new table
        ConnectorTableHandle tableHandle = getTableHandle(tableName);

        // verify the metadata
        tableMetadata = metadata.getTableMetadata(SESSION, getTableHandle(tableName));
        assertEquals(tableMetadata.getOwner(), SESSION.getUser());
        assertEquals(tableMetadata.getColumns(), createTableColumns);

        // verify table format
        Table table = getMetastoreClient(tableName.getSchemaName()).getTable(tableName.getSchemaName(), tableName.getTableName()).get();
        if (!table.getSd().getInputFormat().equals(storageFormat.getInputFormat())) {
            assertEquals(table.getSd().getInputFormat(), storageFormat.getInputFormat());
        }

        // verify the table is empty
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(SESSION, tableHandle).values());
        MaterializedResult result = readTable(tableHandle, columnHandles, SESSION, TupleDomain.all(), OptionalInt.empty(), Optional.of(storageFormat));
        assertEquals(result.getRowCount(), 0);
    }

    private void doInsert(HiveStorageFormat storageFormat, SchemaTableName tableName)
            throws Exception
    {
        // creating the table
        doCreateEmptyTable(tableName, storageFormat, CREATE_TABLE_COLUMNS);

        MaterializedResult.Builder resultBuilder = MaterializedResult.resultBuilder(SESSION, CREATE_TABLE_DATA.getTypes());
        for (int i = 0; i < 3; i++) {
            // begin the insert
            ConnectorTableHandle tableHandle = getTableHandle(tableName);
            ConnectorInsertTableHandle insertTableHandle = metadata.beginInsert(SESSION, tableHandle);

            ConnectorPageSink sink = pageSinkProvider.createPageSink(SESSION, insertTableHandle);

            // write data
            sink.appendPage(CREATE_TABLE_DATA.toPage(), null);
            Collection<Slice> fragments = sink.commit();

            // commit the insert
            metadata.commitInsert(SESSION, insertTableHandle, fragments);

            // load the new table
            List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(SESSION, tableHandle).values());

            // verify the metadata
            ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(SESSION, getTableHandle(tableName));
            assertEquals(tableMetadata.getOwner(), SESSION.getUser());
            assertEquals(tableMetadata.getColumns(), CREATE_TABLE_COLUMNS);

            // verify the data
            resultBuilder.rows(CREATE_TABLE_DATA.getMaterializedRows());
            MaterializedResult result = readTable(tableHandle, columnHandles, SESSION, TupleDomain.<ColumnHandle>all(), OptionalInt.empty(), Optional.empty());
            assertEqualsIgnoreOrder(result.getMaterializedRows(), resultBuilder.build().getMaterializedRows());
        }

        // test rollback
        Set<String> existingFiles = listAllDataFiles(tableName.getSchemaName(), tableName.getTableName());
        assertFalse(existingFiles.isEmpty());

        ConnectorTableHandle tableHandle = getTableHandle(tableName);
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(SESSION, tableHandle).values());

        // "stage" insert data
        ConnectorInsertTableHandle insertTableHandle = metadata.beginInsert(SESSION, tableHandle);
        ConnectorPageSink sink = pageSinkProvider.createPageSink(SESSION, insertTableHandle);
        sink.appendPage(CREATE_TABLE_DATA.toPage(), null);
        sink.appendPage(CREATE_TABLE_DATA.toPage(), null);
        sink.commit();

        // verify we did not modify the table directory
        assertEquals(listAllDataFiles(tableName.getSchemaName(), tableName.getTableName()), existingFiles);

        // verify all temp files start with the unique prefix
        Set<String> tempFiles = listAllDataFiles(insertTableHandle);
        assertTrue(!tempFiles.isEmpty());
        for (String filePath : tempFiles) {
            assertTrue(new Path(filePath).getName().startsWith(getFilePrefix(insertTableHandle)));
        }

        // rollback insert
        metadata.rollbackInsert(SESSION, insertTableHandle);

        // verify the data is unchanged
        MaterializedResult result = readTable(tableHandle, columnHandles, SESSION, TupleDomain.<ColumnHandle>all(), OptionalInt.empty(), Optional.empty());
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
        Path writePath = new Path(hiveOutputTableHandle.getWritePath().get());
        return listAllDataFiles(writePath, new HashSet<>());
    }

    protected Set<String> listAllDataFiles(ConnectorInsertTableHandle tableHandle)
            throws IOException
    {
        HiveInsertTableHandle hiveInsertTableHandle = (HiveInsertTableHandle) tableHandle;
        Path writePath = new Path(hiveInsertTableHandle.getWritePath().get());
        return listAllDataFiles(writePath, new HashSet<>());
    }

    protected Set<String> listAllDataFiles(String schemaName, String tableName)
            throws IOException
    {
        Table table = metastoreClient.getTable(schemaName, tableName).get();
        Path path = new Path(table.getSd().getLocation());
        Set<String> existingFiles = new HashSet<>();
        return listAllDataFiles(path, existingFiles);
    }

    protected Set<String> listAllDataFiles(Path path, Set<String> existingFiles)
            throws IOException
    {
        FileSystem fileSystem = hdfsEnvironment.getFileSystem(path);
        if (fileSystem.exists(path)) {
            for (FileStatus fileStatus : fileSystem.listStatus(path)) {
                if (HadoopFileStatus.isFile(fileStatus)) {
                    existingFiles.add(fileStatus.getPath().toString());
                }
                else if (HadoopFileStatus.isDirectory(fileStatus)) {
                    existingFiles.addAll(listAllDataFiles(fileStatus.getPath(), existingFiles));
                }
            }
        }
        return existingFiles;
    }

    private void doInsertPartitioned(HiveStorageFormat storageFormat, SchemaTableName tableName)
            throws Exception
    {
        // creating the table
        doCreateEmptyTable(tableName, storageFormat, CREATE_TABLE_COLUMNS_PARTITIONED);

        MaterializedResult.Builder resultBuilder = MaterializedResult.resultBuilder(SESSION, CREATE_TABLE_PARTITIONED_DATA.getTypes());
        for (int i = 0; i < 3; i++) {
            ConnectorTableHandle tableHandle = getTableHandle(tableName);

            // insert the data
            insertData(tableHandle, CREATE_TABLE_PARTITIONED_DATA, SESSION);

            // verify partitions were created
            List<String> partitionNames = getMetastoreClient(tableName.getSchemaName()).getPartitionNames(tableName.getSchemaName(), tableName.getTableName())
                    .orElseThrow(() -> new PrestoException(HIVE_METASTORE_ERROR, "Partition metadata not available"));
            assertEqualsIgnoreOrder(partitionNames, CREATE_TABLE_PARTITIONED_DATA.getMaterializedRows().stream()
                    .map(row -> "ds=" + row.getField(CREATE_TABLE_PARTITIONED_DATA.getTypes().size() - 1))
                    .collect(toList()));

            // load the new table
            List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(SESSION, tableHandle).values());

            // verify the data
            resultBuilder.rows(CREATE_TABLE_PARTITIONED_DATA.getMaterializedRows());
            MaterializedResult result = readTable(tableHandle, columnHandles, SESSION, TupleDomain.all(), OptionalInt.empty(), Optional.of(storageFormat));
            assertEqualsIgnoreOrder(result.getMaterializedRows(), resultBuilder.build().getMaterializedRows());
        }

        // test rollback
        Set<String> existingFiles = listAllDataFiles(tableName.getSchemaName(), tableName.getTableName());
        assertFalse(existingFiles.isEmpty());

        ConnectorTableHandle tableHandle = getTableHandle(tableName);
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(SESSION, tableHandle).values());

        // "stage" insert data
        ConnectorInsertTableHandle insertTableHandle = metadata.beginInsert(SESSION, tableHandle);
        ConnectorPageSink sink = pageSinkProvider.createPageSink(SESSION, insertTableHandle);
        sink.appendPage(CREATE_TABLE_PARTITIONED_DATA.toPage(), null);
        sink.appendPage(CREATE_TABLE_PARTITIONED_DATA.toPage(), null);
        sink.commit();

        // verify we did not modify the table directory
        assertEquals(listAllDataFiles(tableName.getSchemaName(), tableName.getTableName()), existingFiles);

        // verify all temp files start with the unique prefix
        Set<String> tempFiles = listAllDataFiles(insertTableHandle);
        assertTrue(!tempFiles.isEmpty());
        for (String filePath : tempFiles) {
            assertTrue(new Path(filePath).getName().startsWith(getFilePrefix(insertTableHandle)));
        }

        // rollback insert
        metadata.rollbackInsert(SESSION, insertTableHandle);

        // verify the data is unchanged
        MaterializedResult result = readTable(tableHandle, columnHandles, SESSION, TupleDomain.<ColumnHandle>all(), OptionalInt.empty(), Optional.empty());
        assertEqualsIgnoreOrder(result.getMaterializedRows(), resultBuilder.build().getMaterializedRows());

        // verify we did not modify the table directory
        assertEquals(listAllDataFiles(tableName.getSchemaName(), tableName.getTableName()), existingFiles);

        // verify temp directory is empty
        assertTrue(listAllDataFiles(insertTableHandle).isEmpty());
    }

    private void insertData(ConnectorTableHandle tableHandle, MaterializedResult data, ConnectorSession session)
    {
        ConnectorInsertTableHandle insertTableHandle = metadata.beginInsert(session, tableHandle);

        ConnectorPageSink sink = pageSinkProvider.createPageSink(session, insertTableHandle);

        // write data
        sink.appendPage(data.toPage(), null);
        Collection<Slice> fragments = sink.commit();

        // commit the insert
        metadata.commitInsert(session, insertTableHandle, fragments);
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
        ConnectorTableHandle tableHandle = getTableHandle(tableName);
        for (int i = 0; i < 3; i++) {
            insertData(tableHandle, CREATE_TABLE_PARTITIONED_DATA, SESSION);
            expectedResultBuilder.rows(CREATE_TABLE_PARTITIONED_DATA.getMaterializedRows());
        }

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
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(SESSION, tableHandle).values());
        MaterializedResult result = readTable(tableHandle, columnHandles, SESSION, TupleDomain.all(), OptionalInt.empty(), Optional.of(storageFormat));
        assertEqualsIgnoreOrder(result.getMaterializedRows(), expectedResultBuilder.build().getMaterializedRows());

        // get ds column handle
        Map<String, HiveColumnHandle> columnHandleMap = columnHandles.stream()
                .map(columnHandle -> (HiveColumnHandle) columnHandle)
                .collect(Collectors.toMap(HiveColumnHandle::getName, Function.identity()));
        HiveColumnHandle dsColumnHandle = columnHandleMap.get("ds");
        int dsColumnOrdinalPosition = columnHandles.indexOf(dsColumnHandle);

        // delete ds=2015-07-03
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withFixedValues(ImmutableMap.of(dsColumnHandle, utf8Slice("2015-07-03")));
        Constraint<ColumnHandle> constraint = new Constraint<>(tupleDomain, convertToPredicate(tupleDomain));
        List<ConnectorTableLayoutResult> tableLayoutResults = metadata.getTableLayouts(SESSION, tableHandle, constraint, Optional.empty());
        ConnectorTableLayoutHandle tableLayoutHandle = Iterables.getOnlyElement(tableLayoutResults).getTableLayout().getHandle();
        metadata.metadataDelete(SESSION, tableHandle, tableLayoutHandle);
        // verify the data
        ImmutableList<MaterializedRow> expectedRows = expectedResultBuilder.build().getMaterializedRows().stream()
                .filter(row -> !"2015-07-03".equals(row.getField(dsColumnOrdinalPosition)))
                .collect(ImmutableCollectors.toImmutableList());
        MaterializedResult actualAfterDelete = readTable(tableHandle, columnHandles, SESSION, TupleDomain.all(), OptionalInt.empty(), Optional.of(storageFormat));
        assertEqualsIgnoreOrder(actualAfterDelete.getMaterializedRows(), expectedRows);

        // delete ds=2015-07-01 and 2015-07-02
        TupleDomain<ColumnHandle> tupleDomain2 = TupleDomain.withColumnDomains(
                ImmutableMap.of(dsColumnHandle, Domain.create(SortedRangeSet.of(Range.range(utf8Slice("2015-07-01"), true, utf8Slice("2015-07-02"), true)), false)));
        Constraint<ColumnHandle> constraint2 = new Constraint<>(tupleDomain2, convertToPredicate(tupleDomain2));
        List<ConnectorTableLayoutResult> tableLayoutResults2 = metadata.getTableLayouts(SESSION, tableHandle, constraint2, Optional.empty());
        ConnectorTableLayoutHandle tableLayoutHandle2 = Iterables.getOnlyElement(tableLayoutResults2).getTableLayout().getHandle();
        metadata.metadataDelete(SESSION, tableHandle, tableLayoutHandle2);
        // verify the data
        MaterializedResult actualAfterDelete2 = readTable(tableHandle, columnHandles, SESSION, TupleDomain.all(), OptionalInt.empty(), Optional.of(storageFormat));
        assertEqualsIgnoreOrder(actualAfterDelete2.getMaterializedRows(), ImmutableList.of());

        // verify table directory is empty
        Set<String> filesAfterDelete = listAllDataFiles(tableName.getSchemaName(), tableName.getTableName());
        assertTrue(filesAfterDelete.isEmpty());
    }

    protected void assertGetRecordsOptional(String tableName, HiveStorageFormat hiveStorageFormat)
            throws Exception
    {
        if (metadata.getTableHandle(SESSION, new SchemaTableName(database, tableName)) != null) {
            assertGetRecords(tableName, hiveStorageFormat);
        }
    }

    protected void assertGetRecords(String tableName, HiveStorageFormat hiveStorageFormat)
            throws Exception
    {
        ConnectorTableHandle tableHandle = getTableHandle(new SchemaTableName(database, tableName));
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(SESSION, tableHandle);
        HiveSplit hiveSplit = getHiveSplit(tableHandle);

        List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(SESSION, tableHandle).values());

        ConnectorPageSource pageSource = pageSourceProvider.createPageSource(SESSION, hiveSplit, columnHandles);
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
            MaterializedResult result = materializeSourceDataStream(SESSION, pageSource, getTypes(columnHandles));

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
            ConnectorTableHandle handle = metadata.getTableHandle(SESSION, table);
            if (handle == null) {
                return;
            }

            metadata.dropTable(SESSION, handle);
            try {
                // todo I have no idea why this is needed... maybe there is a propagation delay in the metastore?
                metadata.dropTable(SESSION, handle);
                fail("expected NotFoundException");
            }
            catch (TableNotFoundException expected) {
            }
        }
        catch (Exception e) {
            Logger.get(getClass()).warn(e, "failed to drop table");
        }
    }

    protected ConnectorTableHandle getTableHandle(SchemaTableName tableName)
    {
        ConnectorTableHandle handle = metadata.getTableHandle(SESSION, tableName);
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
        List<ConnectorTableLayoutResult> tableLayoutResults = metadata.getTableLayouts(session, tableHandle, new Constraint<>(tupleDomain, bindings -> true), Optional.empty());
        ConnectorTableLayoutHandle layoutHandle = getOnlyElement(tableLayoutResults).getTableLayout().getHandle();
        List<ConnectorSplit> splits = getAllSplits(splitManager.getSplits(session, layoutHandle));
        if (expectedSplitCount.isPresent()) {
            assertEquals(splits.size(), expectedSplitCount.getAsInt());
        }

        ImmutableList.Builder<MaterializedRow> allRows = ImmutableList.builder();
        for (ConnectorSplit split : splits) {
            try (ConnectorPageSource pageSource = pageSourceProvider.createPageSource(session, split, columnHandles)) {
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
        List<ConnectorTableLayoutResult> tableLayoutResults = metadata.getTableLayouts(SESSION, tableHandle, new Constraint<>(tupleDomain, bindings -> true), Optional.empty());
        ConnectorTableLayoutHandle layoutHandle = getOnlyElement(tableLayoutResults).getTableLayout().getHandle();
        return getAllSplits(splitManager.getSplits(SESSION, layoutHandle));
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
            case ORC:
                return OrcHiveRecordCursor.class;
            case PARQUET:
                return ParquetHiveRecordCursor.class;
            case DWRF:
                return DwrfHiveRecordCursor.class;
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
        assertEquals(column.isPartitionKey(), partitionKey, name);
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
}

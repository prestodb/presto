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
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorPageSourceProvider;
import com.facebook.presto.spi.ConnectorRecordSinkProvider;
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
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.Range;
import com.facebook.presto.spi.RecordPageSource;
import com.facebook.presto.spi.RecordSink;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.SerializableNativeValue;
import com.facebook.presto.spi.SortedRangeSet;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.ViewNotFoundException;
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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import com.google.common.primitives.Ints;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.units.Duration;
import org.apache.hadoop.conf.Configuration;
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
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_PARTITION_VALUE;
import static com.facebook.presto.hive.HiveStorageFormat.DWRF;
import static com.facebook.presto.hive.HiveStorageFormat.ORC;
import static com.facebook.presto.hive.HiveStorageFormat.PARQUET;
import static com.facebook.presto.hive.HiveStorageFormat.RCBINARY;
import static com.facebook.presto.hive.HiveStorageFormat.RCTEXT;
import static com.facebook.presto.hive.HiveStorageFormat.SEQUENCEFILE;
import static com.facebook.presto.hive.HiveStorageFormat.TEXTFILE;
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
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.MaterializedResult.materializeSourceDataStream;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getFirst;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Maps.uniqueIndex;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
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
    protected ConnectorRecordSinkProvider recordSinkProvider;
    protected ExecutorService executor;

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
        temporaryRenameTableOld = new SchemaTableName(database, "tmp_presto_test_rename_" + randomName());
        temporaryRenameTableNew = new SchemaTableName(database, "tmp_presto_test_rename_" + randomName());
        temporaryCreateView = new SchemaTableName(database, "tmp_presto_test_create_" + randomName());

        invalidClientId = "hive";
        invalidTableHandle = new HiveTableHandle(invalidClientId, database, INVALID_TABLE);
        invalidTableLayoutHandle = new HiveTableLayoutHandle(invalidClientId,
                ImmutableList.of(new HivePartition(invalidTable, TupleDomain.<HiveColumnHandle>all(), "unknown", ImmutableMap.of(), Optional.empty())));
        emptyTableLayoutHandle = new HiveTableLayoutHandle(invalidClientId, ImmutableList.of());

        dsColumn = new HiveColumnHandle(connectorId, "ds", 0, HIVE_STRING, parseTypeSignature(StandardTypes.VARCHAR), -1, true);
        fileFormatColumn = new HiveColumnHandle(connectorId, "file_format", 1, HIVE_STRING, parseTypeSignature(StandardTypes.VARCHAR), -1, true);
        dummyColumn = new HiveColumnHandle(connectorId, "dummy", 2, HIVE_INT, parseTypeSignature(StandardTypes.BIGINT), -1, true);
        intColumn = new HiveColumnHandle(connectorId, "t_int", 0, HIVE_INT, parseTypeSignature(StandardTypes.BIGINT), -1, true);
        invalidColumnHandle = new HiveColumnHandle(connectorId, INVALID_COLUMN, 0, HIVE_STRING, parseTypeSignature(StandardTypes.VARCHAR), 0, false);

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
        tableLayout = new ConnectorTableLayout(
                new HiveTableLayoutHandle(clientId, partitions),
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
                new HiveTableLayoutHandle(clientId, unpartitionedPartitions),
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
        metadata = new HiveMetadata(
                connectorId,
                metastoreClient,
                hdfsEnvironment,
                new HivePartitionManager(connectorId, hiveClientConfig),
                timeZone,
                true,
                true,
                true,
                true,
                true,
                new TypeRegistry());
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
        recordSinkProvider = new HiveRecordSinkProvider(hdfsEnvironment);
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
        assertExpectedPartitions(actual.getPartitions(), expected.getPartitions());
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

        List<ConnectorTableLayoutResult> tableLayoutResults = metadata.getTableLayouts(SESSION, tableHandle, new Constraint<>(TupleDomain.withNullableFixedValues(bindings), x -> true), Optional.empty());
        List<ConnectorSplit> splits = getAllSplits(splitManager.getSplits(SESSION, getOnlyElement(tableLayoutResults).getTableLayout().getHandle()));
        assertEquals(splits.size(), 1);

        try (ConnectorPageSource pageSource = pageSourceProvider.createPageSource(SESSION, splits.get(0), columnHandles)) {
            MaterializedResult result = materializeSourceDataStream(SESSION, pageSource, getTypes(columnHandles));

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

        List<ConnectorTableLayoutResult> tableLayoutResults = metadata.getTableLayouts(SESSION, tableHandle, new Constraint<>(TupleDomain.withNullableFixedValues(bindings), x -> true), Optional.empty());
        List<ConnectorSplit> splits = getAllSplits(splitManager.getSplits(SESSION, getOnlyElement(tableLayoutResults).getTableLayout().getHandle()));
        assertEquals(splits.size(), 1);

        try (ConnectorPageSource pageSource = pageSourceProvider.createPageSource(SESSION, splits.get(0), columnHandles)) {
            MaterializedResult result = materializeSourceDataStream(SESSION, pageSource, getTypes(columnHandles));

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
        List<ConnectorTableLayoutResult> tableLayoutResults = metadata.getTableLayouts(SESSION, tableHandle, new Constraint<>(TupleDomain.withNullableFixedValues(bindings), x -> true), Optional.empty());
        List<ConnectorSplit> splits = getAllSplits(splitManager.getSplits(SESSION, getOnlyElement(tableLayoutResults).getTableLayout().getHandle()));
        assertEquals(splits.size(), 32);

        int count = 0;
        for (ConnectorSplit split : splits) {
            try (ConnectorPageSource pageSource = pageSourceProvider.createPageSource(SESSION, split, columnHandles)) {
                MaterializedResult result = materializeSourceDataStream(SESSION, pageSource, getTypes(columnHandles));
                count += result.getRowCount();
            }
        }
        assertEquals(count, 100);
    }

    private void assertTableIsBucketed(ConnectorTableHandle tableHandle)
            throws Exception
    {
        // the bucketed test tables should have exactly 32 splits
        List<ConnectorTableLayoutResult> tableLayoutResults = metadata.getTableLayouts(SESSION, tableHandle, new Constraint<>(TupleDomain.all(), bindings -> true), Optional.empty());
        List<ConnectorSplit> splits = getAllSplits(splitManager.getSplits(SESSION, getOnlyElement(tableLayoutResults).getTableLayout().getHandle()));
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

        List<ConnectorTableLayoutResult> tableLayoutResults = metadata.getTableLayouts(SESSION, tableHandle, new Constraint<>(TupleDomain.all(), bindings -> true), Optional.empty());
        List<ConnectorSplit> splits = getAllSplits(splitManager.getSplits(SESSION, getOnlyElement(tableLayoutResults).getTableLayout().getHandle()));
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

        List<ConnectorTableLayoutResult> tableLayoutResults = metadata.getTableLayouts(SESSION, tableHandle, new Constraint<>(TupleDomain.all(), bindings -> true), Optional.empty());
        List<ConnectorSplit> splits = getAllSplits(splitManager.getSplits(SESSION, getOnlyElement(tableLayoutResults).getTableLayout().getHandle()));
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

        List<ConnectorTableLayoutResult> tableLayoutResults = metadata.getTableLayouts(SESSION, tableHandle, new Constraint<>(TupleDomain.all(), bindings -> true), Optional.empty());
        List<ConnectorSplit> splits = getAllSplits(splitManager.getSplits(SESSION, getOnlyElement(tableLayoutResults).getTableLayout().getHandle()));
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
        List<ConnectorTableLayoutResult> tableLayoutResults = metadata.getTableLayouts(SESSION, table, new Constraint<>(TupleDomain.all(), bindings -> true), Optional.empty());
        ConnectorSplit split = getFirst(getAllSplits(splitManager.getSplits(SESSION, getOnlyElement(tableLayoutResults).getTableLayout().getHandle())), null);
        pageSourceProvider.createPageSource(SESSION, split, ImmutableList.of(invalidColumnHandle));
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = ".*The column 't_data' in table '.*\\.presto_test_partition_schema_change' is declared as type 'bigint', but partition 'ds=2012-12-29' declared column 't_data' as type 'string'.")
    public void testPartitionSchemaMismatch()
            throws Exception
    {
        ConnectorTableHandle table = getTableHandle(tablePartitionSchemaChange);
        List<ConnectorTableLayoutResult> tableLayoutResults = metadata.getTableLayouts(SESSION, table, new Constraint<>(TupleDomain.all(), bindings -> true), Optional.empty());
        getAllSplits(splitManager.getSplits(SESSION, getOnlyElement(tableLayoutResults).getTableLayout().getHandle()));
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
                doCreateEmptyTable(temporaryCreateEmptyTable, storageFormat);
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
                Map<String, Object> properties = ImmutableMap.of(STORAGE_FORMAT_PROPERTY, storageFormat);
                ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(invalidTable, columns, properties, SESSION.getUser());
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
        Map<String, Object> properties = ImmutableMap.of(STORAGE_FORMAT_PROPERTY, TEXTFILE);
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(tableName, columns, properties, SESSION.getUser());
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

        Map<String, Object> properties = ImmutableMap.of(STORAGE_FORMAT_PROPERTY, RCBINARY);
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(tableName, columns, properties, SESSION.getUser(), true);
        ConnectorOutputTableHandle outputHandle = metadata.beginCreateTable(SESSION, tableMetadata);

        // write the records
        RecordSink sink = recordSinkProvider.getRecordSink(SESSION, outputHandle);

        sink.beginRecord(8);
        sink.appendLong(2);
        sink.finishRecord();

        sink.beginRecord(5);
        sink.appendLong(3);
        sink.finishRecord();

        sink.beginRecord(7);
        sink.appendLong(4);
        sink.finishRecord();

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
        List<ColumnMetadata> columns = ImmutableList.<ColumnMetadata>builder()
                .add(new ColumnMetadata("id", BIGINT, false))
                .add(new ColumnMetadata("t_string", VARCHAR, false))
                .add(new ColumnMetadata("t_bigint", BIGINT, false))
                .add(new ColumnMetadata("t_double", DOUBLE, false))
                .add(new ColumnMetadata("t_boolean", BOOLEAN, false))
                .build();

        Map<String, Object> properties = ImmutableMap.of(STORAGE_FORMAT_PROPERTY, storageFormat);
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(tableName, columns, properties, SESSION.getUser());

        ConnectorOutputTableHandle outputHandle = metadata.beginCreateTable(SESSION, tableMetadata);

        // write the records
        RecordSink sink = recordSinkProvider.getRecordSink(SESSION, outputHandle);

        sink.beginRecord(1);
        sink.appendLong(1);
        sink.appendString("hello".getBytes(UTF_8));
        sink.appendLong(123);
        sink.appendDouble(43.5);
        sink.appendBoolean(true);
        sink.finishRecord();

        sink.beginRecord(1);
        sink.appendLong(2);
        sink.appendNull();
        sink.appendNull();
        sink.appendNull();
        sink.appendNull();
        sink.finishRecord();

        sink.beginRecord(1);
        sink.appendLong(3);
        sink.appendString("bye".getBytes(UTF_8));
        sink.appendLong(456);
        sink.appendDouble(98.1);
        sink.appendBoolean(false);
        sink.finishRecord();

        Collection<Slice> fragments = sink.commit();

        // commit the table
        metadata.commitCreateTable(SESSION, outputHandle, fragments);

        // load the new table
        ConnectorTableHandle tableHandle = getTableHandle(tableName);
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(SESSION, tableHandle).values());

        // verify the metadata
        tableMetadata = metadata.getTableMetadata(SESSION, getTableHandle(tableName));
        assertEquals(tableMetadata.getOwner(), SESSION.getUser());

        Map<String, ColumnMetadata> columnMap = uniqueIndex(tableMetadata.getColumns(), ColumnMetadata::getName);

        assertPrimitiveField(columnMap, "id", BIGINT, false);
        assertPrimitiveField(columnMap, "t_string", VARCHAR, false);
        assertPrimitiveField(columnMap, "t_bigint", BIGINT, false);
        assertPrimitiveField(columnMap, "t_double", DOUBLE, false);
        assertPrimitiveField(columnMap, "t_boolean", BOOLEAN, false);

        // verify the data
        List<ConnectorTableLayoutResult> tableLayoutResults = metadata.getTableLayouts(SESSION, tableHandle, new Constraint<>(TupleDomain.all(), bindings -> true), Optional.empty());
        ConnectorTableLayoutHandle layoutHandle = getOnlyElement(tableLayoutResults).getTableLayout().getHandle();
        assertEquals(getAllPartitions(layoutHandle).size(), 1);
        ConnectorSplitSource splitSource = splitManager.getSplits(SESSION, layoutHandle);
        ConnectorSplit split = getOnlyElement(getAllSplits(splitSource));

        try (ConnectorPageSource pageSource = pageSourceProvider.createPageSource(SESSION, split, columnHandles)) {
            assertPageSourceType(pageSource, storageFormat);
            MaterializedResult result = materializeSourceDataStream(SESSION, pageSource, getTypes(columnHandles));
            assertEquals(result.getRowCount(), 3);

            MaterializedRow row;

            row = result.getMaterializedRows().get(0);
            assertEquals(row.getField(0), 1L);
            assertEquals(row.getField(1), "hello");
            assertEquals(row.getField(2), 123L);
            assertEquals(row.getField(3), 43.5);
            assertEquals(row.getField(4), true);

            row = result.getMaterializedRows().get(1);
            assertEquals(row.getField(0), 2L);
            assertNull(row.getField(1));
            assertNull(row.getField(2));
            assertNull(row.getField(3));
            assertNull(row.getField(4));

            row = result.getMaterializedRows().get(2);
            assertEquals(row.getField(0), 3L);
            assertEquals(row.getField(1), "bye");
            assertEquals(row.getField(2), 456L);
            assertEquals(row.getField(3), 98.1);
            assertEquals(row.getField(4), false);
        }
    }

    protected void doCreateEmptyTable(SchemaTableName tableName, HiveStorageFormat storageFormat)
            throws Exception
    {
        // create the table
        Type arrayStringType = requireNonNull(TYPE_MANAGER.getType(parseTypeSignature("array<varchar>")));
        List<ColumnMetadata> columns = ImmutableList.<ColumnMetadata>builder()
                .add(new ColumnMetadata("id", BIGINT, false))
                .add(new ColumnMetadata("t_string", VARCHAR, false))
                .add(new ColumnMetadata("t_bigint", BIGINT, false))
                .add(new ColumnMetadata("t_double", DOUBLE, false))
                .add(new ColumnMetadata("t_boolean", BOOLEAN, false))
                .add(new ColumnMetadata("t_array_string", arrayStringType, false))
                .build();

        Map<String, Object> properties = ImmutableMap.of(STORAGE_FORMAT_PROPERTY, storageFormat);

        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(tableName, columns, properties, SESSION.getUser());

        metadata.createTable(SESSION, tableMetadata);

        // load the new table
        ConnectorTableHandle tableHandle = getTableHandle(tableName);

        // verify the metadata
        tableMetadata = metadata.getTableMetadata(SESSION, getTableHandle(tableName));
        assertEquals(tableMetadata.getOwner(), SESSION.getUser());

        Map<String, ColumnMetadata> columnMap = uniqueIndex(tableMetadata.getColumns(), ColumnMetadata::getName);

        assertPrimitiveField(columnMap, "id", BIGINT, false);
        assertPrimitiveField(columnMap, "t_string", VARCHAR, false);
        assertPrimitiveField(columnMap, "t_bigint", BIGINT, false);
        assertPrimitiveField(columnMap, "t_double", DOUBLE, false);
        assertPrimitiveField(columnMap, "t_boolean", BOOLEAN, false);
        assertPrimitiveField(columnMap, "t_array_string", arrayStringType, false);

        // verify the table is empty
        List<ConnectorTableLayoutResult> tableLayoutResults = metadata.getTableLayouts(SESSION, tableHandle, new Constraint<>(TupleDomain.all(), bindings -> true), Optional.empty());
        ConnectorTableLayoutHandle layoutHandle = getOnlyElement(tableLayoutResults).getTableLayout().getHandle();
        assertEquals(getAllPartitions(layoutHandle).size(), 1);
        ConnectorSplitSource splitSource = splitManager.getSplits(SESSION, layoutHandle);
        assertEquals(getAllSplits(splitSource).size(), 0);
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
        List<ConnectorTableLayoutResult> tableLayoutResults = metadata.getTableLayouts(SESSION, tableHandle, new Constraint<>(TupleDomain.all(), bindings -> true), Optional.empty());
        List<ConnectorSplit> splits = getAllSplits(splitManager.getSplits(SESSION, getOnlyElement(tableLayoutResults).getTableLayout().getHandle()));
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
            if (handle != null) {
                metadata.dropTable(SESSION, handle);
            }
        }
        catch (RuntimeException e) {
            Logger.get(getClass()).warn(e, "failed to drop table");
        }
    }

    protected ConnectorTableHandle getTableHandle(SchemaTableName tableName)
    {
        ConnectorTableHandle handle = metadata.getTableHandle(SESSION, tableName);
        checkArgument(handle != null, "table not found: %s", tableName);
        return handle;
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
        return ((HiveTableLayoutHandle) layoutHandle).getPartitions();
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
}

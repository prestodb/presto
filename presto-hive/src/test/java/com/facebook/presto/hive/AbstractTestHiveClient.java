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
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.RecordProjectOperator;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorRecordSinkProvider;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Domain;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSink;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.SerializableNativeValue;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.ViewNotFoundException;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.split.ConnectorDataStreamProvider;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.net.HostAndPort;
import io.airlift.slice.Slice;
import io.airlift.units.Duration;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.hive.HiveBucketing.HiveBucket;
import static com.facebook.presto.hive.HiveTestUtils.DEFAULT_HIVE_DATA_STREAM_FACTORIES;
import static com.facebook.presto.hive.HiveTestUtils.DEFAULT_HIVE_RECORD_CURSOR_PROVIDER;
import static com.facebook.presto.hive.HiveTestUtils.close;
import static com.facebook.presto.hive.HiveTestUtils.creteOperatorContext;
import static com.facebook.presto.hive.HiveUtil.partitionIdGetter;
import static com.facebook.presto.hive.util.Types.checkType;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.MaterializedResult.materializeSourceDataStream;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Maps.uniqueIndex;
import static com.google.common.util.concurrent.MoreExecutors.sameThreadExecutor;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static java.nio.charset.StandardCharsets.UTF_8;
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
    private static final ConnectorSession SESSION = new ConnectorSession("user", "test", "default", "default", UTC_KEY, Locale.ENGLISH, null, null);

    protected static final String INVALID_DATABASE = "totally_invalid_database_name";
    protected static final String INVALID_TABLE = "totally_invalid_table_name";
    protected static final String INVALID_COLUMN = "totally_invalid_column_name";

    protected String database;
    protected SchemaTableName table;
    protected SchemaTableName tableUnpartitioned;
    protected SchemaTableName tableOffline;
    protected SchemaTableName tableOfflinePartition;
    protected SchemaTableName view;
    protected SchemaTableName invalidTable;
    protected SchemaTableName tableBucketedStringInt;
    protected SchemaTableName tableBucketedBigintBoolean;
    protected SchemaTableName tableBucketedDoubleFloat;

    protected SchemaTableName temporaryCreateTable;
    protected SchemaTableName temporaryCreateSampledTable;
    protected SchemaTableName temporaryRenameTableOld;
    protected SchemaTableName temporaryRenameTableNew;
    protected SchemaTableName temporaryCreateView;
    protected String tableOwner;

    protected ConnectorTableHandle invalidTableHandle;

    protected ConnectorColumnHandle dsColumn;
    protected ConnectorColumnHandle fileFormatColumn;
    protected ConnectorColumnHandle dummyColumn;
    protected ConnectorColumnHandle intColumn;
    protected ConnectorColumnHandle invalidColumnHandle;

    protected Set<ConnectorPartition> partitions;
    protected Set<ConnectorPartition> unpartitionedPartitions;
    protected ConnectorPartition invalidPartition;

    protected DateTimeZone timeZone;

    protected HiveMetastore metastoreClient;

    protected ConnectorMetadata metadata;
    protected ConnectorSplitManager splitManager;
    protected ConnectorDataStreamProvider dataStreamProvider;
    protected ConnectorRecordSinkProvider recordSinkProvider;
    protected ExecutorService executor;

    @BeforeClass
    public void setUp()
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
        database = databaseName;
        table = new SchemaTableName(database, "presto_test");
        tableUnpartitioned = new SchemaTableName(database, "presto_test_unpartitioned");
        tableOffline = new SchemaTableName(database, "presto_test_offline");
        tableOfflinePartition = new SchemaTableName(database, "presto_test_offline_partition");
        view = new SchemaTableName(database, "presto_test_view");
        invalidTable = new SchemaTableName(database, INVALID_TABLE);
        tableBucketedStringInt = new SchemaTableName(database, "presto_test_bucketed_by_string_int");
        tableBucketedBigintBoolean = new SchemaTableName(database, "presto_test_bucketed_by_bigint_boolean");
        tableBucketedDoubleFloat = new SchemaTableName(database, "presto_test_bucketed_by_double_float");

        temporaryCreateTable = new SchemaTableName(database, "tmp_presto_test_create_" + randomName());
        temporaryCreateSampledTable = new SchemaTableName(database, "tmp_presto_test_create_" + randomName());
        temporaryRenameTableOld = new SchemaTableName(database, "tmp_presto_test_rename_" + randomName());
        temporaryRenameTableNew = new SchemaTableName(database, "tmp_presto_test_rename_" + randomName());
        temporaryCreateView = new SchemaTableName(database, "tmp_presto_test_create_" + randomName());
        tableOwner = "presto_test";

        invalidTableHandle = new HiveTableHandle("hive", database, INVALID_TABLE, SESSION);

        dsColumn = new HiveColumnHandle(connectorId, "ds", 0, HiveType.STRING, -1, true);
        fileFormatColumn = new HiveColumnHandle(connectorId, "file_format", 1, HiveType.STRING, -1, true);
        dummyColumn = new HiveColumnHandle(connectorId, "dummy", 2, HiveType.INT, -1, true);
        intColumn = new HiveColumnHandle(connectorId, "t_int", 0, HiveType.INT, -1, true);
        invalidColumnHandle = new HiveColumnHandle(connectorId, INVALID_COLUMN, 0, HiveType.STRING, 0, false);

        partitions = ImmutableSet.<ConnectorPartition>of(
                new HivePartition(table,
                        "ds=2012-12-29/file_format=rcfile-text/dummy=0",
                        ImmutableMap.of(
                                dsColumn,
                                new SerializableNativeValue(Slice.class, utf8Slice("2012-12-29")),
                                fileFormatColumn,
                                new SerializableNativeValue(Slice.class, utf8Slice("rcfile-text")),
                                dummyColumn,
                                new SerializableNativeValue(Long.class, 0L)),
                        Optional.<HiveBucket>absent()),
                new HivePartition(table,
                        "ds=2012-12-29/file_format=rcfile-binary/dummy=2",
                        ImmutableMap.of(
                                dsColumn,
                                new SerializableNativeValue(Slice.class, utf8Slice("2012-12-29")),
                                fileFormatColumn,
                                new SerializableNativeValue(Slice.class, utf8Slice("rcfile-binary")),
                                dummyColumn,
                                new SerializableNativeValue(Long.class, 2L)),
                        Optional.<HiveBucket>absent()),
                new HivePartition(table,
                        "ds=2012-12-29/file_format=sequencefile/dummy=4",
                        ImmutableMap.of(
                                dsColumn,
                                new SerializableNativeValue(Slice.class, utf8Slice("2012-12-29")),
                                fileFormatColumn,
                                new SerializableNativeValue(Slice.class, utf8Slice("sequencefile")),
                                dummyColumn,
                                new SerializableNativeValue(Long.class, 4L)),
                        Optional.<HiveBucket>absent()),
                new HivePartition(table,
                        "ds=2012-12-29/file_format=textfile/dummy=6",
                        ImmutableMap.of(
                                dsColumn,
                                new SerializableNativeValue(Slice.class, utf8Slice("2012-12-29")),
                                fileFormatColumn,
                                new SerializableNativeValue(Slice.class, utf8Slice("textfile")),
                                dummyColumn,
                                new SerializableNativeValue(Long.class, 6L)),
                        Optional.<HiveBucket>absent()));
        unpartitionedPartitions = ImmutableSet.<ConnectorPartition>of(new HivePartition(tableUnpartitioned));
        invalidPartition = new HivePartition(invalidTable, "unknown", ImmutableMap.<ConnectorColumnHandle, SerializableNativeValue>of(), Optional.<HiveBucket>absent());
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
        String proxy = System.getProperty("hive.metastore.thrift.client.socks-proxy");
        if (proxy != null) {
            hiveClientConfig.setMetastoreSocksProxy(HostAndPort.fromString(proxy));
        }

        HiveCluster hiveCluster = new TestingHiveCluster(hiveClientConfig, host, port);

        metastoreClient = new CachingHiveMetastore(hiveCluster, executor, Duration.valueOf("1m"), Duration.valueOf("15s"));

        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(new HdfsConfiguration(hiveClientConfig));
        HiveClient client = new HiveClient(
                new HiveConnectorId(connectorName),
                metastoreClient,
                new NamenodeStats(),
                hdfsEnvironment,
                new HadoopDirectoryLister(),
                timeZone,
                sameThreadExecutor(),
                hiveClientConfig.getMaxSplitSize(),
                maxOutstandingSplits,
                maxThreads,
                hiveClientConfig.getMinPartitionBatchSize(),
                hiveClientConfig.getMaxPartitionBatchSize(),
                hiveClientConfig.getMaxInitialSplitSize(),
                hiveClientConfig.getMaxInitialSplits(),
                false,
                true,
                hiveClientConfig.getHiveStorageFormat(),
                false);

        metadata = client;
        splitManager = client;
        recordSinkProvider = client;

        dataStreamProvider = new HiveDataStreamProvider(hdfsEnvironment, DEFAULT_HIVE_RECORD_CURSOR_PROVIDER, DEFAULT_HIVE_DATA_STREAM_FACTORIES);
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
        assertTrue(tables.contains(table));
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
        ConnectorTableHandle tableHandle = getTableHandle(table);
        ConnectorPartitionResult partitionResult = splitManager.getPartitions(tableHandle, TupleDomain.<ConnectorColumnHandle>all());
        assertExpectedPartitions(partitionResult.getPartitions(), partitions);
    }

    @Test
    public void testGetPartitionsWithBindings()
            throws Exception
    {
        ConnectorTableHandle tableHandle = getTableHandle(table);
        ConnectorPartitionResult partitionResult = splitManager.getPartitions(tableHandle, TupleDomain.withColumnDomains(ImmutableMap.<ConnectorColumnHandle, Domain>of(intColumn,
                Domain.singleValue(5L))));
        assertExpectedPartitions(partitionResult.getPartitions(), partitions);
    }

    @Test(expectedExceptions = TableNotFoundException.class)
    public void testGetPartitionsException()
            throws Exception
    {
        splitManager.getPartitions(invalidTableHandle, TupleDomain.<ConnectorColumnHandle>all());
    }

    @Test
    public void testGetPartitionNames()
            throws Exception
    {
        ConnectorTableHandle tableHandle = getTableHandle(table);
        ConnectorPartitionResult partitionResult = splitManager.getPartitions(tableHandle, TupleDomain.<ConnectorColumnHandle>all());
        assertExpectedPartitions(partitionResult.getPartitions(), partitions);
    }

    protected void assertExpectedPartitions(List<ConnectorPartition> actualPartitions, Iterable<ConnectorPartition> expectedPartitions)
    {
        Map<String, ConnectorPartition> actualById = uniqueIndex(actualPartitions, partitionIdGetter());
        for (ConnectorPartition expected : expectedPartitions) {
            assertInstanceOf(expected, HivePartition.class);
            HivePartition expectedPartition = (HivePartition) expected;

            ConnectorPartition actual = actualById.get(expectedPartition.getPartitionId());
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
        ConnectorPartitionResult partitionResult = splitManager.getPartitions(tableHandle, TupleDomain.<ConnectorColumnHandle>all());
        assertEquals(partitionResult.getPartitions().size(), 1);
        assertEquals(partitionResult.getPartitions(), unpartitionedPartitions);
    }

    @Test(expectedExceptions = TableNotFoundException.class)
    public void testGetPartitionNamesException()
            throws Exception
    {
        splitManager.getPartitions(invalidTableHandle, TupleDomain.<ConnectorColumnHandle>all());
    }

    @SuppressWarnings({"ValueOfIncrementOrDecrementUsed", "UnusedAssignment"})
    @Test
    public void testGetTableSchema()
            throws Exception
    {
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(getTableHandle(table));
        Map<String, ColumnMetadata> map = uniqueIndex(tableMetadata.getColumns(), columnNameGetter());

        int i = 0;
        assertPrimitiveField(map, i++, "t_string", VARCHAR, false);
        assertPrimitiveField(map, i++, "t_tinyint", BIGINT, false);
        assertPrimitiveField(map, i++, "t_smallint", BIGINT, false);
        assertPrimitiveField(map, i++, "t_int", BIGINT, false);
        assertPrimitiveField(map, i++, "t_bigint", BIGINT, false);
        assertPrimitiveField(map, i++, "t_float", DOUBLE, false);
        assertPrimitiveField(map, i++, "t_double", DOUBLE, false);
        assertPrimitiveField(map, i++, "t_map", VARCHAR, false); // Currently mapped as a string
        assertPrimitiveField(map, i++, "t_boolean", BOOLEAN, false);
        assertPrimitiveField(map, i++, "t_timestamp", TIMESTAMP, false);
        assertPrimitiveField(map, i++, "t_binary", VARBINARY, false);
        assertPrimitiveField(map, i++, "t_array_string", VARCHAR, false); // Currently mapped as a string
        assertPrimitiveField(map, i++, "t_complex", VARCHAR, false); // Currently mapped as a string
        assertPrimitiveField(map, i++, "ds", VARCHAR, true);
        assertPrimitiveField(map, i++, "file_format", VARCHAR, true);
        assertPrimitiveField(map, i++, "dummy", BIGINT, true);
    }

    @Test
    public void testGetTableSchemaUnpartitioned()
            throws Exception
    {
        ConnectorTableHandle tableHandle = getTableHandle(tableUnpartitioned);
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(tableHandle);
        Map<String, ColumnMetadata> map = uniqueIndex(tableMetadata.getColumns(), columnNameGetter());

        assertPrimitiveField(map, 0, "t_string", VARCHAR, false);
        assertPrimitiveField(map, 1, "t_tinyint", BIGINT, false);
    }

    @Test
    public void testGetTableSchemaOffline()
            throws Exception
    {
        ConnectorTableHandle tableHandle = getTableHandle(tableOffline);
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(tableHandle);
        Map<String, ColumnMetadata> map = uniqueIndex(tableMetadata.getColumns(), columnNameGetter());

        assertPrimitiveField(map, 0, "t_string", VARCHAR, false);
    }

    @Test
    public void testGetTableSchemaOfflinePartition()
            throws Exception
    {
        ConnectorTableHandle tableHandle = getTableHandle(tableOfflinePartition);
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(tableHandle);
        Map<String, ColumnMetadata> map = uniqueIndex(tableMetadata.getColumns(), columnNameGetter());

        assertPrimitiveField(map, 0, "t_string", VARCHAR, false);
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
        ConnectorTableHandle tableHandle = getTableHandle(table);
        ConnectorPartitionResult partitionResult = splitManager.getPartitions(tableHandle, TupleDomain.<ConnectorColumnHandle>all());
        ConnectorSplitSource splitSource = splitManager.getPartitionSplits(tableHandle, partitionResult.getPartitions());

        assertEquals(getSplitCount(splitSource), partitions.size());
    }

    @Test
    public void testGetPartitionSplitsBatchUnpartitioned()
            throws Exception
    {
        ConnectorTableHandle tableHandle = getTableHandle(tableUnpartitioned);
        ConnectorPartitionResult partitionResult = splitManager.getPartitions(tableHandle, TupleDomain.<ConnectorColumnHandle>all());
        ConnectorSplitSource splitSource = splitManager.getPartitionSplits(tableHandle, partitionResult.getPartitions());

        assertEquals(getSplitCount(splitSource), 1);
    }

    @Test(expectedExceptions = TableNotFoundException.class)
    public void testGetPartitionSplitsBatchInvalidTable()
            throws Exception
    {
        splitManager.getPartitionSplits(invalidTableHandle, ImmutableList.of(invalidPartition));
    }

    @Test
    public void testGetPartitionSplitsEmpty()
            throws Exception
    {
        ConnectorSplitSource splitSource = splitManager.getPartitionSplits(invalidTableHandle, ImmutableList.<ConnectorPartition>of());
        // fetch full list
        getSplitCount(splitSource);
    }

    @Test
    public void testGetPartitionTableOffline()
            throws Exception
    {
        ConnectorTableHandle tableHandle = getTableHandle(tableOffline);
        try {
            splitManager.getPartitions(tableHandle, TupleDomain.<ConnectorColumnHandle>all());
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

        ConnectorColumnHandle dsColumn = metadata.getColumnHandle(tableHandle, "ds");
        assertNotNull(dsColumn);

        Domain domain = Domain.singleValue(utf8Slice("2012-12-30"));
        TupleDomain<ConnectorColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(dsColumn, domain));
        ConnectorPartitionResult partitionResult = splitManager.getPartitions(tableHandle, tupleDomain);
        for (ConnectorPartition partition : partitionResult.getPartitions()) {
            if (domain.equals(partition.getTupleDomain().getDomains().get(dsColumn))) {
                try {
                    getSplitCount(splitManager.getPartitionSplits(tableHandle, ImmutableList.of(partition)));
                    fail("Expected PartitionOfflineException");
                }
                catch (PartitionOfflineException e) {
                    assertEquals(e.getTableName(), tableOfflinePartition);
                    assertEquals(e.getPartition(), "ds=2012-12-30");
                }
            }
            else {
                getSplitCount(splitManager.getPartitionSplits(tableHandle, ImmutableList.of(partition)));
            }
        }
    }

    @Test
    public void testBucketedTableStringInt()
            throws Exception
    {
        ConnectorTableHandle tableHandle = getTableHandle(tableBucketedStringInt);
        List<ConnectorColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(tableHandle).values());
        Map<String, Integer> columnIndex = indexColumns(columnHandles);

        assertTableIsBucketed(tableHandle);

        String testString = "sequencefile test";
        Long testInt = 413L;
        Long testSmallint = 412L;

        // Reverse the order of bindings as compared to bucketing order
        ImmutableMap<ConnectorColumnHandle, SerializableNativeValue> bindings = ImmutableMap.<ConnectorColumnHandle, SerializableNativeValue>builder()
                .put(columnHandles.get(columnIndex.get("t_int")), new SerializableNativeValue(Long.class, testInt))
                .put(columnHandles.get(columnIndex.get("t_string")), new SerializableNativeValue(Slice.class, utf8Slice(testString)))
                .put(columnHandles.get(columnIndex.get("t_smallint")), new SerializableNativeValue(Long.class, testSmallint))
                .build();

        ConnectorPartitionResult partitionResult = splitManager.getPartitions(tableHandle, TupleDomain.withNullableFixedValues(bindings));
        List<ConnectorSplit> splits = getAllSplits(splitManager.getPartitionSplits(tableHandle, partitionResult.getPartitions()));
        assertEquals(splits.size(), 1);

        Operator dataStream = dataStreamProvider.createNewDataStream(creteOperatorContext(SESSION, executor), splits.get(0), columnHandles);
        try {
            MaterializedResult result = materializeSourceDataStream(SESSION, dataStream);

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
        finally {
            close(dataStream);
        }
    }

    @Test
    public void testBucketedTableBigintBoolean()
            throws Exception
    {
        ConnectorTableHandle tableHandle = getTableHandle(tableBucketedBigintBoolean);
        List<ConnectorColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(tableHandle).values());
        Map<String, Integer> columnIndex = indexColumns(columnHandles);

        assertTableIsBucketed(tableHandle);

        String testString = "textfile test";
        // This needs to match one of the rows where t_string is not empty or null, and where t_bigint is not null
        // (i.e. (testBigint - 604) % 19 > 1 and (testBigint - 604) % 13 != 0)
        Long testBigint = 608L;
        Boolean testBoolean = true;

        ImmutableMap<ConnectorColumnHandle, SerializableNativeValue> bindings = ImmutableMap.<ConnectorColumnHandle, SerializableNativeValue>builder()
                .put(columnHandles.get(columnIndex.get("t_string")), new SerializableNativeValue(Slice.class, utf8Slice(testString)))
                .put(columnHandles.get(columnIndex.get("t_bigint")), new SerializableNativeValue(Long.class, testBigint))
                .put(columnHandles.get(columnIndex.get("t_boolean")), new SerializableNativeValue(Boolean.class, testBoolean))
                .build();

        ConnectorPartitionResult partitionResult = splitManager.getPartitions(tableHandle, TupleDomain.withNullableFixedValues(bindings));
        List<ConnectorSplit> splits = getAllSplits(splitManager.getPartitionSplits(tableHandle, partitionResult.getPartitions()));
        assertEquals(splits.size(), 1);

        Operator dataStream = dataStreamProvider.createNewDataStream(creteOperatorContext(SESSION, executor), splits.get(0), columnHandles);
        try {
            MaterializedResult result = materializeSourceDataStream(SESSION, dataStream);

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
        finally {
            close(dataStream);
        }
    }

    @Test
    public void testBucketedTableDoubleFloat()
            throws Exception
    {
        ConnectorTableHandle tableHandle = getTableHandle(tableBucketedDoubleFloat);
        List<ConnectorColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(tableHandle).values());
        Map<String, Integer> columnIndex = indexColumns(columnHandles);

        assertTableIsBucketed(tableHandle);

        ImmutableMap<ConnectorColumnHandle, SerializableNativeValue> bindings = ImmutableMap.<ConnectorColumnHandle, SerializableNativeValue>builder()
                .put(columnHandles.get(columnIndex.get("t_float")), new SerializableNativeValue(Double.class, 406.1000061035156))
                .put(columnHandles.get(columnIndex.get("t_double")), new SerializableNativeValue(Double.class, 407.2))
                .build();

        // floats and doubles are not supported, so we should see all splits
        ConnectorPartitionResult partitionResult = splitManager.getPartitions(tableHandle, TupleDomain.withNullableFixedValues(bindings));
        List<ConnectorSplit> splits = getAllSplits(splitManager.getPartitionSplits(tableHandle, partitionResult.getPartitions()));
        assertEquals(splits.size(), 32);

        int count = 0;
        for (ConnectorSplit split : splits) {
            Operator dataStream = dataStreamProvider.createNewDataStream(creteOperatorContext(SESSION, executor), split, columnHandles);
            try {
                MaterializedResult result = materializeSourceDataStream(SESSION, dataStream);
                count += result.getRowCount();
            }
            finally {
                close(dataStream);
            }
        }
        assertEquals(count, 300);
    }

    private void assertTableIsBucketed(ConnectorTableHandle tableHandle)
            throws Exception
    {
        // the bucketed test tables should have exactly 32 splits
        ConnectorPartitionResult partitionResult = splitManager.getPartitions(tableHandle, TupleDomain.<ConnectorColumnHandle>all());
        List<ConnectorSplit> splits = getAllSplits(splitManager.getPartitionSplits(tableHandle, partitionResult.getPartitions()));
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
        ConnectorTableHandle tableHandle = getTableHandle(table);
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(tableHandle);
        List<ConnectorColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(tableHandle).values());
        Map<String, Integer> columnIndex = indexColumns(columnHandles);

        ConnectorPartitionResult partitionResult = splitManager.getPartitions(tableHandle, TupleDomain.<ConnectorColumnHandle>all());
        List<ConnectorSplit> splits = getAllSplits(splitManager.getPartitionSplits(tableHandle, partitionResult.getPartitions()));
        assertEquals(splits.size(), this.partitions.size());
        for (ConnectorSplit split : splits) {
            HiveSplit hiveSplit = (HiveSplit) split;

            List<HivePartitionKey> partitionKeys = hiveSplit.getPartitionKeys();
            String ds = partitionKeys.get(0).getValue();
            String fileType = partitionKeys.get(1).getValue();
            long dummy = Long.parseLong(partitionKeys.get(2).getValue());

            long baseValue = getBaseValueForFileType(fileType);
            assertEquals(dummy * 100, baseValue);

            long rowNumber = 0;
            long completedBytes = 0;
            OperatorContext operatorContext = creteOperatorContext(SESSION, executor);
            Operator dataStream = dataStreamProvider.createNewDataStream(operatorContext, hiveSplit, columnHandles);
            try {
                assertOpertorTypes(dataStream, tableMetadata.getColumns());
                MaterializedResult result = materializeSourceDataStream(SESSION, dataStream);

                assertDataStreamType(dataStream, fileType);

                for (MaterializedRow row : result) {
                    try {
                        assertValueTupes(row, tableMetadata.getColumns());
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
                        assertEquals(row.getField(columnIndex.get("t_string")), (fileType + " test"));
                    }

                    assertEquals(row.getField(columnIndex.get("t_tinyint")), (long) ((byte) (baseValue + 1 + rowNumber)));
                    assertEquals(row.getField(columnIndex.get("t_smallint")), baseValue + 2 + rowNumber);
                    assertEquals(row.getField(columnIndex.get("t_int")), baseValue + 3 + rowNumber);

                    if (rowNumber % 13 == 0) {
                        assertNull(row.getField(columnIndex.get("t_bigint")));
                    }
                    else {
                        assertEquals(row.getField(columnIndex.get("t_bigint")), baseValue + 4 + rowNumber);
                    }

                    assertEquals((float) row.getField(columnIndex.get("t_float")), baseValue + 5.1 + rowNumber, 0.001);
                    assertEquals(row.getField(columnIndex.get("t_double")), baseValue + 6.2 + rowNumber);

                    if (rowNumber % 3 == 2) {
                        assertNull(row.getField(columnIndex.get("t_boolean")));
                    }
                    else {
                        assertEquals(row.getField(columnIndex.get("t_boolean")), rowNumber % 3 != 0);
                    }

                    if (rowNumber % 17 == 0) {
                        assertNull(row.getField(columnIndex.get("t_timestamp")));
                    }
                    else {
                        long millis = new DateTime(2011, 5, 6, 7, 8, 9, 123, timeZone).getMillis();
                        assertEquals(row.getField(columnIndex.get("t_timestamp")), millis, (fileType + " test"));
                    }

                    if (rowNumber % 23 == 0) {
                        assertNull(row.getField(columnIndex.get("t_binary")));
                    }
                    else {
                        assertEquals(row.getField(columnIndex.get("t_binary")), (fileType + " test"));
                    }

                    if (rowNumber % 29 == 0) {
                        assertNull(row.getField(columnIndex.get("t_map")));
                    }
                    else {
                        String expectedJson = "{\"format\":\"" + fileType + "\"}";
                        assertEquals(row.getField(columnIndex.get("t_map")), expectedJson);
                    }

                    if (rowNumber % 27 == 0) {
                        assertNull(row.getField(columnIndex.get("t_array_string")));
                    }
                    else {
                        String expectedJson = "[\"" + fileType + "\",\"test\",\"data\"]";
                        assertEquals(row.getField(columnIndex.get("t_array_string")), expectedJson);
                    }

                    if (rowNumber % 31 == 0) {
                        assertNull(row.getField(columnIndex.get("t_complex")));
                    }
                    else {
                        String expectedJson = "{\"1\":[{\"s_string\":\"" + fileType + "-a\",\"s_double\":0.1},{\"s_string\":\"" + fileType + "-b\",\"s_double\":0.2}]}";
                        assertEquals(row.getField(columnIndex.get("t_complex")), expectedJson);
                    }

                    assertEquals(row.getField(columnIndex.get("ds")), ds);
                    assertEquals(row.getField(columnIndex.get("file_format")), fileType);
                    assertEquals(row.getField(columnIndex.get("dummy")), dummy);

                    long newCompletedBytes = operatorContext.getInputDataSize().getTotalCount();
                    assertTrue(newCompletedBytes >= completedBytes);
                    assertTrue(newCompletedBytes <= hiveSplit.getLength());
                    completedBytes = newCompletedBytes;
                }

                assertTrue(completedBytes <= hiveSplit.getLength());
                assertEquals(rowNumber, 100);
            }
            finally {
                close(dataStream);
            }

        }
    }

    @Test
    public void testGetPartialRecords()
            throws Exception
    {
        ConnectorTableHandle tableHandle = getTableHandle(table);
        List<ConnectorColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(tableHandle).values());
        Map<String, Integer> columnIndex = indexColumns(columnHandles);

        ConnectorPartitionResult partitionResult = splitManager.getPartitions(tableHandle, TupleDomain.<ConnectorColumnHandle>all());
        List<ConnectorSplit> splits = getAllSplits(splitManager.getPartitionSplits(tableHandle, partitionResult.getPartitions()));
        assertEquals(splits.size(), this.partitions.size());
        for (ConnectorSplit split : splits) {
            HiveSplit hiveSplit = (HiveSplit) split;

            List<HivePartitionKey> partitionKeys = hiveSplit.getPartitionKeys();
            String ds = partitionKeys.get(0).getValue();
            String fileType = partitionKeys.get(1).getValue();
            long dummy = Long.parseLong(partitionKeys.get(2).getValue());

            long baseValue = getBaseValueForFileType(fileType);

            long rowNumber = 0;
            Operator dataStream = dataStreamProvider.createNewDataStream(creteOperatorContext(SESSION, executor), hiveSplit, columnHandles);
            try {
                assertDataStreamType(dataStream, fileType);
                MaterializedResult result = materializeSourceDataStream(SESSION, dataStream);
                for (MaterializedRow row : result) {
                    rowNumber++;

                    assertEquals(row.getField(columnIndex.get("t_double")), baseValue + 6.2 + rowNumber);
                    assertEquals(row.getField(columnIndex.get("ds")), ds);
                    assertEquals(row.getField(columnIndex.get("file_format")), fileType);
                    assertEquals(row.getField(columnIndex.get("dummy")), dummy);
                }
            }
            finally {
                close(dataStream);
            }
            assertEquals(rowNumber, 100);
        }
    }

    @Test
    public void testGetRecordsUnpartitioned()
            throws Exception
    {
        ConnectorTableHandle tableHandle = getTableHandle(tableUnpartitioned);
        List<ConnectorColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(tableHandle).values());
        Map<String, Integer> columnIndex = indexColumns(columnHandles);

        ConnectorPartitionResult partitionResult = splitManager.getPartitions(tableHandle, TupleDomain.<ConnectorColumnHandle>all());
        List<ConnectorSplit> splits = getAllSplits(splitManager.getPartitionSplits(tableHandle, partitionResult.getPartitions()));
        assertEquals(splits.size(), 1);

        for (ConnectorSplit split : splits) {
            HiveSplit hiveSplit = (HiveSplit) split;

            assertEquals(hiveSplit.getPartitionKeys(), ImmutableList.of());

            long rowNumber = 0;
            OperatorContext operatorContext = creteOperatorContext(SESSION, executor);
            Operator dataStream = dataStreamProvider.createNewDataStream(operatorContext, split, columnHandles);
            try {
                assertDataStreamType(dataStream, "textfile");
                MaterializedResult result = materializeSourceDataStream(SESSION, dataStream);

                assertEquals(operatorContext.getInputDataSize().getTotalCount(), hiveSplit.getLength());
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
            finally {
                close(dataStream);
            }
            assertEquals(rowNumber, 100);
        }
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*" + INVALID_COLUMN + ".*")
    public void testGetRecordsInvalidColumn()
            throws Exception
    {
        ConnectorTableHandle table = getTableHandle(tableUnpartitioned);
        ConnectorPartitionResult partitionResult = splitManager.getPartitions(table, TupleDomain.<ConnectorColumnHandle>all());
        ConnectorSplit split = Iterables.getFirst(getAllSplits(splitManager.getPartitionSplits(table, partitionResult.getPartitions())), null);
        dataStreamProvider.createNewDataStream(creteOperatorContext(SESSION, executor), split, ImmutableList.of(invalidColumnHandle));
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

            metadata.renameTable(getTableHandle(temporaryRenameTableOld), temporaryRenameTableNew);

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
        try {
            doCreateTable();
        }
        finally {
            dropTable(temporaryCreateTable);
        }
    }

    @Test
    public void testSampledTableCreation()
            throws Exception
    {
        try {
            doCreateSampledTable();
        }
        finally {
            dropTable(temporaryCreateSampledTable);
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

    private void createDummyTable(SchemaTableName tableName)
    {
        List<ColumnMetadata> columns = ImmutableList.of(new ColumnMetadata("dummy", VARCHAR, 1, false));
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(tableName, columns, tableOwner);
        ConnectorOutputTableHandle handle = metadata.beginCreateTable(SESSION, tableMetadata);
        metadata.commitCreateTable(handle, ImmutableList.<String>of());
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

        Map<SchemaTableName, String> views = metadata.getViews(SESSION, viewName.toSchemaTablePrefix());
        assertEquals(views.size(), 1);
        assertEquals(views.get(viewName), viewData);

        assertTrue(metadata.listViews(SESSION, viewName.getSchemaName()).contains(viewName));
    }

    private void doCreateSampledTable()
            throws Exception
    {
        // begin creating the table
        List<ColumnMetadata> columns = ImmutableList.<ColumnMetadata>builder()
                .add(new ColumnMetadata("sales", BIGINT, 1, false))
                .build();

        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(temporaryCreateSampledTable, columns, tableOwner, true);
        ConnectorOutputTableHandle outputHandle = metadata.beginCreateTable(SESSION, tableMetadata);

        // write the records
        RecordSink sink = recordSinkProvider.getRecordSink(outputHandle);

        sink.beginRecord(8);
        sink.appendLong(2);
        sink.finishRecord();

        sink.beginRecord(5);
        sink.appendLong(3);
        sink.finishRecord();

        sink.beginRecord(7);
        sink.appendLong(4);
        sink.finishRecord();

        String fragment = sink.commit();

        // commit the table
        metadata.commitCreateTable(outputHandle, ImmutableList.of(fragment));

        // load the new table
        ConnectorTableHandle tableHandle = getTableHandle(temporaryCreateSampledTable);
        List<ConnectorColumnHandle> columnHandles = ImmutableList.<ConnectorColumnHandle>builder()
                .addAll(metadata.getColumnHandles(tableHandle).values())
                .add(metadata.getSampleWeightColumnHandle(tableHandle))
                .build();
        assertEquals(columnHandles.size(), 2);

        // verify the metadata
        tableMetadata = metadata.getTableMetadata(getTableHandle(temporaryCreateSampledTable));
        assertEquals(tableMetadata.getOwner(), tableOwner);

        Map<String, ColumnMetadata> columnMap = uniqueIndex(tableMetadata.getColumns(), columnNameGetter());
        assertEquals(columnMap.size(), 1);

        assertPrimitiveField(columnMap, 0, "sales", BIGINT, false);

        // verify the data
        ConnectorPartitionResult partitionResult = splitManager.getPartitions(tableHandle, TupleDomain.<ConnectorColumnHandle>all());
        assertEquals(partitionResult.getPartitions().size(), 1);
        ConnectorSplitSource splitSource = splitManager.getPartitionSplits(tableHandle, partitionResult.getPartitions());
        ConnectorSplit split = getOnlyElement(getAllSplits(splitSource));

        Operator dataStream = dataStreamProvider.createNewDataStream(creteOperatorContext(SESSION, executor), split, columnHandles);
        try {
            assertDataStreamType(dataStream, "rcfile-binary");
            MaterializedResult result = materializeSourceDataStream(SESSION, dataStream);
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
        finally {
            close(dataStream);
        }
    }

    private void doCreateTable()
            throws Exception
    {
        // begin creating the table
        List<ColumnMetadata> columns = ImmutableList.<ColumnMetadata>builder()
                .add(new ColumnMetadata("id", BIGINT, 1, false))
                .add(new ColumnMetadata("t_string", VARCHAR, 2, false))
                .add(new ColumnMetadata("t_bigint", BIGINT, 3, false))
                .add(new ColumnMetadata("t_double", DOUBLE, 4, false))
                .add(new ColumnMetadata("t_boolean", BOOLEAN, 5, false))
                .build();

        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(temporaryCreateTable, columns, tableOwner);
        ConnectorOutputTableHandle outputHandle = metadata.beginCreateTable(SESSION, tableMetadata);

        // write the records
        RecordSink sink = recordSinkProvider.getRecordSink(outputHandle);

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

        String fragment = sink.commit();

        // commit the table
        metadata.commitCreateTable(outputHandle, ImmutableList.of(fragment));

        // load the new table
        ConnectorTableHandle tableHandle = getTableHandle(temporaryCreateTable);
        List<ConnectorColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(tableHandle).values());

        // verify the metadata
        tableMetadata = metadata.getTableMetadata(getTableHandle(temporaryCreateTable));
        assertEquals(tableMetadata.getOwner(), tableOwner);

        Map<String, ColumnMetadata> columnMap = uniqueIndex(tableMetadata.getColumns(), columnNameGetter());

        assertPrimitiveField(columnMap, 0, "id", BIGINT, false);
        assertPrimitiveField(columnMap, 1, "t_string", VARCHAR, false);
        assertPrimitiveField(columnMap, 2, "t_bigint", BIGINT, false);
        assertPrimitiveField(columnMap, 3, "t_double", DOUBLE, false);
        assertPrimitiveField(columnMap, 4, "t_boolean", BOOLEAN, false);

        // verify the data
        ConnectorPartitionResult partitionResult = splitManager.getPartitions(tableHandle, TupleDomain.<ConnectorColumnHandle>all());
        assertEquals(partitionResult.getPartitions().size(), 1);
        ConnectorSplitSource splitSource = splitManager.getPartitionSplits(tableHandle, partitionResult.getPartitions());
        ConnectorSplit split = getOnlyElement(getAllSplits(splitSource));

        Operator dataStream = dataStreamProvider.createNewDataStream(creteOperatorContext(SESSION, executor), split, columnHandles);
        try {
            assertDataStreamType(dataStream, "rcfile-binary");
            MaterializedResult result = materializeSourceDataStream(SESSION, dataStream);
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
            assertEquals(row.getField(0), 2L);
            assertEquals(row.getField(1), "bye");
            assertEquals(row.getField(2), 456L);
            assertEquals(row.getField(3), 98.1);
            assertEquals(row.getField(4), false);
        }
        finally {
            close(dataStream);
        }
    }

    private void dropTable(SchemaTableName table)
    {
        try {
            metastoreClient.dropTable(table.getSchemaName(), table.getTableName());
        }
        catch (RuntimeException e) {
            // this usually occurs because the table was not created
        }
    }

    private ConnectorTableHandle getTableHandle(SchemaTableName tableName)
    {
        ConnectorTableHandle handle = metadata.getTableHandle(SESSION, tableName);
        checkArgument(handle != null, "table not found: %s", tableName);
        return handle;
    }

    private static int getSplitCount(ConnectorSplitSource splitSource)
            throws InterruptedException
    {
        int splitCount = 0;
        while (!splitSource.isFinished()) {
            List<ConnectorSplit> batch = splitSource.getNextBatch(1000);
            splitCount += batch.size();
        }
        return splitCount;
    }

    private static List<ConnectorSplit> getAllSplits(ConnectorSplitSource splitSource)
            throws InterruptedException
    {
        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
        while (!splitSource.isFinished()) {
            List<ConnectorSplit> batch = splitSource.getNextBatch(1000);
            splits.addAll(batch);
        }
        return splits.build();
    }

    private static long getBaseValueForFileType(String fileType)
    {
        switch (fileType) {
            case "rcfile-text":
                return 0;
            case "rcfile-binary":
                return 200;
            case "sequencefile":
                return 400;
            case "textfile":
                return 600;
            default:
                throw new IllegalArgumentException("Unexpected fileType key " + fileType);
        }
    }

    private static void assertDataStreamType(Operator dataStream, String fileType)
    {
        if (dataStream instanceof RecordProjectOperator) {
            assertRecordCursorType(((RecordProjectOperator) dataStream).getCursor(), fileType);
        }
        fail("Unexpected dataStream operator type for file type " + fileType);
    }

    private static void assertRecordCursorType(RecordCursor cursor, String fileType)
    {
        if (fileType.equals("rcfile-text")) {
            assertInstanceOf(cursor, ColumnarTextHiveRecordCursor.class, fileType);
        }
        else if (fileType.equals("rcfile-binary")) {
            assertInstanceOf(cursor, ColumnarBinaryHiveRecordCursor.class, fileType);
        }
        else {
            assertInstanceOf(cursor, GenericHiveRecordCursor.class, fileType);
        }
    }

    private static void assertOpertorTypes(Operator cursor, List<ColumnMetadata> schema)
    {
        for (int columnIndex = 0; columnIndex < schema.size(); columnIndex++) {
            ColumnMetadata column = schema.get(columnIndex);
            assertEquals(cursor.getTypes().get(columnIndex), column.getType());
        }
    }

    private static void assertValueTupes(MaterializedRow row, List<ColumnMetadata> schema)
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
                else if (VARCHAR.equals(column.getType()) || VARBINARY.equals(column.getType())) {
                    assertInstanceOf(value, String.class);
                }
                else if (TIMESTAMP.equals(column.getType())) {
                    assertInstanceOf(value, Long.class);
                }
                else {
                    fail("Unknown primitive type " + columnIndex);
                }
            }
        }
    }

    private static void assertPrimitiveField(Map<String, ColumnMetadata> map, int position, String name, Type type, boolean partitionKey)
    {
        assertTrue(map.containsKey(name));
        ColumnMetadata column = map.get(name);
        assertEquals(column.getOrdinalPosition(), position);
        assertEquals(column.getType(), type, name);
        assertEquals(column.isPartitionKey(), partitionKey, name);
    }

    private static ImmutableMap<String, Integer> indexColumns(List<ConnectorColumnHandle> columnHandles)
    {
        ImmutableMap.Builder<String, Integer> index = ImmutableMap.builder();
        int i = 0;
        for (ConnectorColumnHandle columnHandle : columnHandles) {
            HiveColumnHandle hiveColumnHandle = checkType(columnHandle, HiveColumnHandle.class, "columnHandle");
            index.put(hiveColumnHandle.getName(), i);
            i++;
        }
        return index.build();
    }

    private static String randomName()
    {
        return UUID.randomUUID().toString().toLowerCase().replace("-", "");
    }

    private static Function<ColumnMetadata, String> columnNameGetter()
    {
        return new Function<ColumnMetadata, String>()
        {
            @Override
            public String apply(ColumnMetadata input)
            {
                return input.getName();
            }
        };
    }
}

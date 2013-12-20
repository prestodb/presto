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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Domain;
import com.facebook.presto.spi.Partition;
import com.facebook.presto.spi.PartitionResult;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.Split;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.airlift.units.Duration;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;

import static com.facebook.presto.hive.HiveBucketing.HiveBucket;
import static com.facebook.presto.hive.HiveUtil.partitionIdGetter;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Maps.uniqueIndex;
import static com.google.common.util.concurrent.MoreExecutors.sameThreadExecutor;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(groups = "hive")
public abstract class AbstractTestHiveClient
{
    public static final String INVALID_DATABASE = "totally_invalid_database";
    public static final String INVALID_COLUMN = "totally_invalid_column_name";
    public static final byte[] EMPTY_STRING = new byte[0];

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

    protected TableHandle invalidTableHandle;

    protected ColumnHandle dsColumn;
    protected ColumnHandle fileFormatColumn;
    protected ColumnHandle dummyColumn;
    protected ColumnHandle intColumn;
    protected ColumnHandle invalidColumnHandle;

    protected Set<Partition> partitions;
    protected Set<Partition> unpartitionedPartitions;
    protected Partition invalidPartition;

    protected ConnectorMetadata metadata;
    protected ConnectorSplitManager splitManager;
    protected ConnectorRecordSetProvider recordSetProvider;

    protected void setupHive(String connectorId, String databaseName)
    {
        database = databaseName;
        table = new SchemaTableName(database, "presto_test");
        tableUnpartitioned = new SchemaTableName(database, "presto_test_unpartitioned");
        tableOffline = new SchemaTableName(database, "presto_test_offline");
        tableOfflinePartition = new SchemaTableName(database, "presto_test_offline_partition");
        view = new SchemaTableName(database, "presto_test_view");
        invalidTable = new SchemaTableName(database, "totally_invalid_table_name");
        tableBucketedStringInt = new SchemaTableName(database, "presto_test_bucketed_by_string_int");
        tableBucketedBigintBoolean = new SchemaTableName(database, "presto_test_bucketed_by_bigint_boolean");
        tableBucketedDoubleFloat = new SchemaTableName(database, "presto_test_bucketed_by_double_float");

        invalidTableHandle = new HiveTableHandle("hive", database, "totally_invalid_table_name");

        dsColumn = new HiveColumnHandle(connectorId, "ds", 0, HiveType.STRING, -1, true);
        fileFormatColumn = new HiveColumnHandle(connectorId, "file_format", 1, HiveType.STRING, -1, true);
        dummyColumn = new HiveColumnHandle(connectorId, "dummy", 2, HiveType.INT, -1, true);
        intColumn = new HiveColumnHandle(connectorId, "t_int", 0, HiveType.INT, -1, true);
        invalidColumnHandle = new HiveColumnHandle(connectorId, INVALID_COLUMN, 0, HiveType.STRING, 0, false);

        partitions = ImmutableSet.<Partition>of(
                new HivePartition(table,
                        "ds=2012-12-29/file_format=rcfile-text/dummy=0",
                        ImmutableMap.<ColumnHandle, Comparable<?>>of(dsColumn, "2012-12-29", fileFormatColumn, "rcfile-text", dummyColumn, 0L),
                        Optional.<HiveBucket>absent()),
                new HivePartition(table,
                        "ds=2012-12-29/file_format=rcfile-binary/dummy=2",
                        ImmutableMap.<ColumnHandle, Comparable<?>>of(dsColumn, "2012-12-29", fileFormatColumn, "rcfile-binary", dummyColumn, 2L),
                        Optional.<HiveBucket>absent()),
                new HivePartition(table,
                        "ds=2012-12-29/file_format=sequencefile/dummy=4",
                        ImmutableMap.<ColumnHandle, Comparable<?>>of(dsColumn, "2012-12-29", fileFormatColumn, "sequencefile", dummyColumn, 4L),
                        Optional.<HiveBucket>absent()),
                new HivePartition(table,
                        "ds=2012-12-29/file_format=textfile/dummy=6",
                        ImmutableMap.<ColumnHandle, Comparable<?>>of(dsColumn, "2012-12-29", fileFormatColumn, "textfile", dummyColumn, 6L),
                        Optional.<HiveBucket>absent()));
        unpartitionedPartitions = ImmutableSet.<Partition>of(new HivePartition(tableUnpartitioned));
        invalidPartition = new HivePartition(invalidTable, "unknown", ImmutableMap.<ColumnHandle, Comparable<?>>of(), Optional.<HiveBucket>absent());
    }

    protected void setup(String host, int port, String databaseName)
    {
        setup(host, port, databaseName, "hive-test", 100, 50);
    }

    protected void setup(String host, int port, String databaseName, String connectorName, int maxOutstandingSplits, int maxThreads)
    {
        setupHive(connectorName, databaseName);

        HiveClientConfig hiveClientConfig = new HiveClientConfig();
        String proxy = System.getProperty("hive.metastore.thrift.client.socks-proxy");
        if (proxy != null) {
            hiveClientConfig.setMetastoreSocksProxy(HostAndPort.fromString(proxy));
        }

        FileSystemWrapper fileSystemWrapper = new FileSystemWrapperProvider(new FileSystemCache(hiveClientConfig)).get();

        HiveCluster hiveCluster = new TestingHiveCluster(hiveClientConfig, host, port);
        ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("hive-%s"));

        HiveClient client = new HiveClient(
                new HiveConnectorId(connectorName),
                new CachingHiveMetastore(hiveCluster, executor, Duration.valueOf("1m"), Duration.valueOf("15s")),
                new HdfsEnvironment(new HdfsConfiguration(hiveClientConfig), fileSystemWrapper),
                sameThreadExecutor(),
                hiveClientConfig.getMaxSplitSize(),
                maxOutstandingSplits,
                maxThreads,
                hiveClientConfig.getMinPartitionBatchSize(),
                hiveClientConfig.getMaxPartitionBatchSize());

        metadata = client;
        splitManager = client;
        recordSetProvider = client;
    }

    @Test
    public void testGetDatabaseNames()
            throws Exception
    {
        List<String> databases = metadata.listSchemaNames();
        assertTrue(databases.contains(database));
    }

    @Test
    public void testGetTableNames()
            throws Exception
    {
        List<SchemaTableName> tables = metadata.listTables(database);
        assertTrue(tables.contains(table));
    }

    // disabled until metadata manager is updated to handle invalid catalogs and schemas
    @Test(enabled = false, expectedExceptions = SchemaNotFoundException.class)
    public void testGetTableNamesException()
            throws Exception
    {
        metadata.listTables(INVALID_DATABASE);
    }

    @Test
    public void testListUnknownSchema()
    {
        assertNull(metadata.getTableHandle(new SchemaTableName("totally_invalid_database_name", "dual")));
        assertEquals(metadata.listTables("totally_invalid_database_name"), ImmutableList.of());
        assertEquals(metadata.listTableColumns(new SchemaTablePrefix("totally_invalid_database_name", "dual")), ImmutableMap.of());
    }

    @Test
    public void testGetPartitions()
            throws Exception
    {
        TableHandle tableHandle = getTableHandle(table);
        PartitionResult partitionResult = splitManager.getPartitions(tableHandle, TupleDomain.all());
        assertExpectedPartitions(partitionResult.getPartitions());
    }

    @Test
    public void testGetPartitionsWithBindings()
            throws Exception
    {
        TableHandle tableHandle = getTableHandle(table);
        PartitionResult partitionResult = splitManager.getPartitions(tableHandle, TupleDomain.withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(intColumn, Domain.singleValue(5L))));
        assertExpectedPartitions(partitionResult.getPartitions());
    }

    @Test(expectedExceptions = TableNotFoundException.class)
    public void testGetPartitionsException()
            throws Exception
    {
        splitManager.getPartitions(invalidTableHandle, TupleDomain.all());
    }

    @Test
    public void testGetPartitionNames()
            throws Exception
    {
        TableHandle tableHandle = getTableHandle(table);
        PartitionResult partitionResult = splitManager.getPartitions(tableHandle, TupleDomain.all());
        assertExpectedPartitions(partitionResult.getPartitions());
    }

    protected void assertExpectedPartitions(List<Partition> actualPartitions)
    {
        Map<String, Partition> actualById = uniqueIndex(actualPartitions, partitionIdGetter());
        for (Partition expected : partitions) {
            assertInstanceOf(expected, HivePartition.class);
            HivePartition expectedPartition = (HivePartition) expected;

            Partition actual = actualById.get(expectedPartition.getPartitionId());
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
        TableHandle tableHandle = getTableHandle(tableUnpartitioned);
        PartitionResult partitionResult = splitManager.getPartitions(tableHandle, TupleDomain.all());
        assertEquals(partitionResult.getPartitions().size(), 1);
        assertEquals(partitionResult.getPartitions(), unpartitionedPartitions);
    }

    @Test(expectedExceptions = TableNotFoundException.class)
    public void testGetPartitionNamesException()
            throws Exception
    {
        splitManager.getPartitions(invalidTableHandle, TupleDomain.all());
    }

    @SuppressWarnings({"ValueOfIncrementOrDecrementUsed", "UnusedAssignment"})
    @Test
    public void testGetTableSchema()
            throws Exception
    {
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(getTableHandle(table));
        Map<String, ColumnMetadata> map = uniqueIndex(tableMetadata.getColumns(), columnNameGetter());

        int i = 0;
        assertPrimitiveField(map, i++, "t_string", ColumnType.STRING, false);
        assertPrimitiveField(map, i++, "t_tinyint", ColumnType.LONG, false);
        assertPrimitiveField(map, i++, "t_smallint", ColumnType.LONG, false);
        assertPrimitiveField(map, i++, "t_int", ColumnType.LONG, false);
        assertPrimitiveField(map, i++, "t_bigint", ColumnType.LONG, false);
        assertPrimitiveField(map, i++, "t_float", ColumnType.DOUBLE, false);
        assertPrimitiveField(map, i++, "t_double", ColumnType.DOUBLE, false);
        assertPrimitiveField(map, i++, "t_map", ColumnType.STRING, false); // Currently mapped as a string
        assertPrimitiveField(map, i++, "t_boolean", ColumnType.BOOLEAN, false);
        assertPrimitiveField(map, i++, "t_timestamp", ColumnType.LONG, false);
        assertPrimitiveField(map, i++, "t_binary", ColumnType.STRING, false);
        assertPrimitiveField(map, i++, "t_array_string", ColumnType.STRING, false); // Currently mapped as a string
        assertPrimitiveField(map, i++, "t_complex", ColumnType.STRING, false); // Currently mapped as a string
        assertPrimitiveField(map, i++, "ds", ColumnType.STRING, true);
        assertPrimitiveField(map, i++, "file_format", ColumnType.STRING, true);
        assertPrimitiveField(map, i++, "dummy", ColumnType.LONG, true);
    }

    @Test
    public void testGetTableSchemaUnpartitioned()
            throws Exception
    {
        TableHandle tableHandle = getTableHandle(tableUnpartitioned);
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(tableHandle);
        Map<String, ColumnMetadata> map = uniqueIndex(tableMetadata.getColumns(), columnNameGetter());

        assertPrimitiveField(map, 0, "t_string", ColumnType.STRING, false);
        assertPrimitiveField(map, 1, "t_tinyint", ColumnType.LONG, false);
    }

    @Test
    public void testGetTableSchemaOffline()
            throws Exception
    {
        TableHandle tableHandle = getTableHandle(tableOffline);
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(tableHandle);
        Map<String, ColumnMetadata> map = uniqueIndex(tableMetadata.getColumns(), columnNameGetter());

        assertPrimitiveField(map, 0, "t_string", ColumnType.STRING, false);
    }

    @Test
    public void testGetTableSchemaOfflinePartition()
            throws Exception
    {
        TableHandle tableHandle = getTableHandle(tableOfflinePartition);
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(tableHandle);
        Map<String, ColumnMetadata> map = uniqueIndex(tableMetadata.getColumns(), columnNameGetter());

        assertPrimitiveField(map, 0, "t_string", ColumnType.STRING, false);
    }

    @Test
    public void testGetTableSchemaException()
            throws Exception
    {
        assertNull(metadata.getTableHandle(invalidTable));
    }

    @Test
    public void testGetPartitionSplitsBatch()
            throws Exception
    {
        TableHandle tableHandle = getTableHandle(table);
        PartitionResult partitionResult = splitManager.getPartitions(tableHandle, TupleDomain.all());
        Iterable<Split> iterator = splitManager.getPartitionSplits(tableHandle, partitionResult.getPartitions());

        List<Split> splits = ImmutableList.copyOf(iterator);
        assertEquals(splits.size(), partitions.size());
    }

    @Test
    public void testGetPartitionSplitsBatchUnpartitioned()
            throws Exception
    {
        TableHandle tableHandle = getTableHandle(tableUnpartitioned);
        PartitionResult partitionResult = splitManager.getPartitions(tableHandle, TupleDomain.all());
        Iterable<Split> iterator = splitManager.getPartitionSplits(tableHandle, partitionResult.getPartitions());

        List<Split> splits = ImmutableList.copyOf(iterator);
        assertEquals(splits.size(), 1);
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
        Iterable<Split> iterator = splitManager.getPartitionSplits(invalidTableHandle, ImmutableList.<Partition>of());
        // fetch full list
        ImmutableList.copyOf(iterator);
    }

    @Test
    public void testGetPartitionTableOffline()
            throws Exception
    {
        TableHandle tableHandle = getTableHandle(tableOffline);
        try {
            splitManager.getPartitions(tableHandle, TupleDomain.all());
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
        TableHandle tableHandle = getTableHandle(tableOfflinePartition);
        assertNotNull(tableHandle);

        ColumnHandle dsColumn = metadata.getColumnHandle(tableHandle, "ds");
        assertNotNull(dsColumn);

        TupleDomain tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(dsColumn, Domain.singleValue("2012-12-30")));
        PartitionResult partitionResult = splitManager.getPartitions(tableHandle, tupleDomain);
        for (Partition partition : partitionResult.getPartitions()) {
            if (Domain.singleValue("2012-12-30").equals(partition.getTupleDomain().getDomains().get(dsColumn))) {
                try {
                    Iterables.size(splitManager.getPartitionSplits(tableHandle, ImmutableList.of(partition)));
                    fail("Expected PartitionOfflineException");
                }
                catch (PartitionOfflineException e) {
                    assertEquals(e.getTableName(), tableOfflinePartition);
                    assertEquals(e.getPartition(), "ds=2012-12-30");
                }
            }
            else {
                Iterables.size(splitManager.getPartitionSplits(tableHandle, ImmutableList.of(partition)));
            }
        }
    }

    @Test
    public void testBucketedTableStringInt()
            throws Exception
    {
        TableHandle tableHandle = getTableHandle(tableBucketedStringInt);
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(tableHandle).values());
        Map<String, Integer> columnIndex = indexColumns(columnHandles);

        String testString = "sequencefile test";
        Long testInt = 413L;
        Long testSmallint = 412L;

        // Reverse the order of bindings as compared to bucketing order
        ImmutableMap<ColumnHandle, Comparable<?>> bindings = ImmutableMap.<ColumnHandle, Comparable<?>>builder()
                .put(columnHandles.get(columnIndex.get("t_int")), testInt)
                .put(columnHandles.get(columnIndex.get("t_string")), testString)
                .put(columnHandles.get(columnIndex.get("t_smallint")), testSmallint)
                .build();

        PartitionResult partitionResult = splitManager.getPartitions(tableHandle, TupleDomain.withFixedValues(bindings));
        List<Split> splits = ImmutableList.copyOf(splitManager.getPartitionSplits(tableHandle, partitionResult.getPartitions()));
        assertEquals(splits.size(), 1);

        boolean rowFound = false;
        try (RecordCursor cursor = recordSetProvider.getRecordSet(splits.get(0), columnHandles).cursor()) {
            while (cursor.advanceNextPosition()) {
                if (testString.equals(new String(cursor.getString(columnIndex.get("t_string")))) &&
                        testInt == cursor.getLong(columnIndex.get("t_int")) &&
                        testSmallint == cursor.getLong(columnIndex.get("t_smallint"))) {
                    rowFound = true;
                }
            }
            assertTrue(rowFound);
        }
    }

    @Test
    public void testBucketedTableBigintBoolean()
            throws Exception
    {
        TableHandle tableHandle = getTableHandle(tableBucketedBigintBoolean);
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(tableHandle).values());
        Map<String, Integer> columnIndex = indexColumns(columnHandles);

        String testString = "textfile test";
        // This needs to match one of the rows where t_string is not empty or null, and where t_bigint is not null
        // (i.e. (testBigint - 604) % 19 > 1 and (testBigint - 604) % 13 != 0)
        Long testBigint = 608L;
        Boolean testBoolean = true;

        ImmutableMap<ColumnHandle, Comparable<?>> bindings = ImmutableMap.<ColumnHandle, Comparable<?>>builder()
                .put(columnHandles.get(columnIndex.get("t_string")), testString)
                .put(columnHandles.get(columnIndex.get("t_bigint")), testBigint)
                .put(columnHandles.get(columnIndex.get("t_boolean")), testBoolean)
                .build();

        PartitionResult partitionResult = splitManager.getPartitions(tableHandle, TupleDomain.withFixedValues(bindings));
        List<Split> splits = ImmutableList.copyOf(splitManager.getPartitionSplits(tableHandle, partitionResult.getPartitions()));
        assertEquals(splits.size(), 1);

        boolean rowFound = false;
        try (RecordCursor cursor = recordSetProvider.getRecordSet(splits.get(0), columnHandles).cursor()) {
            while (cursor.advanceNextPosition()) {
                if (testString.equals(new String(cursor.getString(columnIndex.get("t_string")))) &&
                        testBigint == cursor.getLong(columnIndex.get("t_bigint")) &&
                        testBoolean == cursor.getBoolean(columnIndex.get("t_boolean"))) {
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
        TableHandle tableHandle = getTableHandle(tableBucketedDoubleFloat);
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(tableHandle).values());
        Map<String, Integer> columnIndex = indexColumns(columnHandles);

        ImmutableMap<ColumnHandle, Comparable<?>> bindings = ImmutableMap.<ColumnHandle, Comparable<?>>builder()
                .put(columnHandles.get(columnIndex.get("t_float")), 406.1000061035156)
                .put(columnHandles.get(columnIndex.get("t_double")), 407.2)
                .build();

        PartitionResult partitionResult = splitManager.getPartitions(tableHandle, TupleDomain.withFixedValues(bindings));
        List<Split> splits = ImmutableList.copyOf(splitManager.getPartitionSplits(tableHandle, partitionResult.getPartitions()));
        assertEquals(splits.size(), 32);

        int count = 0;
        for (Split split : splits) {
            try (RecordCursor cursor = recordSetProvider.getRecordSet(split, columnHandles).cursor()) {
                while (cursor.advanceNextPosition()) {
                    count++;
                }
            }
        }
        assertEquals(count, 300);
    }

    @Test
    public void testGetRecords()
            throws Exception
    {
        TableHandle tableHandle = getTableHandle(table);
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(tableHandle);
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(tableHandle).values());
        Map<String, Integer> columnIndex = indexColumns(columnHandles);

        PartitionResult partitionResult = splitManager.getPartitions(tableHandle, TupleDomain.all());
        List<Split> splits = ImmutableList.copyOf(splitManager.getPartitionSplits(tableHandle, partitionResult.getPartitions()));
        assertEquals(splits.size(), this.partitions.size());
        for (Split split : splits) {
            HiveSplit hiveSplit = (HiveSplit) split;

            List<HivePartitionKey> partitionKeys = hiveSplit.getPartitionKeys();
            String ds = partitionKeys.get(0).getValue();
            String fileType = partitionKeys.get(1).getValue();
            long dummy = Long.parseLong(partitionKeys.get(2).getValue());

            long baseValue = getBaseValueForFileType(fileType);
            assertEquals(dummy * 100, baseValue);

            long rowNumber = 0;
            long completedBytes = 0;
            try (RecordCursor cursor = recordSetProvider.getRecordSet(hiveSplit, columnHandles).cursor()) {
                assertRecordCursorType(cursor, fileType);
                assertEquals(cursor.getTotalBytes(), hiveSplit.getLength());

                while (cursor.advanceNextPosition()) {
                    try {
                        assertReadFields(cursor, tableMetadata.getColumns());
                    }
                    catch (RuntimeException e) {
                        throw new RuntimeException("row " + rowNumber, e);
                    }

                    rowNumber++;

                    if (rowNumber % 19 == 0) {
                        assertTrue(cursor.isNull(columnIndex.get("t_string")));
                    }
                    else if (rowNumber % 19 == 1) {
                        assertEquals(cursor.getString(columnIndex.get("t_string")), EMPTY_STRING);
                    }
                    else {
                        assertEquals(cursor.getString(columnIndex.get("t_string")), (fileType + " test").getBytes(Charsets.UTF_8));
                    }

                    assertEquals(cursor.getLong(columnIndex.get("t_tinyint")), (long) ((byte) (baseValue + 1 + rowNumber)));
                    assertEquals(cursor.getLong(columnIndex.get("t_smallint")), baseValue + 2 + rowNumber);
                    assertEquals(cursor.getLong(columnIndex.get("t_int")), baseValue + 3 + rowNumber);

                    if (rowNumber % 13 == 0) {
                        assertTrue(cursor.isNull(columnIndex.get("t_bigint")));
                    }
                    else {
                        assertEquals(cursor.getLong(columnIndex.get("t_bigint")), baseValue + 4 + rowNumber);
                    }

                    assertEquals(cursor.getDouble(columnIndex.get("t_float")), baseValue + 5.1 + rowNumber, 0.001);
                    assertEquals(cursor.getDouble(columnIndex.get("t_double")), baseValue + 6.2 + rowNumber);

                    if (rowNumber % 3 == 2) {
                        assertTrue(cursor.isNull(columnIndex.get("t_boolean")));
                    }
                    else {
                        assertEquals(cursor.getBoolean(columnIndex.get("t_boolean")), rowNumber % 3 != 0);
                    }

                    if (rowNumber % 17 == 0) {
                        assertTrue(cursor.isNull(columnIndex.get("t_timestamp")));
                    }
                    else {
                        long seconds = MILLISECONDS.toSeconds(new DateTime(2011, 5, 6, 7, 8, 9, 123).getMillis());
                        assertEquals(cursor.getLong(columnIndex.get("t_timestamp")), seconds);
                    }

                    if (rowNumber % 23 == 0) {
                        assertTrue(cursor.isNull(columnIndex.get("t_binary")));
                    }
                    else {
                        assertEquals(new String(cursor.getString(columnIndex.get("t_binary"))), (fileType + " test"));
                    }

                    if (rowNumber % 29 == 0) {
                        assertTrue(cursor.isNull(columnIndex.get("t_map")));
                    }
                    else {
                        String expectedJson = "{\"format\":\"" + fileType + "\"}";
                        assertEquals(cursor.getString(columnIndex.get("t_map")), expectedJson.getBytes(Charsets.UTF_8));
                    }

                    if (rowNumber % 27 == 0) {
                        assertTrue(cursor.isNull(columnIndex.get("t_array_string")));
                    }
                    else {
                        String expectedJson = "[\"" + fileType + "\",\"test\",\"data\"]";
                        assertEquals(cursor.getString(columnIndex.get("t_array_string")), expectedJson.getBytes(Charsets.UTF_8));
                    }

                    if (rowNumber % 31 == 0) {
                        assertTrue(cursor.isNull(columnIndex.get("t_complex")));
                    }
                    else {
                        String expectedJson = "{1:[{\"s_string\":\"" + fileType + "-a\",\"s_double\":0.1},{\"s_string\":\"" + fileType + "-b\",\"s_double\":0.2}]}";
                        assertEquals(cursor.getString(columnIndex.get("t_complex")), expectedJson.getBytes(Charsets.UTF_8));
                    }

                    assertEquals(cursor.getString(columnIndex.get("ds")), ds.getBytes(Charsets.UTF_8));
                    assertEquals(cursor.getString(columnIndex.get("file_format")), fileType.getBytes(Charsets.UTF_8));
                    assertEquals(cursor.getLong(columnIndex.get("dummy")), dummy);

                    long newCompletedBytes = cursor.getCompletedBytes();
                    assertTrue(newCompletedBytes >= completedBytes);
                    assertTrue(newCompletedBytes <= hiveSplit.getLength());
                    completedBytes = newCompletedBytes;
                }
            }
            assertTrue(completedBytes <= hiveSplit.getLength());
            assertEquals(rowNumber, 100);
        }
    }

    @Test
    public void testGetPartialRecords()
            throws Exception
    {
        TableHandle tableHandle = getTableHandle(table);
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(tableHandle).values());
        Map<String, Integer> columnIndex = indexColumns(columnHandles);

        PartitionResult partitionResult = splitManager.getPartitions(tableHandle, TupleDomain.all());
        List<Split> splits = ImmutableList.copyOf(splitManager.getPartitionSplits(tableHandle, partitionResult.getPartitions()));
        assertEquals(splits.size(), this.partitions.size());
        for (Split split : splits) {
            HiveSplit hiveSplit = (HiveSplit) split;

            List<HivePartitionKey> partitionKeys = hiveSplit.getPartitionKeys();
            String ds = partitionKeys.get(0).getValue();
            String fileType = partitionKeys.get(1).getValue();
            long dummy = Long.parseLong(partitionKeys.get(2).getValue());

            long baseValue = getBaseValueForFileType(fileType);

            long rowNumber = 0;
            try (RecordCursor cursor = recordSetProvider.getRecordSet(hiveSplit, columnHandles).cursor()) {
                assertRecordCursorType(cursor, fileType);
                while (cursor.advanceNextPosition()) {
                    rowNumber++;

                    assertEquals(cursor.getDouble(columnIndex.get("t_double")), baseValue + 6.2 + rowNumber);
                    assertEquals(cursor.getString(columnIndex.get("ds")), ds.getBytes(Charsets.UTF_8));
                    assertEquals(cursor.getString(columnIndex.get("file_format")), fileType.getBytes(Charsets.UTF_8));
                    assertEquals(cursor.getLong(columnIndex.get("dummy")), dummy);
                }
            }
            assertEquals(rowNumber, 100);
        }
    }

    @Test
    public void testGetRecordsUnpartitioned()
            throws Exception
    {
        TableHandle tableHandle = getTableHandle(tableUnpartitioned);
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(tableHandle).values());
        Map<String, Integer> columnIndex = indexColumns(columnHandles);

        PartitionResult partitionResult = splitManager.getPartitions(tableHandle, TupleDomain.all());
        List<Split> splits = ImmutableList.copyOf(splitManager.getPartitionSplits(tableHandle, partitionResult.getPartitions()));
        assertEquals(splits.size(), 1);

        for (Split split : splits) {
            HiveSplit hiveSplit = (HiveSplit) split;

            assertEquals(hiveSplit.getPartitionKeys(), ImmutableList.of());

            long rowNumber = 0;
            try (RecordCursor cursor = recordSetProvider.getRecordSet(split, columnHandles).cursor()) {
                assertRecordCursorType(cursor, "textfile");
                assertEquals(cursor.getTotalBytes(), hiveSplit.getLength());

                while (cursor.advanceNextPosition()) {
                    rowNumber++;

                    if (rowNumber % 19 == 0) {
                        assertTrue(cursor.isNull(columnIndex.get("t_string")));
                    }
                    else if (rowNumber % 19 == 1) {
                        assertEquals(cursor.getString(columnIndex.get("t_string")), EMPTY_STRING);
                    }
                    else {
                        assertEquals(cursor.getString(columnIndex.get("t_string")), "unpartitioned".getBytes(Charsets.UTF_8));
                    }

                    assertEquals(cursor.getLong(columnIndex.get("t_tinyint")), 1 + rowNumber);
                }
            }
            assertEquals(rowNumber, 100);
        }
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*" + INVALID_COLUMN + ".*")
    public void testGetRecordsInvalidColumn()
            throws Exception
    {
        TableHandle table = getTableHandle(tableUnpartitioned);
        PartitionResult partitionResult = splitManager.getPartitions(table, TupleDomain.all());
        Split split = Iterables.getFirst(splitManager.getPartitionSplits(table, partitionResult.getPartitions()), null);
        RecordSet recordSet = recordSetProvider.getRecordSet(split, ImmutableList.of(invalidColumnHandle));
        recordSet.cursor();
    }

    @Test
    public void testViewsAreNotSupported()
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

    private TableHandle getTableHandle(SchemaTableName tableName)
    {
        TableHandle handle = metadata.getTableHandle(tableName);
        checkArgument(handle != null, "table not found: %s", tableName);
        return handle;
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

    private static void assertReadFields(RecordCursor cursor, List<ColumnMetadata> schema)
    {
        for (int columnIndex = 0; columnIndex < schema.size(); columnIndex++) {
            ColumnMetadata column = schema.get(columnIndex);
            if (!cursor.isNull(columnIndex)) {
                switch (column.getType()) {
                    case BOOLEAN:
                        cursor.getBoolean(columnIndex);
                        break;
                    case LONG:
                        cursor.getLong(columnIndex);
                        break;
                    case DOUBLE:
                        cursor.getDouble(columnIndex);
                        break;
                    case STRING:
                        try {
                            cursor.getString(columnIndex);
                        }
                        catch (RuntimeException e) {
                            throw new RuntimeException("column " + column, e);
                        }
                        break;
                    default:
                        fail("Unknown primitive type " + columnIndex);
                }
            }
        }
    }

    private static void assertPrimitiveField(Map<String, ColumnMetadata> map, int position, String name, ColumnType type, boolean partitionKey)
    {
        assertTrue(map.containsKey(name));
        ColumnMetadata column = map.get(name);
        assertEquals(column.getOrdinalPosition(), position);
        assertEquals(column.getType(), type, name);
        assertEquals(column.isPartitionKey(), partitionKey, name);
    }

    private static ImmutableMap<String, Integer> indexColumns(List<ColumnHandle> columnHandles)
    {
        ImmutableMap.Builder<String, Integer> index = ImmutableMap.builder();
        int i = 0;
        for (ColumnHandle columnHandle : columnHandles) {
            checkArgument(columnHandle instanceof HiveColumnHandle, "columnHandle is not an instance of HiveColumnHandle");
            HiveColumnHandle hiveColumnHandle = (HiveColumnHandle) columnHandle;
            index.put(hiveColumnHandle.getName(), i);
            i++;
        }
        return index.build();
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

    private static ThreadFactory daemonThreadsNamed(String nameFormat)
    {
        return new ThreadFactoryBuilder().setNameFormat(nameFormat).setDaemon(true).build();
    }
}

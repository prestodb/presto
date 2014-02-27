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

import com.facebook.presto.hive.util.HadoopApiStats;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorRecordSinkProvider;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Domain;
import com.facebook.presto.spi.OutputTableHandle;
import com.facebook.presto.spi.Partition;
import com.facebook.presto.spi.PartitionResult;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.RecordSink;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.Split;
import com.facebook.presto.spi.SplitSource;
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
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.hive.HiveBucketing.HiveBucket;
import static com.facebook.presto.hive.HiveUtil.partitionIdGetter;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Maps.uniqueIndex;
import static com.google.common.util.concurrent.MoreExecutors.sameThreadExecutor;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(groups = "hive")
public abstract class AbstractTestHiveClient
{
    protected static final String INVALID_DATABASE = "totally_invalid_database";
    protected static final String INVALID_COLUMN = "totally_invalid_column_name";
    protected static final byte[] EMPTY_STRING = new byte[0];

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
    protected String tableOwner;

    protected TableHandle invalidTableHandle;

    protected ColumnHandle dsColumn;
    protected ColumnHandle fileFormatColumn;
    protected ColumnHandle dummyColumn;
    protected ColumnHandle intColumn;
    protected ColumnHandle invalidColumnHandle;

    protected Set<Partition> partitions;
    protected Set<Partition> unpartitionedPartitions;
    protected Partition invalidPartition;

    protected CachingHiveMetastore metastoreClient;

    protected ConnectorMetadata metadata;
    protected ConnectorSplitManager splitManager;
    protected ConnectorRecordSetProvider recordSetProvider;
    protected ConnectorRecordSinkProvider recordSinkProvider;

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

        String random = UUID.randomUUID().toString().toLowerCase().replace("-", "");
        temporaryCreateTable = new SchemaTableName(database, "tmp_presto_test_create_" + random);
        random = UUID.randomUUID().toString().toLowerCase().replace("-", "");
        temporaryCreateSampledTable = new SchemaTableName(database, "tmp_presto_test_create_" + random);
        tableOwner = "presto_test";

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

        HiveCluster hiveCluster = new TestingHiveCluster(hiveClientConfig, host, port);
        ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("hive-%s"));

        metastoreClient = new CachingHiveMetastore(hiveCluster, executor, Duration.valueOf("1m"), Duration.valueOf("15s"));

        HiveClient client = new HiveClient(
                new HiveConnectorId(connectorName),
                metastoreClient,
                new HadoopApiStats(),
                new HdfsEnvironment(new HdfsConfiguration(hiveClientConfig)),
                sameThreadExecutor(),
                hiveClientConfig.getMaxSplitSize(),
                maxOutstandingSplits,
                maxThreads,
                hiveClientConfig.getMinPartitionBatchSize(),
                hiveClientConfig.getMaxPartitionBatchSize());

        metadata = client;
        splitManager = client;
        recordSetProvider = client;
        recordSinkProvider = client;
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
        SplitSource splitSource = splitManager.getPartitionSplits(tableHandle, partitionResult.getPartitions());

        assertEquals(getSplitCount(splitSource), partitions.size());
    }

    @Test
    public void testGetPartitionSplitsBatchUnpartitioned()
            throws Exception
    {
        TableHandle tableHandle = getTableHandle(tableUnpartitioned);
        PartitionResult partitionResult = splitManager.getPartitions(tableHandle, TupleDomain.all());
        SplitSource splitSource = splitManager.getPartitionSplits(tableHandle, partitionResult.getPartitions());

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
        SplitSource splitSource = splitManager.getPartitionSplits(invalidTableHandle, ImmutableList.<Partition>of());
        // fetch full list
        getSplitCount(splitSource);
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
        TableHandle tableHandle = getTableHandle(tableBucketedStringInt);
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(tableHandle).values());
        Map<String, Integer> columnIndex = indexColumns(columnHandles);

        assertTableIsBucketed(tableHandle);

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
        List<Split> splits = getAllSplits(splitManager.getPartitionSplits(tableHandle, partitionResult.getPartitions()));
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

        assertTableIsBucketed(tableHandle);

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
        List<Split> splits = getAllSplits(splitManager.getPartitionSplits(tableHandle, partitionResult.getPartitions()));
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

        assertTableIsBucketed(tableHandle);

        ImmutableMap<ColumnHandle, Comparable<?>> bindings = ImmutableMap.<ColumnHandle, Comparable<?>>builder()
                .put(columnHandles.get(columnIndex.get("t_float")), 406.1000061035156)
                .put(columnHandles.get(columnIndex.get("t_double")), 407.2)
                .build();

        // floats and doubles are not supported, so we should see all splits
        PartitionResult partitionResult = splitManager.getPartitions(tableHandle, TupleDomain.withFixedValues(bindings));
        List<Split> splits = getAllSplits(splitManager.getPartitionSplits(tableHandle, partitionResult.getPartitions()));
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

    private void assertTableIsBucketed(TableHandle tableHandle)
            throws Exception
    {
        // the bucketed test tables should have exactly 32 splits
        PartitionResult partitionResult = splitManager.getPartitions(tableHandle, TupleDomain.all());
        List<Split> splits = getAllSplits(splitManager.getPartitionSplits(tableHandle, partitionResult.getPartitions()));
        assertEquals(splits.size(), 32);

        // verify all paths are unique
        Set<String> paths = new HashSet<>();
        for (Split split : splits) {
            assertTrue(paths.add(((HiveSplit) split).getPath()));
        }
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
        List<Split> splits = getAllSplits(splitManager.getPartitionSplits(tableHandle, partitionResult.getPartitions()));
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
                        String expectedJson = "{\"1\":[{\"s_string\":\"" + fileType + "-a\",\"s_double\":0.1},{\"s_string\":\"" + fileType + "-b\",\"s_double\":0.2}]}";
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
        List<Split> splits = getAllSplits(splitManager.getPartitionSplits(tableHandle, partitionResult.getPartitions()));
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
        List<Split> splits = getAllSplits(splitManager.getPartitionSplits(tableHandle, partitionResult.getPartitions()));
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
        Split split = Iterables.getFirst(getAllSplits(splitManager.getPartitionSplits(table, partitionResult.getPartitions())), null);
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

    private void doCreateSampledTable()
            throws InterruptedException
    {
        // begin creating the table
        List<ColumnMetadata> columns = ImmutableList.<ColumnMetadata>builder()
                .add(new ColumnMetadata("sales", ColumnType.LONG, 1, false))
                .build();

        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(temporaryCreateSampledTable, columns, tableOwner, true);
        OutputTableHandle outputHandle = metadata.beginCreateTable(tableMetadata);

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
        TableHandle tableHandle = getTableHandle(temporaryCreateSampledTable);
        List<ColumnHandle> columnHandles = ImmutableList.<ColumnHandle>builder()
                .addAll(metadata.getColumnHandles(tableHandle).values())
                .add(metadata.getSampleWeightColumnHandle(tableHandle))
                .build();
        assertEquals(columnHandles.size(), 2);

        // verify the metadata
        tableMetadata = metadata.getTableMetadata(getTableHandle(temporaryCreateSampledTable));
        assertEquals(tableMetadata.getOwner(), tableOwner);

        Map<String, ColumnMetadata> columnMap = uniqueIndex(tableMetadata.getColumns(), columnNameGetter());
        assertEquals(columnMap.size(), 1);

        assertPrimitiveField(columnMap, 0, "sales", ColumnType.LONG, false);

        // verify the data
        PartitionResult partitionResult = splitManager.getPartitions(tableHandle, TupleDomain.all());
        assertEquals(partitionResult.getPartitions().size(), 1);
        SplitSource splitSource = splitManager.getPartitionSplits(tableHandle, partitionResult.getPartitions());
        Split split = getOnlyElement(splitSource.getNextBatch(1000));
        assertTrue(splitSource.isFinished());

        try (RecordCursor cursor = recordSetProvider.getRecordSet(split, columnHandles).cursor()) {
            assertRecordCursorType(cursor, "rcfile-binary");

            assertTrue(cursor.advanceNextPosition());
            assertEquals(cursor.getLong(0), 2);
            assertEquals(cursor.getLong(1), 8);

            assertTrue(cursor.advanceNextPosition());
            assertEquals(cursor.getLong(0), 3);
            assertEquals(cursor.getLong(1), 5);

            assertTrue(cursor.advanceNextPosition());
            assertEquals(cursor.getLong(0), 4);
            assertEquals(cursor.getLong(1), 7);

            assertFalse(cursor.advanceNextPosition());
        }
    }

    private void doCreateTable()
            throws InterruptedException
    {
        // begin creating the table
        List<ColumnMetadata> columns = ImmutableList.<ColumnMetadata>builder()
                .add(new ColumnMetadata("id", ColumnType.LONG, 1, false))
                .add(new ColumnMetadata("t_string", ColumnType.STRING, 2, false))
                .add(new ColumnMetadata("t_bigint", ColumnType.LONG, 3, false))
                .add(new ColumnMetadata("t_double", ColumnType.DOUBLE, 4, false))
                .add(new ColumnMetadata("t_boolean", ColumnType.BOOLEAN, 5, false))
                .build();

        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(temporaryCreateTable, columns, tableOwner);
        OutputTableHandle outputHandle = metadata.beginCreateTable(tableMetadata);

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
        TableHandle tableHandle = getTableHandle(temporaryCreateTable);
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(tableHandle).values());

        // verify the metadata
        tableMetadata = metadata.getTableMetadata(getTableHandle(temporaryCreateTable));
        assertEquals(tableMetadata.getOwner(), tableOwner);

        Map<String, ColumnMetadata> columnMap = uniqueIndex(tableMetadata.getColumns(), columnNameGetter());

        assertPrimitiveField(columnMap, 0, "id", ColumnType.LONG, false);
        assertPrimitiveField(columnMap, 1, "t_string", ColumnType.STRING, false);
        assertPrimitiveField(columnMap, 2, "t_bigint", ColumnType.LONG, false);
        assertPrimitiveField(columnMap, 3, "t_double", ColumnType.DOUBLE, false);
        assertPrimitiveField(columnMap, 4, "t_boolean", ColumnType.BOOLEAN, false);

        // verify the data
        PartitionResult partitionResult = splitManager.getPartitions(tableHandle, TupleDomain.all());
        assertEquals(partitionResult.getPartitions().size(), 1);
        SplitSource splitSource = splitManager.getPartitionSplits(tableHandle, partitionResult.getPartitions());
        Split split = getOnlyElement(splitSource.getNextBatch(1000));
        assertTrue(splitSource.isFinished());

        try (RecordCursor cursor = recordSetProvider.getRecordSet(split, columnHandles).cursor()) {
            assertRecordCursorType(cursor, "rcfile-binary");

            assertTrue(cursor.advanceNextPosition());
            assertEquals(cursor.getLong(0), 1);
            assertEquals(cursor.getString(1), "hello".getBytes(UTF_8));
            assertEquals(cursor.getLong(2), 123);
            assertEquals(cursor.getDouble(3), 43.5);
            assertEquals(cursor.getBoolean(4), true);

            assertTrue(cursor.advanceNextPosition());
            assertEquals(cursor.getLong(0), 2);
            assertTrue(cursor.isNull(1));
            assertTrue(cursor.isNull(2));
            assertTrue(cursor.isNull(3));
            assertTrue(cursor.isNull(4));

            assertTrue(cursor.advanceNextPosition());
            assertEquals(cursor.getLong(0), 3);
            assertEquals(cursor.getString(1), "bye".getBytes(UTF_8));
            assertEquals(cursor.getLong(2), 456);
            assertEquals(cursor.getDouble(3), 98.1);
            assertEquals(cursor.getBoolean(4), false);

            assertFalse(cursor.advanceNextPosition());
        }
    }

    private void dropTable(SchemaTableName table)
    {
        try {
            metastoreClient.dropTable(table.getSchemaName(), table.getTableName());
        }
        catch (RuntimeException e) {
            Logger.get(getClass()).warn(e, "Failed to drop table: %s", table);
        }
    }

    private TableHandle getTableHandle(SchemaTableName tableName)
    {
        TableHandle handle = metadata.getTableHandle(tableName);
        checkArgument(handle != null, "table not found: %s", tableName);
        return handle;
    }

    private static int getSplitCount(SplitSource splitSource)
            throws InterruptedException
    {
        int splitCount = 0;
        while (!splitSource.isFinished()) {
            List<Split> batch = splitSource.getNextBatch(1000);
            splitCount += batch.size();
        }
        return splitCount;
    }

    private static List<Split> getAllSplits(SplitSource splitSource)
            throws InterruptedException
    {
        ImmutableList.Builder<Split> splits = ImmutableList.builder();
        while (!splitSource.isFinished()) {
            List<Split> batch = splitSource.getNextBatch(1000);
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
}

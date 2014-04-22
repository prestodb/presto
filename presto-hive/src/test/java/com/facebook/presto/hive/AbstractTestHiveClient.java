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

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorRecordSinkProvider;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Domain;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.RecordSink;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.Session;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.type.Type;
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
import org.joda.time.DateTimeZone;
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
import static com.facebook.presto.hive.HiveUtil.partitionIdGetter;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
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
    private static final Session SESSION = new Session("user", "test", "default", "default", UTC_KEY, Locale.ENGLISH, null, null);

    protected static final String INVALID_DATABASE = "totally_invalid_database";
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

    protected CachingHiveMetastore metastoreClient;

    protected ConnectorMetadata metadata;
    protected ConnectorSplitManager splitManager;
    protected ConnectorRecordSetProvider recordSetProvider;
    protected ConnectorRecordSinkProvider recordSinkProvider;

    protected void setupHive(String connectorId, String databaseName, String timeZoneId)
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

        invalidTableHandle = new HiveTableHandle("hive", database, "totally_invalid_table_name", SESSION);

        dsColumn = new HiveColumnHandle(connectorId, "ds", 0, HiveType.STRING, -1, true);
        fileFormatColumn = new HiveColumnHandle(connectorId, "file_format", 1, HiveType.STRING, -1, true);
        dummyColumn = new HiveColumnHandle(connectorId, "dummy", 2, HiveType.INT, -1, true);
        intColumn = new HiveColumnHandle(connectorId, "t_int", 0, HiveType.INT, -1, true);
        invalidColumnHandle = new HiveColumnHandle(connectorId, INVALID_COLUMN, 0, HiveType.STRING, 0, false);

        partitions = ImmutableSet.<ConnectorPartition>of(
                new HivePartition(table,
                        "ds=2012-12-29/file_format=rcfile-text/dummy=0",
                        ImmutableMap.<ConnectorColumnHandle, Comparable<?>>of(dsColumn, utf8Slice("2012-12-29"), fileFormatColumn, utf8Slice("rcfile-text"), dummyColumn, 0L),
                        Optional.<HiveBucket>absent()),
                new HivePartition(table,
                        "ds=2012-12-29/file_format=rcfile-binary/dummy=2",
                        ImmutableMap.<ConnectorColumnHandle, Comparable<?>>of(dsColumn, utf8Slice("2012-12-29"), fileFormatColumn, utf8Slice("rcfile-binary"), dummyColumn, 2L),
                        Optional.<HiveBucket>absent()),
                new HivePartition(table,
                        "ds=2012-12-29/file_format=sequencefile/dummy=4",
                        ImmutableMap.<ConnectorColumnHandle, Comparable<?>>of(dsColumn, utf8Slice("2012-12-29"), fileFormatColumn, utf8Slice("sequencefile"), dummyColumn, 4L),
                        Optional.<HiveBucket>absent()),
                new HivePartition(table,
                        "ds=2012-12-29/file_format=textfile/dummy=6",
                        ImmutableMap.<ConnectorColumnHandle, Comparable<?>>of(dsColumn, utf8Slice("2012-12-29"), fileFormatColumn, utf8Slice("textfile"), dummyColumn, 6L),
                        Optional.<HiveBucket>absent()));
        unpartitionedPartitions = ImmutableSet.<ConnectorPartition>of(new HivePartition(tableUnpartitioned));
        invalidPartition = new HivePartition(invalidTable, "unknown", ImmutableMap.<ConnectorColumnHandle, Comparable<?>>of(), Optional.<HiveBucket>absent());
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
        ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("hive-%s"));

        metastoreClient = new CachingHiveMetastore(hiveCluster, executor, Duration.valueOf("1m"), Duration.valueOf("15s"));

        HiveClient client = new HiveClient(
                new HiveConnectorId(connectorName),
                metastoreClient,
                new NamenodeStats(),
                new HdfsEnvironment(new HdfsConfiguration(hiveClientConfig)),
                new HadoopDirectoryLister(),
                timeZone,
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

    // disabled until metadata manager is updated to handle invalid catalogs and schemas
    @Test(enabled = false, expectedExceptions = SchemaNotFoundException.class)
    public void testGetTableNamesException()
            throws Exception
    {
        metadata.listTables(SESSION, INVALID_DATABASE);
    }

    @Test
    public void testListUnknownSchema()
    {
        assertNull(metadata.getTableHandle(SESSION, new SchemaTableName("totally_invalid_database_name", "dual")));
        assertEquals(metadata.listTables(SESSION, "totally_invalid_database_name"), ImmutableList.of());
        assertEquals(metadata.listTableColumns(SESSION, new SchemaTablePrefix("totally_invalid_database_name", "dual")), ImmutableMap.of());
    }

    @Test
    public void testGetPartitions()
            throws Exception
    {
        ConnectorTableHandle tableHandle = getTableHandle(table);
        ConnectorPartitionResult partitionResult = splitManager.getPartitions(tableHandle, TupleDomain.<ConnectorColumnHandle>all());
        assertExpectedPartitions(partitionResult.getPartitions());
    }

    @Test
    public void testGetPartitionsWithBindings()
            throws Exception
    {
        ConnectorTableHandle tableHandle = getTableHandle(table);
        ConnectorPartitionResult partitionResult = splitManager.getPartitions(tableHandle, TupleDomain.withColumnDomains(ImmutableMap.<ConnectorColumnHandle, Domain>of(intColumn, Domain.singleValue(5L))));
        assertExpectedPartitions(partitionResult.getPartitions());
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
        assertExpectedPartitions(partitionResult.getPartitions());
    }

    protected void assertExpectedPartitions(List<ConnectorPartition> actualPartitions)
    {
        Map<String, ConnectorPartition> actualById = uniqueIndex(actualPartitions, partitionIdGetter());
        for (ConnectorPartition expected : partitions) {
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
        assertPrimitiveField(map, i++, "t_binary", VARCHAR, false);
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
        ImmutableMap<ConnectorColumnHandle, Comparable<?>> bindings = ImmutableMap.<ConnectorColumnHandle, Comparable<?>>builder()
                .put(columnHandles.get(columnIndex.get("t_int")), testInt)
                .put(columnHandles.get(columnIndex.get("t_string")), testString)
                .put(columnHandles.get(columnIndex.get("t_smallint")), testSmallint)
                .build();

        ConnectorPartitionResult partitionResult = splitManager.getPartitions(tableHandle, TupleDomain.withFixedValues(bindings));
        List<ConnectorSplit> splits = getAllSplits(splitManager.getPartitionSplits(tableHandle, partitionResult.getPartitions()));
        assertEquals(splits.size(), 1);

        boolean rowFound = false;
        try (RecordCursor cursor = recordSetProvider.getRecordSet(splits.get(0), columnHandles).cursor()) {
            while (cursor.advanceNextPosition()) {
                if (testString.equals(cursor.getSlice(columnIndex.get("t_string")).toStringUtf8()) &&
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
        ConnectorTableHandle tableHandle = getTableHandle(tableBucketedBigintBoolean);
        List<ConnectorColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(tableHandle).values());
        Map<String, Integer> columnIndex = indexColumns(columnHandles);

        assertTableIsBucketed(tableHandle);

        String testString = "textfile test";
        // This needs to match one of the rows where t_string is not empty or null, and where t_bigint is not null
        // (i.e. (testBigint - 604) % 19 > 1 and (testBigint - 604) % 13 != 0)
        Long testBigint = 608L;
        Boolean testBoolean = true;

        ImmutableMap<ConnectorColumnHandle, Comparable<?>> bindings = ImmutableMap.<ConnectorColumnHandle, Comparable<?>>builder()
                .put(columnHandles.get(columnIndex.get("t_string")), testString)
                .put(columnHandles.get(columnIndex.get("t_bigint")), testBigint)
                .put(columnHandles.get(columnIndex.get("t_boolean")), testBoolean)
                .build();

        ConnectorPartitionResult partitionResult = splitManager.getPartitions(tableHandle, TupleDomain.withFixedValues(bindings));
        List<ConnectorSplit> splits = getAllSplits(splitManager.getPartitionSplits(tableHandle, partitionResult.getPartitions()));
        assertEquals(splits.size(), 1);

        boolean rowFound = false;
        try (RecordCursor cursor = recordSetProvider.getRecordSet(splits.get(0), columnHandles).cursor()) {
            while (cursor.advanceNextPosition()) {
                if (testString.equals(cursor.getSlice(columnIndex.get("t_string")).toStringUtf8()) &&
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
        ConnectorTableHandle tableHandle = getTableHandle(tableBucketedDoubleFloat);
        List<ConnectorColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(tableHandle).values());
        Map<String, Integer> columnIndex = indexColumns(columnHandles);

        assertTableIsBucketed(tableHandle);

        ImmutableMap<ConnectorColumnHandle, Comparable<?>> bindings = ImmutableMap.<ConnectorColumnHandle, Comparable<?>>builder()
                .put(columnHandles.get(columnIndex.get("t_float")), 406.1000061035156)
                .put(columnHandles.get(columnIndex.get("t_double")), 407.2)
                .build();

        // floats and doubles are not supported, so we should see all splits
        ConnectorPartitionResult partitionResult = splitManager.getPartitions(tableHandle, TupleDomain.withFixedValues(bindings));
        List<ConnectorSplit> splits = getAllSplits(splitManager.getPartitionSplits(tableHandle, partitionResult.getPartitions()));
        assertEquals(splits.size(), 32);

        int count = 0;
        for (ConnectorSplit split : splits) {
            try (RecordCursor cursor = recordSetProvider.getRecordSet(split, columnHandles).cursor()) {
                while (cursor.advanceNextPosition()) {
                    count++;
                }
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
                        assertEquals(cursor.getSlice(columnIndex.get("t_string")), "");
                    }
                    else {
                        assertEquals(cursor.getSlice(columnIndex.get("t_string")), (fileType + " test").getBytes(Charsets.UTF_8));
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
                        long millis = new DateTime(2011, 5, 6, 7, 8, 9, 123, timeZone).getMillis();
                        assertEquals(cursor.getLong(columnIndex.get("t_timestamp")), millis, (fileType + " test"));
                    }

                    if (rowNumber % 23 == 0) {
                        assertTrue(cursor.isNull(columnIndex.get("t_binary")));
                    }
                    else {
                        assertEquals(cursor.getSlice(columnIndex.get("t_binary")).toStringUtf8(), (fileType + " test"));
                    }

                    if (rowNumber % 29 == 0) {
                        assertTrue(cursor.isNull(columnIndex.get("t_map")));
                    }
                    else {
                        String expectedJson = "{\"format\":\"" + fileType + "\"}";
                        assertEquals(cursor.getSlice(columnIndex.get("t_map")), expectedJson.getBytes(Charsets.UTF_8));
                    }

                    if (rowNumber % 27 == 0) {
                        assertTrue(cursor.isNull(columnIndex.get("t_array_string")));
                    }
                    else {
                        String expectedJson = "[\"" + fileType + "\",\"test\",\"data\"]";
                        assertEquals(cursor.getSlice(columnIndex.get("t_array_string")), expectedJson.getBytes(Charsets.UTF_8));
                    }

                    if (rowNumber % 31 == 0) {
                        assertTrue(cursor.isNull(columnIndex.get("t_complex")));
                    }
                    else {
                        String expectedJson = "{\"1\":[{\"s_string\":\"" + fileType + "-a\",\"s_double\":0.1},{\"s_string\":\"" + fileType + "-b\",\"s_double\":0.2}]}";
                        assertEquals(cursor.getSlice(columnIndex.get("t_complex")), expectedJson.getBytes(Charsets.UTF_8));
                    }

                    assertEquals(cursor.getSlice(columnIndex.get("ds")), ds.getBytes(Charsets.UTF_8));
                    assertEquals(cursor.getSlice(columnIndex.get("file_format")), fileType.getBytes(Charsets.UTF_8));
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
            try (RecordCursor cursor = recordSetProvider.getRecordSet(hiveSplit, columnHandles).cursor()) {
                assertRecordCursorType(cursor, fileType);
                while (cursor.advanceNextPosition()) {
                    rowNumber++;

                    assertEquals(cursor.getDouble(columnIndex.get("t_double")), baseValue + 6.2 + rowNumber);
                    assertEquals(cursor.getSlice(columnIndex.get("ds")).toStringUtf8(), ds);
                    assertEquals(cursor.getSlice(columnIndex.get("file_format")).toStringUtf8(), fileType);
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
            try (RecordCursor cursor = recordSetProvider.getRecordSet(split, columnHandles).cursor()) {
                assertRecordCursorType(cursor, "textfile");
                assertEquals(cursor.getTotalBytes(), hiveSplit.getLength());

                while (cursor.advanceNextPosition()) {
                    rowNumber++;

                    if (rowNumber % 19 == 0) {
                        assertTrue(cursor.isNull(columnIndex.get("t_string")));
                    }
                    else if (rowNumber % 19 == 1) {
                        assertEquals(cursor.getSlice(columnIndex.get("t_string")).toStringUtf8(), "");
                    }
                    else {
                        assertEquals(cursor.getSlice(columnIndex.get("t_string")).toStringUtf8(), "unpartitioned");
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
        ConnectorTableHandle table = getTableHandle(tableUnpartitioned);
        ConnectorPartitionResult partitionResult = splitManager.getPartitions(table, TupleDomain.<ConnectorColumnHandle>all());
        ConnectorSplit split = Iterables.getFirst(getAllSplits(splitManager.getPartitionSplits(table, partitionResult.getPartitions())), null);
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

        try (RecordCursor cursor = recordSetProvider.getRecordSet(split, columnHandles).cursor()) {
            assertRecordCursorType(cursor, "rcfile-binary");

            assertTrue(cursor.advanceNextPosition());
            assertEquals(cursor.getLong(0), 1);
            assertEquals(cursor.getSlice(1).toStringUtf8(), "hello");
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
            assertEquals(cursor.getSlice(1).toStringUtf8(), "bye");
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
                if (BOOLEAN.equals(column.getType())) {
                    cursor.getBoolean(columnIndex);
                }
                else if (BIGINT.equals(column.getType())) {
                    cursor.getLong(columnIndex);
                }
                else if (DOUBLE.equals(column.getType())) {
                    cursor.getDouble(columnIndex);
                }
                else if (VARCHAR.equals(column.getType())) {
                    try {
                        cursor.getSlice(columnIndex);
                    }
                    catch (RuntimeException e) {
                        throw new RuntimeException("column " + column, e);
                    }
                }
                else if (TIMESTAMP.equals(column.getType())) {
                    cursor.getLong(columnIndex);
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

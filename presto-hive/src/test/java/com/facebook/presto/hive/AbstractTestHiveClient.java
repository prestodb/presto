package com.facebook.presto.hive;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.Partition;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.Split;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TableNotFoundException;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Maps.uniqueIndex;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(groups = "hive")
public abstract class AbstractTestHiveClient
{
    public static final String DATABASE = "default";
    public static final String INVALID_DATABASE = "totally_invalid_database";
    public static final SchemaTableName TABLE = new SchemaTableName(DATABASE, "presto_test");
    public static final SchemaTableName TABLE_UNPARTITIONED = new SchemaTableName(DATABASE, "presto_test_unpartitioned");
    public static final SchemaTableName VIEW = new SchemaTableName(DATABASE, "presto_test_view");
    public static final SchemaTableName INVALID_TABLE = new SchemaTableName(DATABASE, "totally_invalid_table_name");
    public static final TableHandle INVALID_TABLE_HANDLE = new HiveTableHandle("hive", INVALID_TABLE);
    public static final String INVALID_COLUMN = "totally_invalid_column_name";
    public static final ColumnHandle INVALID_COLUMN_HANDLE = new HiveColumnHandle("hive", INVALID_COLUMN, 0, HiveType.STRING, 0, false);

    private static final ColumnHandle DS_COLUMN = new HiveColumnHandle("hive", "ds", 0, HiveType.STRING, -1, true);
    private static final ColumnHandle FILE_FORMAT_COLUMN = new HiveColumnHandle("hive", "file_format", 1, HiveType.STRING, 0, true);
    private static final ColumnHandle DUMMY_COLUMN = new HiveColumnHandle("hive", "dummy", 2, HiveType.STRING, 0, true);
    private static final Set<Partition> PARTITIONS = ImmutableSet.<Partition>of(
            new HivePartition(TABLE, "ds=2012-12-29/file_format=rcfile/dummy=1", ImmutableMap.of(DS_COLUMN, "2012-12-29", FILE_FORMAT_COLUMN, "rcfile", DUMMY_COLUMN, "1")),
            new HivePartition(TABLE, "ds=2012-12-29/file_format=sequencefile/dummy=2", ImmutableMap.of(DS_COLUMN, "2012-12-29", FILE_FORMAT_COLUMN, "sequencefile", DUMMY_COLUMN, "2")),
            new HivePartition(TABLE, "ds=2012-12-29/file_format=textfile/dummy=3", ImmutableMap.of(DS_COLUMN, "2012-12-29", FILE_FORMAT_COLUMN, "textfile", DUMMY_COLUMN, "3")));
    private static final Set<Partition> UNPARTITIONED_PARTITIONS = ImmutableSet.<Partition>of(new HivePartition(TABLE_UNPARTITIONED));
    private static final Partition INVALID_PARTITION = new HivePartition(INVALID_TABLE, "unknown", ImmutableMap.<ColumnHandle, String>of());

    protected ConnectorMetadata metadata;
    protected ConnectorSplitManager splitManager;
    protected ConnectorRecordSetProvider recordSetProvider;

    @Test
    public void testGetDatabaseNames()
            throws Exception
    {
        List<String> databases = metadata.listSchemaNames();
        assertTrue(databases.contains(DATABASE));
    }

    @Test
    public void testGetTableNames()
            throws Exception
    {
        List<SchemaTableName> tables = metadata.listTables(DATABASE);
        assertTrue(tables.contains(TABLE));
    }

    @Test(expectedExceptions = SchemaNotFoundException.class)
    public void testGetTableNamesException()
            throws Exception
    {
        metadata.listTables(INVALID_DATABASE);
    }

    @Test
    public void testGetPartitionKeys()
            throws Exception
    {
        TableHandle tableHandle = metadata.getTableHandle(TABLE);
        List<String> partitionKeys = metadata.getTableMetadata(tableHandle).getPartitionKeys();
        assertEquals(partitionKeys.size(), 3);
        assertEquals(partitionKeys.get(0), "ds");
        assertEquals(partitionKeys.get(1), "file_format");
        assertEquals(partitionKeys.get(2), "dummy");
    }

    @Test
    public void testGetPartitions()
            throws Exception
    {
        TableHandle tableHandle = metadata.getTableHandle(TABLE);
        List<Partition> partitions = splitManager.getPartitions(tableHandle, ImmutableMap.<ColumnHandle, Object>of());
        assertEquals(partitions, PARTITIONS);
    }

    @Test(expectedExceptions = TableNotFoundException.class)
    public void testGetPartitionsException()
            throws Exception
    {
        splitManager.getPartitions(INVALID_TABLE_HANDLE, Collections.<ColumnHandle, Object>emptyMap());
    }

    @Test
    public void testGetPartitionNames()
            throws Exception
    {
        TableHandle tableHandle = metadata.getTableHandle(TABLE);
        List<Partition> partitions = splitManager.getPartitions(tableHandle, ImmutableMap.<ColumnHandle, Object>of());
        assertEquals(partitions.size(), 3);
        assertEquals(partitions, PARTITIONS);
    }

    @Test
    public void testGetPartitionNamesUnpartitioned()
            throws Exception
    {
        TableHandle tableHandle = metadata.getTableHandle(TABLE_UNPARTITIONED);
        List<Partition> partitions = splitManager.getPartitions(tableHandle, ImmutableMap.<ColumnHandle, Object>of());
        assertEquals(partitions.size(), 1);
        assertEquals(partitions, UNPARTITIONED_PARTITIONS);
    }

    @Test(expectedExceptions = TableNotFoundException.class)
    public void testGetPartitionNamesException()
            throws Exception
    {
        splitManager.getPartitions(INVALID_TABLE_HANDLE, ImmutableMap.<ColumnHandle, Object>of());
    }

    @Test
    public void testGetTableSchema()
            throws Exception
    {
        SchemaTableMetadata tableMetadata = metadata.getTableMetadata(metadata.getTableHandle(TABLE));
        Map<String, ColumnMetadata> map = uniqueIndex(tableMetadata.getColumns(), columnNameGetter());

        assertPrimitiveField(map, "t_string", ColumnType.STRING);
        assertPrimitiveField(map, "t_tinyint", ColumnType.LONG);
        assertPrimitiveField(map, "t_smallint", ColumnType.LONG);
        assertPrimitiveField(map, "t_int", ColumnType.LONG);
        assertPrimitiveField(map, "t_bigint", ColumnType.LONG);
        assertPrimitiveField(map, "t_float", ColumnType.DOUBLE);
        assertPrimitiveField(map, "t_double", ColumnType.DOUBLE);
        assertPrimitiveField(map, "t_boolean", ColumnType.LONG);
//        assertPrimitiveField(map, "t_timestamp", Type.LONG);
//        assertPrimitiveField(map, "t_binary", Type.STRING);
        assertPrimitiveField(map, "t_array_string", ColumnType.STRING); // Currently mapped as a string
        assertPrimitiveField(map, "t_map", ColumnType.STRING); // Currently mapped as a string
        assertPrimitiveField(map, "t_complex", ColumnType.STRING); // Currently mapped as a string
        assertPrimitiveField(map, "ds", ColumnType.STRING);
        assertPrimitiveField(map, "file_format", ColumnType.STRING);
        assertPrimitiveField(map, "dummy", ColumnType.LONG);
    }

    @Test
    public void testGetTableSchemaUnpartitioned()
            throws Exception
    {
        TableHandle tableHandle = metadata.getTableHandle(TABLE_UNPARTITIONED);
        SchemaTableMetadata tableMetadata = metadata.getTableMetadata(tableHandle);
        Map<String, ColumnMetadata> map = uniqueIndex(tableMetadata.getColumns(), columnNameGetter());

        assertPrimitiveField(map, "t_string", ColumnType.STRING);
        assertPrimitiveField(map, "t_tinyint", ColumnType.LONG);
    }

    @Test
    public void testGetTableSchemaException()
            throws Exception
    {
        assertNull(metadata.getTableHandle(INVALID_TABLE));
    }

    @Test
    public void testGetPartitionSplitsBatch()
            throws Exception
    {
        TableHandle tableHandle = metadata.getTableHandle(TABLE);
        List<Partition> partitions = splitManager.getPartitions(tableHandle, ImmutableMap.<ColumnHandle, Object>of());
        Iterable<Split> iterator = splitManager.getPartitionSplits(partitions);

        List<Split> splits = ImmutableList.copyOf(iterator);
        assertEquals(splits.size(), 3);
    }

    @Test
    public void testGetPartitionSplitsBatchUnpartitioned()
            throws Exception
    {
        TableHandle tableHandle = metadata.getTableHandle(TABLE_UNPARTITIONED);
        List<Partition> partitions = splitManager.getPartitions(tableHandle, ImmutableMap.<ColumnHandle, Object>of());
        Iterable<Split> iterator = splitManager.getPartitionSplits(partitions);

        List<Split> splits = ImmutableList.copyOf(iterator);
        assertEquals(splits.size(), 1);
    }

    @Test(expectedExceptions = TableNotFoundException.class)
    public void testGetPartitionSplitsBatchInvalidTable()
            throws Exception
    {
        splitManager.getPartitionSplits(ImmutableList.of(INVALID_PARTITION));
    }

    @Test
    public void testGetPartitionSplitsEmpty()
            throws Exception
    {
        Iterable<Split> iterator = splitManager.getPartitionSplits(ImmutableList.<Partition>of());
        // fetch full list
        ImmutableList.copyOf(iterator);
    }

    @Test
    public void testGetRecords()
            throws Exception
    {
        TableHandle tableHandle = metadata.getTableHandle(TABLE);
        SchemaTableMetadata tableMetadata = metadata.getTableMetadata(tableHandle);
        List<ColumnHandle>  columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(tableHandle).values());
        Map<String, Integer> columnIndex = indexColumns(columnHandles);

        List<Partition> partitions = splitManager.getPartitions(tableHandle, ImmutableMap.<ColumnHandle, Object>of());
        List<Split> splits = ImmutableList.copyOf(splitManager.getPartitionSplits(partitions));
        assertEquals(splits.size(), PARTITIONS.size());
        for (Split split : splits) {
            HiveSplit hiveSplit = (HiveSplit) split;

            List<HivePartitionKey> partitionKeys = hiveSplit.getPartitionKeys();
            String ds = partitionKeys.get(0).getValue();
            String fileType = partitionKeys.get(1).getValue();
            long dummy = Long.parseLong(partitionKeys.get(2).getValue());

            long baseValue = getBaseValueForFileType(fileType);

            long rowNumber = 0;
            long completedBytes = 0;
            try (RecordCursor cursor = recordSetProvider.getRecordSet(hiveSplit, columnHandles).cursor()) {
                assertEquals(cursor.getTotalBytes(), hiveSplit.getLength());

                while (cursor.advanceNextPosition()) {
                    try {
                        assertReadFields(cursor, tableMetadata.getColumns());
                    }
                    catch (Exception e) {
                        throw new RuntimeException("row " + rowNumber, e);
                    }

                    rowNumber++;

                    if (rowNumber % 19 == 0) {
                        assertTrue(cursor.isNull(columnIndex.get("t_string")));
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
                        assertEquals(cursor.getLong(columnIndex.get("t_boolean")), rowNumber % 3, String.format("row = %s", rowNumber));
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
        TableHandle tableHandle = metadata.getTableHandle(TABLE);
        List<ColumnHandle>  columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(tableHandle).values());
        Map<String, Integer> columnIndex = indexColumns(columnHandles);

        List<Partition> partitions = splitManager.getPartitions(tableHandle, ImmutableMap.<ColumnHandle, Object>of());
        List<Split> splits = ImmutableList.copyOf(splitManager.getPartitionSplits(partitions));
        assertEquals(splits.size(), PARTITIONS.size());
        for (Split split : splits) {
            HiveSplit hiveSplit = (HiveSplit) split;

            List<HivePartitionKey> partitionKeys = hiveSplit.getPartitionKeys();
            String ds = partitionKeys.get(0).getValue();
            String fileType = partitionKeys.get(1).getValue();
            long dummy = Long.parseLong(partitionKeys.get(2).getValue());

            long baseValue = getBaseValueForFileType(fileType);

            long rowNumber = 0;
            try (RecordCursor cursor = recordSetProvider.getRecordSet(hiveSplit, columnHandles).cursor()) {
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
        TableHandle tableHandle = metadata.getTableHandle(TABLE_UNPARTITIONED);
        List<ColumnHandle>  columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(tableHandle).values());
        Map<String, Integer> columnIndex = indexColumns(columnHandles);

        List<Partition> partitions = splitManager.getPartitions(tableHandle, ImmutableMap.<ColumnHandle, Object>of());
        List<Split> splits = ImmutableList.copyOf(splitManager.getPartitionSplits(partitions));
        assertEquals(splits.size(), 1);

        for (Split split : splits) {
            HiveSplit hiveSplit = (HiveSplit) split;

            assertEquals(hiveSplit.getPartitionKeys(), ImmutableList.of());

            long rowNumber = 0;
            try (RecordCursor cursor = recordSetProvider.getRecordSet(split, columnHandles).cursor()) {
                assertEquals(cursor.getTotalBytes(), hiveSplit.getLength());

                while (cursor.advanceNextPosition()) {
                    rowNumber++;

                    if (rowNumber % 19 == 0) {
                        assertTrue(cursor.isNull(columnIndex.get("t_string")));
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
        TableHandle table = metadata.getTableHandle(TABLE_UNPARTITIONED);
        List<Partition> partitions = splitManager.getPartitions(table, ImmutableMap.<ColumnHandle, Object>of());
        Split split = Iterables.getFirst(splitManager.getPartitionSplits(partitions), null);
        RecordSet recordSet = recordSetProvider.getRecordSet(split, ImmutableList.of(INVALID_COLUMN_HANDLE));
        recordSet.cursor();
    }

    @Test(expectedExceptions = TableNotFoundException.class, expectedExceptionsMessageRegExp = HiveClient.HIVE_VIEWS_NOT_SUPPORTED)
    public void testViewsAreNotSupported()
            throws Exception
    {
        metadata.getTableHandle(VIEW);
    }

    private long getBaseValueForFileType(String fileType)
    {
        switch (fileType) {
            case "rcfile":
                return 0;
            case "sequencefile":
                return 200;
            case "textfile":
                return 400;
            default:
                throw new IllegalArgumentException("Unexpected fileType key " + fileType);
        }
    }

    private void assertReadFields(RecordCursor cursor, List<ColumnMetadata> schema)
    {
        for (int columnIndex = 0; columnIndex < schema.size(); columnIndex++) {
            ColumnMetadata column = schema.get(columnIndex);
            if (!cursor.isNull(columnIndex)) {
                switch (column.getType()) {
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
                        catch (Exception e) {
                            throw new RuntimeException("column " + column, e);
                        }
                        break;
                    default:
                        fail("Unknown primitive type " + columnIndex);
                }
            }
        }
    }

    private static void assertPrimitiveField(Map<String, ColumnMetadata> map, String name, ColumnType type)
    {
        assertTrue(map.containsKey(name));
        ColumnMetadata column = map.get(name);
        assertEquals(column.getType(), type);
    }

    private static ImmutableMap<String, Integer> indexColumns(List<ColumnHandle> columnHandles)
    {
        ImmutableMap.Builder<String, Integer> index = ImmutableMap.builder();
        int i = 0;
        for (ColumnHandle columnHandle : columnHandles) {
            checkArgument(columnHandle instanceof HiveColumnHandle, "columnHandle is not an instance of HiveColumnHandle");
            HiveColumnHandle hiveColumnHandle = (HiveColumnHandle) columnHandle;
            index.put(hiveColumnHandle.getName(), i++);
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

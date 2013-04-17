package com.facebook.presto.hive;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.spi.Partition;
import com.facebook.presto.spi.PartitionChunk;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TableNotFoundException;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    public static final TableHandle INVALID_TABLE_HANDLE = new HiveTableHandle(INVALID_TABLE);
    public static final String INVALID_COLUMN = "totally_invalid_column_name";
    public static final ColumnHandle INVALID_COLUMN_HANDLE = new HiveColumnHandle(INVALID_COLUMN, 0, HiveType.STRING, 0, false);

    private static final ColumnHandle DS_COLUMN = new HiveColumnHandle("ds", 0, HiveType.STRING, -1, true);
    private static final ColumnHandle FILE_FORMAT_COLUMN = new HiveColumnHandle("file_format", 1, HiveType.STRING, 0, true);
    private static final ColumnHandle DUMMY_COLUMN = new HiveColumnHandle("dummy", 2, HiveType.STRING, 0, true);
    private static final Set<Partition> PARTITIONS = ImmutableSet.<Partition>of(
            new HivePartition(TABLE, "ds=2012-12-29/file_format=rcfile/dummy=1", ImmutableMap.of(DS_COLUMN, "2012-12-29", FILE_FORMAT_COLUMN, "rcfile", DUMMY_COLUMN, "1")),
            new HivePartition(TABLE, "ds=2012-12-29/file_format=sequencefile/dummy=2", ImmutableMap.of(DS_COLUMN, "2012-12-29", FILE_FORMAT_COLUMN, "sequencefile", DUMMY_COLUMN, "2")),
            new HivePartition(TABLE, "ds=2012-12-29/file_format=textfile/dummy=3", ImmutableMap.of(DS_COLUMN, "2012-12-29", FILE_FORMAT_COLUMN, "textfile", DUMMY_COLUMN, "3")));
    private static final Set<Partition> UNPARTITIONED_PARTITIONS = ImmutableSet.<Partition>of(new HivePartition(TABLE_UNPARTITIONED));
    private static final Partition INVALID_PARTITION = new HivePartition(INVALID_TABLE, "unknown", ImmutableMap.<ColumnHandle, String>of());

    protected ImportClient client;

    @Test
    public void testGetDatabaseNames()
            throws Exception
    {
        List<String> databases = client.listSchemaNames();
        assertTrue(databases.contains(DATABASE));
    }

    @Test
    public void testGetTableNames()
            throws Exception
    {
        List<SchemaTableName> tables = client.listTables(DATABASE);
        assertTrue(tables.contains(TABLE));
    }

    @Test(expectedExceptions = SchemaNotFoundException.class)
    public void testGetTableNamesException()
            throws Exception
    {
        client.listTables(INVALID_DATABASE);
    }

    @Test
    public void testGetPartitionKeys()
            throws Exception
    {
        TableHandle tableHandle = client.getTableHandle(TABLE);
        List<String> partitionKeys = client.getTableMetadata(tableHandle).getPartitionKeys();
        assertEquals(partitionKeys.size(), 3);
        assertEquals(partitionKeys.get(0), "ds");
        assertEquals(partitionKeys.get(1), "file_format");
        assertEquals(partitionKeys.get(2), "dummy");
    }

    @Test
    public void testGetPartitions()
            throws Exception
    {
        TableHandle tableHandle = client.getTableHandle(TABLE);
        List<Partition> partitions = client.getPartitions(tableHandle, ImmutableMap.<ColumnHandle, Object>of());
        assertEquals(partitions, PARTITIONS);
    }

    @Test(expectedExceptions = TableNotFoundException.class)
    public void testGetPartitionsException()
            throws Exception
    {
        client.getPartitions(INVALID_TABLE_HANDLE, Collections.<ColumnHandle, Object>emptyMap());
    }

    @Test
    public void testGetPartitionNames()
            throws Exception
    {
        TableHandle tableHandle = client.getTableHandle(TABLE);
        List<Partition> partitions = client.getPartitions(tableHandle, ImmutableMap.<ColumnHandle, Object>of());
        assertEquals(partitions.size(), 3);
        assertEquals(partitions, PARTITIONS);
    }

    @Test
    public void testGetPartitionNamesUnpartitioned()
            throws Exception
    {
        TableHandle tableHandle = client.getTableHandle(TABLE_UNPARTITIONED);
        List<Partition> partitions = client.getPartitions(tableHandle, ImmutableMap.<ColumnHandle, Object>of());
        assertEquals(partitions.size(), 1);
        assertEquals(partitions, UNPARTITIONED_PARTITIONS);
    }

    @Test(expectedExceptions = TableNotFoundException.class)
    public void testGetPartitionNamesException()
            throws Exception
    {
        client.getPartitions(INVALID_TABLE_HANDLE, ImmutableMap.<ColumnHandle, Object>of());
    }

    @Test
    public void testGetTableSchema()
            throws Exception
    {
        SchemaTableMetadata tableMetadata = client.getTableMetadata(client.getTableHandle(TABLE));
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
        TableHandle tableHandle = client.getTableHandle(TABLE_UNPARTITIONED);
        SchemaTableMetadata tableMetadata = client.getTableMetadata(tableHandle);
        Map<String, ColumnMetadata> map = uniqueIndex(tableMetadata.getColumns(), columnNameGetter());

        assertPrimitiveField(map, "t_string", ColumnType.STRING);
        assertPrimitiveField(map, "t_tinyint", ColumnType.LONG);
    }

    @Test
    public void testGetTableSchemaException()
            throws Exception
    {
        assertNull(client.getTableHandle(INVALID_TABLE));
    }

    @Test
    public void testGetPartitionChunksBatch()
            throws Exception
    {
        TableHandle tableHandle = client.getTableHandle(TABLE);
        List<Partition> partitions = client.getPartitions(tableHandle, ImmutableMap.<ColumnHandle, Object>of());
        Iterable<PartitionChunk> iterator = client.getPartitionChunks(partitions);

        List<PartitionChunk> chunks = ImmutableList.copyOf(iterator);
        assertEquals(chunks.size(), 3);
    }

    @Test
    public void testGetPartitionChunksBatchUnpartitioned()
            throws Exception
    {
        TableHandle tableHandle = client.getTableHandle(TABLE_UNPARTITIONED);
        List<Partition> partitions = client.getPartitions(tableHandle, ImmutableMap.<ColumnHandle, Object>of());
        Iterable<PartitionChunk> iterator = client.getPartitionChunks(partitions);

        List<PartitionChunk> chunks = ImmutableList.copyOf(iterator);
        assertEquals(chunks.size(), 1);
    }

    @Test(expectedExceptions = TableNotFoundException.class)
    public void testGetPartitionChunksBatchInvalidTable()
            throws Exception
    {
        client.getPartitionChunks(ImmutableList.of(INVALID_PARTITION));
    }

    @Test
    public void testGetPartitionChunksEmpty()
            throws Exception
    {
        Iterable<PartitionChunk> iterator = client.getPartitionChunks(ImmutableList.<Partition>of());
        // fetch full list
        ImmutableList.copyOf(iterator);
    }

    @Test
    public void testGetRecords()
            throws Exception
    {
        TableHandle tableHandle = client.getTableHandle(TABLE);
        SchemaTableMetadata tableMetadata = client.getTableMetadata(tableHandle);
        List<Partition> partitions = client.getPartitions(tableHandle, ImmutableMap.<ColumnHandle, Object>of());
        ImmutableList<ColumnHandle> columns = ImmutableList.copyOf(client.getColumnHandles(tableHandle).values());
        List<PartitionChunk> partitionChunks = ImmutableList.copyOf(client.getPartitionChunks(partitions));
        assertEquals(partitionChunks.size(), PARTITIONS.size());
        for (PartitionChunk partitionChunk : partitionChunks) {
            HivePartitionChunk chunk = (HivePartitionChunk) partitionChunk;

            byte[] bytes = client.serializePartitionChunk(chunk);
            chunk = (HivePartitionChunk) client.deserializePartitionChunk(bytes);

            Map<String, ColumnMetadata> map = uniqueIndex(tableMetadata.getColumns(), columnNameGetter());

            List<HivePartitionKey> partitionKeys = chunk.getPartitionKeys();
            String ds = partitionKeys.get(0).getValue();
            String fileType = partitionKeys.get(1).getValue();
            long dummy = Long.parseLong(partitionKeys.get(2).getValue());

            long baseValue = getBaseValueForFileType(fileType);

            long rowNumber = 0;
            long completedBytes = 0;
            try (RecordCursor cursor = client.getRecords(chunk, columns)) {
                assertEquals(cursor.getTotalBytes(), chunk.getLength());

                while (cursor.advanceNextPosition()) {
                    try {
                        assertReadFields(cursor, tableMetadata.getColumns());
                    }
                    catch (Exception e) {
                        throw new RuntimeException("row " + rowNumber, e);
                    }

                    rowNumber++;

                    if (rowNumber % 19 == 0) {
                        assertTrue(cursor.isNull(map.get("t_string").getOrdinalPosition()));
                    }
                    else {
                        assertEquals(cursor.getString(map.get("t_string").getOrdinalPosition()), (fileType + " test").getBytes(Charsets.UTF_8));
                    }

                    assertEquals(cursor.getLong(map.get("t_tinyint").getOrdinalPosition()), (long) ((byte) (baseValue + 1 + rowNumber)));
                    assertEquals(cursor.getLong(map.get("t_smallint").getOrdinalPosition()), baseValue + 2 + rowNumber);
                    assertEquals(cursor.getLong(map.get("t_int").getOrdinalPosition()), baseValue + 3 + rowNumber);

                    if (rowNumber % 13 == 0) {
                        assertTrue(cursor.isNull(map.get("t_bigint").getOrdinalPosition()));
                    }
                    else {
                        assertEquals(cursor.getLong(map.get("t_bigint").getOrdinalPosition()), baseValue + 4 + rowNumber);
                    }

                    assertEquals(cursor.getDouble(map.get("t_float").getOrdinalPosition()), baseValue + 5.1 + rowNumber, 0.001);
                    assertEquals(cursor.getDouble(map.get("t_double").getOrdinalPosition()), baseValue + 6.2 + rowNumber);

                    if (rowNumber % 3 == 2) {
                        assertTrue(cursor.isNull(map.get("t_boolean").getOrdinalPosition()));
                    }
                    else {
                        assertEquals(cursor.getLong(map.get("t_boolean").getOrdinalPosition()), rowNumber % 3, String.format("row = %s", rowNumber));
                    }

                    if (rowNumber % 29 == 0) {
                        assertTrue(cursor.isNull(map.get("t_map").getOrdinalPosition()));
                    }
                    else {
                        String expectedJson = "{\"format\":\"" + fileType + "\"}";
                        assertEquals(cursor.getString(map.get("t_map").getOrdinalPosition()), expectedJson.getBytes(Charsets.UTF_8));
                    }

                    if (rowNumber % 27 == 0) {
                        assertTrue(cursor.isNull(map.get("t_array_string").getOrdinalPosition()));
                    }
                    else {
                        String expectedJson = "[\"" + fileType + "\",\"test\",\"data\"]";
                        assertEquals(cursor.getString(map.get("t_array_string").getOrdinalPosition()), expectedJson.getBytes(Charsets.UTF_8));
                    }

                    if (rowNumber % 31 == 0) {
                        assertTrue(cursor.isNull(map.get("t_complex").getOrdinalPosition()));
                    }
                    else {
                        String expectedJson = "{1:[{\"s_string\":\"" + fileType + "-a\",\"s_double\":0.1},{\"s_string\":\"" + fileType + "-b\",\"s_double\":0.2}]}";
                        assertEquals(cursor.getString(map.get("t_complex").getOrdinalPosition()), expectedJson.getBytes(Charsets.UTF_8));
                    }

                    assertEquals(cursor.getString(map.get("ds").getOrdinalPosition()), ds.getBytes(Charsets.UTF_8));
                    assertEquals(cursor.getString(map.get("file_format").getOrdinalPosition()), fileType.getBytes(Charsets.UTF_8));
                    assertEquals(cursor.getLong(map.get("dummy").getOrdinalPosition()), dummy);

                    long newCompletedBytes = cursor.getCompletedBytes();
                    assertTrue(newCompletedBytes >= completedBytes);
                    assertTrue(newCompletedBytes <= chunk.getLength());
                    completedBytes = newCompletedBytes;
                }
            }
            assertTrue(completedBytes <= chunk.getLength());
            assertEquals(rowNumber, 100);
        }
    }

    @Test
    public void testGetPartialRecords()
            throws Exception
    {
        TableHandle tableHandle = client.getTableHandle(TABLE);
        SchemaTableMetadata tableMetadata = client.getTableMetadata(tableHandle);
        List<Partition> partitions = client.getPartitions(tableHandle, ImmutableMap.<ColumnHandle, Object>of());
        ImmutableList<ColumnHandle> columns = ImmutableList.copyOf(client.getColumnHandles(tableHandle).values());
        List<PartitionChunk> partitionChunks = ImmutableList.copyOf(client.getPartitionChunks(partitions));
        assertEquals(partitionChunks.size(), PARTITIONS.size());
        for (PartitionChunk partitionChunk : partitionChunks) {
            HivePartitionChunk chunk = (HivePartitionChunk) partitionChunk;

            byte[] bytes = client.serializePartitionChunk(chunk);
            chunk = (HivePartitionChunk) client.deserializePartitionChunk(bytes);

            Map<String, ColumnMetadata> map = uniqueIndex(tableMetadata.getColumns(), columnNameGetter());

            List<HivePartitionKey> partitionKeys = chunk.getPartitionKeys();
            String ds = partitionKeys.get(0).getValue();
            String fileType = partitionKeys.get(1).getValue();
            long dummy = Long.parseLong(partitionKeys.get(2).getValue());

            long baseValue = getBaseValueForFileType(fileType);

            long rowNumber = 0;
            try (RecordCursor cursor = client.getRecords(chunk, columns)) {
                while (cursor.advanceNextPosition()) {
                    rowNumber++;

                    assertEquals(cursor.getDouble(map.get("t_double").getOrdinalPosition()), baseValue + 6.2 + rowNumber);
                    assertEquals(cursor.getString(map.get("ds").getOrdinalPosition()), ds.getBytes(Charsets.UTF_8));
                    assertEquals(cursor.getString(map.get("file_format").getOrdinalPosition()), fileType.getBytes(Charsets.UTF_8));
                    assertEquals(cursor.getLong(map.get("dummy").getOrdinalPosition()), dummy);
                }
            }
            assertEquals(rowNumber, 100);
        }
    }

    @Test
    public void testGetRecordsUnpartitioned()
            throws Exception
    {
        TableHandle tableHandle = client.getTableHandle(TABLE_UNPARTITIONED);
        SchemaTableMetadata tableMetadata = client.getTableMetadata(tableHandle);
        List<Partition> partitions = client.getPartitions(tableHandle, ImmutableMap.<ColumnHandle, Object>of());
        ImmutableList<ColumnHandle> columns = ImmutableList.copyOf(client.getColumnHandles(tableHandle).values());
        List<PartitionChunk> partitionChunks = ImmutableList.copyOf(client.getPartitionChunks(partitions));
        assertEquals(partitionChunks.size(), 1);

        for (PartitionChunk partitionChunk : partitionChunks) {
            HivePartitionChunk chunk = (HivePartitionChunk) partitionChunk;

            byte[] bytes = client.serializePartitionChunk(chunk);
            chunk = (HivePartitionChunk) client.deserializePartitionChunk(bytes);

            Map<String, ColumnMetadata> map = uniqueIndex(tableMetadata.getColumns(), columnNameGetter());

            assertEquals(chunk.getPartitionKeys(), ImmutableList.of());

            long rowNumber = 0;
            try (RecordCursor cursor = client.getRecords(chunk, columns)) {
                assertEquals(cursor.getTotalBytes(), chunk.getLength());

                while (cursor.advanceNextPosition()) {
                    rowNumber++;

                    if (rowNumber % 19 == 0) {
                        assertTrue(cursor.isNull(map.get("t_string").getOrdinalPosition()));
                    }
                    else {
                        assertEquals(cursor.getString(map.get("t_string").getOrdinalPosition()), "unpartitioned".getBytes(Charsets.UTF_8));
                    }

                    assertEquals(cursor.getLong(map.get("t_tinyint").getOrdinalPosition()), 1 + rowNumber);
                }
            }
            assertEquals(rowNumber, 100);
        }
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*" + INVALID_COLUMN + ".*")
    public void testGetRecordsInvalidColumn()
            throws Exception
    {
        TableHandle table = client.getTableHandle(TABLE_UNPARTITIONED);
        List<Partition> partitions = client.getPartitions(table, ImmutableMap.<ColumnHandle, Object>of());
        PartitionChunk partitionChunk = Iterables.getFirst(client.getPartitionChunks(partitions), null);
        client.getRecords(partitionChunk, ImmutableList.of(INVALID_COLUMN_HANDLE));
    }


    @Test(expectedExceptions = TableNotFoundException.class, expectedExceptionsMessageRegExp = HiveClient.HIVE_VIEWS_NOT_SUPPORTED)
    public void testViewsAreNotSupported()
            throws Exception
    {
        client.getTableHandle(VIEW);
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
        for (ColumnMetadata column : schema) {
            if (!cursor.isNull(column.getOrdinalPosition())) {
                switch (column.getType()) {
                    case LONG:
                        cursor.getLong(column.getOrdinalPosition());
                        break;
                    case DOUBLE:
                        cursor.getDouble(column.getOrdinalPosition());
                        break;
                    case STRING:
                        try {
                            cursor.getString(column.getOrdinalPosition());
                        }
                        catch (Exception e) {
                            throw new RuntimeException("column " + column, e);
                        }
                        break;
                    default:
                        fail("Unknown primitive type " + column.getOrdinalPosition());
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

    protected HiveChunkEncoder getHiveChunkEncoder()
    {
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        objectMapperProvider.setJsonDeserializers(ImmutableMap.<Class<?>, JsonDeserializer<?>>of(Path.class, PathJsonDeserializer.INSTANCE));
        objectMapperProvider.setJsonSerializers(ImmutableMap.<Class<?>, JsonSerializer<?>>of(Path.class, ToStringSerializer.instance));
        return new HiveChunkEncoder(new JsonCodecFactory(objectMapperProvider).jsonCodec(HivePartitionChunk.class));
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

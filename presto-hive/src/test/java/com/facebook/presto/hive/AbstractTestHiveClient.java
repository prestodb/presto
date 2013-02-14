package com.facebook.presto.hive;

import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.spi.ObjectNotFoundException;
import com.facebook.presto.spi.PartitionChunk;
import com.facebook.presto.spi.PartitionInfo;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaField;
import com.facebook.presto.spi.SchemaField.Category;
import com.facebook.presto.spi.SchemaField.Type;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(groups = "hive")
public abstract class AbstractTestHiveClient
{
    public static final String DATABASE = "default";
    public static final String INVALID_DATABASE = "totally_invalid_database";
    public static final String TABLE = "presto_test";
    public static final String TABLE_UNPARTITIONED = "presto_test_unpartitioned";
    public static final String INVALID_TABLE = "totally_invalid_table_name";
    public static final String INVALID_COLUMN = "totally_invalid_column_name";
    public static final List<String> PARTITIONS = ImmutableList.of(
            "ds=2012-12-29/file_format=rcfile/dummy=1",
            "ds=2012-12-29/file_format=sequencefile/dummy=2",
            "ds=2012-12-29/file_format=textfile/dummy=3");
    public static final String UNPARTITIONED_NAME = "<UNPARTITIONED>";

    protected ImportClient client;

    @Test
    public void testGetDatabaseNames()
            throws Exception
    {
        List<String> databases = client.getDatabaseNames();
        assertTrue(databases.contains(DATABASE));
    }

    @Test
    public void testGetTableNames()
            throws Exception
    {
        List<String> tables = client.getTableNames(DATABASE);
        assertTrue(tables.contains(TABLE));
    }

    @Test(expectedExceptions = ObjectNotFoundException.class)
    public void testGetTableNamesException()
            throws Exception
    {
        client.getTableNames(INVALID_DATABASE);
    }

    @Test
    public void testGetPartitionKeys()
            throws Exception
    {
        List<SchemaField> partitionKeys = client.getPartitionKeys(DATABASE, TABLE);
        assertEquals(partitionKeys.size(), 3);
        assertEquals(partitionKeys.get(0).getFieldName(), "ds");
        assertEquals(partitionKeys.get(1).getFieldName(), "file_format");
        assertEquals(partitionKeys.get(2).getFieldName(), "dummy");
    }

    @Test(expectedExceptions = ObjectNotFoundException.class)
    public void testGetPartitionKeysException()
            throws Exception
    {
        client.getPartitionKeys(DATABASE, INVALID_TABLE);
    }

    @Test
    public void testGetPartitions()
            throws Exception
    {
        List<PartitionInfo> partitions = client.getPartitions(DATABASE, TABLE);
        ImmutableSet.Builder<String> names = ImmutableSet.builder();
        for (PartitionInfo partition : partitions) {
            names.add(partition.getName());
        }
        assertEquals(names.build(), new HashSet<>(PARTITIONS));
    }

    @Test(expectedExceptions = ObjectNotFoundException.class)
    public void testGetPartitionsException()
            throws Exception
    {
        client.getPartitions(DATABASE, INVALID_TABLE);
    }

    @Test
    public void testGetPartitionNames()
            throws Exception
    {
        List<String> partitions = client.getPartitionNames(DATABASE, TABLE);
        assertEquals(partitions.size(), 3);
        assertEquals(new HashSet<>(partitions), new HashSet<>(PARTITIONS));
    }

    @Test
    public void testGetPartitionNamesUnpartitioned()
            throws Exception
    {
        List<String> partitions = client.getPartitionNames(DATABASE, TABLE_UNPARTITIONED);
        assertEquals(partitions.size(), 1);
        assertEquals(partitions, ImmutableList.of(UNPARTITIONED_NAME));
    }

    @Test(expectedExceptions = ObjectNotFoundException.class)
    public void testGetPartitionNamesException()
            throws Exception
    {
        client.getPartitionNames(DATABASE, INVALID_TABLE);
    }

    @Test
    public void testGetTableSchema()
            throws Exception
    {
        List<SchemaField> schema = client.getTableSchema(DATABASE, TABLE);
        Map<String, SchemaField> map = schemaFieldMap(schema);

        assertPrimitiveField(map, "t_string", Type.STRING);
        assertPrimitiveField(map, "t_tinyint", Type.LONG);
        assertPrimitiveField(map, "t_smallint", Type.LONG);
        assertPrimitiveField(map, "t_int", Type.LONG);
        assertPrimitiveField(map, "t_bigint", Type.LONG);
        assertPrimitiveField(map, "t_float", Type.DOUBLE);
        assertPrimitiveField(map, "t_double", Type.DOUBLE);
        assertPrimitiveField(map, "t_boolean", Type.LONG);
//        assertPrimitiveField(map, "t_timestamp", Type.LONG);
//        assertPrimitiveField(map, "t_binary", Type.STRING);
        // assertPrimitiveField(map, "t_array_string", ARRAY);
        // assertPrimitiveField(map, "t_map", MAP);
        assertPrimitiveField(map, "ds", Type.STRING);
        assertPrimitiveField(map, "file_format", Type.STRING);
        assertPrimitiveField(map, "dummy", Type.LONG);
    }

    @Test
    public void testGetTableSchemaUnpartitioned()
            throws Exception
    {
        List<SchemaField> schema = client.getTableSchema(DATABASE, TABLE_UNPARTITIONED);
        Map<String, SchemaField> map = schemaFieldMap(schema);

        assertPrimitiveField(map, "t_string", Type.STRING);
        assertPrimitiveField(map, "t_tinyint", Type.LONG);
    }

    @Test(expectedExceptions = ObjectNotFoundException.class)
    public void testGetTableSchemaException()
            throws Exception
    {
        client.getTableSchema(DATABASE, INVALID_TABLE);
    }

    @Test
    public void testGetPartitionChunksBatch()
            throws Exception
    {
        List<String> partitions = client.getPartitionNames(DATABASE, TABLE);
        Iterable<PartitionChunk> iterator = client.getPartitionChunks(DATABASE, TABLE, partitions, ImmutableList.<String>of());

        List<PartitionChunk> chunks = ImmutableList.copyOf(iterator);
        assertEquals(chunks.size(), 3);
    }

    @Test
    public void testGetPartitionChunksBatchUnpartitioned()
            throws Exception
    {
        List<String> partitions = client.getPartitionNames(DATABASE, TABLE_UNPARTITIONED);
        Iterable<PartitionChunk> iterator = client.getPartitionChunks(DATABASE, TABLE_UNPARTITIONED, partitions, ImmutableList.<String>of());

        List<PartitionChunk> chunks = ImmutableList.copyOf(iterator);
        assertEquals(chunks.size(), 1);
    }

    @Test(expectedExceptions = ObjectNotFoundException.class)
    public void testGetPartitionChunksBatchInvalidTable()
            throws Exception
    {
        List<String> partitions = client.getPartitionNames(DATABASE, TABLE);
        client.getPartitionChunks(DATABASE, INVALID_TABLE, partitions, ImmutableList.<String>of());
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*" + INVALID_COLUMN + ".*")
    public void testGetPartitionChunksBatchInvalidColumn()
            throws Exception
    {
        client.getPartitionChunks(DATABASE, TABLE, PARTITIONS, ImmutableList.of(INVALID_COLUMN));
    }

    @Test
    public void testGetPartitionChunksEmpty()
            throws Exception
    {
        Iterable<PartitionChunk> iterator = client.getPartitionChunks(DATABASE, TABLE, ImmutableList.<String>of(), ImmutableList.<String>of());
        List<PartitionChunk> chunks = ImmutableList.copyOf(iterator);
        assertTrue(chunks.isEmpty());
    }

    @Test
    public void testGetRecords()
            throws Exception
    {
        List<SchemaField> schema = client.getTableSchema(DATABASE, TABLE);
        List<PartitionChunk> partitions = ImmutableList.copyOf(client.getPartitionChunks(DATABASE, TABLE, PARTITIONS, Lists.transform(schema, nameGetter())));
        assertEquals(partitions.size(), PARTITIONS.size());
        for (PartitionChunk partitionChunk : partitions) {
            HivePartitionChunk chunk = (HivePartitionChunk) partitionChunk;

            byte[] bytes = client.serializePartitionChunk(chunk);
            chunk = (HivePartitionChunk) client.deserializePartitionChunk(bytes);

            Map<String, SchemaField> map = schemaFieldMap(schema);

            List<HivePartitionKey> partitionKeys = chunk.getPartitionKeys();
            String ds = partitionKeys.get(0).getValue();
            String fileType = partitionKeys.get(1).getValue();
            long dummy = Long.parseLong(partitionKeys.get(2).getValue());

            long baseValue = getBaseValueForFileType(fileType);

            long rowNumber = 0;
            long completedBytes = 0;
            try (RecordCursor cursor = client.getRecords(chunk)) {
                assertEquals(cursor.getTotalBytes(), chunk.getLength());

                while (cursor.advanceNextPosition()) {
                    try {
                        assertReadFields(cursor, schema);
                    }
                    catch (Exception e) {
                        throw new RuntimeException("row " + rowNumber, e);
                    }

                    rowNumber++;

                    if (rowNumber % 19 == 0) {
                        assertTrue(cursor.isNull(map.get("t_string").getFieldId()));
                    }
                    else {
                        assertEquals(cursor.getString(map.get("t_string").getFieldId()), (fileType + " test").getBytes(Charsets.UTF_8));
                    }

                    assertEquals(cursor.getLong(map.get("t_tinyint").getFieldId()), (long) ((byte) (baseValue + 1 + rowNumber)));
                    assertEquals(cursor.getLong(map.get("t_smallint").getFieldId()), baseValue + 2 + rowNumber);
                    assertEquals(cursor.getLong(map.get("t_int").getFieldId()), baseValue + 3 + rowNumber);

                    if (rowNumber % 13 == 0) {
                        assertTrue(cursor.isNull(map.get("t_bigint").getFieldId()));
                    }
                    else {
                        assertEquals(cursor.getLong(map.get("t_bigint").getFieldId()), baseValue + 4 + rowNumber);
                    }

                    assertEquals(cursor.getDouble(map.get("t_float").getFieldId()), baseValue + 5.1 + rowNumber, 0.001);
                    assertEquals(cursor.getDouble(map.get("t_double").getFieldId()), baseValue + 6.2 + rowNumber);

                    if (rowNumber % 3 == 2) {
                        assertTrue(cursor.isNull(map.get("t_boolean").getFieldId()));
                    }
                    else {
                        assertEquals(cursor.getLong(map.get("t_boolean").getFieldId()), rowNumber % 3, String.format("row = %s", rowNumber));
                    }

                    assertEquals(cursor.getString(map.get("ds").getFieldId()), ds.getBytes(Charsets.UTF_8));
                    assertEquals(cursor.getString(map.get("file_format").getFieldId()), fileType.getBytes(Charsets.UTF_8));
                    assertEquals(cursor.getLong(map.get("dummy").getFieldId()), dummy);

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
        List<SchemaField> schema = client.getTableSchema(DATABASE, TABLE);
        List<PartitionChunk> partitions = ImmutableList.copyOf(client.getPartitionChunks(DATABASE, TABLE, PARTITIONS, ImmutableList.of("t_double")));
        assertEquals(partitions.size(), PARTITIONS.size());
        for (PartitionChunk partitionChunk : partitions) {
            HivePartitionChunk chunk = (HivePartitionChunk) partitionChunk;

            byte[] bytes = client.serializePartitionChunk(chunk);
            chunk = (HivePartitionChunk) client.deserializePartitionChunk(bytes);

            Map<String, SchemaField> map = schemaFieldMap(schema);

            List<HivePartitionKey> partitionKeys = chunk.getPartitionKeys();
            String ds = partitionKeys.get(0).getValue();
            String fileType = partitionKeys.get(1).getValue();
            long dummy = Long.parseLong(partitionKeys.get(2).getValue());

            long baseValue = getBaseValueForFileType(fileType);

            long rowNumber = 0;
            try (RecordCursor cursor = client.getRecords(chunk)) {
                while (cursor.advanceNextPosition()) {
                    rowNumber++;

                    assertEquals(cursor.getDouble(map.get("t_double").getFieldId()), baseValue + 6.2 + rowNumber);
                    assertEquals(cursor.getString(map.get("ds").getFieldId()), ds.getBytes(Charsets.UTF_8));
                    assertEquals(cursor.getString(map.get("file_format").getFieldId()), fileType.getBytes(Charsets.UTF_8));
                    assertEquals(cursor.getLong(map.get("dummy").getFieldId()), dummy);
                }
            }
            assertEquals(rowNumber, 100);
        }
    }

    @Test
    public void testGetRecordsUnpartitioned()
            throws Exception
    {
        List<SchemaField> schema = client.getTableSchema(DATABASE, TABLE_UNPARTITIONED);
        List<String> partitionNames = client.getPartitionNames(DATABASE, TABLE_UNPARTITIONED);
        List<String> columns = Lists.transform(schema, nameGetter());
        List<PartitionChunk> partitions = ImmutableList.copyOf(client.getPartitionChunks(DATABASE, TABLE_UNPARTITIONED, partitionNames, columns));
        assertEquals(partitions.size(), 1);

        for (PartitionChunk partitionChunk : partitions) {
            HivePartitionChunk chunk = (HivePartitionChunk) partitionChunk;

            byte[] bytes = client.serializePartitionChunk(chunk);
            chunk = (HivePartitionChunk) client.deserializePartitionChunk(bytes);

            Map<String, SchemaField> map = schemaFieldMap(schema);

            assertEquals(chunk.getPartitionKeys(), ImmutableList.of());

            long rowNumber = 0;
            try (RecordCursor cursor = client.getRecords(chunk)) {
                assertEquals(cursor.getTotalBytes(), chunk.getLength());

                while (cursor.advanceNextPosition()) {
                    rowNumber++;

                    if (rowNumber % 19 == 0) {
                        assertTrue(cursor.isNull(map.get("t_string").getFieldId()));
                    }
                    else {
                        assertEquals(cursor.getString(map.get("t_string").getFieldId()), "unpartitioned".getBytes(Charsets.UTF_8));
                    }

                    assertEquals(cursor.getLong(map.get("t_tinyint").getFieldId()), 1 + rowNumber);
                }
            }
            assertEquals(rowNumber, 100);
        }
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

    private void assertReadFields(RecordCursor cursor, List<SchemaField> schema)
    {
        for (SchemaField field : schema) {
            assertEquals(field.getCategory(), Category.PRIMITIVE);
            if (!cursor.isNull(field.getFieldId())) {
                switch (field.getPrimitiveType()) {
                    case LONG:
                        cursor.getLong(field.getFieldId());
                        break;
                    case DOUBLE:
                        cursor.getDouble(field.getFieldId());
                        break;
                    case STRING:
                        try {
                            cursor.getString(field.getFieldId());
                        }
                        catch (Exception e) {
                            throw new RuntimeException("field " + field, e);
                        }
                        break;
                    default:
                        fail("Unknown primitive type " + field.getPrimitiveType());
                }
            }
        }
    }

    private static void assertPrimitiveField(Map<String, SchemaField> map, String name, Type type)
    {
        assertTrue(map.containsKey(name));
        SchemaField field = map.get(name);
        assertEquals(field.getCategory(), Category.PRIMITIVE);
        assertEquals(field.getPrimitiveType(), type);
    }

    private static Map<String, SchemaField> schemaFieldMap(List<SchemaField> schema)
    {
        ImmutableMap.Builder<String, SchemaField> map = ImmutableMap.builder();
        for (SchemaField field : schema) {
            map.put(field.getFieldName(), field);
        }
        return map.build();
    }

    protected JsonCodec<HivePartitionChunk> getHivePartitionChunkCodec()
    {
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        objectMapperProvider.setJsonDeserializers(ImmutableMap.<Class<?>, JsonDeserializer<?>>of(Path.class, PathJsonDeserializer.INSTANCE));
        objectMapperProvider.setJsonSerializers(ImmutableMap.<Class<?>, JsonSerializer<?>>of(Path.class, ToStringSerializer.instance));
        return new JsonCodecFactory(objectMapperProvider).jsonCodec(HivePartitionChunk.class);
    }

    private static Function<SchemaField, String> nameGetter()
    {
        return new Function<SchemaField, String>()
        {
            @Override
            public String apply(SchemaField input)
            {
                return input.getFieldName();
            }
        };
    }
}

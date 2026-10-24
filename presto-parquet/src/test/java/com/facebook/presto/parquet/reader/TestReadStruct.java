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
package com.facebook.presto.parquet.reader;

import com.facebook.airlift.units.DataSize;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.ColumnarArray;
import com.facebook.presto.common.block.ColumnarMap;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.parquet.Field;
import com.facebook.presto.parquet.ParquetDataSourceId;
import com.facebook.presto.parquet.cache.MetadataReader;
import com.facebook.presto.parquet.cache.ParquetFileMetadata;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Optional;

import static com.facebook.airlift.units.DataSize.Unit.MEGABYTE;
import static com.facebook.presto.common.block.MethodHandleUtil.nativeValueGetter;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.parquet.ParquetTypeUtils.getColumnIO;
import static com.facebook.presto.parquet.ParquetTypeUtils.lookupColumnByName;
import static org.apache.parquet.io.ColumnIOConverter.constructField;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Unit tests for {@link ParquetReader#readStruct} when all projected sub-fields are absent from the
 * Parquet file — the scenario that arises when an Iceberg table column evolves to contain only
 * {@code unknown} sub-fields that are never physically written.
 *
 * <p>When all children are absent, a "shadow" leaf (the first physical leaf under the struct) is
 * used to read the real definition/repetition levels.  The tests verify that those levels are
 * correctly used to determine (a) which top-level struct positions are null and (b) how many
 * elements a collection contains when the struct is nested inside an ARRAY or MAP.
 */
public class TestReadStruct
{
    /**
     * Top-level optional struct: some rows have a null struct, others a non-null struct.
     * All projected sub-fields are absent from the file; the shadow leaf is the physical "a" column.
     *
     * <p>Expected: null rows produce null structs; non-null rows produce non-null structs.
     * Without the shadow-field fix, all rows read as non-null because empty levels default to
     * batchSize non-null values.
     */
    @Test
    public void testNullSemanticsForAllAbsentChildren()
            throws IOException
    {
        // File schema: optional group s { required int32 a; }
        MessageType schema = new MessageType("root",
                Types.buildGroup(OPTIONAL)
                        .required(INT32).named("a")
                        .named("s"));

        Path parquetPath = tempPath("test_null_struct");
        Configuration conf = confFor(schema);
        GroupType sType = (GroupType) schema.getType("s");

        // Write 4 rows: null, {a=1}, null, {a=2}
        try (ParquetWriter<Group> writer = writer(parquetPath, conf)) {
            writer.write(new SimpleGroup(schema));

            SimpleGroup row1 = new SimpleGroup(schema);
            row1.add("s", group(sType, "a", 1));
            writer.write(row1);

            writer.write(new SimpleGroup(schema));

            SimpleGroup row3 = new SimpleGroup(schema);
            row3.add("s", group(sType, "a", 2));
            writer.write(row3);
        }

        // Request: ROW(c BIGINT) — "c" absent; shadow is the physical "a" leaf.
        RowType requestType = RowType.from(ImmutableList.of(RowType.field("c", BIGINT)));
        boolean[] nulls = readIsNull(parquetPath, conf, "s", requestType, 4);

        assertTrue(nulls[0], "row 0: s=null in file → struct must be null");
        assertFalse(nulls[1], "row 1: s present in file → struct must be non-null");
        assertTrue(nulls[2], "row 2: s=null in file → struct must be null");
        assertFalse(nulls[3], "row 3: s present in file → struct must be non-null");
    }

    /**
     * Struct inside an ARRAY.  All projected sub-fields of the element struct are absent from
     * the file; the shadow leaf is the physical "a" column under the element.
     *
     * <p>Expected: row 0 has 2 elements, row 1 has 1 element.  Without the shadow-field fix,
     * the batchSize fallback produces wrong element counts (both would appear as 2).
     */
    @Test
    public void testElementCountForAllAbsentChildrenInsideArray()
            throws IOException
    {
        // Standard Parquet 3-level LIST encoding:
        //   optional group arr (LIST) {
        //     repeated group list {
        //       optional group element { required int32 a; }
        //     }
        //   }
        GroupType elementType = Types.buildGroup(OPTIONAL).required(INT32).named("a").named("element");
        GroupType listType = new GroupType(REPEATED, "list", elementType);
        GroupType arrType = (GroupType) Types.buildGroup(OPTIONAL)
                .as(LogicalTypeAnnotation.listType())
                .addField(listType)
                .named("arr");
        MessageType schema = new MessageType("root", arrType);

        Path parquetPath = tempPath("test_array_struct");
        Configuration conf = confFor(schema);

        GroupType arrT = (GroupType) schema.getType("arr");
        GroupType listT = (GroupType) arrT.getType("list");
        GroupType elemT = (GroupType) listT.getType("element");

        // Write 2 rows: arr=[{a=1},{a=2}], arr=[{a=3}]
        try (ParquetWriter<Group> writer = writer(parquetPath, conf)) {
            SimpleGroup row0 = new SimpleGroup(schema);
            SimpleGroup arr0 = new SimpleGroup(arrT);
            SimpleGroup list0a = new SimpleGroup(listT);
            list0a.add("element", group(elemT, "a", 1));
            arr0.add("list", list0a);
            SimpleGroup list0b = new SimpleGroup(listT);
            list0b.add("element", group(elemT, "a", 2));
            arr0.add("list", list0b);
            row0.add("arr", arr0);
            writer.write(row0);

            SimpleGroup row1 = new SimpleGroup(schema);
            SimpleGroup arr1 = new SimpleGroup(arrT);
            SimpleGroup list1 = new SimpleGroup(listT);
            list1.add("element", group(elemT, "a", 3));
            arr1.add("list", list1);
            row1.add("arr", arr1);
            writer.write(row1);
        }

        // Request: ARRAY(ROW(c BIGINT)) — "c" absent; shadow is "a" under the element struct.
        RowType elementRequestType = RowType.from(ImmutableList.of(RowType.field("c", BIGINT)));
        ArrayType requestType = new ArrayType(elementRequestType);
        int[] lengths = readArrayLengths(parquetPath, conf, "arr", requestType, 2);

        assertEquals(lengths[0], 2, "first array must have 2 elements");
        assertEquals(lengths[1], 1, "second array must have 1 element");
    }

    /**
     * Struct inside a MAP value.  All projected sub-fields of the value struct are absent from
     * the file; the shadow leaf is the physical "a" column under the value struct.
     *
     * <p>Expected: row 0 has 2 entries, row 1 has 1 entry.
     */
    @Test
    public void testElementCountForAllAbsentChildrenInsideMapValue()
            throws IOException
    {
        // Standard Parquet MAP encoding:
        //   optional group m (MAP) {
        //     repeated group key_value (MAP_KEY_VALUE) {
        //       required binary key (UTF8);
        //       optional group value { required int32 a; }
        //     }
        //   }
        GroupType valueType = Types.buildGroup(OPTIONAL).required(INT32).named("a").named("value");
        GroupType kvType = new GroupType(REPEATED, "key_value",
                Types.required(BINARY).as(LogicalTypeAnnotation.stringType()).named("key"),
                valueType);
        GroupType mType = (GroupType) Types.buildGroup(OPTIONAL)
                .as(LogicalTypeAnnotation.mapType())
                .addField(kvType)
                .named("m");
        MessageType schema = new MessageType("root", mType);

        Path parquetPath = tempPath("test_map_struct");
        Configuration conf = confFor(schema);

        GroupType mT = (GroupType) schema.getType("m");
        GroupType kvT = (GroupType) mT.getType("key_value");
        GroupType valT = (GroupType) kvT.getType("value");

        // Write 2 rows: m={"x":{a=1},"y":{a=2}}, m={"z":{a=3}}
        try (ParquetWriter<Group> writer = writer(parquetPath, conf)) {
            SimpleGroup row0 = new SimpleGroup(schema);
            SimpleGroup m0 = new SimpleGroup(mT);
            SimpleGroup kv0a = new SimpleGroup(kvT);
            kv0a.add("key", org.apache.parquet.io.api.Binary.fromString("x"));
            kv0a.add("value", group(valT, "a", 1));
            SimpleGroup kv0b = new SimpleGroup(kvT);
            kv0b.add("key", org.apache.parquet.io.api.Binary.fromString("y"));
            kv0b.add("value", group(valT, "a", 2));
            m0.add("key_value", kv0a);
            m0.add("key_value", kv0b);
            row0.add("m", m0);
            writer.write(row0);

            SimpleGroup row1 = new SimpleGroup(schema);
            SimpleGroup m1 = new SimpleGroup(mT);
            SimpleGroup kv1 = new SimpleGroup(kvT);
            kv1.add("key", org.apache.parquet.io.api.Binary.fromString("z"));
            kv1.add("value", group(valT, "a", 3));
            m1.add("key_value", kv1);
            row1.add("m", m1);
            writer.write(row1);
        }

        // Request: MAP(VARCHAR, ROW(c BIGINT)) — "c" absent; shadow is "a" under the value struct.
        RowType valueRequestType = RowType.from(ImmutableList.of(RowType.field("c", BIGINT)));
        MapType requestType = new MapType(VARCHAR, valueRequestType, nativeValueGetter(VARCHAR), nativeValueGetter(valueRequestType));
        int[] entryCounts = readMapEntryCounts(parquetPath, conf, "m", requestType, 2);

        assertEquals(entryCounts[0], 2, "first map must have 2 entries");
        assertEquals(entryCounts[1], 1, "second map must have 1 entry");
    }

    // --- Helpers ---

    private static SimpleGroup group(GroupType type, String intField, int value)
    {
        SimpleGroup g = new SimpleGroup(type);
        g.add(intField, value);
        return g;
    }

    private static Path tempPath(String prefix)
            throws IOException
    {
        return new Path(Files.createTempDirectory(prefix).toFile().getAbsolutePath(), "data.parquet");
    }

    private static Configuration confFor(MessageType schema)
    {
        Configuration conf = new Configuration(false);
        conf.set(GroupWriteSupport.PARQUET_EXAMPLE_SCHEMA, schema.toString());
        return conf;
    }

    private static ParquetWriter<Group> writer(Path path, Configuration conf)
            throws IOException
    {
        return ExampleParquetWriter.builder(path)
                .withConf(conf)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build();
    }

    private static boolean[] readIsNull(
            Path path,
            Configuration conf,
            String columnName,
            com.facebook.presto.common.type.Type requestType,
            int totalRows)
            throws IOException
    {
        boolean[] result = new boolean[totalRows];
        int pos = 0;
        try (ParquetReader reader = openReader(path, conf, columnName, requestType)) {
            Field field = openField(path, conf, columnName, requestType);
            while (reader.nextBatch() >= 0) {
                Block block = reader.readBlock(field);
                for (int i = 0; i < block.getPositionCount(); i++) {
                    result[pos++] = block.isNull(i);
                }
            }
        }
        assertEquals(pos, totalRows);
        return result;
    }

    private static int[] readArrayLengths(
            Path path,
            Configuration conf,
            String columnName,
            com.facebook.presto.common.type.Type requestType,
            int totalRows)
            throws IOException
    {
        int[] result = new int[totalRows];
        int pos = 0;
        try (ParquetReader reader = openReader(path, conf, columnName, requestType)) {
            Field field = openField(path, conf, columnName, requestType);
            while (reader.nextBatch() >= 0) {
                ColumnarArray columnar = ColumnarArray.toColumnarArray(reader.readBlock(field));
                for (int i = 0; i < columnar.getPositionCount(); i++) {
                    result[pos++] = columnar.getLength(i);
                }
            }
        }
        assertEquals(pos, totalRows);
        return result;
    }

    private static int[] readMapEntryCounts(
            Path path,
            Configuration conf,
            String columnName,
            com.facebook.presto.common.type.Type requestType,
            int totalRows)
            throws IOException
    {
        int[] result = new int[totalRows];
        int pos = 0;
        try (ParquetReader reader = openReader(path, conf, columnName, requestType)) {
            Field field = openField(path, conf, columnName, requestType);
            while (reader.nextBatch() >= 0) {
                ColumnarMap columnar = ColumnarMap.toColumnarMap(reader.readBlock(field));
                for (int i = 0; i < columnar.getPositionCount(); i++) {
                    result[pos++] = columnar.getEntryCount(i);
                }
            }
        }
        assertEquals(pos, totalRows);
        return result;
    }

    // Shared reader state — calling openReader and openField from the same file is safe because
    // openReader re-opens the stream after MetadataReader has consumed the footer.
    private static ParquetReader openReader(
            Path path,
            Configuration conf,
            String columnName,
            com.facebook.presto.common.type.Type requestType)
            throws IOException
    {
        FileSystem fs = path.getFileSystem(conf);
        MockParquetDataSource footerSource = new MockParquetDataSource(
                new ParquetDataSourceId(path.toString()),
                fs.open(path));
        ParquetFileMetadata fileMeta = MetadataReader.readFooter(
                footerSource, fs.getFileStatus(path).getLen(), Optional.empty(), false);
        footerSource.close();

        MockParquetDataSource dataSource = new MockParquetDataSource(
                new ParquetDataSourceId(path.toString()),
                fs.open(path));
        MessageType fileSchema = fileMeta.getParquetMetadata().getFileMetaData().getSchema();
        MessageColumnIO messageColumnIO = getColumnIO(fileSchema, fileSchema);

        return TestEncryption.createParquetReader(
                fileMeta.getParquetMetadata(),
                messageColumnIO,
                dataSource,
                Optional.empty(),
                new DataSize(1, MEGABYTE));
    }

    private static Field openField(
            Path path,
            Configuration conf,
            String columnName,
            com.facebook.presto.common.type.Type requestType)
            throws IOException
    {
        FileSystem fs = path.getFileSystem(conf);
        MockParquetDataSource footerSource = new MockParquetDataSource(
                new ParquetDataSourceId(path.toString()),
                fs.open(path));
        ParquetFileMetadata fileMeta = MetadataReader.readFooter(
                footerSource, fs.getFileStatus(path).getLen(), Optional.empty(), false);
        footerSource.close();

        MessageType fileSchema = fileMeta.getParquetMetadata().getFileMetaData().getSchema();
        MessageColumnIO messageColumnIO = getColumnIO(fileSchema, fileSchema);

        Optional<Field> field = constructField(requestType, lookupColumnByName(messageColumnIO, columnName), true);
        assertTrue(field.isPresent(), "constructField must return a field with allowEmptyStruct=true");
        return field.get();
    }
}

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

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.collect.Iterables.transform;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardListObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardMapObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaByteObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaFloatObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaIntObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaLongObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaShortObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaTimestampObjectInspector;
import static org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.COMPRESS_CODEC;
import static org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.COMPRESS_TYPE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(groups = "hive")
public abstract class AbstractTestHiveFileFormats
{
    private static final int NUM_ROWS = 1000;
    private static final double EPSILON = 0.001;

    private static final List<ObjectInspector> FIELD_INSPECTORS = ImmutableList.of(
                javaStringObjectInspector,
                javaStringObjectInspector,
                javaStringObjectInspector,
                javaByteObjectInspector,
                javaShortObjectInspector,
                javaIntObjectInspector,
                javaLongObjectInspector,
                javaFloatObjectInspector,
                javaDoubleObjectInspector,
                javaBooleanObjectInspector,
                javaTimestampObjectInspector,
                javaByteArrayObjectInspector,
                getStandardMapObjectInspector(javaStringObjectInspector, javaStringObjectInspector),
                getStandardMapObjectInspector(javaByteObjectInspector, javaByteObjectInspector),
                getStandardMapObjectInspector(javaShortObjectInspector, javaShortObjectInspector),
                getStandardMapObjectInspector(javaIntObjectInspector, javaIntObjectInspector),
                getStandardMapObjectInspector(javaLongObjectInspector, javaLongObjectInspector),
                getStandardMapObjectInspector(javaFloatObjectInspector, javaFloatObjectInspector),
                getStandardMapObjectInspector(javaDoubleObjectInspector, javaDoubleObjectInspector),
                getStandardMapObjectInspector(javaBooleanObjectInspector, javaBooleanObjectInspector),
                getStandardMapObjectInspector(javaTimestampObjectInspector, javaTimestampObjectInspector),
                getStandardListObjectInspector(javaStringObjectInspector),
                getStandardListObjectInspector(javaByteObjectInspector),
                getStandardListObjectInspector(javaShortObjectInspector),
                getStandardListObjectInspector(javaIntObjectInspector),
                getStandardListObjectInspector(javaLongObjectInspector),
                getStandardListObjectInspector(javaFloatObjectInspector),
                getStandardListObjectInspector(javaDoubleObjectInspector),
                getStandardListObjectInspector(javaBooleanObjectInspector),
                getStandardListObjectInspector(javaTimestampObjectInspector),
                getStandardMapObjectInspector(
                        javaStringObjectInspector,
                        getStandardListObjectInspector(
                                getStandardStructObjectInspector(
                                        ImmutableList.of("s_int"),
                                        ImmutableList.<ObjectInspector>of(javaIntObjectInspector)
                                )
                        )
                )
        );

    protected static final String COLUMN_TYPES = Joiner.on(":").join(transform(FIELD_INSPECTORS, new Function<ObjectInspector, String>()
    {
        @Override
        public String apply(ObjectInspector input)
        {
            return input.getTypeName();
        }
    }));

    protected static final List<String> COLUMN_NAMES = ImmutableList.of(
                "t_null_string",
                "t_empty_string",
                "t_string",
                "t_tinyint",
                "t_smallint",
                "t_int",
                "t_bigint",
                "t_float",
                "t_double",
                "t_boolean",
                "t_timestamp",
                "t_binary",
                "t_map_string",
                "t_map_tinyint",
                "t_map_smallint",
                "t_map_int",
                "t_map_bigint",
                "t_map_float",
                "t_map_double",
                "t_map_boolean",
                "t_map_timestamp",
                "t_array_string",
                "t_array_tinyint",
                "t_array_smallint",
                "t_array_int",
                "t_array_bigint",
                "t_array_float",
                "t_array_double",
                "t_array_boolean",
                "t_array_timestamp",
                "t_complex"
        );

    protected static final String COLUMN_NAMES_STRING = Joiner.on(",").join(COLUMN_NAMES);

    public static final long TIMESTAMP = new DateTime(2011, 5, 6, 7, 8, 9, 123).getMillis();
    public static final String TIMESTAMP_STRING = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS").print(TIMESTAMP);

    // Pairs of <value-to-write-to-Hive, value-expected-from-Presto>
    @SuppressWarnings("unchecked")
    private static final List<Pair<Object, Object>>  TEST_VALUES = ImmutableList.of(
                Pair.<Object, Object>of(null, null),
                Pair.<Object, Object>of("", Slices.EMPTY_SLICE),
                Pair.<Object, Object>of("test", Slices.utf8Slice("test")),
                Pair.<Object, Object>of((byte) 1, 1L),
                Pair.<Object, Object>of((short) 2, 2L),
                Pair.<Object, Object>of(3, 3L),
                Pair.<Object, Object>of(4L, 4L),
                Pair.<Object, Object>of(5.1f, 5.1),
                Pair.<Object, Object>of(6.2, 6.2),
                Pair.<Object, Object>of(true, true),
                Pair.<Object, Object>of(new Timestamp(TIMESTAMP), TIMESTAMP),
                Pair.<Object, Object>of(Slices.utf8Slice("test2"), Slices.utf8Slice("test2")),
                Pair.<Object, Object>of(ImmutableMap.of("test", "test"), "{\"test\":\"test\"}"),
                Pair.<Object, Object>of(ImmutableMap.of((byte) 1, (byte) 1), "{\"1\":1}"),
                Pair.<Object, Object>of(ImmutableMap.of((short) 2, (short) 2), "{\"2\":2}"),
                Pair.<Object, Object>of(ImmutableMap.of(3, 3), "{\"3\":3}"),
                Pair.<Object, Object>of(ImmutableMap.of(4L, 4L), "{\"4\":4}"),
                Pair.<Object, Object>of(ImmutableMap.of(5.0f, 5.0f), "{\"5.0\":5.0}"),
                Pair.<Object, Object>of(ImmutableMap.of(6.0, 6.0), "{\"6.0\":6.0}"),
                Pair.<Object, Object>of(ImmutableMap.of(true, true), "{\"true\":true}"),
                Pair.<Object, Object>of(ImmutableMap.of(new Timestamp(TIMESTAMP), new Timestamp(TIMESTAMP)), String.format("{\"%s\":\"%s\"}", TIMESTAMP_STRING, TIMESTAMP_STRING)),
                Pair.<Object, Object>of(ImmutableList.of("test"), "[\"test\"]"),
                Pair.<Object, Object>of(ImmutableList.of((byte) 1), "[1]"),
                Pair.<Object, Object>of(ImmutableList.of((short) 2), "[2]"),
                Pair.<Object, Object>of(ImmutableList.of(3), "[3]"),
                Pair.<Object, Object>of(ImmutableList.of(4L), "[4]"),
                Pair.<Object, Object>of(ImmutableList.of(5.0f), "[5.0]"),
                Pair.<Object, Object>of(ImmutableList.of(6.0), "[6.0]"),
                Pair.<Object, Object>of(ImmutableList.of(true), "[true]"),
                Pair.<Object, Object>of(ImmutableList.of(new Timestamp(TIMESTAMP)), String.format("[\"%s\"]", TIMESTAMP_STRING)),
                Pair.<Object, Object>of(ImmutableMap.of("test", ImmutableList.<Object>of(new Integer[] {1})), "{\"test\":[{\"s_int\":1}]}")
        );

    protected List<HiveColumnHandle> getColumns()
    {
        List<HiveColumnHandle> columns = new ArrayList<>();
        for (int i = 0; i < COLUMN_NAMES.size(); i++) {
            columns.add(new HiveColumnHandle("client_id=0", COLUMN_NAMES.get(i), i, HiveType.getHiveType(FIELD_INSPECTORS.get(i)), i, false));
        }
        return columns;
    }

    public FileSplit createTestFile(String filePath, HiveOutputFormat<?, ?> outputFormat, @SuppressWarnings("deprecation") SerDe serDe, String compressionCodec)
            throws Exception
    {
        JobConf jobConf = new JobConf();
        Properties tableProperties = new Properties();
        tableProperties.setProperty("columns", COLUMN_NAMES_STRING);
        tableProperties.setProperty("columns.types", COLUMN_TYPES);
        serDe.initialize(new Configuration(), tableProperties);

        if (compressionCodec != null) {
            CompressionCodec codec = new CompressionCodecFactory(new Configuration()).getCodecByName(compressionCodec);
            jobConf.set(COMPRESS_CODEC, codec.getClass().getName());
            jobConf.set(COMPRESS_TYPE, SequenceFile.CompressionType.BLOCK.toString());
        }

        RecordWriter recordWriter = outputFormat.getHiveRecordWriter(
                jobConf,
                new Path(filePath),
                Text.class,
                compressionCodec != null,
                tableProperties,
                new Progressable()
                {
                    @Override
                    public void progress()
                    {
                    }
                }
        );

        try {
            serDe.initialize(new Configuration(), tableProperties);

            SettableStructObjectInspector objectInspector = getStandardStructObjectInspector(COLUMN_NAMES, FIELD_INSPECTORS);
            Object row = objectInspector.create();

            List<StructField> fields = ImmutableList.copyOf(objectInspector.getAllStructFieldRefs());

            for (int rowNumber = 0; rowNumber < NUM_ROWS; rowNumber++) {
                for (int i = 0; i < TEST_VALUES.size(); i++) {
                    Object key = TEST_VALUES.get(i).getKey();
                    if (key instanceof Slice) {
                        key = ((Slice) key).getBytes();
                    }
                    objectInspector.setStructFieldData(row, fields.get(i), key);
                }

                Writable record = serDe.serialize(row, objectInspector);
                recordWriter.write(record);
            }
        }
        finally {
            recordWriter.close(false);
        }

        Path path = new Path(filePath);
        path.getFileSystem(new Configuration()).setVerifyChecksum(true);
        File file = new File(filePath);
        return new FileSplit(path, 0, file.length(), new String[0]);
    }

    protected void checkCursor(RecordCursor cursor)
            throws IOException
    {
        for (int row = 0; row < NUM_ROWS; row++) {
            assertTrue(cursor.advanceNextPosition());
            assertTrue(cursor.isNull(0));
            for (int i = 1; i < TEST_VALUES.size(); i++) {
                Object fieldFromCursor;

                Type type = HiveType.getHiveType(FIELD_INSPECTORS.get(i)).getNativeType();
                if (BOOLEAN.equals(type)) {
                    fieldFromCursor = cursor.getBoolean(i);
                }
                else if (BIGINT.equals(type)) {
                    fieldFromCursor = cursor.getLong(i);
                }
                else if (DOUBLE.equals(type)) {
                    fieldFromCursor = cursor.getDouble(i);
                }
                else if (VARCHAR.equals(type)) {
                    fieldFromCursor = cursor.getSlice(i);
                }
                else if (TimestampType.TIMESTAMP.equals(type)) {
                    fieldFromCursor = cursor.getLong(i);
                }
                else {
                    throw new RuntimeException("unknown type");
                }

                if (FIELD_INSPECTORS.get(i).getTypeName().equals("float") ||
                        FIELD_INSPECTORS.get(i).getTypeName().equals("double")) {
                    assertEquals((double) fieldFromCursor, (double) TEST_VALUES.get(i).getValue(), EPSILON);
                }
                else if (FIELD_INSPECTORS.get(i).getCategory() == ObjectInspector.Category.PRIMITIVE) {
                    assertEquals(fieldFromCursor, TEST_VALUES.get(i).getValue(), String.format("Wrong value for column %d", i));
                }
                else {
                    ObjectMapper mapper = new ObjectMapper();
                    JsonNode expected = mapper.readTree((String) TEST_VALUES.get(i).getValue());
                    JsonNode actual = mapper.readTree(((Slice) fieldFromCursor).getBytes());
                    assertEquals(actual, expected, String.format("Wrong value for column %s", COLUMN_NAMES.get(i)));
                }
            }
        }
    }
}

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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import org.testng.annotations.Test;
import sun.misc.Unsafe;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

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
    protected static final List<String> COLUMN_NAMES;
    protected static final String COLUMN_NAMES_STRING;
    protected static final String COLUMN_TYPES;
    private static final List<ObjectInspector> FIELD_INSPECTORS;
    private static final int NUM_ROWS = 1000;
    private static final double EPSILON = 0.001;
    private static final Unsafe unsafe;

    // Pairs of <value-to-write-to-Hive, value-expected-from-Presto>
    private final List<Pair<Object, Object>> TEST_VALUES;

    static {
        try {
            // fetch theUnsafe object
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (Unsafe) field.get(null);
            if (unsafe == null) {
                throw new RuntimeException("Unsafe access not available");
            }

            // make sure the VM thinks bytes are only one byte wide
            if (Unsafe.ARRAY_BYTE_INDEX_SCALE != 1) {
                throw new IllegalStateException("Byte array index scale must be 1, but is " + Unsafe.ARRAY_BYTE_INDEX_SCALE);
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

        FIELD_INSPECTORS = ImmutableList.of(
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
        COLUMN_TYPES = Joiner.on(":").join(transform(FIELD_INSPECTORS, new Function<ObjectInspector, String>()
        {
            @Override
            public String apply(ObjectInspector input)
            {
                return input.getTypeName();
            }
        }));

        COLUMN_NAMES = ImmutableList.of(
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

        COLUMN_NAMES_STRING = Joiner.on(",").join(COLUMN_NAMES);
    }

    public AbstractTestHiveFileFormats()
    {
        long millis = new DateTime(2011, 5, 6, 7, 8, 9, 123).getMillis();

        TEST_VALUES = ImmutableList.of(
                Pair.<Object, Object>of(null, null),
                Pair.<Object, Object>of("", new byte[0]),
                Pair.<Object, Object>of("test", "test".getBytes(Charsets.UTF_8)),
                Pair.<Object, Object>of(Byte.valueOf((byte) 1), Long.valueOf((byte) 1)),
                Pair.<Object, Object>of(Short.valueOf((short) 2), Long.valueOf((short) 2)),
                Pair.<Object, Object>of(Integer.valueOf(3), Long.valueOf(3)),
                Pair.<Object, Object>of(Long.valueOf(4), Long.valueOf(4)),
                Pair.<Object, Object>of(Float.valueOf((float) 5.1), Double.valueOf((float) 5.1)),
                Pair.<Object, Object>of(Double.valueOf(6.2), Double.valueOf(6.2)),
                Pair.<Object, Object>of(Boolean.valueOf(true), Boolean.valueOf(true)),
                Pair.<Object, Object>of(new Timestamp(millis), Long.valueOf(millis / 1000)),
                Pair.<Object, Object>of("test2".getBytes(Charsets.UTF_8), "test2".getBytes(Charsets.UTF_8)),
                Pair.<Object, Object>of(ImmutableMap.of("test", "test"), "{\"test\":\"test\"}"),
                Pair.<Object, Object>of(ImmutableMap.of(Byte.valueOf((byte) 1), Byte.valueOf((byte) 1)), "{\"1\":1}"),
                Pair.<Object, Object>of(ImmutableMap.of(Short.valueOf((short) 2), Short.valueOf((short) 2)), "{\"2\":2}"),
                Pair.<Object, Object>of(ImmutableMap.of(Integer.valueOf(3), Integer.valueOf(3)), "{\"3\":3}"),
                Pair.<Object, Object>of(ImmutableMap.of(Long.valueOf(4), Long.valueOf(4)), "{\"4\":4}"),
                Pair.<Object, Object>of(ImmutableMap.of(Float.valueOf((float) 5.0), Float.valueOf((float) 5.0)), "{\"5.0\":5.0}"),
                Pair.<Object, Object>of(ImmutableMap.of(Double.valueOf(6.0), Double.valueOf(6.0)), "{\"6.0\":6.0}"),
                Pair.<Object, Object>of(ImmutableMap.of(Boolean.valueOf(true), Boolean.valueOf(true)), "{\"true\":true}"),
                Pair.<Object, Object>of(ImmutableMap.of(new Timestamp(millis), new Timestamp(millis)), String.format("{\"%d\":%d}", millis / 1000, millis / 1000)),
                Pair.<Object, Object>of(ImmutableList.of("test"), "[\"test\"]"),
                Pair.<Object, Object>of(ImmutableList.of(Byte.valueOf((byte) 1)), "[1]"),
                Pair.<Object, Object>of(ImmutableList.of(Short.valueOf((short) 2)), "[2]"),
                Pair.<Object, Object>of(ImmutableList.of(Integer.valueOf(3)), "[3]"),
                Pair.<Object, Object>of(ImmutableList.of(Long.valueOf(4)), "[4]"),
                Pair.<Object, Object>of(ImmutableList.of(Float.valueOf((float) 5.0)), "[5.0]"),
                Pair.<Object, Object>of(ImmutableList.of(Double.valueOf(6.0)), "[6.0]"),
                Pair.<Object, Object>of(ImmutableList.of(Boolean.valueOf(true)), "[true]"),
                Pair.<Object, Object>of(ImmutableList.of(new Timestamp(millis)), String.format("[%d]", millis / 1000)),
                Pair.<Object, Object>of(ImmutableMap.of("test", ImmutableList.<Object>of(new Integer[] {1})), "{\"test\":[{\"s_int\":1}]}")
        );
    }

    protected List<HiveColumnHandle> getColumns()
    {
        List<HiveColumnHandle> columns = new ArrayList<>();
        for (int i = 0; i < COLUMN_NAMES.size(); i++) {
            columns.add(new HiveColumnHandle("client_id=0", COLUMN_NAMES.get(i), i, HiveType.getHiveType(FIELD_INSPECTORS.get(i)), i, false));
        }
        return columns;
    }

    public FileSplit createTestFile(String filePath, HiveOutputFormat<?, ?> outputFormat, SerDe serDe, String compressionCodec)
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
                    objectInspector.setStructFieldData(row, fields.get(i), TEST_VALUES.get(i).getKey());
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

    protected void checkCursor(RecordCursor cursor, boolean primitivesOnly)
            throws IOException
    {
        for (int row = 0; row < NUM_ROWS; row++) {
            assertTrue(cursor.advanceNextPosition());
            assertTrue(cursor.isNull(0));
            for (int i = 1; i < TEST_VALUES.size(); i++) {
                Object fieldFromCursor;
                HiveType type = HiveType.getHiveType(FIELD_INSPECTORS.get(i));
                switch (type.getNativeType()) {
                    case BOOLEAN:
                        fieldFromCursor = Boolean.valueOf(cursor.getBoolean(i));
                        break;
                    case LONG:
                        fieldFromCursor = Long.valueOf(cursor.getLong(i));
                        break;
                    case DOUBLE:
                        fieldFromCursor = Double.valueOf(cursor.getDouble(i));
                        break;
                    case STRING:
                        fieldFromCursor = cursor.getString(i);
                        break;
                    default:
                        throw new RuntimeException("unknown type");
                }
                if (FIELD_INSPECTORS.get(i).getTypeName() == "float" || FIELD_INSPECTORS.get(i).getTypeName() == "double") {
                    assertEquals((double) fieldFromCursor, (double) TEST_VALUES.get(i).getValue(), EPSILON);
                }
                else if (FIELD_INSPECTORS.get(i).getCategory() == ObjectInspector.Category.PRIMITIVE) {
                    assertEquals(fieldFromCursor, TEST_VALUES.get(i).getValue(), String.format("Wrong value for column %d", i));
                }
                else if (!primitivesOnly) {
                    ObjectMapper mapper = new ObjectMapper();
                    JsonNode expected = mapper.readTree((String) TEST_VALUES.get(i).getValue());
                    JsonNode actual = mapper.readTree((byte[]) fieldFromCursor);
                    assertEquals(actual, expected, String.format("Wrong value for column %s", COLUMN_NAMES.get(i)));
                }
            }
        }
    }
}

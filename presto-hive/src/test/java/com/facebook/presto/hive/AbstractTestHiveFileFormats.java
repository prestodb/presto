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

import com.facebook.presto.operator.Operator;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.SqlDate;
import com.facebook.presto.spi.type.SqlTimestamp;
import com.facebook.presto.spi.type.SqlVarbinary;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.serde2.ReaderWriterProfiler;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
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
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import static com.facebook.presto.hive.AbstractTestHiveFileFormats.TestColumn.nameGetter;
import static com.facebook.presto.hive.AbstractTestHiveFileFormats.TestColumn.objectInspectorGetter;
import static com.facebook.presto.hive.AbstractTestHiveFileFormats.TestColumn.partitionKeyFilter;
import static com.facebook.presto.hive.AbstractTestHiveFileFormats.TestColumn.typeGetter;
import static com.facebook.presto.hive.HiveTestUtils.close;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.MaterializedResult.materializeSourceDataStream;
import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardListObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardMapObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaByteObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaDateObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaFloatObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaIntObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaLongObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaShortObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaTimestampObjectInspector;
import static org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.COMPRESS_CODEC;
import static org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.COMPRESS_TYPE;
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(groups = "hive")
public abstract class AbstractTestHiveFileFormats
{
    private static final ConnectorSession SESSION = new ConnectorSession("user", "test", "default", "default", UTC_KEY, Locale.ENGLISH, null, null);

    private static final int NUM_ROWS = 1000;
    private static final double EPSILON = 0.001;

    public static final long DATE = new DateTime(2011, 5, 6, 0, 0, UTC).getMillis();
    public static final String DATE_STRING = DateTimeFormat.forPattern("yyyy-MM-dd").withZone(UTC).print(DATE);
    public static final long TIMESTAMP = new DateTime(2011, 5, 6, 7, 8, 9, 123).getMillis();
    public static final String TIMESTAMP_STRING = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS").print(TIMESTAMP);

    // TODO: support null values and determine if timestamp and binary are allowed as partition keys
    public static final List<TestColumn> TEST_COLUMNS = ImmutableList.<TestColumn>builder()
//            .add(new TestColumn("p_null_string", javaStringObjectInspector, null, null, true))
            .add(new TestColumn("p_empty_string", javaStringObjectInspector, "", Slices.EMPTY_SLICE, true))
            .add(new TestColumn("p_string", javaStringObjectInspector, "test", Slices.utf8Slice("test"), true))
            .add(new TestColumn("p_tinyint", javaByteObjectInspector, "1", 1L, true))
            .add(new TestColumn("p_smallint", javaShortObjectInspector, "2", 2L, true))
            .add(new TestColumn("p_int", javaIntObjectInspector, "3", 3L, true))
            .add(new TestColumn("p_bigint", javaLongObjectInspector, "4", 4L, true))
            .add(new TestColumn("p_float", javaFloatObjectInspector, "5.1", 5.1, true))
            .add(new TestColumn("p_double", javaDoubleObjectInspector, "6.2", 6.2, true))
            .add(new TestColumn("p_boolean", javaBooleanObjectInspector, "true", true, true))
            .add(new TestColumn("p_date", javaDateObjectInspector, DATE_STRING, DATE, true))
//            .add(new TestColumn("p_timestamp", javaTimestampObjectInspector, TIMESTAMP_STRING, TIMESTAMP, true))
//            .add(new TestColumn("p_binary", javaByteArrayObjectInspector, "test2", Slices.utf8Slice("test2"), true))
            .add(new TestColumn("t_null_string", javaStringObjectInspector, null, null))
            .add(new TestColumn("t_null_array_int", getStandardListObjectInspector(javaIntObjectInspector), null, null))
            .add(new TestColumn("t_empty_string", javaStringObjectInspector, "", Slices.EMPTY_SLICE))
            .add(new TestColumn("t_string", javaStringObjectInspector, "test", Slices.utf8Slice("test")))
            .add(new TestColumn("t_tinyint", javaByteObjectInspector, (byte) 1, 1L))
            .add(new TestColumn("t_smallint", javaShortObjectInspector, (short) 2, 2L))
            .add(new TestColumn("t_int", javaIntObjectInspector, 3, 3L))
            .add(new TestColumn("t_bigint", javaLongObjectInspector, 4L, 4L))
            .add(new TestColumn("t_float", javaFloatObjectInspector, 5.1f, 5.1))
            .add(new TestColumn("t_double", javaDoubleObjectInspector, 6.2, 6.2))
            .add(new TestColumn("t_boolean", javaBooleanObjectInspector, true, true))
            .add(new TestColumn("t_date", javaDateObjectInspector, new Date(DATE), DATE))
            .add(new TestColumn("t_timestamp", javaTimestampObjectInspector, new Timestamp(TIMESTAMP), TIMESTAMP))
            .add(new TestColumn("t_binary", javaByteArrayObjectInspector, Slices.utf8Slice("test2"), Slices.utf8Slice("test2")))
            .add(new TestColumn("t_map_string",
                    getStandardMapObjectInspector(javaStringObjectInspector, javaStringObjectInspector),
                    ImmutableMap.of("test", "test"),
                    "{\"test\":\"test\"}"))
            .add(new TestColumn("t_map_tinyint", getStandardMapObjectInspector(javaByteObjectInspector, javaByteObjectInspector), ImmutableMap.of((byte) 1, (byte) 1), "{\"1\":1}"))
            .add(new TestColumn("t_map_smallint",
                    getStandardMapObjectInspector(javaShortObjectInspector, javaShortObjectInspector),
                    ImmutableMap.of((short) 2, (short) 2),
                    "{\"2\":2}"))
            .add(new TestColumn("t_map_int", getStandardMapObjectInspector(javaIntObjectInspector, javaIntObjectInspector), ImmutableMap.of(3, 3), "{\"3\":3}"))
            .add(new TestColumn("t_map_bigint", getStandardMapObjectInspector(javaLongObjectInspector, javaLongObjectInspector), ImmutableMap.of(4L, 4L), "{\"4\":4}"))
            .add(new TestColumn("t_map_float", getStandardMapObjectInspector(javaFloatObjectInspector, javaFloatObjectInspector), ImmutableMap.of(5.0f, 5.0f), "{\"5.0\":5.0}"))
            .add(new TestColumn("t_map_double", getStandardMapObjectInspector(javaDoubleObjectInspector, javaDoubleObjectInspector), ImmutableMap.of(6.0, 6.0), "{\"6.0\":6.0}"))
            .add(new TestColumn("t_map_boolean",
                    getStandardMapObjectInspector(javaBooleanObjectInspector, javaBooleanObjectInspector),
                    ImmutableMap.of(true, true),
                    "{\"true\":true}"))
            .add(new TestColumn("t_map_date",
                    getStandardMapObjectInspector(javaDateObjectInspector, javaDateObjectInspector),
                    ImmutableMap.of(new Date(DATE), new Date(DATE)),
                    String.format("{\"%s\":\"%s\"}", DATE_STRING, DATE_STRING)))
            .add(new TestColumn("t_map_timestamp",
                    getStandardMapObjectInspector(javaTimestampObjectInspector, javaTimestampObjectInspector),
                    ImmutableMap.of(new Timestamp(TIMESTAMP), new Timestamp(TIMESTAMP)),
                    String.format("{\"%s\":\"%s\"}", TIMESTAMP_STRING, TIMESTAMP_STRING)))
            .add(new TestColumn("t_array_string", getStandardListObjectInspector(javaStringObjectInspector), ImmutableList.of("test"), "[\"test\"]"))
            .add(new TestColumn("t_array_tinyint", getStandardListObjectInspector(javaByteObjectInspector), ImmutableList.of((byte) 1), "[1]"))
            .add(new TestColumn("t_array_smallint", getStandardListObjectInspector(javaShortObjectInspector), ImmutableList.of((short) 2), "[2]"))
            .add(new TestColumn("t_array_int", getStandardListObjectInspector(javaIntObjectInspector), ImmutableList.of(3), "[3]"))
            .add(new TestColumn("t_array_bigint", getStandardListObjectInspector(javaLongObjectInspector), ImmutableList.of(4L), "[4]"))
            .add(new TestColumn("t_array_float", getStandardListObjectInspector(javaFloatObjectInspector), ImmutableList.of(5.0f), "[5.0]"))
            .add(new TestColumn("t_array_double", getStandardListObjectInspector(javaDoubleObjectInspector), ImmutableList.of(6.0), "[6.0]"))
            .add(new TestColumn("t_array_boolean", getStandardListObjectInspector(javaBooleanObjectInspector), ImmutableList.of(true), "[true]"))
            .add(new TestColumn("t_array_date",
                    getStandardListObjectInspector(javaDateObjectInspector),
                    ImmutableList.of(new Date(DATE)),
                    String.format("[\"%s\"]", DATE_STRING)))
            .add(new TestColumn("t_array_timestamp",
                    getStandardListObjectInspector(javaTimestampObjectInspector),
                    ImmutableList.of(new Timestamp(TIMESTAMP)),
                    String.format("[\"%s\"]", TIMESTAMP_STRING)))
            .add(new TestColumn("t_complex",
                    getStandardMapObjectInspector(
                            javaStringObjectInspector,
                            getStandardListObjectInspector(
                                    getStandardStructObjectInspector(
                                            ImmutableList.of("s_int"),
                                            ImmutableList.<ObjectInspector>of(javaIntObjectInspector)
                                    )
                            )
                    ),
                    ImmutableMap.of("test", ImmutableList.<Object>of(new Integer[] {1})),
                    "{\"test\":[{\"s_int\":1}]}"
            ))
            .build();

    protected List<HiveColumnHandle> getColumnHandles(List<TestColumn> testColumns)
    {
        List<HiveColumnHandle> columns = new ArrayList<>();
        int nextHiveColumnIndex = 0;
        for (int i = 0; i < testColumns.size(); i++) {
            TestColumn testColumn = testColumns.get(i);
            int columnIndex = testColumn.isPartitionKey() ? -1 : nextHiveColumnIndex++;
            columns.add(new HiveColumnHandle("client_id", testColumn.getName(), i, HiveType.getHiveType(testColumn.getObjectInspector()), columnIndex, testColumn.isPartitionKey()));
        }
        return columns;
    }

    public FileSplit createTestFile(String filePath, HiveOutputFormat<?, ?> outputFormat, @SuppressWarnings("deprecation") SerDe serDe, String compressionCodec, List<TestColumn> testColumns)
            throws Exception
    {
        // filter out partition keys, which are not written to the file
        testColumns = ImmutableList.copyOf(filter(testColumns, not(partitionKeyFilter())));

        JobConf jobConf = new JobConf();
        ReaderWriterProfiler.setProfilerOptions(jobConf);

        Properties tableProperties = new Properties();
        tableProperties.setProperty("columns", Joiner.on(',').join(transform(testColumns, nameGetter())));
        tableProperties.setProperty("columns.types", Joiner.on(',').join(transform(testColumns, typeGetter())));
        serDe.initialize(new Configuration(), tableProperties);

        if (compressionCodec != null) {
            CompressionCodec codec = new CompressionCodecFactory(new Configuration()).getCodecByName(compressionCodec);
            jobConf.set(COMPRESS_CODEC, codec.getClass().getName());
            jobConf.set(COMPRESS_TYPE, SequenceFile.CompressionType.BLOCK.toString());
            jobConf.set("parquet.compression", compressionCodec);
            jobConf.set("parquet.enable.dictionary", "true");
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

            SettableStructObjectInspector objectInspector = getStandardStructObjectInspector(
                    ImmutableList.copyOf(transform(testColumns, nameGetter())),
                    ImmutableList.copyOf(transform(testColumns, objectInspectorGetter())));

            Object row = objectInspector.create();

            List<StructField> fields = ImmutableList.copyOf(objectInspector.getAllStructFieldRefs());

            for (int rowNumber = 0; rowNumber < NUM_ROWS; rowNumber++) {
                for (int i = 0; i < testColumns.size(); i++) {
                    Object writeValue = testColumns.get(i).getWriteValue();
                    if (writeValue instanceof Slice) {
                        writeValue = ((Slice) writeValue).getBytes();
                    }
                    objectInspector.setStructFieldData(row, fields.get(i), writeValue);
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

    protected void checkCursor(RecordCursor cursor, List<TestColumn> testColumns)
            throws IOException
    {
        for (int row = 0; row < NUM_ROWS; row++) {
            assertTrue(cursor.advanceNextPosition());
            for (int i = 0, testColumnsSize = testColumns.size(); i < testColumnsSize; i++) {
                TestColumn testColumn = testColumns.get(i);

                Object fieldFromCursor;
                Type type = HiveType.getHiveType(testColumn.getObjectInspector()).getNativeType();
                if (cursor.isNull(i)) {
                    fieldFromCursor = null;
                }
                else if (BOOLEAN.equals(type)) {
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
                else if (VARBINARY.equals(type)) {
                    fieldFromCursor = cursor.getSlice(i);
                }
                else if (DateType.DATE.equals(type)) {
                    fieldFromCursor = cursor.getLong(i);
                }
                else if (TimestampType.TIMESTAMP.equals(type)) {
                    fieldFromCursor = cursor.getLong(i);
                }
                else {
                    throw new RuntimeException("unknown type");
                }

                if (fieldFromCursor == null) {
                    assertEquals(null, testColumn.getExpectedValue(), String.format("Expected null for column %d", i));
                }
                else if (testColumn.getObjectInspector().getTypeName().equals("float") ||
                        testColumn.getObjectInspector().getTypeName().equals("double")) {
                    assertEquals((double) fieldFromCursor, (double) testColumn.getExpectedValue(), EPSILON);
                }
                else if (testColumn.getObjectInspector().getCategory() == Category.PRIMITIVE) {
                    assertEquals(fieldFromCursor, testColumn.getExpectedValue(), String.format("Wrong value for column %d", i));
                }
                else {
                    ObjectMapper mapper = new ObjectMapper();
                    JsonNode expected = mapper.readTree((String) testColumn.getExpectedValue());
                    JsonNode actual = mapper.readTree(((Slice) fieldFromCursor).getBytes());
                    assertEquals(actual, expected, String.format("Wrong value for column %s", testColumn.getName()));
                }
            }
        }
    }

    protected void checkDataStream(Operator dataStream, List<TestColumn> testColumns)
            throws IOException
    {
        try {
            MaterializedResult result = materializeSourceDataStream(SESSION, dataStream);

            for (MaterializedRow row : result) {
                for (int i = 0, testColumnsSize = testColumns.size(); i < testColumnsSize; i++) {
                    TestColumn testColumn = testColumns.get(i);

                    Object actualValue = row.getField(i);
                    Object expectedValue = testColumn.getExpectedValue();
                    if (expectedValue instanceof Slice) {
                        expectedValue = ((Slice) expectedValue).toStringUtf8();
                    }

                    if (actualValue == null) {
                        assertEquals(null, expectedValue, String.format("Expected null for column %d", i));
                    }
                    else if (testColumn.getObjectInspector().getTypeName().equals("float") ||
                            testColumn.getObjectInspector().getTypeName().equals("double")) {
                        assertEquals((double) actualValue, (double) expectedValue, EPSILON);
                    }
                    else if (testColumn.getObjectInspector().getTypeName().equals("date")) {
                        SqlDate expectedDate = new SqlDate((Long) expectedValue, SESSION.getTimeZoneKey());
                        assertEquals(actualValue, expectedDate);
                    }
                    else if (testColumn.getObjectInspector().getTypeName().equals("timestamp")) {
                        SqlTimestamp expectedTimestamp = new SqlTimestamp((Long) expectedValue, SESSION.getTimeZoneKey());
                        assertEquals(actualValue, expectedTimestamp);
                    }
                    else if (testColumn.getObjectInspector().getCategory() == Category.PRIMITIVE) {
                        if (actualValue instanceof Slice) {
                            actualValue = ((Slice) actualValue).toStringUtf8();
                        }
                        if (actualValue instanceof SqlVarbinary) {
                            actualValue = new String(((SqlVarbinary) actualValue).getBytes(), UTF_8);
                        }
                        assertEquals(actualValue, expectedValue, String.format("Wrong value for column %d", i));
                    }
                    else {
                        ObjectMapper mapper = new ObjectMapper();
                        JsonNode expected = mapper.readTree((String) expectedValue);
                        JsonNode actual = mapper.readTree(((String) actualValue));
                        assertEquals(actual, expected, String.format("Wrong value for column %s", testColumn.getName()));
                    }
                }
            }
        }
        finally {
            close(dataStream);
        }
    }

    public static final class TestColumn
    {
        private final String name;
        private final ObjectInspector objectInspector;
        private final Object writeValue;
        private final Object expectedValue;
        private final boolean partitionKey;

        public TestColumn(String name, ObjectInspector objectInspector, Object writeValue, Object expectedValue)
        {
            this(name, objectInspector, writeValue, expectedValue, false);
        }

        public TestColumn(String name, ObjectInspector objectInspector, Object writeValue, Object expectedValue, boolean partitionKey)
        {
            this.name = checkNotNull(name, "name is null");
            this.objectInspector = checkNotNull(objectInspector, "objectInspector is null");
            this.writeValue = writeValue;
            this.expectedValue = expectedValue;
            this.partitionKey = partitionKey;
        }

        public String getName()
        {
            return name;
        }

        public String getType()
        {
            return objectInspector.getTypeName();
        }

        public ObjectInspector getObjectInspector()
        {
            return objectInspector;
        }

        public Object getWriteValue()
        {
            return writeValue;
        }

        public Object getExpectedValue()
        {
            return expectedValue;
        }

        public boolean isPartitionKey()
        {
            return partitionKey;
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder("TestColumn{");
            sb.append("name='").append(name).append('\'');
            sb.append(", objectInspector=").append(objectInspector);
            sb.append(", writeValue=").append(writeValue);
            sb.append(", expectedValue=").append(expectedValue);
            sb.append(", partitionKey=").append(partitionKey);
            sb.append('}');
            return sb.toString();
        }

        public static Function<TestColumn, String> nameGetter()
        {
            return new Function<TestColumn, String>()
            {
                @Override
                public String apply(TestColumn input)
                {
                    return input.getName();
                }
            };
        }

        public static Function<TestColumn, String> typeGetter()
        {
            return new Function<TestColumn, String>()
            {
                @Override
                public String apply(TestColumn input)
                {
                    return input.getType();
                }
            };
        }

        public static Predicate<TestColumn> partitionKeyFilter()
        {
            return new Predicate<TestColumn>()
            {
                @Override
                public boolean apply(TestColumn input)
                {
                    return input.isPartitionKey();
                }
            };
        }

        public static Function<TestColumn, ObjectInspector> objectInspectorGetter()
        {
            return new Function<TestColumn, ObjectInspector>()
            {
                @Override
                public ObjectInspector apply(TestColumn input)
                {
                    return input.getObjectInspector();
                }
            };
        }
    }
}

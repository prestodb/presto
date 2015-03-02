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

import com.facebook.presto.block.BlockSerdeUtil;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.SqlDate;
import com.facebook.presto.spi.type.SqlTimestamp;
import com.facebook.presto.spi.type.SqlVarbinary;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.MapType;
import com.facebook.presto.type.RowType;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
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
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.hive.HivePartitionKey.HIVE_DEFAULT_DYNAMIC_PARTITION;
import static com.facebook.presto.hive.HiveTestUtils.SESSION;
import static com.facebook.presto.hive.HiveUtil.isStructuralType;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.MaterializedResult.materializeSourceDataStream;
import static com.facebook.presto.tests.StructuralTestUtil.arrayBlockOf;
import static com.facebook.presto.tests.StructuralTestUtil.mapBlockOf;
import static com.facebook.presto.tests.StructuralTestUtil.rowBlockOf;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
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
    private static final double EPSILON = 0.001;
    private static final TypeManager TYPE_MANAGER = new TypeRegistry();

    private static final long DATE_MILLIS_UTC = new DateTime(2011, 5, 6, 0, 0, UTC).getMillis();
    private static final long DATE_DAYS = TimeUnit.MILLISECONDS.toDays(DATE_MILLIS_UTC);
    private static final String DATE_STRING = DateTimeFormat.forPattern("yyyy-MM-dd").withZoneUTC().print(DATE_MILLIS_UTC);
    private static final Date SQL_DATE = new Date(UTC.getMillisKeepLocal(DateTimeZone.getDefault(), DATE_MILLIS_UTC));

    public static final long TIMESTAMP = new DateTime(2011, 5, 6, 7, 8, 9, 123).getMillis();
    public static final String TIMESTAMP_STRING = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS").print(TIMESTAMP);

    // TODO: support null values and determine if timestamp and binary are allowed as partition keys
    public static final int NUM_ROWS = 1000;
    public static final List<TestColumn> TEST_COLUMNS = ImmutableList.<TestColumn>builder()
            .add(new TestColumn("p_empty_string", javaStringObjectInspector, "", Slices.EMPTY_SLICE, true))
            .add(new TestColumn("p_string", javaStringObjectInspector, "test", Slices.utf8Slice("test"), true))
            .add(new TestColumn("p_tinyint", javaByteObjectInspector, "1", 1L, true))
            .add(new TestColumn("p_smallint", javaShortObjectInspector, "2", 2L, true))
            .add(new TestColumn("p_int", javaIntObjectInspector, "3", 3L, true))
            .add(new TestColumn("p_bigint", javaLongObjectInspector, "4", 4L, true))
            .add(new TestColumn("p_float", javaFloatObjectInspector, "5.1", 5.1, true))
            .add(new TestColumn("p_double", javaDoubleObjectInspector, "6.2", 6.2, true))
            .add(new TestColumn("p_boolean", javaBooleanObjectInspector, "true", true, true))
            .add(new TestColumn("p_date", javaDateObjectInspector, DATE_STRING, DATE_DAYS, true))
            .add(new TestColumn("p_timestamp", javaTimestampObjectInspector, TIMESTAMP_STRING, TIMESTAMP, true))
//            .add(new TestColumn("p_binary", javaByteArrayObjectInspector, "test2", Slices.utf8Slice("test2"), true))
            .add(new TestColumn("p_null_string", javaStringObjectInspector, HIVE_DEFAULT_DYNAMIC_PARTITION, null, true))
            .add(new TestColumn("p_null_tinyint", javaByteObjectInspector, HIVE_DEFAULT_DYNAMIC_PARTITION, null, true))
            .add(new TestColumn("p_null_smallint", javaShortObjectInspector, HIVE_DEFAULT_DYNAMIC_PARTITION, null, true))
            .add(new TestColumn("p_null_int", javaIntObjectInspector, HIVE_DEFAULT_DYNAMIC_PARTITION, null, true))
            .add(new TestColumn("p_null_bigint", javaLongObjectInspector, HIVE_DEFAULT_DYNAMIC_PARTITION, null, true))
            .add(new TestColumn("p_null_float", javaFloatObjectInspector, HIVE_DEFAULT_DYNAMIC_PARTITION, null, true))
            .add(new TestColumn("p_null_double", javaDoubleObjectInspector, HIVE_DEFAULT_DYNAMIC_PARTITION, null, true))
            .add(new TestColumn("p_null_boolean", javaBooleanObjectInspector, HIVE_DEFAULT_DYNAMIC_PARTITION, null, true))
            .add(new TestColumn("p_null_date", javaDateObjectInspector, HIVE_DEFAULT_DYNAMIC_PARTITION, null, true))
            .add(new TestColumn("p_null_timestamp", javaTimestampObjectInspector, HIVE_DEFAULT_DYNAMIC_PARTITION, null, true))
//            .add(new TestColumn("p_null_binary", javaByteArrayObjectInspector, HIVE_DEFAULT_DYNAMIC_PARTITION, null, true))
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
            .add(new TestColumn("t_boolean_true", javaBooleanObjectInspector, true, true))
            .add(new TestColumn("t_boolean_false", javaBooleanObjectInspector, false, false))
            .add(new TestColumn("t_date", javaDateObjectInspector, SQL_DATE, DATE_DAYS))
            .add(new TestColumn("t_timestamp", javaTimestampObjectInspector, new Timestamp(TIMESTAMP), TIMESTAMP))
            .add(new TestColumn("t_binary", javaByteArrayObjectInspector, Slices.utf8Slice("test2"), Slices.utf8Slice("test2")))
            .add(new TestColumn("t_map_string",
                    getStandardMapObjectInspector(javaStringObjectInspector, javaStringObjectInspector),
                    ImmutableMap.of("test", "test"),
                    mapBlockOf(VARCHAR, VARCHAR, "test", "test")))
            .add(new TestColumn("t_map_tinyint", getStandardMapObjectInspector(javaByteObjectInspector, javaByteObjectInspector), ImmutableMap.of((byte) 1, (byte) 1), mapBlockOf(BIGINT, BIGINT, 1, 1)))
            .add(new TestColumn("t_map_smallint",
                    getStandardMapObjectInspector(javaShortObjectInspector, javaShortObjectInspector),
                    ImmutableMap.of((short) 2, (short) 2),
                    mapBlockOf(BIGINT, BIGINT, 2, 2)))
            .add(new TestColumn("t_map_null_key", getStandardMapObjectInspector(javaIntObjectInspector, javaIntObjectInspector), asMap(null, 0, 2, 3), mapBlockOf(BIGINT, BIGINT, 2, 3)))
            .add(new TestColumn("t_map_int", getStandardMapObjectInspector(javaIntObjectInspector, javaIntObjectInspector), ImmutableMap.of(3, 3), mapBlockOf(BIGINT, BIGINT, 3, 3)))
            .add(new TestColumn("t_map_bigint", getStandardMapObjectInspector(javaLongObjectInspector, javaLongObjectInspector), ImmutableMap.of(4L, 4L), mapBlockOf(BIGINT, BIGINT, 4L, 4L)))
            .add(new TestColumn("t_map_float", getStandardMapObjectInspector(javaFloatObjectInspector, javaFloatObjectInspector), ImmutableMap.of(5.0f, 5.0f), mapBlockOf(DOUBLE, DOUBLE, 5.0f, 5.0f)))
            .add(new TestColumn("t_map_double", getStandardMapObjectInspector(javaDoubleObjectInspector, javaDoubleObjectInspector), ImmutableMap.of(6.0, 6.0), mapBlockOf(DOUBLE, DOUBLE, 6.0, 6.0)))
            .add(new TestColumn("t_map_boolean",
                    getStandardMapObjectInspector(javaBooleanObjectInspector, javaBooleanObjectInspector),
                    ImmutableMap.of(true, true),
                    mapBlockOf(BOOLEAN, BOOLEAN, true, true)))
            .add(new TestColumn("t_map_date",
                    getStandardMapObjectInspector(javaDateObjectInspector, javaDateObjectInspector),
                    ImmutableMap.of(SQL_DATE, SQL_DATE),
                    mapBlockOf(DateType.DATE, DateType.DATE, DATE_DAYS, DATE_DAYS)))
            .add(new TestColumn("t_map_timestamp",
                    getStandardMapObjectInspector(javaTimestampObjectInspector, javaTimestampObjectInspector),
                    ImmutableMap.of(new Timestamp(TIMESTAMP), new Timestamp(TIMESTAMP)),
                    mapBlockOf(TimestampType.TIMESTAMP, TimestampType.TIMESTAMP, TIMESTAMP, TIMESTAMP)))
            .add(new TestColumn("t_array_empty", getStandardListObjectInspector(javaStringObjectInspector), ImmutableList.of(), arrayBlockOf(VARCHAR)))
            .add(new TestColumn("t_array_string", getStandardListObjectInspector(javaStringObjectInspector), ImmutableList.of("test"), arrayBlockOf(VARCHAR, "test")))
            .add(new TestColumn("t_array_tinyint", getStandardListObjectInspector(javaByteObjectInspector), ImmutableList.of((byte) 1), arrayBlockOf(BIGINT, 1)))
            .add(new TestColumn("t_array_smallint", getStandardListObjectInspector(javaShortObjectInspector), ImmutableList.of((short) 2), arrayBlockOf(BIGINT, 2)))
            .add(new TestColumn("t_array_int", getStandardListObjectInspector(javaIntObjectInspector), ImmutableList.of(3), arrayBlockOf(BIGINT, 3)))
            .add(new TestColumn("t_array_bigint", getStandardListObjectInspector(javaLongObjectInspector), ImmutableList.of(4L), arrayBlockOf(BIGINT, 4L)))
            .add(new TestColumn("t_array_float", getStandardListObjectInspector(javaFloatObjectInspector), ImmutableList.of(5.0f), arrayBlockOf(DOUBLE, 5.0f)))
            .add(new TestColumn("t_array_double", getStandardListObjectInspector(javaDoubleObjectInspector), ImmutableList.of(6.0), arrayBlockOf(DOUBLE, 6.0)))
            .add(new TestColumn("t_array_boolean", getStandardListObjectInspector(javaBooleanObjectInspector), ImmutableList.of(true), arrayBlockOf(BOOLEAN, true)))
            .add(new TestColumn("t_array_date",
                    getStandardListObjectInspector(javaDateObjectInspector),
                    ImmutableList.of(SQL_DATE),
                    arrayBlockOf(DateType.DATE, DATE_DAYS)))
            .add(new TestColumn("t_array_timestamp",
                    getStandardListObjectInspector(javaTimestampObjectInspector),
                    ImmutableList.of(new Timestamp(TIMESTAMP)),
                    arrayBlockOf(TimestampType.TIMESTAMP, TIMESTAMP)))
            .add(new TestColumn("t_struct_bigint",
                    getStandardStructObjectInspector(ImmutableList.of("s_bigint"), ImmutableList.of(javaLongObjectInspector)),
                    new Long[] {1L},
                    rowBlockOf(ImmutableList.of(BIGINT), 1)))
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
                    mapBlockOf(VARCHAR, new ArrayType(new RowType(ImmutableList.of(BIGINT), Optional.empty())),
                            "test", arrayBlockOf(new RowType(ImmutableList.of(BIGINT), Optional.empty()), rowBlockOf(ImmutableList.of(BIGINT), 1)))
            ))
            .add(new TestColumn("t_map_null_key_complex_value",
                    getStandardMapObjectInspector(
                            javaStringObjectInspector,
                            getStandardMapObjectInspector(javaLongObjectInspector, javaBooleanObjectInspector)
                    ),
                    asMap(null, ImmutableMap.of(15L, true), "k", ImmutableMap.of(16L, false)),
                    mapBlockOf(VARCHAR, new MapType(BIGINT, BOOLEAN), "k", mapBlockOf(BIGINT, BOOLEAN, 16L, false))))
            .add(new TestColumn("t_map_null_key_complex_key_value",
                    getStandardMapObjectInspector(
                            getStandardListObjectInspector(javaStringObjectInspector),
                            getStandardMapObjectInspector(javaLongObjectInspector, javaBooleanObjectInspector)
                    ),
                    asMap(null, ImmutableMap.of(15L, true), ImmutableList.of("k", "ka"), ImmutableMap.of(16L, false)),
                    mapBlockOf(new ArrayType(VARCHAR), new MapType(BIGINT, BOOLEAN), arrayBlockOf(VARCHAR, "k", "ka"), mapBlockOf(BIGINT, BOOLEAN, 16L, false))))
            .add(new TestColumn("t_struct_nested", getStandardStructObjectInspector(ImmutableList.of("struct_field"),
                    ImmutableList.of(getStandardListObjectInspector(javaStringObjectInspector))), ImmutableList.of(ImmutableList.of("1", "2", "3")), rowBlockOf(ImmutableList.of(new ArrayType(VARCHAR)), arrayBlockOf(VARCHAR, "1", "2", "3"))))
            .add(new TestColumn("t_struct_null", getStandardStructObjectInspector(ImmutableList.of("struct_field", "struct_field2"),
                    ImmutableList.of(javaStringObjectInspector, javaStringObjectInspector)), Arrays.asList(null, null), rowBlockOf(ImmutableList.of(VARCHAR, VARCHAR), null, null)))
            .add(new TestColumn("t_struct_non_nulls_after_nulls", getStandardStructObjectInspector(ImmutableList.of("struct_field1", "struct_field2"),
                    ImmutableList.of(javaIntObjectInspector, javaStringObjectInspector)), Arrays.asList(null, "some string"), rowBlockOf(ImmutableList.of(BIGINT, VARCHAR), null, "some string")))
            .add(new TestColumn("t_nested_struct_non_nulls_after_nulls",
                    getStandardStructObjectInspector(
                            ImmutableList.of("struct_field1", "struct_field2", "strict_field3"),
                            ImmutableList.of(
                                    javaIntObjectInspector,
                                    javaStringObjectInspector,
                                    getStandardStructObjectInspector(
                                            ImmutableList.of("nested_struct_field1", "nested_struct_field2"),
                                            ImmutableList.of(javaIntObjectInspector, javaStringObjectInspector)
                                    )
                            )
                    ),
                    Arrays.asList(null, "some string", Arrays.asList(null, "nested_string2")),
                    rowBlockOf(
                            ImmutableList.of(
                                    BIGINT,
                                    VARCHAR,
                                    new RowType(ImmutableList.of(BIGINT, VARCHAR), Optional.empty())
                            ),
                            null, "some string", rowBlockOf(ImmutableList.of(BIGINT, VARCHAR), null, "nested_string2")
                    )
            ))
            .add(new TestColumn("t_map_null_value",
                    getStandardMapObjectInspector(javaStringObjectInspector, javaStringObjectInspector),
                    asMap("k1", null, "k2", "v2"),
                    mapBlockOf(VARCHAR, VARCHAR, new String[] {"k1", "k2"}, new String[] {null, "v2"})))
            .build();

    private static <K, V> Map<K, V> asMap(K k1, V v1, K k2, V v2)
    {
        Map<K, V> map = new HashMap<>();
        map.put(k1, v1);
        map.put(k2, v2);
        return map;
    }

    protected List<HiveColumnHandle> getColumnHandles(List<TestColumn> testColumns)
    {
        List<HiveColumnHandle> columns = new ArrayList<>();
        int nextHiveColumnIndex = 0;
        for (int i = 0; i < testColumns.size(); i++) {
            TestColumn testColumn = testColumns.get(i);
            int columnIndex = testColumn.isPartitionKey() ? -1 : nextHiveColumnIndex++;

            HiveType hiveType = HiveType.valueOf(testColumn.getObjectInspector().getTypeName());
            columns.add(new HiveColumnHandle("client_id", testColumn.getName(), hiveType, hiveType.getTypeSignature(), columnIndex, testColumn.isPartitionKey()));
        }
        return columns;
    }

    public static FileSplit createTestFile(String filePath,
            HiveOutputFormat<?, ?> outputFormat,
            @SuppressWarnings("deprecation") SerDe serDe,
            String compressionCodec,
            List<TestColumn> testColumns,
            int numRows)
            throws Exception
    {
        // filter out partition keys, which are not written to the file
        testColumns = ImmutableList.copyOf(filter(testColumns, not(TestColumn::isPartitionKey)));

        JobConf jobConf = new JobConf();
        ReaderWriterProfiler.setProfilerOptions(jobConf);

        Properties tableProperties = new Properties();
        tableProperties.setProperty("columns", Joiner.on(',').join(transform(testColumns, TestColumn::getName)));
        tableProperties.setProperty("columns.types", Joiner.on(',').join(transform(testColumns, TestColumn::getType)));
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
                () -> { }
        );

        try {
            serDe.initialize(new Configuration(), tableProperties);

            SettableStructObjectInspector objectInspector = getStandardStructObjectInspector(
                    ImmutableList.copyOf(transform(testColumns, TestColumn::getName)),
                    ImmutableList.copyOf(transform(testColumns, TestColumn::getObjectInspector)));

            Object row = objectInspector.create();

            List<StructField> fields = ImmutableList.copyOf(objectInspector.getAllStructFieldRefs());

            for (int rowNumber = 0; rowNumber < numRows; rowNumber++) {
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

    protected void checkCursor(RecordCursor cursor, List<TestColumn> testColumns, int numRows)
            throws IOException
    {
        for (int row = 0; row < numRows; row++) {
            assertTrue(cursor.advanceNextPosition());
            for (int i = 0, testColumnsSize = testColumns.size(); i < testColumnsSize; i++) {
                TestColumn testColumn = testColumns.get(i);

                Object fieldFromCursor;
                Type type = HiveType.valueOf(testColumn.getObjectInspector().getTypeName()).getType(TYPE_MANAGER);
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
                else if (isStructuralType(type)) {
                    fieldFromCursor = cursor.getObject(i);
                }
                else {
                    throw new RuntimeException("unknown type");
                }

                if (fieldFromCursor == null) {
                    assertEquals(null, testColumn.getExpectedValue(), String.format("Expected null for column %s", testColumn.getName()));
                }
                else if (testColumn.getObjectInspector().getTypeName().equals("float") ||
                        testColumn.getObjectInspector().getTypeName().equals("double")) {
                    assertEquals((double) fieldFromCursor, (double) testColumn.getExpectedValue(), EPSILON);
                }
                else if (testColumn.getObjectInspector().getCategory() == Category.PRIMITIVE) {
                    assertEquals(fieldFromCursor, testColumn.getExpectedValue(), String.format("Wrong value for column %s", testColumn.getName()));
                }
                else {
                    Block expected = (Block) testColumn.getExpectedValue();
                    Block actual = (Block) fieldFromCursor;
                    assertBlockEquals(actual, expected, String.format("Wrong value for column %s", testColumn.getName()));
                }
            }
        }
    }

    protected void checkPageSource(ConnectorPageSource pageSource, List<TestColumn> testColumns, List<Type> types)
            throws IOException
    {
        try {
            MaterializedResult result = materializeSourceDataStream(SESSION, pageSource, types);

            for (MaterializedRow row : result) {
                for (int i = 0, testColumnsSize = testColumns.size(); i < testColumnsSize; i++) {
                    TestColumn testColumn = testColumns.get(i);
                    Type type = types.get(i);

                    Object actualValue = row.getField(i);
                    Object expectedValue = testColumn.getExpectedValue();

                    if (expectedValue instanceof Slice) {
                        expectedValue = ((Slice) expectedValue).toStringUtf8();
                    }

                    if (actualValue == null || expectedValue == null) {
                        assertEquals(actualValue, expectedValue, "Wrong value for column " + testColumn.getName());
                    }
                    else if (testColumn.getObjectInspector().getTypeName().equals("float") ||
                            testColumn.getObjectInspector().getTypeName().equals("double")) {
                        assertEquals((double) actualValue, (double) expectedValue, EPSILON, "Wrong value for column " + testColumn.getName());
                    }
                    else if (testColumn.getObjectInspector().getTypeName().equals("date")) {
                        SqlDate expectedDate = new SqlDate(((Long) expectedValue).intValue());
                        assertEquals(actualValue, expectedDate, "Wrong value for column " + testColumn.getName());
                    }
                    else if (testColumn.getObjectInspector().getTypeName().equals("timestamp")) {
                        SqlTimestamp expectedTimestamp = new SqlTimestamp((Long) expectedValue, SESSION.getTimeZoneKey());
                        assertEquals(actualValue, expectedTimestamp, "Wrong value for column " + testColumn.getName());
                    }
                    else if (testColumn.getObjectInspector().getCategory() == Category.PRIMITIVE) {
                        if (expectedValue instanceof Slice) {
                            expectedValue = ((Slice) expectedValue).toStringUtf8();
                        }

                        if (actualValue instanceof Slice) {
                            actualValue = ((Slice) actualValue).toStringUtf8();
                        }
                        if (actualValue instanceof SqlVarbinary) {
                            actualValue = new String(((SqlVarbinary) actualValue).getBytes(), UTF_8);
                        }
                        assertEquals(actualValue, expectedValue, "Wrong value for column " + testColumn.getName());
                    }
                    else {
                        BlockBuilder builder = type.createBlockBuilder(new BlockBuilderStatus(), 1);
                        type.writeObject(builder, expectedValue);
                        expectedValue = type.getObjectValue(SESSION, builder.build(), 0);
                        assertEquals(actualValue, expectedValue, "Wrong value for column " + testColumn.getName());
                    }
                }
            }
        }
        finally {
            pageSource.close();
        }
    }

    private static void assertBlockEquals(Block actual, Block expected, String message)
    {
        assertEquals(blockToSlice(actual), blockToSlice(expected), message);
    }

    private static Slice blockToSlice(Block block)
    {
        // This function is strictly for testing use only
        SliceOutput sliceOutput = new DynamicSliceOutput(1000);
        BlockSerdeUtil.writeBlock(sliceOutput, block.copyRegion(0, block.getPositionCount()));
        return sliceOutput.slice();
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
            this.name = requireNonNull(name, "name is null");
            this.objectInspector = requireNonNull(objectInspector, "objectInspector is null");
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
    }
}

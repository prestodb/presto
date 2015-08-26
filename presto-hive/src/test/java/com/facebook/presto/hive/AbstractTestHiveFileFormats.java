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

import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.LongDecimalType;
import com.facebook.presto.spi.type.SqlDate;
import com.facebook.presto.spi.type.SqlDecimal;
import com.facebook.presto.spi.type.SqlTimestamp;
import com.facebook.presto.spi.type.SqlVarbinary;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.serde2.ReaderWriterProfiler;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaHiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.hive.HivePartitionKey.HIVE_DEFAULT_DYNAMIC_PARTITION;
import static com.facebook.presto.hive.HiveTestUtils.arraySliceOf;
import static com.facebook.presto.hive.HiveTestUtils.decimalArraySliceOf;
import static com.facebook.presto.hive.HiveTestUtils.decimalMapSliceOf;
import static com.facebook.presto.hive.HiveTestUtils.mapSliceOf;
import static com.facebook.presto.hive.HiveTestUtils.rowSliceOf;
import static com.facebook.presto.hive.HiveType.getType;
import static com.facebook.presto.hive.HiveUtil.isStructuralType;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.MaterializedResult.materializeSourceDataStream;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;
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
    private static final ConnectorSession SESSION = new ConnectorSession("user", UTC_KEY, ENGLISH, System.currentTimeMillis(), null);

    private static final double EPSILON = 0.001;
    private static final TypeManager TYPE_MANAGER = new TypeRegistry();

    private static final long DATE_MILLIS_UTC = new DateTime(2011, 5, 6, 0, 0, UTC).getMillis();
    private static final long DATE_DAYS = TimeUnit.MILLISECONDS.toDays(DATE_MILLIS_UTC);
    private static final String DATE_STRING = DateTimeFormat.forPattern("yyyy-MM-dd").withZoneUTC().print(DATE_MILLIS_UTC);
    private static final Date SQL_DATE = new Date(UTC.getMillisKeepLocal(DateTimeZone.getDefault(), DATE_MILLIS_UTC));

    private static final long TIMESTAMP = new DateTime(2011, 5, 6, 7, 8, 9, 123).getMillis();
    private static final String TIMESTAMP_STRING = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS").print(TIMESTAMP);

    private static final JavaHiveDecimalObjectInspector DECIMAL_INSPECTOR_PRECISION_2 =
            new JavaHiveDecimalObjectInspector(new DecimalTypeInfo(2, 1));
    private static final JavaHiveDecimalObjectInspector DECIMAL_INSPECTOR_PRECISION_4 =
            new JavaHiveDecimalObjectInspector(new DecimalTypeInfo(4, 2));
    private static final JavaHiveDecimalObjectInspector DECIMAL_INSPECTOR_PRECISION_8 =
            new JavaHiveDecimalObjectInspector(new DecimalTypeInfo(8, 4));
    private static final JavaHiveDecimalObjectInspector DECIMAL_INSPECTOR_PRECISION_17 =
            new JavaHiveDecimalObjectInspector(new DecimalTypeInfo(17, 8));
    private static final JavaHiveDecimalObjectInspector DECIMAL_INSPECTOR_PRECISION_18 =
            new JavaHiveDecimalObjectInspector(new DecimalTypeInfo(18, 8));
    private static final JavaHiveDecimalObjectInspector DECIMAL_INSPECTOR_PRECISION_38 =
            new JavaHiveDecimalObjectInspector(new DecimalTypeInfo(38, 16));

    private static final DecimalType DECIMAL_TYPE_PRECISION_2 = DecimalType.createDecimalType(2, 1);
    private static final DecimalType DECIMAL_TYPE_PRECISION_4 = DecimalType.createDecimalType(4, 2);
    private static final DecimalType DECIMAL_TYPE_PRECISION_8 = DecimalType.createDecimalType(8, 4);
    private static final DecimalType DECIMAL_TYPE_PRECISION_17 = DecimalType.createDecimalType(17, 8);
    private static final DecimalType DECIMAL_TYPE_PRECISION_18 = DecimalType.createDecimalType(18, 8);
    private static final DecimalType DECIMAL_TYPE_PRECISION_38 = DecimalType.createDecimalType(38, 16);

    private static final HiveDecimal WRITE_DECIMAL_PRECISION_2 = HiveDecimal.create(new BigDecimal("-1.2"));
    private static final HiveDecimal WRITE_DECIMAL_PRECISION_4 = HiveDecimal.create(new BigDecimal("12.3"));
    private static final HiveDecimal WRITE_DECIMAL_PRECISION_8 = HiveDecimal.create(new BigDecimal("-1234.5678"));
    private static final HiveDecimal WRITE_DECIMAL_PRECISION_17 = HiveDecimal.create(new BigDecimal("123456789.1234"));
    private static final HiveDecimal WRITE_DECIMAL_PRECISION_18 = HiveDecimal.create(new BigDecimal("-1234567890.12345678"));
    private static final HiveDecimal WRITE_DECIMAL_PRECISION_38 = HiveDecimal.create(new BigDecimal("1234567890123456789012.12345678"));

    private static final BigDecimal EXPECTED_DECIMAL_PRECISION_2 = new BigDecimal("-1.2");
    private static final BigDecimal EXPECTED_DECIMAL_PRECISION_4 = new BigDecimal("12.30");
    private static final BigDecimal EXPECTED_DECIMAL_PRECISION_8 = new BigDecimal("-1234.5678");
    private static final BigDecimal EXPECTED_DECIMAL_PRECISION_17 = new BigDecimal("123456789.12340000");
    private static final BigDecimal EXPECTED_DECIMAL_PRECISION_18 = new BigDecimal("-1234567890.12345678");
    private static final BigDecimal EXPECTED_DECIMAL_PRECISION_38 = new BigDecimal("1234567890123456789012.1234567800000000");

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
            .add(new TestColumn("p_decimal_precision_2", DECIMAL_INSPECTOR_PRECISION_2, WRITE_DECIMAL_PRECISION_2.toString(), EXPECTED_DECIMAL_PRECISION_2, true))
            .add(new TestColumn("p_decimal_precision_4", DECIMAL_INSPECTOR_PRECISION_4, WRITE_DECIMAL_PRECISION_4.toString(), EXPECTED_DECIMAL_PRECISION_4, true))
            .add(new TestColumn("p_decimal_precision_8", DECIMAL_INSPECTOR_PRECISION_8, WRITE_DECIMAL_PRECISION_8.toString(), EXPECTED_DECIMAL_PRECISION_8, true))
            .add(new TestColumn("p_decimal_precision_17", DECIMAL_INSPECTOR_PRECISION_17, WRITE_DECIMAL_PRECISION_17.toString(), EXPECTED_DECIMAL_PRECISION_17, true))
            .add(new TestColumn("p_decimal_precision_18", DECIMAL_INSPECTOR_PRECISION_18, WRITE_DECIMAL_PRECISION_18.toString(), EXPECTED_DECIMAL_PRECISION_18, true))
            .add(new TestColumn("p_decimal_precision_38", DECIMAL_INSPECTOR_PRECISION_38, WRITE_DECIMAL_PRECISION_38.toString() + "BD", EXPECTED_DECIMAL_PRECISION_38, true))
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
            .add(new TestColumn("p_null_decimal_precision_2", DECIMAL_INSPECTOR_PRECISION_2, HIVE_DEFAULT_DYNAMIC_PARTITION, null, true))
            .add(new TestColumn("p_null_decimal_precision_4", DECIMAL_INSPECTOR_PRECISION_4, HIVE_DEFAULT_DYNAMIC_PARTITION, null, true))
            .add(new TestColumn("p_null_decimal_precision_8", DECIMAL_INSPECTOR_PRECISION_8, HIVE_DEFAULT_DYNAMIC_PARTITION, null, true))
            .add(new TestColumn("p_null_decimal_precision_17", DECIMAL_INSPECTOR_PRECISION_17, HIVE_DEFAULT_DYNAMIC_PARTITION, null, true))
            .add(new TestColumn("p_null_decimal_precision_18", DECIMAL_INSPECTOR_PRECISION_18, HIVE_DEFAULT_DYNAMIC_PARTITION, null, true))
            .add(new TestColumn("p_null_decimal_precision_38", DECIMAL_INSPECTOR_PRECISION_38, HIVE_DEFAULT_DYNAMIC_PARTITION, null, true))

//            .add(new TestColumn("p_null_binary", javaByteArrayObjectInspector, HIVE_DEFAULT_DYNAMIC_PARTITION, null, true))
            .add(new TestColumn("t_null_string", javaStringObjectInspector, null, null))
            .add(new TestColumn("t_null_array_int", getStandardListObjectInspector(javaIntObjectInspector), null, null))
            .add(new TestColumn("t_null_decimal_precision_2", DECIMAL_INSPECTOR_PRECISION_2, null, null))
            .add(new TestColumn("t_null_decimal_precision_4", DECIMAL_INSPECTOR_PRECISION_4, null, null))
            .add(new TestColumn("t_null_decimal_precision_8", DECIMAL_INSPECTOR_PRECISION_8, null, null))
            .add(new TestColumn("t_null_decimal_precision_17", DECIMAL_INSPECTOR_PRECISION_17, null, null))
            .add(new TestColumn("t_null_decimal_precision_18", DECIMAL_INSPECTOR_PRECISION_18, null, null))
            .add(new TestColumn("t_null_decimal_precision_38", DECIMAL_INSPECTOR_PRECISION_38, null, null))
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
            .add(new TestColumn("t_decimal_precision_2", DECIMAL_INSPECTOR_PRECISION_2, WRITE_DECIMAL_PRECISION_2, EXPECTED_DECIMAL_PRECISION_2))
            .add(new TestColumn("t_decimal_precision_4", DECIMAL_INSPECTOR_PRECISION_4, WRITE_DECIMAL_PRECISION_4, EXPECTED_DECIMAL_PRECISION_4))
            .add(new TestColumn("t_decimal_precision_8", DECIMAL_INSPECTOR_PRECISION_8, WRITE_DECIMAL_PRECISION_8, EXPECTED_DECIMAL_PRECISION_8))
            .add(new TestColumn("t_decimal_precision_17", DECIMAL_INSPECTOR_PRECISION_17, WRITE_DECIMAL_PRECISION_17, EXPECTED_DECIMAL_PRECISION_17))
            .add(new TestColumn("t_decimal_precision_18", DECIMAL_INSPECTOR_PRECISION_18, WRITE_DECIMAL_PRECISION_18, EXPECTED_DECIMAL_PRECISION_18))
            .add(new TestColumn("t_decimal_precision_38", DECIMAL_INSPECTOR_PRECISION_38, WRITE_DECIMAL_PRECISION_38, EXPECTED_DECIMAL_PRECISION_38))
            .add(new TestColumn("t_binary", javaByteArrayObjectInspector, Slices.utf8Slice("test2"), Slices.utf8Slice("test2")))
            .add(new TestColumn("t_map_string",
                    getStandardMapObjectInspector(javaStringObjectInspector, javaStringObjectInspector),
                    ImmutableMap.of("test", "test"),
                    mapSliceOf(VARCHAR, VARCHAR, "test", "test")))
            .add(new TestColumn("t_map_tinyint", getStandardMapObjectInspector(javaByteObjectInspector, javaByteObjectInspector), ImmutableMap.of((byte) 1, (byte) 1), mapSliceOf(BIGINT, BIGINT, 1, 1)))
            .add(new TestColumn("t_map_smallint",
                    getStandardMapObjectInspector(javaShortObjectInspector, javaShortObjectInspector),
                    ImmutableMap.of((short) 2, (short) 2),
                    mapSliceOf(BIGINT, BIGINT, 2, 2)))
            .add(new TestColumn("t_map_null_key", getStandardMapObjectInspector(javaIntObjectInspector, javaIntObjectInspector), mapWithNullKey(), mapSliceOf(BIGINT, BIGINT, 2, 3)))
            .add(new TestColumn("t_map_int", getStandardMapObjectInspector(javaIntObjectInspector, javaIntObjectInspector), ImmutableMap.of(3, 3), mapSliceOf(BIGINT, BIGINT, 3, 3)))
            .add(new TestColumn("t_map_bigint", getStandardMapObjectInspector(javaLongObjectInspector, javaLongObjectInspector), ImmutableMap.of(4L, 4L), mapSliceOf(BIGINT, BIGINT, 4L, 4L)))
            .add(new TestColumn("t_map_float", getStandardMapObjectInspector(javaFloatObjectInspector, javaFloatObjectInspector), ImmutableMap.of(5.0f, 5.0f), mapSliceOf(DOUBLE, DOUBLE, 5.0f, 5.0f)))
            .add(new TestColumn("t_map_double", getStandardMapObjectInspector(javaDoubleObjectInspector, javaDoubleObjectInspector), ImmutableMap.of(6.0, 6.0), mapSliceOf(DOUBLE, DOUBLE, 6.0, 6.0)))
            .add(new TestColumn("t_map_boolean",
                    getStandardMapObjectInspector(javaBooleanObjectInspector, javaBooleanObjectInspector),
                    ImmutableMap.of(true, true),
                    mapSliceOf(BOOLEAN, BOOLEAN, true, true)))
            .add(new TestColumn("t_map_date",
                    getStandardMapObjectInspector(javaDateObjectInspector, javaDateObjectInspector),
                    ImmutableMap.of(SQL_DATE, SQL_DATE),
                    mapSliceOf(DateType.DATE, DateType.DATE, DATE_DAYS, DATE_DAYS)))
            .add(new TestColumn("t_map_timestamp",
                    getStandardMapObjectInspector(javaTimestampObjectInspector, javaTimestampObjectInspector),
                    ImmutableMap.of(new Timestamp(TIMESTAMP), new Timestamp(TIMESTAMP)),
                    mapSliceOf(TimestampType.TIMESTAMP, TimestampType.TIMESTAMP, TIMESTAMP, TIMESTAMP)))
            .add(new TestColumn("t_map_decimal_precision_2",
                    getStandardMapObjectInspector(DECIMAL_INSPECTOR_PRECISION_2, DECIMAL_INSPECTOR_PRECISION_2),
                    ImmutableMap.of(WRITE_DECIMAL_PRECISION_2, WRITE_DECIMAL_PRECISION_2),
                    decimalMapSliceOf(DECIMAL_TYPE_PRECISION_2, EXPECTED_DECIMAL_PRECISION_2)
            ))
            .add(new TestColumn("t_map_decimal_precision_4",
                    getStandardMapObjectInspector(DECIMAL_INSPECTOR_PRECISION_4, DECIMAL_INSPECTOR_PRECISION_4),
                    ImmutableMap.of(WRITE_DECIMAL_PRECISION_4, WRITE_DECIMAL_PRECISION_4),
                    decimalMapSliceOf(DECIMAL_TYPE_PRECISION_4, EXPECTED_DECIMAL_PRECISION_4)
            ))
            .add(new TestColumn("t_map_decimal_precision_8",
                    getStandardMapObjectInspector(DECIMAL_INSPECTOR_PRECISION_8, DECIMAL_INSPECTOR_PRECISION_8),
                    ImmutableMap.of(WRITE_DECIMAL_PRECISION_8, WRITE_DECIMAL_PRECISION_8),
                    decimalMapSliceOf(DECIMAL_TYPE_PRECISION_8, EXPECTED_DECIMAL_PRECISION_8)
            ))
            .add(new TestColumn("t_map_decimal_precision_17",
                    getStandardMapObjectInspector(DECIMAL_INSPECTOR_PRECISION_17, DECIMAL_INSPECTOR_PRECISION_17),
                    ImmutableMap.of(WRITE_DECIMAL_PRECISION_17, WRITE_DECIMAL_PRECISION_17),
                    decimalMapSliceOf(DECIMAL_TYPE_PRECISION_17, EXPECTED_DECIMAL_PRECISION_17)
            ))
            .add(new TestColumn("t_map_decimal_precision_18",
                    getStandardMapObjectInspector(DECIMAL_INSPECTOR_PRECISION_18, DECIMAL_INSPECTOR_PRECISION_18),
                    ImmutableMap.of(WRITE_DECIMAL_PRECISION_18, WRITE_DECIMAL_PRECISION_18),
                    decimalMapSliceOf(DECIMAL_TYPE_PRECISION_18, EXPECTED_DECIMAL_PRECISION_18)
            ))
            .add(new TestColumn("t_map_decimal_precision_38",
                    getStandardMapObjectInspector(DECIMAL_INSPECTOR_PRECISION_38, DECIMAL_INSPECTOR_PRECISION_38),
                    ImmutableMap.of(WRITE_DECIMAL_PRECISION_38, WRITE_DECIMAL_PRECISION_38),
                    decimalMapSliceOf(DECIMAL_TYPE_PRECISION_38, EXPECTED_DECIMAL_PRECISION_38)
            ))
            .add(new TestColumn("t_array_empty", getStandardListObjectInspector(javaStringObjectInspector), ImmutableList.of(), arraySliceOf(VARCHAR)))
            .add(new TestColumn("t_array_string", getStandardListObjectInspector(javaStringObjectInspector), ImmutableList.of("test"), arraySliceOf(VARCHAR, "test")))
            .add(new TestColumn("t_array_tinyint", getStandardListObjectInspector(javaByteObjectInspector), ImmutableList.of((byte) 1), arraySliceOf(BIGINT, 1)))
            .add(new TestColumn("t_array_smallint", getStandardListObjectInspector(javaShortObjectInspector), ImmutableList.of((short) 2), arraySliceOf(BIGINT, 2)))
            .add(new TestColumn("t_array_int", getStandardListObjectInspector(javaIntObjectInspector), ImmutableList.of(3), arraySliceOf(BIGINT, 3)))
            .add(new TestColumn("t_array_bigint", getStandardListObjectInspector(javaLongObjectInspector), ImmutableList.of(4L), arraySliceOf(BIGINT, 4L)))
            .add(new TestColumn("t_array_float", getStandardListObjectInspector(javaFloatObjectInspector), ImmutableList.of(5.0f), arraySliceOf(DOUBLE, 5.0f)))
            .add(new TestColumn("t_array_double", getStandardListObjectInspector(javaDoubleObjectInspector), ImmutableList.of(6.0), arraySliceOf(DOUBLE, 6.0)))
            .add(new TestColumn("t_array_boolean", getStandardListObjectInspector(javaBooleanObjectInspector), ImmutableList.of(true), arraySliceOf(BOOLEAN, true)))
            .add(new TestColumn("t_array_date",
                    getStandardListObjectInspector(javaDateObjectInspector),
                    ImmutableList.of(SQL_DATE),
                    arraySliceOf(DateType.DATE, DATE_DAYS)))
            .add(new TestColumn("t_array_timestamp",
                    getStandardListObjectInspector(javaTimestampObjectInspector),
                    ImmutableList.of(new Timestamp(TIMESTAMP)),
                    arraySliceOf(TimestampType.TIMESTAMP, TIMESTAMP)))
            .add(new TestColumn("t_array_decimal_precision_2",
                    getStandardListObjectInspector(DECIMAL_INSPECTOR_PRECISION_2),
                    ImmutableList.of(WRITE_DECIMAL_PRECISION_2),
                    decimalArraySliceOf(DECIMAL_TYPE_PRECISION_2, EXPECTED_DECIMAL_PRECISION_2)))
            .add(new TestColumn("t_array_decimal_precision_4",
                    getStandardListObjectInspector(DECIMAL_INSPECTOR_PRECISION_4),
                    ImmutableList.of(WRITE_DECIMAL_PRECISION_4),
                    decimalArraySliceOf(DECIMAL_TYPE_PRECISION_4, EXPECTED_DECIMAL_PRECISION_4)))
            .add(new TestColumn("t_array_decimal_precision_8",
                    getStandardListObjectInspector(DECIMAL_INSPECTOR_PRECISION_8),
                    ImmutableList.of(WRITE_DECIMAL_PRECISION_8),
                    decimalArraySliceOf(DECIMAL_TYPE_PRECISION_8, EXPECTED_DECIMAL_PRECISION_8)))
            .add(new TestColumn("t_array_decimal_precision_17",
                    getStandardListObjectInspector(DECIMAL_INSPECTOR_PRECISION_17),
                    ImmutableList.of(WRITE_DECIMAL_PRECISION_17),
                    decimalArraySliceOf(DECIMAL_TYPE_PRECISION_17, EXPECTED_DECIMAL_PRECISION_17)))
            .add(new TestColumn("t_array_decimal_precision_18",
                    getStandardListObjectInspector(DECIMAL_INSPECTOR_PRECISION_18),
                    ImmutableList.of(WRITE_DECIMAL_PRECISION_18),
                    decimalArraySliceOf(DECIMAL_TYPE_PRECISION_18, EXPECTED_DECIMAL_PRECISION_18)))
            .add(new TestColumn("t_array_decimal_precision_38",
                    getStandardListObjectInspector(DECIMAL_INSPECTOR_PRECISION_38),
                    ImmutableList.of(WRITE_DECIMAL_PRECISION_38),
                    decimalArraySliceOf(DECIMAL_TYPE_PRECISION_38, EXPECTED_DECIMAL_PRECISION_38)))
            .add(new TestColumn("t_struct_bigint",
                    getStandardStructObjectInspector(ImmutableList.of("s_bigint"), ImmutableList.of(javaLongObjectInspector)),
                    new Long[] {1L},
                    arraySliceOf(BIGINT, 1)))
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
                    mapSliceOf(VARCHAR, new ArrayType(new ArrayType(BIGINT)), "test", arraySliceOf(new ArrayType(BIGINT), arraySliceOf(BIGINT, 1)))
            ))
            .add(new TestColumn("t_struct_nested", getStandardStructObjectInspector(ImmutableList.of("struct_field"),
                    ImmutableList.of(getStandardListObjectInspector(javaStringObjectInspector))), ImmutableList.of(ImmutableList.of("1", "2", "3")), rowSliceOf(ImmutableList.of(new ArrayType(VARCHAR)), arraySliceOf(VARCHAR, "1", "2", "3"))))
            .add(new TestColumn("t_struct_null", getStandardStructObjectInspector(ImmutableList.of("struct_field", "struct_field2"),
                    ImmutableList.of(javaStringObjectInspector, javaStringObjectInspector)), Arrays.asList(null, null), rowSliceOf(ImmutableList.of(VARCHAR, VARCHAR), null, null)))
            .build();

    private static Map<Integer, Integer> mapWithNullKey()
    {
        Map<Integer, Integer> map = new HashMap<>();
        map.put(null, 0);
        map.put(2, 3);
        return map;
    }

    protected List<HiveColumnHandle> getColumnHandles(List<TestColumn> testColumns)
    {
        List<HiveColumnHandle> columns = new ArrayList<>();
        int nextHiveColumnIndex = 0;
        for (int i = 0; i < testColumns.size(); i++) {
            TestColumn testColumn = testColumns.get(i);
            int columnIndex = testColumn.isPartitionKey() ? -1 : nextHiveColumnIndex++;

            ObjectInspector inspector = testColumn.getObjectInspector();
            HiveType hiveType = HiveType.getHiveType(inspector);
            Type type = getType(inspector, TYPE_MANAGER);

            columns.add(new HiveColumnHandle("client_id", testColumn.getName(), i, hiveType, type.getTypeSignature(), columnIndex, testColumn.isPartitionKey()));
        }
        return columns;
    }

    public FileSplit createTestFile(String filePath,
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
                Type type = getType(testColumn.getObjectInspector(), TYPE_MANAGER);
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
                    fieldFromCursor = cursor.getSlice(i);
                }
                else if (type instanceof DecimalType) {
                    DecimalType decimalType = (DecimalType) type;
                    if (decimalType.isShort()) {
                        fieldFromCursor = new BigDecimal(BigInteger.valueOf(cursor.getLong(i)), decimalType.getScale());
                    }
                    else {
                        fieldFromCursor = new BigDecimal(LongDecimalType.unscaledValueToBigInteger(cursor.getSlice(i)), decimalType.getScale());
                    }
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
                    Slice expected = (Slice) testColumn.getExpectedValue();
                    Slice actual = (Slice) fieldFromCursor;
                    assertEquals(actual, expected, String.format("Wrong value for column %s", testColumn.getName()));
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
                    if (actualValue == null) {
                        assertEquals(null, expectedValue, String.format("Expected null for column %d", i));
                    }
                    else if (testColumn.getObjectInspector().getTypeName().equals("float") ||
                            testColumn.getObjectInspector().getTypeName().equals("double")) {
                        assertEquals((double) actualValue, (double) expectedValue, EPSILON);
                    }
                    else if (testColumn.getObjectInspector().getTypeName().equals("date")) {
                        SqlDate expectedDate = new SqlDate(((Long) expectedValue).intValue());
                        assertEquals(actualValue, expectedDate);
                    }
                    else if (testColumn.getObjectInspector().getTypeName().equals("timestamp")) {
                        SqlTimestamp expectedTimestamp = new SqlTimestamp((Long) expectedValue, SESSION.getTimeZoneKey());
                        assertEquals(actualValue, expectedTimestamp);
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

                        if (actualValue instanceof SqlDecimal) {
                            actualValue = new BigDecimal(actualValue.toString());
                        }

                        assertEquals(actualValue, expectedValue, String.format("Wrong value for column %d", i));
                    }
                    else {
                        BlockBuilder builder = type.createBlockBuilder(new BlockBuilderStatus(), 1, ((Slice) expectedValue).length());
                        type.writeSlice(builder, (Slice) expectedValue);
                        expectedValue = type.getObjectValue(SESSION, builder.build(), 0);
                        assertEquals(actualValue, expectedValue, String.format("Wrong value for column %s", testColumn.getName()));
                    }
                }
            }
        }
        finally {
            pageSource.close();
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
    }
}

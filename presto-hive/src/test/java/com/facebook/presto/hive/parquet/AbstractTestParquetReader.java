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
package com.facebook.presto.hive.parquet;

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.SqlDate;
import com.facebook.presto.common.type.SqlDecimal;
import com.facebook.presto.common.type.SqlTimestamp;
import com.facebook.presto.common.type.SqlVarbinary;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.parquet.ParquetDataSourceId;
import com.facebook.presto.parquet.cache.CachingParquetMetadataSource;
import com.facebook.presto.parquet.cache.MetadataReader;
import com.facebook.presto.parquet.cache.ParquetFileMetadata;
import com.facebook.presto.parquet.cache.ParquetMetadataSource;
import com.facebook.presto.parquet.writer.ParquetWriterOptions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.AbstractSequentialIterator;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.google.common.collect.Streams;
import com.google.common.primitives.Shorts;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaHiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.parquet.format.Statistics;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType;
import org.joda.time.DateTimeZone;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.Decimals.MAX_PRECISION;
import static com.facebook.presto.common.type.Decimals.MAX_SHORT_PRECISION;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.RowType.field;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.hive.parquet.ParquetTester.HIVE_STORAGE_TIME_ZONE;
import static com.facebook.presto.hive.parquet.ParquetTester.insertNullEvery;
import static com.facebook.presto.hive.parquet.ParquetTester.testSingleRead;
import static com.facebook.presto.hive.parquet.ParquetTester.writeParquetFileFromPresto;
import static com.facebook.presto.testing.DateTimeTestingUtils.sqlTimestampOf;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.facebook.presto.tests.StructuralTestUtil.mapType;
import static com.google.common.base.Functions.identity;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toList;
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
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.testng.Assert.assertEquals;

public abstract class AbstractTestParquetReader
{
    private static final int MAX_PRECISION_INT32 = (int) maxPrecision(4);
    private static final int MAX_PRECISION_INT64 = (int) maxPrecision(8);

    private final ParquetTester tester;

    public AbstractTestParquetReader(ParquetTester tester)
    {
        this.tester = tester;
    }

    @BeforeClass
    public void setUp()
    {
        assertEquals(DateTimeZone.getDefault(), HIVE_STORAGE_TIME_ZONE);

        // Parquet has excessive logging at INFO level
        Logger parquetLogger = Logger.getLogger("org.apache.parquet.hadoop");
        parquetLogger.setLevel(Level.WARNING);
    }

    @Test
    public void testArray()
            throws Exception
    {
        Iterable<List<Integer>> values = createTestArrays(
                Stream.generate(() -> Stream.of(1, null, 3, 5, null, null, null, 7, 11, null, 13, 17))
                        .flatMap(identity())
                        .limit(30_000).iterator());
        tester.testRoundTrip(getStandardListObjectInspector(javaIntObjectInspector), values, values, new ArrayType(INTEGER));
    }

    @Test
    public void testEmptyArrays()
            throws Exception
    {
        Iterable<List<Integer>> values = Stream.generate(Collections::<Integer>emptyList)
                .limit(30_000).toList();
        tester.testRoundTrip(getStandardListObjectInspector(javaIntObjectInspector), values, values, new ArrayType(INTEGER));
    }

    @Test
    public void testNestedArrays()
            throws Exception
    {
        int nestingLevel = 8;
        ObjectInspector objectInspector = getStandardListObjectInspector(javaIntObjectInspector);
        Type type = new ArrayType(INTEGER);
        Iterable values = () -> Stream.generate(() -> Stream.of(1, null, 3, null, 5, null, 7, null, null, null, 11, null, 13))
                .flatMap(identity()).limit(3_210).iterator();
        for (int i = 0; i < nestingLevel; i++) {
            values = createNullableTestArrays(values.iterator());
            objectInspector = getStandardListObjectInspector(objectInspector);
            type = new ArrayType(type);
        }
        values = createTestArrays(values.iterator());
        tester.testRoundTrip(objectInspector, values, values, type);
    }

    @Test
    public void testSingleLevelSchemaNestedArrays()
            throws Exception
    {
        int nestingLevel = 5;
        ObjectInspector objectInspector = getStandardListObjectInspector(javaIntObjectInspector);
        Type type = new ArrayType(INTEGER);
        Iterable values = intsBetween(0, 31_234);
        for (int i = 0; i < nestingLevel; i++) {
            values = createTestArrays(values.iterator());
            objectInspector = getStandardListObjectInspector(objectInspector);
            type = new ArrayType(type);
        }
        values = createTestArrays(values.iterator());
        tester.testSingleLevelArraySchemaRoundTrip(objectInspector, values, values, type);
    }

    @Test
    public void testArrayOfStructs()
            throws Exception
    {
        Iterable<List> structs = createNullableTestStructs(
                intsBetween(0, 31_234).stream().map(Object::toString).iterator(),
                longsBetween(0, 31_234).stream().iterator());
        Iterable<List<List>> values = createTestArrays(structs.iterator());
        List<String> structFieldNames = asList("stringField", "longField");
        Type structType = RowType.from(asList(field("stringField", VARCHAR), field("longField", BIGINT)));
        tester.testRoundTrip(
                getStandardListObjectInspector(getStandardStructObjectInspector(structFieldNames, asList(javaStringObjectInspector, javaLongObjectInspector))),
                values, values, new ArrayType(structType));
    }

    @Test
    public void testCustomSchemaArrayOfStructs()
            throws Exception
    {
        MessageType customSchemaArrayOfStructs = parseMessageType("message ParquetSchema { " +
                "  optional group self (LIST) { " +
                "    repeated group self_tuple { " +
                "      optional int64 a; " +
                "      optional boolean b; " +
                "      required binary c (UTF8); " +
                "    } " +
                "  } " +
                "}");
        Iterable<Long> aValues = () -> Stream.generate(() -> Stream.of(1L, null, 3L, 5L, null, null, null, 7L, 11L, null, 13L, 17L))
                .flatMap(identity()).limit(30_000).iterator();
        Iterable<Boolean> bValues = () -> Stream.generate(() -> Stream.of(null, true, false, null, null, true, false))
                .flatMap(identity()).limit(30_000).iterator();
        Iterable<String> cValues = () -> intsBetween(0, 31_234).stream().map(Object::toString).iterator();

        Iterable<List> structs = createTestStructs(aValues.iterator(), bValues.iterator(), cValues.iterator());
        Iterable<List<List>> values = createTestArrays(structs.iterator());
        List<String> structFieldNames = asList("a", "b", "c");
        Type structType = RowType.from(asList(field("a", BIGINT), field("b", BOOLEAN), field("c", VARCHAR)));
        tester.testSingleLevelArrayRoundTrip(
                getStandardListObjectInspector(getStandardStructObjectInspector(structFieldNames, asList(javaLongObjectInspector, javaBooleanObjectInspector, javaStringObjectInspector))),
                values, values, "self", new ArrayType(structType), Optional.of(customSchemaArrayOfStructs));
    }

    @Test
    public void testSingleLevelSchemaArrayOfStructs()
            throws Exception
    {
        Iterable<Long> aValues = () -> Stream.generate(() -> Stream.of(1L, null, 3L, 5L, null, null, null, 7L, 11L, null, 13L, 17L))
                .flatMap(identity()).limit(30_000).iterator();
        Iterable<Boolean> bValues = () -> Stream.generate(() -> Stream.of(null, true, false, null, null, true, false))
                .flatMap(identity()).limit(30_000).iterator();
        Iterable<String> cValues = () -> intsBetween(0, 31_234).stream().map(Object::toString).iterator();

        Iterable<List> structs = createTestStructs(aValues.iterator(), bValues.iterator(), cValues.iterator());
        Iterable<List<List>> values = createTestArrays(structs.iterator());
        List<String> structFieldNames = asList("a", "b", "c");
        Type structType = RowType.from(asList(field("a", BIGINT), field("b", BOOLEAN), field("c", VARCHAR)));
        ObjectInspector objectInspector = getStandardListObjectInspector(getStandardStructObjectInspector(structFieldNames, asList(javaLongObjectInspector, javaBooleanObjectInspector, javaStringObjectInspector)));
        tester.testSingleLevelArraySchemaRoundTrip(objectInspector, values, values, new ArrayType(structType));
    }

    @Test
    public void testArrayOfArrayOfStructOfArray()
            throws Exception
    {
        Iterable<List<String>> stringArrayField = createNullableTestArrays(intsBetween(0, 31_234).stream().map(Object::toString).iterator());
        Iterable<List> structs = createNullableTestStructs(stringArrayField.iterator(),
                Stream.generate(() -> Stream.of(1, null, 3, 5, null, 7, 11, null, 17)).flatMap(identity()).limit(31_234).iterator());
        List<String> structFieldNames = asList("stringArrayField", "intField");
        Type structType = RowType.from(asList(field("stringArrayField", new ArrayType(VARCHAR)), field("intField", INTEGER)));
        Iterable<List<List>> arrays = createNullableTestArrays(structs.iterator());
        Iterable<List<List<List>>> values = createTestArrays(arrays.iterator());
        tester.testRoundTrip(
                getStandardListObjectInspector(
                        getStandardListObjectInspector(
                                getStandardStructObjectInspector(
                                        structFieldNames,
                                        asList(getStandardListObjectInspector(javaStringObjectInspector), javaIntObjectInspector)))),
                values, values, new ArrayType(new ArrayType(structType)));
    }

    @Test
    public void testSingleLevelSchemaArrayOfArrayOfStructOfArray()
            throws Exception
    {
        Iterable<List<String>> stringArrayField = createNullableTestArrays(
                intsBetween(0, 31_234).stream().map(Object::toString).iterator());
        Iterable<List> structs = createTestStructs(stringArrayField.iterator(),
                Stream.generate(() -> Stream.of(1, null, 3, 5, null, 7, 11, null, 17)).flatMap(identity()).limit(31_234).iterator());
        List<String> structFieldNames = asList("stringArrayField", "intField");
        Type structType = RowType.from(asList(field("stringArrayField", new ArrayType(VARCHAR)), field("intField", INTEGER)));
        Iterable<List<List>> arrays = createTestArrays(structs.iterator());
        Iterable<List<List<List>>> values = createTestArrays(arrays.iterator());
        tester.testSingleLevelArraySchemaRoundTrip(
                getStandardListObjectInspector(
                        getStandardListObjectInspector(
                                getStandardStructObjectInspector(
                                        structFieldNames,
                                        asList(getStandardListObjectInspector(javaStringObjectInspector), javaIntObjectInspector)))),
                values, values, new ArrayType(new ArrayType(structType)));
    }

    @Test
    public void testArrayOfStructOfArray()
            throws Exception
    {
        Iterable<List<String>> stringArrayField = createNullableTestArrays(intsBetween(0, 31_234).stream().map(Object::toString).iterator());
        Iterable<List> structs = createNullableTestStructs(stringArrayField.iterator(), Stream.generate(() -> Stream.of(1, 3, null, 5, 7, null, 11, 13, null, 17)).flatMap(identity()).limit(31_234).iterator());
        List<String> structFieldNames = asList("stringArrayField", "intField");
        Type structType = RowType.from(asList(field("stringArrayField", new ArrayType(VARCHAR)), field("intField", INTEGER)));
        Iterable<List<List>> values = createTestArrays(structs.iterator());
        tester.testRoundTrip(
                getStandardListObjectInspector(
                        getStandardStructObjectInspector(
                                structFieldNames,
                                asList(getStandardListObjectInspector(javaStringObjectInspector), javaIntObjectInspector))),
                values, values, new ArrayType(structType));
    }

    @Test
    public void testSingleLevelSchemaArrayOfStructOfArray()
            throws Exception
    {
        Iterable<List<String>> stringArrayField = createNullableTestArrays(
                intsBetween(0, 31_234).stream().map(Object::toString).iterator());
        Iterable<List> structs = createTestStructs(stringArrayField.iterator(),
                Stream.generate(() -> Stream.of(1, 3, null, 5, 7, null, 11, 13, null, 17)).flatMap(identity()).limit(31_234).iterator());
        List<String> structFieldNames = asList("stringArrayField", "intField");
        Type structType = RowType.from(asList(field("stringArrayField", new ArrayType(VARCHAR)), field("intField", INTEGER)));
        Iterable<List<List>> values = createTestArrays(structs.iterator());
        tester.testSingleLevelArraySchemaRoundTrip(
                getStandardListObjectInspector(
                        getStandardStructObjectInspector(
                                structFieldNames,
                                asList(getStandardListObjectInspector(javaStringObjectInspector), javaIntObjectInspector))),
                values, values, new ArrayType(structType));
    }

    @Test
    public void testMap()
            throws Exception
    {
        Iterable<Map<String, Long>> values = createTestMaps(
                intsBetween(0, 100_000).stream().map(Object::toString).iterator(),
                longsBetween(0, 10_000).stream().iterator());
        tester.testRoundTrip(getStandardMapObjectInspector(javaStringObjectInspector, javaLongObjectInspector), values, values, mapType(VARCHAR, BIGINT));
    }

    @Test
    public void testNestedMaps()
            throws Exception
    {
        int nestingLevel = 12;
        Iterable<Integer> keys = intsBetween(0, 3_210);
        Iterable maps = () -> Stream.generate(() -> Stream.of(null, "value2", "value3", null, null, "value6", "value7")).flatMap(identity()).limit(3_210).iterator();
        ObjectInspector objectInspector = getStandardMapObjectInspector(javaIntObjectInspector, javaStringObjectInspector);
        Type type = mapType(INTEGER, VARCHAR);
        for (int i = 0; i < nestingLevel; i++) {
            maps = createNullableTestMaps(keys.iterator(), maps.iterator());
            objectInspector = getStandardMapObjectInspector(javaIntObjectInspector, objectInspector);
            type = mapType(INTEGER, type);
        }
        maps = createTestMaps(keys.iterator(), maps.iterator());
        tester.testRoundTrip(objectInspector, maps, maps, type);
    }

    @Test
    public void testArrayOfMaps()
            throws Exception
    {
        Iterable<Map<String, Long>> maps = createNullableTestMaps(
                intsBetween(0, 10).stream().map(Object::toString).iterator(),
                longsBetween(0, 10).stream().iterator());
        List<List<Map<String, Long>>> values = createTestArrays(maps.iterator());
        tester.testRoundTrip(getStandardListObjectInspector(getStandardMapObjectInspector(javaStringObjectInspector, javaLongObjectInspector)),
                values, values, new ArrayType(mapType(VARCHAR, BIGINT)));
    }

    @Test
    public void testSingleLevelSchemaArrayOfMaps()
            throws Exception
    {
        Iterable<Map<String, Long>> maps = createTestMaps(
                intsBetween(0, 10).stream().map(Object::toString).iterator(),
                longsBetween(0, 10).stream().iterator());
        List<List<Map<String, Long>>> values = createTestArrays(maps.iterator());
        ObjectInspector objectInspector = getStandardListObjectInspector(getStandardMapObjectInspector(javaStringObjectInspector, javaLongObjectInspector));
        tester.testSingleLevelArraySchemaRoundTrip(objectInspector, values, values, new ArrayType(mapType(VARCHAR, BIGINT)));
    }

    @Test
    public void testArrayOfMapOfStruct()
            throws Exception
    {
        Iterable<Integer> keys = intsBetween(0, 10_000);
        Iterable<List> structs = createNullableTestStructs(
                intsBetween(0, 10_000).stream().map(Object::toString).iterator(),
                longsBetween(0, 10_000).stream().iterator());
        List<String> structFieldNames = asList("stringField", "longField");
        Type structType = RowType.from(asList(field("stringField", VARCHAR), field("longField", BIGINT)));
        Iterable<Map<Integer, List>> maps = createNullableTestMaps(keys.iterator(), structs.iterator());
        List<List<Map<Integer, List>>> values = createTestArrays(maps.iterator());
        tester.testRoundTrip(getStandardListObjectInspector(
                        getStandardMapObjectInspector(
                                javaIntObjectInspector,
                                getStandardStructObjectInspector(structFieldNames, asList(javaStringObjectInspector, javaLongObjectInspector)))),
                values, values, new ArrayType(mapType(INTEGER, structType)));
    }

    @Test
    public void testSingleLevelArrayOfMapOfStruct()
            throws Exception
    {
        Iterable<Integer> keys = intsBetween(0, 10_000);
        Iterable<List> structs = createNullableTestStructs(
                intsBetween(0, 10_000).stream().map(Object::toString).iterator(),
                longsBetween(0, 10_000).stream().iterator());
        List<String> structFieldNames = asList("stringField", "longField");
        Type structType = RowType.from(asList(field("stringField", VARCHAR), field("longField", BIGINT)));
        Iterable<Map<Integer, List>> maps = createTestMaps(keys.iterator(), structs.iterator());
        List<List<Map<Integer, List>>> values = createTestArrays(maps.iterator());
        tester.testSingleLevelArraySchemaRoundTrip(getStandardListObjectInspector(
                        getStandardMapObjectInspector(
                                javaIntObjectInspector,
                                getStandardStructObjectInspector(structFieldNames, asList(javaStringObjectInspector, javaLongObjectInspector)))),
                values, values, new ArrayType(mapType(INTEGER, structType)));
    }

    @Test
    public void testSingleLevelArrayOfStructOfSingleElement()
            throws Exception
    {
        Iterable<List> structs = createTestStructs(intsBetween(0, 31_234).stream().map(Object::toString).iterator());
        Iterable<List<List>> values = createTestArrays(structs.iterator());
        List<String> structFieldNames = singletonList("test");
        Type structType = RowType.from(singletonList(field("test", VARCHAR)));
        tester.testRoundTrip(
                getStandardListObjectInspector(getStandardStructObjectInspector(structFieldNames, singletonList(javaStringObjectInspector))),
                values, values, new ArrayType(structType));
        tester.testSingleLevelArraySchemaRoundTrip(
                getStandardListObjectInspector(getStandardStructObjectInspector(structFieldNames, singletonList(javaStringObjectInspector))),
                values, values, new ArrayType(structType));
    }

    @Test
    public void testSingleLevelArrayOfStructOfStructOfSingleElement()
            throws Exception
    {
        Iterable<List> structs = createTestStructs(intsBetween(0, 31_234).stream().map(Object::toString).iterator());
        Iterable<List> structsOfStructs = createTestStructs(structs);
        Iterable<List<List>> values = createTestArrays(structsOfStructs.iterator());
        List<String> structFieldNames = singletonList("test");
        List<String> structsOfStructsFieldNames = singletonList("test");
        Type structType = RowType.from(singletonList(field("test", VARCHAR)));
        Type structsOfStructsType = RowType.from(singletonList(field("test", structType)));
        ObjectInspector structObjectInspector = getStandardStructObjectInspector(structFieldNames, singletonList(javaStringObjectInspector));
        tester.testRoundTrip(
                getStandardListObjectInspector(
                        getStandardStructObjectInspector(structsOfStructsFieldNames, singletonList(structObjectInspector))),
                values, values, new ArrayType(structsOfStructsType));
        tester.testSingleLevelArraySchemaRoundTrip(
                getStandardListObjectInspector(
                        getStandardStructObjectInspector(structsOfStructsFieldNames, singletonList(structObjectInspector))),
                values, values, new ArrayType(structsOfStructsType));
    }

    @Test
    public void testArrayOfMapOfArray()
            throws Exception
    {
        Iterable<List<Integer>> arrays = createNullableTestArrays(Stream.generate(() -> Stream.of(1, null, 3, 5, null, null, null, 7, 11, null, 13, 17))
                .flatMap(identity()).limit(10_000).iterator());
        Iterable<String> keys = () -> intsBetween(0, 10_000).stream().map(Object::toString).iterator();
        Iterable<Map<String, List<Integer>>> maps = createNullableTestMaps(keys.iterator(), arrays.iterator());
        List<List<Map<String, List<Integer>>>> values = createTestArrays(maps.iterator());
        tester.testRoundTrip(getStandardListObjectInspector(
                        getStandardMapObjectInspector(
                                javaStringObjectInspector,
                                getStandardListObjectInspector(javaIntObjectInspector))),
                values, values, new ArrayType(mapType(VARCHAR, new ArrayType(INTEGER))));
    }

    @Test
    public void testSingleLevelArrayOfMapOfArray()
            throws Exception
    {
        Iterable<List<Integer>> arrays = createNullableTestArrays(intsBetween(0, 10_000).stream().iterator());
        Iterable<String> keys = () -> intsBetween(0, 10_000).stream().map(Object::toString).iterator();
        Iterable<Map<String, List<Integer>>> maps = createTestMaps(keys.iterator(), arrays.iterator());
        List<List<Map<String, List<Integer>>>> values = createTestArrays(maps.iterator());
        tester.testSingleLevelArraySchemaRoundTrip(getStandardListObjectInspector(
                        getStandardMapObjectInspector(
                                javaStringObjectInspector,
                                getStandardListObjectInspector(javaIntObjectInspector))),
                values, values, new ArrayType(mapType(VARCHAR, new ArrayType(INTEGER))));
    }

    @Test
    public void testMapOfArrayValues()
            throws Exception
    {
        Iterable<List<Integer>> arrays = createNullableTestArrays(
                Stream.generate(() -> Stream.of(1, null, 3, 5, null, null, null, 7, 11, null, 13, 17))
                        .flatMap(identity()).limit(30_000).iterator());
        Iterable<Integer> keys = intsBetween(0, 30_000);
        Iterable<Map<Integer, List<Integer>>> values = createTestMaps(keys.iterator(), arrays.iterator());
        tester.testRoundTrip(getStandardMapObjectInspector(
                        javaIntObjectInspector,
                        getStandardListObjectInspector(javaIntObjectInspector)),
                values, values, mapType(INTEGER, new ArrayType(INTEGER)));
    }

    @Test
    public void testMapOfArrayKeys()
            throws Exception
    {
        Iterable<List<Integer>> mapKeys = createTestArrays(Stream.generate(() -> Stream.of(1, null, 3, 5, null, null, null, 7, 11, null, 13, 17))
                .flatMap(identity()).limit(30_000).iterator());
        Iterable<Integer> mapValues = intsBetween(0, 30_000);
        Iterable<Map<List<Integer>, Integer>> testMaps = createTestMaps(mapKeys.iterator(), mapValues.iterator());
        tester.testRoundTrip(
                getStandardMapObjectInspector(
                        getStandardListObjectInspector(javaIntObjectInspector),
                        javaIntObjectInspector),
                testMaps,
                testMaps,
                mapType(new ArrayType(INTEGER), INTEGER));
    }

    @Test
    public void testMapOfSingleLevelArray()
            throws Exception
    {
        Iterable<List<Integer>> arrays = createNullableTestArrays(intsBetween(0, 30_000).stream().iterator());
        Iterable<Integer> keys = intsBetween(0, 30_000);
        Iterable<Map<Integer, List<Integer>>> values = createTestMaps(keys.iterator(), arrays.iterator());
        tester.testSingleLevelArraySchemaRoundTrip(getStandardMapObjectInspector(
                        javaIntObjectInspector,
                        getStandardListObjectInspector(javaIntObjectInspector)),
                values, values, mapType(INTEGER, new ArrayType(INTEGER)));
    }

    @Test
    public void testMapOfStruct()
            throws Exception
    {
        Iterable<Long> keys = longsBetween(0, 30_000);
        Iterable<List> structs = createNullableTestStructs(
                intsBetween(0, 30_000).stream().map(Object::toString).iterator(),
                longsBetween(0, 30_000).stream().iterator());
        List<String> structFieldNames = asList("stringField", "longField");
        Type structType = RowType.from(asList(field("stringField", VARCHAR), field("longField", BIGINT)));
        Iterable<Map<Long, List>> values = createTestMaps(keys.iterator(), structs.iterator());
        tester.testRoundTrip(getStandardMapObjectInspector(
                        javaLongObjectInspector,
                        getStandardStructObjectInspector(structFieldNames, asList(javaStringObjectInspector, javaLongObjectInspector))),
                values, values, mapType(BIGINT, structType));
    }

    @Test
    public void testMapWithNullValues()
            throws Exception
    {
        Iterable<Integer> mapKeys = intsBetween(0, 31_234);
        Iterable<String> mapValues = () -> Stream.generate(() -> Stream.of(null, "value2", "value3", null, null, "value6", "value7")).flatMap(identity()).limit(31_234).iterator();
        Iterable<Map<Integer, String>> values = createTestMaps(mapKeys.iterator(), mapValues.iterator());
        tester.testRoundTrip(getStandardMapObjectInspector(javaIntObjectInspector, javaStringObjectInspector), values, values, mapType(INTEGER, VARCHAR));
    }

    @Test
    public void testStruct()
            throws Exception
    {
        List<List> values = createTestStructs(
                intsBetween(0, 31_234).stream().map(Object::toString).iterator(),
                longsBetween(0, 31_234).stream().iterator());
        List<String> structFieldNames = asList("stringField", "longField");
        Type structType = RowType.from(asList(field("stringField", VARCHAR), field("longField", BIGINT)));
        tester.testRoundTrip(getStandardStructObjectInspector(structFieldNames, asList(javaStringObjectInspector, javaLongObjectInspector)), values, values, structType);
    }

    @Test
    public void testNestedStructs()
            throws Exception
    {
        int nestingLevel = 14;
        Optional<List<String>> structFieldNames = Optional.of(singletonList("structField"));
        Iterable<?> values = () -> Stream.generate(() -> Stream.of(1, null, 3, null, 5, null, 7, null, null, null, 11, null, 13)).<Object>flatMap(identity()).limit(3_210).iterator();
        ObjectInspector objectInspector = getStandardStructObjectInspector(structFieldNames.orElseThrow(), singletonList(javaIntObjectInspector));
        Type type = RowType.from(singletonList(field("structField", INTEGER)));
        for (int i = 0; i < nestingLevel; i++) {
            values = createNullableTestStructs(values.iterator());
            objectInspector = getStandardStructObjectInspector(structFieldNames.orElseThrow(), singletonList(objectInspector));
            type = RowType.from(singletonList(field("structField", type)));
        }
        values = createTestStructs(values);
        tester.testRoundTrip(objectInspector, values, values, type);
    }

    @Test
    public void testComplexNestedStructs()
            throws Exception
    {
        final int n = 30;
        Iterable<Integer> mapKeys = intsBetween(0, n);
        Iterable<Integer> intPrimitives = () -> Stream.generate(() -> Stream.of(1, null, 3, null, 5, null, 7, null, null, null, 11, null, 13))
                .flatMap(identity()).limit(n).iterator();
        Iterable<String> stringPrimitives = () -> Stream.generate(() -> Stream.of(null, "value2", "value3", null, null, "value6", "value7"))
                .flatMap(identity()).limit(n).iterator();
        Iterable<Double> doublePrimitives = () -> Stream.generate(() -> Stream.of(1.1, null, 3.3, null, 5.5, null, 7.7, null, null, null, 11.11, null, 13.13))
                .flatMap(identity()).limit(n).iterator();
        Iterable<Boolean> booleanPrimitives = () -> Stream.generate(() -> Stream.of(null, true, false, null, null, true, false))
                .flatMap(identity()).limit(n).iterator();
        Iterable<String> mapStringKeys = Stream.generate(() -> UUID.randomUUID().toString()).limit(n).collect(toList());
        Iterable<Map<Integer, String>> mapsIntString = createNullableTestMaps(mapKeys.iterator(), stringPrimitives.iterator());
        Iterable<List<String>> arraysString = createNullableTestArrays(stringPrimitives.iterator());
        Iterable<Map<Integer, Double>> mapsIntDouble = createNullableTestMaps(mapKeys.iterator(), doublePrimitives.iterator());
        Iterable<List<Boolean>> arraysBoolean = createNullableTestArrays(booleanPrimitives.iterator());
        Iterable<Map<String, String>> mapsStringString = createNullableTestMaps(mapStringKeys.iterator(), stringPrimitives.iterator());

        List<String> struct1FieldNames = asList("mapIntStringField", "stringArrayField", "intField");
        Iterable<?> structs1 = createNullableTestStructs(mapsIntString.iterator(), arraysString.iterator(), intPrimitives.iterator());
        ObjectInspector struct1ObjectInspector = getStandardStructObjectInspector(struct1FieldNames,
                asList(
                        getStandardMapObjectInspector(javaIntObjectInspector, javaStringObjectInspector),
                        getStandardListObjectInspector(javaStringObjectInspector),
                        javaIntObjectInspector));
        Type struct1Type = RowType.from(asList(
                field("mapIntStringField", mapType(INTEGER, VARCHAR)),
                field("stringArrayField", new ArrayType(VARCHAR)),
                field("intField", INTEGER)));

        List<String> struct2FieldNames = asList("mapIntStringField", "stringArrayField", "structField");
        Iterable<?> structs2 = createNullableTestStructs(mapsIntString.iterator(), arraysString.iterator(), structs1.iterator());
        ObjectInspector struct2ObjectInspector = getStandardStructObjectInspector(struct2FieldNames,
                asList(
                        getStandardMapObjectInspector(javaIntObjectInspector, javaStringObjectInspector),
                        getStandardListObjectInspector(javaStringObjectInspector),
                        struct1ObjectInspector));
        Type struct2Type = RowType.from(asList(
                field("mapIntStringField", mapType(INTEGER, VARCHAR)),
                field("stringArrayField", new ArrayType(VARCHAR)),
                field("structField", struct1Type)));

        List<String> struct3FieldNames = asList("mapIntDoubleField", "booleanArrayField", "booleanField");
        Iterable<?> structs3 = createNullableTestStructs(mapsIntDouble.iterator(), arraysBoolean.iterator(), booleanPrimitives.iterator());
        ObjectInspector struct3ObjectInspector = getStandardStructObjectInspector(struct3FieldNames,
                asList(
                        getStandardMapObjectInspector(javaIntObjectInspector, javaDoubleObjectInspector),
                        getStandardListObjectInspector(javaBooleanObjectInspector),
                        javaBooleanObjectInspector));
        Type struct3Type = RowType.from(asList(
                field("mapIntDoubleField", mapType(INTEGER, DOUBLE)),
                field("booleanArrayField", new ArrayType(BOOLEAN)),
                field("booleanField", BOOLEAN)));

        List<String> struct4FieldNames = asList("mapIntDoubleField", "booleanArrayField", "structField");
        Iterable<?> structs4 = createNullableTestStructs(mapsIntDouble.iterator(), arraysBoolean.iterator(), structs3.iterator());
        ObjectInspector struct4ObjectInspector = getStandardStructObjectInspector(struct4FieldNames,
                asList(
                        getStandardMapObjectInspector(javaIntObjectInspector, javaDoubleObjectInspector),
                        getStandardListObjectInspector(javaBooleanObjectInspector),
                        struct3ObjectInspector));
        Type struct4Type = RowType.from(asList(
                field("mapIntDoubleField", mapType(INTEGER, DOUBLE)),
                field("booleanArrayField", new ArrayType(BOOLEAN)),
                field("structField", struct3Type)));

        List<String> structFieldNames = asList("structField1", "structField2", "structField3", "structField4", "mapIntDoubleField", "booleanArrayField", "mapStringStringField");
        List<ObjectInspector> objectInspectors =
                asList(
                        struct1ObjectInspector,
                        struct2ObjectInspector,
                        struct3ObjectInspector,
                        struct4ObjectInspector,
                        getStandardMapObjectInspector(javaIntObjectInspector, javaDoubleObjectInspector),
                        getStandardListObjectInspector(javaBooleanObjectInspector),
                        getStandardMapObjectInspector(javaStringObjectInspector, javaStringObjectInspector));
        List<Type> types = ImmutableList.of(struct1Type, struct2Type, struct3Type, struct4Type, mapType(INTEGER, DOUBLE), new ArrayType(BOOLEAN), mapType(VARCHAR, VARCHAR));

        Iterable<?>[] values = new Iterable<?>[] {structs1, structs2, structs3, structs4, mapsIntDouble, arraysBoolean, mapsStringString};
        tester.assertRoundTrip(objectInspectors, values, values, structFieldNames, types, Optional.empty());
    }

    @Test
    public void testStructOfMaps()
            throws Exception
    {
        Iterable<Integer> mapKeys = Stream.generate(() -> ThreadLocalRandom.current().nextInt(10_000)).limit(10_000).collect(toList());
        Iterable<Integer> intPrimitives = () -> Stream.generate(() -> Stream.of(1, null, 3, null, 5, null, 7, null, null, null, 11, null, 13)).flatMap(identity()).limit(10_000).iterator();
        Iterable<String> stringPrimitives = () -> Stream.generate(() -> Stream.of(null, "value2", "value3", null, null, "value6", "value7")).flatMap(identity()).limit(10_000).iterator();
        Iterable<Map<Integer, String>> maps = createNullableTestMaps(mapKeys.iterator(), stringPrimitives.iterator());
        Iterable<List<String>> stringArrayField = createNullableTestArrays(stringPrimitives.iterator());
        List<List> values = createTestStructs(maps.iterator(), stringArrayField.iterator(), intPrimitives.iterator());
        List<String> structFieldNames = asList("mapIntStringField", "stringArrayField", "intField");

        Type structType = RowType.from(asList(field("mapIntStringField", mapType(INTEGER, VARCHAR)), field("stringArrayField", new ArrayType(VARCHAR)), field("intField", INTEGER)));
        tester.testRoundTrip(getStandardStructObjectInspector(structFieldNames,
                        asList(
                                getStandardMapObjectInspector(javaIntObjectInspector, javaStringObjectInspector),
                                getStandardListObjectInspector(javaStringObjectInspector),
                                javaIntObjectInspector)),
                values, values, structType);
    }

    @Test
    public void testStructOfNullableMapBetweenNonNullFields()
            throws Exception
    {
        Iterable<Integer> intPrimitives = intsBetween(0, 10_000);
        Iterable<String> stringPrimitives = () -> Stream.generate(() -> Stream.of(null, "value2", "value3", null, null, "value6", "value7")).flatMap(identity()).limit(10_000).iterator();
        Iterable<Map<Integer, String>> maps = createNullableTestMaps(intPrimitives.iterator(), stringPrimitives.iterator());
        List<List> values = createTestStructs(intPrimitives.iterator(), maps.iterator(), intPrimitives.iterator());
        List<String> structFieldNames = asList("intField1", "mapIntStringField", "intField2");

        Type structType = RowType.from(asList(field("intField1", INTEGER), field("mapIntStringField", mapType(INTEGER, VARCHAR)), field("intField2", INTEGER)));
        tester.testRoundTrip(getStandardStructObjectInspector(structFieldNames,
                        asList(
                                javaIntObjectInspector,
                                getStandardMapObjectInspector(javaIntObjectInspector, javaStringObjectInspector),
                                javaIntObjectInspector)),
                values, values, structType);
    }

    @Test
    public void testStructOfNullableArrayBetweenNonNullFields()
            throws Exception
    {
        Iterable<Integer> intPrimitives = intsBetween(0, 10_000);
        Iterable<String> stringPrimitives = () -> Stream.generate(() -> Stream.of(null, "value2", "value3", null, null, "value6", "value7"))
                .flatMap(identity()).limit(10_000).iterator();
        Iterable<List<String>> stringArrayField = createNullableTestArrays(stringPrimitives.iterator());
        List<List> values = createTestStructs(intPrimitives.iterator(), stringArrayField.iterator(), intPrimitives.iterator());
        List<String> structFieldNames = asList("intField1", "arrayStringField", "intField2");

        Type structType = RowType.from(asList(field("intField1", INTEGER), field("arrayStringField", new ArrayType(VARCHAR)), field("intField2", INTEGER)));
        tester.testRoundTrip(getStandardStructObjectInspector(structFieldNames,
                        asList(
                                javaIntObjectInspector,
                                getStandardListObjectInspector(javaStringObjectInspector),
                                javaIntObjectInspector)),
                values, values, structType);
    }

    @Test
    public void testStructOfArrayAndPrimitive()
            throws Exception
    {
        Iterable<List<String>> stringArrayField = createNullableTestArrays(
                intsBetween(0, 31_234).stream().map(Object::toString).iterator());
        List<List> values = createTestStructs(stringArrayField.iterator(),
                Stream.generate(() -> Stream.of(1, 3, 5, 7, 11, 13, 17)).flatMap(identity()).limit(31_234).iterator());
        List<String> structFieldNames = asList("stringArrayField", "intField");

        Type structType = RowType.from(asList(field("stringArrayField", new ArrayType(VARCHAR)), field("intField", INTEGER)));
        tester.testRoundTrip(getStandardStructObjectInspector(structFieldNames,
                asList(getStandardListObjectInspector(javaStringObjectInspector), javaIntObjectInspector)), values, values, structType);
    }

    @Test
    public void testStructOfSingleLevelArrayAndPrimitive()
            throws Exception
    {
        Iterable<List<String>> stringArrayField = createNullableTestArrays(
                intsBetween(0, 31_234).stream().map(Object::toString).iterator());
        List<List> values = createTestStructs(stringArrayField.iterator(),
                Stream.generate(() -> Stream.of(1, 3, 5, 7, 11, 13, 17)).flatMap(identity()).limit(31_234).iterator());
        List<String> structFieldNames = asList("stringArrayField", "intField");

        Type structType = RowType.from(asList(field("stringArrayField", new ArrayType(VARCHAR)), field("intField", INTEGER)));
        tester.testSingleLevelArraySchemaRoundTrip(getStandardStructObjectInspector(structFieldNames,
                asList(getStandardListObjectInspector(javaStringObjectInspector), javaIntObjectInspector)), values, values, structType);
    }

    @Test
    public void testStructOfPrimitiveAndArray()
            throws Exception
    {
        Iterable<List<String>> stringArrayField = createNullableTestArrays(
                intsBetween(0, 31_234).stream().map(Object::toString).iterator());
        Iterable<Integer> intField = () -> Stream.generate(() -> Stream.of(1, 3, 5, 7, 11, 13, 17))
                .flatMap(identity()).limit(31_234).iterator();
        List<List> values = createTestStructs(intField.iterator(), stringArrayField.iterator());
        List<String> structFieldNames = asList("intField", "stringArrayField");

        Type structType = RowType.from(asList(field("intField", INTEGER), field("stringArrayField", new ArrayType(VARCHAR))));
        tester.testRoundTrip(getStandardStructObjectInspector(structFieldNames,
                asList(javaIntObjectInspector, getStandardListObjectInspector(javaStringObjectInspector))), values, values, structType);
    }

    @Test
    public void testStructOfPrimitiveAndSingleLevelArray()
            throws Exception
    {
        Iterable<List<String>> stringArrayField = createNullableTestArrays(
                intsBetween(0, 31_234).stream().map(Object::toString).iterator());
        Iterable<Integer> intField = () -> Stream.generate(() -> Stream.of(1, 3, 5, 7, 11, 13, 17))
                .flatMap(identity()).limit(31_234).iterator();
        List<List> values = createTestStructs(intField.iterator(), stringArrayField.iterator());
        List<String> structFieldNames = asList("intField", "stringArrayField");

        Type structType = RowType.from(asList(field("intField", INTEGER), field("stringArrayField", new ArrayType(VARCHAR))));
        tester.testSingleLevelArraySchemaRoundTrip(getStandardStructObjectInspector(structFieldNames,
                asList(javaIntObjectInspector, getStandardListObjectInspector(javaStringObjectInspector))), values, values, structType);
    }

    @Test
    public void testStructOfTwoArrays()
            throws Exception
    {
        Iterable<List<Integer>> intArrayField = createNullableTestArrays(
                Stream.generate(() -> Stream.of(1, 3, 5, 7, 11, 13, 17)).flatMap(identity()).limit(30_000).iterator());
        Iterable<List<String>> stringArrayField = createNullableTestArrays(
                intsBetween(0, 30_000).stream().map(Object::toString).iterator());
        List<List> values = createTestStructs(stringArrayField.iterator(), intArrayField.iterator());
        List<String> structFieldNames = asList("stringArrayField", "intArrayField");

        Type structType = RowType.from(asList(field("stringArrayField", new ArrayType(VARCHAR)), field("intArrayField", new ArrayType(INTEGER))));
        tester.testRoundTrip(getStandardStructObjectInspector(structFieldNames,
                asList(getStandardListObjectInspector(javaStringObjectInspector), getStandardListObjectInspector(javaIntObjectInspector))), values, values, structType);
    }

    @Test
    public void testStructOfTwoNestedArrays()
            throws Exception
    {
        Iterable<List<List<Integer>>> intArrayField = createNullableTestArrays(createNullableTestArrays(
                Stream.generate(() -> Stream.of(1, 3, 5, 7, 11, 13, 17)).flatMap(identity()).limit(30_000).iterator()).iterator());
        Iterable<List<List<String>>> stringArrayField = createNullableTestArrays(createNullableTestArrays(
                intsBetween(0, 31_234).stream().map(Object::toString).iterator()).iterator());
        List<List> values = createTestStructs(stringArrayField.iterator(), intArrayField.iterator());
        List<String> structFieldNames = asList("stringArrayField", "intArrayField");
        Type structType = RowType.from(asList(field("stringArrayField", new ArrayType(new ArrayType(VARCHAR))), field("intArrayField", new ArrayType(new ArrayType(INTEGER)))));
        tester.testRoundTrip(getStandardStructObjectInspector(structFieldNames,
                        asList(
                                getStandardListObjectInspector(getStandardListObjectInspector(javaStringObjectInspector)),
                                getStandardListObjectInspector(getStandardListObjectInspector(javaIntObjectInspector)))),
                values, values, structType);
    }

    @Test
    public void testStructOfTwoNestedSingleLevelSchemaArrays()
            throws Exception
    {
        Iterable<List<List<Integer>>> intArrayField = createNullableTestArrays(createTestArrays(
                Stream.generate(() -> Stream.of(1, 3, 5, 7, 11, 13, 17))
                        .flatMap(identity())
                        .limit(30_000).iterator()).iterator());
        Iterable<List<List<String>>> stringArrayField = createNullableTestArrays(createTestArrays(
                intsBetween(0, 31_234).stream().map(Object::toString).iterator()).iterator());
        List<List> values = createTestStructs(stringArrayField.iterator(), intArrayField.iterator());
        List<String> structFieldNames = asList("stringArrayField", "intArrayField");

        Type structType = RowType.from(asList(field("stringArrayField", new ArrayType(new ArrayType(VARCHAR))), field("intArrayField", new ArrayType(new ArrayType(INTEGER)))));
        ObjectInspector objectInspector = getStandardStructObjectInspector(structFieldNames,
                asList(
                        getStandardListObjectInspector(getStandardListObjectInspector(javaStringObjectInspector)),
                        getStandardListObjectInspector(getStandardListObjectInspector(javaIntObjectInspector))));
        tester.testSingleLevelArraySchemaRoundTrip(objectInspector, values, values, structType);
    }

    @Test
    public void testBooleanSequence()
            throws Exception
    {
        tester.testRoundTrip(javaBooleanObjectInspector,
                () -> Stream.generate(() -> Stream.of(true, false, false))
                        .<Object>flatMap(identity()).limit(30_000).iterator(), BOOLEAN);
    }

    @Test
    public void testSmallIntSequence()
            throws Exception
    {
        List<Short> values = Stream.of(1, 2, 3, 4, 5)
                .map(Integer::shortValue).collect(toList());
        tester.testRoundTrip(javaShortObjectInspector, () -> Stream.generate(values::stream)
                .<Object>flatMap(identity()).limit(30_000).iterator(), SMALLINT);
    }

    @Test
    public void testLongSequence()
            throws Exception
    {
        testRoundTripNumeric(intsBetween(0, 31_234));
    }

    @Test
    public void testLongSequenceWithHoles()
            throws Exception
    {
        testRoundTripNumeric(skipEvery(5, intsBetween(0, 31_234)));
    }

    @Test
    public void testLongDirect()
            throws Exception
    {
        testRoundTripNumeric(() -> Stream.generate(() -> Stream.of(1, 3, 5, 7, 11, 13, 17)).flatMap(identity()).limit(30_000).iterator());
    }

    @Test
    public void testLongDirect2()
            throws Exception
    {
        List<Integer> values = new ArrayList<>(31_234);
        for (int i = 0; i < 31_234; i++) {
            values.add(i);
        }
        Collections.shuffle(values, new Random(0));
        testRoundTripNumeric(values);
    }

    @Test
    public void testLongShortRepeat()
            throws Exception
    {
        testRoundTripNumeric(() -> Streams.stream(repeatEach(4,
                () -> Stream.generate(() -> Stream.of(1, 3, 5, 7, 11, 13, 17))
                        .flatMap(identity()).iterator())).limit(30_000).iterator());
    }

    @Test
    public void testLongPatchedBase()
            throws Exception
    {
        testRoundTripNumeric(() -> Stream.generate(
                        () -> Stream.concat(
                                intsBetween(0, 18).stream(),
                                ImmutableList.of(30_000, 20_000).stream()))
                .flatMap(identity()).limit(30_000).iterator());
    }

    // copied from Parquet code to determine the max decimal precision supported by INT32/INT64
    private static long maxPrecision(int numBytes)
    {
        return Math.round(Math.floor(Math.log10(Math.pow(2, 8 * numBytes - 1) - 1)));
    }

    @Test
    public void testDecimalBackedByINT32()
            throws Exception
    {
        int[] scales = {0, 0, 2, 0, 0, 2, 5, 5, 7, 4};
        for (int precision = 1; precision <= MAX_PRECISION_INT32; precision++) {
            int scale = scales[precision];
            MessageType parquetSchema = parseMessageType(format("message hive_decimal { optional INT32 test (DECIMAL(%d, %d)); }", precision, scale));
            ContiguousSet<Integer> intValues = intsBetween(1, 1_000);
            ImmutableList.Builder<SqlDecimal> expectedValues = new ImmutableList.Builder<>();
            for (Integer value : intValues) {
                expectedValues.add(SqlDecimal.of(value, precision, scale));
            }
            tester.testRoundTrip(javaIntObjectInspector, intValues, expectedValues.build(), createDecimalType(precision, scale), Optional.of(parquetSchema));
        }
    }

    @Test
    public void testTimestampMicrosBackedByINT64()
            throws Exception
    {
        org.apache.parquet.schema.MessageType parquetSchema =
                MessageTypeParser.parseMessageType("message ts_micros { optional INT64 test (TIMESTAMP_MICROS); }");
        ContiguousSet<Long> longValues = longsBetween(1_000_000, 1_001_000);
        ImmutableList.Builder<SqlTimestamp> expectedValues = new ImmutableList.Builder<>();
        for (Long value : longValues) {
            expectedValues.add(new SqlTimestamp(value / 1000L, UTC_KEY, MILLISECONDS));
        }
        tester.testRoundTrip(javaTimestampObjectInspector, longValues, expectedValues.build(), TIMESTAMP, parquetSchema);
    }

    @Test
    public void testTimestampMillisBackedByINT64()
            throws Exception
    {
        MessageType parquetSchema = parseMessageType("message ts_millis { optional INT64 test (TIMESTAMP_MILLIS); }");
        ContiguousSet<Long> longValues = longsBetween(1_000_000, 1_001_000);
        ImmutableList.Builder<SqlTimestamp> expectedValues = new ImmutableList.Builder<>();
        for (Long value : longValues) {
            expectedValues.add(new SqlTimestamp(value, UTC_KEY, MILLISECONDS));
        }
        tester.testRoundTrip(javaLongObjectInspector, longValues, expectedValues.build(), TIMESTAMP, Optional.of(parquetSchema));
    }

    @Test
    public void testDecimalBackedByINT64()
            throws Exception
    {
        int[] scales = {9, 2, 4, 1, 6, 3, 7, 1, 7, 12};
        for (int precision = MAX_PRECISION_INT32 + 1; precision <= MAX_PRECISION_INT64; precision++) {
            int scale = scales[precision - MAX_PRECISION_INT32 - 1];
            MessageType parquetSchema = parseMessageType(format("message hive_decimal { optional INT64 test (DECIMAL(%d, %d)); }", precision, scale));
            ContiguousSet<Long> longValues = longsBetween(1, 1_000);
            ImmutableList.Builder<SqlDecimal> expectedValues = new ImmutableList.Builder<>();
            for (Long value : longValues) {
                expectedValues.add(SqlDecimal.of(value, precision, scale));
            }
            tester.testRoundTrip(javaLongObjectInspector, longValues, expectedValues.build(), createDecimalType(precision, scale), Optional.of(parquetSchema));
        }
    }

    @Test
    public void testRLEDecimalBackedByINT64()
            throws Exception
    {
        int[] scales = {9, 9, 9, 9, 9, 9, 9, 9, 9};
        for (int precision = MAX_PRECISION_INT32 + 1; precision <= MAX_PRECISION_INT64; precision++) {
            int scale = scales[precision - MAX_PRECISION_INT32 - 1];
            MessageType parquetSchema = parseMessageType(format("message hive_decimal { optional INT64 test (DECIMAL(%d, %d)); }", precision, scale));
            ContiguousSet<Long> longValues = longsBetween(1, 1_000);
            ImmutableList.Builder<SqlDecimal> expectedValues = new ImmutableList.Builder<>();
            for (Long value : longValues) {
                expectedValues.add(SqlDecimal.of(value, precision, scale));
            }
            tester.testRoundTrip(javaLongObjectInspector, longValues, expectedValues.build(), createDecimalType(precision, scale), Optional.of(parquetSchema));
        }
    }

    private void testDecimal(int precision, int scale, Optional<MessageType> parquetSchema)
            throws Exception
    {
        ContiguousSet<BigInteger> values = bigIntegersBetween(BigDecimal.valueOf(Math.pow(10, precision - 1)).toBigInteger(), BigDecimal.valueOf(Math.pow(10, precision)).toBigInteger());
        ImmutableList.Builder<SqlDecimal> expectedValues = new ImmutableList.Builder<>();
        ImmutableList.Builder<HiveDecimal> writeValues = new ImmutableList.Builder<>();
        values.stream().limit(1_000).forEach(value -> {
            writeValues.add(HiveDecimal.create(value, scale));
            expectedValues.add(new SqlDecimal(value, precision, scale));
        });
        tester.testRoundTrip(new JavaHiveDecimalObjectInspector(new DecimalTypeInfo(precision, scale)),
                writeValues.build(),
                expectedValues.build(),
                createDecimalType(precision, scale),
                parquetSchema);
    }

    @Test
    public void testShortDecimalBackedByFixedLenByteArray()
            throws Exception
    {
        int[] scales = {0, 0, 2, 2, 4, 5, 0, 1, 5, 7, 4, 8, 4, 4, 13, 11, 16, 15};
        for (int precision = 1; precision <= MAX_SHORT_PRECISION; precision++) {
            int scale = scales[precision - 1];
            testDecimal(precision, scale, Optional.empty());
        }
    }

    @Test
    public void testLongDecimalBackedByFixedLenByteArray()
            throws Exception
    {
        int[] scales = {7, 13, 14, 8, 16, 20, 8, 4, 19, 25, 15, 23, 17, 2, 23, 0, 33, 8, 3, 12};
        for (int precision = MAX_PRECISION_INT64 + 1; precision < MAX_PRECISION; precision++) {
            int scale = scales[precision - MAX_PRECISION_INT64 - 1];
            testDecimal(precision, scale, Optional.empty());
        }
    }

    @Test
    public void testShortDecimalBackedByBinary()
            throws Exception
    {
        int[] scales = {0, 0, 1, 2, 5, 4, 3, 4, 7, 6, 8, 9, 10, 1, 13, 11, 16, 15};
        for (int precision = 1; precision <= MAX_SHORT_PRECISION; precision++) {
            int scale = scales[precision - 1];
            MessageType parquetSchema = parseMessageType(format("message hive_decimal { optional BINARY test (DECIMAL(%d, %d)); }", precision, scale));
            testDecimal(precision, scale, Optional.of(parquetSchema));
        }
    }

    @Test
    public void testLongDecimalBackedByBinary()
            throws Exception
    {
        int[] scales = {1, 1, 7, 8, 22, 3, 15, 14, 7, 21, 6, 12, 1, 15, 14, 29, 17, 7, 26};
        for (int precision = MAX_PRECISION_INT64 + 1; precision < MAX_PRECISION; precision++) {
            int scale = scales[precision - MAX_PRECISION_INT64 - 1];
            MessageType parquetSchema = parseMessageType(format("message hive_decimal { optional BINARY test (DECIMAL(%d, %d)); }", precision, scale));
            testDecimal(precision, scale, Optional.of(parquetSchema));
        }
    }

    @Test
    public void testSchemaWithRepeatedOptionalRequiredFields()
            throws Exception
    {
        MessageType parquetSchema = parseMessageType("message hive_schema {" +
                "  optional group address_book {" +
                "    required binary owner (UTF8);" +
                "    optional group owner_phone_numbers (LIST) {" +
                "      repeated group bag {" +
                "        optional binary array_element (UTF8);" +
                "      }" +
                "    }" +
                "    optional group contacts (LIST) {" +
                "      repeated group bag {" +
                "        optional group array_element {" +
                "          required binary name (UTF8);" +
                "          optional binary phone_number (UTF8);" +
                "        }" +
                "      }" +
                "    }" +
                "  }" +
                "} ");

        Iterable<String> owner = () -> Stream.generate(() -> Stream.of("owner1", "owner2", "owner3")).flatMap(identity()).limit(50_000).iterator();
        Iterable<List<String>> ownerPhoneNumbers = () -> Stream.generate(() -> Stream.of(null, asList("phoneNumber2", "phoneNumber3", null), asList(null, "phoneNumber6", "phoneNumber7"))).flatMap(identity()).limit(50_000).iterator();
        Iterable<String> name = asList("name1", "name2", "name3", "name4", "name5", "name6", "name7");
        Iterable<String> phoneNumber = asList(null, "phoneNumber2", "phoneNumber3", null, null, "phoneNumber6", "phoneNumber7");
        Iterable<List> contact = createNullableTestStructs(name.iterator(), phoneNumber.iterator());
        Iterable<List<List>> contacts = createNullableTestArrays(Stream.generate(() -> Streams.stream(contact)).flatMap(identity()).limit(50_000).iterator());
        List<List> values = createTestStructs(owner.iterator(), ownerPhoneNumbers.iterator(), contacts.iterator());
        List<String> addressBookFieldNames = asList("owner", "owner_phone_numbers", "contacts");
        List<String> contactsFieldNames = asList("name", "phone_number");
        Type contactsType = new ArrayType(RowType.from(asList(field("name", VARCHAR), field("phone_number", VARCHAR))));
        Type addressBookType = RowType.from(asList(field("owner", VARCHAR), field("owner_phone_numbers", new ArrayType(VARCHAR)), field("contacts", contactsType)));
        tester.testRoundTrip(getStandardStructObjectInspector(addressBookFieldNames,
                        asList(
                                javaStringObjectInspector,
                                getStandardListObjectInspector(javaStringObjectInspector),
                                getStandardListObjectInspector(
                                        getStandardStructObjectInspector(contactsFieldNames, asList(javaStringObjectInspector, javaStringObjectInspector))))),
                values, values, "address_book", addressBookType, Optional.of(parquetSchema));
    }

    @Test
    public void testSchemaWithOptionalOptionalRequiredFields()
            throws Exception
    {
        MessageType parquetSchema = parseMessageType("message hive_schema {" +
                "  optional group a {" +
                "    optional group b {" +
                "      optional group c {" +
                "        required binary d (UTF8);" +
                "      }" +
                "    }" +
                "  }" +
                "} ");
        Type cType = RowType.from(singletonList(field("d", VARCHAR)));
        Type bType = RowType.from(singletonList(field("c", cType)));
        Type aType = RowType.from(singletonList(field("b", bType)));
        Iterable<String> dValues = asList("d1", "d2", "d3", "d4", "d5", "d6", "d7");
        Iterable<List> cValues = createNullableTestStructs(dValues.iterator());
        Iterable<List> bValues = createNullableTestStructs(cValues.iterator());
        List<List> aValues = createTestStructs(bValues);
        ObjectInspector cInspector = getStandardStructObjectInspector(singletonList("d"), singletonList(javaStringObjectInspector));
        ObjectInspector bInspector = getStandardStructObjectInspector(singletonList("c"), singletonList(cInspector));
        ObjectInspector aInspector = getStandardStructObjectInspector(singletonList("b"), singletonList(bInspector));
        tester.testRoundTrip(aInspector, aValues, aValues, "a", aType, Optional.of(parquetSchema));
    }

    @Test
    public void testSchemaWithOptionalRequiredOptionalFields()
            throws Exception
    {
        MessageType parquetSchema = parseMessageType("message hive_schema {" +
                "  optional group a {" +
                "    optional group b {" +
                "      required group c {" +
                "        optional int32 d;" +
                "      }" +
                "    }" +
                "  }" +
                "} ");
        Type cType = RowType.from(singletonList(field("d", INTEGER)));
        Type bType = RowType.from(singletonList(field("c", cType)));
        Type aType = RowType.from(singletonList(field("b", bType)));
        Iterable<Integer> dValues = asList(111, null, 333, 444, null, 666, 777);
        List<List> cValues = createTestStructs(dValues);
        Iterable<List> bValues = createNullableTestStructs(cValues.iterator());
        List<List> aValues = createTestStructs(bValues);
        ObjectInspector cInspector = getStandardStructObjectInspector(singletonList("d"), singletonList(javaIntObjectInspector));
        ObjectInspector bInspector = getStandardStructObjectInspector(singletonList("c"), singletonList(cInspector));
        ObjectInspector aInspector = getStandardStructObjectInspector(singletonList("b"), singletonList(bInspector));
        tester.testRoundTrip(aInspector, aValues, aValues, "a", aType, Optional.of(parquetSchema));
    }

    @Test
    public void testSchemaWithRequiredRequiredOptionalFields()
            throws Exception
    {
        MessageType parquetSchema = parseMessageType("message hive_schema {" +
                "  optional group a {" +
                "    required group b {" +
                "      required group c {" +
                "        optional int32 d;" +
                "      }" +
                "    }" +
                "  }" +
                "} ");
        Type cType = RowType.from(singletonList(field("d", INTEGER)));
        Type bType = RowType.from(singletonList(field("c", cType)));
        Type aType = RowType.from(singletonList(field("b", bType)));
        Iterable<Integer> dValues = asList(111, null, 333, 444, null, 666, 777);
        List<List> cValues = createTestStructs(dValues);
        List<List> bValues = createTestStructs(cValues);
        List<List> aValues = createTestStructs(bValues);
        ObjectInspector cInspector = getStandardStructObjectInspector(singletonList("d"), singletonList(javaIntObjectInspector));
        ObjectInspector bInspector = getStandardStructObjectInspector(singletonList("c"), singletonList(cInspector));
        ObjectInspector aInspector = getStandardStructObjectInspector(singletonList("b"), singletonList(bInspector));
        tester.testRoundTrip(aInspector, aValues, aValues, "a", aType, Optional.of(parquetSchema));
    }

    @Test
    public void testSchemaWithRequiredOptionalOptionalFields()
            throws Exception
    {
        MessageType parquetSchema = parseMessageType("message hive_schema {" +
                "  optional group a {" +
                "    required group b {" +
                "      optional group c {" +
                "        optional int32 d;" +
                "      }" +
                "    }" +
                "  }" +
                "} ");
        Type cType = RowType.from(singletonList(field("d", INTEGER)));
        Type bType = RowType.from(singletonList(field("c", cType)));
        Type aType = RowType.from(singletonList(field("b", bType)));
        Iterable<Integer> dValues = asList(111, null, 333, 444, null, 666, 777);
        Iterable<List> cValues = createNullableTestStructs(dValues.iterator());
        List<List> bValues = createTestStructs(cValues);
        List<List> aValues = createTestStructs(bValues);
        ObjectInspector cInspector = getStandardStructObjectInspector(singletonList("d"), singletonList(javaIntObjectInspector));
        ObjectInspector bInspector = getStandardStructObjectInspector(singletonList("c"), singletonList(cInspector));
        ObjectInspector aInspector = getStandardStructObjectInspector(singletonList("b"), singletonList(bInspector));
        tester.testRoundTrip(aInspector, aValues, aValues, "a", aType, Optional.of(parquetSchema));
    }

    @Test
    public void testSchemaWithRequiredOptionalRequiredFields()
            throws Exception
    {
        MessageType parquetSchema = parseMessageType("message hive_schema {" +
                "  optional group a {" +
                "    required group b {" +
                "      optional group c {" +
                "        required binary d (UTF8);" +
                "      }" +
                "    }" +
                "  }" +
                "} ");
        Type cType = RowType.from(singletonList(field("d", VARCHAR)));
        Type bType = RowType.from(singletonList(field("c", cType)));
        Type aType = RowType.from(singletonList(field("b", bType)));
        Iterable<String> dValues = asList("d1", "d2", "d3", "d4", "d5", "d6", "d7");
        Iterable<List> cValues = createNullableTestStructs(dValues.iterator());
        List<List> bValues = createTestStructs(cValues);
        List<List> aValues = createTestStructs(bValues);
        ObjectInspector cInspector = getStandardStructObjectInspector(singletonList("d"), singletonList(javaStringObjectInspector));
        ObjectInspector bInspector = getStandardStructObjectInspector(singletonList("c"), singletonList(cInspector));
        ObjectInspector aInspector = getStandardStructObjectInspector(singletonList("b"), singletonList(bInspector));
        tester.testRoundTrip(aInspector, aValues, aValues, "a", aType, Optional.of(parquetSchema));
    }

    @Test
    public void testSchemaWithRequiredStruct()
            throws Exception
    {
        MessageType parquetSchema = parseMessageType("message hive_schema {" +
                "  required group a {" +
                "    required group b {" +
                "        required binary c (UTF8);" +
                "        required int32 d;" +
                "    }" +
                "    required binary e (UTF8);" +
                "  }" +
                "} ");
        Type bType = RowType.from(asList(field("c", VARCHAR), field("d", INTEGER)));
        Type aType = RowType.from(asList(field("b", bType), field("e", VARCHAR)));
        Iterable<String> cValues = () -> Stream.generate(() -> Stream.of("c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7")).flatMap(identity()).limit(30000).iterator();
        Iterable<Integer> dValues = intsBetween(0, 30000);
        Iterable<String> eValues = () -> Stream.generate(() -> Stream.of("e0", "e1", "e2", "e3", "e4", "e5", "e6", "e7")).flatMap(identity()).limit(30000).iterator();
        List<List> bValues = createTestStructs(cValues.iterator(), dValues.iterator());
        List<List> aValues = createTestStructs(bValues.iterator(), eValues.iterator());
        ObjectInspector bInspector = getStandardStructObjectInspector(asList("c", "d"), asList(javaStringObjectInspector, javaIntObjectInspector));
        ObjectInspector aInspector = getStandardStructObjectInspector(asList("b", "e"), asList(bInspector, javaStringObjectInspector));
        tester.assertRoundTrip(singletonList(aInspector), new Iterable<?>[] {aValues}, new Iterable<?>[] {
                aValues}, singletonList("a"), singletonList(aType), Optional.of(parquetSchema));
    }

    @Test
    public void testSchemaWithRequiredOptionalRequired2Fields()
            throws Exception
    {
        MessageType parquetSchema = parseMessageType("message hive_schema {" +
                "  optional group a {" +
                "    required group b {" +
                "      optional group c {" +
                "        required binary d (UTF8);" +
                "      }" +
                "    }" +
                "  }" +
                "  optional group e {" +
                "    required group f {" +
                "      optional group g {" +
                "        required binary h (UTF8);" +
                "      }" +
                "    }" +
                "  }" +
                "} ");

        Type cType = RowType.from(singletonList(field("d", VARCHAR)));
        Type bType = RowType.from(singletonList(field("c", cType)));
        Type aType = RowType.from(singletonList(field("b", bType)));
        Iterable<String> dValues = asList("d1", "d2", "d3", "d4", "d5", "d6", "d7");
        Iterable<List> cValues = createNullableTestStructs(dValues.iterator());
        List<List> bValues = createTestStructs(cValues);
        List<List> aValues = createTestStructs(bValues);

        Type gType = RowType.from(singletonList(field("h", VARCHAR)));
        Type fType = RowType.from(singletonList(field("g", gType)));
        Type eType = RowType.from(singletonList(field("f", fType)));
        Iterable<String> hValues = asList("h1", "h2", "h3", "h4", "h5", "h6", "h7");
        Iterable<List> gValues = createNullableTestStructs(hValues.iterator());
        List<List> fValues = createTestStructs(gValues);
        List<List> eValues = createTestStructs(fValues);

        ObjectInspector cInspector = getStandardStructObjectInspector(singletonList("d"), singletonList(javaStringObjectInspector));
        ObjectInspector bInspector = getStandardStructObjectInspector(singletonList("c"), singletonList(cInspector));
        ObjectInspector aInspector = getStandardStructObjectInspector(singletonList("b"), singletonList(bInspector));
        ObjectInspector gInspector = getStandardStructObjectInspector(singletonList("h"), singletonList(javaStringObjectInspector));
        ObjectInspector fInspector = getStandardStructObjectInspector(singletonList("g"), singletonList(gInspector));
        ObjectInspector eInspector = getStandardStructObjectInspector(singletonList("f"), singletonList(fInspector));
        tester.testRoundTrip(asList(aInspector, eInspector),
                new Iterable<?>[] {aValues, eValues}, new Iterable<?>[] {aValues, eValues},
                asList("a", "e"), asList(aType, eType), Optional.of(parquetSchema), false);
    }

    @Test
    public void testOldAvroArray()
            throws Exception
    {
        MessageType parquetMrAvroSchema = parseMessageType("message avro_schema_old {" +
                "  optional group my_list (LIST){" +
                "        repeated int32 array;" +
                "  }" +
                "} ");
        Iterable<List<Integer>> nonNullArrayElements = createTestArrays(intsBetween(0, 31_234).stream().iterator());
        tester.testSingleLevelArrayRoundTrip(getStandardListObjectInspector(javaIntObjectInspector), nonNullArrayElements, nonNullArrayElements, "my_list", new ArrayType(INTEGER), Optional.of(parquetMrAvroSchema));
    }

    @Test
    public void testNewAvroArray()
            throws Exception
    {
        MessageType parquetMrAvroSchema = parseMessageType("message avro_schema_new { " +
                "  optional group my_list (LIST) { " +
                "    repeated group list { " +
                "      optional int32 element; " +
                "    } " +
                "  } " +
                "}");
        Iterable<List<Integer>> values = createTestArrays(Stream.generate(() -> Stream.of(1, null, 3, 5, null, null, null, 7, 11, null, 13, 17))
                .flatMap(identity()).limit(30_000).iterator());
        tester.testRoundTrip(getStandardListObjectInspector(javaIntObjectInspector), values, values, "my_list", new ArrayType(INTEGER), Optional.of(parquetMrAvroSchema));
    }

    /**
     * Test reading various arrays schemas compatible with spec
     * https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists
     */
    @Test
    public void testArraySchemas()
            throws Exception
    {
        MessageType parquetMrNullableSpecSchema = parseMessageType("message hive_schema {" +
                "  optional group my_list (LIST){" +
                "    repeated group list {" +
                "        required int32 element;" +
                "    }" +
                "  }" +
                "} ");
        Iterable<List<Integer>> nonNullArrayElements = createTestArrays(intsBetween(0, 31_234).stream().iterator());
        tester.testRoundTrip(getStandardListObjectInspector(javaIntObjectInspector), nonNullArrayElements, nonNullArrayElements, "my_list", new ArrayType(INTEGER), Optional.of(parquetMrNullableSpecSchema));

        MessageType parquetMrNonNullSpecSchema = parseMessageType("message hive_schema {" +
                "  required group my_list (LIST){" +
                "    repeated group list {" +
                "        optional int32 element;" +
                "    }" +
                "  }" +
                "} ");
        Iterable<List<Integer>> values = createTestArrays(
                Stream.generate(() -> Stream.of(1, null, 3, 5, null, null, null, 7, 11, null, 13, 17))
                        .flatMap(identity()).limit(30_000).iterator());
        tester.assertRoundTrip(singletonList(getStandardListObjectInspector(javaIntObjectInspector)), new Iterable<?>[] {values}, new Iterable<?>[] {
                values}, singletonList("my_list"), singletonList(new ArrayType(INTEGER)), Optional.of(parquetMrNonNullSpecSchema));

        MessageType sparkSchema = parseMessageType("message hive_schema {" +
                "  optional group my_list (LIST){" +
                "    repeated group list {" +
                "        optional int32 element;" +
                "    }" +
                "  }" +
                "} ");
        tester.testRoundTrip(getStandardListObjectInspector(javaIntObjectInspector), values, values, "my_list", new ArrayType(INTEGER), Optional.of(sparkSchema));

        MessageType hiveSchema = parseMessageType("message hive_schema {" +
                "  optional group my_list (LIST){" +
                "    repeated group bag {" +
                "        optional int32 array_element;" +
                "    }" +
                "  }" +
                "} ");
        tester.testRoundTrip(getStandardListObjectInspector(javaIntObjectInspector), values, values, "my_list", new ArrayType(INTEGER), Optional.of(hiveSchema));

        MessageType customNamingSchema = parseMessageType("message hive_schema {" +
                "  optional group my_list (LIST){" +
                "    repeated group bag {" +
                "        optional int32 array;" +
                "    }" +
                "  }" +
                "} ");
        tester.testRoundTrip(getStandardListObjectInspector(javaIntObjectInspector), values, values, "my_list", new ArrayType(INTEGER), Optional.of(customNamingSchema));
    }

    /**
     * Test reading various maps schemas compatible with spec
     * https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#maps
     */
    @Test
    public void testMapSchemas()
            throws Exception
    {
        Iterable<Map<String, Integer>> values = createTestMaps(
                intsBetween(0, 100_000).stream().map(Object::toString).iterator(),
                intsBetween(0, 10_000).stream().iterator());
        Iterable<Map<String, Integer>> nullableValues = createTestMaps(
                intsBetween(0, 30_000).stream().map(Object::toString).iterator(),
                Stream.generate(() -> Stream.of(1, null, 3, 5, null, null, null, 7, 11, null, 13, 17)).flatMap(identity()).limit(30_000).iterator());
        tester.testRoundTrip(getStandardMapObjectInspector(javaStringObjectInspector, javaIntObjectInspector), values, values, mapType(VARCHAR, INTEGER));

        // Map<String, Integer> (nullable map, non-null values)
        MessageType map = parseMessageType("message hive_schema {" +
                " optional group my_map (MAP) {" +
                "     repeated group map { " +
                "        required binary str (UTF8);   " +
                "        required int32 num;  " +
                "    }  " +
                "  }" +
                "}   ");
        tester.testRoundTrip(getStandardMapObjectInspector(javaStringObjectInspector, javaIntObjectInspector), values, values, "my_map", mapType(VARCHAR, INTEGER), Optional.of(map));

        // Map<String, Integer> (nullable map, non-null values)
        map = parseMessageType("message hive_schema {" +
                " optional group my_map (MAP_KEY_VALUE) {" +
                "     repeated group map { " +
                "        required binary str (UTF8);   " +
                "        required int32 num;  " +
                "    }  " +
                "  }" +
                "}   ");
        tester.testRoundTrip(getStandardMapObjectInspector(javaStringObjectInspector, javaIntObjectInspector), values, values, "my_map", mapType(VARCHAR, INTEGER), Optional.of(map));

        // Map<String, Integer> (non-null map, nullable values)
        map = parseMessageType("message hive_schema {" +
                " required group my_map (MAP) { " +
                "    repeated group map {  " +
                "        required binary key (UTF8);      " +
                "       optional int32 value;   " +
                "    }   " +
                "  }" +
                " }  ");
        tester.assertRoundTrip(singletonList(getStandardMapObjectInspector(javaStringObjectInspector, javaIntObjectInspector)), new Iterable<?>[] {nullableValues},
                new Iterable<?>[] {nullableValues}, singletonList("my_map"), singletonList(mapType(VARCHAR, INTEGER)), Optional.of(map));

        // Map<String, Integer> (non-null map, nullable values)
        map = parseMessageType("message hive_schema {" +
                " required group my_map (MAP_KEY_VALUE) { " +
                "    repeated group map {  " +
                "        required binary key (UTF8);      " +
                "       optional int32 value;   " +
                "    }   " +
                "  }" +
                " }  ");
        tester.assertRoundTrip(singletonList(getStandardMapObjectInspector(javaStringObjectInspector, javaIntObjectInspector)), new Iterable<?>[] {nullableValues},
                new Iterable<?>[] {nullableValues}, singletonList("my_map"), singletonList(mapType(VARCHAR, INTEGER)), Optional.of(map));

        // Map<String, Integer> (non-null map, nullable values)
        map = parseMessageType("message hive_schema {" +
                " required group my_map (MAP) { " +
                "    repeated group map {  " +
                "        required binary key (UTF8);      " +
                "       required int32 value;   " +
                "    }   " +
                "  }" +
                " }  ");
        tester.assertRoundTrip(singletonList(getStandardMapObjectInspector(javaStringObjectInspector, javaIntObjectInspector)), new Iterable<?>[] {values},
                new Iterable<?>[] {values}, singletonList("my_map"), singletonList(mapType(VARCHAR, INTEGER)), Optional.of(map));

        // Map<String, Integer> (non-null map, nullable values)
        map = parseMessageType("message hive_schema {" +
                " required group my_map (MAP_KEY_VALUE) { " +
                "    repeated group map {  " +
                "        required binary key (UTF8);      " +
                "       required int32 value;   " +
                "    }   " +
                "  }" +
                " }  ");
        tester.assertRoundTrip(singletonList(getStandardMapObjectInspector(javaStringObjectInspector, javaIntObjectInspector)), new Iterable<?>[] {values},
                new Iterable<?>[] {values}, singletonList("my_map"), singletonList(mapType(VARCHAR, INTEGER)), Optional.of(map));

        // Map<String, Integer> (nullable map, nullable values)
        map = parseMessageType("message hive_schema {" +
                " optional group my_map (MAP) { " +
                "    repeated group map {  " +
                "       required binary key (UTF8);      " +
                "       optional int32 value;   " +
                "    }   " +
                "  }" +
                " }  ");
        tester.testRoundTrip(getStandardMapObjectInspector(javaStringObjectInspector, javaIntObjectInspector), nullableValues, nullableValues, "my_map", mapType(VARCHAR, INTEGER), Optional.of(map));

        // Map<String, Integer> (nullable map, nullable values)
        map = parseMessageType("message hive_schema {" +
                " optional group my_map (MAP_KEY_VALUE) { " +
                "    repeated group map {  " +
                "       required binary key (UTF8);      " +
                "       optional int32 value;   " +
                "    }   " +
                "  }" +
                " }  ");
        tester.testRoundTrip(getStandardMapObjectInspector(javaStringObjectInspector, javaIntObjectInspector), nullableValues, nullableValues, "my_map", mapType(VARCHAR, INTEGER), Optional.of(map));
    }

    @Test
    public void testLongStrideDictionary()
            throws Exception
    {
        testRoundTripNumeric(() -> Streams.concat(
                ImmutableList.of(1).stream(),
                Collections.nCopies(9999, 123).stream(),
                ImmutableList.of(2).stream(),
                Collections.nCopies(9999, 123).stream()).iterator());
    }

    private void testRoundTripNumeric(Iterable<Integer> writeValues)
            throws Exception
    {
        tester.testRoundTrip(javaByteObjectInspector,
                () -> Streams.stream(writeValues).map(AbstractTestParquetReader::intToByte).iterator(),
                AbstractTestParquetReader::byteToInt,
                INTEGER);

        tester.testRoundTrip(javaShortObjectInspector,
                () -> Streams.stream(writeValues).map(AbstractTestParquetReader::intToShort).iterator(),
                AbstractTestParquetReader::shortToInt,
                INTEGER);

        tester.testRoundTrip(javaIntObjectInspector, writeValues, INTEGER);
        tester.testRoundTrip(javaLongObjectInspector, () -> Streams.stream(writeValues).<Object>map(AbstractTestParquetReader::intToLong).iterator(), BIGINT);
        tester.testRoundTrip(javaTimestampObjectInspector,
                () -> Streams.stream(writeValues).<Object>map(AbstractTestParquetReader::intToTimestamp).iterator(),
                () -> Streams.stream(writeValues).<Object>map(AbstractTestParquetReader::intToSqlTimestamp).iterator(),
                TIMESTAMP);

        tester.testRoundTrip(javaDateObjectInspector,
                () -> Streams.stream(writeValues).<Object>map(AbstractTestParquetReader::intToDate).iterator(),
                () -> Streams.stream(writeValues).<Object>map(AbstractTestParquetReader::intToSqlDate).iterator(),
                DATE);
    }

    @Test
    public void testFloatSequence()
            throws Exception
    {
        tester.testRoundTrip(javaFloatObjectInspector, floatSequence(0.0f, 0.1f, 30_000), REAL);
    }

    @Test
    public void testFloatNaNInfinity()
            throws Exception
    {
        tester.testRoundTrip(javaFloatObjectInspector, ImmutableList.of(1000.0f, -1.23f, Float.POSITIVE_INFINITY), REAL);
        tester.testRoundTrip(javaFloatObjectInspector, ImmutableList.of(-1000.0f, Float.NEGATIVE_INFINITY, 1.23f), REAL);
        tester.testRoundTrip(javaFloatObjectInspector, ImmutableList.of(0.0f, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY), REAL);

        tester.testRoundTrip(javaFloatObjectInspector, ImmutableList.of(Float.NaN, -0.0f, 1.0f), REAL);
        tester.testRoundTrip(javaFloatObjectInspector, ImmutableList.of(Float.NaN, -1.0f, Float.POSITIVE_INFINITY), REAL);
        tester.testRoundTrip(javaFloatObjectInspector, ImmutableList.of(Float.NaN, Float.NEGATIVE_INFINITY, 1.0f), REAL);
        tester.testRoundTrip(javaFloatObjectInspector, ImmutableList.of(Float.NaN, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY), REAL);
    }

    @Test
    public void testDoubleSequence()
            throws Exception
    {
        tester.testRoundTrip(javaDoubleObjectInspector, doubleSequence(0, 0.1, 30_000), DOUBLE);
    }

    @Test
    public void testDoubleNaNInfinity()
            throws Exception
    {
        tester.testRoundTrip(javaDoubleObjectInspector, ImmutableList.of(1000.0, -1.0, Double.POSITIVE_INFINITY), DOUBLE);
        tester.testRoundTrip(javaDoubleObjectInspector, ImmutableList.of(-1000.0, Double.NEGATIVE_INFINITY, 1.0), DOUBLE);
        tester.testRoundTrip(javaDoubleObjectInspector, ImmutableList.of(0.0, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY), DOUBLE);

        tester.testRoundTrip(javaDoubleObjectInspector, ImmutableList.of(Double.NaN, -1.0, 1.0), DOUBLE);
        tester.testRoundTrip(javaDoubleObjectInspector, ImmutableList.of(Double.NaN, -1.0, Double.POSITIVE_INFINITY), DOUBLE);
        tester.testRoundTrip(javaDoubleObjectInspector, ImmutableList.of(Double.NaN, Double.NEGATIVE_INFINITY, 1.0), DOUBLE);
        tester.testRoundTrip(javaDoubleObjectInspector, ImmutableList.of(Double.NaN, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY), DOUBLE);
    }

    @Test
    public void testStringUnicode()
            throws Exception
    {
        tester.testRoundTrip(javaStringObjectInspector, () -> Stream.generate(() -> Stream.of("apple", "apple pie", "apple\uD835\uDC03", "apple\uFFFD"))
                        .<Object>flatMap(identity()).limit(30_000).iterator(),
                createUnboundedVarcharType());
    }

    @Test
    public void testStringDirectSequence()
            throws Exception
    {
        tester.testRoundTrip(javaStringObjectInspector,
                () -> intsBetween(0, 30_000).stream().<Object>map(Object::toString).iterator(),
                createUnboundedVarcharType());
    }

    @Test
    public void testStringDictionarySequence()
            throws Exception
    {
        tester.testRoundTrip(javaStringObjectInspector,
                () -> Stream.generate(() -> Stream.of(1, 3, 5, 7, 11, 13, 17))
                        .flatMap(identity()).<Object>map(Object::toString).limit(30_000).iterator(),
                createUnboundedVarcharType());
    }

    @Test
    public void testStringStrideDictionary()
            throws Exception
    {
        tester.testRoundTrip(javaStringObjectInspector,
                () -> Streams.<Object>concat(ImmutableList.of("a").stream(),
                                Collections.nCopies(9999, "123").stream(),
                                ImmutableList.of("b").stream(),
                                Collections.nCopies(9999, "123").stream())
                        .iterator(),
                createUnboundedVarcharType());
    }

    @Test
    public void testEmptyStringSequence()
            throws Exception
    {
        tester.testRoundTrip(javaStringObjectInspector,
                () -> Stream.<Object>generate(() -> "").limit(30_000).iterator(),
                createUnboundedVarcharType());
    }

    @Test
    public void testBinaryDirectSequence()
            throws Exception
    {
        Iterable<byte[]> writeValues = intsBetween(0, 30_000)
                .stream()
                .map(Object::toString)
                .map(AbstractTestParquetReader::stringToByteArray)
                .toList();
        tester.testRoundTrip(javaByteArrayObjectInspector,
                writeValues,
                () -> Streams.stream(writeValues).<Object>map(AbstractTestParquetReader::byteArrayToVarbinary).iterator(),
                VARBINARY);
    }

    @Test
    public void testBinaryDictionarySequence()
            throws Exception
    {
        Iterable<byte[]> writeValues = Stream.generate(() -> ImmutableList.of(1, 3, 5, 7, 11, 13, 17)
                        .stream()
                        .map(Object::toString)
                        .map(AbstractTestParquetReader::stringToByteArray))
                .flatMap(identity())
                .limit(30_000)
                .collect(toList());
        tester.testRoundTrip(javaByteArrayObjectInspector,
                writeValues,
                () -> Streams.stream(writeValues).<Object>map(AbstractTestParquetReader::byteArrayToVarbinary).iterator(),
                VARBINARY);
    }

    @Test
    public void testEmptyBinarySequence()
            throws Exception
    {
        tester.testRoundTrip(javaByteArrayObjectInspector,
                () -> Stream.generate(() -> new byte[0]).limit(30_000).iterator(),
                AbstractTestParquetReader::byteArrayToVarbinary, VARBINARY);
    }

    private static <T> Iterable<T> skipEvery(int n, Iterable<T> iterable)
    {
        return () -> new AbstractIterator<T>()
        {
            private final Iterator<T> delegate = iterable.iterator();
            private int position;

            @Override
            protected T computeNext()
            {
                while (true) {
                    if (!delegate.hasNext()) {
                        return endOfData();
                    }

                    T next = delegate.next();
                    position++;
                    if (position <= n) {
                        return next;
                    }
                    position = 0;
                }
            }
        };
    }

    @Test
    public void testStructMaxReadBytes()
            throws Exception
    {
        DataSize maxReadBlockSize = new DataSize(1_000, DataSize.Unit.BYTE);
        List<List> structValues = createTestStructs(
                Collections.nCopies(500, String.join("", Collections.nCopies(33, "test"))).iterator(),
                Collections.nCopies(500, String.join("", Collections.nCopies(1, "test"))).iterator());
        List<String> structFieldNames = asList("a", "b");
        Type structType = RowType.from(asList(field("a", VARCHAR), field("b", VARCHAR)));

        tester.testMaxReadBytes(
                getStandardStructObjectInspector(structFieldNames, asList(javaStringObjectInspector, javaStringObjectInspector)),
                structValues,
                structValues,
                structType,
                maxReadBlockSize);
    }

    @Test
    public void testNullableNullCount()
    {
        PrimitiveType primitiveType = new PrimitiveType(OPTIONAL, BINARY, "testColumn");
        Statistics statistics = new Statistics();
        assertEquals(MetadataReader.readStats(statistics, primitiveType.getPrimitiveTypeName()).getNumNulls(), -1);
        statistics.setNull_count(10);
        assertEquals(MetadataReader.readStats(statistics, primitiveType.getPrimitiveTypeName()).getNumNulls(), 10);
    }

    @Test
    public void testArrayMaxReadBytes()
            throws Exception
    {
        DataSize maxReadBlockSize = new DataSize(1_000, DataSize.Unit.BYTE);
        Iterable<List<Integer>> values = createFixedTestArrays(Stream.generate(() -> Stream.of(1, null, 3, 5, null, null, null, 7, 11, null, 13, 17))
                .flatMap(identity()).limit(30_000).iterator());
        tester.testMaxReadBytes(getStandardListObjectInspector(javaIntObjectInspector), values, values, new ArrayType(INTEGER), maxReadBlockSize);
    }

    @Test
    public void testMapMaxReadBytes()
            throws Exception
    {
        DataSize maxReadBlockSize = new DataSize(1_000, DataSize.Unit.BYTE);
        Iterable<Map<String, Long>> values = createFixedTestMaps(
                Collections.nCopies(5_000, String.join("", Collections.nCopies(33, "test"))).iterator(),
                longsBetween(0, 5_000).stream().iterator());
        tester.testMaxReadBytes(getStandardMapObjectInspector(javaStringObjectInspector, javaLongObjectInspector), values, values, mapType(VARCHAR, BIGINT), maxReadBlockSize);
    }

    @Test
    public void testCaching()
            throws Exception
    {
        Cache<ParquetDataSourceId, ParquetFileMetadata> parquetFileMetadataCache = CacheBuilder.newBuilder()
                .maximumWeight(new DataSize(1, MEGABYTE).toBytes())
                .weigher((id, metadata) -> ((ParquetFileMetadata) metadata).getMetadataSize())
                .expireAfterAccess(new Duration(10, MINUTES).toMillis(), MILLISECONDS)
                .recordStats()
                .build();
        ParquetMetadataSource parquetMetadataSource = new CachingParquetMetadataSource(parquetFileMetadataCache, new MetadataReader());

        try (ParquetTester.TempFile tempFile = new ParquetTester.TempFile("test", "parquet")) {
            Iterable<Integer> values = intsBetween(0, 10);

            List<String> columnNames = singletonList("column1");
            List<Type> columnTypes = singletonList(INTEGER);
            writeParquetFileFromPresto(tempFile.getFile(),
                    columnTypes,
                    columnNames,
                    new Iterable<?>[] {values},
                    10,
                    CompressionCodecName.GZIP,
                    ParquetWriterOptions.DEFAULT_WRITER_VERSION);
            long tempFileCreationTime = System.currentTimeMillis();

            testSingleRead(new Iterable<?>[] {values},
                    columnNames,
                    columnTypes,
                    parquetMetadataSource,
                    tempFile.getFile(),
                    tempFileCreationTime);
            assertEquals(parquetFileMetadataCache.stats().missCount(), 1);
            assertEquals(parquetFileMetadataCache.stats().hitCount(), 0);

            testSingleRead(new Iterable<?>[] {values},
                    columnNames,
                    columnTypes,
                    parquetMetadataSource,
                    tempFile.getFile(),
                    tempFileCreationTime);
            assertEquals(parquetFileMetadataCache.stats().missCount(), 1);
            assertEquals(parquetFileMetadataCache.stats().hitCount(), 1);

            testSingleRead(new Iterable<?>[] {values},
                    columnNames,
                    columnTypes,
                    parquetMetadataSource,
                    tempFile.getFile(),
                    tempFileCreationTime);
            assertEquals(parquetFileMetadataCache.stats().missCount(), 1);
            assertEquals(parquetFileMetadataCache.stats().hitCount(), 2);

            parquetFileMetadataCache.invalidateAll();

            testSingleRead(new Iterable<?>[] {values},
                    columnNames,
                    columnTypes,
                    parquetMetadataSource,
                    tempFile.getFile(),
                    tempFileCreationTime);
            assertEquals(parquetFileMetadataCache.stats().missCount(), 2);
            assertEquals(parquetFileMetadataCache.stats().hitCount(), 2);

            testSingleRead(new Iterable<?>[] {values},
                    columnNames,
                    columnTypes,
                    parquetMetadataSource,
                    tempFile.getFile(),
                    tempFileCreationTime);
            assertEquals(parquetFileMetadataCache.stats().missCount(), 2);
            assertEquals(parquetFileMetadataCache.stats().hitCount(), 3);

            // change the modification time, and set it into a new HiveFileContext to invalidate this cache
            // the cache will hit the first time(with its last modification time), but will be invalidated and not returned
            // the real metadata result will be gotten from the delegate i.e. MetadataReader
            long tempFileModificationTime = System.currentTimeMillis();
            testSingleRead(new Iterable<?>[] {values},
                    columnNames,
                    columnTypes,
                    parquetMetadataSource,
                    tempFile.getFile(),
                    tempFileModificationTime);
            assertEquals(parquetFileMetadataCache.stats().missCount(), 2);
            assertEquals(parquetFileMetadataCache.stats().hitCount(), 4);

            //because the cache is invalidated above, the miss count will be incremented
            testSingleRead(new Iterable<?>[] {values},
                    columnNames,
                    columnTypes,
                    parquetMetadataSource,
                    tempFile.getFile(),
                    tempFileModificationTime);
            assertEquals(parquetFileMetadataCache.stats().missCount(), 2);
            assertEquals(parquetFileMetadataCache.stats().hitCount(), 5);
        }
    }

    private static <T> Iterable<T> repeatEach(int n, Iterable<T> iterable)
    {
        return () -> new AbstractIterator<T>()
        {
            private final Iterator<T> delegate = iterable.iterator();
            private int position;
            private T value;

            @Override
            protected T computeNext()
            {
                if (position == 0) {
                    if (!delegate.hasNext()) {
                        return endOfData();
                    }
                    value = delegate.next();
                }

                position++;
                if (position >= n) {
                    position = 0;
                }
                return value;
            }
        };
    }

    private static Iterable<Float> floatSequence(double start, double step, int items)
    {
        return () -> Streams.stream(doubleSequence(start, step, items)).map(input -> {
            if (input == null) {
                return null;
            }
            return input.floatValue();
        }).iterator();
    }

    private static Iterable<Double> doubleSequence(double start, double step, int items)
    {
        return () -> new AbstractSequentialIterator<Double>(start)
        {
            private int item;

            @Override
            protected Double computeNext(Double previous)
            {
                if (item >= items) {
                    return null;
                }
                item++;
                return previous + step;
            }
        };
    }

    private static ContiguousSet<Integer> intsBetween(int lowerInclusive, int upperExclusive)
    {
        return ContiguousSet.create(Range.closedOpen(lowerInclusive, upperExclusive), DiscreteDomain.integers());
    }

    private static ContiguousSet<Long> longsBetween(long lowerInclusive, long upperExclusive)
    {
        return ContiguousSet.create(Range.closedOpen(lowerInclusive, upperExclusive), DiscreteDomain.longs());
    }

    private static ContiguousSet<BigInteger> bigIntegersBetween(BigInteger lowerInclusive, BigInteger upperExclusive)
    {
        return ContiguousSet.create(Range.closedOpen(lowerInclusive, upperExclusive), DiscreteDomain.bigIntegers());
    }

    private <F> List<List> createTestStructs(Iterable<F> fieldValues)
    {
        checkArgument(fieldValues.iterator().hasNext(), "struct field values cannot be empty");
        List<List> structs = new ArrayList<>();
        for (F field : fieldValues) {
            structs.add(singletonList(field));
        }
        return structs;
    }

    private List<List> createTestStructs(Iterator<?>... values)
    {
        List<List> structs = new ArrayList<>();
        List<Iterator> iterators = Arrays.stream(values).collect(toList());
        iterators.forEach(iter -> checkArgument(iter.hasNext(), "struct field values cannot be empty"));
        while (iterators.stream().allMatch(Iterator::hasNext)) {
            structs.add(iterators.stream().map(Iterator::next).collect(toList()));
        }
        return structs;
    }

    private Iterable<List> createNullableTestStructs(Iterator<?>... values)
    {
        return insertNullEvery(ThreadLocalRandom.current().nextInt(2, 5), createTestStructs(values));
    }

    private <T> List<List<T>> createTestArrays(Iterator<T> values)
    {
        List<List<T>> arrays = new ArrayList<>();
        List<T> array = new ArrayList<>();
        while (values.hasNext()) {
            if (ThreadLocalRandom.current().nextBoolean()) {
                arrays.add(array);
                array = new ArrayList<>();
            }
            if (ThreadLocalRandom.current().nextInt(10) == 0) {
                arrays.add(Collections.emptyList());
            }
            array.add(values.next());
        }
        return arrays;
    }

    private <T> Iterable<List<T>> createNullableTestArrays(Iterator<T> values)
    {
        return insertNullEvery(ThreadLocalRandom.current().nextInt(2, 5), createTestArrays(values));
    }

    private <T> List<List<T>> createFixedTestArrays(Iterator<T> values)
    {
        List<List<T>> arrays = new ArrayList<>();
        List<T> array = new ArrayList<>();
        int count = 1;
        while (values.hasNext()) {
            if (count % 10 == 0) {
                arrays.add(array);
                array = new ArrayList<>();
            }
            if (count % 20 == 0) {
                arrays.add(Collections.emptyList());
            }
            array.add(values.next());
            ++count;
        }
        return arrays;
    }

    private <K, V> Iterable<Map<K, V>> createFixedTestMaps(Iterator<K> keys, Iterator<V> values)
    {
        List<Map<K, V>> maps = new ArrayList<>();
        Map<K, V> map = new HashMap<>();
        int count = 1;
        while (keys.hasNext() && keys.hasNext()) {
            if (count % 5 == 0) {
                maps.add(map);
                map = new HashMap<>();
            }
            if (count % 10 == 0) {
                maps.add(Collections.emptyMap());
            }
            map.put(keys.next(), values.next());
            ++count;
        }
        return maps;
    }

    private <K, V> Iterable<Map<K, V>> createTestMaps(Iterator<K> keys, Iterator<V> values)
    {
        List<Map<K, V>> maps = new ArrayList<>();
        Map<K, V> map = new HashMap<>();
        while (keys.hasNext() && values.hasNext()) {
            if (ThreadLocalRandom.current().nextInt(5) == 0) {
                maps.add(map);
                map = new HashMap<>();
            }
            if (ThreadLocalRandom.current().nextInt(10) == 0) {
                maps.add(Collections.emptyMap());
            }
            map.put(keys.next(), values.next());
        }
        return maps;
    }

    private <K, V> Iterable<Map<K, V>> createNullableTestMaps(Iterator<K> keys, Iterator<V> values)
    {
        return insertNullEvery(ThreadLocalRandom.current().nextInt(2, 5), createTestMaps(keys, values));
    }

    private static Byte intToByte(Integer input)
    {
        if (input == null) {
            return null;
        }
        return input.byteValue();
    }

    private static Short intToShort(Integer input)
    {
        if (input == null) {
            return null;
        }
        return Shorts.checkedCast(input);
    }

    private static Integer byteToInt(Byte input)
    {
        return toInteger(input);
    }

    private static Integer shortToInt(Short input)
    {
        return toInteger(input);
    }

    private static Long intToLong(Integer input)
    {
        return toLong(input);
    }

    private static <N extends Number> Integer toInteger(N input)
    {
        if (input == null) {
            return null;
        }
        return input.intValue();
    }

    private static <N extends Number> Long toLong(N input)
    {
        if (input == null) {
            return null;
        }
        return input.longValue();
    }

    private static byte[] stringToByteArray(String input)
    {
        return input.getBytes(UTF_8);
    }

    private static SqlVarbinary byteArrayToVarbinary(byte[] input)
    {
        if (input == null) {
            return null;
        }
        return new SqlVarbinary(input);
    }

    private static Timestamp intToTimestamp(Integer input)
    {
        if (input == null) {
            return null;
        }
        Timestamp timestamp = new Timestamp(0);
        long seconds = (input / 1000);
        int nanos = ((input % 1000) * 1_000_000);

        // add some junk nanos to the timestamp, which will be truncated
        nanos += 888_888;

        if (nanos < 0) {
            nanos += 1_000_000_000;
            seconds -= 1;
        }
        timestamp.setTime(seconds * 1000);
        timestamp.setNanos(nanos);
        return timestamp;
    }

    protected static SqlTimestamp intToSqlTimestamp(Integer input)
    {
        if (input == null) {
            return null;
        }
        return sqlTimestampOf(input, SESSION);
    }

    private static Date intToDate(Integer input)
    {
        if (input == null) {
            return null;
        }
        return Date.valueOf(LocalDate.ofEpochDay(input));
    }

    private static SqlDate intToSqlDate(Integer input)
    {
        if (input == null) {
            return null;
        }
        return new SqlDate(input);
    }
}

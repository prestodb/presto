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
package com.facebook.presto.orc;

import com.facebook.hive.orc.OrcConf;
import com.facebook.hive.orc.lazy.OrcLazyObject;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.io.OutputStreamDataSink;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.NamedTypeSignature;
import com.facebook.presto.common.type.RowFieldName;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.SqlDate;
import com.facebook.presto.common.type.SqlDecimal;
import com.facebook.presto.common.type.SqlTimestamp;
import com.facebook.presto.common.type.SqlVarbinary;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.orc.TrackingTupleDomainFilter.TestBigintRange;
import com.facebook.presto.orc.TrackingTupleDomainFilter.TestDoubleRange;
import com.facebook.presto.orc.TupleDomainFilter.BigintRange;
import com.facebook.presto.orc.TupleDomainFilter.DoubleRange;
import com.facebook.presto.orc.cache.OrcFileTailSource;
import com.facebook.presto.orc.cache.StorageOrcFileTailSource;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.google.common.base.Functions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcFile.ReaderOptions;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.io.orc.OrcUtil;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaHiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.joda.time.DateTimeZone;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.Chars.truncateToLengthAndTrimSpaces;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.Decimals.rescale;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.Varchars.truncateToLength;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.orc.NoopOrcAggregatedMemoryContext.NOOP_ORC_AGGREGATED_MEMORY_CONTEXT;
import static com.facebook.presto.orc.OrcReader.MAX_BATCH_SIZE;
import static com.facebook.presto.orc.OrcTester.Format.DWRF;
import static com.facebook.presto.orc.OrcTester.Format.ORC_11;
import static com.facebook.presto.orc.OrcTester.Format.ORC_12;
import static com.facebook.presto.orc.OrcWriteValidation.OrcWriteValidationMode.BOTH;
import static com.facebook.presto.orc.TestingOrcPredicate.createOrcPredicate;
import static com.facebook.presto.orc.TupleDomainFilter.IS_NOT_NULL;
import static com.facebook.presto.orc.TupleDomainFilter.IS_NULL;
import static com.facebook.presto.orc.metadata.CompressionKind.LZ4;
import static com.facebook.presto.orc.metadata.CompressionKind.NONE;
import static com.facebook.presto.orc.metadata.CompressionKind.SNAPPY;
import static com.facebook.presto.orc.metadata.CompressionKind.ZLIB;
import static com.facebook.presto.orc.metadata.CompressionKind.ZSTD;
import static com.facebook.presto.orc.metadata.KeyProvider.UNKNOWN;
import static com.facebook.presto.testing.DateTimeTestingUtils.sqlTimestampOf;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Lists.newArrayList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.airlift.units.DataSize.succinctBytes;
import static java.lang.Math.toIntExact;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.serde2.ColumnProjectionUtils.READ_ALL_COLUMNS;
import static org.apache.hadoop.hive.serde2.ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardListObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardMapObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector;
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
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getCharTypeInfo;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class OrcTester
{
    public static final DataSize MAX_BLOCK_SIZE = new DataSize(1, Unit.MEGABYTE);
    public static final DateTimeZone HIVE_STORAGE_TIME_ZONE = DateTimeZone.forID("America/Bahia_Banderas");

    private static final boolean LEGACY_MAP_SUBSCRIPT = true;
    private static final FunctionAndTypeManager FUNCTION_AND_TYPE_MANAGER = createTestFunctionAndTypeManager();
    private static final List<Integer> PRIME_NUMBERS = ImmutableList.of(5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97);

    public enum Format
    {
        ORC_12(OrcEncoding.ORC) {
            @Override
            @SuppressWarnings("deprecation")
            public Serializer createSerializer()
            {
                return new OrcSerde();
            }
        },
        ORC_11(OrcEncoding.ORC) {
            @Override
            @SuppressWarnings("deprecation")
            public Serializer createSerializer()
            {
                return new OrcSerde();
            }
        },
        DWRF(OrcEncoding.DWRF) {
            @Override
            public boolean supportsType(Type type)
            {
                return !hasType(type, ImmutableSet.of(StandardTypes.DATE, StandardTypes.DECIMAL, StandardTypes.CHAR));
            }

            @Override
            @SuppressWarnings("deprecation")
            public Serializer createSerializer()
            {
                return new com.facebook.hive.orc.OrcSerde();
            }
        };

        private final OrcEncoding orcEncoding;

        Format(OrcEncoding orcEncoding)
        {
            this.orcEncoding = requireNonNull(orcEncoding, "orcEncoding is null");
        }

        public OrcEncoding getOrcEncoding()
        {
            return orcEncoding;
        }

        public boolean supportsType(Type type)
        {
            return true;
        }

        @SuppressWarnings("deprecation")
        public abstract Serializer createSerializer();
    }

    private boolean structTestsEnabled;
    private boolean mapTestsEnabled;
    private boolean listTestsEnabled;
    private boolean complexStructuralTestsEnabled;
    private boolean structuralNullTestsEnabled;
    private boolean reverseTestsEnabled;
    private boolean nullTestsEnabled;
    private boolean missingStructFieldsTestsEnabled;
    private boolean skipBatchTestsEnabled;
    private boolean skipStripeTestsEnabled;
    private boolean dwrfEncryptionEnabled;
    private Set<Format> formats = ImmutableSet.of();
    private Set<CompressionKind> compressions = ImmutableSet.of();
    private boolean useSelectiveOrcReader;

    public static OrcTester quickOrcTester()
    {
        OrcTester orcTester = new OrcTester();
        orcTester.structTestsEnabled = true;
        orcTester.mapTestsEnabled = true;
        orcTester.listTestsEnabled = true;
        orcTester.nullTestsEnabled = true;
        orcTester.missingStructFieldsTestsEnabled = true;
        orcTester.skipBatchTestsEnabled = true;
        orcTester.formats = ImmutableSet.of(ORC_12, ORC_11, DWRF);
        orcTester.compressions = ImmutableSet.of(ZLIB);
        orcTester.dwrfEncryptionEnabled = true;
        return orcTester;
    }

    public static OrcTester fullOrcTester()
    {
        OrcTester orcTester = new OrcTester();
        orcTester.structTestsEnabled = true;
        orcTester.mapTestsEnabled = true;
        orcTester.listTestsEnabled = true;
        orcTester.complexStructuralTestsEnabled = true;
        orcTester.structuralNullTestsEnabled = true;
        orcTester.reverseTestsEnabled = true;
        orcTester.nullTestsEnabled = true;
        orcTester.missingStructFieldsTestsEnabled = true;
        orcTester.skipBatchTestsEnabled = true;
        orcTester.skipStripeTestsEnabled = true;
        orcTester.formats = ImmutableSet.copyOf(Format.values());
        orcTester.compressions = ImmutableSet.of(NONE, SNAPPY, ZLIB, LZ4, ZSTD);
        orcTester.dwrfEncryptionEnabled = true;
        return orcTester;
    }

    public static OrcTester quickSelectiveOrcTester()
    {
        OrcTester orcTester = new OrcTester();
        orcTester.listTestsEnabled = true;
        orcTester.structTestsEnabled = true;
        orcTester.nullTestsEnabled = true;
        orcTester.skipBatchTestsEnabled = true;
        orcTester.formats = ImmutableSet.of(ORC_12, ORC_11, DWRF);
        orcTester.compressions = ImmutableSet.of(ZLIB, ZSTD);
        orcTester.useSelectiveOrcReader = true;

        return orcTester;
    }

    public void testRoundTrip(Type type, List<?> readValues)
            throws Exception
    {
        testRoundTrip(type, readValues, ImmutableList.of());
    }

    public void testRoundTrip(Type type, List<?> readValues, TupleDomainFilter... filters)
            throws Exception
    {
        testRoundTrip(type, readValues, Arrays.stream(filters).map(filter -> ImmutableMap.of(new Subfield("c"), filter)).collect(toImmutableList()));
    }

    public void testRoundTrip(Type type, List<?> readValues, List<Map<Subfield, TupleDomainFilter>> filters)
            throws Exception
    {
        List<Map<Integer, Map<Subfield, TupleDomainFilter>>> columnFilters = filters.stream().map(filter -> ImmutableMap.of(0, filter)).collect(toImmutableList());

        // just the values
        testRoundTripTypes(ImmutableList.of(type), ImmutableList.of(readValues), columnFilters);

        // all nulls
        if (nullTestsEnabled) {
            assertRoundTrip(
                    type,
                    readValues.stream()
                            .map(value -> null)
                            .collect(toList()),
                    columnFilters);
        }

        // values wrapped in struct
        if (structTestsEnabled) {
            testStructRoundTrip(type, readValues);
        }

        // values wrapped in a struct wrapped in a struct
        if (complexStructuralTestsEnabled) {
            testStructRoundTrip(
                    rowType(type, type, type),
                    readValues.stream()
                            .map(OrcTester::toHiveStruct)
                            .collect(toList()));
        }

        // values wrapped in map
        if (mapTestsEnabled && type.isComparable()) {
            testMapRoundTrip(type, readValues);
        }

        // values wrapped in list
        if (listTestsEnabled) {
            testListRoundTrip(type, readValues);
        }

        // values wrapped in a list wrapped in a list
        if (complexStructuralTestsEnabled) {
            testListRoundTrip(
                    arrayType(type),
                    readValues.stream()
                            .map(OrcTester::toHiveList)
                            .collect(toList()));
        }
    }

    private void testStructRoundTrip(Type type, List<?> values)
            throws Exception
    {
        Type rowType = rowType(type, type, type);
        // values in simple struct
        testRoundTripType(
                rowType,
                values.stream()
                        .map(OrcTester::toHiveStruct)
                        .collect(toList()));

        if (structuralNullTestsEnabled) {
            // values and nulls in simple struct
            testRoundTripType(
                    rowType,
                    insertNullEvery(5, values).stream()
                            .map(OrcTester::toHiveStruct)
                            .collect(toList()));

            // all null values in simple struct
            testRoundTripType(
                    rowType,
                    values.stream()
                            .map(value -> toHiveStruct(null))
                            .collect(toList()));
        }

        if (missingStructFieldsTestsEnabled) {
            Type readType = rowType(type, type, type, type, type, type);
            Type writeType = rowType(type, type, type);

            List writeValues = values.stream()
                    .map(OrcTester::toHiveStruct)
                    .collect(toList());

            List readValues = values.stream()
                    .map(OrcTester::toHiveStructWithNull)
                    .collect(toList());

            assertRoundTrip(writeType, readType, writeValues, readValues, true, ImmutableList.of());
        }
    }

    private void testMapRoundTrip(Type type, List<?> readValues)
            throws Exception
    {
        Type mapType = mapType(type, type);

        // maps can not have a null key, so select a value to use for the map key when the value is null
        Object readNullKeyValue = Iterables.getLast(readValues);

        // values in simple map
        testRoundTripType(
                mapType,
                readValues.stream()
                        .map(value -> toHiveMap(value, readNullKeyValue))
                        .collect(toList()));

        if (structuralNullTestsEnabled) {
            // values and nulls in simple map
            testRoundTripType(
                    mapType,
                    insertNullEvery(5, readValues).stream()
                            .map(value -> toHiveMap(value, readNullKeyValue))
                            .collect(toList()));

            // all null values in simple map
            testRoundTripType(
                    mapType,
                    readValues.stream()
                            .map(value -> toHiveMap(null, readNullKeyValue))
                            .collect(toList()));
        }
    }

    private void testListRoundTrip(Type type, List<?> readValues)
            throws Exception
    {
        Type arrayType = arrayType(type);
        // values in simple list
        testRoundTripType(
                arrayType,
                readValues.stream()
                        .map(OrcTester::toHiveList)
                        .collect(toList()));

        if (structuralNullTestsEnabled) {
            // values and nulls in simple list
            testRoundTripType(
                    arrayType,
                    insertNullEvery(5, readValues).stream()
                            .map(OrcTester::toHiveList)
                            .collect(toList()));

            // all null values in simple list
            testRoundTripType(
                    arrayType,
                    readValues.stream()
                            .map(value -> toHiveList(null))
                            .collect(toList()));
        }
    }

    private void testRoundTripType(Type type, List<?> readValues)
            throws Exception
    {
        testRoundTripTypes(ImmutableList.of(type), ImmutableList.of(readValues), ImmutableList.of());
    }

    public void testRoundTripTypes(List<Type> types, List<List<?>> readValues, List<Map<Integer, Map<Subfield, TupleDomainFilter>>> filters)
            throws Exception
    {
        testRoundTripTypes(types, readValues, filters, ImmutableList.of());
    }

    public void testRoundTripTypes(List<Type> types, List<List<?>> readValues, List<Map<Integer, Map<Subfield, TupleDomainFilter>>> filters, List<List<Integer>> expectedFilterOrder)
            throws Exception
    {
        assertEquals(types.size(), readValues.size());
        if (!expectedFilterOrder.isEmpty()) {
            assertEquals(filters.size(), expectedFilterOrder.size());
        }

        // forward order
        assertRoundTrip(types, readValues, filters, expectedFilterOrder);

        // reverse order
        if (reverseTestsEnabled) {
            assertRoundTrip(types, Lists.transform(readValues, OrcTester::reverse), filters, expectedFilterOrder);
        }

        if (nullTestsEnabled) {
            // forward order with nulls
            assertRoundTrip(types, insertNulls(types, readValues), filters, expectedFilterOrder);

            // reverse order with nulls
            if (reverseTestsEnabled) {
                assertRoundTrip(types, insertNulls(types, Lists.transform(readValues, OrcTester::reverse)), filters, expectedFilterOrder);
            }
        }
    }

    public void testRoundTripTypesWithOrder(List<Type> types, List<List<?>> readValues, List<Map<Integer, Map<Subfield, TupleDomainFilter>>> filters, List<List<Integer>> expectedFilterOrder)
            throws Exception
    {
        assertNotNull(expectedFilterOrder);
        assertEquals(filters.size(), expectedFilterOrder.size());

        // Forward order
        testRoundTripTypes(types, readValues, filters, expectedFilterOrder);

        // Reverse order
        int columnCount = types.size();
        List<Map<Integer, Map<Subfield, TupleDomainFilter>>> reverseFilters = filters.stream()
                .map(filtersEntry -> filtersEntry.entrySet().stream().collect(toImmutableMap(entry -> columnCount - 1 - entry.getKey(), Entry::getValue)))
                .collect(toImmutableList());
        List<List<Integer>> reverseFilterOrder = expectedFilterOrder.stream()
                .map(columns -> columns.stream().map(column -> columnCount - 1 - column).collect(toImmutableList()))
                .collect(toImmutableList());
        testRoundTripTypes(Lists.reverse(types), Lists.reverse(readValues), reverseFilters, reverseFilterOrder);
    }

    private List<List<?>> insertNulls(List<Type> types, List<List<?>> values)
    {
        assertTrue(types.size() <= PRIME_NUMBERS.size());

        return IntStream.range(0, types.size())
                .mapToObj(i -> insertNullEvery(PRIME_NUMBERS.get(i), values.get(i)))
                .collect(toList());
    }

    public void assertRoundTrip(Type type, List<?> readValues)
            throws Exception
    {
        assertRoundTrip(type, type, readValues, readValues, true, ImmutableList.of());
    }

    public void assertRoundTripWithSettings(Type type, List<?> readValues, List<OrcReaderSettings> settings)
            throws Exception
    {
        assertRoundTrip(type, type, readValues, readValues, true, settings);
    }

    public void assertRoundTrip(Type type, List<?> readValues, List<Map<Integer, Map<Subfield, TupleDomainFilter>>> filters)
            throws Exception
    {
        List<OrcReaderSettings> settings = filters.stream()
                .map(entry -> OrcReaderSettings.builder().setColumnFilters(entry).build())
                .collect(toImmutableList());

        assertRoundTrip(type, type, readValues, readValues, true, settings);
    }

    public void assertRoundTrip(Type type, List<?> readValues, boolean verifyWithHiveReader)
            throws Exception
    {
        assertRoundTrip(type, type, readValues, readValues, verifyWithHiveReader, ImmutableList.of());
    }

    public void assertRoundTrip(List<Type> types, List<List<?>> readValues, List<Map<Integer, Map<Subfield, TupleDomainFilter>>> filters, List<List<Integer>> expectedFilterOrder)
            throws Exception
    {
        List<OrcReaderSettings> settings = IntStream.range(0, filters.size())
                .mapToObj(i -> OrcReaderSettings.builder()
                        .setColumnFilters(filters.get(i))
                        .setExpectedFilterOrder(expectedFilterOrder.isEmpty() ? ImmutableList.of() : expectedFilterOrder.get(i))
                        .build())
                .collect(toImmutableList());
        assertRoundTrip(types, types, readValues, readValues, true, settings);
    }

    private void assertRoundTrip(Type writeType, Type readType, List<?> writeValues, List<?> readValues, boolean verifyWithHiveReader, List<OrcReaderSettings> settings)
            throws Exception
    {
        assertRoundTrip(ImmutableList.of(writeType), ImmutableList.of(readType), ImmutableList.of(writeValues), ImmutableList.of(readValues), verifyWithHiveReader, settings);
    }

    private void assertRoundTrip(List<Type> writeTypes, List<Type> readTypes, List<List<?>> writeValues, List<List<?>> readValues, boolean verifyWithHiveReader, List<OrcReaderSettings> settings)
            throws Exception
    {
        assertEquals(writeTypes.size(), readTypes.size());
        assertEquals(writeTypes.size(), writeValues.size());
        assertEquals(writeTypes.size(), readValues.size());

        OrcWriterStats stats = new OrcWriterStats();
        for (Format format : formats) {
            if (!readTypes.stream().allMatch(readType -> format.supportsType(readType))) {
                return;
            }

            OrcEncoding orcEncoding = format.getOrcEncoding();
            for (CompressionKind compression : compressions) {
                boolean hiveSupported = (compression != LZ4) && (compression != ZSTD);

                // write Hive, read Presto
                if (hiveSupported) {
                    try (TempFile tempFile = new TempFile()) {
                        writeOrcColumnsHive(tempFile.getFile(), format, compression, writeTypes, writeValues);
                        assertFileContentsPresto(readTypes, tempFile, readValues, false, false, orcEncoding, format, true, useSelectiveOrcReader, settings, ImmutableMap.of());
                    }
                }

                // write Presto, read Hive and Presto
                try (TempFile tempFile = new TempFile()) {
                    writeOrcColumnsPresto(tempFile.getFile(), format, compression, Optional.empty(), writeTypes, writeValues, stats);

                    if (verifyWithHiveReader && hiveSupported) {
                        assertFileContentsHive(readTypes, tempFile, format, readValues);
                    }

                    assertFileContentsPresto(readTypes, tempFile, readValues, false, false, orcEncoding, format, false, useSelectiveOrcReader, settings, ImmutableMap.of());

                    if (skipBatchTestsEnabled) {
                        assertFileContentsPresto(readTypes, tempFile, readValues, true, false, orcEncoding, format, false, useSelectiveOrcReader, settings, ImmutableMap.of());
                    }

                    if (skipStripeTestsEnabled) {
                        assertFileContentsPresto(readTypes, tempFile, readValues, false, true, orcEncoding, format, false, useSelectiveOrcReader, settings, ImmutableMap.of());
                    }
                }
                // write presto read presto
                if (dwrfEncryptionEnabled && format == DWRF) {
                    try (TempFile tempFile = new TempFile()) {
                        DwrfWriterEncryption dwrfWriterEncryption = generateWriterEncryption();
                        writeOrcColumnsPresto(tempFile.getFile(), format, compression, Optional.of(dwrfWriterEncryption), writeTypes, writeValues, stats);

                        ImmutableMap.Builder<Integer, Slice> intermediateKeysBuilder = ImmutableMap.builder();
                        for (int i = 0; i < dwrfWriterEncryption.getWriterEncryptionGroups().size(); i++) {
                            for (Integer node : dwrfWriterEncryption.getWriterEncryptionGroups().get(i).getNodes()) {
                                intermediateKeysBuilder.put(node, dwrfWriterEncryption.getWriterEncryptionGroups().get(i).getIntermediateKeyMetadata());
                            }
                        }
                        Map<Integer, Slice> intermediateKeysMap = intermediateKeysBuilder.build();
                        assertFileContentsPresto(
                                readTypes,
                                tempFile,
                                readValues,
                                false,
                                false,
                                orcEncoding,
                                format,
                                false,
                                useSelectiveOrcReader,
                                settings,
                                intermediateKeysMap);

                        if (skipBatchTestsEnabled) {
                            assertFileContentsPresto(
                                    readTypes,
                                    tempFile,
                                    readValues,
                                    true,
                                    false,
                                    orcEncoding,
                                    format,
                                    false,
                                    useSelectiveOrcReader,
                                    settings,
                                    intermediateKeysMap);
                        }

                        if (skipStripeTestsEnabled) {
                            assertFileContentsPresto(
                                    readTypes,
                                    tempFile,
                                    readValues,
                                    false,
                                    true,
                                    orcEncoding,
                                    format,
                                    false,
                                    useSelectiveOrcReader,
                                    settings,
                                    intermediateKeysMap);
                        }
                    }
                }
            }
        }

        assertEquals(stats.getWriterSizeInBytes(), 0);
    }

    public static class OrcReaderSettings
    {
        private final Map<Integer, Map<Subfield, TupleDomainFilter>> columnFilters;
        private final List<Integer> expectedFilterOrder;
        private final List<FilterFunction> filterFunctions;
        private final Map<Integer, Integer> filterFunctionInputMapping;
        private final Map<Integer, List<Subfield>> requiredSubfields;
        private final OrcFileTailSource orcFileTailSource;

        private OrcReaderSettings(
                Map<Integer, Map<Subfield, TupleDomainFilter>> columnFilters,
                List<Integer> expectedFilterOrder,
                List<FilterFunction> filterFunctions,
                Map<Integer, Integer> filterFunctionInputMapping,
                Map<Integer, List<Subfield>> requiredSubfields,
                OrcFileTailSource orcFileTailSource)
        {
            this.columnFilters = requireNonNull(columnFilters, "columnFilters is null");
            this.expectedFilterOrder = requireNonNull(expectedFilterOrder, "expectedFilterOrder is null");
            this.filterFunctions = requireNonNull(filterFunctions, "filterFunctions is null");
            this.filterFunctionInputMapping = requireNonNull(filterFunctionInputMapping, "filterFunctionInputMapping is null");
            this.requiredSubfields = requireNonNull(requiredSubfields, "requiredSubfields is null");
            this.orcFileTailSource = requireNonNull(orcFileTailSource, "orcFileTailSource is null");
        }

        public Map<Integer, Map<Subfield, TupleDomainFilter>> getColumnFilters()
        {
            return columnFilters;
        }

        public List<Integer> getExpectedFilterOrder()
        {
            return expectedFilterOrder;
        }

        public List<FilterFunction> getFilterFunctions()
        {
            return filterFunctions;
        }

        public Map<Integer, Integer> getFilterFunctionInputMapping()
        {
            return filterFunctionInputMapping;
        }

        public Map<Integer, List<Subfield>> getRequiredSubfields()
        {
            return requiredSubfields;
        }

        public OrcFileTailSource getOrcFileTailSource()
        {
            return orcFileTailSource;
        }

        public static Builder builder()
        {
            return new Builder();
        }

        public static class Builder
        {
            private Map<Integer, Map<Subfield, TupleDomainFilter>> columnFilters = ImmutableMap.of();
            private List<Integer> expectedFilterOrder = ImmutableList.of();
            private List<FilterFunction> filterFunctions = ImmutableList.of();
            private Map<Integer, Integer> filterFunctionInputMapping = ImmutableMap.of();
            private Map<Integer, List<Subfield>> requiredSubfields = new HashMap<>();
            private OrcFileTailSource orcFileTailSource = new StorageOrcFileTailSource();

            public Builder setColumnFilters(Map<Integer, Map<Subfield, TupleDomainFilter>> columnFilters)
            {
                this.columnFilters = requireNonNull(columnFilters, "columnFilters is null");
                return this;
            }

            public Builder setExpectedFilterOrder(List<Integer> expectedFilterOrder)
            {
                this.expectedFilterOrder = requireNonNull(expectedFilterOrder, "expectedFilterOrder is null");
                return this;
            }

            public Builder setFilterFunctions(List<FilterFunction> filterFunctions)
            {
                this.filterFunctions = requireNonNull(filterFunctions, "filterFunctions is null");
                return this;
            }

            public Builder setFilterFunctionMapping(Map<Integer, Integer> filterFunctionInputMapping)
            {
                this.filterFunctionInputMapping = requireNonNull(filterFunctionInputMapping, "filterFunctionInputMapping is null");
                return this;
            }

            public Builder setRequiredSubfields(Map<Integer, List<Subfield>> requiredSubfields)
            {
                requireNonNull(requiredSubfields, "requiredSubfields is null");
                this.requiredSubfields.clear();
                this.requiredSubfields.putAll(requiredSubfields);
                return this;
            }

            public Builder addRequiredSubfields(int column, String... subfields)
            {
                this.requiredSubfields.put(column, Arrays.stream(subfields).map(subfield -> new Subfield(subfield)).collect(toImmutableList()));
                return this;
            }

            public Builder setOrcFileTailSource(OrcFileTailSource orcFileTailSource)
            {
                this.orcFileTailSource = requireNonNull(orcFileTailSource, "orcFileTailSource is null");
                return this;
            }

            public OrcReaderSettings build()
            {
                return new OrcReaderSettings(columnFilters, expectedFilterOrder, filterFunctions, filterFunctionInputMapping, requiredSubfields, orcFileTailSource);
            }
        }
    }

    public static void assertFileContentsPresto(
            List<Type> types,
            File file,
            List<List<?>> expectedValues,
            OrcEncoding orcEncoding,
            OrcPredicate orcPredicate,
            Optional<Map<Integer, Map<Subfield, TupleDomainFilter>>> filters,
            List<FilterFunction> filterFunctions,
            Map<Integer, Integer> filterFunctionInputMapping,
            Map<Integer, List<Subfield>> requiredSubfields)
            throws IOException
    {
        Map<Integer, Type> includedColumns = IntStream.range(0, types.size())
                .boxed()
                .collect(toImmutableMap(Function.identity(), types::get));
        List<Integer> outputColumns = IntStream.range(0, types.size())
                .boxed()
                .collect(toImmutableList());

        assertFileContentsPresto(
                types,
                file,
                expectedValues,
                orcEncoding,
                orcPredicate,
                filters,
                filterFunctions,
                filterFunctionInputMapping,
                requiredSubfields,
                ImmutableMap.of(),
                includedColumns,
                outputColumns);
    }

    public static void assertFileContentsPresto(
            List<Type> types,
            File file,
            List<List<?>> expectedValues,
            OrcEncoding orcEncoding,
            OrcPredicate orcPredicate,
            Optional<Map<Integer, Map<Subfield, TupleDomainFilter>>> filters,
            List<FilterFunction> filterFunctions,
            Map<Integer, Integer> filterFunctionInputMapping,
            Map<Integer, List<Subfield>> requiredSubfields,
            Map<Integer, Slice> intermediateEncryptionKeys,
            Map<Integer, Type> includedColumns,
            List<Integer> outputColumns)
            throws IOException
    {
        try (OrcSelectiveRecordReader recordReader = createCustomOrcSelectiveRecordReader(
                file,
                orcEncoding,
                orcPredicate,
                types,
                MAX_BATCH_SIZE,
                filters.orElse(ImmutableMap.of()),
                filterFunctions,
                filterFunctionInputMapping,
                requiredSubfields,
                intermediateEncryptionKeys,
                includedColumns,
                outputColumns,
                false,
                new TestingHiveOrcAggregatedMemoryContext())) {
            assertEquals(recordReader.getReaderPosition(), 0);
            assertEquals(recordReader.getFilePosition(), 0);
            assertFileContentsPresto(types, recordReader, expectedValues, outputColumns);
        }
    }

    public static void assertFileContentsPresto(
            List<Type> types,
            OrcSelectiveRecordReader recordReader,
            List<List<?>> expectedValues,
            List<Integer> outputColumns)
            throws IOException
    {
        int rowsProcessed = 0;
        while (true) {
            Page page = recordReader.getNextPage();
            if (page == null) {
                break;
            }

            int positionCount = page.getPositionCount();
            if (positionCount == 0) {
                continue;
            }

            assertTrue(expectedValues.get(0).size() >= rowsProcessed + positionCount);

            for (int i = 0; i < outputColumns.size(); i++) {
                Type type = types.get(outputColumns.get(i));
                Block block = page.getBlock(i);
                assertEquals(block.getPositionCount(), positionCount);
                checkNullValues(type, block);

                assertBlockEquals(type, block, expectedValues.get(i), rowsProcessed);
            }

            rowsProcessed += positionCount;
        }

        assertEquals(rowsProcessed, expectedValues.get(0).size());
    }

    static void assertBlockEquals(Type type, Block block, List<?> expectedValues, int offset)
    {
        int positionCount = block.getPositionCount();
        for (int position = 0; position < positionCount; position++) {
            assertColumnValueEquals(type, type.getObjectValue(SESSION.getSqlFunctionProperties(), block, position), expectedValues.get(offset + position));
        }
    }

    private static Map<Integer, Map<Subfield, TupleDomainFilter>> addOrderTracking(Map<Integer, Map<Subfield, TupleDomainFilter>> filters, TupleDomainFilterOrderChecker orderChecker)
    {
        return Maps.transformEntries(filters, (column, columnFilters) -> Maps.transformValues(columnFilters, filter -> addOrderTracking(filter, orderChecker, column)));
    }

    private static TupleDomainFilter addOrderTracking(TupleDomainFilter filter, TupleDomainFilterOrderChecker orderChecker, int column)
    {
        if (filter instanceof BigintRange) {
            return TestBigintRange.of((BigintRange) filter, unused -> orderChecker.call(column));
        }
        if (filter instanceof DoubleRange) {
            return TestDoubleRange.of((DoubleRange) filter, unused -> orderChecker.call(column));
        }
        throw new UnsupportedOperationException("Unsupported filter type: " + filter.getClass().getSimpleName());
    }

    private static void assertFileContentsPresto(
            List<Type> types,
            TempFile tempFile,
            List<List<?>> expectedValues,
            boolean skipFirstBatch,
            boolean skipStripe,
            OrcEncoding orcEncoding,
            Format format,
            boolean isHiveWriter,
            boolean useSelectiveOrcReader,
            List<OrcReaderSettings> settings,
            Map<Integer, Slice> intermediateEncryptionKeys)
            throws IOException
    {
        OrcPredicate orcPredicate = createOrcPredicate(types, expectedValues, format, isHiveWriter);

        Map<Integer, Type> includedColumns = IntStream.range(0, types.size())
                .boxed()
                .collect(toImmutableMap(Function.identity(), types::get));

        List<Integer> outputColumns = IntStream.range(0, types.size())
                .boxed()
                .collect(toImmutableList());

        if (useSelectiveOrcReader) {
            assertFileContentsPresto(
                    types,
                    tempFile.getFile(),
                    expectedValues,
                    orcEncoding,
                    orcPredicate,
                    Optional.empty(),
                    ImmutableList.of(),
                    ImmutableMap.of(),
                    ImmutableMap.of(),
                    intermediateEncryptionKeys,
                    includedColumns,
                    outputColumns);
            for (OrcReaderSettings entry : settings) {
                assertTrue(entry.getFilterFunctions().isEmpty(), "Filter functions are not supported yet");
                assertTrue(entry.getFilterFunctionInputMapping().isEmpty(), "Filter functions are not supported yet");

                Map<Integer, Map<Subfield, TupleDomainFilter>> columnFilters = entry.getColumnFilters();
                List<List<?>> prunedAndFilteredRows = pruneValues(types, filterRows(types, expectedValues, columnFilters), entry.getRequiredSubfields());

                Optional<TupleDomainFilterOrderChecker> orderChecker = Optional.empty();
                List<Integer> expectedFilterOrder = entry.getExpectedFilterOrder();
                if (!expectedFilterOrder.isEmpty()) {
                    orderChecker = Optional.of(new TupleDomainFilterOrderChecker(expectedFilterOrder));
                }

                Optional<Map<Integer, Map<Subfield, TupleDomainFilter>>> transformedFilters = Optional.of(orderChecker.map(checker -> addOrderTracking(columnFilters, checker)).orElse(columnFilters));

                assertFileContentsPresto(
                        types,
                        tempFile.getFile(),
                        prunedAndFilteredRows,
                        orcEncoding,
                        orcPredicate,
                        transformedFilters,
                        entry.getFilterFunctions(),
                        entry.getFilterFunctionInputMapping(),
                        entry.getRequiredSubfields());

                orderChecker.ifPresent(TupleDomainFilterOrderChecker::assertOrder);
            }

            return;
        }

        try (OrcBatchRecordReader recordReader = createCustomOrcRecordReader(tempFile, orcEncoding, orcPredicate, types, MAX_BATCH_SIZE, new StorageOrcFileTailSource(), new StorageStripeMetadataSource(), false, intermediateEncryptionKeys, false)) {
            assertEquals(recordReader.getReaderPosition(), 0);
            assertEquals(recordReader.getFilePosition(), 0);

            boolean isFirst = true;
            int rowsProcessed = 0;
            for (int batchSize = toIntExact(recordReader.nextBatch()); batchSize >= 0; batchSize = toIntExact(recordReader.nextBatch())) {
                if (skipStripe && rowsProcessed < 10000) {
                    // skip recordReader.readBlock
                }
                else if (skipFirstBatch && isFirst) {
                    // skip recordReader.readBlock
                    isFirst = false;
                }
                else {
                    for (int i = 0; i < types.size(); i++) {
                        Type type = types.get(i);
                        Block block = recordReader.readBlock(i);
                        assertEquals(block.getPositionCount(), batchSize);
                        checkNullValues(type, block);

                        assertBlockEquals(type, block, expectedValues.get(i), rowsProcessed);
                    }
                }
                assertEquals(recordReader.getReaderPosition(), rowsProcessed);
                assertEquals(recordReader.getFilePosition(), rowsProcessed);
                rowsProcessed += batchSize;
            }
            assertEquals(rowsProcessed, expectedValues.get(0).size());

            assertEquals(recordReader.getReaderPosition(), rowsProcessed);
            assertEquals(recordReader.getFilePosition(), rowsProcessed);
        }
    }

    public static List<List<?>> filterRows(List<Type> types, List<List<?>> values, Map<Integer, Map<Subfield, TupleDomainFilter>> columnFilters)
    {
        if (columnFilters.isEmpty()) {
            return values;
        }

        List<Integer> passingRows = IntStream.range(0, values.get(0).size())
                .filter(row -> testRow(types, values, row, columnFilters))
                .boxed()
                .collect(toList());
        return IntStream.range(0, values.size())
                .mapToObj(column -> passingRows.stream().map(values.get(column)::get).collect(toList()))
                .collect(toList());
    }

    private static boolean testRow(List<Type> types, List<List<?>> values, int row, Map<Integer, Map<Subfield, TupleDomainFilter>> columnFilters)
    {
        for (int column = 0; column < types.size(); column++) {
            Map<Subfield, TupleDomainFilter> filters = columnFilters.get(column);

            if (filters == null) {
                continue;
            }

            Type type = types.get(column);
            Object value = values.get(column).get(row);
            for (Map.Entry<Subfield, TupleDomainFilter> entry : filters.entrySet()) {
                if (!testSubfieldValue(type, value, entry.getKey(), entry.getValue())) {
                    return false;
                }
            }
        }

        return true;
    }

    private static boolean testSubfieldValue(Type type, Object value, Subfield subfield, TupleDomainFilter filter)
    {
        Type nestedType = type;
        Object nestedValue = value;
        for (Subfield.PathElement pathElement : subfield.getPath()) {
            if (nestedType instanceof ArrayType) {
                assertTrue(pathElement instanceof Subfield.LongSubscript);
                if (nestedValue == null) {
                    return filter.testNull();
                }
                int index = toIntExact(((Subfield.LongSubscript) pathElement).getIndex()) - 1;
                nestedType = ((ArrayType) nestedType).getElementType();
                if (index >= ((List) nestedValue).size()) {
                    return true;
                }
                nestedValue = ((List) nestedValue).get(index);
            }
            else if (nestedType instanceof RowType) {
                assertTrue(pathElement instanceof Subfield.NestedField);
                if (nestedValue == null) {
                    return filter.testNull();
                }
                String fieldName = ((Subfield.NestedField) pathElement).getName();
                int index = -1;
                List<RowType.Field> fields = ((RowType) nestedType).getFields();
                for (int i = 0; i < fields.size(); i++) {
                    if (fieldName.equalsIgnoreCase(fields.get(i).getName().get())) {
                        index = i;
                        nestedType = fields.get(i).getType();
                        break;
                    }
                }
                assertTrue(index >= 0, "Struct field not found: " + fieldName);
                nestedValue = ((List) nestedValue).get(index);
            }
            else {
                fail("Unsupported type: " + type);
            }
        }
        return testValue(nestedType, nestedValue, filter);
    }

    private static boolean testValue(Type type, Object value, TupleDomainFilter filter)
    {
        if (value == null) {
            return filter.testNull();
        }

        if (filter == IS_NULL) {
            return false;
        }

        if (filter == IS_NOT_NULL) {
            return true;
        }

        if (type == BOOLEAN) {
            return filter.testBoolean((Boolean) value);
        }

        if (type == TINYINT || type == BIGINT || type == INTEGER || type == SMALLINT) {
            return filter.testLong(((Number) value).longValue());
        }

        if (type == REAL) {
            return filter.testFloat(((Number) value).floatValue());
        }

        if (type == DOUBLE) {
            return filter.testDouble((double) value);
        }

        if (type == DATE) {
            return filter.testLong(((SqlDate) value).getDays());
        }

        if (type == TIMESTAMP) {
            return filter.testLong(((SqlTimestamp) value).getMillisUtc());
        }

        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            BigDecimal bigDecimal = ((SqlDecimal) value).toBigDecimal();
            if (decimalType.isShort()) {
                return filter.testLong(bigDecimal.unscaledValue().longValue());
            }
            else {
                Slice encodedDecimal = Decimals.encodeScaledValue(bigDecimal);
                return filter.testDecimal(encodedDecimal.getLong(0), encodedDecimal.getLong(Long.BYTES));
            }
        }

        if (type == VARCHAR) {
            return filter.testBytes(((String) value).getBytes(), 0, ((String) value).length());
        }
        if (type instanceof CharType) {
            String charString = String.valueOf(value);
            return filter.testBytes(charString.getBytes(), 0, charString.length());
        }
        if (type == VARBINARY) {
            byte[] binary = ((SqlVarbinary) value).getBytes();
            return filter.testBytes(binary, 0, binary.length);
        }
        fail("Unsupported type: " + type);
        return false;
    }

    private interface SubfieldPruner
    {
        Object prune(Object value);
    }

    private static SubfieldPruner createSubfieldPruner(Type type, List<Subfield> requiredSubfields)
    {
        if (type instanceof ArrayType) {
            return new ListSubfieldPruner(type, requiredSubfields);
        }

        if (type instanceof MapType) {
            return new MapSubfieldPruner(type, requiredSubfields);
        }

        throw new UnsupportedOperationException("Unsupported type: " + type);
    }

    private static class ListSubfieldPruner
            implements SubfieldPruner
    {
        private final int maxIndex;
        private final Optional<SubfieldPruner> nestedSubfieldPruner;

        public ListSubfieldPruner(Type type, List<Subfield> requiredSubfields)
        {
            checkArgument(type instanceof ArrayType, "type is not an array type: " + type);

            maxIndex = requiredSubfields.stream()
                    .map(Subfield::getPath)
                    .map(path -> path.get(0))
                    .map(Subfield.LongSubscript.class::cast)
                    .map(Subfield.LongSubscript::getIndex)
                    .mapToInt(Long::intValue)
                    .max()
                    .orElse(-1);

            List<Subfield> elementSubfields = requiredSubfields.stream()
                    .filter(subfield -> subfield.getPath().size() > 1)
                    .map(subfield -> subfield.tail(subfield.getRootName()))
                    .distinct()
                    .collect(toImmutableList());

            if (elementSubfields.isEmpty()) {
                nestedSubfieldPruner = Optional.empty();
            }
            else {
                nestedSubfieldPruner = Optional.of(createSubfieldPruner(((ArrayType) type).getElementType(), elementSubfields));
            }
        }

        @Override
        public Object prune(Object value)
        {
            if (value == null) {
                return null;
            }

            List list = (List) value;
            List prunedList;
            if (maxIndex == -1) {
                prunedList = list;
            }
            else {
                prunedList = list.size() < maxIndex ? list : list.subList(0, maxIndex);
            }

            return nestedSubfieldPruner.map(pruner -> prunedList.stream().map(pruner::prune).collect(toList())).orElse(prunedList);
        }
    }

    private static class MapSubfieldPruner
            implements SubfieldPruner
    {
        private final Set<Long> keys;
        private final Optional<SubfieldPruner> nestedSubfieldPruner;

        public MapSubfieldPruner(Type type, List<Subfield> requiredSubfields)
        {
            checkArgument(type instanceof MapType, "type is not a map type: " + type);

            keys = requiredSubfields.stream()
                    .map(Subfield::getPath)
                    .map(path -> path.get(0))
                    .map(Subfield.LongSubscript.class::cast)
                    .map(Subfield.LongSubscript::getIndex)
                    .collect(toImmutableSet());

            List<Subfield> elementSubfields = requiredSubfields.stream()
                    .filter(subfield -> subfield.getPath().size() > 1)
                    .map(subfield -> subfield.tail(subfield.getRootName()))
                    .distinct()
                    .collect(toImmutableList());

            if (elementSubfields.isEmpty()) {
                nestedSubfieldPruner = Optional.empty();
            }
            else {
                nestedSubfieldPruner = Optional.of(createSubfieldPruner(((MapType) type).getValueType(), elementSubfields));
            }
        }

        @Override
        public Object prune(Object value)
        {
            if (value == null) {
                return null;
            }

            Map map = (Map) value;
            Map prunedMap;
            if (keys.isEmpty()) {
                prunedMap = map;
            }
            else {
                prunedMap = Maps.filterKeys((Map) value, key -> keys.contains(Long.valueOf(((Number) key).longValue())));
            }

            return nestedSubfieldPruner.map(pruner -> Maps.transformValues(prunedMap, pruner::prune)).orElse(prunedMap);
        }
    }

    private static List<List<?>> pruneValues(List<Type> types, List<List<?>> values, Map<Integer, List<Subfield>> requiredSubfields)
    {
        if (requiredSubfields.isEmpty()) {
            return values;
        }

        ImmutableList.Builder<List<?>> builder = ImmutableList.builder();
        for (int i = 0; i < types.size(); i++) {
            List<Subfield> subfields = requiredSubfields.get(i);

            if (subfields.isEmpty()) {
                builder.add(values.get(i));
                continue;
            }

            SubfieldPruner subfieldPruner = createSubfieldPruner(types.get(i), subfields);
            builder.add(values.get(i).stream().map(subfieldPruner::prune).collect(toList()));
        }

        return builder.build();
    }

    public static void assertColumnValueEquals(Type type, Object actual, Object expected)
    {
        if (actual == null) {
            assertEquals(actual, expected);
            return;
        }
        String baseType = type.getTypeSignature().getBase();
        if (StandardTypes.ARRAY.equals(baseType)) {
            List<?> actualArray = (List<?>) actual;
            List<?> expectedArray = (List<?>) expected;
            assertEquals(actualArray == null, expectedArray == null);
            assertEquals(actualArray.size(), expectedArray.size());

            Type elementType = type.getTypeParameters().get(0);
            for (int i = 0; i < actualArray.size(); i++) {
                Object actualElement = actualArray.get(i);
                Object expectedElement = expectedArray.get(i);
                assertColumnValueEquals(elementType, actualElement, expectedElement);
            }
        }
        else if (StandardTypes.MAP.equals(baseType)) {
            Map<?, ?> actualMap = (Map<?, ?>) actual;
            Map<?, ?> expectedMap = (Map<?, ?>) expected;
            assertEquals(actualMap.size(), expectedMap.size());

            Type keyType = type.getTypeParameters().get(0);
            Type valueType = type.getTypeParameters().get(1);

            List<Entry<?, ?>> expectedEntries = new ArrayList<>(expectedMap.entrySet());
            for (Entry<?, ?> actualEntry : actualMap.entrySet()) {
                for (Iterator<Entry<?, ?>> iterator = expectedEntries.iterator(); iterator.hasNext(); ) {
                    Entry<?, ?> expectedEntry = iterator.next();
                    try {
                        assertColumnValueEquals(keyType, actualEntry.getKey(), expectedEntry.getKey());
                        assertColumnValueEquals(valueType, actualEntry.getValue(), expectedEntry.getValue());
                        iterator.remove();
                    }
                    catch (AssertionError ignored) {
                    }
                }
            }
            assertTrue(expectedEntries.isEmpty(), "Unmatched entries " + expectedEntries);
        }
        else if (StandardTypes.ROW.equals(baseType)) {
            List<Type> fieldTypes = type.getTypeParameters();

            List<?> actualRow = (List<?>) actual;
            List<?> expectedRow = (List<?>) expected;
            assertEquals(actualRow.size(), fieldTypes.size());
            assertEquals(actualRow.size(), expectedRow.size());

            for (int fieldId = 0; fieldId < actualRow.size(); fieldId++) {
                Type fieldType = fieldTypes.get(fieldId);
                Object actualElement = actualRow.get(fieldId);
                Object expectedElement = expectedRow.get(fieldId);
                assertColumnValueEquals(fieldType, actualElement, expectedElement);
            }
        }
        else if (type.equals(DOUBLE)) {
            Double actualDouble = (Double) actual;
            Double expectedDouble = (Double) expected;
            if (actualDouble.isNaN()) {
                assertTrue(expectedDouble.isNaN(), "expected double to be NaN");
            }
            else {
                assertEquals(actualDouble, expectedDouble, 0.001);
            }
        }
        else if (!Objects.equals(actual, expected)) {
            assertEquals(actual, expected);
        }
    }

    static OrcBatchRecordReader createCustomOrcRecordReader(TempFile tempFile, OrcEncoding orcEncoding, OrcPredicate predicate, Type type, int initialBatchSize, boolean cacheable, boolean mapNullKeysEnabled)
            throws IOException
    {
        return createCustomOrcRecordReader(tempFile, orcEncoding, predicate, ImmutableList.of(type), initialBatchSize, cacheable, mapNullKeysEnabled);
    }

    static OrcBatchRecordReader createCustomOrcRecordReader(TempFile tempFile, OrcEncoding orcEncoding, OrcPredicate predicate, List<Type> types, int initialBatchSize, boolean cacheable, boolean mapNullKeysEnabled)
            throws IOException
    {
        return createCustomOrcRecordReader(tempFile, orcEncoding, predicate, types, initialBatchSize, new StorageOrcFileTailSource(), new StorageStripeMetadataSource(), cacheable, ImmutableMap.of(), mapNullKeysEnabled);
    }

    static OrcBatchRecordReader createCustomOrcRecordReader(
            TempFile tempFile,
            OrcEncoding orcEncoding,
            OrcPredicate predicate,
            List<Type> types,
            int initialBatchSize,
            OrcFileTailSource orcFileTailSource,
            StripeMetadataSource stripeMetadataSource,
            boolean cacheable,
            Map<Integer, Slice> intermediateEncryptionKeys,
            boolean mapNullKeysEnabled)
            throws IOException
    {
        OrcDataSource orcDataSource = new FileOrcDataSource(tempFile.getFile(), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), true);
        OrcReader orcReader = new OrcReader(
                orcDataSource,
                orcEncoding,
                orcFileTailSource,
                stripeMetadataSource,
                Optional.empty(),
                NOOP_ORC_AGGREGATED_MEMORY_CONTEXT,
                new OrcReaderOptions(
                        new DataSize(1, MEGABYTE),
                        new DataSize(1, MEGABYTE),
                        MAX_BLOCK_SIZE,
                        false,
                        mapNullKeysEnabled,
                        false),
                cacheable,
                new DwrfEncryptionProvider(new UnsupportedEncryptionLibrary(), new TestingEncryptionLibrary()),
                DwrfKeyProvider.of(intermediateEncryptionKeys));

        assertEquals(orcReader.getFooter().getRowsInRowGroup(), 10_000);

        Map<Integer, Type> columnTypes = IntStream.range(0, types.size())
                .boxed()
                .collect(toImmutableMap(Functions.identity(), types::get));

        return orcReader.createBatchRecordReader(columnTypes, predicate, HIVE_STORAGE_TIME_ZONE, new TestingHiveOrcAggregatedMemoryContext(), initialBatchSize);
    }

    static OrcReader createCustomOrcReader(
            TempFile tempFile,
            OrcEncoding orcEncoding,
            boolean mapNullKeysEnabled,
            Map<Integer, Slice> intermediateEncryptionKeys)
            throws IOException
    {
        OrcDataSource orcDataSource = new FileOrcDataSource(tempFile.getFile(), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), true);
        OrcReader orcReader = new OrcReader(
                orcDataSource,
                orcEncoding,
                new StorageOrcFileTailSource(),
                new StorageStripeMetadataSource(),
                NOOP_ORC_AGGREGATED_MEMORY_CONTEXT,
                new OrcReaderOptions(
                        new DataSize(1, MEGABYTE),
                        new DataSize(1, MEGABYTE),
                        MAX_BLOCK_SIZE,
                        false,
                        mapNullKeysEnabled,
                        false),
                false,
                new DwrfEncryptionProvider(new UnsupportedEncryptionLibrary(), new TestingEncryptionLibrary()),
                DwrfKeyProvider.of(intermediateEncryptionKeys));
        return orcReader;
    }

    public static void writeOrcColumnPresto(File outputFile, Format format, CompressionKind compression, Type type, List<?> values)
            throws Exception
    {
        writeOrcColumnsPresto(outputFile, format, compression, Optional.empty(), ImmutableList.of(type), ImmutableList.of(values), new OrcWriterStats());
    }

    public static void writeOrcColumnsPresto(File outputFile, Format format, CompressionKind compression, Optional<DwrfWriterEncryption> dwrfWriterEncryption, List<Type> types, List<List<?>> values, WriterStats stats)
            throws Exception
    {
        List<String> columnNames = makeColumnNames(types.size());

        ImmutableMap.Builder<String, String> metadata = ImmutableMap.builder();
        metadata.put("columns", String.join(", ", columnNames));
        metadata.put("columns.types", createSettableStructObjectInspector(types).getTypeName());

        OrcWriter writer = new OrcWriter(
                new OutputStreamDataSink(new FileOutputStream(outputFile)),
                columnNames,
                types,
                format.getOrcEncoding(),
                compression,
                dwrfWriterEncryption,
                new DwrfEncryptionProvider(new UnsupportedEncryptionLibrary(), new TestingEncryptionLibrary()),
                new OrcWriterOptions(),
                ImmutableMap.of(),
                HIVE_STORAGE_TIME_ZONE,
                true,
                BOTH,
                stats);

        Block[] blocks = new Block[types.size()];
        for (int i = 0; i < types.size(); i++) {
            Type type = types.get(i);
            BlockBuilder blockBuilder = type.createBlockBuilder(null, 1024);
            for (Object value : values.get(i)) {
                writeValue(type, blockBuilder, value);
            }
            blocks[i] = blockBuilder.build();
        }

        writer.write(new Page(blocks));
        writer.close();
        writer.validate(new FileOrcDataSource(
                outputFile,
                new DataSize(1, MEGABYTE),
                new DataSize(1, MEGABYTE),
                new DataSize(1, MEGABYTE),
                true));
    }

    private static DwrfWriterEncryption generateWriterEncryption()
    {
        return new DwrfWriterEncryption(
                UNKNOWN,
                ImmutableList.of(
                        new WriterEncryptionGroup(
                                ImmutableList.of(1),
                                Slices.utf8Slice("encryptionKey"))));
    }

    static OrcSelectiveRecordReader createCustomOrcSelectiveRecordReader(
            TempFile tempFile,
            OrcEncoding orcEncoding,
            OrcPredicate predicate,
            Type type,
            int initialBatchSize,
            boolean mapNullKeysEnabled)
            throws IOException
    {
        return createCustomOrcSelectiveRecordReader(
                tempFile.getFile(),
                orcEncoding,
                predicate,
                ImmutableList.of(type),
                initialBatchSize,
                ImmutableMap.of(),
                ImmutableList.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of(0, type),
                ImmutableList.of(0),
                mapNullKeysEnabled,
                new TestingHiveOrcAggregatedMemoryContext());
    }

    public static OrcSelectiveRecordReader createCustomOrcSelectiveRecordReader(
            File file,
            OrcEncoding orcEncoding,
            OrcPredicate predicate,
            List<Type> types,
            int initialBatchSize,
            Map<Integer, Map<Subfield, TupleDomainFilter>> filters,
            List<FilterFunction> filterFunctions,
            Map<Integer, Integer> filterFunctionInputMapping,
            Map<Integer, List<Subfield>> requiredSubfields,
            Map<Integer, Slice> intermediateEncryptionKeys,
            Map<Integer, Type> includedColumns,
            List<Integer> outputColumns,
            boolean mapNullKeysEnabled,
            OrcAggregatedMemoryContext systemMemoryUsage)
            throws IOException
    {
        OrcDataSource orcDataSource = new FileOrcDataSource(file, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), true);
        OrcReader orcReader = new OrcReader(
                orcDataSource,
                orcEncoding,
                new StorageOrcFileTailSource(),
                new StorageStripeMetadataSource(),
                NOOP_ORC_AGGREGATED_MEMORY_CONTEXT,
                new OrcReaderOptions(
                        new DataSize(1, MEGABYTE),
                        new DataSize(1, MEGABYTE),
                        MAX_BLOCK_SIZE,
                        false,
                        mapNullKeysEnabled,
                        false),
                false,
                new DwrfEncryptionProvider(new UnsupportedEncryptionLibrary(), new TestingEncryptionLibrary()),
                DwrfKeyProvider.of(intermediateEncryptionKeys));

        assertEquals(orcReader.getColumnNames().subList(0, types.size()), makeColumnNames(types.size()));
        assertEquals(orcReader.getFooter().getRowsInRowGroup(), 10_000);

        return orcReader.createSelectiveRecordReader(
                includedColumns,
                outputColumns,
                filters,
                filterFunctions,
                filterFunctionInputMapping,
                requiredSubfields,
                ImmutableMap.of(),
                ImmutableMap.of(),
                predicate,
                0,
                orcDataSource.getSize(),
                HIVE_STORAGE_TIME_ZONE,
                LEGACY_MAP_SUBSCRIPT,
                systemMemoryUsage,
                Optional.empty(),
                initialBatchSize);
    }

    private static void writeValue(Type type, BlockBuilder blockBuilder, Object value)
    {
        if (value == null) {
            blockBuilder.appendNull();
        }
        else {
            if (BOOLEAN.equals(type)) {
                type.writeBoolean(blockBuilder, (Boolean) value);
            }
            else if (TINYINT.equals(type) || SMALLINT.equals(type) || INTEGER.equals(type) || BIGINT.equals(type)) {
                type.writeLong(blockBuilder, ((Number) value).longValue());
            }
            else if (Decimals.isShortDecimal(type)) {
                type.writeLong(blockBuilder, ((SqlDecimal) value).toBigDecimal().unscaledValue().longValue());
            }
            else if (Decimals.isLongDecimal(type)) {
                type.writeSlice(blockBuilder, Decimals.encodeUnscaledValue(((SqlDecimal) value).toBigDecimal().unscaledValue()));
            }
            else if (DOUBLE.equals(type)) {
                type.writeDouble(blockBuilder, ((Number) value).doubleValue());
            }
            else if (REAL.equals(type)) {
                float floatValue = ((Number) value).floatValue();
                type.writeLong(blockBuilder, Float.floatToIntBits(floatValue));
            }
            else if (type instanceof VarcharType) {
                Slice slice = truncateToLength(utf8Slice((String) value), type);
                type.writeSlice(blockBuilder, slice);
            }
            else if (type instanceof CharType) {
                Slice slice = truncateToLengthAndTrimSpaces(utf8Slice((String) value), type);
                type.writeSlice(blockBuilder, slice);
            }
            else if (VARBINARY.equals(type)) {
                type.writeSlice(blockBuilder, Slices.wrappedBuffer(((SqlVarbinary) value).getBytes()));
            }
            else if (DATE.equals(type)) {
                long days = ((SqlDate) value).getDays();
                type.writeLong(blockBuilder, days);
            }
            else if (TIMESTAMP.equals(type)) {
                long millis = ((SqlTimestamp) value).getMillisUtc();
                type.writeLong(blockBuilder, millis);
            }
            else {
                String baseType = type.getTypeSignature().getBase();
                if (StandardTypes.ARRAY.equals(baseType)) {
                    List<?> array = (List<?>) value;
                    Type elementType = type.getTypeParameters().get(0);
                    BlockBuilder arrayBlockBuilder = blockBuilder.beginBlockEntry();
                    for (Object elementValue : array) {
                        writeValue(elementType, arrayBlockBuilder, elementValue);
                    }
                    blockBuilder.closeEntry();
                }
                else if (StandardTypes.MAP.equals(baseType)) {
                    Map<?, ?> map = (Map<?, ?>) value;
                    Type keyType = type.getTypeParameters().get(0);
                    Type valueType = type.getTypeParameters().get(1);
                    BlockBuilder mapBlockBuilder = blockBuilder.beginBlockEntry();
                    for (Entry<?, ?> entry : map.entrySet()) {
                        writeValue(keyType, mapBlockBuilder, entry.getKey());
                        writeValue(valueType, mapBlockBuilder, entry.getValue());
                    }
                    blockBuilder.closeEntry();
                }
                else if (StandardTypes.ROW.equals(baseType)) {
                    List<?> array = (List<?>) value;
                    List<Type> fieldTypes = type.getTypeParameters();
                    BlockBuilder rowBlockBuilder = blockBuilder.beginBlockEntry();
                    for (int fieldId = 0; fieldId < fieldTypes.size(); fieldId++) {
                        Type fieldType = fieldTypes.get(fieldId);
                        writeValue(fieldType, rowBlockBuilder, array.get(fieldId));
                    }
                    blockBuilder.closeEntry();
                }
                else {
                    throw new IllegalArgumentException("Unsupported type " + type);
                }
            }
        }
    }

    private static void assertFileContentsHive(
            List<Type> types,
            TempFile tempFile,
            Format format,
            List<List<?>> expectedValues)
            throws Exception
    {
        if (format == DWRF) {
            assertFileContentsDwrfHive(types, tempFile, expectedValues);
        }
        else {
            assertFileContentsOrcHive(types, tempFile, expectedValues);
        }
    }

    private static void assertFileContentsOrcHive(
            List<Type> types,
            TempFile tempFile,
            List<List<?>> expectedValues)
            throws Exception
    {
        JobConf configuration = new JobConf(new Configuration(false));
        configuration.set(READ_COLUMN_IDS_CONF_STR, "0");
        configuration.setBoolean(READ_ALL_COLUMNS, false);

        Reader reader = OrcFile.createReader(
                new Path(tempFile.getFile().getAbsolutePath()),
                new ReaderOptions(configuration));
        org.apache.hadoop.hive.ql.io.orc.RecordReader recordReader = reader.rows();

        StructObjectInspector rowInspector = (StructObjectInspector) reader.getObjectInspector();

        List<StructField> fields = makeColumnNames(types.size()).stream()
                .map(rowInspector::getStructFieldRef)
                .collect(toList());

        Object rowData = null;
        int rowCount = 0;
        while (recordReader.hasNext()) {
            rowData = recordReader.next(rowData);

            for (int i = 0; i < fields.size(); i++) {
                Object actualValue = rowInspector.getStructFieldData(rowData, fields.get(i));
                actualValue = decodeRecordReaderValue(types.get(i), actualValue);
                assertColumnValueEquals(types.get(i), actualValue, expectedValues.get(i).get(rowCount));
            }
            rowCount++;
        }
        assertEquals(rowCount, expectedValues.get(0).size());
    }

    private static void assertFileContentsDwrfHive(
            List<Type> types,
            TempFile tempFile,
            List<List<?>> expectedValues)
            throws Exception
    {
        JobConf configuration = new JobConf(new Configuration(false));
        configuration.set(READ_COLUMN_IDS_CONF_STR, "0");
        configuration.setBoolean(READ_ALL_COLUMNS, false);

        Path path = new Path(tempFile.getFile().getAbsolutePath());
        com.facebook.hive.orc.Reader reader = com.facebook.hive.orc.OrcFile.createReader(
                path.getFileSystem(configuration),
                path,
                configuration);
        boolean[] include = new boolean[reader.getTypes().size() + 100000];
        Arrays.fill(include, true);
        com.facebook.hive.orc.RecordReader recordReader = reader.rows(include);

        StructObjectInspector rowInspector = (StructObjectInspector) reader.getObjectInspector();

        List<StructField> fields = makeColumnNames(types.size()).stream()
                .map(rowInspector::getStructFieldRef)
                .collect(toList());

        Object rowData = null;
        int rowCount = 0;
        while (recordReader.hasNext()) {
            rowData = recordReader.next(rowData);

            for (int i = 0; i < fields.size(); i++) {
                Object actualValue = rowInspector.getStructFieldData(rowData, fields.get(i));
                actualValue = decodeRecordReaderValue(types.get(i), actualValue);
                assertColumnValueEquals(types.get(i), actualValue, expectedValues.get(i).get(rowCount));
            }
            rowCount++;
        }
        assertEquals(rowCount, expectedValues.get(0).size());
    }

    private static List<String> makeColumnNames(int columns)
    {
        return IntStream.range(0, columns)
                .mapToObj(i -> i == 0 ? "test" : "test" + (i + 1))
                .collect(toList());
    }

    private static Object decodeRecordReaderValue(Type type, Object actualValue)
    {
        if (actualValue instanceof OrcLazyObject) {
            try {
                actualValue = ((OrcLazyObject) actualValue).materialize();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        if (actualValue instanceof BooleanWritable) {
            actualValue = ((BooleanWritable) actualValue).get();
        }
        else if (actualValue instanceof ByteWritable) {
            actualValue = ((ByteWritable) actualValue).get();
        }
        else if (actualValue instanceof BytesWritable) {
            actualValue = new SqlVarbinary(((BytesWritable) actualValue).copyBytes());
        }
        else if (actualValue instanceof DateWritable) {
            actualValue = new SqlDate(((DateWritable) actualValue).getDays());
        }
        else if (actualValue instanceof DoubleWritable) {
            actualValue = ((DoubleWritable) actualValue).get();
        }
        else if (actualValue instanceof FloatWritable) {
            actualValue = ((FloatWritable) actualValue).get();
        }
        else if (actualValue instanceof IntWritable) {
            actualValue = ((IntWritable) actualValue).get();
        }
        else if (actualValue instanceof HiveCharWritable) {
            actualValue = ((HiveCharWritable) actualValue).getPaddedValue().toString();
        }
        else if (actualValue instanceof LongWritable) {
            actualValue = ((LongWritable) actualValue).get();
        }
        else if (actualValue instanceof ShortWritable) {
            actualValue = ((ShortWritable) actualValue).get();
        }
        else if (actualValue instanceof HiveDecimalWritable) {
            DecimalType decimalType = (DecimalType) type;
            HiveDecimalWritable writable = (HiveDecimalWritable) actualValue;
            // writable messes with the scale so rescale the values to the Presto type
            BigInteger rescaledValue = rescale(writable.getHiveDecimal().unscaledValue(), writable.getScale(), decimalType.getScale());
            actualValue = new SqlDecimal(rescaledValue, decimalType.getPrecision(), decimalType.getScale());
        }
        else if (actualValue instanceof Text) {
            actualValue = actualValue.toString();
        }
        else if (actualValue instanceof TimestampWritable) {
            TimestampWritable timestamp = (TimestampWritable) actualValue;
            actualValue = sqlTimestampOf((timestamp.getSeconds() * 1000) + (timestamp.getNanos() / 1000000L), SESSION);
        }
        else if (actualValue instanceof OrcStruct) {
            List<Object> fields = new ArrayList<>();
            OrcStruct structObject = (OrcStruct) actualValue;
            for (int fieldId = 0; fieldId < structObject.getNumFields(); fieldId++) {
                fields.add(OrcUtil.getFieldValue(structObject, fieldId));
            }
            actualValue = decodeRecordReaderStruct(type, fields);
        }
        else if (actualValue instanceof com.facebook.hive.orc.OrcStruct) {
            List<Object> fields = new ArrayList<>();
            com.facebook.hive.orc.OrcStruct structObject = (com.facebook.hive.orc.OrcStruct) actualValue;
            for (int fieldId = 0; fieldId < structObject.getNumFields(); fieldId++) {
                fields.add(structObject.getFieldValue(fieldId));
            }
            actualValue = decodeRecordReaderStruct(type, fields);
        }
        else if (actualValue instanceof List) {
            actualValue = decodeRecordReaderList(type, ((List<?>) actualValue));
        }
        else if (actualValue instanceof Map) {
            actualValue = decodeRecordReaderMap(type, (Map<?, ?>) actualValue);
        }
        return actualValue;
    }

    private static List<Object> decodeRecordReaderList(Type type, List<?> list)
    {
        Type elementType = type.getTypeParameters().get(0);
        return list.stream()
                .map(element -> decodeRecordReaderValue(elementType, element))
                .collect(toList());
    }

    private static Object decodeRecordReaderMap(Type type, Map<?, ?> map)
    {
        Type keyType = type.getTypeParameters().get(0);
        Type valueType = type.getTypeParameters().get(1);
        Map<Object, Object> newMap = new HashMap<>();
        for (Entry<?, ?> entry : map.entrySet()) {
            newMap.put(decodeRecordReaderValue(keyType, entry.getKey()), decodeRecordReaderValue(valueType, entry.getValue()));
        }
        return newMap;
    }

    private static List<Object> decodeRecordReaderStruct(Type type, List<?> fields)
    {
        List<Type> fieldTypes = type.getTypeParameters();
        List<Object> newFields = new ArrayList<>(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            Type fieldType = fieldTypes.get(i);
            Object field = fields.get(i);
            newFields.add(decodeRecordReaderValue(fieldType, field));
        }

        for (int j = fields.size(); j < fieldTypes.size(); j++) {
            newFields.add(null);
        }

        return newFields;
    }

    public static DataSize writeOrcColumnHive(File outputFile, Format format, CompressionKind compression, Type type, List<?> values)
            throws Exception
    {
        return writeOrcColumnsHive(outputFile, format, compression, ImmutableList.of(type), ImmutableList.of(values));
    }

    public static DataSize writeOrcColumnsHive(File outputFile, Format format, CompressionKind compression, List<Type> types, List<List<?>> values)
            throws Exception
    {
        RecordWriter recordWriter;
        if (DWRF == format) {
            recordWriter = createDwrfRecordWriter(outputFile, compression, types);
        }
        else {
            recordWriter = createOrcRecordWriter(outputFile, format, compression, types);
        }
        return writeOrcFileColumnHive(outputFile, format, recordWriter, types, values);
    }

    private static DataSize writeOrcFileColumnHive(File outputFile, Format format, RecordWriter recordWriter, List<Type> types, List<List<?>> values)
            throws Exception
    {
        SettableStructObjectInspector objectInspector = createSettableStructObjectInspector(types);
        Object row = objectInspector.create();

        List<StructField> fields = ImmutableList.copyOf(objectInspector.getAllStructFieldRefs());
        @SuppressWarnings("deprecation") Serializer serializer = format.createSerializer();

        for (int i = 0; i < values.get(0).size(); i++) {
            for (int j = 0; j < types.size(); j++) {
                Object value = preprocessWriteValueHive(types.get(j), values.get(j).get(i));
                objectInspector.setStructFieldData(row, fields.get(j), value);
            }

            if (DWRF == format) {
                if (i == 142_345) {
                    setDwrfLowMemoryFlag(recordWriter);
                }
            }

            Writable record = serializer.serialize(row, objectInspector);
            recordWriter.write(record);
        }

        recordWriter.close(false);
        return succinctBytes(outputFile.length());
    }

    public static DataSize writeOrcFileColumnHive(File outputFile, Format format, RecordWriter recordWriter, Type type, List<?> values)
            throws Exception
    {
        return writeOrcFileColumnHive(outputFile, format, recordWriter, ImmutableList.of(type), ImmutableList.of(values));
    }

    private static ObjectInspector getJavaObjectInspector(Type type)
    {
        if (type.equals(BOOLEAN)) {
            return javaBooleanObjectInspector;
        }
        if (type.equals(BIGINT)) {
            return javaLongObjectInspector;
        }
        if (type.equals(INTEGER)) {
            return javaIntObjectInspector;
        }
        if (type.equals(SMALLINT)) {
            return javaShortObjectInspector;
        }
        if (type.equals(TINYINT)) {
            return javaByteObjectInspector;
        }
        if (type.equals(REAL)) {
            return javaFloatObjectInspector;
        }
        if (type.equals(DOUBLE)) {
            return javaDoubleObjectInspector;
        }
        if (type instanceof VarcharType) {
            return javaStringObjectInspector;
        }
        if (type instanceof CharType) {
            int charLength = ((CharType) type).getLength();
            return new JavaHiveCharObjectInspector(getCharTypeInfo(charLength));
        }
        if (type instanceof VarbinaryType) {
            return javaByteArrayObjectInspector;
        }
        if (type.equals(DATE)) {
            return javaDateObjectInspector;
        }
        if (type.equals(TIMESTAMP)) {
            return javaTimestampObjectInspector;
        }
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            return getPrimitiveJavaObjectInspector(new DecimalTypeInfo(decimalType.getPrecision(), decimalType.getScale()));
        }
        if (type.getTypeSignature().getBase().equals(StandardTypes.ARRAY)) {
            return getStandardListObjectInspector(getJavaObjectInspector(type.getTypeParameters().get(0)));
        }
        if (type.getTypeSignature().getBase().equals(StandardTypes.MAP)) {
            ObjectInspector keyObjectInspector = getJavaObjectInspector(type.getTypeParameters().get(0));
            ObjectInspector valueObjectInspector = getJavaObjectInspector(type.getTypeParameters().get(1));
            return getStandardMapObjectInspector(keyObjectInspector, valueObjectInspector);
        }
        if (type.getTypeSignature().getBase().equals(StandardTypes.ROW)) {
            return getStandardStructObjectInspector(
                    type.getTypeSignature().getParameters().stream()
                            .map(parameter -> parameter.getNamedTypeSignature().getName().get())
                            .collect(toList()),
                    type.getTypeParameters().stream()
                            .map(OrcTester::getJavaObjectInspector)
                            .collect(toList()));
        }
        throw new IllegalArgumentException("unsupported type: " + type);
    }

    private static Object preprocessWriteValueHive(Type type, Object value)
    {
        if (value == null) {
            return null;
        }

        if (type.equals(BOOLEAN)) {
            return value;
        }
        else if (type.equals(TINYINT)) {
            return ((Number) value).byteValue();
        }
        else if (type.equals(SMALLINT)) {
            return ((Number) value).shortValue();
        }
        else if (type.equals(INTEGER)) {
            return ((Number) value).intValue();
        }
        else if (type.equals(BIGINT)) {
            return ((Number) value).longValue();
        }
        else if (type.equals(REAL)) {
            return ((Number) value).floatValue();
        }
        else if (type.equals(DOUBLE)) {
            return ((Number) value).doubleValue();
        }
        else if (type instanceof VarcharType) {
            return value;
        }
        else if (type instanceof CharType) {
            return new HiveChar((String) value, ((CharType) type).getLength());
        }
        else if (type.equals(VARBINARY)) {
            return ((SqlVarbinary) value).getBytes();
        }
        else if (type.equals(DATE)) {
            int days = ((SqlDate) value).getDays();
            LocalDate localDate = LocalDate.ofEpochDay(days);
            ZonedDateTime zonedDateTime = localDate.atStartOfDay(ZoneId.systemDefault());

            long millis = SECONDS.toMillis(zonedDateTime.toEpochSecond());
            Date date = new Date(0);
            // millis must be set separately to avoid masking
            date.setTime(millis);
            return date;
        }
        else if (type.equals(TIMESTAMP)) {
            long millisUtc = (int) ((SqlTimestamp) value).getMillisUtc();
            return new Timestamp(millisUtc);
        }
        else if (type instanceof DecimalType) {
            return HiveDecimal.create(((SqlDecimal) value).toBigDecimal());
        }
        else if (type.getTypeSignature().getBase().equals(StandardTypes.ARRAY)) {
            Type elementType = type.getTypeParameters().get(0);
            return ((List<?>) value).stream()
                    .map(element -> preprocessWriteValueHive(elementType, element))
                    .collect(toList());
        }
        else if (type.getTypeSignature().getBase().equals(StandardTypes.MAP)) {
            Type keyType = type.getTypeParameters().get(0);
            Type valueType = type.getTypeParameters().get(1);
            Map<Object, Object> newMap = new HashMap<>();
            for (Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                newMap.put(preprocessWriteValueHive(keyType, entry.getKey()), preprocessWriteValueHive(valueType, entry.getValue()));
            }
            return newMap;
        }
        else if (type.getTypeSignature().getBase().equals(StandardTypes.ROW)) {
            List<?> fieldValues = (List<?>) value;
            List<Type> fieldTypes = type.getTypeParameters();
            List<Object> newStruct = new ArrayList<>();
            for (int fieldId = 0; fieldId < fieldValues.size(); fieldId++) {
                newStruct.add(preprocessWriteValueHive(fieldTypes.get(fieldId), fieldValues.get(fieldId)));
            }
            return newStruct;
        }
        throw new IllegalArgumentException("unsupported type: " + type);
    }

    private static void checkNullValues(Type type, Block block)
    {
        if (!block.mayHaveNull()) {
            return;
        }
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                if (type.equals(TINYINT) || type.equals(SMALLINT) || type.equals(INTEGER) || type.equals(BIGINT) || type.equals(REAL) || type.equals(DATE) || type.equals(TIMESTAMP)) {
                    assertEquals(type.getLong(block, position), 0);
                }
                if (type.equals(BOOLEAN)) {
                    assertFalse(type.getBoolean(block, position));
                }
                if (type.equals(DOUBLE)) {
                    assertEquals(type.getDouble(block, position), 0.0);
                }
                if (type instanceof VarcharType || type instanceof CharType || type.equals(VARBINARY)) {
                    assertEquals(type.getSlice(block, position).length(), 0);
                }
            }
        }
    }

    private static void setDwrfLowMemoryFlag(RecordWriter recordWriter)
    {
        Object writer = getFieldValue(recordWriter, "writer");
        Object memoryManager = getFieldValue(writer, "memoryManager");
        setFieldValue(memoryManager, "lowMemoryMode", true);
        try {
            writer.getClass().getMethod("enterLowMemoryMode").invoke(writer);
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private static Object getFieldValue(Object instance, String name)
    {
        try {
            Field writerField = instance.getClass().getDeclaredField(name);
            writerField.setAccessible(true);
            return writerField.get(instance);
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private static void setFieldValue(Object instance, String name, Object value)
    {
        try {
            Field writerField = instance.getClass().getDeclaredField(name);
            writerField.setAccessible(true);
            writerField.set(instance, value);
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    static RecordWriter createOrcRecordWriter(File outputFile, Format format, CompressionKind compression, Type type)
            throws IOException
    {
        return createOrcRecordWriter(outputFile, format, compression, ImmutableList.of(type));
    }

    static RecordWriter createOrcRecordWriter(File outputFile, Format format, CompressionKind compression, List<Type> types)
            throws IOException
    {
        JobConf jobConf = new JobConf();
        jobConf.set("hive.exec.orc.write.format", format == ORC_12 ? "0.12" : "0.11");
        jobConf.set("hive.exec.orc.default.compress", compression.name());

        return new OrcOutputFormat().getHiveRecordWriter(
                jobConf,
                new Path(outputFile.toURI()),
                Text.class,
                compression != NONE,
                createTableProperties(types),
                () -> {});
    }

    private static RecordWriter createDwrfRecordWriter(File outputFile, CompressionKind compressionCodec, List<Type> types)
            throws IOException
    {
        JobConf jobConf = new JobConf();
        jobConf.set("hive.exec.orc.default.compress", compressionCodec.name());
        jobConf.set("hive.exec.orc.compress", compressionCodec.name());
        OrcConf.setIntVar(jobConf, OrcConf.ConfVars.HIVE_ORC_ENTROPY_STRING_THRESHOLD, 1);
        OrcConf.setIntVar(jobConf, OrcConf.ConfVars.HIVE_ORC_DICTIONARY_ENCODING_INTERVAL, 2);
        OrcConf.setBoolVar(jobConf, OrcConf.ConfVars.HIVE_ORC_BUILD_STRIDE_DICTIONARY, true);

        return new com.facebook.hive.orc.OrcOutputFormat().getHiveRecordWriter(
                jobConf,
                new Path(outputFile.toURI()),
                Text.class,
                compressionCodec != NONE,
                createTableProperties(types),
                () -> {});
    }

    static SettableStructObjectInspector createSettableStructObjectInspector(String name, Type type)
    {
        return getStandardStructObjectInspector(ImmutableList.of(name), ImmutableList.of(getJavaObjectInspector(type)));
    }

    static SettableStructObjectInspector createSettableStructObjectInspector(List<Type> types)
    {
        List<ObjectInspector> columnTypes = types.stream()
                .map(OrcTester::getJavaObjectInspector)
                .collect(toList());

        return getStandardStructObjectInspector(makeColumnNames(types.size()), columnTypes);
    }

    private static Properties createTableProperties(List<Type> types)
    {
        String columnTypes = types.stream()
                .map(OrcTester::getJavaObjectInspector)
                .map(ObjectInspector::getTypeName)
                .collect(Collectors.joining(", "));

        Properties orderTableProperties = new Properties();
        orderTableProperties.setProperty("columns", String.join(", ", makeColumnNames(types.size())));
        orderTableProperties.setProperty("columns.types", columnTypes);
        orderTableProperties.setProperty("orc.bloom.filter.columns", String.join(", ", makeColumnNames(types.size())));
        orderTableProperties.setProperty("orc.bloom.filter.fpp", "0.50");
        orderTableProperties.setProperty("orc.bloom.filter.write.version", "original");
        return orderTableProperties;
    }

    private static <T> List<T> reverse(List<T> iterable)
    {
        return Lists.reverse(ImmutableList.copyOf(iterable));
    }

    private static <T> List<T> insertNullEvery(int n, List<T> iterable)
    {
        return newArrayList(() -> new AbstractIterator<T>()
        {
            private final Iterator<T> delegate = iterable.iterator();
            private int position;
            private int totalCount;

            @Override
            protected T computeNext()
            {
                if (totalCount >= iterable.size()) {
                    return endOfData();
                }

                totalCount++;
                position++;
                if (position > n) {
                    position = 0;
                    return null;
                }

                if (!delegate.hasNext()) {
                    return endOfData();
                }

                return delegate.next();
            }
        });
    }

    public static List<Object> toHiveStruct(Object input)
    {
        return asList(input, input, input);
    }

    private static List<Object> toHiveStructWithNull(Object input)
    {
        return asList(input, input, input, null, null, null);
    }

    private static Map<Object, Object> toHiveMap(Object input, Object nullKeyValue)
    {
        Map<Object, Object> map = new HashMap<>();
        map.put(input != null ? input : nullKeyValue, input);
        return map;
    }

    private static List<Object> toHiveList(Object input)
    {
        return asList(input, input, input, input);
    }

    private static boolean hasType(Type testType, Set<String> baseTypes)
    {
        String testBaseType = testType.getTypeSignature().getBase();
        if (StandardTypes.ARRAY.equals(testBaseType)) {
            Type elementType = testType.getTypeParameters().get(0);
            return hasType(elementType, baseTypes);
        }
        if (StandardTypes.MAP.equals(testBaseType)) {
            Type keyType = testType.getTypeParameters().get(0);
            Type valueType = testType.getTypeParameters().get(1);
            return hasType(keyType, baseTypes) || hasType(valueType, baseTypes);
        }
        if (StandardTypes.ROW.equals(testBaseType)) {
            return testType.getTypeParameters().stream()
                    .anyMatch(fieldType -> hasType(fieldType, baseTypes));
        }

        return baseTypes.contains(testBaseType);
    }

    public static Type arrayType(Type elementType)
    {
        return FUNCTION_AND_TYPE_MANAGER.getParameterizedType(StandardTypes.ARRAY, ImmutableList.of(TypeSignatureParameter.of(elementType.getTypeSignature())));
    }

    public static Type mapType(Type keyType, Type valueType)
    {
        return FUNCTION_AND_TYPE_MANAGER.getParameterizedType(StandardTypes.MAP, ImmutableList.of(TypeSignatureParameter.of(keyType.getTypeSignature()), TypeSignatureParameter.of(valueType.getTypeSignature())));
    }

    public static Type rowType(Type... fieldTypes)
    {
        ImmutableList.Builder<TypeSignatureParameter> typeSignatureParameters = ImmutableList.builder();
        for (int i = 0; i < fieldTypes.length; i++) {
            String filedName = "field_" + i;
            Type fieldType = fieldTypes[i];
            typeSignatureParameters.add(TypeSignatureParameter.of(new NamedTypeSignature(Optional.of(new RowFieldName(filedName, false)), fieldType.getTypeSignature())));
        }
        return FUNCTION_AND_TYPE_MANAGER.getParameterizedType(StandardTypes.ROW, typeSignatureParameters.build());
    }
}

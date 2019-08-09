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
import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.Subfield;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.NamedTypeSignature;
import com.facebook.presto.spi.type.RowFieldName;
import com.facebook.presto.spi.type.SqlDate;
import com.facebook.presto.spi.type.SqlDecimal;
import com.facebook.presto.spi.type.SqlTimestamp;
import com.facebook.presto.spi.type.SqlVarbinary;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.type.TypeRegistry;
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

import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
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
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.Chars.truncateToLengthAndTrimSpaces;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.Decimals.rescale;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.Varchars.truncateToLength;
import static com.facebook.presto.testing.DateTimeTestingUtils.sqlTimestampOf;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
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
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class OrcTester
{
    public static final DataSize MAX_BLOCK_SIZE = new DataSize(1, Unit.MEGABYTE);
    public static final DateTimeZone HIVE_STORAGE_TIME_ZONE = DateTimeZone.forID("America/Bahia_Banderas");

    private static final TypeManager TYPE_MANAGER = new TypeRegistry();
    private static final List<Integer> PRIME_NUMBERS = ImmutableList.of(5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97);

    static {
        // associate TYPE_MANAGER with a function manager
        new FunctionManager(TYPE_MANAGER, new BlockEncodingManager(TYPE_MANAGER), new FeaturesConfig());
    }

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
        orcTester.compressions = ImmutableSet.of(ZLIB);
        orcTester.useSelectiveOrcReader = true;

        return orcTester;
    }

    public void testRoundTrip(Type type, List<?> readValues)
            throws Exception
    {
        testRoundTrip(type, readValues, ImmutableList.of());
    }

    public void testRoundTrip(Type type, List<?> readValues, List<Map<Integer, TupleDomainFilter>> filters)
            throws Exception
    {
        // just the values
        testRoundTripTypes(ImmutableList.of(type), ImmutableList.of(readValues), filters);

        // all nulls
        if (nullTestsEnabled) {
            assertRoundTrip(
                    type,
                    readValues.stream()
                            .map(value -> null)
                            .collect(toList()),
                    filters);
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

    public void testRoundTripTypes(List<Type> types, List<List<?>> readValues, List<Map<Integer, TupleDomainFilter>> filters)
            throws Exception
    {
        assertEquals(types.size(), readValues.size());

        // forward order
        assertRoundTrip(types, readValues, filters);

        // reverse order
        if (reverseTestsEnabled) {
            assertRoundTrip(types, Lists.transform(readValues, OrcTester::reverse), filters);
        }

        if (nullTestsEnabled) {
            // forward order with nulls
            assertRoundTrip(types, insertNulls(types, readValues), filters);

            // reverse order with nulls
            if (reverseTestsEnabled) {
                assertRoundTrip(types, insertNulls(types, Lists.transform(readValues, OrcTester::reverse)), filters);
            }
        }
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

    public void assertRoundTrip(Type type, List<?> readValues, List<Map<Integer, TupleDomainFilter>> filters)
            throws Exception
    {
        assertRoundTrip(type, type, readValues, readValues, true, filters);
    }

    public void assertRoundTrip(Type type, List<?> readValues, boolean verifyWithHiveReader)
            throws Exception
    {
        assertRoundTrip(type, type, readValues, readValues, verifyWithHiveReader, ImmutableList.of());
    }

    public void assertRoundTrip(List<Type> types, List<List<?>> readValues, List<Map<Integer, TupleDomainFilter>> filters)
            throws Exception
    {
        assertRoundTrip(types, types, readValues, readValues, true, filters);
    }

    private void assertRoundTrip(Type writeType, Type readType, List<?> writeValues, List<?> readValues, boolean verifyWithHiveReader, List<Map<Integer, TupleDomainFilter>> filters)
            throws Exception
    {
        assertRoundTrip(ImmutableList.of(writeType), ImmutableList.of(readType), ImmutableList.of(writeValues), ImmutableList.of(readValues), verifyWithHiveReader, filters);
    }

    private void assertRoundTrip(List<Type> writeTypes, List<Type> readTypes, List<List<?>> writeValues, List<List<?>> readValues, boolean verifyWithHiveReader, List<Map<Integer, TupleDomainFilter>> filters)
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
                        assertFileContentsPresto(readTypes, tempFile, readValues, false, false, orcEncoding, format, true, useSelectiveOrcReader, filters);
                    }
                }

                // write Presto, read Hive and Presto
                try (TempFile tempFile = new TempFile()) {
                    writeOrcColumnsPresto(tempFile.getFile(), format, compression, writeTypes, writeValues, stats);

                    if (verifyWithHiveReader && hiveSupported) {
                        assertFileContentsHive(readTypes, tempFile, format, readValues);
                    }

                    assertFileContentsPresto(readTypes, tempFile, readValues, false, false, orcEncoding, format, false, useSelectiveOrcReader, filters);

                    if (skipBatchTestsEnabled) {
                        assertFileContentsPresto(readTypes, tempFile, readValues, true, false, orcEncoding, format, false, useSelectiveOrcReader, filters);
                    }

                    if (skipStripeTestsEnabled) {
                        assertFileContentsPresto(readTypes, tempFile, readValues, false, true, orcEncoding, format, false, useSelectiveOrcReader, filters);
                    }
                }
            }
        }

        assertEquals(stats.getWriterSizeInBytes(), 0);
    }

    private static void assertFileContentsPresto(
            List<Type> types,
            TempFile tempFile,
            List<List<?>> expectedValues,
            OrcEncoding orcEncoding,
            OrcPredicate orcPredicate,
            Optional<Map<Integer, TupleDomainFilter>> filters)
            throws IOException
    {
        try (OrcSelectiveRecordReader recordReader = createCustomOrcSelectiveRecordReader(tempFile, orcEncoding, orcPredicate, types, MAX_BATCH_SIZE, filters.orElse(ImmutableMap.of()))) {
            assertEquals(recordReader.getReaderPosition(), 0);
            assertEquals(recordReader.getFilePosition(), 0);

            int rowsProcessed = 0;
            while (true) {
                Page page = recordReader.getNextPage();
                if (page == null) {
                    break;
                }

                if (page.getPositionCount() == 0) {
                    continue;
                }

                for (int i = 0; i < types.size(); i++) {
                    Type type = types.get(i);
                    Block block = page.getBlock(i);

                    List<Object> data = new ArrayList<>(block.getPositionCount());
                    for (int position = 0; position < block.getPositionCount(); position++) {
                        data.add(type.getObjectValue(SESSION, block, position));
                    }

                    for (int j = 0; j < block.getPositionCount(); j++) {
                        assertColumnValueEquals(type, data.get(j), expectedValues.get(i).get(rowsProcessed + j));
                    }
                }

                rowsProcessed += page.getPositionCount();
            }

            assertEquals(rowsProcessed, expectedValues.get(0).size());
        }
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
            List<Map<Integer, TupleDomainFilter>> filters)
            throws IOException
    {
        OrcPredicate orcPredicate = createOrcPredicate(types, expectedValues, format, isHiveWriter);
        if (useSelectiveOrcReader) {
            assertFileContentsPresto(types, tempFile, expectedValues, orcEncoding, orcPredicate, Optional.empty());

            for (Map<Integer, TupleDomainFilter> columnFilters : filters) {
                assertFileContentsPresto(types, tempFile, filterRows(types, expectedValues, columnFilters), orcEncoding, orcPredicate, Optional.of(columnFilters));
            }

            return;
        }

        try (OrcBatchRecordReader recordReader = createCustomOrcRecordReader(tempFile, orcEncoding, orcPredicate, types, MAX_BATCH_SIZE)) {
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
                        Block block = recordReader.readBlock(type, i);
                        assertEquals(block.getPositionCount(), batchSize);

                        List<Object> data = new ArrayList<>(block.getPositionCount());
                        for (int position = 0; position < block.getPositionCount(); position++) {
                            data.add(type.getObjectValue(SESSION, block, position));
                        }

                        for (int position = 0; position < block.getPositionCount(); position++) {
                            assertColumnValueEquals(type, data.get(position), expectedValues.get(i).get(rowsProcessed + position));
                        }
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

    private static List<List<?>> filterRows(List<Type> types, List<List<?>> values, Map<Integer, TupleDomainFilter> columnFilters)
    {
        List<Integer> passingRows = IntStream.range(0, values.get(0).size())
                .filter(row -> testRow(types, values, row, columnFilters))
                .boxed()
                .collect(toList());
        return IntStream.range(0, values.size())
                .mapToObj(column -> passingRows.stream().map(values.get(column)::get).collect(toList()))
                .collect(toList());
    }

    private static boolean testRow(List<Type> types, List<List<?>> values, int row, Map<Integer, TupleDomainFilter> columnFilters)
    {
        for (int column = 0; column < types.size(); column++) {
            TupleDomainFilter filter = columnFilters.get(column);
            if (filter == null) {
                continue;
            }

            Object value = values.get(column).get(row);
            if (filter == IS_NULL) {
                if (value != null) {
                    return false;
                }
            }
            else if (filter == IS_NOT_NULL) {
                if (value == null) {
                    return false;
                }
            }
            else if (value == null) {
                if (!filter.testNull()) {
                    return false;
                }
            }
            else {
                Type type = types.get(column);
                if (type == BOOLEAN) {
                    if (!filter.testBoolean((Boolean) value)) {
                        return false;
                    }
                }
                else if (type == TINYINT || type == BIGINT || type == INTEGER || type == SMALLINT) {
                    if (!filter.testLong(((Number) value).longValue())) {
                        return false;
                    }
                }
                else if (type == DOUBLE) {
                    if (!filter.testDouble((double) value)) {
                        return false;
                    }
                }
                else if (type == DATE) {
                    if (!filter.testLong(((SqlDate) value).getDays())) {
                        return false;
                    }
                }
                else if (type == REAL) {
                    if (!filter.testFloat(((Number) value).floatValue())) {
                        return false;
                    }
                }
                else if (type == TIMESTAMP) {
                    if (!filter.testLong(((SqlTimestamp) value).getMillisUtc())) {
                        return false;
                    }
                }

                else {
                    fail("Unsupported type: " + type);
                }
            }
        }

        return true;
    }

    private static void assertColumnValueEquals(Type type, Object actual, Object expected)
    {
        if (actual == null) {
            assertNull(expected);
            return;
        }
        String baseType = type.getTypeSignature().getBase();
        if (StandardTypes.ARRAY.equals(baseType)) {
            List<?> actualArray = (List<?>) actual;
            List<?> expectedArray = (List<?>) expected;
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

    static OrcBatchRecordReader createCustomOrcRecordReader(TempFile tempFile, OrcEncoding orcEncoding, OrcPredicate predicate, Type type, int initialBatchSize)
            throws IOException
    {
        return createCustomOrcRecordReader(tempFile, orcEncoding, predicate, ImmutableList.of(type), initialBatchSize);
    }

    private static OrcBatchRecordReader createCustomOrcRecordReader(TempFile tempFile, OrcEncoding orcEncoding, OrcPredicate predicate, List<Type> types, int initialBatchSize)
            throws IOException
    {
        OrcDataSource orcDataSource = new FileOrcDataSource(tempFile.getFile(), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), true);
        OrcReader orcReader = new OrcReader(orcDataSource, orcEncoding, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), MAX_BLOCK_SIZE);

        assertEquals(orcReader.getColumnNames(), ImmutableList.of("test"));
        assertEquals(orcReader.getFooter().getRowsInRowGroup(), 10_000);

        Map<Integer, Type> columnTypes = IntStream.range(0, types.size())
                .boxed()
                .collect(toImmutableMap(Functions.identity(), types::get));

        return orcReader.createBatchRecordReader(columnTypes, predicate, HIVE_STORAGE_TIME_ZONE, newSimpleAggregatedMemoryContext(), initialBatchSize);
    }

    private static void writeOrcColumnsPresto(File outputFile, Format format, CompressionKind compression, List<Type> types, List<List<?>> values, OrcWriterStats stats)
            throws Exception
    {
        List<String> columnNames = makeColumnNames(types.size());

        ImmutableMap.Builder<String, String> metadata = ImmutableMap.builder();
        metadata.put("columns", String.join(", ", columnNames));
        metadata.put("columns.types", createSettableStructObjectInspector(types).getTypeName());

        OrcWriter writer = new OrcWriter(
                new OutputStreamOrcDataSink(new FileOutputStream(outputFile)),
                columnNames,
                types,
                format.getOrcEncoding(),
                compression,
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
        writer.validate(new FileOrcDataSource(outputFile, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), true));
    }

    static OrcSelectiveRecordReader createCustomOrcSelectiveRecordReader(
            TempFile tempFile,
            OrcEncoding orcEncoding,
            OrcPredicate predicate,
            List<Type> types,
            int initialBatchSize,
            Map<Integer, TupleDomainFilter> filters)
            throws IOException
    {
        OrcDataSource orcDataSource = new FileOrcDataSource(tempFile.getFile(), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), true);
        OrcReader orcReader = new OrcReader(orcDataSource, orcEncoding, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), MAX_BLOCK_SIZE);

        assertEquals(orcReader.getColumnNames(), makeColumnNames(types.size()));
        assertEquals(orcReader.getFooter().getRowsInRowGroup(), 10_000);

        Map<Integer, Type> columnTypes = IntStream.range(0, types.size())
                .boxed()
                .collect(toImmutableMap(Function.identity(), types::get));

        return orcReader.createSelectiveRecordReader(
                columnTypes,
                IntStream.range(0, types.size()).boxed().collect(toList()),
                Maps.transformValues(filters, v -> ImmutableMap.of(new Subfield("c"), v)),
                ImmutableList.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                predicate,
                0,
                orcDataSource.getSize(),
                HIVE_STORAGE_TIME_ZONE,
                newSimpleAggregatedMemoryContext(),
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

    private static DataSize writeOrcColumnsHive(File outputFile, Format format, CompressionKind compression, List<Type> types, List<List<?>> values)
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

    private static RecordWriter createOrcRecordWriter(File outputFile, Format format, CompressionKind compression, List<Type> types)
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

    private static SettableStructObjectInspector createSettableStructObjectInspector(List<Type> types)
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

    private static List<Object> toHiveStruct(Object input)
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
        return TYPE_MANAGER.getParameterizedType(StandardTypes.ARRAY, ImmutableList.of(TypeSignatureParameter.of(elementType.getTypeSignature())));
    }

    private static Type mapType(Type keyType, Type valueType)
    {
        return TYPE_MANAGER.getParameterizedType(StandardTypes.MAP, ImmutableList.of(TypeSignatureParameter.of(keyType.getTypeSignature()), TypeSignatureParameter.of(valueType.getTypeSignature())));
    }

    public static Type rowType(Type... fieldTypes)
    {
        ImmutableList.Builder<TypeSignatureParameter> typeSignatureParameters = ImmutableList.builder();
        for (int i = 0; i < fieldTypes.length; i++) {
            String filedName = "field_" + i;
            Type fieldType = fieldTypes[i];
            typeSignatureParameters.add(TypeSignatureParameter.of(new NamedTypeSignature(Optional.of(new RowFieldName(filedName, false)), fieldType.getTypeSignature())));
        }
        return TYPE_MANAGER.getParameterizedType(StandardTypes.ROW, typeSignatureParameters.build());
    }
}

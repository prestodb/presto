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
package com.facebook.presto.rcfile;

import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.hadoop.HadoopNative;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.rcfile.binary.BinaryRcFileEncoding;
import com.facebook.presto.rcfile.text.TextRcFileEncoding;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.SqlDate;
import com.facebook.presto.spi.type.SqlDecimal;
import com.facebook.presto.spi.type.SqlTimestamp;
import com.facebook.presto.spi.type.SqlVarbinary;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.hadoop.compression.lzo.LzoCodec;
import io.airlift.slice.OutputStreamSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.StructObject;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.lazy.LazyArray;
import org.apache.hadoop.hive.serde2.lazy.LazyMap;
import org.apache.hadoop.hive.serde2.lazy.LazyPrimitive;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryArray;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryMap;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
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
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.joda.time.DateTimeZone;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.rcfile.RcFileDecoderUtils.findFirstSyncPosition;
import static com.facebook.presto.rcfile.RcFileTester.Compression.BZIP2;
import static com.facebook.presto.rcfile.RcFileTester.Compression.LZ4;
import static com.facebook.presto.rcfile.RcFileTester.Compression.NONE;
import static com.facebook.presto.rcfile.RcFileTester.Compression.SNAPPY;
import static com.facebook.presto.rcfile.RcFileTester.Compression.ZLIB;
import static com.facebook.presto.rcfile.RcFileWriter.PRESTO_RCFILE_WRITER_VERSION;
import static com.facebook.presto.rcfile.RcFileWriter.PRESTO_RCFILE_WRITER_VERSION_METADATA_KEY;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.Decimals.rescale;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.StandardTypes.ARRAY;
import static com.facebook.presto.spi.type.StandardTypes.MAP;
import static com.facebook.presto.spi.type.StandardTypes.ROW;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.google.common.base.Functions.constant;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Iterators.advance;
import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Math.toIntExact;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMNS;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMN_TYPES;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;
import static org.apache.hadoop.hive.serde2.ColumnProjectionUtils.READ_ALL_COLUMNS;
import static org.apache.hadoop.hive.serde2.ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR;
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
import static org.apache.hadoop.mapred.Reporter.NULL;
import static org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.COMPRESS_CODEC;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@SuppressWarnings("StaticPseudoFunctionalStyleMethod")
public class RcFileTester
{
    private static final TypeManager TYPE_MANAGER = new TypeRegistry();

    static {
        // associate TYPE_MANAGER with a function registry
        new FunctionRegistry(TYPE_MANAGER, new BlockEncodingManager(TYPE_MANAGER), new FeaturesConfig());

        HadoopNative.requireHadoopNative();
    }

    public static final DateTimeZone HIVE_STORAGE_TIME_ZONE = DateTimeZone.forID("Asia/Katmandu");

    public enum Format
    {
        BINARY {
            @Override
            @SuppressWarnings("deprecation")
            public Serializer createSerializer()
            {
                return new LazyBinaryColumnarSerDe();
            }

            @Override
            public RcFileEncoding getVectorEncoding()
            {
                return new BinaryRcFileEncoding();
            }
        },

        TEXT {
            @Override
            @SuppressWarnings("deprecation")
            public Serializer createSerializer()
            {
                try {
                    ColumnarSerDe columnarSerDe = new ColumnarSerDe();
                    Properties tableProperties = new Properties();
                    tableProperties.setProperty("columns", "test");
                    tableProperties.setProperty("columns.types", "string");
                    columnarSerDe.initialize(new JobConf(false), tableProperties);
                    return columnarSerDe;
                }
                catch (SerDeException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public RcFileEncoding getVectorEncoding()
            {
                return new TextRcFileEncoding(HIVE_STORAGE_TIME_ZONE);
            }
        };

        @SuppressWarnings("deprecation")
        public abstract Serializer createSerializer();

        public abstract RcFileEncoding getVectorEncoding();
    }

    public enum Compression
    {
        BZIP2 {
            @Override
            Optional<String> getCodecName()
            {
                return Optional.of(BZip2Codec.class.getName());
            }
        },
        ZLIB {
            @Override
            Optional<String> getCodecName()
            {
                return Optional.of(GzipCodec.class.getName());
            }
        },
        SNAPPY {
            @Override
            Optional<String> getCodecName()
            {
                return Optional.of(SnappyCodec.class.getName());
            }
        },
        LZO {
            @Override
            Optional<String> getCodecName()
            {
                return Optional.of(LzoCodec.class.getName());
            }
        },
        LZ4 {
            @Override
            Optional<String> getCodecName()
            {
                return Optional.of(Lz4Codec.class.getName());
            }
        },
        NONE {
            @Override
            Optional<String> getCodecName()
            {
                return Optional.empty();
            }
        };

        abstract Optional<String> getCodecName();
    }

    private boolean structTestsEnabled;
    private boolean mapTestsEnabled;
    private boolean listTestsEnabled;
    private boolean complexStructuralTestsEnabled;
    private boolean readLastBatchOnlyEnabled;
    private Set<Format> formats = ImmutableSet.of();
    private Set<Compression> compressions = ImmutableSet.of();

    public static RcFileTester quickTestRcFileReader()
    {
        RcFileTester rcFileTester = new RcFileTester();
        rcFileTester.structTestsEnabled = true;
        rcFileTester.mapTestsEnabled = true;
        rcFileTester.listTestsEnabled = true;
        rcFileTester.complexStructuralTestsEnabled = false;
        rcFileTester.readLastBatchOnlyEnabled = false;
        rcFileTester.formats = ImmutableSet.copyOf(Format.values());
        rcFileTester.compressions = ImmutableSet.of(SNAPPY);
        return rcFileTester;
    }

    public static RcFileTester fullTestRcFileReader()
    {
        RcFileTester rcFileTester = new RcFileTester();
        rcFileTester.structTestsEnabled = true;
        rcFileTester.mapTestsEnabled = true;
        rcFileTester.listTestsEnabled = true;
        rcFileTester.complexStructuralTestsEnabled = true;
        rcFileTester.readLastBatchOnlyEnabled = true;
        rcFileTester.formats = ImmutableSet.copyOf(Format.values());
        // These compression algorithms were chosen to cover the three different
        // cases: uncompressed, aircompressor, and hadoop compression
        // We assume that the compression algorithms generally work
        rcFileTester.compressions = ImmutableSet.of(NONE, LZ4, ZLIB, BZIP2);
        return rcFileTester;
    }

    public void testRoundTrip(Type type, Iterable<?> writeValues, Format... skipFormats)
            throws Exception
    {
        ImmutableSet<Format> skipFormatsSet = ImmutableSet.copyOf(skipFormats);

        // just the values
        testRoundTripType(type, writeValues, skipFormatsSet);

        // all nulls
        assertRoundTrip(type, transform(writeValues, constant(null)), skipFormatsSet);

        // values wrapped in struct
        if (structTestsEnabled) {
            testStructRoundTrip(type, writeValues, skipFormatsSet);
        }

        // values wrapped in a struct wrapped in a struct
        if (complexStructuralTestsEnabled) {
            Iterable<Object> simpleStructs = transform(insertNullEvery(5, writeValues), RcFileTester::toHiveStruct);
            testRoundTripType(
                    RowType.from(ImmutableList.of(RowType.field("field", createRowType(type)))),
                    transform(simpleStructs, Collections::singletonList),
                    skipFormatsSet);
        }

        // values wrapped in map
        if (mapTestsEnabled) {
            testMapRoundTrip(type, writeValues, skipFormatsSet);
        }

        // values wrapped in list
        if (listTestsEnabled) {
            testListRoundTrip(type, writeValues, skipFormatsSet);
        }

        // values wrapped in a list wrapped in a list
        if (complexStructuralTestsEnabled) {
            testListRoundTrip(
                    createListType(type),
                    transform(writeValues, RcFileTester::toHiveList),
                    skipFormatsSet);
        }
    }

    private void testStructRoundTrip(Type type, Iterable<?> writeValues, Set<Format> skipFormats)
            throws Exception
    {
        // values in simple struct and mix in some null values
        testRoundTripType(
                createRowType(type),
                transform(insertNullEvery(5, writeValues), RcFileTester::toHiveStruct),
                skipFormats);
    }

    private void testMapRoundTrip(Type type, Iterable<?> writeValues, Set<Format> skipFormats)
            throws Exception
    {
        // json does not support null keys, so we just write the first value
        Object nullKeyWrite = Iterables.getFirst(writeValues, null);

        // values in simple map and mix in some null values
        testRoundTripType(
                createMapType(type),
                transform(insertNullEvery(5, writeValues), value -> toHiveMap(nullKeyWrite, value)),
                skipFormats);
    }

    private void testListRoundTrip(Type type, Iterable<?> writeValues, Set<Format> skipFormats)
            throws Exception
    {
        // values in simple list and mix in some null values
        testRoundTripType(
                createListType(type),
                transform(insertNullEvery(5, writeValues), RcFileTester::toHiveList),
                skipFormats);
    }

    private void testRoundTripType(Type type, Iterable<?> writeValues, Set<Format> skipFormats)
            throws Exception
    {
        // mix in some nulls
        assertRoundTrip(type, insertNullEvery(5, writeValues), skipFormats);
    }

    private void assertRoundTrip(Type type, Iterable<?> writeValues, Set<Format> skipFormats)
            throws Exception
    {
        List<?> finalValues = Lists.newArrayList(writeValues);

        Set<Format> formats = new LinkedHashSet<>(this.formats);
        formats.removeAll(skipFormats);

        for (Format format : formats) {
            for (Compression compression : compressions) {
                // write old, read new
                try (TempFile tempFile = new TempFile()) {
                    writeRcFileColumnOld(tempFile.getFile(), format, compression, type, finalValues.iterator());
                    assertFileContentsNew(type, tempFile, format, finalValues, false, ImmutableMap.of());
                }

                // write new, read old and new
                try (TempFile tempFile = new TempFile()) {
                    Map<String, String> metadata = ImmutableMap.of(String.valueOf(ThreadLocalRandom.current().nextLong()), String.valueOf(ThreadLocalRandom.current().nextLong()));
                    writeRcFileColumnNew(tempFile.getFile(), format, compression, type, finalValues.iterator(), metadata);

                    assertFileContentsOld(type, tempFile, format, finalValues);

                    Map<String, String> expectedMetadata = ImmutableMap.<String, String>builder()
                            .putAll(metadata)
                            .put(PRESTO_RCFILE_WRITER_VERSION_METADATA_KEY, PRESTO_RCFILE_WRITER_VERSION)
                            .build();

                    assertFileContentsNew(type, tempFile, format, finalValues, false, expectedMetadata);

                    if (readLastBatchOnlyEnabled) {
                        assertFileContentsNew(type, tempFile, format, finalValues, true, expectedMetadata);
                    }
                }
            }
        }
    }

    private static void assertFileContentsNew(
            Type type,
            TempFile tempFile,
            Format format,
            List<?> expectedValues,
            boolean readLastBatchOnly,
            Map<String, String> metadata)
            throws IOException
    {
        try (RcFileReader recordReader = createRcFileReader(tempFile, type, format.getVectorEncoding())) {
            assertIndexOf(recordReader, tempFile.getFile());
            assertEquals(recordReader.getMetadata(), ImmutableMap.builder()
                    .putAll(metadata)
                    .put("hive.io.rcfile.column.number", "1")
                    .build());

            Iterator<?> iterator = expectedValues.iterator();
            int totalCount = 0;
            for (int batchSize = recordReader.advance(); batchSize >= 0; batchSize = toIntExact(recordReader.advance())) {
                totalCount += batchSize;
                if (readLastBatchOnly && totalCount == expectedValues.size()) {
                    assertEquals(advance(iterator, batchSize), batchSize);
                }
                else {
                    Block block = recordReader.readBlock(0);

                    List<Object> data = new ArrayList<>(block.getPositionCount());
                    for (int position = 0; position < block.getPositionCount(); position++) {
                        data.add(type.getObjectValue(SESSION, block, position));
                    }

                    for (int i = 0; i < batchSize; i++) {
                        assertTrue(iterator.hasNext());
                        Object expected = iterator.next();

                        Object actual = data.get(i);
                        assertColumnValueEquals(type, actual, expected);
                    }
                }
            }
            assertFalse(iterator.hasNext());
            assertEquals(recordReader.getRowsRead(), totalCount);
        }
    }

    private static void assertColumnValueEquals(Type type, Object actual, Object expected)
    {
        if (actual == null) {
            assertNull(expected);
            return;
        }
        String baseType = type.getTypeSignature().getBase();
        if (ARRAY.equals(baseType)) {
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
        else if (MAP.equals(baseType)) {
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
        else if (ROW.equals(baseType)) {
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

    private static void assertIndexOf(RcFileReader recordReader, File file)
            throws IOException
    {
        List<Long> syncPositionsBruteForce = getSyncPositionsBruteForce(recordReader, file);
        List<Long> syncPositionsSimple = getSyncPositionsSimple(recordReader, file);

        assertEquals(syncPositionsBruteForce, syncPositionsSimple);
    }

    private static List<Long> getSyncPositionsBruteForce(RcFileReader recordReader, File file)
    {
        Slice slice = Slices.allocate((int) file.length());
        try (InputStream in = new FileInputStream(file)) {
            slice.setBytes(0, in, slice.length());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        List<Long> syncPositionsBruteForce = new ArrayList<>();
        Slice sync = Slices.allocate(SIZE_OF_INT + SIZE_OF_LONG + SIZE_OF_LONG);
        sync.setInt(0, -1);
        sync.setBytes(SIZE_OF_INT, recordReader.getSync());

        long syncPosition = 0;
        while (syncPosition >= 0) {
            syncPosition = slice.indexOf(sync, (int) syncPosition);
            if (syncPosition > 0) {
                syncPositionsBruteForce.add(syncPosition);
                syncPosition++;
            }
        }
        return syncPositionsBruteForce;
    }

    private static List<Long> getSyncPositionsSimple(RcFileReader recordReader, File file)
            throws IOException
    {
        List<Long> syncPositions = new ArrayList<>();
        Slice sync = recordReader.getSync();
        long syncFirst = sync.getLong(0);
        long syncSecond = sync.getLong(8);
        long syncPosition = 0;
        try (RcFileDataSource dataSource = new FileRcFileDataSource(file)) {
            while (syncPosition >= 0) {
                syncPosition = findFirstSyncPosition(dataSource, syncPosition, file.length() - syncPosition, syncFirst, syncSecond);
                if (syncPosition > 0) {
                    assertEquals(findFirstSyncPosition(dataSource, syncPosition, 1, syncFirst, syncSecond), syncPosition);
                    assertEquals(findFirstSyncPosition(dataSource, syncPosition, 2, syncFirst, syncSecond), syncPosition);
                    assertEquals(findFirstSyncPosition(dataSource, syncPosition, 10, syncFirst, syncSecond), syncPosition);

                    assertEquals(findFirstSyncPosition(dataSource, syncPosition - 1, 1, syncFirst, syncSecond), -1);
                    assertEquals(findFirstSyncPosition(dataSource, syncPosition - 2, 2, syncFirst, syncSecond), -1);
                    assertEquals(findFirstSyncPosition(dataSource, syncPosition + 1, 1, syncFirst, syncSecond), -1);

                    syncPositions.add(syncPosition);
                    syncPosition++;
                }
            }
        }
        return syncPositions;
    }

    private static RcFileReader createRcFileReader(TempFile tempFile, Type type, RcFileEncoding encoding)
            throws IOException
    {
        RcFileDataSource rcFileDataSource = new FileRcFileDataSource(tempFile.getFile());
        RcFileReader rcFileReader = new RcFileReader(
                rcFileDataSource,
                encoding,
                ImmutableMap.of(0, type),
                new AircompressorCodecFactory(new HadoopCodecFactory(RcFileTester.class.getClassLoader())),
                0,
                tempFile.getFile().length(),
                new DataSize(8, MEGABYTE));

        assertEquals(rcFileReader.getColumnCount(), 1);

        return rcFileReader;
    }

    private static DataSize writeRcFileColumnNew(File outputFile, Format format, Compression compression, Type type, Iterator<?> values, Map<String, String> metadata)
            throws Exception
    {
        OutputStreamSliceOutput output = new OutputStreamSliceOutput(new FileOutputStream(outputFile));
        AircompressorCodecFactory codecFactory = new AircompressorCodecFactory(new HadoopCodecFactory(RcFileTester.class.getClassLoader()));
        RcFileWriter writer = new RcFileWriter(
                output,
                ImmutableList.of(type),
                format.getVectorEncoding(),
                compression.getCodecName(),
                codecFactory,
                metadata,
                new DataSize(100, KILOBYTE),   // use a smaller size to create more row groups
                new DataSize(200, KILOBYTE),
                true);
        BlockBuilder blockBuilder = type.createBlockBuilder(null, 1024);
        while (values.hasNext()) {
            Object value = values.next();
            writeValue(type, blockBuilder, value);
        }

        writer.write(new Page(blockBuilder.build()));
        writer.close();

        writer.validate(new FileRcFileDataSource(outputFile));

        return new DataSize(output.size(), BYTE);
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
            else if (TINYINT.equals(type)) {
                type.writeLong(blockBuilder, ((Number) value).longValue());
            }
            else if (SMALLINT.equals(type)) {
                type.writeLong(blockBuilder, ((Number) value).longValue());
            }
            else if (INTEGER.equals(type)) {
                type.writeLong(blockBuilder, ((Number) value).longValue());
            }
            else if (BIGINT.equals(type)) {
                type.writeLong(blockBuilder, ((Number) value).longValue());
            }
            else if (Decimals.isShortDecimal(type)) {
                type.writeLong(blockBuilder, ((SqlDecimal) value).toBigDecimal().unscaledValue().longValue());
            }
            else if (Decimals.isLongDecimal(type)) {
                type.writeSlice(blockBuilder, Decimals.encodeUnscaledValue(((SqlDecimal) value).toBigDecimal().unscaledValue()));
            }
            else if (REAL.equals(type)) {
                type.writeLong(blockBuilder, Float.floatToIntBits((Float) value));
            }
            else if (DOUBLE.equals(type)) {
                type.writeDouble(blockBuilder, ((Number) value).doubleValue());
            }
            else if (VARCHAR.equals(type)) {
                type.writeSlice(blockBuilder, Slices.utf8Slice((String) value));
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
                if (ARRAY.equals(baseType)) {
                    List<?> array = (List<?>) value;
                    Type elementType = type.getTypeParameters().get(0);
                    BlockBuilder arrayBlockBuilder = blockBuilder.beginBlockEntry();
                    for (Object elementValue : array) {
                        writeValue(elementType, arrayBlockBuilder, elementValue);
                    }
                    blockBuilder.closeEntry();
                }
                else if (MAP.equals(baseType)) {
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
                else if (ROW.equals(baseType)) {
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

    private static <K extends LongWritable, V extends BytesRefArrayWritable> void assertFileContentsOld(
            Type type,
            TempFile tempFile,
            Format format,
            Iterable<?> expectedValues)
            throws Exception
    {
        JobConf configuration = new JobConf(new Configuration(false));
        configuration.set(READ_COLUMN_IDS_CONF_STR, "0");
        configuration.setBoolean(READ_ALL_COLUMNS, false);

        Properties schema = new Properties();
        schema.setProperty(META_TABLE_COLUMNS, "test");
        schema.setProperty(META_TABLE_COLUMN_TYPES, getJavaObjectInspector(type).getTypeName());

        @SuppressWarnings("deprecation")
        Deserializer deserializer;
        if (format == Format.BINARY) {
            deserializer = new LazyBinaryColumnarSerDe();
        }
        else {
            deserializer = new ColumnarSerDe();
        }
        deserializer.initialize(configuration, schema);
        configuration.set(SERIALIZATION_LIB, deserializer.getClass().getName());

        InputFormat<K, V> inputFormat = new RCFileInputFormat<>();
        RecordReader<K, V> recordReader = inputFormat.getRecordReader(
                new FileSplit(new Path(tempFile.getFile().getAbsolutePath()), 0, tempFile.getFile().length(), (String[]) null),
                configuration,
                NULL);

        K key = recordReader.createKey();
        V value = recordReader.createValue();

        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();
        StructField field = rowInspector.getStructFieldRef("test");

        Iterator<?> iterator = expectedValues.iterator();
        while (recordReader.next(key, value)) {
            Object expectedValue = iterator.next();

            Object rowData = deserializer.deserialize(value);
            Object actualValue = rowInspector.getStructFieldData(rowData, field);
            actualValue = decodeRecordReaderValue(type, actualValue);
            assertColumnValueEquals(type, actualValue, expectedValue);
        }
        assertFalse(iterator.hasNext());
    }

    private static Object decodeRecordReaderValue(Type type, Object actualValue)
    {
        if (actualValue instanceof LazyPrimitive) {
            actualValue = ((LazyPrimitive<?, ?>) actualValue).getWritableObject();
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
            if (SESSION.isLegacyTimestamp()) {
                actualValue = new SqlTimestamp((timestamp.getSeconds() * 1000) + (timestamp.getNanos() / 1000000L), UTC_KEY);
            }
            else {
                actualValue = new SqlTimestamp((timestamp.getSeconds() * 1000) + (timestamp.getNanos() / 1000000L));
            }
        }
        else if (actualValue instanceof StructObject) {
            StructObject structObject = (StructObject) actualValue;
            actualValue = decodeRecordReaderStruct(type, structObject.getFieldsAsList());
        }
        else if (actualValue instanceof LazyBinaryArray) {
            actualValue = decodeRecordReaderList(type, ((LazyBinaryArray) actualValue).getList());
        }
        else if (actualValue instanceof LazyBinaryMap) {
            actualValue = decodeRecordReaderMap(type, ((LazyBinaryMap) actualValue).getMap());
        }
        else if (actualValue instanceof LazyArray) {
            actualValue = decodeRecordReaderList(type, ((LazyArray) actualValue).getList());
        }
        else if (actualValue instanceof LazyMap) {
            actualValue = decodeRecordReaderMap(type, ((LazyMap) actualValue).getMap());
        }
        else if (actualValue instanceof List) {
            actualValue = decodeRecordReaderList(type, ((List<?>) actualValue));
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
        return newFields;
    }

    private static DataSize writeRcFileColumnOld(File outputFile, Format format, Compression compression, Type type, Iterator<?> values)
            throws Exception
    {
        ObjectInspector columnObjectInspector = getJavaObjectInspector(type);
        RecordWriter recordWriter = createRcFileWriterOld(outputFile, compression, columnObjectInspector);

        SettableStructObjectInspector objectInspector = createSettableStructObjectInspector("test", columnObjectInspector);
        Object row = objectInspector.create();

        List<StructField> fields = ImmutableList.copyOf(objectInspector.getAllStructFieldRefs());
        @SuppressWarnings("deprecation") Serializer serializer = format.createSerializer();

        Properties tableProperties = new Properties();
        tableProperties.setProperty("columns", "test");
        tableProperties.setProperty("columns.types", objectInspector.getTypeName());
        serializer.initialize(new JobConf(false), tableProperties);

        while (values.hasNext()) {
            Object value = values.next();
            value = preprocessWriteValueOld(type, value);
            objectInspector.setStructFieldData(row, fields.get(0), value);

            Writable record = serializer.serialize(row, objectInspector);
            recordWriter.write(record);
        }

        recordWriter.close(false);
        return new DataSize(outputFile.length(), BYTE).convertToMostSuccinctDataSize();
    }

    private static ObjectInspector getJavaObjectInspector(Type type)
    {
        if (type.equals(BOOLEAN)) {
            return javaBooleanObjectInspector;
        }
        else if (type.equals(BIGINT)) {
            return javaLongObjectInspector;
        }
        else if (type.equals(INTEGER)) {
            return javaIntObjectInspector;
        }
        else if (type.equals(SMALLINT)) {
            return javaShortObjectInspector;
        }
        else if (type.equals(TINYINT)) {
            return javaByteObjectInspector;
        }
        else if (type.equals(REAL)) {
            return javaFloatObjectInspector;
        }
        else if (type.equals(DOUBLE)) {
            return javaDoubleObjectInspector;
        }
        else if (type instanceof VarcharType) {
            return javaStringObjectInspector;
        }
        else if (type.equals(VARBINARY)) {
            return javaByteArrayObjectInspector;
        }
        else if (type.equals(DATE)) {
            return javaDateObjectInspector;
        }
        else if (type.equals(TIMESTAMP)) {
            return javaTimestampObjectInspector;
        }
        else if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            return getPrimitiveJavaObjectInspector(new DecimalTypeInfo(decimalType.getPrecision(), decimalType.getScale()));
        }
        else if (type.getTypeSignature().getBase().equals(ARRAY)) {
            return ObjectInspectorFactory.getStandardListObjectInspector(getJavaObjectInspector(type.getTypeParameters().get(0)));
        }
        else if (type.getTypeSignature().getBase().equals(MAP)) {
            ObjectInspector keyObjectInspector = getJavaObjectInspector(type.getTypeParameters().get(0));
            ObjectInspector valueObjectInspector = getJavaObjectInspector(type.getTypeParameters().get(1));
            return ObjectInspectorFactory.getStandardMapObjectInspector(keyObjectInspector, valueObjectInspector);
        }
        else if (type.getTypeSignature().getBase().equals(ROW)) {
            return getStandardStructObjectInspector(
                    type.getTypeSignature().getParameters().stream()
                            .map(parameter -> parameter.getNamedTypeSignature().getName().get())
                            .collect(toList()),
                    type.getTypeParameters().stream()
                            .map(RcFileTester::getJavaObjectInspector)
                            .collect(toList()));
        }
        throw new IllegalArgumentException("unsupported type: " + type);
    }

    private static Object preprocessWriteValueOld(Type type, Object value)
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
        else if (type.equals(VARBINARY)) {
            return ((SqlVarbinary) value).getBytes();
        }
        else if (type.equals(DATE)) {
            int days = ((SqlDate) value).getDays();
            LocalDate localDate = LocalDate.ofEpochDay(days);
            ZonedDateTime zonedDateTime = localDate.atStartOfDay(ZoneId.systemDefault());

            long millis = zonedDateTime.toEpochSecond() * 1000;
            Date date = new Date(0);
            // mills must be set separately to avoid masking
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
        else if (type.getTypeSignature().getBase().equals(ARRAY)) {
            Type elementType = type.getTypeParameters().get(0);
            return ((List<?>) value).stream()
                    .map(element -> preprocessWriteValueOld(elementType, element))
                    .collect(toList());
        }
        else if (type.getTypeSignature().getBase().equals(MAP)) {
            Type keyType = type.getTypeParameters().get(0);
            Type valueType = type.getTypeParameters().get(1);
            Map<Object, Object> newMap = new HashMap<>();
            for (Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                newMap.put(preprocessWriteValueOld(keyType, entry.getKey()), preprocessWriteValueOld(valueType, entry.getValue()));
            }
            return newMap;
        }
        else if (type.getTypeSignature().getBase().equals(ROW)) {
            List<?> fieldValues = (List<?>) value;
            List<Type> fieldTypes = type.getTypeParameters();
            List<Object> newStruct = new ArrayList<>();
            for (int fieldId = 0; fieldId < fieldValues.size(); fieldId++) {
                newStruct.add(preprocessWriteValueOld(fieldTypes.get(fieldId), fieldValues.get(fieldId)));
            }
            return newStruct;
        }
        throw new IllegalArgumentException("unsupported type: " + type);
    }

    private static RecordWriter createRcFileWriterOld(File outputFile, Compression compression, ObjectInspector columnObjectInspector)
            throws IOException
    {
        JobConf jobConf = new JobConf(false);
        Optional<String> codecName = compression.getCodecName();
        codecName.ifPresent(s -> jobConf.set(COMPRESS_CODEC, s));

        return new RCFileOutputFormat().getHiveRecordWriter(
                jobConf,
                new Path(outputFile.toURI()),
                Text.class,
                codecName.isPresent(),
                createTableProperties("test", columnObjectInspector.getTypeName()),
                () -> {});
    }

    private static SettableStructObjectInspector createSettableStructObjectInspector(String name, ObjectInspector objectInspector)
    {
        return getStandardStructObjectInspector(ImmutableList.of(name), ImmutableList.of(objectInspector));
    }

    @SuppressWarnings("SpellCheckingInspection")
    private static Properties createTableProperties(String name, String type)
    {
        Properties orderTableProperties = new Properties();
        orderTableProperties.setProperty("columns", name);
        orderTableProperties.setProperty("columns.types", type);
        orderTableProperties.setProperty("file.inputformat", RCFileInputFormat.class.getName());
        return orderTableProperties;
    }

    private static class TempFile
            implements Closeable
    {
        private final File tempDir;
        private final File file;

        private TempFile()
        {
            tempDir = createTempDir();
            tempDir.mkdirs();
            file = new File(tempDir, "data.rcfile");
        }

        public File getFile()
        {
            return file;
        }

        @Override
        public void close()
                throws IOException
        {
            // hadoop creates crc files that must be deleted also, so just delete the whole directory
            deleteRecursively(tempDir.toPath(), ALLOW_INSECURE);
        }
    }

    private static <T> Iterable<T> insertNullEvery(int n, Iterable<T> iterable)
    {
        return () -> new AbstractIterator<T>()
        {
            private final Iterator<T> delegate = iterable.iterator();
            private int position;

            @Override
            protected T computeNext()
            {
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
        };
    }

    private static RowType createRowType(Type type)
    {
        return RowType.from(ImmutableList.of(
                RowType.field("a", type),
                RowType.field("b", type),
                RowType.field("c", type)));
    }

    private static Object toHiveStruct(Object input)
    {
        List<Object> data = new ArrayList<>();
        data.add(input);
        data.add(input);
        data.add(input);
        return data;
    }

    private static MapType createMapType(Type type)
    {
        return (MapType) TYPE_MANAGER.getParameterizedType(StandardTypes.MAP, ImmutableList.of(
                TypeSignatureParameter.of(type.getTypeSignature()),
                TypeSignatureParameter.of(type.getTypeSignature())));
    }

    private static Object toHiveMap(Object nullKeyValue, Object input)
    {
        Map<Object, Object> map = new HashMap<>();
        if (input == null) {
            // json doesn't support null keys, so just write the nullKeyValue
            map.put(nullKeyValue, null);
        }
        else {
            map.put(input, input);
        }
        return map;
    }

    private static ArrayType createListType(Type type)
    {
        return new ArrayType(type);
    }

    private static Object toHiveList(Object input)
    {
        ArrayList<Object> list = new ArrayList<>(4);
        for (int i = 0; i < 4; i++) {
            list.add(input);
        }
        return list;
    }
}

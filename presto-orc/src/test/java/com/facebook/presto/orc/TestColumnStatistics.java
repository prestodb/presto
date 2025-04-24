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

import com.facebook.airlift.units.DataSize;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.block.ArrayBlockBuilder;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.ByteArrayBlock;
import com.facebook.presto.common.block.IntArrayBlock;
import com.facebook.presto.common.block.LongArrayBlock;
import com.facebook.presto.common.block.MapBlockBuilder;
import com.facebook.presto.common.block.RowBlockBuilder;
import com.facebook.presto.common.block.ShortArrayBlock;
import com.facebook.presto.common.block.VariableWidthBlockBuilder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.cache.StorageOrcFileTailSource;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.metadata.DwrfSequenceEncoding;
import com.facebook.presto.orc.metadata.Footer;
import com.facebook.presto.orc.metadata.OrcType;
import com.facebook.presto.orc.metadata.RowGroupIndex;
import com.facebook.presto.orc.metadata.Stream;
import com.facebook.presto.orc.metadata.StripeFooter;
import com.facebook.presto.orc.metadata.StripeInformation;
import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.IntegerColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.IntegerStatistics;
import com.facebook.presto.orc.metadata.statistics.MapColumnStatisticsBuilder;
import com.facebook.presto.orc.metadata.statistics.MapStatistics;
import com.facebook.presto.orc.metadata.statistics.MapStatisticsEntry;
import com.facebook.presto.orc.proto.DwrfProto.KeyInfo;
import com.facebook.presto.orc.protobuf.ByteString;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import org.joda.time.DateTimeZone;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.facebook.airlift.units.DataSize.Unit.MEGABYTE;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP_MICROSECONDS;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.TypeUtils.writeNativeValue;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.orc.DwrfEncryptionProvider.NO_ENCRYPTION;
import static com.facebook.presto.orc.NoOpOrcWriterStats.NOOP_WRITER_STATS;
import static com.facebook.presto.orc.NoopOrcAggregatedMemoryContext.NOOP_ORC_AGGREGATED_MEMORY_CONTEXT;
import static com.facebook.presto.orc.OrcEncoding.DWRF;
import static com.facebook.presto.orc.OrcTester.createOrcWriter;
import static com.facebook.presto.orc.OrcTester.mapType;
import static com.facebook.presto.orc.metadata.CompressionKind.ZLIB;
import static com.facebook.presto.orc.metadata.statistics.ColumnStatistics.mergeColumnStatistics;
import static com.facebook.presto.orc.writer.ColumnWriter.NULL_SIZE;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestColumnStatistics
{
    private static final int STRIPE_MAX_ROW_COUNT = 4;
    private static final int ROW_GROUP_MAX_ROW_COUNT = 2;
    private static final int COLUMN = 1;
    private static final int DICT_ROW_GROUP_SIZE = 4;
    private static final int DICT_STRIPE_SIZE = 8;

    @DataProvider
    public static Object[][] mapKeyTypeProvider()
    {
        return new Object[][] {
                {BIGINT},
                {VARCHAR}
        };
    }

    @DataProvider
    public static Object[][] directEncodingRawAndStorageSizeProvider()
    {
        // total six values, the third value is null
        int size = 3 * ROW_GROUP_MAX_ROW_COUNT;

        Optional<boolean[]> valueIsNull = Optional.of(new boolean[] {false, false, true, false, false, false});
        Block byteValues = new ByteArrayBlock(size, valueIsNull, new byte[] {1, 1, 1, 1, 1, 1});
        Block shortValues = new ShortArrayBlock(size, valueIsNull, new short[] {1, 1, 1, 1, 1, 1});
        Block intValues = new IntArrayBlock(size, valueIsNull, new int[] {1, 1, 1, 1, 1, 1});
        Block bigIntValues = new LongArrayBlock(size, valueIsNull, new long[] {1L, 1L, 1L, 1L, 1L, 1L});

        Slice slice = utf8Slice("0123456789");
        Block sliceValues = new VariableWidthBlockBuilder(null, 6, 20)
                .writeBytes(slice, 0, slice.length()).closeEntry()
                .writeBytes(slice, 0, slice.length()).closeEntry()
                .appendNull()
                .writeBytes(slice, 0, slice.length()).closeEntry()
                .writeBytes(slice, 0, slice.length()).closeEntry()
                .writeBytes(slice, 0, slice.length()).closeEntry()
                .build();

        ArrayType arrayType = new ArrayType(INTEGER);
        ArrayBlockBuilder arrayBlockBuilder = (ArrayBlockBuilder) arrayType.createBlockBuilder(null, size);
        for (int i = 0; i < size; i++) {
            if (i == 2) {
                arrayBlockBuilder.appendNull();
            }
            else {
                BlockBuilder arrayElementBuilder = arrayBlockBuilder.beginBlockEntry();
                arrayElementBuilder.writeInt(1);
                arrayElementBuilder.writeInt(1);
                arrayBlockBuilder.closeEntry();
            }
        }
        Block arrayBlock = arrayBlockBuilder.build();

        Type mapType = mapType(INTEGER, BIGINT);
        MapBlockBuilder mapBlockBuilder = (MapBlockBuilder) mapType.createBlockBuilder(null, size);
        BlockBuilder mapKeyBuilder = mapBlockBuilder.getKeyBlockBuilder();
        BlockBuilder mapValueBuilder = mapBlockBuilder.getValueBlockBuilder();
        for (int i = 0; i < size; i++) {
            if (i == 2) {
                mapBlockBuilder.appendNull();
            }
            else {
                mapBlockBuilder.beginDirectEntry();
                mapKeyBuilder.writeInt(1);
                mapValueBuilder.writeLong(2L);
                mapBlockBuilder.closeEntry();
            }
        }
        Block mapBlock = mapBlockBuilder.build();

        RowType rowType = RowType.withDefaultFieldNames(ImmutableList.of(INTEGER));
        RowBlockBuilder rowBlockBuilder = new RowBlockBuilder(ImmutableList.of(INTEGER), null, size);
        for (int i = 0; i < size; i++) {
            if (i == 2) {
                rowBlockBuilder.appendNull();
            }
            else {
                BlockBuilder elementBuilder = rowBlockBuilder.beginBlockEntry();
                elementBuilder.writeInt(1);
                rowBlockBuilder.closeEntry();
            }
        }
        Block rowBlock = rowBlockBuilder.build();

        return new Object[][] {
                {BOOLEAN, 2, 2, 2, byteValues},
                {TINYINT, 2, 2, 2, byteValues},
                {SMALLINT, 4, 2 + 1, 4, shortValues},
                {INTEGER, 8, 4 + 1, 8, intValues},
                {BIGINT, 16, 8 + 1, 16, bigIntValues},
                {REAL, 8, 4 + 1, 8, intValues},
                {DOUBLE, 16, 8 + 1, 16, bigIntValues},
                {TIMESTAMP, 24, 12 + 1, 24, bigIntValues},
                {TIMESTAMP_MICROSECONDS, 24, 12 + 1, 24, bigIntValues},
                {VARCHAR, 20, 10 + 1, 20, sliceValues},
                {VARBINARY, 20, 10 + 1, 20, sliceValues},
                {arrayType, 16, 8 + 1, 16, arrayBlock},
                {rowType, 8, 4 + 1, 8, rowBlock},
                {mapType, 24, 12 + 1, 24, mapBlock},
        };
    }

    @DataProvider
    public static Object[][] rawAndStorageSizeDictionaryEncodingProvider()
    {
        // creates a file with two stripes, first stripe has tw row groups of 4 elements,
        // the second stripe has a single row group with 4 elements
        int size = DICT_ROW_GROUP_SIZE * 3;

        // prepare blocks with 12 elements, for the "some nulls" case set the last element to null
        boolean[] valueIsNullValues = new boolean[size];
        Arrays.fill(valueIsNullValues, false);
        valueIsNullValues[size - 1] = true;
        Optional<boolean[]> someNulls = Optional.of(valueIsNullValues);

        boolean[] valueIsNullAllNulls = new boolean[size];
        Arrays.fill(valueIsNullAllNulls, true);
        Optional<boolean[]> allNulls = Optional.of(valueIsNullAllNulls);

        int[] intDictArray = new int[size];
        Arrays.fill(intDictArray, Integer.MAX_VALUE);

        int[] intDirectArray = new int[size];
        for (int i = 0; i < intDirectArray.length; i++) {
            intDirectArray[i] = i;
        }

        long[] longDictArray = new long[size];
        Arrays.fill(longDictArray, Long.MAX_VALUE);

        long[] longDirectArray = new long[size];
        for (int i = 0; i < intDirectArray.length; i++) {
            longDirectArray[i] = i;
        }

        // build int blocks
        Block intBlockAllNulls = new IntArrayBlock(size, allNulls, intDictArray);
        Block intDictBlockSomeNulls = new IntArrayBlock(size, someNulls, intDictArray);
        Block intDictBlockNoNulls = new IntArrayBlock(size, Optional.empty(), intDictArray);
        Block intDirectBlockSomeNulls = new IntArrayBlock(size, someNulls, intDirectArray);
        Block intDirectBlockNoNulls = new IntArrayBlock(size, Optional.empty(), intDirectArray);

        // build long blocks
        Block longBlockAllNulls = new LongArrayBlock(size, allNulls, longDictArray);
        Block longDictBlockSomeNulls = new LongArrayBlock(size, someNulls, longDictArray);
        Block longDictBlockNoNulls = new LongArrayBlock(size, Optional.empty(), longDictArray);
        Block longDirectBlockSomeNulls = new LongArrayBlock(size, someNulls, longDirectArray);
        Block longDirectBlockNoNulls = new LongArrayBlock(size, Optional.empty(), longDirectArray);

        // build slice blocks
        VariableWidthBlockBuilder sliceAllNullsBuilder = new VariableWidthBlockBuilder(null, size, size);
        for (int i = 0; i < size; i++) {
            sliceAllNullsBuilder.appendNull();
        }

        Slice dictSlice = utf8Slice("0123456789"); // use a nice repeating slice for a dictionary
        VariableWidthBlockBuilder sliceDictSomeNullsBuilder = new VariableWidthBlockBuilder(null, size, size);
        VariableWidthBlockBuilder sliceDirectSomeNullsBuilder = new VariableWidthBlockBuilder(null, size, size);

        for (int i = 0; i < size; i++) {
            if (i == size - 1) {
                sliceDirectSomeNullsBuilder.appendNull();
                sliceDictSomeNullsBuilder.appendNull();
            }
            else {
                Slice directSlice = utf8Slice(Character.toString((char) ('a' + i))); // no repeats in this slice
                sliceDirectSomeNullsBuilder.writeBytes(directSlice, 0, directSlice.length()).closeEntry();
                sliceDictSomeNullsBuilder.writeBytes(dictSlice, 0, dictSlice.length()).closeEntry();
            }
        }

        VariableWidthBlockBuilder sliceDictNoNullsBuilder = new VariableWidthBlockBuilder(null, size, size);
        VariableWidthBlockBuilder sliceDirectNoNullsBuilder = new VariableWidthBlockBuilder(null, size, size);

        for (int i = 0; i < size; i++) {
            Slice directSlice = utf8Slice(Character.toString((char) ('a' + i))); // no repeats in this slice
            sliceDirectNoNullsBuilder.writeBytes(directSlice, 0, directSlice.length()).closeEntry();
            sliceDictNoNullsBuilder.writeBytes(dictSlice, 0, dictSlice.length()).closeEntry();
        }

        Block sliceBlockAllNulls = sliceAllNullsBuilder.build();
        Block sliceDictBlockSomeNulls = sliceDictSomeNullsBuilder.build();
        Block sliceDirectBlockSomeNulls = sliceDirectSomeNullsBuilder.build();
        Block sliceDictBlockNoNulls = sliceDictNoNullsBuilder.build();
        Block sliceDirectBlockNoNulls = sliceDirectNoNullsBuilder.build();

        int allNullsSize = (int) (DICT_ROW_GROUP_SIZE * NULL_SIZE); // = 4
        int intFullRowGroupSize = DICT_ROW_GROUP_SIZE * Integer.BYTES; // = 4 * 4 = 16
        int intPartialRowGroupSize = (int) ((DICT_ROW_GROUP_SIZE - 1) * Integer.BYTES + NULL_SIZE); // = 4 * 3 + 1 = 13

        int longFullRowGroupSize = DICT_ROW_GROUP_SIZE * Long.BYTES; // = 8 * 4 = 32
        int longPartialRowGroupSize = (int) ((DICT_ROW_GROUP_SIZE - 1) * Long.BYTES + NULL_SIZE); // = 8 * 3 + 1 = 25

        int sliceDictFullRowGroupSize = DICT_ROW_GROUP_SIZE * 10;
        int sliceDictPartialRowGroupSize = (int) ((DICT_ROW_GROUP_SIZE - 1) * 10 + NULL_SIZE);

        int sliceDirectFullRowGroupSize = DICT_ROW_GROUP_SIZE;
        int sliceDirectPartialRowGroupSize = (int) ((DICT_ROW_GROUP_SIZE - 1) + NULL_SIZE);

        return new Object[][] {
                {INTEGER, allNullsSize, allNullsSize, allNullsSize, ImmutableList.of(intBlockAllNulls)},
                {INTEGER, intFullRowGroupSize, intFullRowGroupSize, intPartialRowGroupSize, ImmutableList.of(intDictBlockSomeNulls)},
                {INTEGER, intFullRowGroupSize, intFullRowGroupSize, intFullRowGroupSize, ImmutableList.of(intDictBlockNoNulls)},
                {INTEGER, allNullsSize, allNullsSize, allNullsSize, partitionBlock(intBlockAllNulls)},
                {INTEGER, intFullRowGroupSize, intFullRowGroupSize, intPartialRowGroupSize, partitionBlock(intDictBlockSomeNulls)},
                {INTEGER, intFullRowGroupSize, intFullRowGroupSize, intFullRowGroupSize, partitionBlock(intDictBlockNoNulls)},
                {INTEGER, intFullRowGroupSize, intFullRowGroupSize, intPartialRowGroupSize, ImmutableList.of(intDirectBlockSomeNulls)},
                {INTEGER, intFullRowGroupSize, intFullRowGroupSize, intFullRowGroupSize, ImmutableList.of(intDirectBlockNoNulls)},
                {INTEGER, intFullRowGroupSize, intFullRowGroupSize, intPartialRowGroupSize, partitionBlock(intDirectBlockSomeNulls)},
                {INTEGER, intFullRowGroupSize, intFullRowGroupSize, intFullRowGroupSize, partitionBlock(intDirectBlockNoNulls)},

                {BIGINT, allNullsSize, allNullsSize, allNullsSize, ImmutableList.of(longBlockAllNulls)},
                {BIGINT, longFullRowGroupSize, longFullRowGroupSize, longPartialRowGroupSize, ImmutableList.of(longDictBlockSomeNulls)},
                {BIGINT, longFullRowGroupSize, longFullRowGroupSize, longFullRowGroupSize, ImmutableList.of(longDictBlockNoNulls)},
                {BIGINT, allNullsSize, allNullsSize, allNullsSize, partitionBlock(longBlockAllNulls)},
                {BIGINT, longFullRowGroupSize, longFullRowGroupSize, longPartialRowGroupSize, partitionBlock(longDictBlockSomeNulls)},
                {BIGINT, longFullRowGroupSize, longFullRowGroupSize, longFullRowGroupSize, partitionBlock(longDictBlockNoNulls)},
                {BIGINT, longFullRowGroupSize, longFullRowGroupSize, longPartialRowGroupSize, ImmutableList.of(longDirectBlockSomeNulls)},
                {BIGINT, longFullRowGroupSize, longFullRowGroupSize, longFullRowGroupSize, ImmutableList.of(longDirectBlockNoNulls)},
                {BIGINT, longFullRowGroupSize, longFullRowGroupSize, longPartialRowGroupSize, partitionBlock(longDirectBlockSomeNulls)},
                {BIGINT, longFullRowGroupSize, longFullRowGroupSize, longFullRowGroupSize, partitionBlock(longDirectBlockNoNulls)},

                {VARCHAR, allNullsSize, allNullsSize, allNullsSize, ImmutableList.of(sliceBlockAllNulls)},
                {VARCHAR, sliceDictFullRowGroupSize, sliceDictFullRowGroupSize, sliceDictPartialRowGroupSize, ImmutableList.of(sliceDictBlockSomeNulls)},
                {VARCHAR, sliceDictFullRowGroupSize, sliceDictFullRowGroupSize, sliceDictFullRowGroupSize, ImmutableList.of(sliceDictBlockNoNulls)},
                {VARCHAR, allNullsSize, allNullsSize, allNullsSize, partitionBlock(sliceBlockAllNulls)},
                {VARCHAR, sliceDictFullRowGroupSize, sliceDictFullRowGroupSize, sliceDictPartialRowGroupSize, partitionBlock(sliceDictBlockSomeNulls)},
                {VARCHAR, sliceDictFullRowGroupSize, sliceDictFullRowGroupSize, sliceDictFullRowGroupSize, partitionBlock(sliceDictBlockNoNulls)},
                {VARCHAR, sliceDirectFullRowGroupSize, sliceDirectFullRowGroupSize, sliceDirectPartialRowGroupSize, ImmutableList.of(sliceDirectBlockSomeNulls)},
                {VARCHAR, sliceDirectFullRowGroupSize, sliceDirectFullRowGroupSize, sliceDirectFullRowGroupSize, ImmutableList.of(sliceDirectBlockNoNulls)},
                {VARCHAR, sliceDirectFullRowGroupSize, sliceDirectFullRowGroupSize, sliceDirectPartialRowGroupSize, partitionBlock(sliceDirectBlockSomeNulls)},
                {VARCHAR, sliceDirectFullRowGroupSize, sliceDirectFullRowGroupSize, sliceDirectFullRowGroupSize, partitionBlock(sliceDirectBlockNoNulls)},
        };
    }

    @Test(dataProvider = "directEncodingRawAndStorageSizeProvider")
    public void testRawAndStorageSizeDirectEncoding(
            Type type,
            long expectedRowGroupStatsRawSize1,
            long expectedRowGroupStatsRawSize2,
            long expectedRowGroupStatsRawSize3,
            Block block)
            throws IOException
    {
        boolean stringDictionaryEncodingEnabled = false;
        boolean integerDictionaryEncodingEnabled = false;

        OrcWriterOptions orcWriterOptions = createOrcWriterOptions(
                STRIPE_MAX_ROW_COUNT,
                ROW_GROUP_MAX_ROW_COUNT,
                stringDictionaryEncodingEnabled,
                integerDictionaryEncodingEnabled);

        assertRawAndStorageSizeForPrimitiveType(
                type,
                expectedRowGroupStatsRawSize1,
                expectedRowGroupStatsRawSize2,
                expectedRowGroupStatsRawSize3,
                ImmutableList.of(block),
                orcWriterOptions);
    }

    @Test(dataProvider = "rawAndStorageSizeDictionaryEncodingProvider")
    public void testRawAndStorageSizeDictionaryEncoding(
            Type type,
            long expectedRowGroupStatsRawSize1,
            long expectedRowGroupStatsRawSize2,
            long expectedRowGroupStatsRawSize3,
            List<Block> blocks)
            throws IOException
    {
        boolean stringDictionaryEncodingEnabled = true;
        boolean integerDictionaryEncodingEnabled = true;

        OrcWriterOptions orcWriterOptions = createOrcWriterOptions(
                DICT_STRIPE_SIZE,
                DICT_ROW_GROUP_SIZE,
                stringDictionaryEncodingEnabled,
                integerDictionaryEncodingEnabled);

        assertRawAndStorageSizeForPrimitiveType(
                type,
                expectedRowGroupStatsRawSize1,
                expectedRowGroupStatsRawSize2,
                expectedRowGroupStatsRawSize3,
                blocks,
                orcWriterOptions);
    }

    public void assertRawAndStorageSizeForPrimitiveType(
            Type type,
            long expectedRowGroupStatsRawSize1,
            long expectedRowGroupStatsRawSize2,
            long expectedRowGroupStatsRawSize3,
            List<Block> blocks,
            OrcWriterOptions orcWriterOptions)
            throws IOException
    {
        long expectedFileStatsRawSize = expectedRowGroupStatsRawSize1 + expectedRowGroupStatsRawSize2 + expectedRowGroupStatsRawSize3;

        CapturingOrcFileIntrospector introspector;

        try (TempFile tempFile = new TempFile()) {
            List<Page> pages = blocks.stream()
                    .map(block -> new Page(block.getPositionCount(), block))
                    .collect(toList());
            new OrcWrite(tempFile.getFile())
                    .withTypes(type)
                    .withWriterOptions(orcWriterOptions)
                    .writePages(pages)
                    .close();
            introspector = introspectOrcFile(type, tempFile);
        }

        // validate file statistics
        List<ColumnStatistics> allFileStatistics = introspector.getFileFooter().getFileStats();
        ColumnStatistics actualFileStatistics = allFileStatistics.get(COLUMN);
        assertEquals(actualFileStatistics.getRawSize(), expectedFileStatsRawSize, "file raw sizes do not match");

        // prepare to validate row group statistics
        assertEquals(introspector.getStripes().size(), 2);
        List<StripeInformation> stripeInformations = introspector.getStripeInformations();
        assertEquals(stripeInformations.size(), 2);
        assertEquals(introspector.getRowGroupIndexesByStripeOffset().size(), 2);

        long stripeOffset1 = stripeInformations.get(0).getOffset();
        long stripeOffset2 = stripeInformations.get(1).getOffset();

        Map<StreamId, List<RowGroupIndex>> stripeRowGroupIndexes1 = introspector.getRowGroupIndexesByStripeOffset().get(stripeOffset1);
        Map<StreamId, List<RowGroupIndex>> stripeRowGroupIndexes2 = introspector.getRowGroupIndexesByStripeOffset().get(stripeOffset2);
        assertNumberOfRowGroupStatistics(stripeRowGroupIndexes1, 2);
        assertNumberOfRowGroupStatistics(stripeRowGroupIndexes2, 1);

        // validate row group statistics
        ColumnStatistics actualRowGroupStats1 = getColumnRowGroupStats(stripeRowGroupIndexes1, 0); // stripe 1, row group 1
        ColumnStatistics actualRowGroupStats2 = getColumnRowGroupStats(stripeRowGroupIndexes1, 1); // stripe 1, row group 2
        ColumnStatistics actualRowGroupStats3 = getColumnRowGroupStats(stripeRowGroupIndexes2, 0); // stripe 2, row group 1
        assertEquals(actualRowGroupStats1.getRawSize(), expectedRowGroupStatsRawSize1);
        assertEquals(actualRowGroupStats2.getRawSize(), expectedRowGroupStatsRawSize2);
        assertEquals(actualRowGroupStats3.getRawSize(), expectedRowGroupStatsRawSize3);

        assertTrue(introspector.getFileFooter().getRawSize().isPresent());
        assertEquals(allFileStatistics.get(0).getRawSize(), expectedFileStatsRawSize);

        assertCommonRawAndStorageSize(introspector);
    }

    @Test
    public void testMergeColumnStatisticsRawSize()
    {
        ColumnStatistics nullStats = withRawSize(null);
        assertFalse(nullStats.hasRawSize());

        ColumnStatistics nonNullStats = withRawSize(15L);
        assertTrue(nonNullStats.hasRawSize());
        assertEquals(nonNullStats.getRawSize(), 15L);

        assertEquals(withRawSize(15L), withRawSize(15L));
        assertNotEquals(withRawSize(15L), withRawSize(27L));

        assertEquals(withRawSize(15L).hashCode(), withRawSize(15L).hashCode());
        assertNotEquals(withRawSize(15L).hashCode(), withRawSize(27L).hashCode());

        // all non-nulls
        ColumnStatistics mergedStats1 = mergeColumnStatistics(ImmutableList.of(withRawSize(3L), withRawSize(5L)));
        assertTrue(mergedStats1.hasRawSize());
        assertEquals(mergedStats1.getRawSize(), 8L);

        // all nulls
        ColumnStatistics mergedStats2 = mergeColumnStatistics(ImmutableList.of(withRawSize(null), withRawSize(null)));
        assertFalse(mergedStats2.hasRawSize());

        // some nulls
        ColumnStatistics mergedStats3 = mergeColumnStatistics(ImmutableList.of(withRawSize(null), withRawSize(5L)));
        assertTrue(mergedStats3.hasRawSize());
        assertEquals(mergedStats3.getRawSize(), 5L);
    }

    @Test
    public void testMergeColumnStatisticsStorageSize()
    {
        ColumnStatistics nullStats = withStorageSize(null);
        assertFalse(nullStats.hasStorageSize());

        ColumnStatistics nonNullStats = withStorageSize(15L);
        assertTrue(nonNullStats.hasStorageSize());
        assertEquals(nonNullStats.getStorageSize(), 15L);

        assertEquals(withStorageSize(15L), withStorageSize(15L));
        assertNotEquals(withStorageSize(15L), withStorageSize(27L));

        assertEquals(withStorageSize(15L).hashCode(), withStorageSize(15L).hashCode());
        assertNotEquals(withStorageSize(15L).hashCode(), withStorageSize(27L).hashCode());

        // all non-nulls
        ColumnStatistics mergedStats1 = mergeColumnStatistics(ImmutableList.of(withStorageSize(3L), withStorageSize(5L)));
        assertTrue(mergedStats1.hasStorageSize());
        assertEquals(mergedStats1.getStorageSize(), 8L);

        // all nulls
        ColumnStatistics mergedStats2 = mergeColumnStatistics(ImmutableList.of(withStorageSize(null), withStorageSize(null)));
        assertFalse(mergedStats2.hasStorageSize());

        // some nulls
        ColumnStatistics mergedStats3 = mergeColumnStatistics(ImmutableList.of(withStorageSize(null), withStorageSize(5L)));
        assertTrue(mergedStats3.hasStorageSize());
        assertEquals(mergedStats3.getStorageSize(), 5L);
    }

    @Test(dataProvider = "mapKeyTypeProvider")
    public void testEmptyMapStatistics(Type mapKeyType)
            throws Exception
    {
        MapType mapType = (MapType) mapType(mapKeyType, BIGINT);

        Page emptyMaps = createMapPage(mapType, Arrays.asList(
                mapOf(),
                mapOf(),
                mapOf(),
                mapOf(),
                mapOf(),
                mapOf()));

        Page emptyMapsWithNulls = createMapPage(mapType, Arrays.asList(
                null,
                mapOf(),
                null,
                null,
                null,
                mapOf()));

        Page emptyMapsWithAllNulls = createMapPage(mapType, Arrays.asList(
                null,
                null,
                null,
                null,
                null,
                null));

        doTestFlatMapStatistics(mapType, emptyMaps,
                new ColumnStatistics(6L, null, 0L, 0L),
                new ColumnStatistics(2L, null, 0L, 0L),
                new ColumnStatistics(2L, null, 0L, 0L),
                new ColumnStatistics(2L, null, 0L, 0L));

        doTestFlatMapStatistics(mapType, emptyMapsWithNulls,
                new ColumnStatistics(2L, null, 4L, 0L),
                new ColumnStatistics(1L, null, 1L, 0L),
                new ColumnStatistics(0L, null, 2L, 0L),
                new ColumnStatistics(1L, null, 1L, 0L));

        doTestFlatMapStatistics(mapType, emptyMapsWithAllNulls,
                new ColumnStatistics(0L, null, 6L, 0L),
                new ColumnStatistics(0L, null, 2L, 0L),
                new ColumnStatistics(0L, null, 2L, 0L),
                new ColumnStatistics(0L, null, 2L, 0L));
    }

    @Test(dataProvider = "mapKeyTypeProvider")
    public void testMapStatistics(Type mapKeyType)
            throws Exception
    {
        MapType mapType = (MapType) mapType(mapKeyType, BIGINT);
        Object[] keys = mapKeyType == BIGINT ? new Object[] {0L, 1L, 2L, 3L, 4L, 5L} : new Object[] {"k0", "k1", "k2", "k3", "k4", "k5"};
        int entryCount = 12;
        int keyRawSize = mapKeyType == BIGINT ? Long.BYTES : 2;
        int keysRawSize = entryCount * keyRawSize;
        int valuesRawSize = 96;
        Object everyRowKey = keys[0];  // [1, 2, 3, 4, 5, 6]
        Object firstRowGroupKey = keys[1]; // [11]
        Object secondRowGroupKey = keys[2]; // [22]
        Object thirdRowGroupKey = keys[3]; // [33]
        Object firstStripeKey = keys[4]; // [100]
        Object secondStripeKey = keys[5]; // [1000]

        Page maps = createMapPage(mapType, Arrays.asList(
                // stripe 1, row group 1
                mapOf(everyRowKey, 1L, firstRowGroupKey, 11L),
                mapOf(everyRowKey, 2L, firstStripeKey, 100L),

                // stripe 1, row group 2
                mapOf(everyRowKey, 3L, firstStripeKey, 100L),
                mapOf(everyRowKey, 4L, secondRowGroupKey, 22L),

                // stripe 2, row group 1
                mapOf(everyRowKey, 5L, thirdRowGroupKey, 33L, secondStripeKey, 1000L),
                mapOf(everyRowKey, 6L)));

        MapColumnStatisticsBuilder fileStatistics = new MapColumnStatisticsBuilder(true);
        fileStatistics.increaseValueCount(6);
        fileStatistics.incrementRawSize(keysRawSize + valuesRawSize);
        addIntStats(fileStatistics, everyRowKey, 6L, 1L, 6L, 21L, 48L, 0L);
        addIntStats(fileStatistics, firstRowGroupKey, 1L, 11L, 11L, 11L, 8L, 0L);
        addIntStats(fileStatistics, firstStripeKey, 2L, 100L, 100L, 200L, 16L, 0L);
        addIntStats(fileStatistics, secondRowGroupKey, 1L, 22L, 22L, 22L, 8L, 0L);
        addIntStats(fileStatistics, thirdRowGroupKey, 1L, 33L, 33L, 33L, 8L, 0L);
        addIntStats(fileStatistics, secondStripeKey, 1L, 1000L, 1000L, 1000L, 8L, 0L);

        MapColumnStatisticsBuilder rowGroupStats1 = new MapColumnStatisticsBuilder(true);
        rowGroupStats1.increaseValueCount(2);
        rowGroupStats1.incrementRawSize(32 + 4 * keyRawSize);
        addIntStats(rowGroupStats1, everyRowKey, 2L, 1L, 2L, 3L, 16L, 0L);
        addIntStats(rowGroupStats1, firstRowGroupKey, 1L, 11L, 11L, 11L, 8L, 0L);
        addIntStats(rowGroupStats1, firstStripeKey, 1L, 100L, 100L, 100L, 8L, 0L);

        // keys from rowGroup1 are carried over as generic ColumnStatistics with 0 number of values
        MapColumnStatisticsBuilder rowGroupStats2 = new MapColumnStatisticsBuilder(true);
        rowGroupStats2.increaseValueCount(2);
        rowGroupStats2.incrementRawSize(32 + 4 * keyRawSize);
        addIntStats(rowGroupStats2, everyRowKey, 2L, 3L, 4L, 7L, 16L, 0L);
        rowGroupStats2.addMapStatistics(keyInfo(firstRowGroupKey), new ColumnStatistics(0L, null, 0L, 0L));
        addIntStats(rowGroupStats2, firstStripeKey, 1L, 100L, 100L, 100L, 8L, 0L);
        addIntStats(rowGroupStats2, secondRowGroupKey, 1L, 22L, 22L, 22L, 8L, 0L);

        MapColumnStatisticsBuilder rowGroupStats3 = new MapColumnStatisticsBuilder(true);
        rowGroupStats3.increaseValueCount(2);
        rowGroupStats3.incrementRawSize(32 + 4 * keyRawSize);
        addIntStats(rowGroupStats3, everyRowKey, 2L, 5L, 6L, 11L, 16L, 0L);
        addIntStats(rowGroupStats3, thirdRowGroupKey, 1L, 33L, 33L, 33L, 8L, 0L);
        addIntStats(rowGroupStats3, secondStripeKey, 1L, 1000L, 1000L, 1000L, 8L, 0L);

        doTestFlatMapStatistics(mapType, maps,
                fileStatistics.buildColumnStatistics(),
                rowGroupStats1.buildColumnStatistics(),
                rowGroupStats2.buildColumnStatistics(),
                rowGroupStats3.buildColumnStatistics());
    }

    @Test(dataProvider = "mapKeyTypeProvider")
    public void testMapStatisticsWithNulls(Type mapKeyType)
            throws Exception
    {
        MapType mapType = (MapType) mapType(mapKeyType, BIGINT);
        Object key = mapKeyType == BIGINT ? 0L : "k0";
        int entryCount = 4;
        int keyRawSize = mapKeyType == BIGINT ? Long.BYTES : 2;
        int keysRawSize = entryCount * keyRawSize;

        Page maps = createMapPage(mapType, Arrays.asList(
                // stripe 1, row group 1
                mapOf(key, 1L),
                mapOf(),

                // stripe 1, row group 2
                null,
                mapOf(key, null),

                // stripe 2, row group 1
                mapOf(key, null),
                mapOf(key, 2L)));

        MapColumnStatisticsBuilder fileStatistics = new MapColumnStatisticsBuilder(true);
        fileStatistics.increaseValueCount(5);
        fileStatistics.incrementRawSize(19 + keysRawSize);
        addIntStats(fileStatistics, key, 2L, 1L, 2L, 3L, 18L, 0L);

        MapColumnStatisticsBuilder rowGroupStats1 = new MapColumnStatisticsBuilder(true);
        rowGroupStats1.increaseValueCount(2);
        rowGroupStats1.incrementRawSize(8 + keyRawSize);
        addIntStats(rowGroupStats1, key, 1L, 1L, 1L, 1L, 8L, 0L);

        MapColumnStatisticsBuilder rowGroupStats2 = new MapColumnStatisticsBuilder(true);
        rowGroupStats2.increaseValueCount(1);
        rowGroupStats2.incrementRawSize(2 + keyRawSize);
        rowGroupStats2.addMapStatistics(keyInfo(key), new ColumnStatistics(0L, null, 1L, 0L));

        MapColumnStatisticsBuilder rowGroupStats3 = new MapColumnStatisticsBuilder(true);
        rowGroupStats3.increaseValueCount(2);
        rowGroupStats3.incrementRawSize(9 + 2 * keyRawSize);
        addIntStats(rowGroupStats3, key, 1L, 2L, 2L, 2L, 9L, 0L);

        doTestFlatMapStatistics(mapType, maps,
                fileStatistics.buildColumnStatistics(),
                rowGroupStats1.buildColumnStatistics(),
                rowGroupStats2.buildColumnStatistics(),
                rowGroupStats3.buildColumnStatistics());
    }

    // test a case when first row group has values, but second row group is all nulls,
    // thus forcing the keys from first row group to be carried over to the second row group
    // and use 0 value count
    @Test(dataProvider = "mapKeyTypeProvider")
    public void testMapStatisticsWithNullsFullRowGroup(Type mapKeyType)
            throws Exception
    {
        MapType mapType = (MapType) mapType(mapKeyType, BIGINT);
        Object key = mapKeyType == BIGINT ? 0L : "k0";
        int entryCount = 3;
        int keyRawSize = mapKeyType == BIGINT ? Long.BYTES : 2;
        int keysRawSize = entryCount * keyRawSize;

        Page maps = createMapPage(mapType, Arrays.asList(
                // stripe 1, row group 1
                mapOf(key, 1L),
                mapOf(),

                // stripe 1, row group 2 - make all values null to have make
                // nonNullRowGroupValueCount=0 in MapFlatColumnWriter
                null,
                null,

                // stripe 2, row group 1
                mapOf(key, null),
                mapOf(key, 2L)));

        MapColumnStatisticsBuilder fileStatistics = new MapColumnStatisticsBuilder(true);
        fileStatistics.increaseValueCount(4);
        fileStatistics.incrementRawSize(19 + keysRawSize);
        addIntStats(fileStatistics, key, 2L, 1L, 2L, 3L, 17L, 0L);

        MapColumnStatisticsBuilder rowGroupStats1 = new MapColumnStatisticsBuilder(true);
        rowGroupStats1.increaseValueCount(2);
        rowGroupStats1.incrementRawSize(8 + keyRawSize);
        addIntStats(rowGroupStats1, key, 1L, 1L, 1L, 1L, 8L, 0L);

        MapColumnStatisticsBuilder rowGroupStats2 = new MapColumnStatisticsBuilder(true);
        rowGroupStats2.increaseValueCount(0);
        rowGroupStats2.incrementRawSize(2L);
        rowGroupStats2.addMapStatistics(keyInfo(key), new ColumnStatistics(0L, null, 0L, 0L));

        MapColumnStatisticsBuilder rowGroupStats3 = new MapColumnStatisticsBuilder(true);
        rowGroupStats3.increaseValueCount(2);
        rowGroupStats3.incrementRawSize(9L + 2 * keyRawSize);
        addIntStats(rowGroupStats3, key, 1L, 2L, 2L, 2L, 9L, 0L);

        doTestFlatMapStatistics(mapType, maps,
                fileStatistics.buildColumnStatistics(),
                rowGroupStats1.buildColumnStatistics(),
                rowGroupStats2.buildColumnStatistics(),
                rowGroupStats3.buildColumnStatistics());
    }

    private void doTestFlatMapStatistics(
            Type type,
            Page page,
            ColumnStatistics expectedFileStats,
            ColumnStatistics expectedRowGroupStats1,
            ColumnStatistics expectedRowGroupStats2,
            ColumnStatistics expectedRowGroupStats3)
            throws Exception
    {
        // this test case is geared toward pages with 6 rows to create two stripes with 4 and 2 rows each
        // row group size is 2 rows (MAP_STATS_TEST_ROW_GROUP_MAX_ROW_COUNT)
        assertEquals(page.getPositionCount(), 6);

        CapturingOrcFileIntrospector introspector;

        try (TempFile tempFile = new TempFile()) {
            writeFileForMapStatistics(type, page, tempFile);
            introspector = introspectOrcFile(type, tempFile);
        }

        // validate file statistics
        List<ColumnStatistics> allFileStatistics = introspector.getFileFooter().getFileStats();
        ColumnStatistics actualFileStatistics = allFileStatistics.get(COLUMN);
        assertColumnStats(actualFileStatistics, expectedFileStats);

        // prepare to validate row group statistics
        assertEquals(introspector.getStripes().size(), 2);
        List<StripeInformation> stripeInformations = introspector.getStripeInformations();
        assertEquals(stripeInformations.size(), 2);
        assertEquals(introspector.getRowGroupIndexesByStripeOffset().size(), 2);
        Stripe stripe1 = introspector.getStripes().get(0);
        Stripe stripe2 = introspector.getStripes().get(1);
        assertEquals(stripe1.getRowCount(), 4);
        assertEquals(stripe2.getRowCount(), 2);

        long stripeOffset1 = stripeInformations.get(0).getOffset();
        long stripeOffset2 = stripeInformations.get(1).getOffset();

        Map<StreamId, List<RowGroupIndex>> stripeRowGroupIndexes1 = introspector.getRowGroupIndexesByStripeOffset().get(stripeOffset1);
        Map<StreamId, List<RowGroupIndex>> stripeRowGroupIndexes2 = introspector.getRowGroupIndexesByStripeOffset().get(stripeOffset2);
        assertNumberOfRowGroupStatistics(stripeRowGroupIndexes1, 2);
        assertNumberOfRowGroupStatistics(stripeRowGroupIndexes2, 1);

        // validate row group statistics
        ColumnStatistics actualRowGroupStats1 = getColumnRowGroupStats(stripeRowGroupIndexes1, 0); // stripe 1, row group 1
        ColumnStatistics actualRowGroupStats2 = getColumnRowGroupStats(stripeRowGroupIndexes1, 1); // stripe 1, row group 2
        ColumnStatistics actualRowGroupStats3 = getColumnRowGroupStats(stripeRowGroupIndexes2, 0); // stripe 2, row group 1
        assertColumnStats(actualRowGroupStats1, expectedRowGroupStats1);
        assertColumnStats(actualRowGroupStats2, expectedRowGroupStats2);
        assertColumnStats(actualRowGroupStats3, expectedRowGroupStats3);

        assertEquals(allFileStatistics.get(0).getRawSize(), introspector.getFileFooter().getRawSize().getAsLong());

        assertCommonRawAndStorageSize(introspector);
    }

    // compares all fields excluding the storage size, because storage size
    // is unpredictable for the testing purpose
    private void assertColumnStats(ColumnStatistics actual, ColumnStatistics expected)
    {
        assertEquals(actual.hasNumberOfValues(), expected.hasNumberOfValues());
        assertEquals(actual.getNumberOfValues(), expected.getNumberOfValues());
        assertEquals(actual.hasRawSize(), expected.hasRawSize());
        assertEquals(actual.getRawSize(), expected.getRawSize());
        assertEquals(actual.getBloomFilter(), expected.getBloomFilter());

        assertEquals(actual.getBooleanStatistics(), expected.getBooleanStatistics());
        assertEquals(actual.getIntegerStatistics(), expected.getIntegerStatistics());
        assertEquals(actual.getDoubleStatistics(), expected.getDoubleStatistics());
        assertEquals(actual.getStringStatistics(), expected.getStringStatistics());
        assertEquals(actual.getDateStatistics(), expected.getDateStatistics());
        assertEquals(actual.getDecimalStatistics(), expected.getDecimalStatistics());
        assertEquals(actual.getBinaryStatistics(), expected.getBinaryStatistics());

        MapStatistics actualMapStatistics = actual.getMapStatistics();
        MapStatistics expectedMapStatistics = expected.getMapStatistics();
        assertEquals(actualMapStatistics != null, expectedMapStatistics != null);
        if (actualMapStatistics != null) {
            List<MapStatisticsEntry> actualEntries = actualMapStatistics.getEntries();
            List<MapStatisticsEntry> expectedEntries = expectedMapStatistics.getEntries();
            assertEquals(actualEntries.size(), expectedEntries.size());

            ImmutableMap.Builder<KeyInfo, ColumnStatistics> actualEntriesByKeyBuilder = ImmutableMap.builder();
            ImmutableMap.Builder<KeyInfo, ColumnStatistics> expectedEntriesByKeyBuilder = ImmutableMap.builder();
            for (int i = 0; i < actualEntries.size(); i++) {
                MapStatisticsEntry actualEntry = actualEntries.get(i);
                MapStatisticsEntry expectedEntry = expectedEntries.get(i);
                actualEntriesByKeyBuilder.put(actualEntry.getKey(), actualEntry.getColumnStatistics());
                expectedEntriesByKeyBuilder.put(expectedEntry.getKey(), expectedEntry.getColumnStatistics());
            }

            Map<KeyInfo, ColumnStatistics> actualEntriesByKey = actualEntriesByKeyBuilder.build();
            Map<KeyInfo, ColumnStatistics> expectedEntriesByKey = expectedEntriesByKeyBuilder.build();
            assertEquals(actualEntriesByKey.keySet(), expectedEntriesByKey.keySet());
            actualEntriesByKey.forEach((key, value) -> assertColumnStats(value, expectedEntriesByKey.get(key)));
        }
    }

    // make sure the number of row group stats is as expected
    private void assertNumberOfRowGroupStatistics(Map<StreamId, List<RowGroupIndex>> rowGroupIndexes, int size)
    {
        for (Map.Entry<StreamId, List<RowGroupIndex>> e : rowGroupIndexes.entrySet()) {
            assertEquals(e.getValue().size(), size);
        }
    }

    // extract column statistics for a specific column and row group
    private ColumnStatistics getColumnRowGroupStats(Map<StreamId, List<RowGroupIndex>> rowGroupIndexes, int rowGroupIndex)
    {
        ColumnStatistics columnStatistics = null;
        for (Map.Entry<StreamId, List<RowGroupIndex>> e : rowGroupIndexes.entrySet()) {
            if (e.getKey().getColumn() == COLUMN) {
                // keep iterating to make sure there is only one StreamId for this column
                assertNull(columnStatistics);
                columnStatistics = e.getValue().get(rowGroupIndex).getColumnStatistics();
            }
        }

        assertNotNull(columnStatistics);
        return columnStatistics;
    }

    private static Page createMapPage(MapType type, List<Map<Object, Object>> maps)
    {
        Type keyType = type.getKeyType();
        Type valueType = type.getValueType();
        MapBlockBuilder mapBlockBuilder = (MapBlockBuilder) type.createBlockBuilder(null, maps.size());
        BlockBuilder mapKeyBuilder = mapBlockBuilder.getKeyBlockBuilder();
        BlockBuilder mapValueBuilder = mapBlockBuilder.getValueBlockBuilder();

        for (Map<Object, Object> map : maps) {
            if (map == null) {
                mapBlockBuilder.appendNull();
                continue;
            }

            mapBlockBuilder.beginDirectEntry();
            for (Map.Entry<Object, Object> entry : map.entrySet()) {
                writeNativeValue(keyType, mapKeyBuilder, entry.getKey());
                writeNativeValue(valueType, mapValueBuilder, entry.getValue());
            }
            mapBlockBuilder.closeEntry();
        }

        return new Page(mapBlockBuilder.build());
    }

    private void writeFileForMapStatistics(Type type, Page page, TempFile file)
            throws IOException
    {
        DefaultOrcWriterFlushPolicy flushPolicy = DefaultOrcWriterFlushPolicy.builder()
                .withStripeMaxRowCount(STRIPE_MAX_ROW_COUNT)
                .build();

        OrcWriterOptions writerOptions = OrcWriterOptions.builder()
                .withFlushPolicy(flushPolicy)
                .withRowGroupMaxRowCount(ROW_GROUP_MAX_ROW_COUNT)
                .withFlattenedColumns(ImmutableSet.of(0))
                .withMapStatisticsEnabled(true)
                .build();

        try (OrcWriter orcWriter = createOrcWriter(
                file.getFile(),
                DWRF,
                ZLIB,
                Optional.empty(),
                ImmutableList.of(type),
                writerOptions,
                NOOP_WRITER_STATS)) {
            orcWriter.write(page);
        }
    }

    private static OrcWriterOptions createOrcWriterOptions(
            int stripeMaxRowCount,
            int rowGroupMaxRowCount,
            boolean stringDictionaryEncodingEnabled,
            boolean integerDictionaryEncodingEnabled)
    {
        DefaultOrcWriterFlushPolicy flushPolicy = DefaultOrcWriterFlushPolicy.builder()
                .withStripeMaxRowCount(stripeMaxRowCount)
                .build();

        return OrcWriterOptions.builder()
                .withFlushPolicy(flushPolicy)
                .withStringDictionaryEncodingEnabled(stringDictionaryEncodingEnabled)
                .withIntegerDictionaryEncodingEnabled(integerDictionaryEncodingEnabled)
                .withRowGroupMaxRowCount(rowGroupMaxRowCount)
                .build();
    }

    private static CapturingOrcFileIntrospector introspectOrcFile(Type type, TempFile file)
            throws IOException
    {
        OrcDataSource dataSource = new FileOrcDataSource(file.getFile(),
                new DataSize(1, MEGABYTE),
                new DataSize(1, MEGABYTE),
                new DataSize(1, MEGABYTE),
                true);

        OrcReaderOptions readerOptions = OrcReaderOptions.builder()
                .withMaxMergeDistance(new DataSize(1, MEGABYTE))
                .withTinyStripeThreshold(new DataSize(1, MEGABYTE))
                .withMaxBlockSize(new DataSize(1, MEGABYTE))
                .withReadMapStatistics(true)
                .build();

        CapturingOrcFileIntrospector introspector = new CapturingOrcFileIntrospector();

        OrcReader reader = new OrcReader(
                dataSource,
                DWRF,
                new StorageOrcFileTailSource(),
                StripeMetadataSourceFactory.of(new StorageStripeMetadataSource()),
                Optional.empty(),
                NOOP_ORC_AGGREGATED_MEMORY_CONTEXT,
                readerOptions,
                false,
                NO_ENCRYPTION,
                DwrfKeyProvider.EMPTY,
                new RuntimeStats(),
                Optional.of(introspector),
                file.getFile().lastModified());

        OrcSelectiveRecordReader recordReader = reader.createSelectiveRecordReader(
                ImmutableMap.of(0, type),
                ImmutableList.of(0),
                Collections.emptyMap(),
                Collections.emptyList(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                OrcPredicate.TRUE,
                0,
                dataSource.getSize(),
                DateTimeZone.UTC,
                NOOP_ORC_AGGREGATED_MEMORY_CONTEXT,
                Optional.empty(),
                1000);

        while (recordReader.getNextPage() != null) {
            // read all to load all row groups into the introspector
        }

        recordReader.close();
        dataSource.close();

        return introspector;
    }

    // create a KeyInfo object for a given key
    private static KeyInfo keyInfo(Object key)
    {
        if (key instanceof Long) {
            return KeyInfo.newBuilder().setIntKey((Long) key).build();
        }
        else {
            return KeyInfo.newBuilder().setBytesKey(ByteString.copyFromUtf8((String) key)).build();
        }
    }

    private static void addIntStats(MapColumnStatisticsBuilder stats, Object key, long numberOfValues, long min, long max, long sum, long rawSize, long storageSize)
    {
        stats.addMapStatistics(keyInfo(key), new IntegerColumnStatistics(numberOfValues, null, rawSize, storageSize, new IntegerStatistics(min, max, sum)));
    }

    private static Map<Object, Object> mapOf(Object... values)
    {
        LinkedHashMap<Object, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < values.length; i += 2) {
            map.put(values[i], values[i + 1]);
        }
        return map;
    }

    private static ColumnStatistics withRawSize(Long rawSize)
    {
        return new ColumnStatistics(null, null, rawSize, null);
    }

    private static ColumnStatistics withStorageSize(Long storageSize)
    {
        return new ColumnStatistics(null, null, null, storageSize);
    }

    /**
     * Split the block into blockSize regions with a single position.
     * Used to hit all dictionary fallback to direct cases.
     */
    private static List<Block> partitionBlock(Block block)
    {
        ImmutableList.Builder<Block> blocks = ImmutableList.builder();
        for (int i = 0; i < block.getPositionCount(); i++) {
            blocks.add(block.getRegion(i, 1));
        }
        return blocks.build();
    }

    private static void assertCommonRawAndStorageSize(CapturingOrcFileIntrospector introspector)
    {
        Footer fileFooter = introspector.getFileFooter();
        List<OrcType> orcTypes = fileFooter.getTypes();
        List<ColumnStatistics> fileStats = fileFooter.getFileStats();
        // node storage sizes are populated from stripe streams
        long[] nodeSizes = new long[orcTypes.size()];

        // node raw sizes are populated from row group indexes
        long[] nodeRawSizes = new long[orcTypes.size()];

        long totalRawSize = 0;
        List<StripeInformation> stripeInformations = introspector.getStripeInformations();
        List<Stripe> stripes = introspector.getStripes();
        assertEquals(stripeInformations.size(), stripes.size());

        // skip raw size + row group index checks for nodes that do not have
        // index streams: such as root and flat map key nodes
        Set<Integer> skipRawSizeNodes = new HashSet<>();
        skipRawSizeNodes.add(0);

        Set<Integer> flatMapNodes = new HashSet<>();
        Int2ObjectMap<Object2LongMap<KeyInfo>> flatMapKeySizes = new Int2ObjectOpenHashMap<>();
        Int2ObjectMap<Object2LongMap<KeyInfo>> flatMapKeyRawSizes = new Int2ObjectOpenHashMap<>();

        for (int i = 0; i < stripeInformations.size(); i++) {
            StripeInformation stripeInformation = stripeInformations.get(i);
            assertTrue(stripeInformation.getRawDataSize().isPresent());
            totalRawSize += stripeInformation.getRawDataSize().getAsLong();

            // collect raw sizes from row group indexes, later we'll compare them to file column statistics
            long stripeOffset = stripeInformation.getOffset();
            Map<StreamId, List<RowGroupIndex>> rowGroupIndexes = introspector.getRowGroupIndexesByStripeOffset().get(stripeOffset);
            for (Map.Entry<StreamId, List<RowGroupIndex>> entry : rowGroupIndexes.entrySet()) {
                StreamId streamId = entry.getKey();
                int node = streamId.getColumn();
                List<RowGroupIndex> streamIndexes = entry.getValue();

                for (RowGroupIndex rowGroupIndex : streamIndexes) {
                    ColumnStatistics columnStatistics = rowGroupIndex.getColumnStatistics();
                    nodeRawSizes[node] += columnStatistics.getRawSize();

                    // collect flat map key raw sizes
                    MapStatistics mapStatistics = columnStatistics.getMapStatistics();
                    if (mapStatistics != null) {
                        Object2LongMap<KeyInfo> keyRawSizes = flatMapKeyRawSizes.computeIfAbsent(node, (ignore) -> new Object2LongOpenHashMap<>());
                        for (MapStatisticsEntry mapStatisticsEntry : mapStatistics.getEntries()) {
                            keyRawSizes.mergeLong(mapStatisticsEntry.getKey(), mapStatisticsEntry.getColumnStatistics().getRawSize(), Long::sum);
                        }
                    }
                }
            }

            Stripe stripe = stripes.get(i);

            // collect flat map nodes
            Map<Integer, ColumnEncoding> columnEncodings = stripe.getColumnEncodings();
            for (Map.Entry<Integer, ColumnEncoding> entry : columnEncodings.entrySet()) {
                if (entry.getValue().getColumnEncodingKind() == ColumnEncoding.ColumnEncodingKind.DWRF_MAP_FLAT) {
                    Integer mapNode = entry.getKey();
                    flatMapNodes.add(mapNode);
                    int flatMapKeyNode = orcTypes.get(mapNode).getFieldTypeIndex(0);

                    // don't validate flat map key node raw size against the row group
                    // indexes because there are no checkpoints for flat
                    skipRawSizeNodes.add(flatMapKeyNode);
                }
            }

            // map all flat map values nodes to their top-level map node
            Map<Integer, Integer> flatMapNodesToColumnNode = mapFlatMapNodesToColumnNode(flatMapNodes, orcTypes);

            StripeFooter stripeFooter = introspector.getStripeFooterByStripeOffset().get(stripeOffset);
            assertNotNull(stripeFooter);

            Object2LongMap<NodeSequenceKey> stripeNodeSequenceToSize = collectStripeNodeSequenceSizes(stripeFooter, nodeSizes, flatMapNodesToColumnNode);

            // aggregate the stripe level flat map sub-nodes stream sizes into the file level sizes
            for (Integer flatMapNode : flatMapNodes) {
                int flatMapValueNode = getMapValueNode(orcTypes, flatMapNode);
                ColumnEncoding flatMapColumnEncoding = columnEncodings.get(flatMapValueNode);

                // encoding will be absent if there are no streams for a certain node (e.g. no values written)
                if (flatMapColumnEncoding == null) {
                    continue;
                }

                // map statistics must be explicitly enabled through the writer configuration
                // additionalSequence will be absent if map statistics are not enabled
                if (flatMapColumnEncoding.getAdditionalSequenceEncodings().isPresent()) {
                    Object2LongMap<KeyInfo> fileLevelKeySizes = flatMapKeySizes.computeIfAbsent(flatMapNode.intValue(), (ignore) -> new Object2LongOpenHashMap<>());
                    Map<Integer, DwrfSequenceEncoding> stripeSequenceToKey = flatMapColumnEncoding.getAdditionalSequenceEncodings().get();
                    for (Map.Entry<Integer, DwrfSequenceEncoding> entry : stripeSequenceToKey.entrySet()) {
                        Integer sequence = entry.getKey();
                        KeyInfo flatMapKey = entry.getValue().getKey();
                        long keySize = stripeNodeSequenceToSize.getLong(new NodeSequenceKey(flatMapNode, sequence));
                        fileLevelKeySizes.mergeLong(flatMapKey, keySize, Long::sum);
                    }
                }
            }
        }

        for (Integer flatMapNode : flatMapNodes) {
            Object2LongMap<KeyInfo> keySizes = flatMapKeySizes.get(flatMapNode);
            Object2LongMap<KeyInfo> keyRawSizes = flatMapKeyRawSizes.get(flatMapNode);
            assertEquals(keySizes != null, keyRawSizes != null);

            if (keySizes != null) {
                ColumnStatistics columnStatistics = fileStats.get(flatMapNode);
                MapStatistics mapStatistics = columnStatistics.getMapStatistics();
                assertEquals(keySizes.size(), mapStatistics.getEntries().size());

                for (MapStatisticsEntry entry : mapStatistics.getEntries()) {
                    ColumnStatistics keyColumnStatistics = entry.getColumnStatistics();
                    KeyInfo flatMapKey = entry.getKey();

                    // Key size is the total storage size of flat map value nodes
                    // computed from flat map value streams.
                    // Let's compare it to what's stored in the file column statistics.
                    long keySize = keySizes.getLong(flatMapKey);
                    assertEquals(keyColumnStatistics.getStorageSize(), keySize);

                    // keyRawSize is a total of all raw sizes from row group index
                    long keyRawSize = keyRawSizes.getLong(flatMapKey);
                    assertEquals(keyColumnStatistics.getRawSize(), keyRawSize);
                }
            }
        }

        assertTrue(fileFooter.getRawSize().isPresent());
        assertEquals(totalRawSize, fileStats.get(0).getRawSize());
        assertEquals(totalRawSize, fileFooter.getRawSize().getAsLong());
        assertNodeRawAndStorageSize(orcTypes, nodeSizes, fileStats);

        // Compare raw sizes from the row group indexes and file column statistics.
        // Raw sizes in the row group indexes are already rolled up, so no need to
        // traverse the tree and collect raw sizes for sub-nodes.
        for (int i = 0; i < fileStats.size(); i++) {
            if (skipRawSizeNodes.contains(i)) {
                continue;
            }

            ColumnStatistics columnStatistics = fileStats.get(i);
            long rawSizeFromStats = zeroIfNull(columnStatistics.getRawSize());
            assertEquals(rawSizeFromStats, nodeRawSizes[i], "mismatched raw size for node " + i);
        }
    }

    private static Object2LongMap<NodeSequenceKey> collectStripeNodeSequenceSizes(StripeFooter stripeFooter, long[] nodeSizes, Map<Integer, Integer> flatMapNodesToColumnNode)
    {
        Object2LongMap<NodeSequenceKey> stripeNodeSequenceToSize = new Object2LongOpenHashMap<>();
        for (Stream stream : stripeFooter.getStreams()) {
            int node = stream.getColumn();
            nodeSizes[node] += stream.getLength();

            // aggregate stream sizes by flat map key
            Integer flatMapNode = flatMapNodesToColumnNode.get(node);
            if (flatMapNode != null) {
                stripeNodeSequenceToSize.mergeLong(new NodeSequenceKey(flatMapNode, stream.getSequence()), stream.getLength(), Long::sum);
            }
        }
        return stripeNodeSequenceToSize;
    }

    private static int getMapValueNode(List<OrcType> orcTypes, int mapNode)
    {
        OrcType mapType = orcTypes.get(mapNode);
        assertNotNull(mapType);
        assertEquals(mapType.getOrcTypeKind(), OrcType.OrcTypeKind.MAP);
        return mapType.getFieldTypeIndex(1);
    }

    private static Map<Integer, Integer> mapFlatMapNodesToColumnNode(Set<Integer> flatMapNodes, List<OrcType> orcTypes)
    {
        Map<Integer, Integer> flattenedNodeTrees = new HashMap<>();
        for (Integer mapNode : flatMapNodes) {
            OrcType mapType = orcTypes.get(mapNode);
            checkArgument(mapType.getOrcTypeKind() == OrcType.OrcTypeKind.MAP, "flat map node %s must be a map, but was %s", mapNode, mapType.getOrcTypeKind());
            checkArgument(mapType.getFieldCount() == 2, "flat map node %s must have exactly 2 sub-fields but had %s", mapNode, mapType.getFieldCount());
            int mapValueNode = mapType.getFieldTypeIndex(1);
            List<Integer> deepValueNodes = collectDeepTreeNodes(orcTypes, mapValueNode);
            for (Integer valueNode : deepValueNodes) {
                flattenedNodeTrees.put(valueNode, mapNode);
            }
        }
        return flattenedNodeTrees;
    }

    private static void assertNodeRawAndStorageSize(List<OrcType> orcTypes, long[] nodeSizes, List<ColumnStatistics> fileColumnStatistics)
    {
        getAndAssertNodeStorageSize(0, orcTypes, nodeSizes, fileColumnStatistics);
    }

    private static long getAndAssertNodeStorageSize(int nodeId, List<OrcType> orcTypes, long[] nodeSizes, List<ColumnStatistics> fileColumnStatistics)
    {
        OrcType node = orcTypes.get(nodeId);
        long actualStorageSize = nodeSizes[nodeId];

        for (int i = 0; i < node.getFieldCount(); i++) {
            int subNodeId = node.getFieldTypeIndex(i);
            actualStorageSize += getAndAssertNodeStorageSize(subNodeId, orcTypes, nodeSizes, fileColumnStatistics);
        }

        ColumnStatistics columnStatistics = fileColumnStatistics.get(nodeId);
        assertNotNull(columnStatistics);

        long statisticsStorageSize = zeroIfNull(columnStatistics.getStorageSize());
        assertEquals(actualStorageSize, statisticsStorageSize, "Storage sizes in streams and file column statistics are different for node " + nodeId);

        return actualStorageSize;
    }

    private static List<Integer> collectDeepTreeNodes(List<OrcType> orcTypes, int startNode)
    {
        LinkedList<Integer> toVisit = new LinkedList<>();
        List<Integer> result = new ArrayList<>();

        toVisit.add(startNode);
        while (!toVisit.isEmpty()) {
            Integer node = toVisit.pop();
            result.add(node);
            OrcType orcType = orcTypes.get(node);
            for (int i = 0; i < orcType.getFieldCount(); i++) {
                toVisit.push(orcType.getFieldTypeIndex(i));
            }
        }

        return result;
    }

    private static long zeroIfNull(Long value)
    {
        return value == null ? 0 : value;
    }

    private static class NodeSequenceKey
    {
        private final int node;
        private final int sequence;

        public NodeSequenceKey(int node, int sequence)
        {
            this.node = node;
            this.sequence = sequence;
        }

        @Override
        public boolean equals(Object o)
        {
            NodeSequenceKey that = (NodeSequenceKey) o;
            return node == that.node && sequence == that.sequence;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(node, sequence);
        }
    }
}

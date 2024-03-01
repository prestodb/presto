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
package com.facebook.presto.operator.aggregation.approxmostfrequent;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.aggregation.approxmostfrequent.stream.StreamSummary;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createSlicesBlock;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.util.StructuralTestUtil.mapType;
import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;

public class TestStreamSummary
{
    private static final Type INT_SERIALIZED_TYPE = RowType.withDefaultFieldNames(ImmutableList.of(BIGINT, BIGINT, new ArrayType(BIGINT), new ArrayType(BIGINT)));
    private static final Type VARCHAR_SERIALIZED_TYPE = RowType.withDefaultFieldNames(ImmutableList.of(BIGINT, BIGINT, new ArrayType(VARCHAR), new ArrayType(BIGINT)));

    private static final int TEST_COMPACT_THRESHOLD_BYTES = 10;
    private static final int TEST_COMPACT_THRESHOLD_RATIO = 2;

    @DataProvider(name = "streamLongDataProvider")
    private Object[][] streamLongDataProvider()
    {
        return new Object[][] {
                {new Long[] {1L, 1L, 2L, 3L, 4L}, 3, 15, ImmutableMap.of(1L, 2L, 2L, 1L, 3L, 1L)},
                {new Long[] {14L, 14L, 11L, 13L, 13L, 13L, 14L, 14L}, 2, 4, ImmutableMap.of(14L, 4L, 13L, 3L)},
                {new Long[] {80L, 54L, 32L, 4L, 67L, 68L, 68L, 15L, 38L, 30L}, 2, 4, ImmutableMap.of(68L, 3L, 30L, 3L)},
                {new Long[] {29L, 51L, 84L, 60L, 32L, 54L, 82L, 30L, 53L, 57L}, 2, 3, ImmutableMap.of(57L, 4L, 30L, 3L)},
        };
    }

    @DataProvider(name = "streamSliceDataProvider")
    private Object[][] streamStringDataProvider()
    {
        return new Object[][] {
                {new Slice[] {utf8Slice("A"), utf8Slice("A"), utf8Slice("B"), utf8Slice("C"), utf8Slice("D")}, 3, 15,
                        ImmutableMap.of(utf8Slice("A"), 2L, utf8Slice("B"), 1L, utf8Slice("C"), 1L)},
                {new Slice[] {utf8Slice("1"), utf8Slice("1"), utf8Slice("2"), utf8Slice("3"), utf8Slice("4")}, 3, 15,
                        ImmutableMap.of(utf8Slice("1"), 2L, utf8Slice("2"), 1L, utf8Slice("3"), 1L)},
                {new Slice[] {utf8Slice("14"), utf8Slice("14"), utf8Slice("11"), utf8Slice("13"), utf8Slice("13"), utf8Slice("13"), utf8Slice("14"), utf8Slice("14")}, 2, 4,
                        ImmutableMap.of(utf8Slice("14"), 4L, utf8Slice("13"), 3L)},
                {new Slice[] {utf8Slice("80"), utf8Slice("54"), utf8Slice("32"), utf8Slice("4"), utf8Slice("67"), utf8Slice("68"), utf8Slice("68"), utf8Slice("15"),
                        utf8Slice("38"), utf8Slice("30")}, 2, 4, ImmutableMap.of(utf8Slice("68"), 3L, utf8Slice("30"), 3L)},
        };
    }

    @Test(dataProvider = "streamLongDataProvider")
    public void testLongHistogram(Long[] dataStream, int maxBuckets, int capacity, Map<Long, Long> expectedSummary)
    {
        Block longsBlock = createLongsBlock(dataStream);
        StreamSummary streamSummary = buildStreamSummary(BIGINT, maxBuckets, capacity);
        for (int pos = 0; pos < dataStream.length; pos++) {
            streamSummary.add(longsBlock, pos, 1);
        }
        Map<Long, Long> buckets = getMapForLongType(streamSummary);
        assertEquals(buckets.size(), expectedSummary.size());
        assertEquals(buckets, expectedSummary);
        //test deserialization
        BlockBuilder blockBuilder = INT_SERIALIZED_TYPE.createBlockBuilder(null, 10);
        streamSummary.serialize(blockBuilder);

        StreamSummary deserialize = StreamSummary.deserialize(BIGINT, (Block) INT_SERIALIZED_TYPE.getObject(blockBuilder, 0));
        Map<Long, Long> deserializedMap = getMapForLongType(deserialize);
        assertEquals(buckets, deserializedMap);
    }

    @Test(dataProvider = "streamSliceDataProvider")
    public void testStringHistogram(Slice[] dataStream, int maxBuckets, int capacity, Map<Slice, Long> expectedSummary)
    {
        Block slicesBlock = createSlicesBlock(dataStream);
        StreamSummary streamSummary = buildStreamSummary(VARCHAR, maxBuckets, capacity);
        for (int pos = 0; pos < dataStream.length; pos++) {
            streamSummary.add(slicesBlock, pos, 1);
        }
        Map<Slice, Long> buckets = getMapFromStreamSummaryOfSlices(streamSummary);
        assertEquals(buckets.size(), expectedSummary.size());
        assertEquals(buckets, expectedSummary);

        //test deserialization
        BlockBuilder blockBuilder = VARCHAR_SERIALIZED_TYPE.createBlockBuilder(null, 10);
        streamSummary.serialize(blockBuilder);

        StreamSummary deserialize = StreamSummary.deserialize(VARCHAR, (Block) VARCHAR_SERIALIZED_TYPE.getObject(blockBuilder, 0));
        Map<Slice, Long> deserializedMap = getMapFromStreamSummaryOfSlices(deserialize);
        assertEquals(buckets, deserializedMap);
    }

    @Test
    public void testMerge()
    {
        StreamSummary histogram1 = new StreamSummary(BIGINT, 3, 15);
        Long[] values = {1L, 1L, 2L};
        int pos = 0;
        Block longsBlock = createLongsBlock(values);
        for (int i = 0; i < values.length; i++) {
            histogram1.add(longsBlock, pos++, 1);
        }

        StreamSummary histogram2 = new StreamSummary(BIGINT, 3, 15);
        values = new Long[] {3L, 4L};
        pos = 0;
        longsBlock = createLongsBlock(values);
        for (int i = 0; i < values.length; i++) {
            histogram2.add(longsBlock, pos++, 1);
        }

        histogram1.merge(histogram2);
        Map<Long, Long> buckets = getMapForLongType(histogram1);

        assertEquals(buckets.size(), 3);
        assertEquals(buckets, ImmutableMap.of(1L, 2L, 2L, 1L, 3L, 1L));
    }

    private StreamSummary buildStreamSummary(Type type, int maxBuckets, int hashCapacity)
    {
        StreamSummary histogram = new StreamSummary(type, maxBuckets, hashCapacity)
        {
            //override compaction to trigger it early for tests so that the test also covers compaction and rehashing
            @Override
            protected boolean shouldCompact(long sizeInBytes, int numberOfPositionInBlock)
            {
                return sizeInBytes >= TEST_COMPACT_THRESHOLD_BYTES && numberOfPositionInBlock / getHeapSize() >= TEST_COMPACT_THRESHOLD_RATIO;
            }
        };
        return histogram;
    }

    private Map<Long, Long> getMapForLongType(StreamSummary histogram)
    {
        MapType mapType = mapType(BIGINT, BIGINT);
        BlockBuilder blockBuilder = mapType.createBlockBuilder(null, 10);
        histogram.topK(blockBuilder);
        Block object = mapType.getObject(blockBuilder, 0);
        Map<Long, Long> buckets = getMapFromLongBucket(object);
        return buckets;
    }

    public Map<Long, Long> getMapFromLongBucket(Block block)
    {
        ImmutableMap.Builder<Long, Long> buckets = new ImmutableMap.Builder<>();
        for (int pos = 0; pos < block.getPositionCount(); pos += 2) {
            buckets.put(block.getLong(pos), block.getLong(pos + 1));
        }
        return buckets.build();
    }

    public Map<Slice, Long> getMapFromSliceBucket(Block block)
    {
        ImmutableMap.Builder<Slice, Long> buckets = new ImmutableMap.Builder<>();
        for (int pos = 0; pos < block.getPositionCount(); pos += 2) {
            buckets.put(VARCHAR.getSlice(block, pos), block.getLong(pos + 1));
        }
        return buckets.build();
    }

    private Map<Slice, Long> getMapFromStreamSummaryOfSlices(StreamSummary streamSummary)
    {
        MapType mapType = mapType(VARCHAR, BIGINT);
        BlockBuilder blockBuilder = mapType.createBlockBuilder(null, 10);
        streamSummary.topK(blockBuilder);
        Block object = mapType.getObject(blockBuilder, 0);
        Map<Slice, Long> buckets = getMapFromSliceBucket(object);
        return buckets;
    }
}

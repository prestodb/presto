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
package com.facebook.presto.orc.metadata.statistics;

import com.facebook.presto.orc.proto.DwrfProto.KeyInfo;
import com.facebook.presto.orc.protobuf.ByteString;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class TestMapColumnStatisticsBuilder
{
    private static final KeyInfo INT_KEY1 = KeyInfo.newBuilder().setIntKey(1).build();
    private static final KeyInfo INT_KEY2 = KeyInfo.newBuilder().setIntKey(2).build();
    private static final KeyInfo INT_KEY3 = KeyInfo.newBuilder().setIntKey(3).build();
    private static final KeyInfo STRING_KEY1 = KeyInfo.newBuilder().setBytesKey(ByteString.copyFromUtf8("s1")).build();
    private static final KeyInfo STRING_KEY2 = KeyInfo.newBuilder().setBytesKey(ByteString.copyFromUtf8("s2")).build();
    private static final KeyInfo STRING_KEY3 = KeyInfo.newBuilder().setBytesKey(ByteString.copyFromUtf8("s3")).build();

    @DataProvider
    public Object[][] keySupplier()
    {
        return new Object[][] {
                {INT_KEY1, INT_KEY2, INT_KEY3},
                {STRING_KEY1, STRING_KEY2, STRING_KEY3},
        };
    }

    @Test
    public void testAddMapStatisticsNoValues()
    {
        MapColumnStatisticsBuilder builder = new MapColumnStatisticsBuilder(true);
        ColumnStatistics columnStatistics = builder.buildColumnStatistics();
        assertEquals(columnStatistics.getClass(), ColumnStatistics.class);
        assertEquals(columnStatistics.getNumberOfValues(), 0);
        assertNull(columnStatistics.getMapStatistics());
    }

    @Test(dataProvider = "keySupplier")
    public void testAddMapStatistics(KeyInfo[] keys)
    {
        KeyInfo key1 = keys[0];
        KeyInfo key2 = keys[1];

        ColumnStatistics columnStatistics1 = new ColumnStatistics(3L, null);
        ColumnStatistics columnStatistics2 = new ColumnStatistics(5L, null);

        MapColumnStatisticsBuilder builder = new MapColumnStatisticsBuilder(true);
        builder.addMapStatistics(key1, columnStatistics1);
        builder.addMapStatistics(key2, columnStatistics2);
        builder.increaseValueCount(10);

        MapColumnStatistics columnStatistics = (MapColumnStatistics) builder.buildColumnStatistics();
        assertEquals(columnStatistics.getNumberOfValues(), 10L);

        MapStatistics mapStatistics = columnStatistics.getMapStatistics();
        List<MapStatisticsEntry> entries = mapStatistics.getEntries();
        assertEquals(entries.size(), 2);
        assertEquals(entries.get(0).getKey(), key1);
        assertEquals(entries.get(0).getColumnStatistics(), columnStatistics1);
        assertEquals(entries.get(1).getKey(), key2);
        assertEquals(entries.get(1).getColumnStatistics(), columnStatistics2);
    }

    // test a case when keys are carried of from one row group, to another row
    // group having all null entries
    @Test(dataProvider = "keySupplier")
    public void testAddMapStatisticsNoValues(KeyInfo[] keys)
    {
        KeyInfo key1 = keys[0];
        KeyInfo key2 = keys[1];

        ColumnStatistics columnStatistics1 = new ColumnStatistics(3L, null);
        ColumnStatistics columnStatistics2 = new ColumnStatistics(5L, null);

        MapColumnStatisticsBuilder builder = new MapColumnStatisticsBuilder(true);
        builder.addMapStatistics(key1, columnStatistics1);
        builder.addMapStatistics(key2, columnStatistics2);
        builder.increaseValueCount(0);

        MapColumnStatistics columnStatistics = (MapColumnStatistics) builder.buildColumnStatistics();
        assertEquals(columnStatistics.getNumberOfValues(), 0);

        MapStatistics mapStatistics = columnStatistics.getMapStatistics();
        List<MapStatisticsEntry> entries = mapStatistics.getEntries();
        assertEquals(entries.size(), 2);
        assertEquals(entries.get(0).getKey(), key1);
        assertEquals(entries.get(0).getColumnStatistics(), columnStatistics1);
        assertEquals(entries.get(1).getKey(), key2);
        assertEquals(entries.get(1).getColumnStatistics(), columnStatistics2);
    }

    @Test(dataProvider = "keySupplier")
    public void testMergeMapStatistics(KeyInfo[] keys)
    {
        // merge two stats with keys: [k0,k1] and [k1,k2]
        // column statistics for k1 should be merged together
        MapColumnStatisticsBuilder builder1 = new MapColumnStatisticsBuilder(true);
        builder1.addMapStatistics(keys[0], new IntegerColumnStatistics(3L, null, new IntegerStatistics(1L, 2L, 3L)));
        builder1.addMapStatistics(keys[1], new IntegerColumnStatistics(5L, null, new IntegerStatistics(10L, 20L, 30L)));
        builder1.increaseValueCount(8);
        ColumnStatistics columnStatistics1 = builder1.buildColumnStatistics();

        MapColumnStatisticsBuilder builder2 = new MapColumnStatisticsBuilder(true);
        builder2.addMapStatistics(keys[1], new IntegerColumnStatistics(7L, null, new IntegerStatistics(25L, 95L, 100L)));
        builder2.addMapStatistics(keys[2], new IntegerColumnStatistics(9L, null, new IntegerStatistics(12L, 22L, 32L)));
        builder2.increaseValueCount(16);
        ColumnStatistics columnStatistics2 = builder2.buildColumnStatistics();

        MapStatistics mergedMapStatistics = MapColumnStatisticsBuilder.mergeMapStatistics(ImmutableList.of(columnStatistics1, columnStatistics2)).get();
        assertMergedMapStatistics(keys, mergedMapStatistics);
    }

    @Test(dataProvider = "keySupplier")
    public void testMergeMapStatisticsMissingStats(KeyInfo[] keys)
    {
        // valid map stat
        MapColumnStatisticsBuilder builder1 = new MapColumnStatisticsBuilder(true);
        builder1.addMapStatistics(keys[0], new ColumnStatistics(3L, null));
        builder1.increaseValueCount(3);
        ColumnStatistics columnStatistics1 = builder1.buildColumnStatistics();

        // invalid map stat
        ColumnStatistics columnStatistics2 = new ColumnStatistics(7L, null);

        Optional<MapStatistics> mergedMapStats = MapColumnStatisticsBuilder.mergeMapStatistics(ImmutableList.of(columnStatistics1, columnStatistics2));
        assertFalse(mergedMapStats.isPresent());
    }

    @Test(dataProvider = "keySupplier")
    public void testMergeColumnStatistics(KeyInfo[] keys)
    {
        // merge two stats with keys: [k0,k1] and [k1,k2]
        // column statistics for k1 should be merged together
        MapColumnStatisticsBuilder builder1 = new MapColumnStatisticsBuilder(true);
        builder1.addMapStatistics(keys[0], new IntegerColumnStatistics(3L, null, new IntegerStatistics(1L, 2L, 3L)));
        builder1.addMapStatistics(keys[1], new IntegerColumnStatistics(5L, null, new IntegerStatistics(10L, 20L, 30L)));
        builder1.increaseValueCount(10);
        ColumnStatistics columnStatistics1 = builder1.buildColumnStatistics();

        MapColumnStatisticsBuilder builder2 = new MapColumnStatisticsBuilder(true);
        builder2.addMapStatistics(keys[1], new IntegerColumnStatistics(7L, null, new IntegerStatistics(25L, 95L, 100L)));
        builder2.addMapStatistics(keys[2], new IntegerColumnStatistics(9L, null, new IntegerStatistics(12L, 22L, 32L)));
        builder2.increaseValueCount(20);
        ColumnStatistics columnStatistics2 = builder2.buildColumnStatistics();

        ColumnStatistics mergedColumnStatistics = ColumnStatistics.mergeColumnStatistics(ImmutableList.of(columnStatistics1, columnStatistics2));
        assertEquals(mergedColumnStatistics.getNumberOfValues(), 30);
        MapStatistics mergedMapStatistics = mergedColumnStatistics.getMapStatistics();
        assertMergedMapStatistics(keys, mergedMapStatistics);
    }

    private void assertMergedMapStatistics(KeyInfo[] keys, MapStatistics mergedMapStatistics)
    {
        assertNotNull(mergedMapStatistics);
        List<MapStatisticsEntry> entries = mergedMapStatistics.getEntries();

        assertEquals(entries.size(), 3);
        Map<KeyInfo, ColumnStatistics> columnStatisticsByKey = new HashMap<>();
        for (MapStatisticsEntry entry : entries) {
            columnStatisticsByKey.put(entry.getKey(), entry.getColumnStatistics());
        }

        assertEquals(columnStatisticsByKey.get(keys[0]), new IntegerColumnStatistics(3L, null, new IntegerStatistics(1L, 2L, 3L)));
        assertEquals(columnStatisticsByKey.get(keys[1]), new IntegerColumnStatistics(12L, null, new IntegerStatistics(10L, 95L, 130L))); // merged stats
        assertEquals(columnStatisticsByKey.get(keys[2]), new IntegerColumnStatistics(9L, null, new IntegerStatistics(12L, 22L, 32L)));
    }
}

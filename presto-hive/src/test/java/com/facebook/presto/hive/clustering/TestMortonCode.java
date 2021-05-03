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
package com.facebook.presto.hive.clustering;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.IntArrayBlockBuilder;
import com.facebook.presto.common.block.LongArrayBlockBuilder;
import com.facebook.presto.common.block.VariableWidthBlockBuilder;
import com.facebook.presto.common.predicate.Marker;
import com.facebook.presto.common.predicate.Utils;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestMortonCode
{
    @Test
    public void testDoubleComparison()
    {
        Block value = Utils.nativeValueToBlock(DoubleType.DOUBLE, 1.0);
        Marker splittingValue = new Marker(
                DoubleType.DOUBLE,
                Optional.of(Utils.nativeValueToBlock(DoubleType.DOUBLE, 1.2)),
                Marker.Bound.EXACTLY);
        assertTrue(MortonCode.smallerOrEqual(value, splittingValue, DoubleType.DOUBLE));

        splittingValue = new Marker(
                DoubleType.DOUBLE,
                Optional.of(Utils.nativeValueToBlock(DoubleType.DOUBLE, 0.9)),
                Marker.Bound.EXACTLY);
        assertFalse(MortonCode.smallerOrEqual(value, splittingValue, DoubleType.DOUBLE));
    }

    @Test
    public void testIntegerComparison()
    {
        Block value = Utils.nativeValueToBlock(IntegerType.INTEGER, 1L);
        Marker splittingValue = new Marker(
                IntegerType.INTEGER,
                Optional.of(Utils.nativeValueToBlock(IntegerType.INTEGER, 2L)),
                Marker.Bound.EXACTLY);
        assertTrue(MortonCode.smallerOrEqual(value, splittingValue, IntegerType.INTEGER));

        splittingValue = new Marker(
                IntegerType.INTEGER,
                Optional.of(Utils.nativeValueToBlock(IntegerType.INTEGER, -1L)),
                Marker.Bound.EXACTLY);
        assertFalse(MortonCode.smallerOrEqual(value, splittingValue, IntegerType.INTEGER));
    }

    @Test
    public void testSliceComparison()
    {
        Block value = Utils.nativeValueToBlock(
                VarcharType.VARCHAR, Slices.wrappedBuffer("abc".getBytes(StandardCharsets.UTF_8)));
        Marker splittingValue = new Marker(
                VarcharType.VARCHAR,
                Optional.of(Utils.nativeValueToBlock(
                        VarcharType.VARCHAR, Slices.wrappedBuffer("abd".getBytes(StandardCharsets.UTF_8)))),
                Marker.Bound.EXACTLY);
        assertTrue(MortonCode.smallerOrEqual(value, splittingValue, VarcharType.VARCHAR));

        splittingValue = new Marker(
                VarcharType.VARCHAR,
                Optional.of(Utils.nativeValueToBlock(
                        VarcharType.VARCHAR, Slices.wrappedBuffer("ab".getBytes(StandardCharsets.UTF_8)))),
                Marker.Bound.EXACTLY);
        assertFalse(MortonCode.smallerOrEqual(value, splittingValue, VarcharType.VARCHAR));
    }

    @Test
    public void testCharComparison()
    {
        Type charType = CharType.createCharType(3);
        Block value = Utils.nativeValueToBlock(
                charType, Slices.wrappedBuffer("abc".getBytes(StandardCharsets.UTF_8)));
        Marker splittingValue = new Marker(
                charType,
                Optional.of(Utils.nativeValueToBlock(
                        charType, Slices.wrappedBuffer("abd".getBytes(StandardCharsets.UTF_8)))),
                Marker.Bound.EXACTLY);
        assertTrue(MortonCode.smallerOrEqual(value, splittingValue, VarcharType.VARCHAR));

        splittingValue = new Marker(
                charType,
                Optional.of(Utils.nativeValueToBlock(
                        charType, Slices.wrappedBuffer("ab".getBytes(StandardCharsets.UTF_8)))),
                Marker.Bound.EXACTLY);
        assertFalse(MortonCode.smallerOrEqual(value, splittingValue, charType));
    }

    @Test
    public void testGetIntervalIndexForInteger()
    {
        List<Marker> splittingValues = new ArrayList<>();
        splittingValues.add(new Marker(
                IntegerType.INTEGER,
                Optional.of(Utils.nativeValueToBlock(IntegerType.INTEGER, 1L)),
                Marker.Bound.EXACTLY));
        splittingValues.add(new Marker(
                IntegerType.INTEGER,
                Optional.of(Utils.nativeValueToBlock(IntegerType.INTEGER, 3L)),
                Marker.Bound.EXACTLY));
        splittingValues.add(new Marker(
                IntegerType.INTEGER,
                Optional.of(Utils.nativeValueToBlock(IntegerType.INTEGER, 5L)),
                Marker.Bound.EXACTLY));

        Block value = Utils.nativeValueToBlock(IntegerType.INTEGER, 1L);
        assertEquals(
                MortonCode.getIntervalIndex(splittingValues, value, IntegerType.INTEGER),
                0);

        value = Utils.nativeValueToBlock(IntegerType.INTEGER, 2L);
        assertEquals(
                MortonCode.getIntervalIndex(splittingValues, value, IntegerType.INTEGER),
                1);

        value = Utils.nativeValueToBlock(IntegerType.INTEGER, 10L);
        assertEquals(
                MortonCode.getIntervalIndex(splittingValues, value, IntegerType.INTEGER),
                3);

        value = Utils.nativeValueToBlock(IntegerType.INTEGER, null);
        assertEquals(
                MortonCode.getIntervalIndex(splittingValues, value, IntegerType.INTEGER),
                0);
    }

    @Test
    public void testGetIntervalIndexForString()
    {
        List<Marker> splittingValues = new ArrayList<>();
        splittingValues.add(new Marker(
                VarcharType.VARCHAR,
                Optional.of(Utils.nativeValueToBlock(
                        VarcharType.VARCHAR, Slices.wrappedBuffer("abd".getBytes(StandardCharsets.UTF_8)))),
                Marker.Bound.EXACTLY));
        splittingValues.add(new Marker(
                VarcharType.VARCHAR,
                Optional.of(Utils.nativeValueToBlock(
                        VarcharType.VARCHAR, Slices.wrappedBuffer("ce".getBytes(StandardCharsets.UTF_8)))),
                Marker.Bound.EXACTLY));
        splittingValues.add(new Marker(
                VarcharType.VARCHAR,
                Optional.of(Utils.nativeValueToBlock(
                        VarcharType.VARCHAR, Slices.wrappedBuffer("st".getBytes(StandardCharsets.UTF_8)))),
                Marker.Bound.EXACTLY));

        Block value = Utils.nativeValueToBlock(
                VarcharType.VARCHAR, Slices.wrappedBuffer("abc".getBytes(StandardCharsets.UTF_8)));
        assertEquals(
                MortonCode.getIntervalIndex(splittingValues, value, VarcharType.VARCHAR),
                0);

        value = Utils.nativeValueToBlock(
                VarcharType.VARCHAR, Slices.wrappedBuffer("abe".getBytes(StandardCharsets.UTF_8)));
        assertEquals(
                MortonCode.getIntervalIndex(splittingValues, value, VarcharType.VARCHAR),
                1);

        value = Utils.nativeValueToBlock(
                VarcharType.VARCHAR, Slices.wrappedBuffer("sta".getBytes(StandardCharsets.UTF_8)));
        assertEquals(
                MortonCode.getIntervalIndex(splittingValues, value, VarcharType.VARCHAR),
                3);

        value = Utils.nativeValueToBlock(
                VarcharType.VARCHAR, Slices.EMPTY_SLICE);
        assertEquals(
                MortonCode.getIntervalIndex(splittingValues, value, VarcharType.VARCHAR),
                0);
    }

    @Test
    public void testGetCluster()
    {
        List<String> clusteredBy = new ArrayList<>(Arrays.asList("c1", "c2", "c3"));
        List<Integer> clusterCount = new ArrayList<>(Arrays.asList(4, 4, 4));
        List<Type> types = new ArrayList<>(Arrays.asList(
                IntegerType.INTEGER, DoubleType.DOUBLE, VarcharType.VARCHAR));
        List<Object> distribution = new ArrayList<>(Arrays.asList(
                1L, 3L, 5L,
                1.0, 3.0, 5.0,
                Slices.wrappedBuffer("abc".getBytes(StandardCharsets.UTF_8)),
                Slices.wrappedBuffer("def".getBytes(StandardCharsets.UTF_8)),
                Slices.wrappedBuffer("sta".getBytes(StandardCharsets.UTF_8))));
        MortonCode mortonCode = new MortonCode(
                clusterCount, clusteredBy, distribution, types);

        int entries = 6;
        BlockBuilder blockBuilder = new IntArrayBlockBuilder(null, entries);
        for (int i = 0; i < entries; i++) {
            blockBuilder.writeInt(i);
        }
        Block c1Block = blockBuilder.build();

        blockBuilder = new LongArrayBlockBuilder(null, entries);
        for (int i = 0; i < entries; i++) {
            blockBuilder.writeLong(Double.doubleToLongBits(Double.valueOf(i)));
        }
        Block c2Block = blockBuilder.build();

        blockBuilder = new VariableWidthBlockBuilder(null, entries, 1000);
        Slice var1 = Slices.wrappedBuffer("ab".getBytes(StandardCharsets.UTF_8));
        Slice var2 = Slices.wrappedBuffer("cd".getBytes(StandardCharsets.UTF_8));
        Slice var3 = Slices.wrappedBuffer("ef".getBytes(StandardCharsets.UTF_8));
        Slice var4 = Slices.wrappedBuffer("gh".getBytes(StandardCharsets.UTF_8));
        Slice var5 = Slices.wrappedBuffer("hi".getBytes(StandardCharsets.UTF_8));
        Slice var6 = Slices.wrappedBuffer("xy".getBytes(StandardCharsets.UTF_8));
        VarcharType.VARCHAR.writeSlice(blockBuilder, var1);
        VarcharType.VARCHAR.writeSlice(blockBuilder, var2);
        VarcharType.VARCHAR.writeSlice(blockBuilder, var3);
        VarcharType.VARCHAR.writeSlice(blockBuilder, var4);
        VarcharType.VARCHAR.writeSlice(blockBuilder, var5);
        VarcharType.VARCHAR.writeSlice(blockBuilder, var6);
        Block c3Block = blockBuilder.build();

        Page page = new Page(c1Block, c2Block, c3Block);
        assertEquals(mortonCode.getCluster(page, 0), 0);
        assertEquals(mortonCode.getCluster(page, 1), 4);
        assertEquals(mortonCode.getCluster(page, 2), 35);
        assertEquals(mortonCode.getCluster(page, 3), 35);
        assertEquals(mortonCode.getCluster(page, 4), 56);
        assertEquals(mortonCode.getCluster(page, 5), 60);
    }

    @Test
    public void testInterleaveBits()
    {
        assertEquals(MortonCode.interleaveBits(0, 0), 0);
        assertEquals(MortonCode.interleaveBits(0, 1), 2);
        assertEquals(MortonCode.interleaveBits(2, 1), 6);
        assertEquals(MortonCode.interleaveBits(10, 10), 204);
    }
}

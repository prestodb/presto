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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.operator.aggregation.TypedSet.ElementReference;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.InterleavedBlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.MapType;
import com.facebook.presto.type.RowType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.facebook.presto.block.BlockAssertions.createArrayBigintBlock;
import static com.facebook.presto.block.BlockAssertions.createDoublesBlock;
import static com.facebook.presto.block.BlockAssertions.createEmptyLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createLongDecimalsBlock;
import static com.facebook.presto.block.BlockAssertions.createLongSequenceBlock;
import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static com.facebook.presto.spi.type.DateTimeEncoding.unpackMillisUtc;
import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimeZoneKey.getTimeZoneKeyForOffset;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.type.TypeJsonUtils.appendToBlockBuilder;
import static com.facebook.presto.util.StructuralTestUtil.mapBlockOf;
import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestTypedSet
{
    @Test
    public void testConstructor()
    {
        for (int i = -2; i <= -1; i++) {
            try {
                //noinspection ResultOfObjectAllocationIgnored
                new TypedSet(BIGINT, i);
                fail("Should throw exception if expectedSize < 0");
            }
            catch (IllegalArgumentException ignored) {
            }
        }

        try {
            //noinspection ResultOfObjectAllocationIgnored
            new TypedSet(null, 1);
            fail("Should throw exception if type is null");
        }
        catch (NullPointerException | IllegalArgumentException ignored) {
        }
    }

    @Test
    public void testNullElements()
    {
        Stream<String> stringStream = Stream.<String>generate(() -> null).limit(1_000);
        Block nullStringBlock = createStringsBlock(stringStream.toArray(String[]::new));
        TypedSet typedSet = new TypedSet(VARCHAR, 1_000);

        for (int i = 0; i < nullStringBlock.getPositionCount(); i++) {
            typedSet.add(nullStringBlock, i);
        }

        assertTrue(typedSet.size() == 1);
        assertTrue(typedSet.contains(nullStringBlock, 0));

        //add a non-null element & verify
        Block nonNullStringBlock = createStringsBlock("test");
        typedSet.add(nonNullStringBlock, 0);
        assertTrue(typedSet.size() == 2);
        assertTrue(typedSet.contains(nullStringBlock, 0));
        assertTrue(typedSet.contains(nonNullStringBlock, 0));
    }

    @Test
    public void testBooleanTypedSet()
    {
        BlockBuilder blockBuilder = BOOLEAN.createBlockBuilder(new BlockBuilderStatus(), 5);
        BOOLEAN.writeBoolean(blockBuilder, true);
        BOOLEAN.writeBoolean(blockBuilder, false);
        BOOLEAN.writeBoolean(blockBuilder, true);
        BOOLEAN.writeBoolean(blockBuilder, false);
        BOOLEAN.writeBoolean(blockBuilder, false);
        testTypedSet(blockBuilder.build(), BOOLEAN, 2);
    }

    @Test
    public void testDecimalTypedSet()
    {
        Block decimalsBlock = createLongDecimalsBlock("11.11", "22.22", null, "11.11", "33.33");
        testTypedSet(decimalsBlock, createDecimalType(18), 4);
    }

    @Test
    public void testTimestampWithTimeZoneTypedSet()
    {
        BlockBuilder blockBuilder = TIMESTAMP_WITH_TIME_ZONE.createBlockBuilder(new BlockBuilderStatus(), 10);
        TIMESTAMP_WITH_TIME_ZONE.writeLong(blockBuilder, packDateTimeWithZone(1111, getTimeZoneKeyForOffset(0)));
        TIMESTAMP_WITH_TIME_ZONE.writeLong(blockBuilder, packDateTimeWithZone(1111, getTimeZoneKeyForOffset(1)));
        TIMESTAMP_WITH_TIME_ZONE.writeLong(blockBuilder, packDateTimeWithZone(2222, getTimeZoneKeyForOffset(2)));
        TIMESTAMP_WITH_TIME_ZONE.writeLong(blockBuilder, packDateTimeWithZone(2222, getTimeZoneKeyForOffset(3)));
        TIMESTAMP_WITH_TIME_ZONE.writeLong(blockBuilder, packDateTimeWithZone(3333, getTimeZoneKeyForOffset(4)));
        TIMESTAMP_WITH_TIME_ZONE.writeLong(blockBuilder, packDateTimeWithZone(4444, getTimeZoneKeyForOffset(5)));
        testTypedSet(blockBuilder.build(), TIMESTAMP_WITH_TIME_ZONE, 4);
    }

    @Test
    public void testTimestampTypedSet()
    {
        BlockBuilder blockBuilder = TIMESTAMP.createBlockBuilder(new BlockBuilderStatus(), 10);
        TIMESTAMP.writeLong(blockBuilder, 1111);
        TIMESTAMP.writeLong(blockBuilder, 1111);
        TIMESTAMP.writeLong(blockBuilder, 2222);
        TIMESTAMP.writeLong(blockBuilder, 3333);
        TIMESTAMP.writeLong(blockBuilder, 3333);
        TIMESTAMP.writeLong(blockBuilder, 4444);
        testTypedSet(blockBuilder.build(), TIMESTAMP, 4);
    }

    @Test
    public void testMapTypedSet()
    {
        MapType mapType = new MapType(VARCHAR, VARCHAR);
        BlockBuilder builder = mapType.createBlockBuilder(new BlockBuilderStatus(), 5);
        mapType.writeObject(builder, mapBlockOf(VARCHAR, VARCHAR, ImmutableMap.of("a", "b")));
        mapType.writeObject(builder, mapBlockOf(VARCHAR, VARCHAR, ImmutableMap.of("c", "d")));
        mapType.writeObject(builder, mapBlockOf(VARCHAR, VARCHAR, ImmutableMap.of("e", "f")));
        mapType.writeObject(builder, mapBlockOf(VARCHAR, VARCHAR, ImmutableMap.of("e", "f")));
        mapType.writeObject(builder, mapBlockOf(VARCHAR, VARCHAR, ImmutableMap.of("e", "f")));
        testTypedSet(builder.build(), mapType, 3);
    }

    @Test
    public void testRowTypedSet()
    {
        List<Type> fieldTypes = ImmutableList.of(DOUBLE, VARCHAR, TINYINT);
        RowType rowType = new RowType(fieldTypes, Optional.empty());
        BlockBuilder rowArrayBuilder = rowType.createBlockBuilder(new BlockBuilderStatus(), 5);
        List<List<Object>> elements = ImmutableList.of(
                ImmutableList.of(1.0, "a", 1), ImmutableList.of(2.0, "b", 2), ImmutableList.of(3.0, "c", 3), ImmutableList.of(1.0, "a", 1), ImmutableList.of(2.0, "b", 2));
        for (List<Object> element : elements) {
            BlockBuilder rowBuilder = new InterleavedBlockBuilder(fieldTypes, new BlockBuilderStatus(), fieldTypes.size());
            for (int j = 0; j < fieldTypes.size(); j++) {
                appendToBlockBuilder(fieldTypes.get(j), element.get(j), rowBuilder);
            }
            rowType.writeObject(rowArrayBuilder, rowBuilder.build());
        }
        testTypedSet(rowArrayBuilder.build(), rowType, 3);
    }

    @Test
    public void testArrayTypedSet()
    {
        Block arrayBlock = createArrayBigintBlock(ImmutableList.of(ImmutableList.of(1L),
                ImmutableList.of(1L, 2L), ImmutableList.of(1L, 2L), ImmutableList.of(3L)));
        testTypedSet(arrayBlock, new ArrayType(BIGINT), 3);
    }

    @Test
    public void testDoubleTypedSet()
    {
        Double[] values = IntStream.rangeClosed(0, 99).mapToDouble(integer -> (double) integer).boxed().toArray(Double[]::new);
        Block doubleBlock = createDoublesBlock(values);
        testTypedSet(doubleBlock, DOUBLE, 100);
    }

    @Test
    public void testVarcharTypedSet()
    {
        BlockBuilder varcharBlockBuilder = VARCHAR.createBlockBuilder(new BlockBuilderStatus(), 5);
        VARCHAR.writeSlice(varcharBlockBuilder, utf8Slice("hello"));
        VARCHAR.writeSlice(varcharBlockBuilder, utf8Slice("bye"));
        VARCHAR.writeSlice(varcharBlockBuilder, utf8Slice("abc"));
        VARCHAR.writeSlice(varcharBlockBuilder, utf8Slice("abc"));
        VARCHAR.writeSlice(varcharBlockBuilder, utf8Slice("abc"));
        testTypedSet(varcharBlockBuilder.build(), VARCHAR, 3);
    }

    @Test
    public void testBigintTypedSet()
    {
        List<Integer> expectedSetSizes = ImmutableList.of(0, 1, 3, 3, 3, 1, 100, 200, 1, 1, 1);
        List<Block> longBlocks =
                ImmutableList.of(
                        createEmptyLongsBlock(),
                        createLongsBlock(1L),
                        createLongsBlock(1L, 2L, 3L),
                        createLongsBlock(1L, 2L, 3L, 1L, 2L, 3L),
                        createLongsBlock(1L, null, 3L),
                        createLongsBlock(null, null, null),
                        createLongSequenceBlock(0, 100),
                        createLongSequenceBlock(-100, 100),
                        createLongsBlock(Collections.nCopies(1, null)),
                        createLongsBlock(Collections.nCopies(100, null)),
                        createLongsBlock(Collections.nCopies(100, 0L))
                );

        for (int i = 0; i < longBlocks.size(); i++) {
            testTypedSet(longBlocks.get(i), BIGINT, expectedSetSizes.get(i));
        }
    }

    @Test
    public void testFind()
    {
        Block longBlock1 = createLongsBlock(1L);
        Block longBlock2 = createLongsBlock(1L);
        TypedSet typedSet = new TypedSet(BIGINT, 1);
        typedSet.add(longBlock1, 0);
        typedSet.add(longBlock2, 0);

        Optional<ElementReference> existing = typedSet.find(ElementReference.of(longBlock2, BIGINT, 0));
        assertTrue(existing.isPresent());
        assertTrue(existing.get().getBlock() == longBlock1);
    }

    private void testTypedSet(Block block, Type elementType, int expectedSize)
    {
        Set<Object> set = new HashSet<>();
        TypedSet typedSet = new TypedSet(elementType, 10);
        for (int i = 0; i < block.getPositionCount(); i++) {
            typedSet.add(block, i);
            set.add(getElement(block, elementType, i));
        }

        assertTrue(typedSet.size() == expectedSize);

        if (elementType.getJavaType().isPrimitive()) {
            assertTrue(typedSet.size() == set.size());
        }

        for (int i = 0; i < block.getPositionCount(); i++) {
            assertTrue(typedSet.contains(block, i));
            if (elementType.getJavaType().isPrimitive()) {
                assertTrue(set.contains(getElement(block, elementType, i)));
            }
        }
    }

    private Object getElement(Block block, Type type, int position)
    {
        Class javaType = type.getJavaType();
        if (javaType == boolean.class) {
            return type.getBoolean(block, position);
        }
        else if (javaType == long.class) {
            if (type == TIMESTAMP_WITH_TIME_ZONE) {
                return unpackMillisUtc(type.getLong(block, position));
            }
            else {
                return type.getLong(block, position);
            }
        }
        else if (javaType == double.class) {
            return type.getDouble(block, position);
        }
        else if (javaType == Slice.class) {
            return type.getSlice(block, position);
        }
        else if (!type.getJavaType().isPrimitive()) {
            return type.getObject(block, position);
        }
        else {
            throw new UnsupportedOperationException("Type not handled: " + type.getJavaType());
        }
    }
}

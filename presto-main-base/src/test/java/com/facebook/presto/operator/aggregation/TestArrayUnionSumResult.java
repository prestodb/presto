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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.aggregation.state.ArrayUnionSumState;
import com.facebook.presto.operator.aggregation.state.ArrayUnionSumStateFactory;
import com.facebook.presto.operator.aggregation.state.ArrayUnionSumStateSerializer;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.RealType.REAL;
import static java.lang.Float.floatToRawIntBits;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestArrayUnionSumResult
{
    private static final ArrayType ARRAY_BIGINT = new ArrayType(BIGINT);
    private static final ArrayType ARRAY_DOUBLE = new ArrayType(DOUBLE);
    private static final ArrayType ARRAY_REAL = new ArrayType(REAL);

    @Test
    public void testBasicUnionSum()
    {
        ArrayUnionSumState state = new ArrayUnionSumStateFactory(BIGINT).createSingleState();

        // Create array [1, 2, 3]
        Block array1 = createLongsBlock(BIGINT, 1L, 2L, 3L);
        ArrayUnionSumResult result1 = ArrayUnionSumResult.create(BIGINT, state.getAdder(), array1);
        assertEquals(result1.size(), 3);

        // Create array [10, 5, 4, 1]
        Block array2 = createLongsBlock(BIGINT, 10L, 5L, 4L, 1L);
        ArrayUnionSumResult result2 = result1.unionSum(array2);
        assertEquals(result2.size(), 4);

        // Create array [9, 0, 5, 4]
        Block array3 = createLongsBlock(BIGINT, 9L, 0L, 5L, 4L);
        ArrayUnionSumResult result3 = result2.unionSum(array3);
        assertEquals(result3.size(), 4);

        // Verify the result: [20, 7, 12, 5]
        BlockBuilder out = ARRAY_BIGINT.createBlockBuilder(null, 1);
        result3.serialize(out);
        Block resultBlock = out.build();
        Block arrayBlock = ARRAY_BIGINT.getObject(resultBlock, 0);

        assertEquals(BIGINT.getLong(arrayBlock, 0), 20L);
        assertEquals(BIGINT.getLong(arrayBlock, 1), 7L);
        assertEquals(BIGINT.getLong(arrayBlock, 2), 12L);
        assertEquals(BIGINT.getLong(arrayBlock, 3), 5L);
    }

    @Test
    public void testNullHandling()
    {
        ArrayUnionSumState state = new ArrayUnionSumStateFactory(BIGINT).createSingleState();

        // Create array [1, null, 3]
        Block array1 = createLongsBlock(BIGINT, 1L, null, 3L);
        ArrayUnionSumResult result1 = ArrayUnionSumResult.create(BIGINT, state.getAdder(), array1);

        // Create array [10, 5]
        Block array2 = createLongsBlock(BIGINT, 10L, 5L);
        ArrayUnionSumResult result2 = result1.unionSum(array2);

        // Verify the result: [11, 5, 3] - null treated as 0
        BlockBuilder out = ARRAY_BIGINT.createBlockBuilder(null, 1);
        result2.serialize(out);
        Block resultBlock = out.build();
        Block arrayBlock = ARRAY_BIGINT.getObject(resultBlock, 0);

        assertEquals(BIGINT.getLong(arrayBlock, 0), 11L);
        assertEquals(BIGINT.getLong(arrayBlock, 1), 5L);
        assertEquals(BIGINT.getLong(arrayBlock, 2), 3L);
    }

    @Test
    public void testBothNullsAtSamePosition()
    {
        ArrayUnionSumState state = new ArrayUnionSumStateFactory(BIGINT).createSingleState();

        // Create array [1, null, 3]
        Block array1 = createLongsBlock(BIGINT, 1L, null, 3L);
        ArrayUnionSumResult result1 = ArrayUnionSumResult.create(BIGINT, state.getAdder(), array1);

        // Create array [10, null, 5]
        Block array2 = createLongsBlock(BIGINT, 10L, null, 5L);
        ArrayUnionSumResult result2 = result1.unionSum(array2);

        // Verify the result: [11, 0, 8] - both nulls treated as 0
        BlockBuilder out = ARRAY_BIGINT.createBlockBuilder(null, 1);
        result2.serialize(out);
        Block resultBlock = out.build();
        Block arrayBlock = ARRAY_BIGINT.getObject(resultBlock, 0);

        assertEquals(BIGINT.getLong(arrayBlock, 0), 11L);
        assertEquals(BIGINT.getLong(arrayBlock, 1), 0L);
        assertEquals(BIGINT.getLong(arrayBlock, 2), 8L);
    }

    @Test
    public void testDifferentLengthArrays()
    {
        ArrayUnionSumState state = new ArrayUnionSumStateFactory(BIGINT).createSingleState();

        // Create array [1, 2]
        Block array1 = createLongsBlock(BIGINT, 1L, 2L);
        ArrayUnionSumResult result1 = ArrayUnionSumResult.create(BIGINT, state.getAdder(), array1);

        // Create array [10, 5, 100, 200]
        Block array2 = createLongsBlock(BIGINT, 10L, 5L, 100L, 200L);
        ArrayUnionSumResult result2 = result1.unionSum(array2);

        // Verify result: [11, 7, 100, 200]
        BlockBuilder out = ARRAY_BIGINT.createBlockBuilder(null, 1);
        result2.serialize(out);
        Block resultBlock = out.build();
        Block arrayBlock = ARRAY_BIGINT.getObject(resultBlock, 0);

        assertEquals(arrayBlock.getPositionCount(), 4);
        assertEquals(BIGINT.getLong(arrayBlock, 0), 11L);
        assertEquals(BIGINT.getLong(arrayBlock, 1), 7L);
        assertEquals(BIGINT.getLong(arrayBlock, 2), 100L);
        assertEquals(BIGINT.getLong(arrayBlock, 3), 200L);
    }

    @Test
    public void testDoubleAdder()
    {
        ArrayUnionSumState state = new ArrayUnionSumStateFactory(DOUBLE).createSingleState();

        // Create array [1.5, 2.5]
        Block array1 = createDoublesBlock(1.5, 2.5);
        ArrayUnionSumResult result1 = ArrayUnionSumResult.create(DOUBLE, state.getAdder(), array1);

        // Create array [10.5, 5.5, 100.0]
        Block array2 = createDoublesBlock(10.5, 5.5, 100.0);
        ArrayUnionSumResult result2 = result1.unionSum(array2);

        // Verify result: [12.0, 8.0, 100.0]
        BlockBuilder out = ARRAY_DOUBLE.createBlockBuilder(null, 1);
        result2.serialize(out);
        Block resultBlock = out.build();
        Block arrayBlock = ARRAY_DOUBLE.getObject(resultBlock, 0);

        assertEquals(arrayBlock.getPositionCount(), 3);
        assertEquals(DOUBLE.getDouble(arrayBlock, 0), 12.0, 0.001);
        assertEquals(DOUBLE.getDouble(arrayBlock, 1), 8.0, 0.001);
        assertEquals(DOUBLE.getDouble(arrayBlock, 2), 100.0, 0.001);
    }

    @Test
    public void testDoubleWithNulls()
    {
        ArrayUnionSumState state = new ArrayUnionSumStateFactory(DOUBLE).createSingleState();

        // Create array [1.5, null]
        Block array1 = createDoublesBlock(1.5, null);
        ArrayUnionSumResult result1 = ArrayUnionSumResult.create(DOUBLE, state.getAdder(), array1);

        // Create array [10.5, 5.5]
        Block array2 = createDoublesBlock(10.5, 5.5);
        ArrayUnionSumResult result2 = result1.unionSum(array2);

        // Verify result: [12.0, 5.5] - null treated as 0
        BlockBuilder out = ARRAY_DOUBLE.createBlockBuilder(null, 1);
        result2.serialize(out);
        Block resultBlock = out.build();
        Block arrayBlock = ARRAY_DOUBLE.getObject(resultBlock, 0);

        assertEquals(DOUBLE.getDouble(arrayBlock, 0), 12.0, 0.001);
        assertEquals(DOUBLE.getDouble(arrayBlock, 1), 5.5, 0.001);
    }

    @Test
    public void testRealAdder()
    {
        ArrayUnionSumState state = new ArrayUnionSumStateFactory(REAL).createSingleState();

        // Create array [1.5f, 2.5f]
        Block array1 = createRealsBlock(1.5f, 2.5f);
        ArrayUnionSumResult result1 = ArrayUnionSumResult.create(REAL, state.getAdder(), array1);

        // Create array [10.5f, 5.5f]
        Block array2 = createRealsBlock(10.5f, 5.5f);
        ArrayUnionSumResult result2 = result1.unionSum(array2);

        // Verify result: [12.0f, 8.0f]
        BlockBuilder out = ARRAY_REAL.createBlockBuilder(null, 1);
        result2.serialize(out);
        Block resultBlock = out.build();
        Block arrayBlock = ARRAY_REAL.getObject(resultBlock, 0);

        assertEquals(arrayBlock.getPositionCount(), 2);
        assertEquals(Float.intBitsToFloat((int) REAL.getLong(arrayBlock, 0)), 12.0f, 0.001f);
        assertEquals(Float.intBitsToFloat((int) REAL.getLong(arrayBlock, 1)), 8.0f, 0.001f);
    }

    @Test
    public void testRealWithNulls()
    {
        ArrayUnionSumState state = new ArrayUnionSumStateFactory(REAL).createSingleState();

        // Create array [1.5f, null]
        Block array1 = createRealsBlock(1.5f, null);
        ArrayUnionSumResult result1 = ArrayUnionSumResult.create(REAL, state.getAdder(), array1);

        // Create array [10.5f, 5.5f]
        Block array2 = createRealsBlock(10.5f, 5.5f);
        ArrayUnionSumResult result2 = result1.unionSum(array2);

        // Verify result: [12.0f, 5.5f]
        BlockBuilder out = ARRAY_REAL.createBlockBuilder(null, 1);
        result2.serialize(out);
        Block resultBlock = out.build();
        Block arrayBlock = ARRAY_REAL.getObject(resultBlock, 0);

        assertEquals(Float.intBitsToFloat((int) REAL.getLong(arrayBlock, 0)), 12.0f, 0.001f);
        assertEquals(Float.intBitsToFloat((int) REAL.getLong(arrayBlock, 1)), 5.5f, 0.001f);
    }

    @Test
    public void testSingleStateFactory()
    {
        ArrayUnionSumStateFactory factory = new ArrayUnionSumStateFactory(BIGINT);
        ArrayUnionSumState state = factory.createSingleState();

        assertNotNull(state);
        assertNull(state.get());
        assertEquals(state.getElementType(), BIGINT);
        assertNotNull(state.getAdder());

        // Test set and get
        Block array = createLongsBlock(BIGINT, 1L, 2L, 3L);
        ArrayUnionSumResult result = ArrayUnionSumResult.create(BIGINT, state.getAdder(), array);
        state.set(result);
        assertNotNull(state.get());
        assertEquals(state.get().size(), 3);

        // Test estimated size
        assertTrue(state.getEstimatedSize() > 0);
    }

    @Test
    public void testGroupedStateFactory()
    {
        ArrayUnionSumStateFactory factory = new ArrayUnionSumStateFactory(BIGINT);
        ArrayUnionSumState state = factory.createGroupedState();

        assertNotNull(state);
        assertNull(state.get());
        assertEquals(state.getElementType(), BIGINT);
        assertNotNull(state.getAdder());

        // Test estimated size
        assertTrue(state.getEstimatedSize() > 0);
    }

    @Test
    public void testStateSerializer()
    {
        ArrayUnionSumStateFactory factory = new ArrayUnionSumStateFactory(BIGINT);
        ArrayUnionSumState state = factory.createSingleState();
        ArrayUnionSumStateSerializer serializer = new ArrayUnionSumStateSerializer(ARRAY_BIGINT);

        assertEquals(serializer.getSerializedType(), ARRAY_BIGINT);

        // Test serialize null state
        BlockBuilder out = ARRAY_BIGINT.createBlockBuilder(null, 1);
        serializer.serialize(state, out);
        Block serialized = out.build();
        assertTrue(serialized.isNull(0));

        // Test serialize with value
        Block array = createLongsBlock(BIGINT, 1L, 2L, 3L);
        ArrayUnionSumResult result = ArrayUnionSumResult.create(BIGINT, state.getAdder(), array);
        state.set(result);

        BlockBuilder out2 = ARRAY_BIGINT.createBlockBuilder(null, 1);
        serializer.serialize(state, out2);
        Block serialized2 = out2.build();

        // Test deserialize
        ArrayUnionSumState newState = factory.createSingleState();
        serializer.deserialize(serialized2, 0, newState);
        assertNotNull(newState.get());
        assertEquals(newState.get().size(), 3);
    }

    @Test
    public void testRetainedSizeInBytes()
    {
        ArrayUnionSumState state = new ArrayUnionSumStateFactory(BIGINT).createSingleState();

        Block array1 = createLongsBlock(BIGINT, 1L, 2L, 3L);
        ArrayUnionSumResult result1 = ArrayUnionSumResult.create(BIGINT, state.getAdder(), array1);
        assertTrue(result1.getRetainedSizeInBytes() > 0);

        Block array2 = createLongsBlock(BIGINT, 10L, 5L, 4L, 1L);
        ArrayUnionSumResult result2 = result1.unionSum(array2);
        assertTrue(result2.getRetainedSizeInBytes() > 0);
    }

    @Test
    public void testGetElementType()
    {
        ArrayUnionSumState state = new ArrayUnionSumStateFactory(BIGINT).createSingleState();
        Block array = createLongsBlock(BIGINT, 1L, 2L, 3L);
        ArrayUnionSumResult result = ArrayUnionSumResult.create(BIGINT, state.getAdder(), array);
        assertEquals(result.getElementType(), BIGINT);
    }

    @Test
    public void testEmptyArray()
    {
        ArrayUnionSumState state = new ArrayUnionSumStateFactory(BIGINT).createSingleState();

        // Create empty array
        Block emptyArray = createLongsBlock(BIGINT);
        ArrayUnionSumResult result = ArrayUnionSumResult.create(BIGINT, state.getAdder(), emptyArray);
        assertEquals(result.size(), 0);

        // Serialize empty array
        BlockBuilder out = ARRAY_BIGINT.createBlockBuilder(null, 1);
        result.serialize(out);
        Block resultBlock = out.build();
        Block arrayBlock = ARRAY_BIGINT.getObject(resultBlock, 0);
        assertEquals(arrayBlock.getPositionCount(), 0);
    }

    @Test
    public void testEmptyArrayUnionWithNonEmpty()
    {
        ArrayUnionSumState state = new ArrayUnionSumStateFactory(BIGINT).createSingleState();

        // Create empty array
        Block emptyArray = createLongsBlock(BIGINT);
        ArrayUnionSumResult result1 = ArrayUnionSumResult.create(BIGINT, state.getAdder(), emptyArray);

        // Union with non-empty array [1, 2, 3]
        Block array2 = createLongsBlock(BIGINT, 1L, 2L, 3L);
        ArrayUnionSumResult result2 = result1.unionSum(array2);

        // Result should be [1, 2, 3]
        assertEquals(result2.size(), 3);
        BlockBuilder out = ARRAY_BIGINT.createBlockBuilder(null, 1);
        result2.serialize(out);
        Block resultBlock = out.build();
        Block arrayBlock = ARRAY_BIGINT.getObject(resultBlock, 0);

        assertEquals(BIGINT.getLong(arrayBlock, 0), 1L);
        assertEquals(BIGINT.getLong(arrayBlock, 1), 2L);
        assertEquals(BIGINT.getLong(arrayBlock, 2), 3L);
    }

    @Test
    public void testNonEmptyUnionWithEmptyArray()
    {
        ArrayUnionSumState state = new ArrayUnionSumStateFactory(BIGINT).createSingleState();

        // Create non-empty array [1, 2, 3]
        Block array1 = createLongsBlock(BIGINT, 1L, 2L, 3L);
        ArrayUnionSumResult result1 = ArrayUnionSumResult.create(BIGINT, state.getAdder(), array1);

        // Union with empty array
        Block emptyArray = createLongsBlock(BIGINT);
        ArrayUnionSumResult result2 = result1.unionSum(emptyArray);

        // Result should still be [1, 2, 3]
        assertEquals(result2.size(), 3);
        BlockBuilder out = ARRAY_BIGINT.createBlockBuilder(null, 1);
        result2.serialize(out);
        Block resultBlock = out.build();
        Block arrayBlock = ARRAY_BIGINT.getObject(resultBlock, 0);

        assertEquals(BIGINT.getLong(arrayBlock, 0), 1L);
        assertEquals(BIGINT.getLong(arrayBlock, 1), 2L);
        assertEquals(BIGINT.getLong(arrayBlock, 2), 3L);
    }

    @Test
    public void testEmptyArrayUnionWithEmpty()
    {
        ArrayUnionSumState state = new ArrayUnionSumStateFactory(BIGINT).createSingleState();

        // Create two empty arrays
        Block emptyArray1 = createLongsBlock(BIGINT);
        Block emptyArray2 = createLongsBlock(BIGINT);

        ArrayUnionSumResult result1 = ArrayUnionSumResult.create(BIGINT, state.getAdder(), emptyArray1);
        ArrayUnionSumResult result2 = result1.unionSum(emptyArray2);

        // Result should be empty
        assertEquals(result2.size(), 0);
        BlockBuilder out = ARRAY_BIGINT.createBlockBuilder(null, 1);
        result2.serialize(out);
        Block resultBlock = out.build();
        Block arrayBlock = ARRAY_BIGINT.getObject(resultBlock, 0);
        assertEquals(arrayBlock.getPositionCount(), 0);
    }

    @Test
    public void testEmptyArraySerializationDeserialization()
    {
        ArrayUnionSumStateFactory factory = new ArrayUnionSumStateFactory(BIGINT);
        ArrayUnionSumState state = factory.createSingleState();
        ArrayUnionSumStateSerializer serializer = new ArrayUnionSumStateSerializer(ARRAY_BIGINT);

        // Create empty array and set to state
        Block emptyArray = createLongsBlock(BIGINT);
        ArrayUnionSumResult result = ArrayUnionSumResult.create(BIGINT, state.getAdder(), emptyArray);
        state.set(result);

        // Serialize
        BlockBuilder out = ARRAY_BIGINT.createBlockBuilder(null, 1);
        serializer.serialize(state, out);
        Block serialized = out.build();

        // Deserialize
        ArrayUnionSumState newState = factory.createSingleState();
        serializer.deserialize(serialized, 0, newState);

        // Verify empty array is preserved
        assertNotNull(newState.get());
        assertEquals(newState.get().size(), 0);
    }

    @Test
    public void testUnionSumWithResult()
    {
        ArrayUnionSumState state = new ArrayUnionSumStateFactory(BIGINT).createSingleState();

        Block array1 = createLongsBlock(BIGINT, 1L, 2L);
        ArrayUnionSumResult result1 = ArrayUnionSumResult.create(BIGINT, state.getAdder(), array1);

        Block array2 = createLongsBlock(BIGINT, 10L, 5L);
        ArrayUnionSumResult result2 = ArrayUnionSumResult.create(BIGINT, state.getAdder(), array2);

        // Test unionSum with another result (not a block)
        ArrayUnionSumResult combined = result1.unionSum(result2);
        assertEquals(combined.size(), 2);

        BlockBuilder out = ARRAY_BIGINT.createBlockBuilder(null, 1);
        combined.serialize(out);
        Block resultBlock = out.build();
        Block arrayBlock = ARRAY_BIGINT.getObject(resultBlock, 0);

        assertEquals(BIGINT.getLong(arrayBlock, 0), 11L);
        assertEquals(BIGINT.getLong(arrayBlock, 1), 7L);
    }

    private static Block createLongsBlock(Type type, Long... values)
    {
        BlockBuilder builder = type.createBlockBuilder(null, values.length);
        for (Long value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                type.writeLong(builder, value);
            }
        }
        return builder.build();
    }

    private static Block createDoublesBlock(Double... values)
    {
        BlockBuilder builder = DOUBLE.createBlockBuilder(null, values.length);
        for (Double value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                DOUBLE.writeDouble(builder, value);
            }
        }
        return builder.build();
    }

    private static Block createRealsBlock(Float... values)
    {
        BlockBuilder builder = REAL.createBlockBuilder(null, values.length);
        for (Float value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                REAL.writeLong(builder, floatToRawIntBits(value));
            }
        }
        return builder.build();
    }
}

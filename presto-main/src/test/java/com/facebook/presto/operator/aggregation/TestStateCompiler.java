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

import com.facebook.presto.bytecode.DynamicClassLoader;
import com.facebook.presto.operator.aggregation.state.LongState;
import com.facebook.presto.operator.aggregation.state.NullableLongState;
import com.facebook.presto.operator.aggregation.state.StateCompiler;
import com.facebook.presto.operator.aggregation.state.VarianceState;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.InterleavedBlockBuilder;
import com.facebook.presto.spi.function.AccumulatorState;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.function.GroupedAccumulatorState;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.MapType;
import com.facebook.presto.type.RowType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedDoubleArray;
import static org.testng.Assert.assertEquals;

public class TestStateCompiler
{
    @Test
    public void testPrimitiveNullableLongSerialization()
    {
        AccumulatorStateFactory<NullableLongState> factory = StateCompiler.generateStateFactory(NullableLongState.class);
        AccumulatorStateSerializer<NullableLongState> serializer = StateCompiler.generateStateSerializer(NullableLongState.class);
        NullableLongState state = factory.createSingleState();
        NullableLongState deserializedState = factory.createSingleState();

        state.setLong(2);
        state.setNull(false);

        BlockBuilder builder = BIGINT.createBlockBuilder(new BlockBuilderStatus(), 2);
        serializer.serialize(state, builder);
        state.setNull(true);
        serializer.serialize(state, builder);

        Block block = builder.build();

        assertEquals(block.isNull(0), false);
        assertEquals(BIGINT.getLong(block, 0), state.getLong());
        serializer.deserialize(block, 0, deserializedState);
        assertEquals(deserializedState.getLong(), state.getLong());

        assertEquals(block.isNull(1), true);
    }

    @Test
    public void testPrimitiveLongSerialization()
    {
        AccumulatorStateFactory<LongState> factory = StateCompiler.generateStateFactory(LongState.class);
        AccumulatorStateSerializer<LongState> serializer = StateCompiler.generateStateSerializer(LongState.class);
        LongState state = factory.createSingleState();
        LongState deserializedState = factory.createSingleState();

        state.setLong(2);

        BlockBuilder builder = BIGINT.createBlockBuilder(new BlockBuilderStatus(), 1);
        serializer.serialize(state, builder);

        Block block = builder.build();

        assertEquals(BIGINT.getLong(block, 0), state.getLong());
        serializer.deserialize(block, 0, deserializedState);
        assertEquals(deserializedState.getLong(), state.getLong());
    }

    @Test
    public void testGetSerializedType()
    {
        AccumulatorStateSerializer<LongState> serializer = StateCompiler.generateStateSerializer(LongState.class);
        assertEquals(serializer.getSerializedType(), BIGINT);
    }

    @Test
    public void testPrimitiveBooleanSerialization()
    {
        AccumulatorStateFactory<BooleanState> factory = StateCompiler.generateStateFactory(BooleanState.class);
        AccumulatorStateSerializer<BooleanState> serializer = StateCompiler.generateStateSerializer(BooleanState.class);
        BooleanState state = factory.createSingleState();
        BooleanState deserializedState = factory.createSingleState();

        state.setBoolean(true);

        BlockBuilder builder = BOOLEAN.createBlockBuilder(new BlockBuilderStatus(), 1);
        serializer.serialize(state, builder);

        Block block = builder.build();
        serializer.deserialize(block, 0, deserializedState);
        assertEquals(deserializedState.isBoolean(), state.isBoolean());
    }

    @Test
    public void testPrimitiveByteSerialization()
    {
        AccumulatorStateFactory<ByteState> factory = StateCompiler.generateStateFactory(ByteState.class);
        AccumulatorStateSerializer<ByteState> serializer = StateCompiler.generateStateSerializer(ByteState.class);
        ByteState state = factory.createSingleState();
        ByteState deserializedState = factory.createSingleState();

        state.setByte((byte) 3);

        BlockBuilder builder = TINYINT.createBlockBuilder(new BlockBuilderStatus(), 1);
        serializer.serialize(state, builder);

        Block block = builder.build();
        serializer.deserialize(block, 0, deserializedState);
        assertEquals(deserializedState.getByte(), state.getByte());
    }

    @Test
    public void testNonPrimitiveSerialization()
    {
        AccumulatorStateFactory<SliceState> factory = StateCompiler.generateStateFactory(SliceState.class);
        AccumulatorStateSerializer<SliceState> serializer = StateCompiler.generateStateSerializer(SliceState.class);
        SliceState state = factory.createSingleState();
        SliceState deserializedState = factory.createSingleState();

        state.setSlice(null);
        BlockBuilder nullBlockBuilder = VARCHAR.createBlockBuilder(new BlockBuilderStatus(), 1);
        serializer.serialize(state, nullBlockBuilder);
        Block nullBlock = nullBlockBuilder.build();
        serializer.deserialize(nullBlock, 0, deserializedState);
        assertEquals(deserializedState.getSlice(), state.getSlice());

        state.setSlice(utf8Slice("test"));
        BlockBuilder builder = VARCHAR.createBlockBuilder(new BlockBuilderStatus(), 1);
        serializer.serialize(state, builder);
        Block block = builder.build();
        serializer.deserialize(block, 0, deserializedState);
        assertEquals(deserializedState.getSlice(), state.getSlice());
    }

    @Test
    public void testVarianceStateSerialization()
    {
        AccumulatorStateFactory<VarianceState> factory = StateCompiler.generateStateFactory(VarianceState.class);
        AccumulatorStateSerializer<VarianceState> serializer = StateCompiler.generateStateSerializer(VarianceState.class);
        VarianceState singleState = factory.createSingleState();
        VarianceState deserializedState = factory.createSingleState();

        singleState.setMean(1);
        singleState.setCount(2);
        singleState.setM2(3);

        BlockBuilder builder = new RowType(ImmutableList.of(BIGINT, DOUBLE, DOUBLE), Optional.empty()).createBlockBuilder(new BlockBuilderStatus(), 1);
        serializer.serialize(singleState, builder);

        Block block = builder.build();
        serializer.deserialize(block, 0, deserializedState);

        assertEquals(deserializedState.getCount(), singleState.getCount());
        assertEquals(deserializedState.getMean(), singleState.getMean());
        assertEquals(deserializedState.getM2(), singleState.getM2());
    }

    @Test
    public void testComplexSerialization()
    {
        Type arrayType = new ArrayType(BIGINT);
        Type mapType = new MapType(BIGINT, VARCHAR);
        Map<String, Type> fieldMap = ImmutableMap.of("Block", arrayType, "AnotherBlock", mapType);
        AccumulatorStateFactory<TestComplexState> factory = StateCompiler.generateStateFactory(TestComplexState.class, fieldMap, new DynamicClassLoader(TestComplexState.class.getClassLoader()));
        AccumulatorStateSerializer<TestComplexState> serializer = StateCompiler.generateStateSerializer(TestComplexState.class, fieldMap, new DynamicClassLoader(TestComplexState.class.getClassLoader()));
        TestComplexState singleState = factory.createSingleState();
        TestComplexState deserializedState = factory.createSingleState();

        singleState.setBoolean(true);
        singleState.setLong(1);
        singleState.setDouble(2.0);
        singleState.setByte((byte) 3);
        singleState.setSlice(utf8Slice("test"));
        singleState.setAnotherSlice(wrappedDoubleArray(1.0, 2.0, 3.0));
        singleState.setYetAnotherSlice(null);
        Block array = createLongsBlock(45);
        singleState.setBlock(array);

        BlockBuilder mapBlockBuilder = new InterleavedBlockBuilder(ImmutableList.of(BIGINT, VARCHAR), new BlockBuilderStatus(), 1);
        BIGINT.writeLong(mapBlockBuilder, 123L);
        VARCHAR.writeSlice(mapBlockBuilder, utf8Slice("testBlock"));
        Block map = mapBlockBuilder.build();
        singleState.setAnotherBlock(map);

        BlockBuilder builder = new RowType(ImmutableList.of(BOOLEAN, TINYINT, DOUBLE, BIGINT, mapType, VARBINARY, arrayType, VARBINARY, VARBINARY), Optional.empty())
                .createBlockBuilder(new BlockBuilderStatus(), 1);
        serializer.serialize(singleState, builder);

        Block block = builder.build();
        serializer.deserialize(block, 0, deserializedState);

        assertEquals(deserializedState.getBoolean(), singleState.getBoolean());
        assertEquals(deserializedState.getLong(), singleState.getLong());
        assertEquals(deserializedState.getDouble(), singleState.getDouble());
        assertEquals(deserializedState.getByte(), singleState.getByte());
        assertEquals(deserializedState.getSlice(), singleState.getSlice());
        assertEquals(deserializedState.getAnotherSlice(), singleState.getAnotherSlice());
        assertEquals(deserializedState.getYetAnotherSlice(), singleState.getYetAnotherSlice());
        assertEquals(deserializedState.getBlock().getLong(0, 0), singleState.getBlock().getLong(0, 0));
        assertEquals(deserializedState.getAnotherBlock().getLong(0, 0), singleState.getAnotherBlock().getLong(0, 0));
        assertEquals(deserializedState.getAnotherBlock().getSlice(1, 0, 9), singleState.getAnotherBlock().getSlice(1, 0, 9));
    }

    @Test
    public void testComplexStateEstimatedSize()
    {
        Map<String, Type> fieldMap = ImmutableMap.of("Block", new ArrayType(BIGINT), "AnotherBlock", new MapType(BIGINT, VARCHAR));
        AccumulatorStateFactory<TestComplexState> factory = StateCompiler.generateStateFactory(TestComplexState.class, fieldMap, new DynamicClassLoader(TestComplexState.class.getClassLoader()));

        TestComplexState groupedState = factory.createGroupedState();
        assertEquals(groupedState.getEstimatedSize(), 76064);
        for (int i = 0; i < 1000; i++) {
            ((GroupedAccumulatorState) groupedState).setGroupId(i);
            groupedState.setBoolean(true);
            groupedState.setLong(1);
            groupedState.setDouble(2.0);
            groupedState.setByte((byte) 3);
            groupedState.setSlice(utf8Slice("test"));
            groupedState.setAnotherSlice(wrappedDoubleArray(1.0, 2.0, 3.0));
            groupedState.setYetAnotherSlice(null);
            Block array = createLongsBlock(45);
            groupedState.setBlock(array);
            BlockBuilder mapBlockBuilder = new InterleavedBlockBuilder(ImmutableList.of(BIGINT, VARCHAR), new BlockBuilderStatus(), 1);
            BIGINT.writeLong(mapBlockBuilder, 123L);
            VARCHAR.writeSlice(mapBlockBuilder, utf8Slice("testBlock"));
            Block map = mapBlockBuilder.build();
            groupedState.setAnotherBlock(map);
            assertEquals(groupedState.getEstimatedSize(), 76064 + 1274 * (i + 1));
        }

        for (int i = 0; i < 1000; i++) {
            ((GroupedAccumulatorState) groupedState).setGroupId(i);
            groupedState.setBoolean(true);
            groupedState.setLong(1);
            groupedState.setDouble(2.0);
            groupedState.setByte((byte) 3);
            groupedState.setSlice(utf8Slice("test"));
            groupedState.setAnotherSlice(wrappedDoubleArray(1.0, 2.0, 3.0));
            groupedState.setYetAnotherSlice(null);
            Block array = createLongsBlock(45);
            groupedState.setBlock(array);
            BlockBuilder mapBlockBuilder = new InterleavedBlockBuilder(ImmutableList.of(BIGINT, VARCHAR), new BlockBuilderStatus(), 1);
            BIGINT.writeLong(mapBlockBuilder, 123L);
            VARCHAR.writeSlice(mapBlockBuilder, utf8Slice("testBlock"));
            Block map = mapBlockBuilder.build();
            groupedState.setAnotherBlock(map);
            assertEquals(groupedState.getEstimatedSize(), 76064 + 1274 * 1000);
        }
    }

    public interface TestComplexState
            extends AccumulatorState
    {
        double getDouble();

        void setDouble(double value);

        boolean getBoolean();

        void setBoolean(boolean value);

        long getLong();

        void setLong(long value);

        byte getByte();

        void setByte(byte value);

        Slice getSlice();

        void setSlice(Slice slice);

        Slice getAnotherSlice();

        void setAnotherSlice(Slice slice);

        Slice getYetAnotherSlice();

        void setYetAnotherSlice(Slice slice);

        Block getBlock();

        void setBlock(Block block);

        Block getAnotherBlock();

        void setAnotherBlock(Block block);
    }

    public interface BooleanState
            extends AccumulatorState
    {
        boolean isBoolean();

        void setBoolean(boolean value);
    }

    public interface ByteState
            extends AccumulatorState
    {
        byte getByte();

        void setByte(byte value);
    }

    public interface SliceState
            extends AccumulatorState
    {
        Slice getSlice();

        void setSlice(Slice slice);
    }
}

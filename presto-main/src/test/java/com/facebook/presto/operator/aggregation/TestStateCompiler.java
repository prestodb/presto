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

import com.facebook.presto.operator.aggregation.state.LongState;
import com.facebook.presto.operator.aggregation.state.NullableLongState;
import com.facebook.presto.operator.aggregation.state.StateCompiler;
import com.facebook.presto.operator.aggregation.state.VarianceState;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.function.AccumulatorState;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.VarcharType;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
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

        BlockBuilder builder = BooleanType.BOOLEAN.createBlockBuilder(new BlockBuilderStatus(), 1);
        serializer.serialize(state, builder);

        Block block = builder.build();
        serializer.deserialize(block, 0, deserializedState);
        assertEquals(deserializedState.getBoolean(), state.getBoolean());
    }

    @Test
    public void testPrimitiveByteSerialization()
    {
        AccumulatorStateFactory<ByteState> factory = StateCompiler.generateStateFactory(ByteState.class);
        AccumulatorStateSerializer<ByteState> serializer = StateCompiler.generateStateSerializer(ByteState.class);
        ByteState state = factory.createSingleState();
        ByteState deserializedState = factory.createSingleState();

        state.setByte((byte) 3);

        BlockBuilder builder = BIGINT.createBlockBuilder(new BlockBuilderStatus(), 1);
        serializer.serialize(state, builder);

        Block block = builder.build();
        serializer.deserialize(block, 0, deserializedState);
        assertEquals(deserializedState.getByte(), state.getByte());
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

        BlockBuilder builder = VarcharType.VARCHAR.createBlockBuilder(new BlockBuilderStatus(), 1);
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
        AccumulatorStateFactory<TestComplexState> factory = StateCompiler.generateStateFactory(TestComplexState.class);
        AccumulatorStateSerializer<TestComplexState> serializer = StateCompiler.generateStateSerializer(TestComplexState.class);
        TestComplexState singleState = factory.createSingleState();
        TestComplexState deserializedState = factory.createSingleState();

        singleState.setBoolean(true);
        singleState.setLong(1);
        singleState.setDouble(2.0);
        singleState.setByte((byte) 3);

        BlockBuilder builder = VarcharType.VARCHAR.createBlockBuilder(new BlockBuilderStatus(), 1);
        serializer.serialize(singleState, builder);

        Block block = builder.build();
        serializer.deserialize(block, 0, deserializedState);

        assertEquals(deserializedState.getBoolean(), singleState.getBoolean());
        assertEquals(deserializedState.getLong(), singleState.getLong());
        assertEquals(deserializedState.getDouble(), singleState.getDouble());
        assertEquals(deserializedState.getByte(), singleState.getByte());
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
    }

    public interface BooleanState
            extends AccumulatorState
    {
        boolean getBoolean();

        void setBoolean(boolean value);
    }

    public interface ByteState
            extends AccumulatorState
    {
        byte getByte();

        void setByte(byte value);
    }
}

package com.facebook.presto.operator.aggregation.reservoirsample;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.aggregation.arrayagg.ArrayAggregationState;
import com.facebook.presto.operator.aggregation.state.SetAggregationState;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;

public class ReservoirSampleStateSerializer implements AccumulatorStateSerializer<ReservoirSampleState> {

    private final Type elementType;
    private final Type arrayType;

    public ReservoirSampleStateSerializer(Type elementType)
    {
        this.elementType = elementType;
        this.arrayType = new ArrayType(elementType);
    }

    @Override
    public Type getSerializedType()
    {
        return arrayType;
    }

    @Override
    public void serialize(ReservoirSampleState state, BlockBuilder out)
    {
        Block[] samples = state.getSamples();
        if (state.isEmpty()) {
            out.appendNull();
        }
        else {
            BlockBuilder entryBuilder = out.beginBlockEntry();
            for (int i = 0; i < samples.length; i++) {
                elementType.appendTo(samples[i], 0, entryBuilder);
            }
            out.closeEntry();
        }

    }

    @Override
    public void deserialize(Block block, int index, ReservoirSampleState state)
    {
        state.reset();
        Block stateBlock = (Block) arrayType.getObject(block, index);
        for (int i = 0; i < stateBlock.getPositionCount(); i++) {
            state.add(stateBlock, i);
        }
    }
}

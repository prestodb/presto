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
package com.facebook.presto.operator.aggregation.reservoirsample;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.RowType.Field;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.function.TypeParameter;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.IntegerType.INTEGER;

public class ReservoirSampleStateSerializer
        implements AccumulatorStateSerializer<ReservoirSampleState>
{
    private final Type elementType;
    private final Type arrayType;

    public ReservoirSampleStateSerializer(@TypeParameter("T") Type elementType)
    {
        this.elementType = elementType;
        this.arrayType = new ArrayType(elementType);
    }

    @Override
    public Type getSerializedType()
    {
        Field initialSample = new Field(Optional.of("initialSample"), arrayType);
        Field initialSeenCount = new Field(Optional.of("initialSeenCount"), BIGINT);
        Field seenCount = new Field(Optional.of("seenCount"), BIGINT);
        Field maxSampleSize = new Field(Optional.of("maxSampleSize"), INTEGER);
        Field sample = new Field(Optional.of("sample"), arrayType);
        List<Field> fields = Arrays.asList(initialSample, initialSeenCount, seenCount, maxSampleSize, sample);
        return RowType.from(fields);
    }

    @Override
    public void serialize(ReservoirSampleState state, BlockBuilder out)
    {
        if (state.get() == null) {
            out.appendNull();
        }
        else {
            BlockBuilder entryBuilder = out.beginBlockEntry();
            state.get().serialize(entryBuilder);
            out.closeEntry();
        }
    }

    @Override
    public void deserialize(Block block, int index, ReservoirSampleState state)
    {
        if (block.isNull(index)) {
            state.set(null);
            return;
        }
        Type rowTypes = getSerializedType();
        Block stateBlock = (Block) rowTypes.getObject(block, index);
        Block initialSample = (Block) arrayType.getObject(stateBlock, 0);
        long initialSeenCount = stateBlock.getLong(1);
        long seenCount = stateBlock.getLong(2);
        int maxSampleSize = stateBlock.getInt(3);
        Block samplesBlock = (Block) arrayType.getObject(stateBlock, 4);
        ReservoirSample reservoirSample = new ReservoirSample(elementType, seenCount, maxSampleSize, samplesBlock, initialSample, initialSeenCount);
        state.set(reservoirSample);
    }
}

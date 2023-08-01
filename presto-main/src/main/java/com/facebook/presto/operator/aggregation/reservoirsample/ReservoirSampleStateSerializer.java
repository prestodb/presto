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

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.IntegerType.INTEGER;

public class ReservoirSampleStateSerializer
        implements AccumulatorStateSerializer<ReservoirSampleState>
{
    private final Type elementType;
    private final Type arrayType;

    public ReservoirSampleStateSerializer(Type elementType)
    {
//        assert elementType instanceof RowType;
//        RowType sampleRows = (RowType) elementType;
//        List<Field> sampleFields = sampleRows.getFields();
//        Field count = new Field(Optional.of("rowCount"), INTEGER);
//        Field length = new Field(Optional.of("sampleLength"), INTEGER);
//        sampleFields.add(count);
//        sampleFields.add(length);
        this.elementType = elementType;
        this.arrayType = new ArrayType(elementType);
    }

    @Override
    public Type getSerializedType()
    {
        Field count = new Field(Optional.of("count"), INTEGER);
        Field length = new Field(Optional.of("sampleLength"), INTEGER);
        Field sample = new Field(Optional.of("sample"), arrayType);
        List<Field> fields = Arrays.asList(count, length, sample);
        return RowType.from(fields);
    }

    @Override
    public void serialize(ReservoirSampleState state, BlockBuilder out)
    {
        if (state.isEmpty()) {
            out.appendNull();
        }
        else {
            BlockBuilder entryBuilder = out.beginBlockEntry();
            state.serialize(entryBuilder);
            out.closeEntry();
        }
    }

    @Override
    public void deserialize(Block block, int index, ReservoirSampleState state)
    {
        Type rowTypes = getSerializedType();
        Block stateBlock = (Block) rowTypes.getObject(block, index);
        int seenCount = stateBlock.getInt(0);
        int sampleLength = stateBlock.getInt(1);
        Block samplesBlock = (Block) arrayType.getObject(stateBlock, 2);
        Block[] samples = new Block[sampleLength];
        for (int i = 0; i < sampleLength; i++) {
            BlockBuilder elementBlock = elementType.createBlockBuilder(null, 16);
            elementType.appendTo(samplesBlock, i, elementBlock);
            samples[i] = elementBlock.build();
        }
        ReservoirSample reservoirSample = new ReservoirSample(seenCount, samples, elementType);
        state.setSample(reservoirSample);
    }
}

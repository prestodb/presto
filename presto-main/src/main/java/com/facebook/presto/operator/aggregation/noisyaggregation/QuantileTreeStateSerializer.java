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
package com.facebook.presto.operator.aggregation.noisyaggregation;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.aggregation.noisyaggregation.sketch.quantiletree.QuantileTree;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;

public class QuantileTreeStateSerializer
        implements AccumulatorStateSerializer<QuantileTreeState>
{
    @Override
    public Type getSerializedType()
    {
        return VARBINARY;
    }

    @Override
    public void serialize(QuantileTreeState state, BlockBuilder out)
    {
        if (state.getQuantileTree() == null) {
            out.appendNull();
        }
        else {
            Slice treeSlice = state.getQuantileTree().serialize();
            int sizeInBytes = 2 * SIZE_OF_DOUBLE + // epsilon, delta
                    SIZE_OF_INT + // probability array size
                    state.getProbabilities().size() * SIZE_OF_DOUBLE + // probabilities
                    treeSlice.length();
            DynamicSliceOutput output = new DynamicSliceOutput(sizeInBytes);
            output.appendDouble(state.getEpsilon());
            output.appendDouble(state.getDelta());

            // write probabilities
            List<Double> probabilities = state.getProbabilities();
            output.appendInt(probabilities.size());
            for (Double probability : probabilities) {
                output.appendDouble(probability);
            }

            output.appendBytes(treeSlice);
            VARBINARY.writeSlice(out, output.slice());
        }
    }

    @Override
    public void deserialize(Block block, int index, QuantileTreeState state)
    {
        Slice stateSlice = VARBINARY.getSlice(block, index);
        BasicSliceInput input = stateSlice.getInput();
        state.setEpsilon(input.readDouble());
        state.setDelta(input.readDouble());

        // read probabilities
        List<Double> probabilities = new ArrayList<>();
        int numProbabilities = input.readInt();
        for (int i = 0; i < numProbabilities; i++) {
            probabilities.add(input.readDouble());
        }
        state.setProbabilities(probabilities);
        Slice treeSlice = input.slice();
        state.setQuantileTree(QuantileTree.deserialize(treeSlice));
    }
}

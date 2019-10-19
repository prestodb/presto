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
package com.facebook.presto.operator.aggregation.differentialmutualinformationclassification;

import com.facebook.presto.operator.aggregation.differentialentropy.DifferentialEntropyStateStrategy;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.SizeOf;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.google.common.base.Verify.verify;

public class DifferentialMutualInformationStateSerializer
        implements AccumulatorStateSerializer<DifferentialMutualInformationClassificationState>
{
    @Override
    public Type getSerializedType()
    {
        return VARBINARY;
    }

    @Override
    public void serialize(DifferentialMutualInformationClassificationState state, BlockBuilder output)
    {
        DifferentialEntropyStateStrategy strategy = state.getFeatureStrategy();
        int requiredBytes = DifferentialEntropyStateStrategy.getRequiredBytesForSerialization(strategy);
        requiredBytes += SizeOf.SIZE_OF_INT;
        if (state.getFeatureStrategiesForOutcomes() != null) {
            for (DifferentialEntropyStateStrategy outcomeStrategy : state.getFeatureStrategiesForOutcomes().values()) {
                requiredBytes += SizeOf.SIZE_OF_INT +
                        DifferentialEntropyStateStrategy.getRequiredBytesForSerialization(outcomeStrategy);
            }
        }
        SliceOutput sliceOut = Slices.allocate(requiredBytes).getOutput();
        DifferentialEntropyStateStrategy.serialize(strategy, sliceOut);
        if (state.getFeatureStrategiesForOutcomes() == null) {
            sliceOut.writeInt(0);
        }
        else {
            sliceOut.writeInt(state.getFeatureStrategiesForOutcomes().size());
            for (Map.Entry<Integer, DifferentialEntropyStateStrategy> entry : state.getFeatureStrategiesForOutcomes().entrySet()) {
                sliceOut.writeInt(entry.getKey());
                DifferentialEntropyStateStrategy.serialize(entry.getValue(), sliceOut);
            }
        }
        VARBINARY.writeSlice(output, sliceOut.getUnderlyingSlice());
    }

    @Override
    public void deserialize(
            Block block,
            int index,
            DifferentialMutualInformationClassificationState state)
    {
        SliceInput input = VARBINARY.getSlice(block, index).getInput();
        DifferentialEntropyStateStrategy strategy = DifferentialEntropyStateStrategy.deserialize(input);
        if (strategy != null) {
            state.setFeatureStrategy(strategy);
        }
        int numOutcomes = input.readInt();
        if (numOutcomes == 0) {
            return;
        }
        Map<Integer, DifferentialEntropyStateStrategy> outcomeStrategies = new HashMap<>();
        for (int i = 0; i < numOutcomes; ++i) {
            int outcome = input.readInt();
            DifferentialEntropyStateStrategy outcomeStrategy = DifferentialEntropyStateStrategy.deserialize(input);
            verify(outcomeStrategies.put(outcome, outcomeStrategy) == null, "outcomes must be unique");
        }
        state.setFeatureStrategiesForOutcomes(outcomeStrategies);
    }
}

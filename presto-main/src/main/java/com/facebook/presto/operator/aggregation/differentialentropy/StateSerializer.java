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
package com.facebook.presto.operator.aggregation.differentialentropy;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.SizeOf;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import java.security.InvalidParameterException;

import static com.facebook.presto.spi.StandardErrorCode.CONSTRAINT_VIOLATION;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;

/*
Serializes sample-entropy states. */
public class StateSerializer
        implements AccumulatorStateSerializer<State>
{
    public static StateStrategy create(
            long size,
            boolean hasWeight)
    {
        return hasWeight ?
                new WeightedReservoirSampleStateStrategy(size) :
                new UnweightedReservoirSampleStateStrategy(size);
    }

    public static StateStrategy create(
            long size,
            String method)
    {
        if (method == "reservoir_vasicek") {
            return new UnweightedReservoirSampleStateStrategy(size);
        }

        throw new InvalidParameterException(String.format("Unsupported"));
    }

    public static StateStrategy create(
            long size,
            String method,
            double min,
            double max)
    {
        if (method.equalsIgnoreCase("fixed_histogram_mle")) {
            return new FixedHistogramMLEStateStrategy(size, min, max);
        }

        if (method.equalsIgnoreCase("fixed_histogram_jacknife")) {
            return new FixedHistogramJacknifeStateStrategy(size, min, max);
        }

        throw new InvalidParameterException(String.format("unknown method %s or method/argument mismatch", method));
    }

    public static void validate(StateStrategy strategy, String method)
    {
        if (method.equalsIgnoreCase("reservoir_vasicek")) {
            if (!(strategy instanceof UnweightedReservoirSampleStateStrategy)) {
                throw new PrestoException(
                        INVALID_FUNCTION_ARGUMENT,
                        "Inconsistent method");
            }

            return;
        }

        if (method.equalsIgnoreCase("weighted_reservoir")) {
            if (!(strategy instanceof WeightedReservoirSampleStateStrategy)) {
                throw new PrestoException(
                        INVALID_FUNCTION_ARGUMENT,
                        "Inconsistent method");
            }

            return;
        }

        if (method.equalsIgnoreCase("fixed_histogram_mle")) {
            if (!(strategy instanceof FixedHistogramMLEStateStrategy)) {
                throw new PrestoException(
                        INVALID_FUNCTION_ARGUMENT,
                        "Inconsistent method");
            }

            return;
        }

        if (method.equalsIgnoreCase("fixed_histogram_jacknife")) {
            if (!(strategy instanceof FixedHistogramJacknifeStateStrategy)) {
                throw new PrestoException(
                        INVALID_FUNCTION_ARGUMENT,
                        "Inconsistent method");
            }

            return;
        }
        throw new InvalidParameterException("unknown method");
    }

    public static void validate(StateStrategy strategy)
    {
        if (strategy instanceof UnweightedReservoirSampleStateStrategy) {
            return;
        }

        throw new InvalidParameterException(String.format("Unsupported"));
    }

    public static void combine(StateStrategy target, StateStrategy source)
    {
        if (target instanceof UnweightedReservoirSampleStateStrategy) {
            if (!(source instanceof UnweightedReservoirSampleStateStrategy)) {
                throw new PrestoException(
                        INVALID_FUNCTION_ARGUMENT,
                        "Inconsistent strategy");
            }
            ((UnweightedReservoirSampleStateStrategy) target).mergeWith((UnweightedReservoirSampleStateStrategy) source);
            return;
        }

        if (target instanceof WeightedReservoirSampleStateStrategy) {
            if (!(source instanceof WeightedReservoirSampleStateStrategy)) {
                throw new PrestoException(
                        INVALID_FUNCTION_ARGUMENT,
                        "Inconsistent strategy");
            }
            ((WeightedReservoirSampleStateStrategy) target).mergeWith((WeightedReservoirSampleStateStrategy) source);
            return;
        }

        if (target instanceof FixedHistogramMLEStateStrategy) {
            if (!(source instanceof FixedHistogramMLEStateStrategy)) {
                throw new PrestoException(
                        INVALID_FUNCTION_ARGUMENT,
                        "Inconsistent strategy");
            }
            ((FixedHistogramMLEStateStrategy) target).mergeWith((FixedHistogramMLEStateStrategy) source);
            return;
        }

        if (target instanceof FixedHistogramJacknifeStateStrategy) {
            if (!(source instanceof FixedHistogramJacknifeStateStrategy)) {
                throw new PrestoException(
                        INVALID_FUNCTION_ARGUMENT,
                        "Inconsistent strategy");
            }
            ((FixedHistogramJacknifeStateStrategy) target).mergeWith((FixedHistogramJacknifeStateStrategy) source);
            return;
        }

        throw new InvalidParameterException("unknown strategy combination");
    }

    @Override
    public Type getSerializedType()
    {
        return VARBINARY;
    }

    @Override
    public void serialize(State state, BlockBuilder out)
    {
        final int requiredBytes =
                SizeOf.SIZE_OF_INT + // Method
                        (state.getStrategy() == null ? 0 : state.getStrategy().getRequiredBytesForSerialization());

        final StateStrategy strategy = state.getStrategy();

        SliceOutput sliceOut = Slices.allocate(requiredBytes).getOutput();

        if (strategy == null) {
            sliceOut.appendInt(0);
        }
        else if (strategy instanceof UnweightedReservoirSampleStateStrategy) {
            sliceOut.appendInt(1);
        }
        else if (strategy instanceof WeightedReservoirSampleStateStrategy) {
            sliceOut.appendInt(2);
        }
        else if (strategy instanceof FixedHistogramMLEStateStrategy) {
            sliceOut.appendInt(3);
        }
        else if (strategy instanceof FixedHistogramJacknifeStateStrategy) {
            sliceOut.appendInt(4);
        }
        else {
            throw new InvalidParameterException("unknown method in serialize");
        }

        if (strategy != null) {
            strategy.serialize(sliceOut);
        }

        VARBINARY.writeSlice(out, sliceOut.getUnderlyingSlice());
    }

    @Override
    public void deserialize(
            Block block,
            int index,
            State state)
    {
        StateSerializer.deserialize(
                VARBINARY.getSlice(block, index).getInput(),
                state);
    }

    public static void deserialize(
            SliceInput input,
            State state)
    {
        final StateStrategy strategy =
                StateSerializer.deserializeStrategy(input);
        if (strategy == null && state.getStrategy() != null) {
            throw new PrestoException(
                    CONSTRAINT_VIOLATION,
                    "strategy is not null for null method");
        }
        if (strategy != null) {
            state.setStrategy(strategy);
        }
    }

    public static StateStrategy deserializeStrategy(SliceInput input)
    {
        final int method = input.readInt();
        if (method == 0) {
            return null;
        }
        if (method == 1) {
            return new UnweightedReservoirSampleStateStrategy(input);
        }
        if (method == 2) {
            return new WeightedReservoirSampleStateStrategy(input);
        }
        if (method == 3) {
            return new FixedHistogramMLEStateStrategy(input);
        }
        if (method == 4) {
            return new FixedHistogramJacknifeStateStrategy(input);
        }
        throw new InvalidParameterException("unknown method in deserialize");
    }
}

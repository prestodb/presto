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

public class ReservoirSampleStateSerializer
        implements AccumulatorStateSerializer<ReservoirSampleState>
{
    public static AbstractReservoirSample create(long size, Double weight)
    {
        return weight == null ?
                new ReservoirSample(size) :
                new WeightedReservoirSample(size);
    }

    public static void combine(AbstractReservoirSample target, AbstractReservoirSample source)
    {
        if (target instanceof ReservoirSample) {
            if (!(source instanceof ReservoirSample)) {
                throw new PrestoException(
                        INVALID_FUNCTION_ARGUMENT,
                        "Inconsistent sample");
            }
            ((ReservoirSample) target).mergeWith((ReservoirSample) source);
            return;
        }

        if (target instanceof WeightedReservoirSample) {
            if (!(source instanceof WeightedReservoirSample)) {
                throw new PrestoException(
                        INVALID_FUNCTION_ARGUMENT,
                        "Inconsistent sample");
            }
            ((WeightedReservoirSample) target).mergeWith((WeightedReservoirSample) source);
            return;
        }

        throw new InvalidParameterException("unknown sample combination");
    }

    @Override
    public Type getSerializedType()
    {
        return VARBINARY;
    }

    @Override
    public void serialize(ReservoirSampleState state, BlockBuilder out)
    {
        AbstractReservoirSample sample = state.getSample();

        int requiredBytes = SizeOf.SIZE_OF_BYTE; // weighted
        if (sample != null) {
            requiredBytes += state.getSample().getRequiredBytesForSerialization();
        }

        SliceOutput sliceOut = Slices.allocate(requiredBytes).getOutput();

        if (sample == null) {
            sliceOut.appendByte(0);
        }
        else if (sample instanceof ReservoirSample) {
            sliceOut.appendInt(1);
        }
        else if (sample instanceof WeightedReservoirSample) {
            sliceOut.appendInt(2);
        }
        else {
            throw new InvalidParameterException("unknown method in serialize");
        }

        if (sample != null) {
            // sample.serialize(sliceOut);
        }

        VARBINARY.writeSlice(out, sliceOut.getUnderlyingSlice());
    }

    @Override
    public void deserialize(
            Block block,
            int index,
            ReservoirSampleState state)
    {
        ReservoirSampleStateSerializer.deserialize(
                VARBINARY.getSlice(block, index).getInput(),
                state);
    }

    public static void deserialize(
            SliceInput input,
            ReservoirSampleState state)
    {
        final AbstractReservoirSample sample =
                ReservoirSampleStateSerializer.deserialize(input);
        if (sample == null && state.getSample() != null) {
            throw new PrestoException(
                    CONSTRAINT_VIOLATION,
                    "sample is not null for null method");
        }
        if (sample != null) {
            state.setSample(sample);
        }
    }

    public static AbstractReservoirSample deserialize(SliceInput input)
    {
        final int method = input.readInt();
        System.out.println("des method " + method);
        if (method == 0) {
            return null;
        }
        if (method == 1) {
            return new ReservoirSample(input);
        }
        if (method == 2) {
            return new WeightedReservoirSample(input);
        }
        throw new InvalidParameterException("unknown method in deserialize");
    }
}

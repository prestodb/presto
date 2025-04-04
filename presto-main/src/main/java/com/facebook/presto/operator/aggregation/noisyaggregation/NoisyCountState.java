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

import com.facebook.presto.operator.aggregation.state.InitialBooleanValue;
import com.facebook.presto.spi.function.AccumulatorState;
import com.facebook.presto.spi.function.AccumulatorStateMetadata;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;

@AccumulatorStateMetadata(stateSerializerClass = NoisyCountStateSerializer.class)
public interface NoisyCountState
        extends AccumulatorState
{
    long getCount();

    void setCount(long value);

    double getNoiseScale();

    void setNoiseScale(double value);

    @InitialBooleanValue(true)
    boolean isNullRandomSeed();

    void setNullRandomSeed(boolean value);

    long getRandomSeed();

    void setRandomSeed(long value);

    static int calculateSerializationCapacity()
    {
        return SIZE_OF_LONG + // count
                SIZE_OF_DOUBLE + // noiseScale
                SIZE_OF_BYTE + // isNullRandomSeed
                SIZE_OF_LONG; // randomSeed
    }

    static void writeToSerializer(NoisyCountState state, SliceOutput output)
    {
        output.appendLong(state.getCount());
        output.appendDouble(state.getNoiseScale());
        output.appendByte(state.isNullRandomSeed() ? 1 : 0);
        output.appendLong(state.getRandomSeed());
    }

    static void readFromSerializer(NoisyCountState state, SliceInput input)
    {
        state.setCount(input.readLong());
        state.setNoiseScale(input.readDouble());
        state.setNullRandomSeed(input.readByte() == 1);
        state.setRandomSeed(input.readLong());
    }
}

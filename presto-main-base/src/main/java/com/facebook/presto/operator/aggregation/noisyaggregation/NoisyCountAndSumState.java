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
import com.facebook.presto.spi.function.AccumulatorStateMetadata;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;

/**
 * State class for noisy sum and noisy avg aggregations.
 * This class extends noisy count state to store the number of input rows explicitly.
 * That count is used to compute noisy average.
 * In the case of noisy sum, if there is no input rows, NULL should be returned
 * and that count is used to check that condition.
 */
@AccumulatorStateMetadata(stateSerializerClass = NoisyCountAndSumStateSerializer.class)
public interface NoisyCountAndSumState
        extends NoisyCountState
{
    @InitialBooleanValue(true)
    boolean isNullLowerUpper();

    void setNullLowerUpper(boolean value);

    double getLower();

    void setLower(double value);

    double getUpper();

    void setUpper(double value);

    double getSum();

    void setSum(double value);

    static int calculateSerializationCapacity()
    {
        return NoisyCountState.calculateSerializationCapacity() +
                SIZE_OF_BYTE + // isNullLowerUpper
                SIZE_OF_DOUBLE + // lower
                SIZE_OF_DOUBLE + // upper
                SIZE_OF_DOUBLE; // sum
    }

    static void writeToSerializer(NoisyCountAndSumState state, SliceOutput output)
    {
        NoisyCountState.writeToSerializer(state, output);

        output.appendByte(state.isNullLowerUpper() ? 1 : 0);
        output.appendDouble(state.getLower());
        output.appendDouble(state.getUpper());

        output.appendDouble(state.getSum());
    }

    static void readFromSerializer(NoisyCountAndSumState state, SliceInput input)
    {
        NoisyCountState.readFromSerializer(state, input);

        state.setNullLowerUpper(input.readByte() == 1);
        state.setLower(input.readDouble());
        state.setUpper(input.readDouble());

        state.setSum(input.readDouble());
    }
}

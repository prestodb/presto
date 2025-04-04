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

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.operator.aggregation.noisyaggregation.sketch.SfmSketch;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.type.SfmSketchType;
import io.airlift.slice.Slice;

import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

@AggregationFunction("merge")
public final class SfmSketchMergeAggregation
{
    private SfmSketchMergeAggregation() {}

    @InputFunction
    public static void input(@AggregationState SfmSketchState state, @SqlType(SfmSketchType.NAME) Slice value)
    {
        SfmSketch sketch = SfmSketch.deserialize(value);
        SfmSketch previous = state.getSketch();
        if (previous == null) {
            // if state sketch is empty, add this sketch to the state
            state.setSketch(sketch);
            state.setEpsilon(0); // not used
        }
        else {
            // if state already has a sketch, merge in the current sketch
            try {
                // throws if the sketches are incompatible (e.g., different bucket count/size)
                state.mergeSketch(sketch);
            }
            catch (IllegalArgumentException e) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e.getMessage(), e);
            }
        }
    }

    @CombineFunction
    public static void combine(@AggregationState SfmSketchState state, @AggregationState SfmSketchState otherState)
    {
        SfmSketchAggregationUtils.mergeStates(state, otherState);
    }

    @OutputFunction(SfmSketchType.NAME)
    public static void output(@AggregationState SfmSketchState state, BlockBuilder out)
    {
        VARBINARY.writeSlice(out, state.getSketch().serialize());
    }
}

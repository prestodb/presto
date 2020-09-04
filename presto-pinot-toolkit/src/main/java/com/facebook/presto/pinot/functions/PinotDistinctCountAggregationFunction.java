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
package com.facebook.presto.pinot.functions;

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.function.AccumulatorState;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import io.airlift.slice.Slice;

@Description("Pinot UDF: A distinct count function, should always be pushed down to Pinot")
@AggregationFunction("distinctCount")
public class PinotDistinctCountAggregationFunction
{
    private PinotDistinctCountAggregationFunction()
    {
    }

    @InputFunction
    @TypeParameter("T")
    public static void input(AccumulatorState state, @SqlType("T") Slice value)
    {
        throw new IllegalStateException("Expected 'distinctCount' function to be always pushed down to Pinot.");
    }

    @CombineFunction
    public static void combine(AccumulatorState state, AccumulatorState otherState)
    {
        throw new IllegalStateException("Expected 'distinctCount' function to be always pushed down to Pinot.");
    }

    @OutputFunction(StandardTypes.BIGINT)
    public static void output(AccumulatorState state, BlockBuilder out)
    {
        throw new IllegalStateException("Expected 'distinctCount' function to be always pushed down to Pinot.");
    }
}

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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.FloatType;
import com.facebook.presto.type.SqlType;

import java.util.Map;

import static com.facebook.presto.spi.type.StandardTypes.BIGINT;
import static com.facebook.presto.spi.type.StandardTypes.DOUBLE;
import static com.facebook.presto.spi.type.StandardTypes.FLOAT;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;

@AggregationFunction("numeric_histogram")
public class FloatHistogramAggregation
{
    private FloatHistogramAggregation()
    {
    }

    @InputFunction
    public static void add(DoubleHistogramAggregation.State state, @SqlType(BIGINT) long buckets, @SqlType(FLOAT) long value, @SqlType(DOUBLE) double weight)
    {
        DoubleHistogramAggregation.add(state, buckets, intBitsToFloat((int) value), weight);
    }

    @InputFunction
    public static void add(DoubleHistogramAggregation.State state, @SqlType(BIGINT) long buckets, @SqlType(FLOAT) long value)
    {
        add(state, buckets, value, 1);
    }

    @CombineFunction
    public static void merge(DoubleHistogramAggregation.State state, DoubleHistogramAggregation.State other)
    {
        DoubleHistogramAggregation.merge(state, other);
    }

    @OutputFunction("map(float,float)")
    public static void output(DoubleHistogramAggregation.State state, BlockBuilder out)
    {
        if (state.get() == null) {
            out.appendNull();
        }
        else {
            Map<Double, Double> value = state.get().getBuckets();
            BlockBuilder blockBuilder = FloatType.FLOAT.createBlockBuilder(new BlockBuilderStatus(), value.size() * 2);
            for (Map.Entry<Double, Double> entry : value.entrySet()) {
                FloatType.FLOAT.writeLong(blockBuilder, floatToRawIntBits(entry.getKey().floatValue()));
                FloatType.FLOAT.writeLong(blockBuilder, floatToRawIntBits(entry.getValue().floatValue()));
            }
            Block block = blockBuilder.build();
            out.writeObject(block);
            out.closeEntry();
        }
    }
}

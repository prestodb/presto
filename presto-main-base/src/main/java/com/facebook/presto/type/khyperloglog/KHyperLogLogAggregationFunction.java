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

package com.facebook.presto.type.khyperloglog;

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import io.airlift.slice.Slice;
import io.airlift.slice.XxHash64;

@AggregationFunction("khyperloglog_agg")
public final class KHyperLogLogAggregationFunction
{
    private static final KHyperLogLogStateSerializer SERIALIZER = new KHyperLogLogStateSerializer();

    private KHyperLogLogAggregationFunction() {}

    @InputFunction
    public static void input(@AggregationState KHyperLogLogState state, @SqlType(StandardTypes.BIGINT) long value, @SqlType(StandardTypes.BIGINT) long uii)
    {
        if (state.getKHLL() == null) {
            state.setKHLL(new KHyperLogLog());
        }
        state.getKHLL().add(value, uii);
    }

    @InputFunction
    @LiteralParameters("x")
    public static void input(@AggregationState KHyperLogLogState state, @SqlType("varchar(x)") Slice value, @SqlType(StandardTypes.BIGINT) long uii)
    {
        if (state.getKHLL() == null) {
            state.setKHLL(new KHyperLogLog());
        }
        state.getKHLL().add(value, uii);
    }

    @InputFunction
    public static void input(@AggregationState KHyperLogLogState state, @SqlType(StandardTypes.DOUBLE) double value, @SqlType(StandardTypes.BIGINT) long uii)
    {
        input(state, Double.doubleToLongBits(value), uii);
    }

    @InputFunction
    @LiteralParameters("x")
    public static void input(@AggregationState KHyperLogLogState state, @SqlType(StandardTypes.BIGINT) long value, @SqlType("varchar(x)") Slice uii)
    {
        input(state, value, XxHash64.hash(uii));
    }

    @InputFunction
    @LiteralParameters({"x", "y"})
    public static void input(@AggregationState KHyperLogLogState state, @SqlType("varchar(x)") Slice value, @SqlType("varchar(y)") Slice uii)
    {
        input(state, value, XxHash64.hash(uii));
    }

    @InputFunction
    @LiteralParameters("x")
    public static void input(@AggregationState KHyperLogLogState state, @SqlType(StandardTypes.DOUBLE) double value, @SqlType("varchar(x)") Slice uii)
    {
        input(state, Double.doubleToLongBits(value), XxHash64.hash(uii));
    }

    @CombineFunction
    public static void combine(@AggregationState KHyperLogLogState state, @AggregationState KHyperLogLogState otherState)
    {
        if (state.getKHLL() == null) {
            KHyperLogLog copy = new KHyperLogLog();
            copy.mergeWith(otherState.getKHLL());
            state.setKHLL(copy);
        }
        else {
            state.getKHLL().mergeWith(otherState.getKHLL());
        }
    }

    @OutputFunction(KHyperLogLogType.NAME)
    public static void output(@AggregationState KHyperLogLogState state, BlockBuilder out)
    {
        SERIALIZER.serialize(state, out);
    }
}

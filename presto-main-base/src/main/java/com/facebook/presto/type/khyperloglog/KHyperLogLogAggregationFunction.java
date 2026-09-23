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
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import io.airlift.slice.Slice;
import io.airlift.slice.XxHash64;

import static com.facebook.presto.common.type.StandardTypes.BIGINT;
import static com.facebook.presto.common.type.StandardTypes.DOUBLE;
import static com.facebook.presto.common.type.StandardTypes.K_HYPER_LOG_LOG;

/**
 * The {@code khyperloglog_agg_java_compat} alias exists for cross-engine
 * portability. Velox's {@code khyperloglog_agg} hashes its inputs differently
 * from this implementation, so sketches built by the two engines are not byte
 * compatible. Velox therefore registers a second aggregate,
 * {@code khyperloglog_agg_java_compat}, that reproduces this implementation's
 * hashing exactly.
 *
 * <p>Registering the same name here lets a single query resolve and produce
 * identical sketches on both engines. On a native cluster the coordinator still
 * resolves the name against this registry before dispatching execution to the
 * worker, so the alias is required there too, not only when the query runs on
 * Java. It is deliberately an alias rather than a second implementation.
 */
@AggregationFunction(value = "khyperloglog_agg", alias = "khyperloglog_agg_java_compat")
public final class KHyperLogLogAggregationFunction
{
    private static final KHyperLogLogStateSerializer SERIALIZER = new KHyperLogLogStateSerializer();

    private KHyperLogLogAggregationFunction() {}

    @InputFunction
    public static void input(@AggregationState KHyperLogLogState state, @SqlType(BIGINT) long value, @SqlType(BIGINT) long uii)
    {
        if (state.getKHLL() == null) {
            state.setKHLL(new KHyperLogLog());
        }
        state.getKHLL().add(value, uii);
    }

    @InputFunction
    @LiteralParameters("x")
    public static void input(@AggregationState KHyperLogLogState state, @SqlType("varchar(x)") Slice value, @SqlType(BIGINT) long uii)
    {
        if (state.getKHLL() == null) {
            state.setKHLL(new KHyperLogLog());
        }
        state.getKHLL().add(value, uii);
    }

    @InputFunction
    public static void input(@AggregationState KHyperLogLogState state, @SqlType(DOUBLE) double value, @SqlType(BIGINT) long uii)
    {
        input(state, Double.doubleToLongBits(value), uii);
    }

    @InputFunction
    @LiteralParameters("x")
    public static void input(@AggregationState KHyperLogLogState state, @SqlType(BIGINT) long value, @SqlType("varchar(x)") Slice uii)
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
    public static void input(@AggregationState KHyperLogLogState state, @SqlType(DOUBLE) double value, @SqlType("varchar(x)") Slice uii)
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

    @OutputFunction(K_HYPER_LOG_LOG)
    public static void output(@AggregationState KHyperLogLogState state, BlockBuilder out)
    {
        SERIALIZER.serialize(state, out);
    }
}

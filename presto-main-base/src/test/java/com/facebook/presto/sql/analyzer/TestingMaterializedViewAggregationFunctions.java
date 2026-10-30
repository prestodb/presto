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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.operator.aggregation.state.LongState;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;

import static com.facebook.presto.common.type.BigintType.BIGINT;

public final class TestingMaterializedViewAggregationFunctions
{
    private TestingMaterializedViewAggregationFunctions() {}

    @AggregationFunction("test_partial")
    public static final class TestPartial
    {
        private TestPartial() {}

        @InputFunction
        public static void input(@AggregationState LongState state, @SqlType(StandardTypes.BIGINT) long value)
        {
            state.setLong(state.getLong() + value);
        }

        @CombineFunction
        public static void combine(@AggregationState LongState state, @AggregationState LongState otherState)
        {
            state.setLong(state.getLong() + otherState.getLong());
        }

        @OutputFunction(StandardTypes.BIGINT)
        public static void output(@AggregationState LongState state, BlockBuilder out)
        {
            BIGINT.writeLong(out, state.getLong());
        }
    }

    @AggregationFunction("test_merge")
    public static final class TestMerge
    {
        private TestMerge() {}

        @InputFunction
        public static void input(@AggregationState LongState state, @SqlType(StandardTypes.BIGINT) long value)
        {
            state.setLong(state.getLong() + value);
        }

        @CombineFunction
        public static void combine(@AggregationState LongState state, @AggregationState LongState otherState)
        {
            state.setLong(state.getLong() + otherState.getLong());
        }

        @OutputFunction(StandardTypes.BIGINT)
        public static void output(@AggregationState LongState state, BlockBuilder out)
        {
            BIGINT.writeLong(out, state.getLong());
        }
    }
}

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

import com.facebook.presto.operator.aggregation.state.LongState;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.type.SqlType;
import com.google.common.collect.ImmutableList;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

@AggregationFunction("count")
public final class CountColumnAggregations
{
    public static final InternalAggregationFunction COUNT_BOOLEAN_COLUMN = new AggregationCompiler().generateAggregationFunction(CountColumnAggregations.class, BIGINT, ImmutableList.<Type>of(BOOLEAN));
    public static final InternalAggregationFunction COUNT_LONG_COLUMN = new AggregationCompiler().generateAggregationFunction(CountColumnAggregations.class, BIGINT, ImmutableList.<Type>of(BIGINT));
    public static final InternalAggregationFunction COUNT_VARCHAR_COLUMN = new AggregationCompiler().generateAggregationFunction(CountColumnAggregations.class, BIGINT, ImmutableList.<Type>of(VARCHAR));

    private CountColumnAggregations() {}

    @InputFunction
    public static void booleanInput(LongState state, @SqlType(BooleanType.class) Block block, @BlockIndex int index)
    {
        state.setLong(state.getLong() + 1);
    }

    @InputFunction
    public static void doubleInput(LongState state, @SqlType(DoubleType.class) Block block, @BlockIndex int index)
    {
        state.setLong(state.getLong() + 1);
    }

    @InputFunction
    public static void varcharInput(LongState state, @SqlType(VarcharType.class) Block block, @BlockIndex int index)
    {
        state.setLong(state.getLong() + 1);
    }

    @InputFunction
    public static void bigintInput(LongState state, @SqlType(BigintType.class) Block block, @BlockIndex int index)
    {
        state.setLong(state.getLong() + 1);
    }

    @CombineFunction
    public static void combine(LongState state, LongState otherState)
    {
        state.setLong(state.getLong() + otherState.getLong());
    }
}

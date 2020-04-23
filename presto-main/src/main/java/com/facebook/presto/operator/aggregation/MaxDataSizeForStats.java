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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.operator.aggregation.state.NullableLongState;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.BlockIndex;
import com.facebook.presto.spi.function.BlockPosition;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;

import static com.facebook.presto.spi.function.SqlFunctionVisibility.HIDDEN;
import static java.lang.Math.max;

@AggregationFunction(value = MaxDataSizeForStats.NAME, visibility = HIDDEN)
public final class MaxDataSizeForStats
{
    public static final String NAME = "$internal$max_data_size_for_stats";

    private MaxDataSizeForStats() {}

    @InputFunction
    @TypeParameter(value = "T")
    public static void input(@AggregationState NullableLongState state, @BlockPosition @SqlType("T") Block block, @BlockIndex int index)
    {
        update(state, block.getEstimatedDataSizeForStats(index));
    }

    @CombineFunction
    public static void combine(@AggregationState NullableLongState state, @AggregationState NullableLongState otherState)
    {
        update(state, otherState.getLong());
    }

    private static void update(NullableLongState state, long size)
    {
        if (state.isNull()) {
            state.setNull(false);
            state.setLong(size);
        }
        else {
            state.setLong(max(state.getLong(), size));
        }
    }

    @OutputFunction(StandardTypes.BIGINT)
    public static void output(@AggregationState NullableLongState state, BlockBuilder out)
    {
        NullableLongState.write(BigintType.BIGINT, state, out);
    }
}

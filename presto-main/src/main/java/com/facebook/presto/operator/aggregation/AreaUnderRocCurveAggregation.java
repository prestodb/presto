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

import com.facebook.presto.operator.aggregation.state.AreaUnderRocCurveState;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;

import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;

@AggregationFunction("area_under_roc_curve")
public final class AreaUnderRocCurveAggregation
{
    private AreaUnderRocCurveAggregation() {}

    @InputFunction
    public static void input(@AggregationState AreaUnderRocCurveState state, @SqlType(StandardTypes.BOOLEAN) boolean label, @SqlType(StandardTypes.DOUBLE) double score)
    {
        GroupedAreaUnderRocCurve auc = state.get();

        BlockBuilder labelBlockBuilder = BOOLEAN.createBlockBuilder(new BlockBuilderStatus(), 1);
        BOOLEAN.writeBoolean(labelBlockBuilder, label);

        BlockBuilder scoreBlockBuilder = DOUBLE.createBlockBuilder(new BlockBuilderStatus(), 1);
        DOUBLE.writeDouble(scoreBlockBuilder, score);

        long startSize = auc.estimatedInMemorySize();
        auc.add(labelBlockBuilder.build(), scoreBlockBuilder.build(), 0, 0);
        state.addMemoryUsage(auc.estimatedInMemorySize() - startSize);
    }

    @CombineFunction
    public static void combine(@AggregationState AreaUnderRocCurveState state, @AggregationState AreaUnderRocCurveState otherState)
    {
        if (!state.get().isCurrentGroupEmpty() && !otherState.get().isCurrentGroupEmpty()) {
            GroupedAreaUnderRocCurve auc = state.get();
            long startSize = auc.estimatedInMemorySize();
            auc.addAll(otherState.get());
            state.addMemoryUsage(auc.estimatedInMemorySize() - startSize);
        }
        else if (state.get().isCurrentGroupEmpty()) {
            state.set(otherState.get());
        }
    }

    @OutputFunction(StandardTypes.DOUBLE)
    public static void output(@AggregationState AreaUnderRocCurveState state, BlockBuilder out)
    {
        GroupedAreaUnderRocCurve auc = state.get();
        auc.serialize(out);
    }
}

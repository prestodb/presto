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
package com.facebook.presto.operator.aggregation.arrayagg;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.aggregation.NullablePosition;
import com.facebook.presto.operator.aggregation.SetOfValues;
import com.facebook.presto.operator.aggregation.state.SetAggregationState;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.BlockIndex;
import com.facebook.presto.spi.function.BlockPosition;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;

@AggregationFunction(value = "set_union", isCalledOnNullInput = true)
@Description("Given a column of array type, return an array of all the unique values contained in each of the arrays in the column")
public class SetUnionFunction
{
    private SetUnionFunction()
    {
    }

    @InputFunction
    @TypeParameter("T")
    public static void input(
            @TypeParameter("T") Type elementType,
            @TypeParameter("array(T)") ArrayType arrayType,
            @AggregationState SetAggregationState state,
            @BlockPosition @SqlType("array(T)") @NullablePosition Block inputBlock,
            @BlockIndex int position)
    {
        SetOfValues set = state.get();
        if (set == null) {
            set = new SetOfValues(elementType);
            state.set(set);
        }

        long startSize = set.estimatedInMemorySize();
        Block arrayBlock = arrayType.getObject(inputBlock, position);
        for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
            set.add(arrayBlock, i);
        }
        state.addMemoryUsage(set.estimatedInMemorySize() - startSize);
    }

    @CombineFunction
    public static void combine(
            @AggregationState SetAggregationState state,
            @AggregationState SetAggregationState otherState)
    {
        if (state.get() != null && otherState.get() != null) {
            SetOfValues otherSet = otherState.get();
            Block otherValues = otherSet.getvalues();

            SetOfValues set = state.get();
            long startSize = set.estimatedInMemorySize();
            for (int i = 0; i < otherValues.getPositionCount(); i++) {
                set.add(otherValues, i);
            }
            state.addMemoryUsage(set.estimatedInMemorySize() - startSize);
        }
        else if (state.get() == null) {
            state.set(otherState.get());
        }
    }

    @OutputFunction("array(T)")
    public static void output(
            @AggregationState SetAggregationState state,
            BlockBuilder out)
    {
        SetOfValues set = state.get();
        if (set == null) {
            out.appendNull();
        }
        else {
            set.serialize(out);
        }
    }
}

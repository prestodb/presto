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
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.operator.aggregation.state.SliceState;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;

/**
 * Dummy aggregate function for CREATE VECTOR INDEX planning.
 * This function is never executed — the connector optimizer replaces
 * the plan tree before execution.
 */
@AggregationFunction("create_vector_index")
public final class CreateVectorIndexAggregation
{
    private CreateVectorIndexAggregation() {}

    // 1-arg overloads: embedding only (no id)

    @InputFunction
    public static void inputRealArray(
            @AggregationState SliceState state,
            @SqlType("array(real)") Block embedding)
    {
    }

    @InputFunction
    public static void inputDoubleArray(
            @AggregationState SliceState state,
            @SqlType("array(double)") Block embedding)
    {
    }

    // 2-arg overloads: id + embedding (matches SQL syntax: ON table(id, embedding))

    @InputFunction
    @TypeParameter("T")
    public static void inputRealArrayWithLongId(
            @AggregationState SliceState state,
            @SqlType("T") long id,
            @SqlType("array(real)") Block embedding)
    {
    }

    @InputFunction
    @TypeParameter("T")
    public static void inputRealArrayWithDoubleId(
            @AggregationState SliceState state,
            @SqlType("T") double id,
            @SqlType("array(real)") Block embedding)
    {
    }

    @InputFunction
    @TypeParameter("T")
    public static void inputRealArrayWithSliceId(
            @AggregationState SliceState state,
            @SqlType("T") Slice id,
            @SqlType("array(real)") Block embedding)
    {
    }

    @InputFunction
    @TypeParameter("T")
    public static void inputDoubleArrayWithLongId(
            @AggregationState SliceState state,
            @SqlType("T") long id,
            @SqlType("array(double)") Block embedding)
    {
    }

    @InputFunction
    @TypeParameter("T")
    public static void inputDoubleArrayWithDoubleId(
            @AggregationState SliceState state,
            @SqlType("T") double id,
            @SqlType("array(double)") Block embedding)
    {
    }

    @InputFunction
    @TypeParameter("T")
    public static void inputDoubleArrayWithSliceId(
            @AggregationState SliceState state,
            @SqlType("T") Slice id,
            @SqlType("array(double)") Block embedding)
    {
    }

    @CombineFunction
    public static void combine(
            @AggregationState SliceState state,
            @AggregationState SliceState otherState)
    {
    }

    @OutputFunction(StandardTypes.VARCHAR)
    public static void output(@AggregationState SliceState state, BlockBuilder out)
    {
        VARCHAR.writeSlice(out, Slices.utf8Slice(""));
    }
}

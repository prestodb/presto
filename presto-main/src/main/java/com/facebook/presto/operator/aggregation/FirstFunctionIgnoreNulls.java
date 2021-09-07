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
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.aggregation.state.BlockState;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.NULLABLE_BLOCK_INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.util.Reflection.methodHandle;

public class FirstFunctionIgnoreNulls
        extends AbstractFirstLastFunction
{
    public static final FirstFunctionIgnoreNulls FIRST_FUNCTION_IGNORE_NULLS = new FirstFunctionIgnoreNulls();

    public FirstFunctionIgnoreNulls()
    {
        super(ImmutableList.of(parseTypeSignature("T"), parseTypeSignature(StandardTypes.BOOLEAN)), "first");
    }

    @Override
    public String getDescription()
    {
        return "Returns the first value";
    }

    @Override
    protected List<Type> getInputTypes(Type type)
    {
        return ImmutableList.of(type, BOOLEAN);
    }

    @Override
    protected List<AggregationMetadata.ParameterMetadata> getInputParameterMetadata(Type type)
    {
        return ImmutableList.of(
                new AggregationMetadata.ParameterMetadata(STATE),
                new AggregationMetadata.ParameterMetadata(NULLABLE_BLOCK_INPUT_CHANNEL, type),
                new AggregationMetadata.ParameterMetadata(NULLABLE_BLOCK_INPUT_CHANNEL, BOOLEAN),
                new AggregationMetadata.ParameterMetadata(BLOCK_INDEX));
    }

    @Override
    protected MethodHandle getInputFunction()
    {
        return methodHandle(FirstFunctionIgnoreNulls.class, "input", BlockState.class, Block.class, Block.class, int.class);
    }

    @Override
    protected MethodHandle getCombineFunction()
    {
        return methodHandle(FirstFunctionIgnoreNulls.class, "combine", BlockState.class, BlockState.class);
    }

    public static void input(BlockState state, Block blockValue, Block blockIgnoreNulls, int position)
    {
        boolean ignoreNulls = blockIgnoreNulls.getByte(position) == 0x00 ? false : true;
        if (ignoreNulls) {
            if (state.getBlock() == null && !blockValue.isNull(position)) {
                state.setBlock(blockValue.getSingleValueBlock(position));
            }
        }
        else {
            if (state.getBlock() == null) {
                state.setBlock(blockValue.getSingleValueBlock(position));
            }
        }
    }

    public static void combine(BlockState state, BlockState otherState)
    {
        if (state.getBlock() == null && otherState.getBlock() != null) {
            state.setBlock(otherState.getBlock());
        }
    }
}

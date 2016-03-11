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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;

import java.lang.invoke.MethodHandle;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static com.facebook.presto.metadata.Signature.orderableTypeParameter;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

public final class ArraySortFunction
        extends SqlScalarFunction
{
    public static final ArraySortFunction ARRAY_SORT_FUNCTION = new ArraySortFunction();
    private static final String FUNCTION_NAME = "array_sort";
    private static final MethodHandle METHOD_HANDLE = methodHandle(ArraySortFunction.class, "sort", Type.class, Block.class);

    public ArraySortFunction()
    {
        super(FUNCTION_NAME, ImmutableList.of(orderableTypeParameter("E")), ImmutableList.of(), "array(E)", ImmutableList.of("array(E)"));
    }

    @Override
    public boolean isHidden()
    {
        return false;
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    @Override
    public String getDescription()
    {
        return "Sorts the given array in ascending order according to the natural ordering of its elements.";
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        checkArgument(boundVariables.getTypeVariables().size() == 1, format("%s expects only one argument", FUNCTION_NAME));
        Type type = boundVariables.getTypeVariable("E");
        MethodHandle methodHandle = METHOD_HANDLE.bindTo(type);
        return new ScalarFunctionImplementation(false, ImmutableList.of(false), methodHandle, isDeterministic());
    }

    public static Block sort(Type type, Block block)
    {
        List<Integer> positions = Ints.asList(new int[block.getPositionCount()]);
        for (int i = 0; i < block.getPositionCount(); i++) {
            positions.set(i, i);
        }

        Collections.sort(positions, new Comparator<Integer>()
        {
            @Override
            public int compare(Integer p1, Integer p2)
            {
                //TODO: This could be quite slow, it should use parametric equals
                return type.compareTo(block, p1, block, p2);
            }
        });

        BlockBuilder blockBuilder = type.createBlockBuilder(new BlockBuilderStatus(), block.getPositionCount());

        for (int position : positions) {
            type.appendTo(block, position, blockBuilder);
        }

        return blockBuilder.build();
    }
}

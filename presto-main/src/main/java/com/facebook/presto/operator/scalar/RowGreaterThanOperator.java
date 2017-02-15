package com.facebook.presto.operator.scalar;
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

import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.type.RowType;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.util.Reflection.methodHandle;

public final class RowGreaterThanOperator
        extends RowComparisonOperator
{
    public static final RowGreaterThanOperator ROW_GREATER_THAN = new RowGreaterThanOperator();
    private static final MethodHandle METHOD_HANDLE = methodHandle(RowGreaterThanOperator.class, "greater", RowType.class, List.class, Block.class, Block.class);

    private RowGreaterThanOperator()
    {
        super(GREATER_THAN);
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type type = boundVariables.getTypeVariable("T");
        return new ScalarFunctionImplementation(
                false,
                ImmutableList.of(false, false),
                METHOD_HANDLE.bindTo(type).bindTo(getMethodHandles((RowType) type, functionRegistry, GREATER_THAN)),
                isDeterministic());
    }

    public static boolean greater(
            RowType rowType,
            List<MethodHandle> lessThanFunctions,
            Block leftRow, Block rightRow)
    {
        int compareResult = compare(rowType, lessThanFunctions, leftRow, rightRow);
        return compareResult > 0;
    }
}

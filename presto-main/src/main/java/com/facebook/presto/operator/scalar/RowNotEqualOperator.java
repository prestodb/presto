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
import com.facebook.presto.metadata.SqlOperator;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.metadata.OperatorType.NOT_EQUAL;
import static com.facebook.presto.metadata.Signature.comparableWithVariadicBound;
import static com.facebook.presto.util.Reflection.methodHandle;

public class RowNotEqualOperator
        extends SqlOperator
{
    public static final RowNotEqualOperator ROW_NOT_EQUAL = new RowNotEqualOperator();
    private static final MethodHandle METHOD_HANDLE = methodHandle(RowNotEqualOperator.class, "notEqual", Type.class, Block.class, Block.class);

    private RowNotEqualOperator()
    {
        super(NOT_EQUAL, ImmutableList.of(comparableWithVariadicBound("T", "row")), ImmutableList.of(), StandardTypes.BOOLEAN, ImmutableList.of("T", "T"));
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type type = boundVariables.getTypeVariable("T");
        return new ScalarFunctionImplementation(false, ImmutableList.of(false, false), METHOD_HANDLE.bindTo(type), isDeterministic());
    }

    public static boolean notEqual(Type rowType, Block leftRow, Block rightRow)
    {
        return !RowEqualOperator.equals(rowType, leftRow, rightRow);
    }
}

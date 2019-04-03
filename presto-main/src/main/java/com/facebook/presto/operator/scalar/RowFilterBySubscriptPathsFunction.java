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
import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.operator.scalar.FilterBySubscriptPathsFunction.FILTER_BY_SUBSCRIPT_PATHS;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static com.facebook.presto.spi.function.FunctionKind.SCALAR;
import static com.facebook.presto.spi.function.Signature.withVariadicBound;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.util.Reflection.methodHandle;

public class RowFilterBySubscriptPathsFunction
        extends SqlScalarFunction
{
    public static final RowFilterBySubscriptPathsFunction ROW_FILTER_BY_SUBSCRIPT_PATHS = new RowFilterBySubscriptPathsFunction();

    private static final MethodHandle METHOD_HANDLE = methodHandle(RowFilterBySubscriptPathsFunction.class, "filter", Block.class, Block.class);

    private RowFilterBySubscriptPathsFunction()
    {
        super(new Signature(
                FILTER_BY_SUBSCRIPT_PATHS,
                SCALAR,
                ImmutableList.of(withVariadicBound("T", "row")),
                ImmutableList.of(),
                parseTypeSignature("T"),
                ImmutableList.of(parseTypeSignature("T"), new ArrayType(VARCHAR).getTypeSignature()),
                false));
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionManager functionManager)
    {
        return new ScalarFunctionImplementation(
                true,
                ImmutableList.of(valueTypeArgumentProperty(RETURN_NULL_ON_NULL), valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                METHOD_HANDLE,
                isDeterministic());
    }

    @Override
    public boolean isHidden()
    {
        return true;
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    @Override
    public String getDescription()
    {
        // Internal function, doesn't need a description
        return null;
    }

    public static Block filter(Block block, Block paths)
    {
        return block;
    }
}

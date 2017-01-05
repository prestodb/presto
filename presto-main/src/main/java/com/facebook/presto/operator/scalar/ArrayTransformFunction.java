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
import com.facebook.presto.metadata.FunctionKind;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.metadata.Signature.typeVariable;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.TypeUtils.readNativeValue;
import static com.facebook.presto.spi.type.TypeUtils.writeNativeValue;
import static com.facebook.presto.util.Reflection.methodHandle;

public final class ArrayTransformFunction
        extends SqlScalarFunction
{
    public static final ArrayTransformFunction ARRAY_TRANSFORM_FUNCTION = new ArrayTransformFunction();

    private static final MethodHandle METHOD_HANDLE = methodHandle(ArrayTransformFunction.class, "transform", Type.class, Type.class, Block.class, MethodHandle.class);

    private ArrayTransformFunction()
    {
        super(new Signature(
                "transform",
                FunctionKind.SCALAR,
                ImmutableList.of(typeVariable("T"), typeVariable("U")),
                ImmutableList.of(),
                parseTypeSignature("array(U)"),
                ImmutableList.of(parseTypeSignature("array(T)"), parseTypeSignature("function(T,U)")),
                false));
    }

    @Override
    public boolean isHidden()
    {
        return false;
    }

    @Override
    public boolean isDeterministic()
    {
        return false;
    }

    @Override
    public String getDescription()
    {
        return "apply lambda to each element of the array";
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type inputType = boundVariables.getTypeVariable("T");
        Type outputType = boundVariables.getTypeVariable("U");
        return new ScalarFunctionImplementation(
                false,
                ImmutableList.of(false, false),
                METHOD_HANDLE.bindTo(inputType).bindTo(outputType),
                isDeterministic());
    }

    public static Block transform(Type inputType, Type outputType, Block block, MethodHandle function)
    {
        int positionCount = block.getPositionCount();
        BlockBuilder resultBuilder = outputType.createBlockBuilder(new BlockBuilderStatus(), positionCount);
        for (int position = 0; position < positionCount; position++) {
            Object input = readNativeValue(inputType, block, position);
            Object output;
            try {
                output = function.invoke(input);
            }
            catch (Throwable throwable) {
                throw Throwables.propagate(throwable);
            }
            writeNativeValue(outputType, resultBuilder, output);
        }
        return resultBuilder.build();
    }
}

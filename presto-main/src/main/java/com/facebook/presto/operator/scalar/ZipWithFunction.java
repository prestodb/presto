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
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.TypeUtils.readNativeValue;
import static com.facebook.presto.spi.type.TypeUtils.writeNativeValue;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.facebook.presto.util.Reflection.methodHandle;

public final class ZipWithFunction
        extends SqlScalarFunction
{
    public static final ZipWithFunction ZIP_WITH_FUNCTION = new ZipWithFunction();

    private static final MethodHandle METHOD_HANDLE = methodHandle(ZipWithFunction.class, "zipWith", Type.class, Type.class, Type.class, Block.class, Block.class, MethodHandle.class);

    private ZipWithFunction()
    {
        super(new Signature(
                "zip_with",
                FunctionKind.SCALAR,
                ImmutableList.of(typeVariable("T"), typeVariable("U"), typeVariable("R")),
                ImmutableList.of(),
                parseTypeSignature("array(R)"),
                ImmutableList.of(parseTypeSignature("array(T)"), parseTypeSignature("array(U)"), parseTypeSignature("function(T,U,R)")),
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
        return "merge two arrays, element-wise, into a single array using the lambda function";
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type leftElementType = boundVariables.getTypeVariable("T");
        Type rightElementType = boundVariables.getTypeVariable("U");
        Type outputElementType = boundVariables.getTypeVariable("R");
        return new ScalarFunctionImplementation(
                false,
                ImmutableList.of(false, false, false),
                METHOD_HANDLE.bindTo(leftElementType).bindTo(rightElementType).bindTo(outputElementType),
                isDeterministic());
    }

    public static Block zipWith(Type leftElementType, Type rightElementType, Type outputElementType, Block leftBlock, Block rightBlock, MethodHandle function)
    {
        checkCondition(leftBlock.getPositionCount() == rightBlock.getPositionCount(), INVALID_FUNCTION_ARGUMENT, "Arrays must have the same length");
        BlockBuilder resultBuilder = outputElementType.createBlockBuilder(new BlockBuilderStatus(), leftBlock.getPositionCount());
        for (int position = 0; position < leftBlock.getPositionCount(); position++) {
            Object left = readNativeValue(leftElementType, leftBlock, position);
            Object right = readNativeValue(rightElementType, rightBlock, position);
            Object output;
            try {
                output = function.invoke(left, right);
            }
            catch (Throwable throwable) {
                throw Throwables.propagate(throwable);
            }
            writeNativeValue(outputElementType, resultBuilder, output);
        }
        return resultBuilder.build();
    }
}

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
package com.facebook.presto.sql.gen;

import com.facebook.presto.byteCode.Block;
import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.CompilerContext;
import com.facebook.presto.byteCode.instruction.LabelNode;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.relational.RowExpression;
import com.google.common.primitives.Primitives;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.sql.gen.ByteCodeUtils.ifWasNullPopAndGoto;
import static com.facebook.presto.sql.gen.ByteCodeUtils.invoke;
import static com.facebook.presto.sql.gen.ByteCodeUtils.unboxPrimitiveIfNecessary;
import static com.facebook.presto.type.UnknownType.UNKNOWN;
import static java.lang.invoke.MethodHandles.catchException;
import static java.lang.invoke.MethodHandles.constant;
import static java.lang.invoke.MethodHandles.dropArguments;
import static java.lang.invoke.MethodType.methodType;

public class TryCastCodeGenerator
        implements ByteCodeGenerator
{
    @Override
    public ByteCodeNode generateExpression(Signature signature, ByteCodeGeneratorContext generatorContext, Type returnType, List<RowExpression> arguments)
    {
        CompilerContext context = generatorContext.getContext();
        RowExpression argument = arguments.get(0);

        Class<?> argumentType = argument.getType().getJavaType();
        Class<?> unboxedReturnType = returnType.getJavaType();
        Class<?> boxedReturnType = Primitives.wrap(unboxedReturnType);

        if (argument.getType().equals(UNKNOWN)) {
            return new Block(context)
                    .putVariable("wasNull", true)
                    .pushJavaDefault(unboxedReturnType);
        }

        MethodHandle function = generatorContext
                .getRegistry()
                .getCoercion(argument.getType(), returnType)
                .getMethodHandle()
                .asType(methodType(boxedReturnType, argumentType));

        MethodHandle tryCast = exceptionToNull(function, boxedReturnType, RuntimeException.class);

        Binding tryCastBinding = generatorContext.getCallSiteBinder().bind(tryCast);

        LabelNode end = new LabelNode("end");

        return new Block(context)
                .comment("call tryCast method")
                .append(generatorContext.generate(argument))
                .append(ifWasNullPopAndGoto(context, end, unboxedReturnType, argumentType))
                .append(invoke(generatorContext.getContext(), tryCastBinding, "tryCast"))
                .append(unboxPrimitiveIfNecessary(context, boxedReturnType))
                .visitLabel(end);
    }

    private static MethodHandle exceptionToNull(MethodHandle target, Class<?> type, Class<? extends Throwable> throwable)
    {
        MethodHandle toNull = dropArguments(constant(type, null), 0, throwable);
        return catchException(target, throwable, toNull);
    }
}

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

import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.BytecodeNode;
import com.facebook.presto.bytecode.ClassDefinition;
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.Parameter;
import com.facebook.presto.bytecode.ParameterizedType;
import com.facebook.presto.bytecode.Scope;
import com.facebook.presto.bytecode.control.TryCatch;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.relational.CallExpression;
import com.facebook.presto.sql.relational.RowExpression;
import com.google.common.primitives.Primitives;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.bytecode.Access.PUBLIC;
import static com.facebook.presto.bytecode.Access.a;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.spi.StandardErrorCode.DIVISION_BY_ZERO;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static com.facebook.presto.sql.gen.BytecodeUtils.boxPrimitiveIfNecessary;
import static com.facebook.presto.sql.gen.BytecodeUtils.invoke;
import static com.facebook.presto.sql.gen.BytecodeUtils.unboxPrimitiveIfNecessary;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.invoke.MethodType.methodType;

public class TryCodeGenerator
        implements BytecodeGenerator
{
    private static final String EXCEPTION_HANDLER_NAME = "tryExpressionExceptionHandler";

    private final Map<CallExpression, MethodDefinition> tryMethodsMap;

    public TryCodeGenerator(Map<CallExpression, MethodDefinition> tryMethodsMap)
    {
        this.tryMethodsMap = tryMethodsMap;
    }

    @Override
    public BytecodeNode generateExpression(Signature signature, BytecodeGeneratorContext context, Type returnType, List<RowExpression> arguments)
    {
        checkArgument(arguments.size() == 1, "try methods only contain a single expression");
        checkArgument(getOnlyElement(arguments) instanceof CallExpression, "try methods must contain a call expression");

        CallExpression innerCallExpression = (CallExpression) getOnlyElement(arguments);
        checkState(tryMethodsMap.containsKey(innerCallExpression), "try methods map does not contain this try call");

        BytecodeBlock bytecodeBlock = new BytecodeBlock()
                .comment("load required variables")
                .getVariable(context.getScope().getVariable("this"));

        MethodDefinition definition = tryMethodsMap.get(innerCallExpression);

        definition.getParameters().stream()
                .map(parameter -> context.getScope().getVariable(parameter.getName()))
                .forEach(bytecodeBlock::getVariable);

        bytecodeBlock.comment("call dynamic try method: " + definition.getName())
                .invokeVirtual(definition)
                .append(unboxPrimitiveIfNecessary(context.getScope(), Primitives.wrap(innerCallExpression.getType().getJavaType())));

        return bytecodeBlock;
    }

    public static MethodDefinition defineTryMethod(
            BytecodeExpressionVisitor innerExpressionVisitor,
            ClassDefinition classDefinition,
            String methodName,
            List<Parameter> inputParameters,
            Class<?> returnType,
            RowExpression innerRowExpression,
            CallSiteBinder callSiteBinder)
    {
        MethodDefinition method = classDefinition.declareMethod(a(PUBLIC), methodName, type(returnType), inputParameters);
        Scope calleeMethodScope = method.getScope();

        BytecodeNode innerExpression = innerRowExpression.accept(innerExpressionVisitor, calleeMethodScope);

        MethodType exceptionHandlerType = methodType(returnType, PrestoException.class);
        MethodHandle exceptionHandler = methodHandle(TryCodeGenerator.class, EXCEPTION_HANDLER_NAME, PrestoException.class).asType(exceptionHandlerType);
        Binding binding = callSiteBinder.bind(exceptionHandler);

        method.comment("Try projection: %s", innerRowExpression.toString());
        method.getBody()
                .append(new TryCatch(
                        new BytecodeBlock()
                                .append(innerExpression)
                                .append(boxPrimitiveIfNecessary(calleeMethodScope, returnType)),
                        new BytecodeBlock()
                                .append(invoke(binding, EXCEPTION_HANDLER_NAME)),
                        ParameterizedType.type(PrestoException.class)))
                .ret(returnType);

        return method;
    }

    public static Object tryExpressionExceptionHandler(PrestoException e)
            throws PrestoException
    {
        int errorCode = e.getErrorCode().getCode();
        if (errorCode == DIVISION_BY_ZERO.toErrorCode().getCode()
                || errorCode == INVALID_CAST_ARGUMENT.toErrorCode().getCode()
                || errorCode == INVALID_FUNCTION_ARGUMENT.toErrorCode().getCode()
                || errorCode == NUMERIC_VALUE_OUT_OF_RANGE.toErrorCode().getCode()) {
            return null;
        }

        throw e;
    }
}

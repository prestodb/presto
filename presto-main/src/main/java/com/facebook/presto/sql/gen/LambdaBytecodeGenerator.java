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
import com.facebook.presto.bytecode.FieldDefinition;
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.Parameter;
import com.facebook.presto.bytecode.Scope;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.expression.BytecodeExpressions;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.sql.relational.CallExpression;
import com.facebook.presto.sql.relational.ConstantExpression;
import com.facebook.presto.sql.relational.InputReferenceExpression;
import com.facebook.presto.sql.relational.LambdaDefinitionExpression;
import com.facebook.presto.sql.relational.RowExpressionVisitor;
import com.facebook.presto.sql.relational.VariableReferenceExpression;
import com.facebook.presto.util.Reflection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Primitives;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.bytecode.Access.FINAL;
import static com.facebook.presto.bytecode.Access.PRIVATE;
import static com.facebook.presto.bytecode.Access.PUBLIC;
import static com.facebook.presto.bytecode.Access.STATIC;
import static com.facebook.presto.bytecode.Access.a;
import static com.facebook.presto.bytecode.Parameter.arg;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantClass;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantString;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.invokeStatic;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.newArray;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.setStatic;
import static com.facebook.presto.sql.gen.BytecodeUtils.boxPrimitiveIfNecessary;
import static com.facebook.presto.sql.gen.BytecodeUtils.unboxPrimitiveIfNecessary;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class LambdaBytecodeGenerator
{
    private LambdaBytecodeGenerator()
    {
    }

    /**
     * @return a MethodHandle field that represents the lambda expression
     */
    public static FieldDefinition preGenerateLambdaExpression(
            LambdaDefinitionExpression lambdaExpression,
            String fieldName,
            ClassDefinition classDefinition,
            PreGeneratedExpressions preGeneratedExpressions,
            CallSiteBinder callSiteBinder,
            CachedInstanceBinder cachedInstanceBinder,
            FunctionRegistry functionRegistry)
    {
        ImmutableList.Builder<Parameter> parameters = ImmutableList.builder();
        ImmutableMap.Builder<String, ParameterAndType> parameterMapBuilder = ImmutableMap.builder();

        parameters.add(arg("session", ConnectorSession.class));
        for (int i = 0; i < lambdaExpression.getArguments().size(); i++) {
            Class<?> type = Primitives.wrap(lambdaExpression.getArgumentTypes().get(i).getJavaType());
            String argumentName = lambdaExpression.getArguments().get(i);
            Parameter arg = arg("lambda_" + argumentName, type);
            parameters.add(arg);
            parameterMapBuilder.put(argumentName, new ParameterAndType(arg, type));
        }

        BytecodeExpressionVisitor innerExpressionVisitor = new BytecodeExpressionVisitor(
                callSiteBinder,
                cachedInstanceBinder,
                variableReferenceCompiler(parameterMapBuilder.build()),
                functionRegistry,
                preGeneratedExpressions);

        return defineLambdaMethodAndField(
                innerExpressionVisitor,
                classDefinition,
                fieldName,
                parameters.build(),
                lambdaExpression);
    }

    private static FieldDefinition defineLambdaMethodAndField(
            BytecodeExpressionVisitor innerExpressionVisitor,
            ClassDefinition classDefinition,
            String fieldAndMethodName,
            List<Parameter> inputParameters,
            LambdaDefinitionExpression lambda)
    {
        Class<?> returnType = Primitives.wrap(lambda.getBody().getType().getJavaType());
        MethodDefinition method = classDefinition.declareMethod(a(PUBLIC), fieldAndMethodName, type(returnType), inputParameters);

        Scope scope = method.getScope();
        Variable wasNull = scope.declareVariable(boolean.class, "wasNull");
        BytecodeNode compiledBody = lambda.getBody().accept(innerExpressionVisitor, scope);
        method.getBody()
                .putVariable(wasNull, false)
                .append(compiledBody)
                .append(boxPrimitiveIfNecessary(scope, returnType))
                .ret(returnType);

        FieldDefinition methodHandleField = classDefinition.declareField(a(PRIVATE, STATIC, FINAL), fieldAndMethodName, type(MethodHandle.class));

        classDefinition.getClassInitializer().getBody()
                .append(setStatic(
                        methodHandleField,
                        invokeStatic(
                                Reflection.class,
                                "methodHandle",
                                MethodHandle.class,
                                constantClass(classDefinition.getType()),
                                constantString(fieldAndMethodName),
                                newArray(
                                        type(Class[].class),
                                        inputParameters.stream()
                                                .map(Parameter::getType)
                                                .map(BytecodeExpressions::constantClass)
                                                .collect(toImmutableList())))));
        return methodHandleField;
    }

    private static RowExpressionVisitor<Scope, BytecodeNode> variableReferenceCompiler(Map<String, ParameterAndType> parameterMap)
    {
        return new RowExpressionVisitor<Scope, BytecodeNode>()
        {
            @Override
            public BytecodeNode visitInputReference(InputReferenceExpression node, Scope scope)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public BytecodeNode visitCall(CallExpression call, Scope scope)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public BytecodeNode visitConstant(ConstantExpression literal, Scope scope)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public BytecodeNode visitLambda(LambdaDefinitionExpression lambda, Scope context)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public BytecodeNode visitVariableReference(VariableReferenceExpression reference, Scope context)
            {
                ParameterAndType parameterAndType = parameterMap.get(reference.getName());
                Parameter parameter = parameterAndType.getParameter();
                Class<?> type = parameterAndType.getType();
                return new BytecodeBlock()
                        .append(parameter)
                        .append(unboxPrimitiveIfNecessary(context, type));
            }
        };
    }
}

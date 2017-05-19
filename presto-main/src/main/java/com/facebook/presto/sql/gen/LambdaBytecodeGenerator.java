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
import com.facebook.presto.bytecode.ParameterizedType;
import com.facebook.presto.bytecode.Scope;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.expression.BytecodeExpression;
import com.facebook.presto.bytecode.expression.BytecodeExpressions;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.sql.relational.CallExpression;
import com.facebook.presto.sql.relational.ConstantExpression;
import com.facebook.presto.sql.relational.InputReferenceExpression;
import com.facebook.presto.sql.relational.LambdaDefinitionExpression;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.sql.relational.RowExpressionVisitor;
import com.facebook.presto.sql.relational.VariableReferenceExpression;
import com.facebook.presto.util.Reflection;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Primitives;
import org.objectweb.asm.Handle;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.lang.invoke.MethodHandle;
import java.lang.reflect.Method;
import java.util.Arrays;
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
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantFalse;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantString;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.getStatic;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.invokeDynamic;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.invokeStatic;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.newArray;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.setStatic;
import static com.facebook.presto.spi.StandardErrorCode.COMPILER_ERROR;
import static com.facebook.presto.sql.gen.BytecodeUtils.boxPrimitiveIfNecessary;
import static com.facebook.presto.sql.gen.BytecodeUtils.unboxPrimitiveIfNecessary;
import static com.facebook.presto.sql.gen.LambdaCapture.LAMBDA_CAPTURE_METHOD;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static org.objectweb.asm.Type.getMethodType;
import static org.objectweb.asm.Type.getType;

public class LambdaBytecodeGenerator
{
    private LambdaBytecodeGenerator()
    {
    }

    /**
     * @return a MethodHandle field that represents the lambda expression
     */
    public static CompiledLambda preGenerateLambdaExpression(
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

        RowExpressionCompiler innerExpressionCompiler = new RowExpressionCompiler(
                callSiteBinder,
                cachedInstanceBinder,
                variableReferenceCompiler(parameterMapBuilder.build()),
                functionRegistry,
                preGeneratedExpressions);

        return defineLambdaMethodAndField(
                innerExpressionCompiler,
                classDefinition,
                fieldName,
                parameters.build(),
                lambdaExpression);
    }

    private static CompiledLambda defineLambdaMethodAndField(
            RowExpressionCompiler innerExpressionCompiler,
            ClassDefinition classDefinition,
            String fieldAndMethodName,
            List<Parameter> inputParameters,
            LambdaDefinitionExpression lambda)
    {
        Class<?> returnType = Primitives.wrap(lambda.getBody().getType().getJavaType());
        MethodDefinition method = classDefinition.declareMethod(a(PUBLIC), fieldAndMethodName, type(returnType), inputParameters);

        Scope scope = method.getScope();
        Variable wasNull = scope.declareVariable(boolean.class, "wasNull");
        BytecodeNode compiledBody = innerExpressionCompiler.compile(lambda.getBody(), scope);
        method.getBody()
                .putVariable(wasNull, false)
                .append(compiledBody)
                .append(boxPrimitiveIfNecessary(scope, returnType))
                .ret(returnType);

        FieldDefinition staticField = classDefinition.declareField(a(PRIVATE, STATIC, FINAL), fieldAndMethodName, type(MethodHandle.class));
        FieldDefinition instanceField = classDefinition.declareField(a(PRIVATE, FINAL), "binded_" + fieldAndMethodName, type(MethodHandle.class));

        classDefinition.getClassInitializer().getBody()
                .append(setStatic(
                        staticField,
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

        Handle lambdaAsmHandle = new Handle(
                Opcodes.H_INVOKEVIRTUAL,
                method.getThis().getType().getClassName(),
                method.getName(),
                method.getMethodDescriptor());

        return new CompiledLambda(
                lambdaAsmHandle,
                method.getReturnType(),
                method.getParameterTypes(),
                staticField,
                instanceField);
    }

    public static BytecodeNode generateLambda(
            BytecodeGeneratorContext context,
            List<RowExpression> captureExpressions,
            CompiledLambda compiledLambda,
            Class lambdaInterface)
    {
        if (!lambdaInterface.isAnnotationPresent(FunctionalInterface.class)) {
            // lambdaInterface is checked to be annotated with FunctionalInterface when generating ScalarFunctionImplementation
            throw new VerifyException("lambda should be generated as class annotated with FunctionalInterface");
        }

        BytecodeBlock block = new BytecodeBlock().setDescription("Partial apply");
        Scope scope = context.getScope();

        Variable wasNull = scope.getVariable("wasNull");

        // generate values to be captured
        ImmutableList.Builder<BytecodeExpression> captureVariableBuilder = ImmutableList.builder();
        for (RowExpression captureExpression : captureExpressions) {
            Class<?> valueType = Primitives.wrap(captureExpression.getType().getJavaType());
            Variable valueVariable = scope.createTempVariable(valueType);
            block.append(context.generate(captureExpression));
            block.append(boxPrimitiveIfNecessary(scope, valueType));
            block.putVariable(valueVariable);
            block.append(wasNull.set(constantFalse()));
            captureVariableBuilder.add(valueVariable);
        }

        List<BytecodeExpression> captureVariables = ImmutableList.<BytecodeExpression>builder()
                .add(scope.getThis(), scope.getVariable("session"))
                .addAll(captureVariableBuilder.build())
                .build();

        Type instantiatedMethodAsmType = getMethodType(
                compiledLambda.getReturnType().getAsmType(),
                compiledLambda.getParameterTypes().stream()
                    .skip(captureExpressions.size() + 1) // skip capture variables and ConnectorSession
                    .map(ParameterizedType::getAsmType)
                    .collect(toImmutableList()).toArray(new Type[0]));

        block.append(
                invokeDynamic(
                        LAMBDA_CAPTURE_METHOD,
                        ImmutableList.of(
                                getType(getSingleApplyMethod(lambdaInterface)),
                                compiledLambda.getLambdaAsmHandle(),
                                instantiatedMethodAsmType
                        ),
                        "apply",
                        type(lambdaInterface),
                        captureVariables)
        );
        return block;
    }

    private static Method getSingleApplyMethod(Class lambdaFunctionInterface)
    {
        checkCondition(lambdaFunctionInterface.isAnnotationPresent(FunctionalInterface.class), COMPILER_ERROR, "Lambda function interface is required to be annotated with FunctionalInterface");

        List<Method> applyMethods = Arrays.stream(lambdaFunctionInterface.getMethods())
                .filter(method -> method.getName().equals("apply"))
                .collect(toImmutableList());

        checkCondition(applyMethods.size() == 1, COMPILER_ERROR, "Expect to have exactly 1 method with name 'apply' in interface " + lambdaFunctionInterface.getName());
        return applyMethods.get(0);
    }

    private static RowExpressionVisitor<BytecodeNode, Scope> variableReferenceCompiler(Map<String, ParameterAndType> parameterMap)
    {
        return new RowExpressionVisitor<BytecodeNode, Scope>()
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

    static class CompiledLambda
    {
        private final FieldDefinition staticField;
        // the instance field will be binded to "this" in constructor
        private final FieldDefinition instanceField;

        // lambda method information
        private final Handle lambdaAsmHandle;
        private final ParameterizedType returnType;
        private final List<ParameterizedType> parameterTypes;

        public CompiledLambda(
                Handle lambdaAsmHandle,
                ParameterizedType returnType,
                List<ParameterizedType> parameterTypes,
                FieldDefinition staticField,
                FieldDefinition instanceField)
        {
            this.staticField = requireNonNull(staticField, "staticField is null");
            this.instanceField = requireNonNull(instanceField, "instanceField is null");
            this.lambdaAsmHandle = requireNonNull(lambdaAsmHandle, "lambdaMethodAsmHandle is null");
            this.returnType = requireNonNull(returnType, "returnType is null");
            this.parameterTypes = ImmutableList.copyOf(requireNonNull(parameterTypes, "returnType is null"));
        }

        public Handle getLambdaAsmHandle()
        {
            return lambdaAsmHandle;
        }

        public ParameterizedType getReturnType()
        {
            return returnType;
        }

        public List<ParameterizedType> getParameterTypes()
        {
            return parameterTypes;
        }

        public FieldDefinition getInstanceField()
        {
            return instanceField;
        }

        public void generateInitialization(Variable thisVariable, BytecodeBlock block)
        {
            block.append(
                    thisVariable.setField(
                            instanceField,
                            getStatic(staticField).invoke("bindTo", MethodHandle.class, thisVariable.cast(Object.class))));
        }
    }
}

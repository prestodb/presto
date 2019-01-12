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
package io.prestosql.sql.gen;

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Primitives;
import io.airlift.bytecode.Access;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.BytecodeNode;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.FieldDefinition;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.ParameterizedType;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.expression.BytecodeExpression;
import io.prestosql.metadata.FunctionRegistry;
import io.prestosql.operator.aggregation.AccumulatorCompiler;
import io.prestosql.operator.aggregation.LambdaProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.sql.relational.CallExpression;
import io.prestosql.sql.relational.ConstantExpression;
import io.prestosql.sql.relational.InputReferenceExpression;
import io.prestosql.sql.relational.LambdaDefinitionExpression;
import io.prestosql.sql.relational.RowExpression;
import io.prestosql.sql.relational.RowExpressionVisitor;
import io.prestosql.sql.relational.VariableReferenceExpression;
import org.objectweb.asm.Handle;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.bytecode.Access.PRIVATE;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantFalse;
import static io.airlift.bytecode.expression.BytecodeExpressions.invokeDynamic;
import static io.prestosql.spi.StandardErrorCode.COMPILER_ERROR;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.sql.gen.BytecodeUtils.boxPrimitiveIfNecessary;
import static io.prestosql.sql.gen.BytecodeUtils.unboxPrimitiveIfNecessary;
import static io.prestosql.sql.gen.LambdaCapture.LAMBDA_CAPTURE_METHOD;
import static io.prestosql.sql.gen.LambdaExpressionExtractor.extractLambdaExpressions;
import static io.prestosql.util.CompilerUtils.defineClass;
import static io.prestosql.util.CompilerUtils.makeClassName;
import static io.prestosql.util.Failures.checkCondition;
import static java.util.Objects.requireNonNull;
import static org.objectweb.asm.Type.getMethodType;
import static org.objectweb.asm.Type.getType;

public class LambdaBytecodeGenerator
{
    private LambdaBytecodeGenerator()
    {
    }

    public static Map<LambdaDefinitionExpression, CompiledLambda> generateMethodsForLambda(
            ClassDefinition containerClassDefinition,
            CallSiteBinder callSiteBinder,
            CachedInstanceBinder cachedInstanceBinder,
            RowExpression expression,
            FunctionRegistry functionRegistry)
    {
        Set<LambdaDefinitionExpression> lambdaExpressions = ImmutableSet.copyOf(extractLambdaExpressions(expression));
        ImmutableMap.Builder<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap = ImmutableMap.builder();

        int counter = 0;
        for (LambdaDefinitionExpression lambdaExpression : lambdaExpressions) {
            CompiledLambda compiledLambda = LambdaBytecodeGenerator.preGenerateLambdaExpression(
                    lambdaExpression,
                    "lambda_" + counter,
                    containerClassDefinition,
                    compiledLambdaMap.build(),
                    callSiteBinder,
                    cachedInstanceBinder,
                    functionRegistry);
            compiledLambdaMap.put(lambdaExpression, compiledLambda);
            counter++;
        }

        return compiledLambdaMap.build();
    }

    /**
     * @return a MethodHandle field that represents the lambda expression
     */
    public static CompiledLambda preGenerateLambdaExpression(
            LambdaDefinitionExpression lambdaExpression,
            String methodName,
            ClassDefinition classDefinition,
            Map<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap,
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
                compiledLambdaMap);

        return defineLambdaMethod(
                innerExpressionCompiler,
                classDefinition,
                methodName,
                parameters.build(),
                lambdaExpression);
    }

    private static CompiledLambda defineLambdaMethod(
            RowExpressionCompiler innerExpressionCompiler,
            ClassDefinition classDefinition,
            String methodName,
            List<Parameter> inputParameters,
            LambdaDefinitionExpression lambda)
    {
        checkCondition(inputParameters.size() <= 254, NOT_SUPPORTED, "Too many arguments for lambda expression");
        Class<?> returnType = Primitives.wrap(lambda.getBody().getType().getJavaType());
        MethodDefinition method = classDefinition.declareMethod(a(PUBLIC), methodName, type(returnType), inputParameters);

        Scope scope = method.getScope();
        Variable wasNull = scope.declareVariable(boolean.class, "wasNull");
        BytecodeNode compiledBody = innerExpressionCompiler.compile(lambda.getBody(), scope);
        method.getBody()
                .putVariable(wasNull, false)
                .append(compiledBody)
                .append(boxPrimitiveIfNecessary(scope, returnType))
                .ret(returnType);

        Handle lambdaAsmHandle = new Handle(
                Opcodes.H_INVOKEVIRTUAL,
                method.getThis().getType().getClassName(),
                method.getName(),
                method.getMethodDescriptor(),
                false);

        return new CompiledLambda(
                lambdaAsmHandle,
                method.getReturnType(),
                method.getParameterTypes());
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
                                instantiatedMethodAsmType),
                        "apply",
                        type(lambdaInterface),
                        captureVariables));
        return block;
    }

    public static Class<? extends LambdaProvider> compileLambdaProvider(LambdaDefinitionExpression lambdaExpression, FunctionRegistry functionRegistry, Class lambdaInterface)
    {
        ClassDefinition lambdaProviderClassDefinition = new ClassDefinition(
                a(PUBLIC, Access.FINAL),
                makeClassName("LambdaProvider"),
                type(Object.class),
                type(LambdaProvider.class));

        FieldDefinition sessionField = lambdaProviderClassDefinition.declareField(a(PRIVATE), "session", ConnectorSession.class);

        CallSiteBinder callSiteBinder = new CallSiteBinder();
        CachedInstanceBinder cachedInstanceBinder = new CachedInstanceBinder(lambdaProviderClassDefinition, callSiteBinder);

        Map<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap = generateMethodsForLambda(
                lambdaProviderClassDefinition,
                callSiteBinder,
                cachedInstanceBinder,
                lambdaExpression,
                functionRegistry);

        MethodDefinition method = lambdaProviderClassDefinition.declareMethod(
                a(PUBLIC),
                "getLambda",
                type(Object.class),
                ImmutableList.of());

        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();
        scope.declareVariable("wasNull", body, constantFalse());
        scope.declareVariable("session", body, method.getThis().getField(sessionField));

        RowExpressionCompiler rowExpressionCompiler = new RowExpressionCompiler(
                callSiteBinder,
                cachedInstanceBinder,
                variableReferenceCompiler(ImmutableMap.of()),
                functionRegistry,
                compiledLambdaMap);

        BytecodeGeneratorContext generatorContext = new BytecodeGeneratorContext(
                rowExpressionCompiler,
                scope,
                callSiteBinder,
                cachedInstanceBinder,
                functionRegistry);

        body.append(
                generateLambda(
                        generatorContext,
                        ImmutableList.of(),
                        compiledLambdaMap.get(lambdaExpression),
                        lambdaInterface))
                .retObject();

        // constructor
        Parameter sessionParameter = arg("session", ConnectorSession.class);

        MethodDefinition constructorDefinition = lambdaProviderClassDefinition.declareConstructor(a(PUBLIC), sessionParameter);
        BytecodeBlock constructorBody = constructorDefinition.getBody();
        Variable constructorThisVariable = constructorDefinition.getThis();

        constructorBody.comment("super();")
                .append(constructorThisVariable)
                .invokeConstructor(Object.class)
                .append(constructorThisVariable.setField(sessionField, sessionParameter));

        cachedInstanceBinder.generateInitializations(constructorThisVariable, constructorBody);
        constructorBody.ret();

        return defineClass(lambdaProviderClassDefinition, LambdaProvider.class, callSiteBinder.getBindings(), AccumulatorCompiler.class.getClassLoader());
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
        // lambda method information
        private final Handle lambdaAsmHandle;
        private final ParameterizedType returnType;
        private final List<ParameterizedType> parameterTypes;

        public CompiledLambda(
                Handle lambdaAsmHandle,
                ParameterizedType returnType,
                List<ParameterizedType> parameterTypes)
        {
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
    }
}

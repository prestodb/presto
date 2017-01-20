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
import com.facebook.presto.bytecode.DynamicClassLoader;
import com.facebook.presto.bytecode.FieldDefinition;
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.Parameter;
import com.facebook.presto.bytecode.Scope;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.control.IfStatement;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.InternalJoinFilterFunction;
import com.facebook.presto.operator.JoinFilterFunction;
import com.facebook.presto.operator.StandardJoinFilterFunction;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.sql.relational.CallExpression;
import com.facebook.presto.sql.relational.LambdaDefinitionExpression;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.sql.relational.RowExpressionVisitor;
import com.facebook.presto.sql.relational.Signatures;
import com.google.common.base.Throwables;
import com.google.common.base.VerifyException;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Primitives;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import javax.inject.Inject;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static com.facebook.presto.bytecode.Access.FINAL;
import static com.facebook.presto.bytecode.Access.PRIVATE;
import static com.facebook.presto.bytecode.Access.PUBLIC;
import static com.facebook.presto.bytecode.Access.a;
import static com.facebook.presto.bytecode.CompilerUtils.defineClass;
import static com.facebook.presto.bytecode.CompilerUtils.makeClassName;
import static com.facebook.presto.bytecode.Parameter.arg;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantFalse;
import static com.facebook.presto.sql.gen.BytecodeUtils.invoke;
import static com.facebook.presto.sql.gen.LambdaAndTryExpressionExtractor.extractLambdaAndTryExpressions;
import static com.facebook.presto.sql.gen.TryCodeGenerator.defineTryMethod;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Verify.verify;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class JoinFilterFunctionCompiler
{
    private final Metadata metadata;

    @Inject
    public JoinFilterFunctionCompiler(Metadata metadata)
    {
        this.metadata = metadata;
    }

    private final LoadingCache<JoinFilterCacheKey, JoinFilterFunctionFactory> joinFilterFunctionFactories = CacheBuilder.newBuilder().maximumSize(1000).build(
            new CacheLoader<JoinFilterCacheKey, JoinFilterFunctionFactory>()
            {
                @Override
                public JoinFilterFunctionFactory load(JoinFilterCacheKey key)
                        throws Exception
                {
                    return internalCompileFilterFunctionFactory(key.getFilter(), key.getLeftBlocksSize());
                }
            });

    public JoinFilterFunctionFactory compileJoinFilterFunction(RowExpression filter, int leftBlocksSize)
    {
        return joinFilterFunctionFactories.getUnchecked(new JoinFilterCacheKey(filter, leftBlocksSize));
    }

    private JoinFilterFunctionFactory internalCompileFilterFunctionFactory(RowExpression filterExpression, int leftBlocksSize)
    {
        Class<? extends InternalJoinFilterFunction> internalJoinFilterFunction = compileInternalJoinFilterFunction(filterExpression, leftBlocksSize);
        return new IsolatedJoinFilterFunctionFactory(internalJoinFilterFunction);
    }

    private Class<? extends InternalJoinFilterFunction> compileInternalJoinFilterFunction(RowExpression filterExpression, int leftBlocksSize)
    {
        ClassDefinition classDefinition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName("JoinFilterFunction"),
                type(Object.class),
                type(InternalJoinFilterFunction.class));

        CallSiteBinder callSiteBinder = new CallSiteBinder();

        new JoinFilterFunctionCompiler(metadata).generateMethods(classDefinition, callSiteBinder, filterExpression, leftBlocksSize);

        //
        // toString method
        //
        generateToString(
                classDefinition,
                callSiteBinder,
                toStringHelper(classDefinition.getType().getJavaClassName())
                        .add("filter", filterExpression)
                        .add("leftBlocksSize", leftBlocksSize)
                        .toString());

        return defineClass(classDefinition, InternalJoinFilterFunction.class, callSiteBinder.getBindings(), getClass().getClassLoader());
    }

    private void generateMethods(ClassDefinition classDefinition, CallSiteBinder callSiteBinder, RowExpression filter, int leftBlocksSize)
    {
        CachedInstanceBinder cachedInstanceBinder = new CachedInstanceBinder(classDefinition, callSiteBinder);

        FieldDefinition sessionField = classDefinition.declareField(a(PRIVATE, FINAL), "session", ConnectorSession.class);
        generateFilterMethod(classDefinition, callSiteBinder, cachedInstanceBinder, filter, leftBlocksSize, sessionField);
        generateConstructor(classDefinition, sessionField, cachedInstanceBinder);
    }

    private static void generateConstructor(ClassDefinition classDefinition, FieldDefinition sessionField, CachedInstanceBinder cachedInstanceBinder)
    {
        Parameter sessionParameter = arg("session", ConnectorSession.class);
        MethodDefinition constructorDefinition = classDefinition.declareConstructor(a(PUBLIC), sessionParameter);

        BytecodeBlock body = constructorDefinition.getBody();
        Variable thisVariable = constructorDefinition.getThis();

        body.comment("super();")
                .append(thisVariable)
                .invokeConstructor(Object.class);

        body.append(thisVariable.setField(sessionField, sessionParameter));

        cachedInstanceBinder.generateInitializations(thisVariable, body);

        body.ret();
    }

    private void generateFilterMethod(ClassDefinition classDefinition, CallSiteBinder callSiteBinder, CachedInstanceBinder cachedInstanceBinder, RowExpression filter, int leftBlocksSize, FieldDefinition sessionField)
    {
        PreGeneratedExpressions preGeneratedExpressions = generateMethodsForLambdaAndTry(classDefinition, callSiteBinder, cachedInstanceBinder, leftBlocksSize, filter);

        // int leftPosition, Block[] leftBlocks, int rightPosition, Block[] rightBlocks
        Parameter leftPosition = arg("leftPosition", int.class);
        Parameter leftBlocks = arg("leftBlocks", Block[].class);
        Parameter rightPosition = arg("rightPosition", int.class);
        Parameter rightBlocks = arg("rightBlocks", Block[].class);

        MethodDefinition method = classDefinition.declareMethod(
                a(PUBLIC),
                "filter",
                type(boolean.class),
                ImmutableList.<Parameter>builder()
                        .add(leftPosition)
                        .add(leftBlocks)
                        .add(rightPosition)
                        .add(rightBlocks)
                        .build());

        method.comment("filter: %s", filter.toString());
        BytecodeBlock body = method.getBody();

        Scope scope = method.getScope();
        Variable wasNullVariable = scope.declareVariable("wasNull", body, constantFalse());
        scope.declareVariable("session", body, method.getThis().getField(sessionField));

        BytecodeExpressionVisitor visitor = new BytecodeExpressionVisitor(
                callSiteBinder,
                cachedInstanceBinder,
                fieldReferenceCompiler(callSiteBinder, leftPosition, leftBlocks, rightPosition, rightBlocks, leftBlocksSize),
                metadata.getFunctionRegistry(),
                preGeneratedExpressions);

        BytecodeNode visitorBody = filter.accept(visitor, scope);

        Variable result = scope.declareVariable(boolean.class, "result");
        body.append(visitorBody)
                .putVariable(result)
                .append(new IfStatement()
                        .condition(wasNullVariable)
                        .ifTrue(constantFalse().ret())
                        .ifFalse(result.ret()));
    }

    private PreGeneratedExpressions generateMethodsForLambdaAndTry(
            ClassDefinition containerClassDefinition,
            CallSiteBinder callSiteBinder,
            CachedInstanceBinder cachedInstanceBinder,
            int leftBlocksSize,
            RowExpression filter)
    {
        Set<RowExpression> lambdaAndTryExpressions = ImmutableSet.copyOf(extractLambdaAndTryExpressions(filter));
        ImmutableMap.Builder<CallExpression, MethodDefinition> tryMethodMap = ImmutableMap.builder();
        ImmutableMap.Builder<LambdaDefinitionExpression, FieldDefinition> lambdaFieldMap = ImmutableMap.builder();

        int counter = 0;
        for (RowExpression expression : lambdaAndTryExpressions) {
            if (expression instanceof CallExpression) {
                CallExpression tryExpression = (CallExpression) expression;
                verify(!Signatures.TRY.equals(tryExpression.getSignature().getName()));

                Parameter session = arg("session", ConnectorSession.class);
                Parameter leftPosition = arg("leftPosition", int.class);
                Parameter leftBlocks = arg("leftBlocks", Block[].class);
                Parameter rightPosition = arg("rightPosition", int.class);
                Parameter rightBlocks = arg("rightBlocks", Block[].class);

                BytecodeExpressionVisitor innerExpressionVisitor = new BytecodeExpressionVisitor(
                        callSiteBinder,
                        cachedInstanceBinder,
                        fieldReferenceCompiler(callSiteBinder, leftPosition, leftBlocks, rightPosition, rightBlocks, leftBlocksSize),
                        metadata.getFunctionRegistry(),
                        new PreGeneratedExpressions(tryMethodMap.build(), lambdaFieldMap.build()));

                List<Parameter> inputParameters = ImmutableList.<Parameter>builder()
                        .add(session)
                        .add(leftPosition)
                        .add(leftBlocks)
                        .add(rightPosition)
                        .add(rightBlocks)
                        .build();

                MethodDefinition tryMethod = defineTryMethod(
                        innerExpressionVisitor,
                        containerClassDefinition,
                        "try_" + counter,
                        inputParameters,
                        Primitives.wrap(tryExpression.getType().getJavaType()),
                        tryExpression,
                        callSiteBinder);

                tryMethodMap.put(tryExpression, tryMethod);
            }
            else if (expression instanceof LambdaDefinitionExpression) {
                LambdaDefinitionExpression lambdaExpression = (LambdaDefinitionExpression) expression;
                PreGeneratedExpressions preGeneratedExpressions = new PreGeneratedExpressions(tryMethodMap.build(), lambdaFieldMap.build());
                FieldDefinition methodHandleField = LambdaBytecodeGenerator.preGenerateLambdaExpression(
                        lambdaExpression,
                        "lambda_" + counter,
                        containerClassDefinition,
                        preGeneratedExpressions,
                        callSiteBinder,
                        cachedInstanceBinder,
                        metadata.getFunctionRegistry());
                lambdaFieldMap.put(lambdaExpression, methodHandleField);
            }
            else {
                throw new VerifyException(format("unexpected expression: %s", expression.toString()));
            }
            counter++;
        }

        return new PreGeneratedExpressions(tryMethodMap.build(), lambdaFieldMap.build());
    }

    private static void generateToString(ClassDefinition classDefinition, CallSiteBinder callSiteBinder, String string)
    {
        // bind constant via invokedynamic to avoid constant pool issues due to large strings
        classDefinition.declareMethod(a(PUBLIC), "toString", type(String.class))
                .getBody()
                .append(invoke(callSiteBinder.bind(string, String.class), "toString"))
                .retObject();
    }

    @FunctionalInterface
    public interface JoinFilterFunctionFactory
    {
        JoinFilterFunction create(ConnectorSession session, LongArrayList addresses, List<List<Block>> channels);
    }

    private static RowExpressionVisitor<Scope, BytecodeNode> fieldReferenceCompiler(
            final CallSiteBinder callSiteBinder,
            final Variable leftPosition,
            final Variable leftBlocks,
            final Variable rightPosition,
            final Variable rightBlocks,
            final int leftBlocksSize)
    {
        return new InputReferenceCompiler(
                (scope, field) -> field < leftBlocksSize ? leftBlocks.getElement(field) : rightBlocks.getElement(field - leftBlocksSize),
                (scope, field) -> field < leftBlocksSize ? leftPosition : rightPosition,
                callSiteBinder);
    }

    private static final class JoinFilterCacheKey
    {
        private final RowExpression filter;
        private final int leftBlocksSize;

        public JoinFilterCacheKey(RowExpression filter, int leftBlocksSize)
        {
            this.filter = requireNonNull(filter, "filter can not be null");
            this.leftBlocksSize = leftBlocksSize;
        }

        public RowExpression getFilter()
        {
            return filter;
        }

        public int getLeftBlocksSize()
        {
            return leftBlocksSize;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            JoinFilterCacheKey that = (JoinFilterCacheKey) o;
            return leftBlocksSize == that.leftBlocksSize &&
                    Objects.equals(filter, that.filter);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(filter, leftBlocksSize);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("filter", filter)
                    .add("leftBlocksSize", leftBlocksSize)
                    .toString();
        }
    }

    private static class IsolatedJoinFilterFunctionFactory
            implements JoinFilterFunctionFactory
    {
        private final Constructor<? extends InternalJoinFilterFunction> internalJoinFilterFunctionConstructor;
        private final Constructor<? extends JoinFilterFunction> isolatedJoinFilterFunctionConstructor;

        public IsolatedJoinFilterFunctionFactory(Class<? extends InternalJoinFilterFunction> internalJoinFilterFunction)
        {
            try {
                internalJoinFilterFunctionConstructor = internalJoinFilterFunction
                        .getConstructor(ConnectorSession.class);

                Class<? extends JoinFilterFunction> isolatedJoinFilterFunction = IsolatedClass.isolateClass(
                        new DynamicClassLoader(getClass().getClassLoader()),
                        JoinFilterFunction.class,
                        StandardJoinFilterFunction.class);
                isolatedJoinFilterFunctionConstructor = isolatedJoinFilterFunction.getConstructor(InternalJoinFilterFunction.class, LongArrayList.class, List.class);
            }
            catch (NoSuchMethodException e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        public JoinFilterFunction create(ConnectorSession session, LongArrayList addresses, List<List<Block>> channels)
        {
            try {
                InternalJoinFilterFunction internalJoinFilterFunction = internalJoinFilterFunctionConstructor.newInstance(session);
                return isolatedJoinFilterFunctionConstructor.newInstance(internalJoinFilterFunction, addresses, channels);
            }
            catch (ReflectiveOperationException e) {
                throw Throwables.propagate(e);
            }
        }
    }
}

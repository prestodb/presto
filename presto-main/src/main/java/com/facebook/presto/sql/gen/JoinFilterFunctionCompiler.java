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
import com.facebook.presto.bytecode.control.IfStatement;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.JoinFilterFunction;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.sql.relational.CallExpression;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.sql.relational.RowExpressionVisitor;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Primitives;
import com.google.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Objects;

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
import static com.facebook.presto.sql.gen.TryCodeGenerator.defineTryMethod;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class JoinFilterFunctionCompiler
{
    private final Metadata metadata;

    @Inject
    public JoinFilterFunctionCompiler(Metadata metadata)
    {
        this.metadata = metadata;
    }

    private final LoadingCache<JoinFilterCacheKey, Class<? extends JoinFilterFunction>> joinFilterFunctions = CacheBuilder.newBuilder().maximumSize(1000).build(
            new CacheLoader<JoinFilterCacheKey, Class<? extends JoinFilterFunction>>()
            {
                @Override
                public Class<? extends JoinFilterFunction> load(JoinFilterCacheKey key)
                        throws Exception
                {
                    return compileFilterFunctionInternal(key.getFilter(), key.getLeftBlocksSize());
                }
            });

    public JoinFilterFunctionFactory compileJoinFilterFunction(RowExpression filter, int leftBlocksSize)
    {
        Class<? extends JoinFilterFunction> joinFilterFunction = joinFilterFunctions.getUnchecked(new JoinFilterCacheKey(filter, leftBlocksSize));
        return (session) -> {
            try {
                return joinFilterFunction.getConstructor(ConnectorSession.class).newInstance(session);
            }
            catch (ReflectiveOperationException e) {
                throw Throwables.propagate(e);
            }
        };
    }

    private Class<? extends JoinFilterFunction> compileFilterFunctionInternal(RowExpression filterExpression, int leftBlocksSize)
    {
        ClassDefinition classDefinition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName("JoinFilterFunction"),
                type(Object.class),
                type(JoinFilterFunction.class));

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

        return defineClass(classDefinition, JoinFilterFunction.class, callSiteBinder.getBindings(), getClass().getClassLoader());
    }

    private void generateMethods(ClassDefinition classDefinition, CallSiteBinder callSiteBinder, RowExpression filter, int leftBlocksSize)
    {
        CachedInstanceBinder cachedInstanceBinder = new CachedInstanceBinder(classDefinition, callSiteBinder);

        FieldDefinition sessionField = classDefinition.declareField(a(PRIVATE, FINAL), "session", ConnectorSession.class);
        generateConstructor(classDefinition, sessionField);
        generateFilterMethod(classDefinition, callSiteBinder, cachedInstanceBinder, filter, leftBlocksSize, sessionField);
    }

    private void generateConstructor(ClassDefinition classDefinition, FieldDefinition sessionField)
    {
        Parameter sessionParameter = arg("session", ConnectorSession.class);
        MethodDefinition constructorDefinition = classDefinition.declareConstructor(a(PUBLIC), sessionParameter);

        BytecodeBlock body = constructorDefinition.getBody();
        Variable thisVariable = constructorDefinition.getThis();

        body.comment("super();")
                .append(thisVariable)
                .invokeConstructor(Object.class);

        body.append(thisVariable.setField(sessionField, sessionParameter));
        body.ret();
    }

    private void generateFilterMethod(ClassDefinition classDefinition, CallSiteBinder callSiteBinder, CachedInstanceBinder cachedInstanceBinder, RowExpression filter, int leftBlocksSize, FieldDefinition sessionField)
    {
        Map<CallExpression, MethodDefinition> tryMethodMap = generateTryMethods(classDefinition, callSiteBinder, cachedInstanceBinder, leftBlocksSize, filter);

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
                fieldReferenceCompiler(callSiteBinder, leftPosition, leftBlocks, rightPosition, rightBlocks, leftBlocksSize, wasNullVariable),
                metadata.getFunctionRegistry(),
                tryMethodMap);

        BytecodeNode visitorBody = filter.accept(visitor, scope);

        Variable result = scope.declareVariable(boolean.class, "result");
        body.append(visitorBody)
                .putVariable(result)
                .append(new IfStatement()
                        .condition(wasNullVariable)
                        .ifTrue(constantFalse().ret())
                        .ifFalse(result.ret()));
    }

    private Map<CallExpression, MethodDefinition> generateTryMethods(
            ClassDefinition containerClassDefinition,
            CallSiteBinder callSiteBinder,
            CachedInstanceBinder cachedInstanceBinder,
            int leftBlocksSize,
            RowExpression filter)
    {
        TryExpressionExtractor tryExtractor = new TryExpressionExtractor();
        filter.accept(tryExtractor, null);
        List<CallExpression> tryExpressions = tryExtractor.getTryExpressionsPreOrder();

        ImmutableMap.Builder<CallExpression, MethodDefinition> tryMethodMap = ImmutableMap.builder();

        int methodId = 0;
        for (CallExpression tryExpression : tryExpressions) {
            Parameter session = arg("session", ConnectorSession.class);
            Parameter leftPosition = arg("leftPosition", int.class);
            Parameter leftBlocks = arg("leftBlocks", Block[].class);
            Parameter rightPosition = arg("rightPosition", int.class);
            Parameter rightBlocks = arg("rightBlocks", Block[].class);
            Parameter wasNullVariable = arg("wasNull", boolean.class);

            BytecodeExpressionVisitor innerExpressionVisitor = new BytecodeExpressionVisitor(
                    callSiteBinder,
                    cachedInstanceBinder,
                    fieldReferenceCompiler(callSiteBinder, leftPosition, leftBlocks, rightPosition, rightBlocks, leftBlocksSize, wasNullVariable),
                    metadata.getFunctionRegistry(),
                    tryMethodMap.build());

            List<Parameter> inputParameters = ImmutableList.<Parameter>builder()
                    .add(session)
                    .add(leftPosition)
                    .add(leftBlocks)
                    .add(rightPosition)
                    .add(rightBlocks)
                    .add(wasNullVariable)
                    .build();

            MethodDefinition tryMethod = defineTryMethod(
                    innerExpressionVisitor,
                    containerClassDefinition,
                    "try_" + methodId,
                    inputParameters,
                    Primitives.wrap(tryExpression.getType().getJavaType()),
                    tryExpression,
                    callSiteBinder);

            tryMethodMap.put(tryExpression, tryMethod);
            methodId++;
        }

        return tryMethodMap.build();
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
        JoinFilterFunction create(ConnectorSession session);
    }

    private static RowExpressionVisitor<Scope, BytecodeNode> fieldReferenceCompiler(
            final CallSiteBinder callSiteBinder,
            final Variable leftPosition,
            final Variable leftBlocks,
            final Variable rightPosition,
            final Variable rightBlocks,
            final int leftBlocksSize,
            final Variable wasNullVariable)
    {
        return new InputReferenceCompiler(
                (scope, field) -> field < leftBlocksSize ? leftBlocks.getElement(field) : rightBlocks.getElement(field - leftBlocksSize),
                (scope, field) -> field < leftBlocksSize ? leftPosition : rightPosition,
                wasNullVariable,
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
}

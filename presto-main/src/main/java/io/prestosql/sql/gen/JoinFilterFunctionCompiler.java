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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.BytecodeNode;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.DynamicClassLoader;
import io.airlift.bytecode.FieldDefinition;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.control.IfStatement;
import io.prestosql.metadata.Metadata;
import io.prestosql.operator.InternalJoinFilterFunction;
import io.prestosql.operator.JoinFilterFunction;
import io.prestosql.operator.StandardJoinFilterFunction;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.sql.gen.LambdaBytecodeGenerator.CompiledLambda;
import io.prestosql.sql.relational.LambdaDefinitionExpression;
import io.prestosql.sql.relational.RowExpression;
import io.prestosql.sql.relational.RowExpressionVisitor;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.inject.Inject;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PRIVATE;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantFalse;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantInt;
import static io.prestosql.sql.gen.BytecodeUtils.invoke;
import static io.prestosql.sql.gen.LambdaExpressionExtractor.extractLambdaExpressions;
import static io.prestosql.util.CompilerUtils.defineClass;
import static io.prestosql.util.CompilerUtils.makeClassName;
import static java.util.Objects.requireNonNull;

public class JoinFilterFunctionCompiler
{
    private final Metadata metadata;

    @Inject
    public JoinFilterFunctionCompiler(Metadata metadata)
    {
        this.metadata = metadata;
    }

    private final LoadingCache<JoinFilterCacheKey, JoinFilterFunctionFactory> joinFilterFunctionFactories = CacheBuilder.newBuilder()
            .recordStats()
            .maximumSize(1000)
            .build(CacheLoader.from(key -> internalCompileFilterFunctionFactory(key.getFilter(), key.getLeftBlocksSize())));

    @Managed
    @Nested
    public CacheStatsMBean getJoinFilterFunctionFactoryStats()
    {
        return new CacheStatsMBean(joinFilterFunctionFactories);
    }

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

        Map<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap = generateMethodsForLambda(classDefinition, callSiteBinder, cachedInstanceBinder, leftBlocksSize, filter);
        generateFilterMethod(classDefinition, callSiteBinder, cachedInstanceBinder, compiledLambdaMap, filter, leftBlocksSize, sessionField);

        generateConstructor(classDefinition, sessionField, cachedInstanceBinder);
    }

    private static void generateConstructor(
            ClassDefinition classDefinition,
            FieldDefinition sessionField,
            CachedInstanceBinder cachedInstanceBinder)
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

    private void generateFilterMethod(
            ClassDefinition classDefinition,
            CallSiteBinder callSiteBinder,
            CachedInstanceBinder cachedInstanceBinder,
            Map<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap,
            RowExpression filter,
            int leftBlocksSize,
            FieldDefinition sessionField)
    {
        // int leftPosition, Page leftPage, int rightPosition, Page rightPage
        Parameter leftPosition = arg("leftPosition", int.class);
        Parameter leftPage = arg("leftPage", Page.class);
        Parameter rightPosition = arg("rightPosition", int.class);
        Parameter rightPage = arg("rightPage", Page.class);

        MethodDefinition method = classDefinition.declareMethod(
                a(PUBLIC),
                "filter",
                type(boolean.class),
                ImmutableList.<Parameter>builder()
                        .add(leftPosition)
                        .add(leftPage)
                        .add(rightPosition)
                        .add(rightPage)
                        .build());

        method.comment("filter: %s", filter.toString());
        BytecodeBlock body = method.getBody();

        Scope scope = method.getScope();
        Variable wasNullVariable = scope.declareVariable("wasNull", body, constantFalse());
        scope.declareVariable("session", body, method.getThis().getField(sessionField));

        RowExpressionCompiler compiler = new RowExpressionCompiler(
                callSiteBinder,
                cachedInstanceBinder,
                fieldReferenceCompiler(callSiteBinder, leftPosition, leftPage, rightPosition, rightPage, leftBlocksSize),
                metadata.getFunctionRegistry(),
                compiledLambdaMap);

        BytecodeNode visitorBody = compiler.compile(filter, scope);

        Variable result = scope.declareVariable(boolean.class, "result");
        body.append(visitorBody)
                .putVariable(result)
                .append(new IfStatement()
                        .condition(wasNullVariable)
                        .ifTrue(constantFalse().ret())
                        .ifFalse(result.ret()));
    }

    private Map<LambdaDefinitionExpression, CompiledLambda> generateMethodsForLambda(
            ClassDefinition containerClassDefinition,
            CallSiteBinder callSiteBinder,
            CachedInstanceBinder cachedInstanceBinder,
            int leftBlocksSize,
            RowExpression filter)
    {
        Set<LambdaDefinitionExpression> lambdaExpressions = ImmutableSet.copyOf(extractLambdaExpressions(filter));
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
                    metadata.getFunctionRegistry());
            compiledLambdaMap.put(lambdaExpression, compiledLambda);
            counter++;
        }

        return compiledLambdaMap.build();
    }

    private static void generateToString(ClassDefinition classDefinition, CallSiteBinder callSiteBinder, String string)
    {
        // bind constant via invokedynamic to avoid constant pool issues due to large strings
        classDefinition.declareMethod(a(PUBLIC), "toString", type(String.class))
                .getBody()
                .append(invoke(callSiteBinder.bind(string, String.class), "toString"))
                .retObject();
    }

    public interface JoinFilterFunctionFactory
    {
        JoinFilterFunction create(ConnectorSession session, LongArrayList addresses, List<Page> pages);
    }

    private static RowExpressionVisitor<BytecodeNode, Scope> fieldReferenceCompiler(
            final CallSiteBinder callSiteBinder,
            final Variable leftPosition,
            final Variable leftPage,
            final Variable rightPosition,
            final Variable rightPage,
            final int leftBlocksSize)
    {
        return new InputReferenceCompiler(
                (scope, field) -> {
                    if (field < leftBlocksSize) {
                        return leftPage.invoke("getBlock", Block.class, constantInt(field));
                    }
                    return rightPage.invoke("getBlock", Block.class, constantInt(field - leftBlocksSize));
                },
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
            return Objects.hash(leftBlocksSize, filter);
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
                throw new RuntimeException(e);
            }
        }

        @Override
        public JoinFilterFunction create(ConnectorSession session, LongArrayList addresses, List<Page> pages)
        {
            try {
                InternalJoinFilterFunction internalJoinFilterFunction = internalJoinFilterFunctionConstructor.newInstance(session);
                return isolatedJoinFilterFunctionConstructor.newInstance(internalJoinFilterFunction, addresses, pages);
            }
            catch (ReflectiveOperationException e) {
                throw new RuntimeException(e);
            }
        }
    }
}

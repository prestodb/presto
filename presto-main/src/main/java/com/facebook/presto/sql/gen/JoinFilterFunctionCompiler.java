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
import com.facebook.presto.bytecode.CallSiteBinder;
import com.facebook.presto.bytecode.ClassDefinition;
import com.facebook.presto.bytecode.DynamicClassLoader;
import com.facebook.presto.bytecode.FieldDefinition;
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.Parameter;
import com.facebook.presto.bytecode.Scope;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.control.IfStatement;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.array.AdaptiveLongBigArray;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.InternalJoinFilterFunction;
import com.facebook.presto.operator.JoinFilterFunction;
import com.facebook.presto.operator.StandardJoinFilterFunction;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.sql.gen.LambdaBytecodeGenerator.CompiledLambda;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.inject.Inject;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.bytecode.Access.FINAL;
import static com.facebook.presto.bytecode.Access.PRIVATE;
import static com.facebook.presto.bytecode.Access.PUBLIC;
import static com.facebook.presto.bytecode.Access.a;
import static com.facebook.presto.bytecode.Parameter.arg;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantFalse;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantInt;
import static com.facebook.presto.sql.gen.BytecodeUtils.invoke;
import static com.facebook.presto.sql.gen.LambdaBytecodeGenerator.generateMethodsForLambda;
import static com.facebook.presto.util.CompilerUtils.defineClass;
import static com.facebook.presto.util.CompilerUtils.makeClassName;
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

    private final LoadingCache<JoinFilterCacheKey, JoinFilterFunctionFactory> joinFilterFunctionFactories = CacheBuilder.newBuilder()
            .recordStats()
            .maximumSize(1000)
            .build(CacheLoader.from(key -> internalCompileFilterFunctionFactory(key.getSqlFunctionProperties(), key.getSessionFunctions(), key.getFilter(), key.getLeftBlocksSize())));

    @Managed
    @Nested
    public CacheStatsMBean getJoinFilterFunctionFactoryStats()
    {
        return new CacheStatsMBean(joinFilterFunctionFactories);
    }

    public JoinFilterFunctionFactory compileJoinFilterFunction(
            SqlFunctionProperties sqlFunctionProperties,
            Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions,
            RowExpression filter,
            int leftBlocksSize)
    {
        return joinFilterFunctionFactories.getUnchecked(new JoinFilterCacheKey(sqlFunctionProperties, sessionFunctions, filter, leftBlocksSize));
    }

    private JoinFilterFunctionFactory internalCompileFilterFunctionFactory(
            SqlFunctionProperties sqlFunctionProperties,
            Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions,
            RowExpression filterExpression,
            int leftBlocksSize)
    {
        Class<? extends InternalJoinFilterFunction> internalJoinFilterFunction = compileInternalJoinFilterFunction(
                sqlFunctionProperties,
                sessionFunctions,
                filterExpression,
                leftBlocksSize);
        return new IsolatedJoinFilterFunctionFactory(internalJoinFilterFunction);
    }

    private Class<? extends InternalJoinFilterFunction> compileInternalJoinFilterFunction(
            SqlFunctionProperties sqlFunctionProperties,
            Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions,
            RowExpression filterExpression,
            int leftBlocksSize)
    {
        ClassDefinition classDefinition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName("JoinFilterFunction"),
                type(Object.class),
                type(InternalJoinFilterFunction.class));

        CallSiteBinder callSiteBinder = new CallSiteBinder();

        new JoinFilterFunctionCompiler(metadata).generateMethods(sqlFunctionProperties, sessionFunctions, classDefinition, callSiteBinder, filterExpression, leftBlocksSize);

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

    private void generateMethods(
            SqlFunctionProperties sqlFunctionProperties,
            Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions,
            ClassDefinition classDefinition,
            CallSiteBinder callSiteBinder,
            RowExpression filter,
            int leftBlocksSize)
    {
        CachedInstanceBinder cachedInstanceBinder = new CachedInstanceBinder(classDefinition, callSiteBinder);

        FieldDefinition propertiesField = classDefinition.declareField(a(PRIVATE, FINAL), "properties", SqlFunctionProperties.class);

        Map<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap = generateMethodsForLambda(
                classDefinition,
                callSiteBinder,
                cachedInstanceBinder,
                filter,
                metadata,
                sqlFunctionProperties,
                sessionFunctions);
        generateFilterMethod(sqlFunctionProperties, sessionFunctions, classDefinition, callSiteBinder, cachedInstanceBinder, compiledLambdaMap, filter, leftBlocksSize, propertiesField);

        generateConstructor(classDefinition, propertiesField, cachedInstanceBinder);
    }

    private static void generateConstructor(
            ClassDefinition classDefinition,
            FieldDefinition propertiesField,
            CachedInstanceBinder cachedInstanceBinder)
    {
        Parameter propertiesParameter = arg("properties", SqlFunctionProperties.class);
        MethodDefinition constructorDefinition = classDefinition.declareConstructor(a(PUBLIC), propertiesParameter);

        BytecodeBlock body = constructorDefinition.getBody();
        Variable thisVariable = constructorDefinition.getThis();

        body.comment("super();")
                .append(thisVariable)
                .invokeConstructor(Object.class);

        body.append(thisVariable.setField(propertiesField, propertiesParameter));
        cachedInstanceBinder.generateInitializations(thisVariable, body);
        body.ret();
    }

    private void generateFilterMethod(
            SqlFunctionProperties sqlFunctionProperties,
            Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions,
            ClassDefinition classDefinition,
            CallSiteBinder callSiteBinder,
            CachedInstanceBinder cachedInstanceBinder,
            Map<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap,
            RowExpression filter,
            int leftBlocksSize,
            FieldDefinition propertiesField)
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
        scope.declareVariable("properties", body, method.getThis().getField(propertiesField));

        RowExpressionCompiler compiler = new RowExpressionCompiler(
                classDefinition,
                callSiteBinder,
                cachedInstanceBinder,
                fieldReferenceCompiler(callSiteBinder, leftPosition, leftPage, rightPosition, rightPage, leftBlocksSize),
                metadata,
                sqlFunctionProperties,
                sessionFunctions,
                compiledLambdaMap);

        BytecodeNode visitorBody = compiler.compile(filter, scope, Optional.empty());

        Variable result = scope.declareVariable(boolean.class, "result");
        body.append(visitorBody)
                .putVariable(result)
                .append(new IfStatement()
                        .condition(wasNullVariable)
                        .ifTrue(constantFalse().ret())
                        .ifFalse(result.ret()));
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
        JoinFilterFunction create(SqlFunctionProperties properties, AdaptiveLongBigArray addresses, List<Page> pages);
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
        private final SqlFunctionProperties sqlFunctionProperties;
        private final Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions;
        private final RowExpression filter;
        private final int leftBlocksSize;

        public JoinFilterCacheKey(SqlFunctionProperties sqlFunctionProperties, Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions, RowExpression filter, int leftBlocksSize)
        {
            this.sqlFunctionProperties = requireNonNull(sqlFunctionProperties, "sqlFunctionProperties is null");
            this.sessionFunctions = requireNonNull(sessionFunctions, "sessionFunctions is null");
            this.filter = requireNonNull(filter, "filter can not be null");
            this.leftBlocksSize = leftBlocksSize;
        }

        public SqlFunctionProperties getSqlFunctionProperties()
        {
            return sqlFunctionProperties;
        }

        public Map<SqlFunctionId, SqlInvokedFunction> getSessionFunctions()
        {
            return sessionFunctions;
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
            return Objects.equals(sqlFunctionProperties, that.sqlFunctionProperties) &&
                    leftBlocksSize == that.leftBlocksSize &&
                    Objects.equals(filter, that.filter);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(sqlFunctionProperties, leftBlocksSize, filter);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("sqlFunctionProperties", sqlFunctionProperties)
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
                        .getConstructor(SqlFunctionProperties.class);

                Class<? extends JoinFilterFunction> isolatedJoinFilterFunction = IsolatedClass.isolateClass(
                        new DynamicClassLoader(getClass().getClassLoader()),
                        JoinFilterFunction.class,
                        StandardJoinFilterFunction.class);
                isolatedJoinFilterFunctionConstructor = isolatedJoinFilterFunction.getConstructor(InternalJoinFilterFunction.class, AdaptiveLongBigArray.class, List.class);
            }
            catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public JoinFilterFunction create(SqlFunctionProperties properties, AdaptiveLongBigArray addresses, List<Page> pages)
        {
            try {
                InternalJoinFilterFunction internalJoinFilterFunction = internalJoinFilterFunctionConstructor.newInstance(properties);
                return isolatedJoinFilterFunctionConstructor.newInstance(internalJoinFilterFunction, addresses, pages);
            }
            catch (ReflectiveOperationException e) {
                throw new RuntimeException(e);
            }
        }
    }
}

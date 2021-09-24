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
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.Parameter;
import com.facebook.presto.bytecode.Scope;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.relation.Predicate;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.project.PageFieldsToInputParametersRewriter;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.PredicateCompiler;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.sql.relational.Expressions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeSet;
import java.util.function.Supplier;

import static com.facebook.presto.bytecode.Access.FINAL;
import static com.facebook.presto.bytecode.Access.PUBLIC;
import static com.facebook.presto.bytecode.Access.a;
import static com.facebook.presto.bytecode.Parameter.arg;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.and;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantFalse;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantInt;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.not;
import static com.facebook.presto.operator.project.PageFieldsToInputParametersRewriter.rewritePageFieldsToInputParameters;
import static com.facebook.presto.spi.StandardErrorCode.COMPILER_ERROR;
import static com.facebook.presto.sql.gen.BytecodeUtils.invoke;
import static com.facebook.presto.sql.gen.LambdaBytecodeGenerator.generateMethodsForLambda;
import static com.facebook.presto.util.CompilerUtils.defineClass;
import static com.facebook.presto.util.CompilerUtils.makeClassName;
import static java.util.Objects.requireNonNull;

public class RowExpressionPredicateCompiler
        implements PredicateCompiler
{
    private final Metadata metadata;
    private final LoadingCache<CacheKey, Supplier<Predicate>> predicateCache;

    @Inject
    public RowExpressionPredicateCompiler(Metadata metadata)
    {
        this(requireNonNull(metadata, "metadata is null"), 10_000);
    }

    public RowExpressionPredicateCompiler(Metadata metadata, int predicateCacheSize)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");

        if (predicateCacheSize > 0) {
            predicateCache = CacheBuilder.newBuilder()
                    .recordStats()
                    .maximumSize(predicateCacheSize)
                    .build(CacheLoader.from(cacheKey -> compilePredicateInternal(cacheKey.sqlFunctionProperties, cacheKey.sessionFunctions, cacheKey.rowExpression)));
        }
        else {
            predicateCache = null;
        }
    }

    @Override
    public Supplier<Predicate> compilePredicate(SqlFunctionProperties sqlFunctionProperties, Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions, RowExpression predicate)
    {
        if (predicateCache == null) {
            return compilePredicateInternal(sqlFunctionProperties, sessionFunctions, predicate);
        }
        return predicateCache.getUnchecked(new CacheKey(sqlFunctionProperties, sessionFunctions, predicate));
    }

    private Supplier<Predicate> compilePredicateInternal(SqlFunctionProperties sqlFunctionProperties, Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions, RowExpression predicate)
    {
        requireNonNull(predicate, "predicate is null");

        PageFieldsToInputParametersRewriter.Result result = rewritePageFieldsToInputParameters(predicate);
        int[] inputChannels = result.getInputChannels().getInputChannels().stream()
                .mapToInt(Integer::intValue)
                .toArray();

        CallSiteBinder callSiteBinder = new CallSiteBinder();
        ClassDefinition classDefinition = definePredicateClass(sqlFunctionProperties, sessionFunctions, result.getRewrittenExpression(), inputChannels, callSiteBinder);

        Class<? extends Predicate> predicateClass;
        try {
            predicateClass = defineClass(classDefinition, Predicate.class, callSiteBinder.getBindings(), getClass().getClassLoader());
        }
        catch (Exception e) {
            throw new PrestoException(COMPILER_ERROR, predicate.toString(), e.getCause());
        }

        return () -> {
            try {
                return predicateClass.getConstructor().newInstance();
            }
            catch (ReflectiveOperationException e) {
                throw new PrestoException(COMPILER_ERROR, e);
            }
        };
    }

    private ClassDefinition definePredicateClass(
            SqlFunctionProperties sqlFunctionProperties,
            Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions,
            RowExpression predicate,
            int[] inputChannels,
            CallSiteBinder callSiteBinder)
    {
        ClassDefinition classDefinition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName(Predicate.class.getSimpleName(), Optional.empty()),
                type(Object.class),
                type(Predicate.class));

        CachedInstanceBinder cachedInstanceBinder = new CachedInstanceBinder(classDefinition, callSiteBinder);

        generatePredicateMethod(sqlFunctionProperties, sessionFunctions, classDefinition, callSiteBinder, cachedInstanceBinder, predicate);

        // getInputChannels
        classDefinition.declareMethod(a(PUBLIC), "getInputChannels", type(int[].class))
                .getBody()
                .append(invoke(callSiteBinder.bind(inputChannels, int[].class), "getInputChannels"))
                .retObject();

        // constructor
        generateConstructor(classDefinition, cachedInstanceBinder);

        return classDefinition;
    }

    private MethodDefinition generatePredicateMethod(
            SqlFunctionProperties sqlFunctionProperties,
            Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions,
            ClassDefinition classDefinition,
            CallSiteBinder callSiteBinder,
            CachedInstanceBinder cachedInstanceBinder,
            RowExpression predicate)
    {
        Map<LambdaDefinitionExpression, LambdaBytecodeGenerator.CompiledLambda> compiledLambdaMap = generateMethodsForLambda(
                classDefinition,
                callSiteBinder,
                cachedInstanceBinder,
                predicate,
                metadata,
                sqlFunctionProperties,
                sessionFunctions);

        Parameter properties = arg("properties", SqlFunctionProperties.class);
        Parameter page = arg("page", Page.class);
        Parameter position = arg("position", int.class);

        MethodDefinition method = classDefinition.declareMethod(
                a(PUBLIC),
                "evaluate",
                type(boolean.class),
                ImmutableList.<Parameter>builder()
                        .add(properties)
                        .add(page)
                        .add(position)
                        .build());

        method.comment("Predicate: %s", predicate.toString());

        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();

        declareBlockVariables(predicate, page, scope, body);

        Variable wasNullVariable = scope.declareVariable("wasNull", body, constantFalse());
        RowExpressionCompiler compiler = new RowExpressionCompiler(
                classDefinition,
                callSiteBinder,
                cachedInstanceBinder,
                fieldReferenceCompiler(callSiteBinder),
                metadata,
                sqlFunctionProperties,
                sessionFunctions,
                compiledLambdaMap);

        Variable result = scope.declareVariable(boolean.class, "result");
        body.append(compiler.compile(predicate, scope, Optional.empty()))
                // store result so we can check for null
                .putVariable(result)
                .append(and(not(wasNullVariable), result).ret());
        return method;
    }

    private static void declareBlockVariables(RowExpression expression, Parameter page, Scope scope, BytecodeBlock body)
    {
        for (int channel : getInputChannels(expression)) {
            scope.declareVariable("block_" + channel, body, page.invoke("getBlock", Block.class, constantInt(channel)));
        }
    }

    private static List<Integer> getInputChannels(Iterable<RowExpression> expressions)
    {
        TreeSet<Integer> channels = new TreeSet<>();
        for (RowExpression expression : Expressions.subExpressions(expressions)) {
            if (expression instanceof InputReferenceExpression) {
                channels.add(((InputReferenceExpression) expression).getField());
            }
        }
        return ImmutableList.copyOf(channels);
    }

    private static List<Integer> getInputChannels(RowExpression expression)
    {
        return getInputChannels(ImmutableList.of(expression));
    }

    private static RowExpressionVisitor<BytecodeNode, Scope> fieldReferenceCompiler(CallSiteBinder callSiteBinder)
    {
        return new InputReferenceCompiler(
                (scope, field) -> scope.getVariable("block_" + field),
                (scope, field) -> scope.getVariable("position"),
                callSiteBinder);
    }

    private static void generateConstructor(
            ClassDefinition classDefinition,
            CachedInstanceBinder cachedInstanceBinder)
    {
        MethodDefinition constructorDefinition = classDefinition.declareConstructor(a(PUBLIC));

        BytecodeBlock body = constructorDefinition.getBody();
        Variable thisVariable = constructorDefinition.getThis();

        body.comment("super();")
                .append(thisVariable)
                .invokeConstructor(Object.class);

        cachedInstanceBinder.generateInitializations(thisVariable, body);
        body.ret();
    }

    private static final class CacheKey
    {
        private final SqlFunctionProperties sqlFunctionProperties;
        private final Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions;
        private final RowExpression rowExpression;

        private CacheKey(SqlFunctionProperties sqlFunctionProperties, Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions, RowExpression rowExpression)
        {
            this.sqlFunctionProperties = requireNonNull(sqlFunctionProperties, "sqlFunctionProperties is null");
            this.sessionFunctions = requireNonNull(sessionFunctions, "sessionFunctions is null");
            this.rowExpression = requireNonNull(rowExpression, "rowExpression is null");
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (!(o instanceof CacheKey)) {
                return false;
            }
            CacheKey that = (CacheKey) o;
            return Objects.equals(sqlFunctionProperties, that.sqlFunctionProperties) &&
                    Objects.equals(rowExpression, that.rowExpression);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(sqlFunctionProperties, rowExpression);
        }
    }
}

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
import com.facebook.presto.bytecode.Scope;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.project.PageFieldsToInputParametersRewriter;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.Predicate;
import com.facebook.presto.spi.relation.PredicateCompiler;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.sql.relational.Expressions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
import static com.facebook.presto.sql.gen.LambdaExpressionExtractor.extractLambdaExpressions;
import static com.facebook.presto.util.CompilerUtils.defineClass;
import static com.facebook.presto.util.CompilerUtils.makeClassName;
import static java.util.Objects.requireNonNull;

public class RowExpressionPredicateCompiler
        implements PredicateCompiler
{
    private final FunctionManager functionManager;

    private final LoadingCache<RowExpression, Supplier<Predicate>> predicateCache;

    @Inject
    public RowExpressionPredicateCompiler(Metadata metadata)
    {
        this(requireNonNull(metadata, "metadata is null").getFunctionManager(), 10_000);
    }

    public RowExpressionPredicateCompiler(FunctionManager functionManager, int predicateCacheSize)
    {
        this.functionManager = requireNonNull(functionManager, "functionManager is null");

        if (predicateCacheSize > 0) {
            predicateCache = CacheBuilder.newBuilder()
                    .recordStats()
                    .maximumSize(predicateCacheSize)
                    .build(CacheLoader.from(this::compilePredicateInternal));
        }
        else {
            predicateCache = null;
        }
    }

    @Override
    public Supplier<Predicate> compilePredicate(RowExpression predicate)
    {
        if (predicateCache == null) {
            return compilePredicateInternal(predicate);
        }
        return predicateCache.getUnchecked(predicate);
    }

    private Supplier<Predicate> compilePredicateInternal(RowExpression predicate)
    {
        requireNonNull(predicate, "predicate is null");

        PageFieldsToInputParametersRewriter.Result result = rewritePageFieldsToInputParameters(predicate);
        int[] inputChannels = result.getInputChannels().getInputChannels().stream()
                .mapToInt(Integer::intValue)
                .toArray();

        CallSiteBinder callSiteBinder = new CallSiteBinder();
        ClassDefinition classDefinition = definePredicateClass(result.getRewrittenExpression(), inputChannels, callSiteBinder);

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

    private ClassDefinition definePredicateClass(RowExpression predicate, int[] inputChannels, CallSiteBinder callSiteBinder)
    {
        ClassDefinition classDefinition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName(Predicate.class.getSimpleName(), Optional.empty()),
                type(Object.class),
                type(Predicate.class));

        CachedInstanceBinder cachedInstanceBinder = new CachedInstanceBinder(classDefinition, callSiteBinder);

        generatePredicateMethod(classDefinition, callSiteBinder, cachedInstanceBinder, predicate);

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
            ClassDefinition classDefinition,
            CallSiteBinder callSiteBinder,
            CachedInstanceBinder cachedInstanceBinder,
            RowExpression predicate)
    {
        Map<LambdaDefinitionExpression, LambdaBytecodeGenerator.CompiledLambda> compiledLambdaMap = generateMethodsForLambda(classDefinition, callSiteBinder, cachedInstanceBinder, predicate);

        Parameter session = arg("session", ConnectorSession.class);
        Parameter page = arg("page", Page.class);
        Parameter position = arg("position", int.class);

        MethodDefinition method = classDefinition.declareMethod(
                a(PUBLIC),
                "evaluate",
                type(boolean.class),
                ImmutableList.<Parameter>builder()
                        .add(session)
                        .add(page)
                        .add(position)
                        .build());

        method.comment("Predicate: %s", predicate.toString());

        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();

        declareBlockVariables(predicate, page, scope, body);

        Variable wasNullVariable = scope.declareVariable("wasNull", body, constantFalse());
        RowExpressionCompiler compiler = new RowExpressionCompiler(
                callSiteBinder,
                cachedInstanceBinder,
                fieldReferenceCompiler(callSiteBinder),
                functionManager,
                compiledLambdaMap);

        Variable result = scope.declareVariable(boolean.class, "result");
        body.append(compiler.compile(predicate, scope, Optional.empty()))
                // store result so we can check for null
                .putVariable(result)
                .append(and(not(wasNullVariable), result).ret());
        return method;
    }

    private Map<LambdaDefinitionExpression, LambdaBytecodeGenerator.CompiledLambda> generateMethodsForLambda(
            ClassDefinition containerClassDefinition,
            CallSiteBinder callSiteBinder,
            CachedInstanceBinder cachedInstanceBinder,
            RowExpression expression)
    {
        Set<LambdaDefinitionExpression> lambdaExpressions = ImmutableSet.copyOf(extractLambdaExpressions(expression));
        ImmutableMap.Builder<LambdaDefinitionExpression, LambdaBytecodeGenerator.CompiledLambda> compiledLambdaMap = ImmutableMap.builder();

        int counter = 0;
        for (LambdaDefinitionExpression lambdaExpression : lambdaExpressions) {
            LambdaBytecodeGenerator.CompiledLambda compiledLambda = LambdaBytecodeGenerator.preGenerateLambdaExpression(
                    lambdaExpression,
                    "lambda_" + counter,
                    containerClassDefinition,
                    compiledLambdaMap.build(),
                    callSiteBinder,
                    cachedInstanceBinder,
                    functionManager);
            compiledLambdaMap.put(lambdaExpression, compiledLambda);
            counter++;
        }

        return compiledLambdaMap.build();
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
}

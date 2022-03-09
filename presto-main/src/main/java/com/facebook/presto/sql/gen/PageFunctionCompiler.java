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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.BytecodeNode;
import com.facebook.presto.bytecode.CallSiteBinder;
import com.facebook.presto.bytecode.ClassDefinition;
import com.facebook.presto.bytecode.FieldDefinition;
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.Parameter;
import com.facebook.presto.bytecode.ParameterizedType;
import com.facebook.presto.bytecode.Scope;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.control.ForLoop;
import com.facebook.presto.bytecode.control.IfStatement;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.Work;
import com.facebook.presto.operator.project.ConstantPageProjection;
import com.facebook.presto.operator.project.GeneratedPageProjection;
import com.facebook.presto.operator.project.InputChannels;
import com.facebook.presto.operator.project.InputPageProjection;
import com.facebook.presto.operator.project.PageFieldsToInputParametersRewriter;
import com.facebook.presto.operator.project.PageFilter;
import com.facebook.presto.operator.project.PageProjection;
import com.facebook.presto.operator.project.PageProjectionWithOutputs;
import com.facebook.presto.operator.project.SelectedPositions;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.gen.LambdaBytecodeGenerator.CompiledLambda;
import com.facebook.presto.sql.planner.CompilerConfig;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Primitives;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeSet;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static com.facebook.presto.bytecode.Access.FINAL;
import static com.facebook.presto.bytecode.Access.PRIVATE;
import static com.facebook.presto.bytecode.Access.PUBLIC;
import static com.facebook.presto.bytecode.Access.a;
import static com.facebook.presto.bytecode.Parameter.arg;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.add;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.and;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantBoolean;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantFalse;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantInt;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantNull;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.invokeStatic;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.lessThan;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.newArray;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.not;
import static com.facebook.presto.operator.project.PageFieldsToInputParametersRewriter.rewritePageFieldsToInputParameters;
import static com.facebook.presto.spi.StandardErrorCode.COMPILER_ERROR;
import static com.facebook.presto.sql.gen.BytecodeUtils.boxPrimitiveIfNecessary;
import static com.facebook.presto.sql.gen.BytecodeUtils.invoke;
import static com.facebook.presto.sql.gen.BytecodeUtils.unboxPrimitiveIfNecessary;
import static com.facebook.presto.sql.gen.CommonSubExpressionRewriter.CommonSubExpressionFields;
import static com.facebook.presto.sql.gen.CommonSubExpressionRewriter.CommonSubExpressionFields.declareCommonSubExpressionFields;
import static com.facebook.presto.sql.gen.CommonSubExpressionRewriter.CommonSubExpressionFields.initializeCommonSubExpressionFields;
import static com.facebook.presto.sql.gen.CommonSubExpressionRewriter.collectCSEByLevel;
import static com.facebook.presto.sql.gen.CommonSubExpressionRewriter.getExpressionsPartitionedByCSE;
import static com.facebook.presto.sql.gen.CommonSubExpressionRewriter.rewriteExpressionWithCSE;
import static com.facebook.presto.sql.gen.LambdaBytecodeGenerator.generateMethodsForLambda;
import static com.facebook.presto.sql.relational.Expressions.subExpressions;
import static com.facebook.presto.util.CompilerUtils.defineClass;
import static com.facebook.presto.util.CompilerUtils.makeClassName;
import static com.facebook.presto.util.Reflection.constructorMethodHandle;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

public class PageFunctionCompiler
{
    private static Logger log = Logger.get(PageFunctionCompiler.class);
    // Benchmark and experiments showed that when we put too many projections into a single PageProjection, performance degrades. Flamechart shows that a lot of cpus are
    // spent in evaluate. The root cause is not well understood. Maybe when the function is too large JIT has problem optimizing it. Empirical evidence shows that when there
    // are less than 10 projections performance is generally better with common sub-expressions. So we set an upper limit on how many projections we would compile together here.
    private static final int MAX_PROJECTION_GROUP_SIZE = 10;

    private final Metadata metadata;
    private final DeterminismEvaluator determinismEvaluator;

    private final LoadingCache<CacheKey, Supplier<PageProjection>> projectionCache;
    private final LoadingCache<CacheKey, Supplier<PageFilter>> filterCache;

    private final CacheStatsMBean projectionCacheStats;
    private final CacheStatsMBean filterCacheStats;

    @Inject
    public PageFunctionCompiler(Metadata metadata, CompilerConfig config)
    {
        this(metadata, requireNonNull(config, "config is null").getExpressionCacheSize());
    }

    public PageFunctionCompiler(Metadata metadata, int expressionCacheSize)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.determinismEvaluator = new RowExpressionDeterminismEvaluator(metadata.getFunctionAndTypeManager());

        if (expressionCacheSize > 0) {
            projectionCache = CacheBuilder.newBuilder()
                    .recordStats()
                    .maximumSize(expressionCacheSize)
                    .build(CacheLoader.from(cacheKey -> compileProjectionInternal(cacheKey.sqlFunctionProperties, cacheKey.sessionFunctions, cacheKey.rowExpressions, cacheKey.isOptimizeCommonSubExpression, Optional.empty())));
            projectionCacheStats = new CacheStatsMBean(projectionCache);
        }
        else {
            projectionCache = null;
            projectionCacheStats = null;
        }

        if (expressionCacheSize > 0) {
            filterCache = CacheBuilder.newBuilder()
                    .recordStats()
                    .maximumSize(expressionCacheSize)
                    .build(CacheLoader.from(cacheKey -> compileFilterInternal(cacheKey.sqlFunctionProperties, cacheKey.sessionFunctions, cacheKey.rowExpressions.get(0), cacheKey.isOptimizeCommonSubExpression, Optional.empty())));
            filterCacheStats = new CacheStatsMBean(filterCache);
        }
        else {
            filterCache = null;
            filterCacheStats = null;
        }
    }

    @Nullable
    @Managed
    @Nested
    public CacheStatsMBean getProjectionCache()
    {
        return projectionCacheStats;
    }

    @Nullable
    @Managed
    @Nested
    public CacheStatsMBean getFilterCache()
    {
        return filterCacheStats;
    }

    public List<Supplier<PageProjectionWithOutputs>> compileProjections(
            SqlFunctionProperties sqlFunctionProperties,
            Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions,
            List<? extends RowExpression> projections,
            boolean isOptimizeCommonSubExpression,
            Optional<String> classNameSuffix)
    {
        if (isOptimizeCommonSubExpression) {
            ImmutableList.Builder<Supplier<PageProjectionWithOutputs>> pageProjections = ImmutableList.builder();
            ImmutableMap.Builder<RowExpression, Integer> expressionsWithPositionBuilder = ImmutableMap.builder();
            for (int i = 0; i < projections.size(); i++) {
                RowExpression projection = projections.get(i);
                if (projection instanceof ConstantExpression || projection instanceof InputReferenceExpression) {
                    pageProjections.add(toPageProjectionWithOutputs(compileProjection(sqlFunctionProperties, sessionFunctions, projection, classNameSuffix), new int[] {i}));
                }
                else {
                    expressionsWithPositionBuilder.put(projection, i);
                }
            }
            Map<RowExpression, Integer> expressionsWithPosition = expressionsWithPositionBuilder.build();

            Map<List<RowExpression>, Boolean> projectionsPartitionedByCSE = getExpressionsPartitionedByCSE(expressionsWithPosition.keySet(), MAX_PROJECTION_GROUP_SIZE);

            for (Map.Entry<List<RowExpression>, Boolean> entry : projectionsPartitionedByCSE.entrySet()) {
                if (entry.getValue()) {
                    pageProjections.add(toPageProjectionWithOutputs(
                            compileProjectionCached(sqlFunctionProperties, sessionFunctions, entry.getKey(), true, classNameSuffix),
                            toIntArray(entry.getKey().stream().map(expressionsWithPosition::get).collect(toImmutableList()))));
                }
                else {
                    verify(entry.getKey().size() == 1, "Expect non-cse expression list to only have one element");
                    RowExpression projection = entry.getKey().get(0);
                    pageProjections.add(toPageProjectionWithOutputs(
                            compileProjection(sqlFunctionProperties, sessionFunctions, projection, classNameSuffix),
                            new int[] {expressionsWithPosition.get(projection)}));
                }
            }
            return pageProjections.build();
        }
        return IntStream.range(0, projections.size())
                .mapToObj(outputChannel -> toPageProjectionWithOutputs(
                        compileProjection(sqlFunctionProperties, sessionFunctions, projections.get(outputChannel), classNameSuffix),
                        new int[] {outputChannel}))
                .collect(toImmutableList());
    }

    @VisibleForTesting
    public List<Supplier<PageProjectionWithOutputs>> compileProjections(
            SqlFunctionProperties sqlFunctionProperties,
            List<? extends RowExpression> projections,
            boolean isOptimizeCommonSubExpression,
            Optional<String> classNameSuffix)
    {
        return compileProjections(sqlFunctionProperties, emptyMap(), projections, isOptimizeCommonSubExpression, classNameSuffix);
    }

    @VisibleForTesting
    public Supplier<PageProjection> compileProjection(
            SqlFunctionProperties sqlFunctionProperties,
            RowExpression projection,
            Optional<String> classNameSuffix)
    {
        return compileProjection(sqlFunctionProperties, emptyMap(), projection, classNameSuffix);
    }

    private Supplier<PageProjection> compileProjection(
            SqlFunctionProperties sqlFunctionProperties,
            Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions,
            RowExpression projection,
            Optional<String> classNameSuffix)
    {
        if (projection instanceof InputReferenceExpression) {
            InputReferenceExpression input = (InputReferenceExpression) projection;
            InputPageProjection projectionFunction = new InputPageProjection(input.getField());
            return () -> projectionFunction;
        }

        if (projection instanceof ConstantExpression) {
            ConstantExpression constant = (ConstantExpression) projection;
            ConstantPageProjection projectionFunction = new ConstantPageProjection(constant.getValue(), constant.getType());
            return () -> projectionFunction;
        }

        return compileProjectionCached(sqlFunctionProperties, sessionFunctions, ImmutableList.of(projection), false, classNameSuffix);
    }

    private Supplier<PageProjection> compileProjectionCached(
            SqlFunctionProperties sqlFunctionProperties,
            Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions,
            List<RowExpression> projections,
            boolean isOptimizeCommonSubExpression,
            Optional<String> classNameSuffix)
    {
        if (projectionCache == null) {
            return compileProjectionInternal(sqlFunctionProperties, sessionFunctions, projections, isOptimizeCommonSubExpression, classNameSuffix);
        }
        try {
            return projectionCache.getUnchecked(new CacheKey(sqlFunctionProperties, sessionFunctions, projections, isOptimizeCommonSubExpression));
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), PrestoException.class);
            throw e;
        }
    }

    private Supplier<PageProjectionWithOutputs> toPageProjectionWithOutputs(Supplier<PageProjection> pageProjection, int[] outputChannels)
    {
        return () -> new PageProjectionWithOutputs(pageProjection.get(), outputChannels);
    }

    private Supplier<PageProjection> compileProjectionInternal(
            SqlFunctionProperties sqlFunctionProperties,
            Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions,
            List<RowExpression> projections,
            boolean isOptimizeCommonSubExpression,
            Optional<String> classNameSuffix)
    {
        requireNonNull(projections, "projections is null");
        checkArgument(!projections.isEmpty() && projections.stream().allMatch(projection -> projection instanceof CallExpression || projection instanceof SpecialFormExpression));

        PageFieldsToInputParametersRewriter.Result result = rewritePageFieldsToInputParameters(projections);
        List<RowExpression> rewrittenExpression = result.getRewrittenExpressions();

        CallSiteBinder callSiteBinder = new CallSiteBinder();

        // generate Work
        ClassDefinition pageProjectionWorkDefinition = definePageProjectWorkClass(
                sqlFunctionProperties,
                sessionFunctions,
                rewrittenExpression,
                callSiteBinder,
                isOptimizeCommonSubExpression,
                classNameSuffix);

        Class<? extends Work> pageProjectionWorkClass;
        try {
            pageProjectionWorkClass = defineClass(pageProjectionWorkDefinition, Work.class, callSiteBinder.getBindings(), getClass().getClassLoader());
        }
        catch (PrestoException prestoException) {
            throw prestoException;
        }
        catch (Exception e) {
            throw new PrestoException(COMPILER_ERROR, e);
        }

        return () -> new GeneratedPageProjection(
                rewrittenExpression,
                rewrittenExpression.stream().allMatch(determinismEvaluator::isDeterministic),
                result.getInputChannels(),
                constructorMethodHandle(pageProjectionWorkClass, List.class, SqlFunctionProperties.class, Page.class, SelectedPositions.class));
    }

    private static ParameterizedType generateProjectionWorkClassName(Optional<String> classNameSuffix)
    {
        return makeClassName("PageProjectionWork", classNameSuffix);
    }

    private ClassDefinition definePageProjectWorkClass(
            SqlFunctionProperties sqlFunctionProperties,
            Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions,
            List<RowExpression> projections,
            CallSiteBinder callSiteBinder,
            boolean isOptimizeCommonSubExpression,
            Optional<String> classNameSuffix)
    {
        ClassDefinition classDefinition = new ClassDefinition(
                a(PUBLIC, FINAL),
                generateProjectionWorkClassName(classNameSuffix),
                type(Object.class),
                type(Work.class));

        FieldDefinition blockBuilderFields = classDefinition.declareField(a(PRIVATE), "blockBuilders", type(List.class, BlockBuilder.class));
        FieldDefinition propertiesField = classDefinition.declareField(a(PRIVATE), "properties", SqlFunctionProperties.class);
        FieldDefinition pageField = classDefinition.declareField(a(PRIVATE), "page", Page.class);
        FieldDefinition selectedPositionsField = classDefinition.declareField(a(PRIVATE), "selectedPositions", SelectedPositions.class);
        FieldDefinition nextIndexOrPositionField = classDefinition.declareField(a(PRIVATE), "nextIndexOrPosition", int.class);
        FieldDefinition resultField = classDefinition.declareField(a(PRIVATE), "result", List.class);

        CachedInstanceBinder cachedInstanceBinder = new CachedInstanceBinder(classDefinition, callSiteBinder);

        // process
        generateProcessMethod(classDefinition, blockBuilderFields, projections.size(), propertiesField, pageField, selectedPositionsField, nextIndexOrPositionField, resultField);

        // getResult
        MethodDefinition method = classDefinition.declareMethod(a(PUBLIC), "getResult", type(Object.class), ImmutableList.of());
        method.getBody().append(method.getThis().getField(resultField)).ret(Object.class);

        Map<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap = generateMethodsForLambda(classDefinition,
                callSiteBinder,
                cachedInstanceBinder,
                projections,
                metadata,
                sqlFunctionProperties,
                sessionFunctions,
                "");

        // cse
        Map<VariableReferenceExpression, CommonSubExpressionFields> cseFields = ImmutableMap.of();

        RowExpressionCompiler compiler = new RowExpressionCompiler(
                classDefinition,
                callSiteBinder,
                cachedInstanceBinder,
                new FieldAndVariableReferenceCompiler(callSiteBinder, cseFields),
                metadata,
                sqlFunctionProperties,
                sessionFunctions,
                compiledLambdaMap);

        if (isOptimizeCommonSubExpression) {
            Map<Integer, Map<RowExpression, VariableReferenceExpression>> commonSubExpressionsByLevel = collectCSEByLevel(projections);
            if (!commonSubExpressionsByLevel.isEmpty()) {
                cseFields = declareCommonSubExpressionFields(classDefinition, commonSubExpressionsByLevel);
                compiler = new RowExpressionCompiler(
                        classDefinition,
                        callSiteBinder,
                        cachedInstanceBinder,
                        new FieldAndVariableReferenceCompiler(callSiteBinder, cseFields),
                        metadata,
                        sqlFunctionProperties,
                        sessionFunctions,
                        compiledLambdaMap);

                generateCommonSubExpressionMethods(classDefinition, compiler, commonSubExpressionsByLevel, cseFields);
                Map<RowExpression, VariableReferenceExpression> commonSubExpressions = commonSubExpressionsByLevel.values().stream()
                        .flatMap(m -> m.entrySet().stream())
                        .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
                projections = projections.stream().map(projection -> rewriteExpressionWithCSE(projection, commonSubExpressions)).collect(toImmutableList());
                if (log.isDebugEnabled()) {
                    log.debug("Extracted %d common sub-expressions", commonSubExpressions.size());
                    commonSubExpressions.entrySet().forEach(entry -> log.debug("\t%s = %s", entry.getValue(), entry.getKey()));
                    log.debug("Rewrote %d projections: %s", projections.size(), Joiner.on(", ").join(projections));
                }
            }
        }

        // evaluate
        generateEvaluateMethod(classDefinition, compiler, projections, blockBuilderFields, cseFields);

        // constructor
        Parameter blockBuilders = arg("blockBuilders", type(List.class, BlockBuilder.class));
        Parameter properties = arg("properties", SqlFunctionProperties.class);
        Parameter page = arg("page", Page.class);
        Parameter selectedPositions = arg("selectedPositions", SelectedPositions.class);

        MethodDefinition constructorDefinition = classDefinition.declareConstructor(a(PUBLIC), blockBuilders, properties, page, selectedPositions);

        BytecodeBlock body = constructorDefinition.getBody();
        Variable thisVariable = constructorDefinition.getThis();

        body.comment("super();")
                .append(thisVariable)
                .invokeConstructor(Object.class)
                .append(thisVariable.setField(blockBuilderFields, invokeStatic(ImmutableList.class, "copyOf", ImmutableList.class, blockBuilders.cast(Collection.class))))
                .append(thisVariable.setField(propertiesField, properties))
                .append(thisVariable.setField(pageField, page))
                .append(thisVariable.setField(selectedPositionsField, selectedPositions))
                .append(thisVariable.setField(nextIndexOrPositionField, selectedPositions.invoke("getOffset", int.class)))
                .append(thisVariable.setField(resultField, constantNull(Block.class)));

        initializeCommonSubExpressionFields(cseFields.values(), thisVariable, body);

        cachedInstanceBinder.generateInitializations(thisVariable, body);
        body.ret();

        return classDefinition;
    }

    private static MethodDefinition generateProcessMethod(
            ClassDefinition classDefinition,
            FieldDefinition blockBuilders,
            int blockBuilderSize,
            FieldDefinition properties,
            FieldDefinition page,
            FieldDefinition selectedPositions,
            FieldDefinition nextIndexOrPosition,
            FieldDefinition result)
    {
        MethodDefinition method = classDefinition.declareMethod(a(PUBLIC), "process", type(boolean.class), ImmutableList.of());

        Scope scope = method.getScope();
        Variable thisVariable = method.getThis();
        BytecodeBlock body = method.getBody();

        Variable from = scope.declareVariable("from", body, thisVariable.getField(nextIndexOrPosition));
        Variable to = scope.declareVariable("to", body, add(thisVariable.getField(selectedPositions).invoke("getOffset", int.class), thisVariable.getField(selectedPositions).invoke("size", int.class)));
        Variable positions = scope.declareVariable(int[].class, "positions");
        Variable index = scope.declareVariable(int.class, "index");

        IfStatement ifStatement = new IfStatement()
                .condition(thisVariable.getField(selectedPositions).invoke("isList", boolean.class));
        body.append(ifStatement);

        ifStatement.ifTrue(new BytecodeBlock()
                .append(positions.set(thisVariable.getField(selectedPositions).invoke("getPositions", int[].class)))
                .append(new ForLoop("positions loop")
                        .initialize(index.set(from))
                        .condition(lessThan(index, to))
                        .update(index.increment())
                        .body(new BytecodeBlock()
                                .append(thisVariable.invoke("evaluate", void.class, thisVariable.getField(properties), thisVariable.getField(page), positions.getElement(index))))));

        ifStatement.ifFalse(new ForLoop("range based loop")
                .initialize(index.set(from))
                .condition(lessThan(index, to))
                .update(index.increment())
                .body(new BytecodeBlock()
                        .append(thisVariable.invoke("evaluate", void.class, thisVariable.getField(properties), thisVariable.getField(page), index))));

        Variable blocksBuilder = scope.declareVariable("blocksBuilder", body, invokeStatic(ImmutableList.class, "builder", ImmutableList.Builder.class));
        Variable iterator = scope.createTempVariable(int.class);
        ForLoop forLoop = new ForLoop("for (iterator = 0; iterator < this.blockBuilders.size(); iterator ++) blockBuildersBuilder.add(this.blockBuilders.get(iterator).builder();")
                .initialize(iterator.set(constantInt(0)))
                .condition(lessThan(iterator, constantInt(blockBuilderSize)))
                .update(iterator.increment())
                .body(new BytecodeBlock()
                        .append(blocksBuilder.invoke(
                                "add",
                                ImmutableList.Builder.class,
                                thisVariable.getField(blockBuilders).invoke("get", Object.class, iterator).cast(BlockBuilder.class).invoke("build", Block.class).cast(Object.class)).pop()));

        body.append(forLoop)
                .comment("result = blockBuildersBuilder.build(); return true")
                .append(thisVariable.setField(result, blocksBuilder.invoke("build", ImmutableList.class)))
                .push(true)
                .retBoolean();

        return method;
    }

    private List<MethodDefinition> generateCommonSubExpressionMethods(
            ClassDefinition classDefinition,
            RowExpressionCompiler compiler,
            Map<Integer, Map<RowExpression, VariableReferenceExpression>> commonSubExpressionsByLevel,
            Map<VariableReferenceExpression, CommonSubExpressionFields> commonSubExpressionFieldsMap)
    {
        ImmutableList.Builder<MethodDefinition> methods = ImmutableList.builder();

        Parameter properties = arg("properties", SqlFunctionProperties.class);
        Parameter page = arg("page", Page.class);
        Parameter position = arg("position", int.class);

        int startLevel = commonSubExpressionsByLevel.keySet().stream().reduce(Math::min).get();
        int maxLevel = commonSubExpressionsByLevel.keySet().stream().reduce(Math::max).get();
        for (int i = startLevel; i <= maxLevel; i++) {
            if (commonSubExpressionsByLevel.containsKey(i)) {
                for (Map.Entry<RowExpression, VariableReferenceExpression> entry : commonSubExpressionsByLevel.get(i).entrySet()) {
                    RowExpression cse = entry.getKey();
                    Class<?> type = Primitives.wrap(cse.getType().getJavaType());
                    VariableReferenceExpression cseVariable = entry.getValue();
                    CommonSubExpressionFields cseFields = commonSubExpressionFieldsMap.get(cseVariable);
                    MethodDefinition method = classDefinition.declareMethod(
                            a(PRIVATE),
                            "get" + cseVariable.getName(),
                            type(cseFields.getResultType()),
                            ImmutableList.<Parameter>builder()
                                    .add(properties)
                                    .add(page)
                                    .add(position)
                                    .build());

                    method.comment("cse: %s", cse);

                    Scope scope = method.getScope();
                    BytecodeBlock body = method.getBody();
                    Variable thisVariable = method.getThis();

                    declareBlockVariables(ImmutableList.of(cse), page, scope, body);
                    scope.declareVariable("wasNull", body, constantFalse());

                    IfStatement ifStatement = new IfStatement()
                            .condition(thisVariable.getField(cseFields.getEvaluatedField()))
                            .ifFalse(new BytecodeBlock()
                                    .append(thisVariable)
                                    .append(compiler.compile(cse, scope, Optional.empty()))
                                    .append(boxPrimitiveIfNecessary(scope, type))
                                    .putField(cseFields.getResultField())
                                    .append(thisVariable.setField(cseFields.getEvaluatedField(), constantBoolean(true))));

                    body.append(ifStatement)
                            .append(thisVariable)
                            .getField(cseFields.getResultField())
                            .retObject();

                    methods.add(method);
                }
            }
        }
        return methods.build();
    }

    private MethodDefinition generateEvaluateMethod(
            ClassDefinition classDefinition,
            RowExpressionCompiler compiler,
            List<RowExpression> projections,
            FieldDefinition blockBuilders,
            Map<VariableReferenceExpression, CommonSubExpressionFields> cseFields)
    {
        Parameter properties = arg("properties", SqlFunctionProperties.class);
        Parameter page = arg("page", Page.class);
        Parameter position = arg("position", int.class);

        MethodDefinition method = classDefinition.declareMethod(
                a(PUBLIC),
                "evaluate",
                type(void.class),
                ImmutableList.<Parameter>builder()
                        .add(properties)
                        .add(page)
                        .add(position)
                        .build());

        method.comment("Projections: %s", Joiner.on(", ").join(projections));

        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();
        Variable thisVariable = method.getThis();

        declareBlockVariables(projections, page, scope, body);
        Variable wasNull = scope.declareVariable("wasNull", body, constantFalse());
        cseFields.values().forEach(fields -> body.append(thisVariable.setField(fields.getEvaluatedField(), constantBoolean(false))));

        Variable outputBlockVariable = scope.createTempVariable(BlockBuilder.class);
        for (int i = 0; i < projections.size(); i++) {
            body.append(outputBlockVariable.set(thisVariable.getField(blockBuilders).invoke("get", Object.class, constantInt(i))))
                    .append(compiler.compile(projections.get(i), scope, Optional.of(outputBlockVariable)))
                    .append(constantBoolean(false))
                    .putVariable(wasNull);
        }
        body.ret();
        return method;
    }

    @VisibleForTesting
    public Supplier<PageFilter> compileFilter(
            SqlFunctionProperties sqlFunctionProperties,
            RowExpression filter,
            boolean isOptimizeCommonSubExpression,
            Optional<String> classNameSuffix)
    {
        return compileFilter(sqlFunctionProperties, emptyMap(), filter, isOptimizeCommonSubExpression, classNameSuffix);
    }

    public Supplier<PageFilter> compileFilter(
            SqlFunctionProperties sqlFunctionProperties,
            Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions,
            RowExpression filter,
            boolean isOptimizeCommonSubExpression,
            Optional<String> classNameSuffix)
    {
        if (filterCache == null) {
            return compileFilterInternal(sqlFunctionProperties, sessionFunctions, filter, isOptimizeCommonSubExpression, classNameSuffix);
        }
        try {
            return filterCache.getUnchecked(new CacheKey(sqlFunctionProperties, sessionFunctions, ImmutableList.of(filter), isOptimizeCommonSubExpression));
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), PrestoException.class);
            throw e;
        }
    }

    private Supplier<PageFilter> compileFilterInternal(
            SqlFunctionProperties sqlFunctionProperties,
            Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions,
            RowExpression filter,
            boolean isOptimizeCommonSubExpression,
            Optional<String> classNameSuffix)
    {
        requireNonNull(filter, "filter is null");

        PageFieldsToInputParametersRewriter.Result result = rewritePageFieldsToInputParameters(filter);

        CallSiteBinder callSiteBinder = new CallSiteBinder();
        ClassDefinition classDefinition = defineFilterClass(
                sqlFunctionProperties,
                sessionFunctions,
                result.getRewrittenExpression(),
                result.getInputChannels(),
                callSiteBinder,
                isOptimizeCommonSubExpression,
                classNameSuffix);

        Class<? extends PageFilter> functionClass;
        try {
            functionClass = defineClass(classDefinition, PageFilter.class, callSiteBinder.getBindings(), getClass().getClassLoader());
        }
        catch (PrestoException prestoException) {
            throw prestoException;
        }
        catch (Exception e) {
            throw new PrestoException(COMPILER_ERROR, filter.toString(), e.getCause());
        }

        return () -> {
            try {
                return functionClass.getConstructor().newInstance();
            }
            catch (ReflectiveOperationException e) {
                throw new PrestoException(COMPILER_ERROR, e);
            }
        };
    }

    private static ParameterizedType generateFilterClassName(Optional<String> classNameSuffix)
    {
        return makeClassName(PageFilter.class.getSimpleName(), classNameSuffix);
    }

    private ClassDefinition defineFilterClass(
            SqlFunctionProperties sqlFunctionProperties,
            Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions,
            RowExpression filter,
            InputChannels inputChannels,
            CallSiteBinder callSiteBinder,
            boolean isOptimizeCommonSubExpression,
            Optional<String> classNameSuffix)
    {
        ClassDefinition classDefinition = new ClassDefinition(
                a(PUBLIC, FINAL),
                generateFilterClassName(classNameSuffix),
                type(Object.class),
                type(PageFilter.class));

        CachedInstanceBinder cachedInstanceBinder = new CachedInstanceBinder(classDefinition, callSiteBinder);

        Map<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap = generateMethodsForLambda(
                classDefinition,
                callSiteBinder,
                cachedInstanceBinder,
                filter,
                metadata,
                sqlFunctionProperties,
                sessionFunctions);

        // cse
        Map<VariableReferenceExpression, CommonSubExpressionFields> cseFields = ImmutableMap.of();
        RowExpressionCompiler compiler = new RowExpressionCompiler(
                classDefinition,
                callSiteBinder,
                cachedInstanceBinder,
                new FieldAndVariableReferenceCompiler(callSiteBinder, cseFields),
                metadata,
                sqlFunctionProperties,
                sessionFunctions,
                compiledLambdaMap);

        if (isOptimizeCommonSubExpression) {
            Map<Integer, Map<RowExpression, VariableReferenceExpression>> commonSubExpressionsByLevel = collectCSEByLevel(filter);
            if (!commonSubExpressionsByLevel.isEmpty()) {
                cseFields = declareCommonSubExpressionFields(classDefinition, commonSubExpressionsByLevel);
                compiler = new RowExpressionCompiler(
                        classDefinition,
                        callSiteBinder,
                        cachedInstanceBinder,
                        new FieldAndVariableReferenceCompiler(callSiteBinder, cseFields),
                        metadata,
                        sqlFunctionProperties,
                        sessionFunctions,
                        compiledLambdaMap);

                generateCommonSubExpressionMethods(classDefinition, compiler, commonSubExpressionsByLevel, cseFields);
                Map<RowExpression, VariableReferenceExpression> commonSubExpressions = commonSubExpressionsByLevel.values().stream()
                        .flatMap(m -> m.entrySet().stream())
                        .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
                filter = rewriteExpressionWithCSE(filter, commonSubExpressions);
                if (log.isDebugEnabled()) {
                    log.debug("Extracted %d common sub-expressions", commonSubExpressions.size());
                    commonSubExpressions.entrySet().forEach(entry -> log.debug("\t%s = %s", entry.getValue(), entry.getKey()));
                    log.debug("Rewrote filter: %s", filter);
                }
            }
        }

        generateFilterMethod(classDefinition, compiler, filter, cseFields);

        FieldDefinition selectedPositions = classDefinition.declareField(a(PRIVATE), "selectedPositions", boolean[].class);
        generatePageFilterMethod(classDefinition, selectedPositions);

        // isDeterministic
        classDefinition.declareMethod(a(PUBLIC), "isDeterministic", type(boolean.class))
                .getBody()
                .append(constantBoolean(determinismEvaluator.isDeterministic(filter)))
                .retBoolean();

        // getInputChannels
        classDefinition.declareMethod(a(PUBLIC), "getInputChannels", type(InputChannels.class))
                .getBody()
                .append(invoke(callSiteBinder.bind(inputChannels, InputChannels.class), "getInputChannels"))
                .retObject();

        // toString
        String toStringResult = toStringHelper(classDefinition.getType()
                .getJavaClassName())
                .add("filter", filter)
                .toString();
        classDefinition.declareMethod(a(PUBLIC), "toString", type(String.class))
                .getBody()
                // bind constant via invokedynamic to avoid constant pool issues due to large strings
                .append(invoke(callSiteBinder.bind(toStringResult, String.class), "toString"))
                .retObject();

        // constructor
        MethodDefinition constructorDefinition = classDefinition.declareConstructor(a(PUBLIC));

        BytecodeBlock body = constructorDefinition.getBody();
        Variable thisVariable = constructorDefinition.getThis();

        body.comment("super();")
                .append(thisVariable)
                .invokeConstructor(Object.class)
                .append(thisVariable.setField(selectedPositions, newArray(type(boolean[].class), 0)));

        initializeCommonSubExpressionFields(cseFields.values(), thisVariable, body);

        cachedInstanceBinder.generateInitializations(thisVariable, body);
        body.ret();
        return classDefinition;
    }

    private static MethodDefinition generatePageFilterMethod(ClassDefinition classDefinition, FieldDefinition selectedPositionsField)
    {
        Parameter properties = arg("properties", SqlFunctionProperties.class);
        Parameter page = arg("page", Page.class);

        MethodDefinition method = classDefinition.declareMethod(
                a(PUBLIC),
                "filter",
                type(SelectedPositions.class),
                ImmutableList.<Parameter>builder()
                        .add(properties)
                        .add(page)
                        .build());

        Scope scope = method.getScope();
        Variable thisVariable = method.getThis();
        BytecodeBlock body = method.getBody();

        Variable positionCount = scope.declareVariable("positionCount", body, page.invoke("getPositionCount", int.class));

        body.append(new IfStatement("grow selectedPositions if necessary")
                .condition(lessThan(thisVariable.getField(selectedPositionsField).length(), positionCount))
                .ifTrue(thisVariable.setField(selectedPositionsField, newArray(type(boolean[].class), positionCount))));

        Variable selectedPositions = scope.declareVariable("selectedPositions", body, thisVariable.getField(selectedPositionsField));
        Variable position = scope.declareVariable(int.class, "position");
        body.append(new ForLoop()
                .initialize(position.set(constantInt(0)))
                .condition(lessThan(position, positionCount))
                .update(position.increment())
                .body(selectedPositions.setElement(position, thisVariable.invoke("filter", boolean.class, properties, page, position))));

        body.append(invokeStatic(
                PageFilter.class,
                "positionsArrayToSelectedPositions",
                SelectedPositions.class,
                selectedPositions,
                positionCount)
                .ret());

        return method;
    }

    private MethodDefinition generateFilterMethod(
            ClassDefinition classDefinition,
            RowExpressionCompiler compiler,
            RowExpression filter,
            Map<VariableReferenceExpression, CommonSubExpressionFields> cseFields)
    {
        Parameter properties = arg("properties", SqlFunctionProperties.class);
        Parameter page = arg("page", Page.class);
        Parameter position = arg("position", int.class);

        MethodDefinition method = classDefinition.declareMethod(
                a(PUBLIC),
                "filter",
                type(boolean.class),
                ImmutableList.<Parameter>builder()
                        .add(properties)
                        .add(page)
                        .add(position)
                        .build());

        method.comment("Filter: %s", filter.toString());

        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();
        Variable thisVariable = scope.getThis();

        declareBlockVariables(ImmutableList.of(filter), page, scope, body);
        cseFields.values().forEach(fields -> body.append(thisVariable.setField(fields.getEvaluatedField(), constantBoolean(false))));

        Variable wasNullVariable = scope.declareVariable("wasNull", body, constantFalse());
        Variable result = scope.declareVariable(boolean.class, "result");
        body.append(compiler.compile(filter, scope, Optional.empty()))
                // store result so we can check for null
                .putVariable(result)
                .append(and(not(wasNullVariable), result).ret());
        return method;
    }

    private static void declareBlockVariables(List<RowExpression> expressions, Parameter page, Scope scope, BytecodeBlock body)
    {
        for (int channel : getInputChannels(expressions)) {
            scope.declareVariable("block_" + channel, body, page.invoke("getBlock", Block.class, constantInt(channel)));
        }
    }

    private static List<Integer> getInputChannels(Iterable<RowExpression> expressions)
    {
        TreeSet<Integer> channels = new TreeSet<>();
        for (RowExpression expression : subExpressions(expressions)) {
            if (expression instanceof InputReferenceExpression) {
                channels.add(((InputReferenceExpression) expression).getField());
            }
        }
        return ImmutableList.copyOf(channels);
    }

    private static int[] toIntArray(List<Integer> list)
    {
        int[] array = new int[list.size()];
        for (int i = 0; i < list.size(); i++) {
            array[i] = list.get(i);
        }
        return array;
    }

    private static class FieldAndVariableReferenceCompiler
            implements RowExpressionVisitor<BytecodeNode, Scope>
    {
        private final InputReferenceCompiler inputReferenceCompiler;
        private final Map<VariableReferenceExpression, CommonSubExpressionFields> variableMap;

        public FieldAndVariableReferenceCompiler(CallSiteBinder callSiteBinder, Map<VariableReferenceExpression, CommonSubExpressionFields> variableMap)
        {
            this.inputReferenceCompiler = new InputReferenceCompiler(
                    (scope, field) -> scope.getVariable("block_" + field),
                    (scope, field) -> scope.getVariable("position"),
                    callSiteBinder);
            this.variableMap = ImmutableMap.copyOf(variableMap);
        }
        @Override
        public BytecodeNode visitCall(CallExpression call, Scope context)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public BytecodeNode visitInputReference(InputReferenceExpression reference, Scope context)
        {
            return inputReferenceCompiler.visitInputReference(reference, context);
        }

        @Override
        public BytecodeNode visitConstant(ConstantExpression literal, Scope context)
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
            CommonSubExpressionFields fields = variableMap.get(reference);
            return new BytecodeBlock()
                    .append(context.getThis().invoke(fields.getMethodName(), fields.getResultType(), context.getVariable("properties"), context.getVariable("page"), context.getVariable("position")))
                    .append(unboxPrimitiveIfNecessary(context, Primitives.wrap(reference.getType().getJavaType())));
        }

        @Override
        public BytecodeNode visitSpecialForm(SpecialFormExpression specialForm, Scope context)
        {
            throw new UnsupportedOperationException();
        }
    }

    private static final class CacheKey
    {
        private final SqlFunctionProperties sqlFunctionProperties;
        private final Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions;
        private final List<RowExpression> rowExpressions;
        private final boolean isOptimizeCommonSubExpression;

        private CacheKey(SqlFunctionProperties sqlFunctionProperties, Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions, List<RowExpression> rowExpressions, boolean isOptimizeCommonSubExpression)
        {
            requireNonNull(rowExpressions, "rowExpressions is null");
            checkArgument(rowExpressions.size() >= 1, "Expect at least one RowExpression");
            this.sqlFunctionProperties = requireNonNull(sqlFunctionProperties, "sqlFunctionProperties is null");
            this.sessionFunctions = requireNonNull(sessionFunctions, "sessionFunctions is null");
            this.rowExpressions = ImmutableList.copyOf(rowExpressions);
            this.isOptimizeCommonSubExpression = isOptimizeCommonSubExpression;
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
                    Objects.equals(rowExpressions, that.rowExpressions) &&
                    isOptimizeCommonSubExpression == that.isOptimizeCommonSubExpression;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(sqlFunctionProperties, rowExpressions, isOptimizeCommonSubExpression);
        }
    }
}

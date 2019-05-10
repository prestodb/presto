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

import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.project.CursorProcessor;
import com.facebook.presto.operator.project.PageFilter;
import com.facebook.presto.operator.project.PageProcessor;
import com.facebook.presto.operator.project.PageProjection;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.CompilationException;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Supplier;

import static com.facebook.presto.spi.StandardErrorCode.COMPILER_ERROR;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.gen.BytecodeUtils.invoke;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.util.CompilerUtils.defineClass;
import static com.facebook.presto.util.CompilerUtils.makeClassName;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.ParameterizedType.type;
import static java.util.Objects.requireNonNull;

public class ExpressionCompiler
{
    private final FunctionManager functionManager;
    private final PageFunctionCompiler pageFunctionCompiler;
    private final LoadingCache<CacheKey, Class<? extends CursorProcessor>> cursorProcessors;
    private final CacheStatsMBean cacheStatsMBean;

    @Inject
    public ExpressionCompiler(Metadata metadata, PageFunctionCompiler pageFunctionCompiler)
    {
        this.functionManager = requireNonNull(metadata, "metadata is null").getFunctionManager();
        this.pageFunctionCompiler = requireNonNull(pageFunctionCompiler, "pageFunctionCompiler is null");
        this.cursorProcessors = CacheBuilder.newBuilder()
                .recordStats()
                .maximumSize(1000)
                .build(CacheLoader.from(key -> compile(key.getFilter(), key.getProjections(), new CursorProcessorCompiler(metadata), CursorProcessor.class)));
        this.cacheStatsMBean = new CacheStatsMBean(cursorProcessors);
    }

    @Managed
    @Nested
    public CacheStatsMBean getCursorProcessorCache()
    {
        return cacheStatsMBean;
    }

    public Supplier<CursorProcessor> compileCursorProcessor(Optional<RowExpression> filter, List<? extends RowExpression> projections, Object uniqueKey)
    {
        Class<? extends CursorProcessor> cursorProcessor = cursorProcessors.getUnchecked(new CacheKey(filter, projections, uniqueKey));
        return () -> {
            try {
                return cursorProcessor.getConstructor().newInstance();
            }
            catch (ReflectiveOperationException e) {
                throw new RuntimeException(e);
            }
        };
    }

    public Supplier<PageProcessor> compilePageProcessor(Optional<RowExpression> filter, List<? extends RowExpression> projections, Optional<String> classNameSuffix)
    {
        return compilePageProcessor(filter, projections, classNameSuffix, OptionalInt.empty());
    }

    private Supplier<PageProcessor> compilePageProcessor(
            Optional<RowExpression> filter,
            List<? extends RowExpression> projections,
            Optional<String> classNameSuffix,
            OptionalInt initialBatchSize)
    {
        Optional<Supplier<PageFilter>> filterFunctionSupplier = filter.map(expression -> pageFunctionCompiler.compileFilter(expression, classNameSuffix));
        List<Supplier<PageFilter>> filterFunctionWithoutTupleDomainSuppliers = makeReorderableFilters(filter, classNameSuffix);
        List<Supplier<PageProjection>> pageProjectionSuppliers = projections.stream()
                .map(projection -> pageFunctionCompiler.compileProjection(projection, classNameSuffix))
                .collect(toImmutableList());

        return () -> {
            Optional<PageFilter> filterFunction = filterFunctionSupplier.map(Supplier::get);

            List<PageFilter> filterFunctionWithoutTupleDomain = filterFunctionWithoutTupleDomainSuppliers.stream()
                    .map(Supplier::get)
                    .collect(toImmutableList());

            List<PageProjection> pageProjections = pageProjectionSuppliers.stream()
                    .map(Supplier::get)
                    .collect(toImmutableList());
            return new PageProcessor(filterFunction, filterFunctionWithoutTupleDomain, pageProjections, initialBatchSize);
        };
    }

    public Supplier<PageProcessor> compilePageProcessor(Optional<RowExpression> filter, List<? extends RowExpression> projections)
    {
        return compilePageProcessor(filter, projections, Optional.empty());
    }

    @VisibleForTesting
    public Supplier<PageProcessor> compilePageProcessor(Optional<RowExpression> filter, List<? extends RowExpression> projections, int initialBatchSize)
    {
        return compilePageProcessor(filter, projections, Optional.empty(), OptionalInt.of(initialBatchSize));
    }

    private <T> Class<? extends T> compile(Optional<RowExpression> filter, List<RowExpression> projections, BodyCompiler bodyCompiler, Class<? extends T> superType)
    {
        // create filter and project page iterator class
        try {
            return compileProcessor(filter.orElse(constant(true, BOOLEAN)), projections, bodyCompiler, superType);
        }
        catch (CompilationException e) {
            throw new PrestoException(COMPILER_ERROR, e.getCause());
        }
    }

    private <T> Class<? extends T> compileProcessor(
            RowExpression filter,
            List<RowExpression> projections,
            BodyCompiler bodyCompiler,
            Class<? extends T> superType)
    {
        ClassDefinition classDefinition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName(superType.getSimpleName()),
                type(Object.class),
                type(superType));

        CallSiteBinder callSiteBinder = new CallSiteBinder();
        bodyCompiler.generateMethods(classDefinition, callSiteBinder, filter, projections);

        //
        // toString method
        //
        generateToString(
                classDefinition,
                callSiteBinder,
                toStringHelper(classDefinition.getType().getJavaClassName())
                        .add("filter", filter)
                        .add("projections", projections)
                        .toString());

        return defineClass(classDefinition, superType, callSiteBinder.getBindings(), getClass().getClassLoader());
    }

    private static void generateToString(ClassDefinition classDefinition, CallSiteBinder callSiteBinder, String string)
    {
        // bind constant via invokedynamic to avoid constant pool issues due to large strings
        classDefinition.declareMethod(a(PUBLIC), "toString", type(String.class))
                .getBody()
                .append(invoke(callSiteBinder.bind(string, String.class), "toString"))
                .retObject();
    }

    private static final class CacheKey
    {
        private final Optional<RowExpression> filter;
        private final List<RowExpression> projections;
        private final Object uniqueKey;

        private CacheKey(Optional<RowExpression> filter, List<? extends RowExpression> projections, Object uniqueKey)
        {
            this.filter = filter;
            this.uniqueKey = uniqueKey;
            this.projections = ImmutableList.copyOf(projections);
        }

        private Optional<RowExpression> getFilter()
        {
            return filter;
        }

        private List<RowExpression> getProjections()
        {
            return projections;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(filter, projections, uniqueKey);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            CacheKey other = (CacheKey) obj;
            return Objects.equals(this.filter, other.filter) &&
                    Objects.equals(this.projections, other.projections) &&
                    Objects.equals(this.uniqueKey, other.uniqueKey);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("filter", filter)
                    .add("projections", projections)
                    .add("uniqueKey", uniqueKey)
                    .toString();
        }
    }

    private void collectConjuncts(RowExpression expression, ImmutableList.Builder<RowExpression> conjuncts)
    {
        if (expression instanceof CallExpression && functionManager.getFunctionMetadata(((CallExpression) expression).getFunctionHandle()).getName().equals("AND")) {
            for (RowExpression argument : ((CallExpression) expression).getArguments()) {
                collectConjuncts(argument, conjuncts);
            }
        }
        else {
            conjuncts.add(expression);
        }
    }

    private void collectInputs(RowExpression expression, ImmutableSet.Builder<InputReferenceExpression> inputs)
    {
        if (expression instanceof InputReferenceExpression) {
            inputs.add((InputReferenceExpression) expression);
        }
        else if (expression instanceof CallExpression) {
            for (RowExpression argument : ((CallExpression) expression).getArguments()) {
                collectInputs(argument, inputs);
            }
        }
        else if (expression instanceof LambdaDefinitionExpression) {
            collectInputs(((LambdaDefinitionExpression) expression).getBody(), inputs);
        }
    }

    // Splits filter into top level conjuncts and groups the conjuncts
    // that depend on the same set of inputs into their own
    // PageFilter. Within each PageFilter, the conjuncts are ordered
    // left to right.
    List<Supplier<PageFilter>> makeReorderableFilters(Optional<RowExpression> filter, Optional<String> classNameSuffix)
    {
        if (!filter.isPresent()) {
            return ImmutableList.of();
        }

        ImmutableList.Builder<RowExpression> conjunctBuilder = ImmutableList.builder();
        collectConjuncts(filter.get(), conjunctBuilder);
        List<RowExpression> allConjuncts = conjunctBuilder.build();

        if (allConjuncts.size() == 1) {
            return ImmutableList.of(pageFunctionCompiler.compileFilter(filter.get(), classNameSuffix));
        }

        ImmutableList.Builder<Supplier<PageFilter>> result = ImmutableList.builder();
        CallExpression topAnd = (CallExpression) filter.get();
        Map<Set<InputReferenceExpression>, List<RowExpression>> inputsToConjuncts = new HashMap();
        for (RowExpression conjunct : allConjuncts) {
            ImmutableSet.Builder<InputReferenceExpression> inputs = ImmutableSet.builder();
            collectInputs(conjunct, inputs);
            inputsToConjuncts.computeIfAbsent(inputs.build(), k -> new ArrayList<>()).add(conjunct);
        }

        for (List<RowExpression> conjuncts : inputsToConjuncts.values()) {
            RowExpression firstConjunct = conjuncts.get(0);
            for (int i = 1; i < conjuncts.size(); i++) {
                firstConjunct = new CallExpression(topAnd.getDisplayName(), topAnd.getFunctionHandle(), topAnd.getType(), ImmutableList.of(firstConjunct, conjuncts.get(i)));
            }
            result.add(pageFunctionCompiler.compileFilter(firstConjunct, classNameSuffix));
        }
        return result.build();
    }
}

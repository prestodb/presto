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

import com.facebook.presto.bytecode.CallSiteBinder;
import com.facebook.presto.bytecode.ClassDefinition;
import com.facebook.presto.bytecode.CompilationException;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.project.CursorProcessor;
import com.facebook.presto.operator.project.PageFilter;
import com.facebook.presto.operator.project.PageProcessor;
import com.facebook.presto.operator.project.PageProjectionWithOutputs;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.relation.RowExpression;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Supplier;

import static com.facebook.presto.bytecode.Access.FINAL;
import static com.facebook.presto.bytecode.Access.PUBLIC;
import static com.facebook.presto.bytecode.Access.a;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.StandardErrorCode.COMPILER_ERROR;
import static com.facebook.presto.sql.gen.BytecodeUtils.invoke;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.util.CompilerUtils.defineClass;
import static com.facebook.presto.util.CompilerUtils.makeClassName;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

public class ExpressionCompiler
{
    private final PageFunctionCompiler pageFunctionCompiler;
    private final LoadingCache<CacheKey, Class<? extends CursorProcessor>> cursorProcessors;
    private final CacheStatsMBean cacheStatsMBean;

    @Inject
    public ExpressionCompiler(Metadata metadata, PageFunctionCompiler pageFunctionCompiler)
    {
        requireNonNull(metadata, "metadata is null");
        this.pageFunctionCompiler = requireNonNull(pageFunctionCompiler, "pageFunctionCompiler is null");
        this.cursorProcessors = CacheBuilder.newBuilder()
                .recordStats()
                .maximumSize(1000)
                .build(CacheLoader.from(key -> compile(
                        key.getSqlFunctionProperties(),
                        key.getFilter(),
                        key.getProjections(),
                        new CursorProcessorCompiler(metadata, key.isOptimizeCommonSubExpression(), key.getSessionFunctions()),
                        CursorProcessor.class)));

        this.cacheStatsMBean = new CacheStatsMBean(cursorProcessors);
    }

    @Managed
    @Nested
    public CacheStatsMBean getCursorProcessorCache()
    {
        return cacheStatsMBean;
    }

    public Supplier<CursorProcessor> compileCursorProcessor(
            SqlFunctionProperties sqlFunctionProperties,
            Optional<RowExpression> filter,
            List<? extends RowExpression> projections,
            Object uniqueKey)
    {
        return compileCursorProcessor(sqlFunctionProperties, filter, projections, uniqueKey, true);
    }

    public Supplier<CursorProcessor> compileCursorProcessor(
            SqlFunctionProperties sqlFunctionProperties,
            Optional<RowExpression> filter,
            List<? extends RowExpression> projections,
            Object uniqueKey,
            boolean isOptimizeCommonSubExpression)
    {
        return compileCursorProcessor(sqlFunctionProperties, filter, projections, uniqueKey, isOptimizeCommonSubExpression, emptyMap());
    }

    public Supplier<CursorProcessor> compileCursorProcessor(
            SqlFunctionProperties sqlFunctionProperties,
            Optional<RowExpression> filter,
            List<? extends RowExpression> projections,
            Object uniqueKey,
            boolean isOptimizeCommonSubExpression,
            Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions)
    {
        Class<? extends CursorProcessor> cursorProcessor = cursorProcessors.getUnchecked(
                new CacheKey(
                        sqlFunctionProperties,
                        sessionFunctions,
                        filter,
                        projections,
                        uniqueKey,
                        isOptimizeCommonSubExpression));
        return () -> {
            try {
                return cursorProcessor.getConstructor().newInstance();
            }
            catch (ReflectiveOperationException e) {
                throw new RuntimeException(e);
            }
        };
    }

    public Supplier<PageProcessor> compilePageProcessor(
            SqlFunctionProperties sqlFunctionProperties,
            Optional<RowExpression> filter,
            List<? extends RowExpression> projections,
            boolean isOptimizeCommonSubExpression,
            Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions,
            Optional<String> classNameSuffix)
    {
        return compilePageProcessor(sqlFunctionProperties, filter, projections, isOptimizeCommonSubExpression, sessionFunctions, classNameSuffix, OptionalInt.empty());
    }

    private Supplier<PageProcessor> compilePageProcessor(
            SqlFunctionProperties sqlFunctionProperties,
            Optional<RowExpression> filter,
            List<? extends RowExpression> projections,
            boolean isOptimizeCommonSubExpression,
            Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions,
            Optional<String> classNameSuffix,
            OptionalInt initialBatchSize)
    {
        Optional<Supplier<PageFilter>> filterFunctionSupplier = filter.map(expression ->
                pageFunctionCompiler.compileFilter(sqlFunctionProperties, sessionFunctions, expression, isOptimizeCommonSubExpression, classNameSuffix));
        List<Supplier<PageProjectionWithOutputs>> pageProjectionSuppliers = pageFunctionCompiler.compileProjections(
                sqlFunctionProperties,
                sessionFunctions,
                projections,
                isOptimizeCommonSubExpression,
                classNameSuffix);

        return () -> {
            Optional<PageFilter> filterFunction = filterFunctionSupplier.map(Supplier::get);
            List<PageProjectionWithOutputs> pageProjections = pageProjectionSuppliers.stream()
                    .map(Supplier::get)
                    .collect(toImmutableList());
            return new PageProcessor(filterFunction, pageProjections, initialBatchSize);
        };
    }

    @VisibleForTesting
    public Supplier<PageProcessor> compilePageProcessor(SqlFunctionProperties sqlFunctionProperties, Optional<RowExpression> filter, List<? extends RowExpression> projections)
    {
        return compilePageProcessor(sqlFunctionProperties, filter, projections, true, emptyMap(), Optional.empty());
    }

    @VisibleForTesting
    public Supplier<PageProcessor> compilePageProcessor(SqlFunctionProperties sqlFunctionProperties, Optional<RowExpression> filter, List<? extends RowExpression> projections, boolean isOptimizeCommonSubExpression, int initialBatchSize)
    {
        return compilePageProcessor(sqlFunctionProperties, filter, projections, isOptimizeCommonSubExpression, emptyMap(), Optional.empty(), OptionalInt.of(initialBatchSize));
    }

    @VisibleForTesting
    public Supplier<PageProcessor> compilePageProcessor(SqlFunctionProperties sqlFunctionProperties, Optional<RowExpression> filter, List<? extends RowExpression> projections, boolean isOptimizeCommonSubExpression, Optional<String> classNameSuffix)
    {
        return compilePageProcessor(sqlFunctionProperties, filter, projections, isOptimizeCommonSubExpression, emptyMap(), classNameSuffix);
    }

    private <T> Class<? extends T> compile(
            SqlFunctionProperties sqlFunctionProperties,
            Optional<RowExpression> filter,
            List<RowExpression> projections,
            BodyCompiler bodyCompiler,
            Class<? extends T> superType)
    {
        // create filter and project page iterator class
        try {
            return compileProcessor(sqlFunctionProperties, filter.orElse(constant(true, BOOLEAN)), projections, bodyCompiler, superType);
        }
        catch (CompilationException e) {
            throw new PrestoException(COMPILER_ERROR, e.getCause());
        }
    }

    private <T> Class<? extends T> compileProcessor(
            SqlFunctionProperties sqlFunctionProperties,
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
        bodyCompiler.generateMethods(sqlFunctionProperties, classDefinition, callSiteBinder, filter, projections);

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
        private final SqlFunctionProperties sqlFunctionProperties;
        private final Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions;
        private final Optional<RowExpression> filter;
        private final List<RowExpression> projections;
        private final Object uniqueKey;
        private final boolean isOptimizeCommonSubExpression;

        private CacheKey(
                SqlFunctionProperties sqlFunctionProperties,
                Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions,
                Optional<RowExpression> filter,
                List<? extends RowExpression> projections,
                Object uniqueKey,
                boolean isOptimizeCommonSubExpression)
        {
            this.sqlFunctionProperties = sqlFunctionProperties;
            this.sessionFunctions = sessionFunctions;
            this.filter = filter;
            this.uniqueKey = uniqueKey;
            this.projections = ImmutableList.copyOf(projections);
            this.isOptimizeCommonSubExpression = isOptimizeCommonSubExpression;
        }

        public SqlFunctionProperties getSqlFunctionProperties()
        {
            return sqlFunctionProperties;
        }

        public Map<SqlFunctionId, SqlInvokedFunction> getSessionFunctions()
        {
            return sessionFunctions;
        }

        private Optional<RowExpression> getFilter()
        {
            return filter;
        }

        private List<RowExpression> getProjections()
        {
            return projections;
        }

        private boolean isOptimizeCommonSubExpression()
        {
            return isOptimizeCommonSubExpression;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(sqlFunctionProperties, filter, projections, uniqueKey, isOptimizeCommonSubExpression);
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
            return Objects.equals(this.sqlFunctionProperties, other.sqlFunctionProperties) &&
                    Objects.equals(this.filter, other.filter) &&
                    Objects.equals(this.projections, other.projections) &&
                    Objects.equals(this.uniqueKey, other.uniqueKey) &&
                    Objects.equals(this.isOptimizeCommonSubExpression, other.isOptimizeCommonSubExpression);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("sqlFunctionProperties", sqlFunctionProperties)
                    .add("filter", filter)
                    .add("projections", projections)
                    .add("uniqueKey", uniqueKey)
                    .add("isOptimizeCommonSubExpression", isOptimizeCommonSubExpression)
                    .toString();
        }
    }
}

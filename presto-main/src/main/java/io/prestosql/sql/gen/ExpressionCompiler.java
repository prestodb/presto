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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.CompilationException;
import io.prestosql.metadata.Metadata;
import io.prestosql.operator.project.CursorProcessor;
import io.prestosql.operator.project.PageFilter;
import io.prestosql.operator.project.PageProcessor;
import io.prestosql.operator.project.PageProjection;
import io.prestosql.spi.PrestoException;
import io.prestosql.sql.relational.RowExpression;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.inject.Inject;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.prestosql.spi.StandardErrorCode.COMPILER_ERROR;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.sql.gen.BytecodeUtils.invoke;
import static io.prestosql.sql.relational.Expressions.constant;
import static io.prestosql.util.CompilerUtils.defineClass;
import static io.prestosql.util.CompilerUtils.makeClassName;
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
        List<Supplier<PageProjection>> pageProjectionSuppliers = projections.stream()
                .map(projection -> pageFunctionCompiler.compileProjection(projection, classNameSuffix))
                .collect(toImmutableList());

        return () -> {
            Optional<PageFilter> filterFunction = filterFunctionSupplier.map(Supplier::get);
            List<PageProjection> pageProjections = pageProjectionSuppliers.stream()
                    .map(Supplier::get)
                    .collect(toImmutableList());
            return new PageProcessor(filterFunction, pageProjections, initialBatchSize);
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
}

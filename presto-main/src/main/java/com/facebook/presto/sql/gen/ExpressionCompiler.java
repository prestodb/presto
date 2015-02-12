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

import com.facebook.presto.byteCode.ClassDefinition;
import com.facebook.presto.byteCode.CompilerContext;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.CursorProcessor;
import com.facebook.presto.operator.PageProcessor;
import com.facebook.presto.sql.relational.RowExpression;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import org.weakref.jmx.Managed;

import javax.inject.Inject;

import java.util.List;
import java.util.Objects;

import static com.facebook.presto.byteCode.Access.FINAL;
import static com.facebook.presto.byteCode.Access.PUBLIC;
import static com.facebook.presto.byteCode.Access.a;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.sql.gen.Bootstrap.BOOTSTRAP_METHOD;
import static com.facebook.presto.sql.gen.ByteCodeUtils.invoke;
import static com.facebook.presto.sql.gen.CompilerUtils.defineClass;
import static com.facebook.presto.sql.gen.CompilerUtils.makeClassName;
import static com.google.common.base.MoreObjects.toStringHelper;

public class ExpressionCompiler
{
    private final Metadata metadata;

    private final LoadingCache<CacheKey, PageProcessor> pageProcessors = CacheBuilder.newBuilder().maximumSize(1000).build(
            new CacheLoader<CacheKey, PageProcessor>()
            {
                @Override
                public PageProcessor load(CacheKey key)
                        throws Exception
                {
                    return compileAndInstantiate(key.getFilter(), key.getProjections(), new PageProcessorCompiler(metadata), PageProcessor.class);
                }
            });

    private final LoadingCache<CacheKey, CursorProcessor> cursorProcessors = CacheBuilder.newBuilder().maximumSize(1000).build(
            new CacheLoader<CacheKey, CursorProcessor>()
            {
                @Override
                public CursorProcessor load(CacheKey key)
                        throws Exception
                {
                    return compileAndInstantiate(key.getFilter(), key.getProjections(), new CursorProcessorCompiler(metadata), CursorProcessor.class);
                }
            });

    @Inject
    public ExpressionCompiler(Metadata metadata)
    {
        this.metadata = metadata;
    }

    @Managed
    public long getCacheSize()
    {
        return pageProcessors.size();
    }

    public CursorProcessor compileCursorProcessor(RowExpression filter, List<RowExpression> projections, Object uniqueKey)
    {
        return cursorProcessors.getUnchecked(new CacheKey(filter, projections, uniqueKey));
    }

    public PageProcessor compilePageProcessor(RowExpression filter, List<RowExpression> projections)
    {
        return pageProcessors.getUnchecked(new CacheKey(filter, projections, null));
    }

    private <T> T compileAndInstantiate(RowExpression filter, List<RowExpression> projections, BodyCompiler<T> bodyCompiler, Class<? extends T> superType)
    {
        // create filter and project page iterator class
        Class<? extends T> clazz = compileProcessor(filter, projections, bodyCompiler, superType);
        try {
            return clazz.newInstance();
        }
        catch (ReflectiveOperationException e) {
            throw Throwables.propagate(e);
        }
    }

    private <T> Class<? extends T> compileProcessor(
            RowExpression filter,
            List<RowExpression> projections,
            BodyCompiler<T> bodyCompiler,
            Class<? extends T> superType)
    {
        ClassDefinition classDefinition = new ClassDefinition(new CompilerContext(BOOTSTRAP_METHOD),
                a(PUBLIC, FINAL),
                makeClassName(superType.getSimpleName()),
                type(Object.class),
                type(superType));

        classDefinition.declareDefaultConstructor(a(PUBLIC));

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
        CompilerContext context = new CompilerContext(BOOTSTRAP_METHOD);
        classDefinition.declareMethod(context, a(PUBLIC), "toString", type(String.class))
                .getBody()
                .append(invoke(context, callSiteBinder.bind(string, String.class), "toString"))
                .retObject();
    }

    private static final class CacheKey
    {
        private final RowExpression filter;
        private final List<RowExpression> projections;
        private final Object uniqueKey;

        private CacheKey(RowExpression filter, List<RowExpression> projections, Object uniqueKey)
        {
            this.filter = filter;
            this.uniqueKey = uniqueKey;
            this.projections = ImmutableList.copyOf(projections);
        }

        private RowExpression getFilter()
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

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

import com.facebook.presto.byteCode.Block;
import com.facebook.presto.byteCode.ClassDefinition;
import com.facebook.presto.byteCode.CompilerContext;
import com.facebook.presto.byteCode.DynamicClassLoader;
import com.facebook.presto.byteCode.LocalVariableDefinition;
import com.facebook.presto.byteCode.MethodDefinition;
import com.facebook.presto.byteCode.instruction.LabelNode;
import com.facebook.presto.operator.PagesIndex;
import com.facebook.presto.operator.PagesIndexComparator;
import com.facebook.presto.operator.PagesIndexOrdering;
import com.facebook.presto.operator.SimplePagesIndexComparator;
import com.facebook.presto.operator.SyntheticAddress;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ExecutionError;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.log.Logger;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.byteCode.Access.FINAL;
import static com.facebook.presto.byteCode.Access.PUBLIC;
import static com.facebook.presto.byteCode.Access.a;
import static com.facebook.presto.byteCode.NamedParameterDefinition.arg;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.byteCode.ParameterizedType.typeFromPathName;
import static com.facebook.presto.sql.gen.CompilerUtils.defineClass;
import static com.google.common.base.Preconditions.checkNotNull;

public class OrderingCompiler
{
    private static final Logger log = Logger.get(ExpressionCompiler.class);

    private static final AtomicLong CLASS_ID = new AtomicLong();

    private final Method bootstrapMethod = null;

    private final LoadingCache<PagesIndexComparatorCacheKey, PagesIndexOrdering> pagesIndexOrderings = CacheBuilder.newBuilder().maximumSize(1000).build(
            new CacheLoader<PagesIndexComparatorCacheKey, PagesIndexOrdering>()
            {
                @Override
                public PagesIndexOrdering load(PagesIndexComparatorCacheKey key)
                        throws Exception
                {
                    return internalCompilePagesIndexOrdering(key.getSortTypes(), key.getSortChannels(), key.getSortOrders());
                }
            });

    public PagesIndexOrdering compilePagesIndexOrdering(List<Type> sortTypes, List<Integer> sortChannels, List<SortOrder> sortOrders)
    {
        checkNotNull(sortTypes, "sortTypes is null");
        checkNotNull(sortChannels, "sortChannels is null");
        checkNotNull(sortOrders, "sortOrders is null");

        try {
            return pagesIndexOrderings.get(new PagesIndexComparatorCacheKey(sortTypes, sortChannels, sortOrders));
        }
        catch (ExecutionException | UncheckedExecutionException | ExecutionError e) {
            throw Throwables.propagate(e.getCause());
        }
    }

    @VisibleForTesting
    public PagesIndexOrdering internalCompilePagesIndexOrdering(List<Type> sortTypes, List<Integer> sortChannels, List<SortOrder> sortOrders)
            throws Exception
    {
        checkNotNull(sortChannels, "sortChannels is null");
        checkNotNull(sortOrders, "sortOrders is null");

        PagesIndexComparator comparator;
        try {
            DynamicClassLoader classLoader = new DynamicClassLoader(getClass().getClassLoader());

            Class<? extends PagesIndexComparator> pagesHashStrategyClass = compilePagesIndexComparator(sortTypes, sortChannels, sortOrders, classLoader);
            comparator = pagesHashStrategyClass.newInstance();
        }
        catch (Throwable e) {
            log.error(e, "Error compiling comparator for channels %s with order %s", sortChannels, sortChannels);
            comparator = new SimplePagesIndexComparator(sortTypes, sortChannels, sortOrders);
        }

        // we may want to load a separate PagesIndexOrdering for each comparator
        return new PagesIndexOrdering(comparator);
    }

    private Class<? extends PagesIndexComparator> compilePagesIndexComparator(
            List<Type> sortTypes,
            List<Integer> sortChannels,
            List<SortOrder> sortOrders,
            DynamicClassLoader classLoader)
    {
        ClassDefinition classDefinition = new ClassDefinition(new CompilerContext(bootstrapMethod),
                a(PUBLIC, FINAL),
                typeFromPathName("PagesIndexComparator" + CLASS_ID.incrementAndGet()),
                type(Object.class),
                type(PagesIndexComparator.class));

        classDefinition.addDefaultConstructor();
        generateCompareTo(classDefinition, sortTypes, sortChannels, sortOrders);

        Class<? extends PagesIndexComparator> joinHashClass = defineClass(classDefinition, PagesIndexComparator.class, classLoader);
        return joinHashClass;
    }

    private void generateCompareTo(ClassDefinition classDefinition, List<Type> sortTypes, List<Integer> sortChannels, List<SortOrder> sortOrders)
    {
        CompilerContext compilerContext = new CompilerContext(bootstrapMethod);
        MethodDefinition compareToMethod = classDefinition.declareMethod(compilerContext,
                a(PUBLIC),
                "compareTo",
                type(int.class),
                arg("pagesIndex", PagesIndex.class),
                arg("leftPosition", int.class),
                arg("rightPosition", int.class));

        LocalVariableDefinition valueAddresses = compilerContext.declareVariable(LongArrayList.class, "valueAddresses");
        compareToMethod
                .getBody()
                .comment("LongArrayList valueAddresses = pagesIndex.valueAddresses")
                .getVariable("pagesIndex")
                .invokeVirtual(PagesIndex.class, "getValueAddresses", LongArrayList.class)
                .putVariable(valueAddresses);

        LocalVariableDefinition leftPageAddress = compilerContext.declareVariable(long.class, "leftPageAddress");
        compareToMethod
                .getBody()
                .comment("long leftPageAddress = valueAddresses.getLong(leftPosition)")
                .getVariable(valueAddresses)
                .getVariable("leftPosition")
                .invokeVirtual(LongArrayList.class, "getLong", long.class, int.class)
                .putVariable(leftPageAddress);

        LocalVariableDefinition leftBlockIndex = compilerContext.declareVariable(int.class, "leftBlockIndex");
        compareToMethod
                .getBody()
                .comment("int leftBlockIndex = decodeSliceIndex(leftPageAddress)")
                .getVariable(leftPageAddress)
                .invokeStatic(SyntheticAddress.class, "decodeSliceIndex", int.class, long.class)
                .putVariable(leftBlockIndex);

        LocalVariableDefinition leftBlockPosition = compilerContext.declareVariable(int.class, "leftBlockPosition");
        compareToMethod
                .getBody()
                .comment("int leftBlockPosition = decodePosition(leftPageAddress)")
                .getVariable(leftPageAddress)
                .invokeStatic(SyntheticAddress.class, "decodePosition", int.class, long.class)
                .putVariable(leftBlockPosition);

        LocalVariableDefinition rightPageAddress = compilerContext.declareVariable(long.class, "rightPageAddress");
        compareToMethod
                .getBody()
                .comment("long rightPageAddress = valueAddresses.getLong(rightPosition);")
                .getVariable(valueAddresses)
                .getVariable("rightPosition")
                .invokeVirtual(LongArrayList.class, "getLong", long.class, int.class)
                .putVariable(rightPageAddress);

        LocalVariableDefinition rightBlockIndex = compilerContext.declareVariable(int.class, "rightBlockIndex");
        compareToMethod
                .getBody()
                .comment("int rightBlockIndex = decodeSliceIndex(rightPageAddress)")
                .getVariable(rightPageAddress)
                .invokeStatic(SyntheticAddress.class, "decodeSliceIndex", int.class, long.class)
                .putVariable(rightBlockIndex);

        LocalVariableDefinition rightBlockPosition = compilerContext.declareVariable(int.class, "rightBlockPosition");
        compareToMethod
                .getBody()
                .comment("int rightBlockPosition = decodePosition(rightPageAddress)")
                .getVariable(rightPageAddress)
                .invokeStatic(SyntheticAddress.class, "decodePosition", int.class, long.class)
                .putVariable(rightBlockPosition);

        for (int i = 0; i < sortChannels.size(); i++) {
            int sortChannel = sortChannels.get(i);
            SortOrder sortOrder = sortOrders.get(i);

            Block block = new Block(compilerContext)
                    .setDescription("compare channel " + sortChannel + " " + sortOrder);

            block.comment("push sortOrder")
                    .getStaticField(SortOrder.class, sortOrder.name(), SortOrder.class);

            Type sortType = sortTypes.get(i);
            block.comment("push sortType")
                    .invokeStatic(sortType.getClass(), "getInstance", sortType.getClass());

            block.comment("push leftBlock -- pagesIndex.getChannel(sortChannel).get(leftBlockIndex)")
                    .getVariable("pagesIndex")
                    .push(sortChannel)
                    .invokeVirtual(PagesIndex.class, "getChannel", ObjectArrayList.class, int.class)
                    .getVariable(leftBlockIndex)
                    .invokeVirtual(ObjectArrayList.class, "get", Object.class, int.class)
                    .checkCast(com.facebook.presto.spi.block.Block.class);

            block.comment("push leftBlockPosition")
                    .getVariable(leftBlockPosition);

            block.comment("push rightBlock -- pagesIndex.getChannel(sortChannel).get(rightBlockIndex)")
                    .getVariable("pagesIndex")
                    .push(sortChannel)
                    .invokeVirtual(PagesIndex.class, "getChannel", ObjectArrayList.class, int.class)
                    .getVariable(rightBlockIndex)
                    .invokeVirtual(ObjectArrayList.class, "get", Object.class, int.class)
                    .checkCast(com.facebook.presto.spi.block.Block.class);

            block.comment("push rightBlockPosition")
                    .getVariable(rightBlockPosition);

            block.comment("invoke compareTo")
                    .invokeVirtual(SortOrder.class,
                            "compareBlockValue",
                            int.class,
                            Type.class,
                            com.facebook.presto.spi.block.Block.class,
                            int.class,
                            com.facebook.presto.spi.block.Block.class,
                            int.class);

            LabelNode equal = new LabelNode("equal");
            block.comment("if (compare != 0) return compare")
                    .dup()
                    .ifZeroGoto(equal)
                    .retInt()
                    .visitLabel(equal)
                    .pop(int.class);

            compareToMethod.getBody().append(block);
        }

        // values are equal
        compareToMethod.getBody()
                .push(0)
                .retInt();
    }

    private static final class PagesIndexComparatorCacheKey
    {
        private List<Type> sortTypes;
        private List<Integer> sortChannels;
        private List<SortOrder> sortOrders;

        private PagesIndexComparatorCacheKey(List<Type> sortTypes, List<Integer> sortChannels, List<SortOrder> sortOrders)
        {
            this.sortTypes = ImmutableList.copyOf(sortTypes);
            this.sortChannels = ImmutableList.copyOf(sortChannels);
            this.sortOrders = ImmutableList.copyOf(sortOrders);
        }

        public List<Type> getSortTypes()
        {
            return sortTypes;
        }

        public List<Integer> getSortChannels()
        {
            return sortChannels;
        }

        public List<SortOrder> getSortOrders()
        {
            return sortOrders;
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(sortTypes, sortChannels, sortOrders);
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
            PagesIndexComparatorCacheKey other = (PagesIndexComparatorCacheKey) obj;
            return Objects.equal(this.sortTypes, other.sortTypes) &&
                    Objects.equal(this.sortChannels, other.sortChannels) &&
                    Objects.equal(this.sortOrders, other.sortOrders);
        }
    }
}

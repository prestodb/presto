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
import com.facebook.presto.bytecode.ClassDefinition;
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.Parameter;
import com.facebook.presto.bytecode.Scope;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.expression.BytecodeExpression;
import com.facebook.presto.bytecode.instruction.LabelNode;
import com.facebook.presto.operator.PagesIndex;
import com.facebook.presto.operator.PagesIndexComparator;
import com.facebook.presto.operator.PagesIndexOrdering;
import com.facebook.presto.operator.SimplePagesIndexComparator;
import com.facebook.presto.operator.SyntheticAddress;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.google.common.annotations.VisibleForTesting;
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

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

import static com.facebook.presto.bytecode.Access.FINAL;
import static com.facebook.presto.bytecode.Access.PUBLIC;
import static com.facebook.presto.bytecode.Access.a;
import static com.facebook.presto.bytecode.CompilerUtils.defineClass;
import static com.facebook.presto.bytecode.CompilerUtils.makeClassName;
import static com.facebook.presto.bytecode.Parameter.arg;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantInt;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.getStatic;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.invokeStatic;
import static com.facebook.presto.sql.gen.SqlTypeBytecodeExpression.constantType;
import static java.util.Objects.requireNonNull;

public class OrderingCompiler
{
    private static final Logger log = Logger.get(OrderingCompiler.class);

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
        requireNonNull(sortTypes, "sortTypes is null");
        requireNonNull(sortChannels, "sortChannels is null");
        requireNonNull(sortOrders, "sortOrders is null");

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
        requireNonNull(sortChannels, "sortChannels is null");
        requireNonNull(sortOrders, "sortOrders is null");

        PagesIndexComparator comparator;
        try {
            Class<? extends PagesIndexComparator> pagesHashStrategyClass = compilePagesIndexComparator(sortTypes, sortChannels, sortOrders);
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
            List<SortOrder> sortOrders)
    {
        CallSiteBinder callSiteBinder = new CallSiteBinder();

        ClassDefinition classDefinition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName("PagesIndexComparator"),
                type(Object.class),
                type(PagesIndexComparator.class));

        classDefinition.declareDefaultConstructor(a(PUBLIC));
        generateCompareTo(classDefinition, callSiteBinder, sortTypes, sortChannels, sortOrders);

        return defineClass(classDefinition, PagesIndexComparator.class, callSiteBinder.getBindings(), getClass().getClassLoader());
    }

    private void generateCompareTo(ClassDefinition classDefinition, CallSiteBinder callSiteBinder, List<Type> sortTypes, List<Integer> sortChannels, List<SortOrder> sortOrders)
    {
        Parameter pagesIndex = arg("pagesIndex", PagesIndex.class);
        Parameter leftPosition = arg("leftPosition", int.class);
        Parameter rightPosition = arg("rightPosition", int.class);
        MethodDefinition compareToMethod = classDefinition.declareMethod(a(PUBLIC), "compareTo", type(int.class), pagesIndex, leftPosition, rightPosition);
        Scope scope = compareToMethod.getScope();

        Variable valueAddresses = scope.declareVariable(LongArrayList.class, "valueAddresses");
        compareToMethod
                .getBody()
                .comment("LongArrayList valueAddresses = pagesIndex.valueAddresses")
                .append(valueAddresses.set(pagesIndex.invoke("getValueAddresses", LongArrayList.class)));

        Variable leftPageAddress = scope.declareVariable(long.class, "leftPageAddress");
        compareToMethod
                .getBody()
                .comment("long leftPageAddress = valueAddresses.getLong(leftPosition)")
                .append(leftPageAddress.set(valueAddresses.invoke("getLong", long.class, leftPosition)));

        Variable leftBlockIndex = scope.declareVariable(int.class, "leftBlockIndex");
        compareToMethod
                .getBody()
                .comment("int leftBlockIndex = decodeSliceIndex(leftPageAddress)")
                .append(leftBlockIndex.set(invokeStatic(SyntheticAddress.class, "decodeSliceIndex", int.class, leftPageAddress)));

        Variable leftBlockPosition = scope.declareVariable(int.class, "leftBlockPosition");
        compareToMethod
                .getBody()
                .comment("int leftBlockPosition = decodePosition(leftPageAddress)")
                .append(leftBlockPosition.set(invokeStatic(SyntheticAddress.class, "decodePosition", int.class, leftPageAddress)));

        Variable rightPageAddress = scope.declareVariable(long.class, "rightPageAddress");
        compareToMethod
                .getBody()
                .comment("long rightPageAddress = valueAddresses.getLong(rightPosition);")
                .append(rightPageAddress.set(valueAddresses.invoke("getLong", long.class, rightPosition)));

        Variable rightBlockIndex = scope.declareVariable(int.class, "rightBlockIndex");
        compareToMethod
                .getBody()
                .comment("int rightBlockIndex = decodeSliceIndex(rightPageAddress)")
                .append(rightBlockIndex.set(invokeStatic(SyntheticAddress.class, "decodeSliceIndex", int.class, rightPageAddress)));

        Variable rightBlockPosition = scope.declareVariable(int.class, "rightBlockPosition");
        compareToMethod
                .getBody()
                .comment("int rightBlockPosition = decodePosition(rightPageAddress)")
                .append(rightBlockPosition.set(invokeStatic(SyntheticAddress.class, "decodePosition", int.class, rightPageAddress)));

        for (int i = 0; i < sortChannels.size(); i++) {
            int sortChannel = sortChannels.get(i);
            SortOrder sortOrder = sortOrders.get(i);

            BytecodeBlock block = new BytecodeBlock()
                    .setDescription("compare channel " + sortChannel + " " + sortOrder);

            Type sortType = sortTypes.get(i);

            BytecodeExpression leftBlock = pagesIndex
                    .invoke("getChannel", ObjectArrayList.class, constantInt(sortChannel))
                    .invoke("get", Object.class, leftBlockIndex)
                    .cast(Block.class);

            BytecodeExpression rightBlock = pagesIndex
                    .invoke("getChannel", ObjectArrayList.class, constantInt(sortChannel))
                    .invoke("get", Object.class, rightBlockIndex)
                    .cast(Block.class);

            block.append(getStatic(SortOrder.class, sortOrder.name())
                    .invoke("compareBlockValue",
                            int.class,
                            ImmutableList.of(Type.class, Block.class, int.class, Block.class, int.class),
                            constantType(callSiteBinder, sortType),
                            leftBlock,
                            leftBlockPosition,
                            rightBlock,
                            rightBlockPosition));

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
        private final List<Type> sortTypes;
        private final List<Integer> sortChannels;
        private final List<SortOrder> sortOrders;

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
            return Objects.hash(sortTypes, sortChannels, sortOrders);
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
            return Objects.equals(this.sortTypes, other.sortTypes) &&
                    Objects.equals(this.sortChannels, other.sortChannels) &&
                    Objects.equals(this.sortOrders, other.sortOrders);
        }
    }
}

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
import com.facebook.presto.byteCode.FieldDefinition;
import com.facebook.presto.byteCode.LocalVariableDefinition;
import com.facebook.presto.byteCode.MethodDefinition;
import com.facebook.presto.byteCode.OpCodes;
import com.facebook.presto.byteCode.instruction.LabelNode;
import com.facebook.presto.operator.InMemoryJoinHash;
import com.facebook.presto.operator.LookupSource;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.PageBuilder;
import com.facebook.presto.operator.PagesHashStrategy;
import com.facebook.presto.spi.block.BlockBuilder;
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
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.byteCode.Access.FINAL;
import static com.facebook.presto.byteCode.Access.PRIVATE;
import static com.facebook.presto.byteCode.Access.PUBLIC;
import static com.facebook.presto.byteCode.Access.a;
import static com.facebook.presto.byteCode.NamedParameterDefinition.arg;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.byteCode.ParameterizedType.typeFromPathName;
import static com.facebook.presto.sql.gen.CompilerUtils.defineClass;
import static com.google.common.base.Preconditions.checkNotNull;

public class JoinCompiler
{
    private static final AtomicLong CLASS_ID = new AtomicLong();

    private final LoadingCache<LookupSourceCacheKey, LookupSourceFactory> lookupSourceFactories = CacheBuilder.newBuilder().maximumSize(1000).build(
            new CacheLoader<LookupSourceCacheKey, LookupSourceFactory>()
            {
                @Override
                public LookupSourceFactory load(LookupSourceCacheKey key)
                        throws Exception
                {
                    return internalCompileLookupSourceFactory(key.getTypes(), key.getJoinChannels());
                }
            });

    public LookupSourceFactory compileLookupSourceFactory(List<? extends Type> types, List<Integer> joinChannels)
    {
        try {
            return lookupSourceFactories.get(new LookupSourceCacheKey(types, joinChannels));
        }
        catch (ExecutionException | UncheckedExecutionException | ExecutionError e) {
            throw Throwables.propagate(e.getCause());
        }
    }

    @VisibleForTesting
    public LookupSourceFactory internalCompileLookupSourceFactory(List<Type> types, List<Integer> joinChannels)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(getClass().getClassLoader());

        Class<? extends PagesHashStrategy> pagesHashStrategyClass = compilePagesHashStrategy(types, joinChannels, classLoader);

        Class<? extends LookupSource> lookupSourceClass = IsolatedClass.isolateClass(
                classLoader,
                LookupSource.class,
                InMemoryJoinHash.class);

        return new LookupSourceFactory(lookupSourceClass, new PagesHashStrategyFactory(pagesHashStrategyClass));
    }

    @VisibleForTesting
    public PagesHashStrategyFactory compilePagesHashStrategy(List<Type> types, List<Integer> joinChannels)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(getClass().getClassLoader());

        Class<? extends PagesHashStrategy> pagesHashStrategyClass = compilePagesHashStrategy(types, joinChannels, classLoader);

        return new PagesHashStrategyFactory(pagesHashStrategyClass);
    }

    private Class<? extends PagesHashStrategy> compilePagesHashStrategy(List<Type> types, List<Integer> joinChannels, DynamicClassLoader classLoader)
    {
        ClassDefinition classDefinition = new ClassDefinition(new CompilerContext(),
                a(PUBLIC, FINAL),
                typeFromPathName("PagesHashStrategy_" + CLASS_ID.incrementAndGet()),
                type(Object.class),
                type(PagesHashStrategy.class));

        // declare fields
        List<FieldDefinition> channelFields = new ArrayList<>();
        for (int i = 0; i < types.size(); i++) {
            FieldDefinition channelField = classDefinition.declareField(a(PRIVATE, FINAL), "channel_" + i, type(List.class, com.facebook.presto.spi.block.Block.class));
            channelFields.add(channelField);
        }
        List<Type> joinChannelTypes = new ArrayList<>();
        List<FieldDefinition> joinChannelFields = new ArrayList<>();
        for (int i = 0; i < joinChannels.size(); i++) {
            joinChannelTypes.add(types.get(joinChannels.get(i)));
            FieldDefinition channelField = classDefinition.declareField(a(PRIVATE, FINAL), "joinChannel_" + i, type(List.class, com.facebook.presto.spi.block.Block.class));
            joinChannelFields.add(channelField);
        }

        generateConstructor(classDefinition, joinChannels, channelFields, joinChannelFields);
        generateGetChannelCountMethod(classDefinition, channelFields);
        generateAppendToMethod(classDefinition, types, channelFields);
        generateHashPositionMethod(classDefinition, joinChannelTypes, joinChannelFields);
        generatePositionEqualsRowMethod(classDefinition, joinChannelTypes, joinChannelFields);
        generatePositionEqualsPositionMethod(classDefinition, joinChannelTypes, joinChannelFields);

        Class<? extends PagesHashStrategy> pagesHashStrategyClass = defineClass(classDefinition, PagesHashStrategy.class, classLoader);
        return pagesHashStrategyClass;
    }

    private void generateConstructor(ClassDefinition classDefinition,
            List<Integer> joinChannels,
            List<FieldDefinition> channelFields,
            List<FieldDefinition> joinChannelFields)
    {
        Block constructor = classDefinition.declareConstructor(new CompilerContext(),
                a(PUBLIC),
                arg("channels", type(List.class, type(List.class, com.facebook.presto.spi.block.Block.class))))
                .getBody()
                .comment("super();")
                .pushThis()
                .invokeConstructor(Object.class);

        constructor.comment("Set channel fields");
        for (int index = 0; index < channelFields.size(); index++) {
            constructor
                    .pushThis()
                    .getVariable("channels")
                    .push(index)
                    .invokeInterface(List.class, "get", Object.class, int.class)
                    .checkCast(type(List.class, com.facebook.presto.spi.block.Block.class))
                    .putField(channelFields.get(index));
        }

        constructor.comment("Set join channel fields");
        for (int index = 0; index < joinChannelFields.size(); index++) {
            constructor
                    .pushThis()
                    .getVariable("channels")
                    .push(joinChannels.get(index))
                    .invokeInterface(List.class, "get", Object.class, int.class)
                    .checkCast(type(List.class, com.facebook.presto.spi.block.Block.class))
                    .putField(joinChannelFields.get(index));
        }

        constructor.ret();
    }

    private void generateGetChannelCountMethod(ClassDefinition classDefinition, List<FieldDefinition> channelFields)
    {
        classDefinition.declareMethod(new CompilerContext(),
                a(PUBLIC),
                "getChannelCount",
                type(int.class))
                .getBody()
                .push(channelFields.size())
                .retInt();
    }

    private void generateAppendToMethod(ClassDefinition classDefinition, List<Type> types, List<FieldDefinition> channelFields)
    {
        Block appendToBody = classDefinition.declareMethod(new CompilerContext(),
                a(PUBLIC),
                "appendTo",
                type(void.class),
                arg("blockIndex", int.class),
                arg("blockPosition", int.class),
                arg("pageBuilder", PageBuilder.class),
                arg("outputChannelOffset", int.class))
                .getBody();

        for (int index = 0; index < channelFields.size(); index++) {
            Type type = types.get(index);
            appendToBody
                    .comment("%s.appendTo(channel_%s.get(blockIndex), blockPosition, pageBuilder.getBlockBuilder(outputChannelOffset + %s));", type.getClass(), index, index)
                    .invokeStatic(type.getClass(), "getInstance", type.getClass())
                    .pushThis()
                    .getField(channelFields.get(index))
                    .getVariable("blockIndex")
                    .invokeInterface(List.class, "get", Object.class, int.class)
                    .checkCast(com.facebook.presto.spi.block.Block.class)
                    .getVariable("blockPosition")
                    .getVariable("pageBuilder")
                    .getVariable("outputChannelOffset")
                    .push(index)
                    .append(OpCodes.IADD)
                    .invokeVirtual(PageBuilder.class, "getBlockBuilder", BlockBuilder.class, int.class)
                    .invokeVirtual(type.getClass(), "appendTo", void.class, com.facebook.presto.spi.block.Block.class, int.class, BlockBuilder.class);
        }
        appendToBody.ret();
    }

    private void generateHashPositionMethod(ClassDefinition classDefinition, List<Type> joinChannelTypes, List<FieldDefinition> joinChannelFields)
    {
        MethodDefinition hashPositionMethod = classDefinition.declareMethod(new CompilerContext(),
                a(PUBLIC),
                "hashPosition",
                type(int.class),
                arg("blockIndex", int.class),
                arg("blockPosition", int.class));

        LocalVariableDefinition resultVariable = hashPositionMethod.getCompilerContext().declareVariable(int.class, "result");
        hashPositionMethod.getBody().push(0).putVariable(resultVariable);

        for (int index = 0; index < joinChannelTypes.size(); index++) {
            Type joinChannelType = joinChannelTypes.get(index);
            FieldDefinition joinChannelField = joinChannelFields.get(index);
            hashPositionMethod
                    .getBody()
                    .getVariable(resultVariable)
                    .push(31)
                    .append(OpCodes.IMUL)
                    .invokeStatic(joinChannelType.getClass(), "getInstance", joinChannelType.getClass())
                    .pushThis()
                    .getField(joinChannelField)
                    .getVariable("blockIndex")
                    .invokeInterface(List.class, "get", Object.class, int.class)
                    .checkCast(com.facebook.presto.spi.block.Block.class)
                    .getVariable("blockPosition")
                    .invokeVirtual(
                            joinChannelType.getClass(),
                            "hash",
                            int.class,
                            com.facebook.presto.spi.block.Block.class,
                            int.class)
                    .append(OpCodes.IADD)
                    .putVariable(resultVariable);
        }

        hashPositionMethod
                .getBody()
                .getVariable(resultVariable)
                .retInt();
    }

    private void generatePositionEqualsRowMethod(ClassDefinition classDefinition, List<Type> joinChannelTypes, List<FieldDefinition> joinChannelFields)
    {
        MethodDefinition hashPositionMethod = classDefinition.declareMethod(new CompilerContext(),
                a(PUBLIC),
                "positionEqualsRow",
                type(boolean.class),
                arg("leftBlockIndex", int.class),
                arg("leftBlockPosition", int.class),
                arg("rightPosition", int.class),
                arg("rightBlocks", com.facebook.presto.spi.block.Block[].class));

        for (int index = 0; index < joinChannelTypes.size(); index++) {
            Type joinChannelType = joinChannelTypes.get(index);
            LabelNode checkNextField = new LabelNode("checkNextField");
            hashPositionMethod
                    .getBody()
                    .invokeStatic(joinChannelType.getClass(), "getInstance", joinChannelType.getClass())
                    .pushThis()
                    .getField(joinChannelFields.get(index))
                    .getVariable("leftBlockIndex")
                    .invokeInterface(List.class, "get", Object.class, int.class)
                    .checkCast(com.facebook.presto.spi.block.Block.class)
                    .getVariable("leftBlockPosition")
                    .getVariable("rightBlocks")
                    .push(index)
                    .getObjectArrayElement()
                    .getVariable("rightPosition")
                    .invokeVirtual(joinChannelType.getClass(),
                            "equalTo",
                            boolean.class,
                            com.facebook.presto.spi.block.Block.class,
                            int.class,
                            com.facebook.presto.spi.block.Block.class,
                            int.class)
                    .ifTrueGoto(checkNextField)
                    .push(false)
                    .retBoolean()
                    .visitLabel(checkNextField);
        }

        hashPositionMethod
                .getBody()
                .push(true)
                .retInt();
    }

    private void generatePositionEqualsPositionMethod(ClassDefinition classDefinition, List<Type> joinChannelTypes, List<FieldDefinition> joinChannelFields)
    {
        MethodDefinition hashPositionMethod = classDefinition.declareMethod(new CompilerContext(),
                a(PUBLIC),
                "positionEqualsPosition",
                type(boolean.class),
                arg("leftBlockIndex", int.class),
                arg("leftBlockPosition", int.class),
                arg("rightBlockIndex", int.class),
                arg("rightBlockPosition", int.class));

        for (int i = 0; i < joinChannelTypes.size(); i++) {
            Type joinChannelType = joinChannelTypes.get(i);
            FieldDefinition joinChannelField = joinChannelFields.get(i);
            LabelNode checkNextField = new LabelNode("checkNextField");
            hashPositionMethod
                    .getBody()
                    .invokeStatic(joinChannelType.getClass(), "getInstance", joinChannelType.getClass())
                    .pushThis()
                    .getField(joinChannelField)
                    .getVariable("leftBlockIndex")
                    .invokeInterface(List.class, "get", Object.class, int.class)
                    .checkCast(com.facebook.presto.spi.block.Block.class)
                    .getVariable("leftBlockPosition")
                    .pushThis()
                    .getField(joinChannelField)
                    .getVariable("rightBlockIndex")
                    .invokeInterface(List.class, "get", Object.class, int.class)
                    .checkCast(com.facebook.presto.spi.block.Block.class)
                    .getVariable("rightBlockPosition")
                    .invokeVirtual(
                            joinChannelType.getClass(),
                            "equalTo",
                            boolean.class,
                            com.facebook.presto.spi.block.Block.class,
                            int.class,
                            com.facebook.presto.spi.block.Block.class,
                            int.class)
                    .ifTrueGoto(checkNextField)
                    .push(false)
                    .retBoolean()
                    .visitLabel(checkNextField);
        }

        hashPositionMethod
                .getBody()
                .push(true)
                .retInt();
    }

    public static class LookupSourceFactory
    {
        private final Constructor<? extends LookupSource> constructor;
        private final PagesHashStrategyFactory pagesHashStrategyFactory;

        public LookupSourceFactory(Class<? extends LookupSource> lookupSourceClass, PagesHashStrategyFactory pagesHashStrategyFactory)
        {
            this.pagesHashStrategyFactory = pagesHashStrategyFactory;
            try {
                constructor = lookupSourceClass.getConstructor(LongArrayList.class, List.class, PagesHashStrategy.class, OperatorContext.class);
            }
            catch (NoSuchMethodException e) {
                throw Throwables.propagate(e);
            }
        }

        public LookupSource createLookupSource(LongArrayList addresses, List<Type> types, List<List<com.facebook.presto.spi.block.Block>> channels, OperatorContext operatorContext)
        {
            PagesHashStrategy pagesHashStrategy = pagesHashStrategyFactory.createPagesHashStrategy(channels);
            try {
                return constructor.newInstance(addresses, types, pagesHashStrategy, operatorContext);
            }
            catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
    }

    public static class PagesHashStrategyFactory
    {
        private final Constructor<? extends PagesHashStrategy> constructor;

        public PagesHashStrategyFactory(Class<? extends PagesHashStrategy> pagesHashStrategyClass)
        {
            try {
                constructor = pagesHashStrategyClass.getConstructor(List.class);
            }
            catch (NoSuchMethodException e) {
                throw Throwables.propagate(e);
            }
        }

        public PagesHashStrategy createPagesHashStrategy(List<List<com.facebook.presto.spi.block.Block>> channels)
        {
            try {
                return constructor.newInstance(channels);
            }
            catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
    }

    private static final class LookupSourceCacheKey
    {
        private final List<Type> types;
        private final List<Integer> joinChannels;

        private LookupSourceCacheKey(List<? extends Type> types, List<Integer> joinChannels)
        {
            this.types = ImmutableList.copyOf(checkNotNull(types, "types is null"));
            this.joinChannels = ImmutableList.copyOf(checkNotNull(joinChannels, "joinChannels is null"));
        }

        private List<Type> getTypes()
        {
            return types;
        }

        private List<Integer> getJoinChannels()
        {
            return joinChannels;
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(types, joinChannels);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof LookupSourceCacheKey)) {
                return false;
            }
            final LookupSourceCacheKey other = (LookupSourceCacheKey) obj;
            return Objects.equal(this.types, other.types) &&
                    Objects.equal(this.joinChannels, other.joinChannels);
        }
    }
}

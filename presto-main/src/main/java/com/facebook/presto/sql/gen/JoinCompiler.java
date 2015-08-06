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
import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.ClassDefinition;
import com.facebook.presto.byteCode.Scope;
import com.facebook.presto.byteCode.DynamicClassLoader;
import com.facebook.presto.byteCode.FieldDefinition;
import com.facebook.presto.byteCode.MethodDefinition;
import com.facebook.presto.byteCode.OpCode;
import com.facebook.presto.byteCode.Parameter;
import com.facebook.presto.byteCode.Variable;
import com.facebook.presto.byteCode.control.ForLoop;
import com.facebook.presto.byteCode.control.IfStatement;
import com.facebook.presto.byteCode.expression.ByteCodeExpression;
import com.facebook.presto.byteCode.instruction.LabelNode;
import com.facebook.presto.operator.InMemoryJoinHash;
import com.facebook.presto.operator.LookupSource;
import com.facebook.presto.operator.PagesHashStrategy;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.Type;
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
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static com.facebook.presto.byteCode.Access.FINAL;
import static com.facebook.presto.byteCode.Access.PRIVATE;
import static com.facebook.presto.byteCode.Access.PUBLIC;
import static com.facebook.presto.byteCode.Access.a;
import static com.facebook.presto.byteCode.Parameter.arg;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantInt;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantLong;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantNull;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.notEqual;
import static com.facebook.presto.sql.gen.CompilerUtils.defineClass;
import static com.facebook.presto.sql.gen.CompilerUtils.makeClassName;
import static com.facebook.presto.sql.gen.SqlTypeByteCodeExpression.constantType;
import static com.google.common.base.Preconditions.checkNotNull;

public class JoinCompiler
{
    private final LoadingCache<CacheKey, LookupSourceFactory> lookupSourceFactories = CacheBuilder.newBuilder().maximumSize(1000).build(
            new CacheLoader<CacheKey, LookupSourceFactory>()
            {
                @Override
                public LookupSourceFactory load(CacheKey key)
                        throws Exception
                {
                    return internalCompileLookupSourceFactory(key.getTypes(), key.getJoinChannels());
                }
            });

    private final LoadingCache<CacheKey, Class<? extends PagesHashStrategy>> hashStrategies = CacheBuilder.newBuilder().maximumSize(1000).build(
            new CacheLoader<CacheKey, Class<? extends PagesHashStrategy>>() {
                @Override
                public Class<? extends PagesHashStrategy> load(CacheKey key)
                        throws Exception
                {
                    return internalCompileHashStrategy(key.getTypes(), key.getJoinChannels());
                }
            });

    public LookupSourceFactory compileLookupSourceFactory(List<? extends Type> types, List<Integer> joinChannels)
    {
        try {
            return lookupSourceFactories.get(new CacheKey(types, joinChannels));
        }
        catch (ExecutionException | UncheckedExecutionException | ExecutionError e) {
            throw Throwables.propagate(e.getCause());
        }
    }

    public PagesHashStrategyFactory compilePagesHashStrategyFactory(List<Type> types, List<Integer> joinChannels)
    {
        checkNotNull(types, "types is null");
        checkNotNull(joinChannels, "joinChannels is null");

        try {
            return new PagesHashStrategyFactory(hashStrategies.get(new CacheKey(types, joinChannels)));
        }
        catch (ExecutionException | UncheckedExecutionException | ExecutionError e) {
            throw Throwables.propagate(e.getCause());
        }
    }

    private LookupSourceFactory internalCompileLookupSourceFactory(List<Type> types, List<Integer> joinChannels)
    {
        Class<? extends PagesHashStrategy> pagesHashStrategyClass = internalCompileHashStrategy(types, joinChannels);

        Class<? extends LookupSource> lookupSourceClass = IsolatedClass.isolateClass(
                new DynamicClassLoader(getClass().getClassLoader()),
                LookupSource.class,
                InMemoryJoinHash.class,
                InMemoryJoinHash.UnvisitedJoinPositionIterator.class);

        return new LookupSourceFactory(lookupSourceClass, new PagesHashStrategyFactory(pagesHashStrategyClass));
    }

    private Class<? extends PagesHashStrategy> internalCompileHashStrategy(List<Type> types, List<Integer> joinChannels)
    {
        CallSiteBinder callSiteBinder = new CallSiteBinder();

        ClassDefinition classDefinition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName("PagesHashStrategy"),
                type(Object.class),
                type(PagesHashStrategy.class));

        FieldDefinition sizeField = classDefinition.declareField(a(PRIVATE, FINAL), "size", type(long.class));
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
        FieldDefinition hashChannelField = classDefinition.declareField(a(PRIVATE, FINAL), "hashChannel", type(List.class, com.facebook.presto.spi.block.Block.class));

        generateConstructor(classDefinition, joinChannels, sizeField, channelFields, joinChannelFields, hashChannelField);
        generateGetChannelCountMethod(classDefinition, channelFields);
        generateGetSizeInBytesMethod(classDefinition, sizeField);
        generateAppendToMethod(classDefinition, callSiteBinder, types, channelFields);
        generateHashPositionMethod(classDefinition, callSiteBinder, joinChannelTypes, joinChannelFields, hashChannelField);
        generateHashRowMethod(classDefinition, callSiteBinder, joinChannelTypes);
        generateRowEqualsRowMethod(classDefinition, callSiteBinder, joinChannelTypes);
        generatePositionEqualsRowMethod(classDefinition, callSiteBinder, joinChannelTypes, joinChannelFields);
        generatePositionEqualsPositionMethod(classDefinition, callSiteBinder, joinChannelTypes, joinChannelFields);

        return defineClass(classDefinition, PagesHashStrategy.class, callSiteBinder.getBindings(), getClass().getClassLoader());
    }

    private void generateConstructor(ClassDefinition classDefinition,
            List<Integer> joinChannels,
            FieldDefinition sizeField,
            List<FieldDefinition> channelFields,
            List<FieldDefinition> joinChannelFields,
            FieldDefinition hashChannelField)
    {
        Parameter channels = arg("channels", type(List.class, type(List.class, com.facebook.presto.spi.block.Block.class)));
        Parameter hashChannel = arg("hashChannel", type(Optional.class, Integer.class));
        MethodDefinition constructorDefinition = classDefinition.declareConstructor(a(PUBLIC), channels, hashChannel);

        Variable thisVariable = constructorDefinition.getThis();
        Variable blockIndex = constructorDefinition.getScope().declareVariable(int.class, "blockIndex");

        Block constructor = constructorDefinition
                .getBody()
                .comment("super();")
                .append(thisVariable)
                .invokeConstructor(Object.class);

        constructor.comment("this.size = 0")
                    .append(thisVariable.setField(sizeField, constantLong(0L)));

        constructor.comment("Set channel fields");

        for (int index = 0; index < channelFields.size(); index++) {
            ByteCodeExpression channel = channels.invoke("get", Object.class, constantInt(index))
                    .cast(type(List.class, com.facebook.presto.spi.block.Block.class));

            constructor.append(thisVariable.setField(channelFields.get(index), channel));

            Block loopBody = new Block();

            constructor.comment("for(blockIndex = 0; blockIndex < channel.size(); blockIndex++) { size += channel.get(i).getRetainedSizeInBytes() }")
                    .append(new ForLoop()
                            .initialize(blockIndex.set(constantInt(0)))
                            .condition(new Block()
                                    .append(blockIndex)
                                    .append(channel.invoke("size", int.class))
                                    .invokeStatic(CompilerOperations.class, "lessThan", boolean.class, int.class, int.class))
                            .update(new Block().incrementVariable(blockIndex, (byte) 1))
                            .body(loopBody));

            loopBody.append(thisVariable)
                    .append(thisVariable)
                    .getField(sizeField)
                    .append(
                            channel.invoke("get", Object.class, blockIndex)
                            .cast(type(com.facebook.presto.spi.block.Block.class))
                            .invoke("getRetainedSizeInBytes", int.class)
                            .cast(long.class))
                    .longAdd()
                    .putField(sizeField);
        }

        constructor.comment("Set join channel fields");
        for (int index = 0; index < joinChannelFields.size(); index++) {
            ByteCodeExpression joinChannel = channels.invoke("get", Object.class, constantInt(joinChannels.get(index)))
                    .cast(type(List.class, com.facebook.presto.spi.block.Block.class));

            constructor.append(thisVariable.setField(joinChannelFields.get(index), joinChannel));
        }

        constructor.comment("Set hashChannel");
        constructor.append(new IfStatement()
                .condition(hashChannel.invoke("isPresent", boolean.class))
                .ifTrue(thisVariable.setField(
                        hashChannelField,
                        channels.invoke("get", Object.class, hashChannel.invoke("get", Object.class).cast(Integer.class).cast(int.class))))
                .ifFalse(thisVariable.setField(
                        hashChannelField,
                        constantNull(hashChannelField.getType()))));
        constructor.ret();
    }

    private void generateGetChannelCountMethod(ClassDefinition classDefinition, List<FieldDefinition> channelFields)
    {
        classDefinition.declareMethod(
                a(PUBLIC),
                "getChannelCount",
                type(int.class))
                .getBody()
                .push(channelFields.size())
                .retInt();
    }

    private void generateGetSizeInBytesMethod(ClassDefinition classDefinition, FieldDefinition sizeField)
    {
        MethodDefinition method = classDefinition.declareMethod(a(PUBLIC), "getSizeInBytes", type(long.class));

        Variable thisVariable = method.getThis();
        method.getBody()
                .append(thisVariable.getField(sizeField))
                .retLong();
    }

    private void generateAppendToMethod(ClassDefinition classDefinition, CallSiteBinder callSiteBinder, List<Type> types, List<FieldDefinition> channelFields)
    {
        Parameter blockIndex = arg("blockIndex", int.class);
        Parameter blockPosition = arg("blockPosition", int.class);
        Parameter pageBuilder = arg("pageBuilder", PageBuilder.class);
        Parameter outputChannelOffset = arg("outputChannelOffset", int.class);
        MethodDefinition method = classDefinition.declareMethod(a(PUBLIC), "appendTo", type(void.class), blockIndex, blockPosition, pageBuilder, outputChannelOffset);

        Variable thisVariable = method.getThis();
        Block appendToBody = method.getBody();

        for (int index = 0; index < channelFields.size(); index++) {
            Type type = types.get(index);
            ByteCodeExpression typeExpression = constantType(callSiteBinder, type);

            ByteCodeExpression block = thisVariable
                    .getField(channelFields.get(index))
                    .invoke("get", Object.class, blockIndex)
                    .cast(com.facebook.presto.spi.block.Block.class);

            appendToBody
                    .comment("%s.appendTo(channel_%s.get(blockIndex), blockPosition, pageBuilder.getBlockBuilder(outputChannelOffset + %s));", type.getClass(), index, index)
                    .append(typeExpression)
                    .append(block)
                    .append(blockPosition)
                    .append(pageBuilder)
                    .append(outputChannelOffset)
                    .push(index)
                    .append(OpCode.IADD)
                    .invokeVirtual(PageBuilder.class, "getBlockBuilder", BlockBuilder.class, int.class)
                    .invokeInterface(Type.class, "appendTo", void.class, com.facebook.presto.spi.block.Block.class, int.class, BlockBuilder.class);
        }
        appendToBody.ret();
    }

    private void generateHashPositionMethod(ClassDefinition classDefinition, CallSiteBinder callSiteBinder, List<Type> joinChannelTypes, List<FieldDefinition> joinChannelFields, FieldDefinition hashChannelField)
    {
        Parameter blockIndex = arg("blockIndex", int.class);
        Parameter blockPosition = arg("blockPosition", int.class);
        MethodDefinition hashPositionMethod = classDefinition.declareMethod(
                a(PUBLIC),
                "hashPosition",
                type(int.class),
                blockIndex,
                blockPosition);

        Variable thisVariable = hashPositionMethod.getThis();
        ByteCodeExpression hashChannel = thisVariable.getField(hashChannelField);
        ByteCodeExpression bigintType = constantType(callSiteBinder, BigintType.BIGINT);

        IfStatement ifStatement = new IfStatement();
        ifStatement.condition(notEqual(hashChannel, constantNull(hashChannelField.getType())));
        ifStatement.ifTrue(
                bigintType.invoke(
                        "getLong",
                        long.class,
                        hashChannel.invoke("get", Object.class, blockIndex).cast(com.facebook.presto.spi.block.Block.class),
                        blockPosition)
                        .cast(int.class)
                        .ret()
        );

        hashPositionMethod
                .getBody()
                .append(ifStatement);

        Variable resultVariable = hashPositionMethod.getScope().declareVariable(int.class, "result");
        hashPositionMethod.getBody().push(0).putVariable(resultVariable);

        for (int index = 0; index < joinChannelTypes.size(); index++) {
            ByteCodeExpression type = constantType(callSiteBinder, joinChannelTypes.get(index));

            ByteCodeExpression block = hashPositionMethod
                    .getThis()
                    .getField(joinChannelFields.get(index))
                    .invoke("get", Object.class, blockIndex)
                    .cast(com.facebook.presto.spi.block.Block.class);

            hashPositionMethod
                    .getBody()
                    .getVariable(resultVariable)
                    .push(31)
                    .append(OpCode.IMUL)
                    .append(typeHashCode(type, block, blockPosition))
                    .append(OpCode.IADD)
                    .putVariable(resultVariable);
        }

        hashPositionMethod
                .getBody()
                .getVariable(resultVariable)
                .retInt();
    }

    private void generateHashRowMethod(ClassDefinition classDefinition, CallSiteBinder callSiteBinder, List<Type> joinChannelTypes)
    {
        Parameter position = arg("position", int.class);
        Parameter blocks = arg("blocks", com.facebook.presto.spi.block.Block[].class);
        MethodDefinition hashPositionMethod = classDefinition.declareMethod(a(PUBLIC), "hashRow", type(int.class), position, blocks);

        Variable resultVariable = hashPositionMethod.getScope().declareVariable(int.class, "result");
        hashPositionMethod.getBody().push(0).putVariable(resultVariable);

        for (int index = 0; index < joinChannelTypes.size(); index++) {
            ByteCodeExpression type = constantType(callSiteBinder, joinChannelTypes.get(index));

            // todo is the case needed
            ByteCodeExpression block = blocks.getElement(index).cast(com.facebook.presto.spi.block.Block.class);

            hashPositionMethod
                    .getBody()
                    .getVariable(resultVariable)
                    .push(31)
                    .append(OpCode.IMUL)
                    .append(typeHashCode(type, block, position))
                    .append(OpCode.IADD)
                    .putVariable(resultVariable);
        }

        hashPositionMethod
                .getBody()
                .getVariable(resultVariable)
                .retInt();
    }

    private static ByteCodeNode typeHashCode(ByteCodeExpression type, ByteCodeExpression blockRef, ByteCodeExpression blockPosition)
    {
        return new IfStatement()
            .condition(blockRef.invoke("isNull", boolean.class, blockPosition))
            .ifTrue(constantInt(0))
            .ifFalse(type.invoke("hash", int.class, blockRef, blockPosition));
    }

    private void generateRowEqualsRowMethod(
            ClassDefinition classDefinition,
            CallSiteBinder callSiteBinder,
            List<Type> joinChannelTypes)
    {
        MethodDefinition hashPositionMethod = classDefinition.declareMethod(
                a(PUBLIC),
                "rowEqualsRow",
                type(boolean.class),
                arg("leftPosition", int.class),
                arg("leftBlocks", com.facebook.presto.spi.block.Block[].class),
                arg("rightPosition", int.class),
                arg("rightBlocks", com.facebook.presto.spi.block.Block[].class));

        Scope compilerContext = hashPositionMethod.getScope();
        for (int index = 0; index < joinChannelTypes.size(); index++) {
            ByteCodeExpression type = constantType(callSiteBinder, joinChannelTypes.get(index));

            ByteCodeExpression leftBlock = compilerContext
                    .getVariable("leftBlocks")
                    .getElement(index);

            ByteCodeExpression rightBlock = compilerContext
                    .getVariable("rightBlocks")
                    .getElement(index);

            LabelNode checkNextField = new LabelNode("checkNextField");
            hashPositionMethod
                    .getBody()
                    .append(typeEquals(
                            type,
                            leftBlock,
                            compilerContext.getVariable("leftPosition"),
                            rightBlock,
                            compilerContext.getVariable("rightPosition")))
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

    private void generatePositionEqualsRowMethod(
            ClassDefinition classDefinition,
            CallSiteBinder callSiteBinder,
            List<Type> joinChannelTypes,
            List<FieldDefinition> joinChannelFields)
    {
        Parameter leftBlockIndex = arg("leftBlockIndex", int.class);
        Parameter leftBlockPosition = arg("leftBlockPosition", int.class);
        Parameter rightPosition = arg("rightPosition", int.class);
        Parameter rightBlocks = arg("rightBlocks", com.facebook.presto.spi.block.Block[].class);
        MethodDefinition hashPositionMethod = classDefinition.declareMethod(
                a(PUBLIC),
                "positionEqualsRow",
                type(boolean.class),
                leftBlockIndex,
                leftBlockPosition,
                rightPosition,
                rightBlocks);

        Variable thisVariable = hashPositionMethod.getThis();

        for (int index = 0; index < joinChannelTypes.size(); index++) {
            ByteCodeExpression type = constantType(callSiteBinder, joinChannelTypes.get(index));

            ByteCodeExpression leftBlock = thisVariable
                    .getField(joinChannelFields.get(index))
                    .invoke("get", Object.class, leftBlockIndex)
                    .cast(com.facebook.presto.spi.block.Block.class);

            ByteCodeExpression rightBlock = rightBlocks.getElement(index);

            LabelNode checkNextField = new LabelNode("checkNextField");
            hashPositionMethod
                    .getBody()
                    .append(typeEquals(type, leftBlock, leftBlockPosition, rightBlock, rightPosition))
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

    private void generatePositionEqualsPositionMethod(
            ClassDefinition classDefinition,
            CallSiteBinder callSiteBinder,
            List<Type> joinChannelTypes,
            List<FieldDefinition> joinChannelFields)
    {
        Parameter leftBlockIndex = arg("leftBlockIndex", int.class);
        Parameter leftBlockPosition = arg("leftBlockPosition", int.class);
        Parameter rightBlockIndex = arg("rightBlockIndex", int.class);
        Parameter rightBlockPosition = arg("rightBlockPosition", int.class);
        MethodDefinition hashPositionMethod = classDefinition.declareMethod(
                a(PUBLIC),
                "positionEqualsPosition",
                type(boolean.class),
                leftBlockIndex,
                leftBlockPosition,
                rightBlockIndex,
                rightBlockPosition);

        Variable thisVariable = hashPositionMethod.getThis();
        for (int index = 0; index < joinChannelTypes.size(); index++) {
            ByteCodeExpression type = constantType(callSiteBinder, joinChannelTypes.get(index));

            ByteCodeExpression leftBlock = thisVariable
                    .getField(joinChannelFields.get(index))
                    .invoke("get", Object.class, leftBlockIndex)
                    .cast(com.facebook.presto.spi.block.Block.class);

            ByteCodeExpression rightBlock = thisVariable
                    .getField(joinChannelFields.get(index))
                    .invoke("get", Object.class, rightBlockIndex)
                    .cast(com.facebook.presto.spi.block.Block.class);

            LabelNode checkNextField = new LabelNode("checkNextField");
            hashPositionMethod
                    .getBody()
                    .append(typeEquals(type, leftBlock, leftBlockPosition, rightBlock, rightBlockPosition))
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

    private static ByteCodeNode typeEquals(
            ByteCodeExpression type,
            ByteCodeExpression leftBlock,
            ByteCodeExpression leftBlockPosition,
            ByteCodeExpression rightBlock,
            ByteCodeExpression rightBlockPosition)
    {
        IfStatement ifStatement = new IfStatement();
        ifStatement.condition()
                .append(leftBlock.invoke("isNull", boolean.class, leftBlockPosition))
                .append(rightBlock.invoke("isNull", boolean.class, rightBlockPosition))
                .append(OpCode.IOR);

        ifStatement.ifTrue()
                .append(leftBlock.invoke("isNull", boolean.class, leftBlockPosition))
                .append(rightBlock.invoke("isNull", boolean.class, rightBlockPosition))
                .append(OpCode.IAND);

        ifStatement.ifFalse().append(type.invoke("equalTo", boolean.class, leftBlock, leftBlockPosition, rightBlock, rightBlockPosition));

        return ifStatement;
    }

    public static class LookupSourceFactory
    {
        private final Constructor<? extends LookupSource> constructor;
        private final PagesHashStrategyFactory pagesHashStrategyFactory;

        public LookupSourceFactory(Class<? extends LookupSource> lookupSourceClass, PagesHashStrategyFactory pagesHashStrategyFactory)
        {
            this.pagesHashStrategyFactory = pagesHashStrategyFactory;
            try {
                constructor = lookupSourceClass.getConstructor(LongArrayList.class, PagesHashStrategy.class);
            }
            catch (NoSuchMethodException e) {
                throw Throwables.propagate(e);
            }
        }

        public LookupSource createLookupSource(LongArrayList addresses, List<List<com.facebook.presto.spi.block.Block>> channels, Optional<Integer> hashChannel)
        {
            PagesHashStrategy pagesHashStrategy = pagesHashStrategyFactory.createPagesHashStrategy(channels, hashChannel);
            try {
                return constructor.newInstance(addresses, pagesHashStrategy);
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
                constructor = pagesHashStrategyClass.getConstructor(List.class, Optional.class);
            }
            catch (NoSuchMethodException e) {
                throw Throwables.propagate(e);
            }
        }

        public PagesHashStrategy createPagesHashStrategy(List<? extends List<com.facebook.presto.spi.block.Block>> channels, Optional<Integer> hashChannel)
        {
            try {
                return constructor.newInstance(channels, hashChannel);
            }
            catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
    }

    private static final class CacheKey
    {
        private final List<Type> types;
        private final List<Integer> joinChannels;

        private CacheKey(List<? extends Type> types, List<Integer> joinChannels)
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
            return Objects.hash(types, joinChannels);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof CacheKey)) {
                return false;
            }
            CacheKey other = (CacheKey) obj;
            return Objects.equals(this.types, other.types) &&
                    Objects.equals(this.joinChannels, other.joinChannels);
        }
    }
}

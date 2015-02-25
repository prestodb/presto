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
import com.facebook.presto.byteCode.CompilerContext;
import com.facebook.presto.byteCode.DynamicClassLoader;
import com.facebook.presto.byteCode.FieldDefinition;
import com.facebook.presto.byteCode.MethodDefinition;
import com.facebook.presto.byteCode.OpCode;
import com.facebook.presto.byteCode.Variable;
import com.facebook.presto.byteCode.control.IfStatement;
import com.facebook.presto.byteCode.control.IfStatement.IfStatementBuilder;
import com.facebook.presto.byteCode.expression.ByteCodeExpression;
import com.facebook.presto.byteCode.instruction.LabelNode;
import com.facebook.presto.operator.InMemoryJoinHash;
import com.facebook.presto.operator.LookupSource;
import com.facebook.presto.operator.OperatorContext;
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
import static com.facebook.presto.byteCode.NamedParameterDefinition.arg;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantInt;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantNull;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.notEqual;
import static com.facebook.presto.sql.gen.Bootstrap.BOOTSTRAP_METHOD;
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
                InMemoryJoinHash.class);

        return new LookupSourceFactory(lookupSourceClass, new PagesHashStrategyFactory(pagesHashStrategyClass));
    }

    private Class<? extends PagesHashStrategy> internalCompileHashStrategy(List<Type> types, List<Integer> joinChannels)
    {
        CallSiteBinder callSiteBinder = new CallSiteBinder();

        ClassDefinition classDefinition = new ClassDefinition(new CompilerContext(BOOTSTRAP_METHOD),
                a(PUBLIC, FINAL),
                makeClassName("PagesHashStrategy"),
                type(Object.class),
                type(PagesHashStrategy.class));

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

        generateConstructor(classDefinition, joinChannels, channelFields, joinChannelFields, hashChannelField);
        generateGetChannelCountMethod(classDefinition, channelFields);
        generateAppendToMethod(classDefinition, callSiteBinder, types, channelFields);
        generateHashPositionMethod(classDefinition, callSiteBinder, joinChannelTypes, joinChannelFields, hashChannelField);
        generateHashRowMethod(classDefinition, callSiteBinder, joinChannelTypes, joinChannelFields);
        generatePositionEqualsRowMethod(classDefinition, callSiteBinder, joinChannelTypes, joinChannelFields);
        generatePositionEqualsPositionMethod(classDefinition, callSiteBinder, joinChannelTypes, joinChannelFields);

        return defineClass(classDefinition, PagesHashStrategy.class, callSiteBinder.getBindings(), getClass().getClassLoader());
    }

    private void generateConstructor(ClassDefinition classDefinition,
            List<Integer> joinChannels,
            List<FieldDefinition> channelFields,
            List<FieldDefinition> joinChannelFields,
            FieldDefinition hashChannelField)
    {
        CompilerContext compilerContext = new CompilerContext(BOOTSTRAP_METHOD);
        Block constructor = classDefinition.declareConstructor(compilerContext,
                a(PUBLIC),
                arg("channels", type(List.class, type(List.class, com.facebook.presto.spi.block.Block.class))),
                arg("hashChannel", type(Optional.class, Integer.class)))
                .getBody()
                .comment("super();")
                .pushThis()
                .invokeConstructor(Object.class);

        constructor.comment("Set channel fields");
        for (int index = 0; index < channelFields.size(); index++) {
            ByteCodeExpression channel = compilerContext.getVariable("channels")
                    .invoke("get", Object.class, constantInt(index))
                    .cast(type(List.class, com.facebook.presto.spi.block.Block.class));

            constructor.append(compilerContext.getVariable("this").setField(channelFields.get(index), channel));
        }

        constructor.comment("Set join channel fields");
        for (int index = 0; index < joinChannelFields.size(); index++) {
            ByteCodeExpression joinChannel = compilerContext.getVariable("channels")
                    .invoke("get", Object.class, constantInt(joinChannels.get(index)))
                    .cast(type(List.class, com.facebook.presto.spi.block.Block.class));

            constructor.append(compilerContext.getVariable("this").setField(joinChannelFields.get(index), joinChannel));
        }

        constructor.comment("Set hashChannel");
        Variable hashChannel = compilerContext.getVariable("hashChannel");
        constructor.append(new IfStatement(
                compilerContext,
                hashChannel.invoke("isPresent", boolean.class),
                compilerContext.getVariable("this").setField(hashChannelField, compilerContext.getVariable("channels").invoke("get", Object.class, hashChannel.invoke("get", Object.class).cast(Integer.class).cast(int.class))),
                compilerContext.getVariable("this").setField(hashChannelField, constantNull(hashChannelField.getType()))
        ));
        constructor.ret();
    }

    private void generateGetChannelCountMethod(ClassDefinition classDefinition, List<FieldDefinition> channelFields)
    {
        classDefinition.declareMethod(new CompilerContext(BOOTSTRAP_METHOD),
                a(PUBLIC),
                "getChannelCount",
                type(int.class))
                .getBody()
                .push(channelFields.size())
                .retInt();
    }

    private void generateAppendToMethod(ClassDefinition classDefinition, CallSiteBinder callSiteBinder, List<Type> types, List<FieldDefinition> channelFields)
    {
        CompilerContext compilerContext = new CompilerContext(BOOTSTRAP_METHOD);
        Block appendToBody = classDefinition.declareMethod(compilerContext,
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
            ByteCodeExpression typeExpression = constantType(compilerContext, callSiteBinder, type);

            ByteCodeExpression block = compilerContext
                    .getVariable("this")
                    .getField(channelFields.get(index))
                    .invoke("get", Object.class, compilerContext.getVariable("blockIndex"))
                    .cast(com.facebook.presto.spi.block.Block.class);

            appendToBody
                    .comment("%s.appendTo(channel_%s.get(blockIndex), blockPosition, pageBuilder.getBlockBuilder(outputChannelOffset + %s));", type.getClass(), index, index)
                    .append(typeExpression)
                    .append(block)
                    .getVariable("blockPosition")
                    .getVariable("pageBuilder")
                    .getVariable("outputChannelOffset")
                    .push(index)
                    .append(OpCode.IADD)
                    .invokeVirtual(PageBuilder.class, "getBlockBuilder", BlockBuilder.class, int.class)
                    .invokeInterface(Type.class, "appendTo", void.class, com.facebook.presto.spi.block.Block.class, int.class, BlockBuilder.class);
        }
        appendToBody.ret();
    }

    private void generateHashPositionMethod(ClassDefinition classDefinition, CallSiteBinder callSiteBinder, List<Type> joinChannelTypes, List<FieldDefinition> joinChannelFields, FieldDefinition hashChannelField)
    {
        CompilerContext compilerContext = new CompilerContext(BOOTSTRAP_METHOD);
        MethodDefinition hashPositionMethod = classDefinition.declareMethod(compilerContext,
                a(PUBLIC),
                "hashPosition",
                type(int.class),
                arg("blockIndex", int.class),
                arg("blockPosition", int.class));

        Variable thisVariable = compilerContext.getVariable("this");
        ByteCodeExpression hashChannel = thisVariable.getField(hashChannelField);
        ByteCodeExpression bigintType = constantType(compilerContext, callSiteBinder, BigintType.BIGINT);
        Variable blockIndex = compilerContext.getVariable("blockIndex");
        Variable blockPosition = compilerContext.getVariable("blockPosition");

        IfStatementBuilder ifStatementBuilder = new IfStatementBuilder(compilerContext);
        ifStatementBuilder.condition(notEqual(hashChannel, constantNull(hashChannelField.getType())));
        ifStatementBuilder.ifTrue(
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
                .append(ifStatementBuilder.build());

        Variable resultVariable = hashPositionMethod.getCompilerContext().declareVariable(int.class, "result");
        hashPositionMethod.getBody().push(0).putVariable(resultVariable);

        for (int index = 0; index < joinChannelTypes.size(); index++) {
            ByteCodeExpression type = constantType(compilerContext, callSiteBinder, joinChannelTypes.get(index));

            ByteCodeExpression block = compilerContext
                    .getVariable("this")
                    .getField(joinChannelFields.get(index))
                    .invoke("get", Object.class, compilerContext.getVariable("blockIndex"))
                    .cast(com.facebook.presto.spi.block.Block.class);

            hashPositionMethod
                    .getBody()
                    .getVariable(resultVariable)
                    .push(31)
                    .append(OpCode.IMUL)
                    .append(typeHashCode(compilerContext, type, block, compilerContext.getVariable("blockPosition")))
                    .append(OpCode.IADD)
                    .putVariable(resultVariable);
        }

        hashPositionMethod
                .getBody()
                .getVariable(resultVariable)
                .retInt();
    }

    private void generateHashRowMethod(ClassDefinition classDefinition, CallSiteBinder callSiteBinder, List<Type> joinChannelTypes, List<FieldDefinition> joinChannelFields)
    {
        CompilerContext compilerContext = new CompilerContext(BOOTSTRAP_METHOD);
        MethodDefinition hashPositionMethod = classDefinition.declareMethod(compilerContext,
                a(PUBLIC),
                "hashRow",
                type(int.class),
                arg("position", int.class),
                arg("blocks", com.facebook.presto.spi.block.Block[].class));

        Variable resultVariable = hashPositionMethod.getCompilerContext().declareVariable(int.class, "result");
        hashPositionMethod.getBody().push(0).putVariable(resultVariable);

        for (int index = 0; index < joinChannelTypes.size(); index++) {
            ByteCodeExpression type = constantType(compilerContext, callSiteBinder, joinChannelTypes.get(index));

            ByteCodeExpression block = compilerContext
                    .getVariable("blocks")
                    .getElement(index)
                    .cast(com.facebook.presto.spi.block.Block.class);

            hashPositionMethod
                    .getBody()
                    .getVariable(resultVariable)
                    .push(31)
                    .append(OpCode.IMUL)
                    .append(typeHashCode(compilerContext, type, block, compilerContext.getVariable("position")))
                    .append(OpCode.IADD)
                    .putVariable(resultVariable);
        }

        hashPositionMethod
                .getBody()
                .getVariable(resultVariable)
                .retInt();
    }

    private static ByteCodeNode typeHashCode(CompilerContext compilerContext, ByteCodeExpression type, ByteCodeExpression blockRef, ByteCodeExpression blockPosition)
    {
        IfStatementBuilder ifStatementBuilder = new IfStatementBuilder(compilerContext);

        ifStatementBuilder.condition(new Block(compilerContext).append(blockRef.invoke("isNull", boolean.class, blockPosition)));

        ifStatementBuilder.ifTrue(new Block(compilerContext).push(0));

        ifStatementBuilder.ifFalse(new Block(compilerContext).append(type.invoke("hash", int.class, blockRef, blockPosition)));

        return ifStatementBuilder.build();
    }

    private void generatePositionEqualsRowMethod(
            ClassDefinition classDefinition,
            CallSiteBinder callSiteBinder,
            List<Type> joinChannelTypes,
            List<FieldDefinition> joinChannelFields)
    {
        CompilerContext compilerContext = new CompilerContext(BOOTSTRAP_METHOD);
        MethodDefinition hashPositionMethod = classDefinition.declareMethod(compilerContext,
                a(PUBLIC),
                "positionEqualsRow",
                type(boolean.class),
                arg("leftBlockIndex", int.class),
                arg("leftBlockPosition", int.class),
                arg("rightPosition", int.class),
                arg("rightBlocks", com.facebook.presto.spi.block.Block[].class));

        for (int index = 0; index < joinChannelTypes.size(); index++) {
            ByteCodeExpression type = constantType(compilerContext, callSiteBinder, joinChannelTypes.get(index));

            ByteCodeExpression leftBlock = compilerContext
                    .getVariable("this")
                    .getField(joinChannelFields.get(index))
                    .invoke("get", Object.class, compilerContext.getVariable("leftBlockIndex"))
                    .cast(com.facebook.presto.spi.block.Block.class);

            ByteCodeExpression rightBlock = compilerContext
                    .getVariable("rightBlocks")
                    .getElement(index);

            LabelNode checkNextField = new LabelNode("checkNextField");
            hashPositionMethod
                    .getBody()
                    .append(typeEquals(compilerContext,
                            type,
                            leftBlock,
                            compilerContext.getVariable("leftBlockPosition"),
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

    private void generatePositionEqualsPositionMethod(
            ClassDefinition classDefinition,
            CallSiteBinder callSiteBinder,
            List<Type> joinChannelTypes,
            List<FieldDefinition> joinChannelFields)
    {
        CompilerContext compilerContext = new CompilerContext(BOOTSTRAP_METHOD);
        MethodDefinition hashPositionMethod = classDefinition.declareMethod(compilerContext,
                a(PUBLIC),
                "positionEqualsPosition",
                type(boolean.class),
                arg("leftBlockIndex", int.class),
                arg("leftBlockPosition", int.class),
                arg("rightBlockIndex", int.class),
                arg("rightBlockPosition", int.class));

        for (int index = 0; index < joinChannelTypes.size(); index++) {
            ByteCodeExpression type = constantType(compilerContext, callSiteBinder, joinChannelTypes.get(index));

            Variable blockIndex = compilerContext.getVariable("leftBlockIndex");
            ByteCodeExpression leftBlock = compilerContext
                    .getVariable("this")
                    .getField(joinChannelFields.get(index))
                    .invoke("get", Object.class, blockIndex)
                    .cast(com.facebook.presto.spi.block.Block.class);

            ByteCodeExpression rightBlock = compilerContext
                    .getVariable("this")
                    .getField(joinChannelFields.get(index))
                    .invoke("get", Object.class, compilerContext.getVariable("rightBlockIndex"))
                    .cast(com.facebook.presto.spi.block.Block.class);

            LabelNode checkNextField = new LabelNode("checkNextField");
            hashPositionMethod
                    .getBody()
                    .append(typeEquals(compilerContext,
                            type,
                            leftBlock,
                            compilerContext.getVariable("leftBlockPosition"),
                            rightBlock,
                            compilerContext.getVariable("rightBlockPosition")))
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
            CompilerContext compilerContext,
            ByteCodeExpression type,
            ByteCodeExpression leftBlock,
            ByteCodeExpression leftBlockPosition,
            ByteCodeExpression rightBlock,
            ByteCodeExpression rightBlockPosition)
    {
        IfStatementBuilder ifStatementBuilder = new IfStatementBuilder(compilerContext);
        ifStatementBuilder.condition(new Block(compilerContext)
                .append(leftBlock.invoke("isNull", boolean.class, leftBlockPosition))
                .append(rightBlock.invoke("isNull", boolean.class, rightBlockPosition))
                .append(OpCode.IOR));

        ifStatementBuilder.ifTrue(new Block(compilerContext)
                .append(leftBlock.invoke("isNull", boolean.class, leftBlockPosition))
                .append(rightBlock.invoke("isNull", boolean.class, rightBlockPosition))
                .append(OpCode.IAND));

        ifStatementBuilder.ifFalse(new Block(compilerContext).append(type.invoke("equalTo", boolean.class, leftBlock, leftBlockPosition, rightBlock, rightBlockPosition)));

        return ifStatementBuilder.build();
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

        public LookupSource createLookupSource(LongArrayList addresses, List<Type> types, List<List<com.facebook.presto.spi.block.Block>> channels, Optional<Integer> hashChannel, OperatorContext operatorContext)
        {
            PagesHashStrategy pagesHashStrategy = pagesHashStrategyFactory.createPagesHashStrategy(channels, hashChannel);
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

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
import com.facebook.presto.bytecode.DynamicClassLoader;
import com.facebook.presto.bytecode.FieldDefinition;
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.Parameter;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.control.IfStatement;
import com.facebook.presto.bytecode.expression.BytecodeExpression;
import com.facebook.presto.bytecode.instruction.JumpInstruction;
import com.facebook.presto.bytecode.instruction.LabelNode;
import com.facebook.presto.operator.JoinProbe;
import com.facebook.presto.operator.JoinProbeFactory;
import com.facebook.presto.operator.LookupJoinOperator;
import com.facebook.presto.operator.LookupJoinOperatorFactory;
import com.facebook.presto.operator.LookupJoinOperators.JoinType;
import com.facebook.presto.operator.LookupSource;
import com.facebook.presto.operator.LookupSourceFactory;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.SimpleJoinProbe;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ExecutionError;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static com.facebook.presto.bytecode.Access.FINAL;
import static com.facebook.presto.bytecode.Access.PRIVATE;
import static com.facebook.presto.bytecode.Access.PUBLIC;
import static com.facebook.presto.bytecode.Access.a;
import static com.facebook.presto.bytecode.CompilerUtils.defineClass;
import static com.facebook.presto.bytecode.CompilerUtils.makeClassName;
import static com.facebook.presto.bytecode.Parameter.arg;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantInt;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantLong;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.newInstance;
import static com.facebook.presto.sql.gen.SqlTypeBytecodeExpression.constantType;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class JoinProbeCompiler
{
    private final LoadingCache<JoinOperatorCacheKey, HashJoinOperatorFactoryFactory> joinProbeFactories = CacheBuilder.newBuilder()
            .recordStats()
            .maximumSize(1000)
            .build(new CacheLoader<JoinOperatorCacheKey, HashJoinOperatorFactoryFactory>()
            {
                @Override
                public HashJoinOperatorFactoryFactory load(JoinOperatorCacheKey key)
                        throws Exception
                {
                    return internalCompileJoinOperatorFactory(key.getTypes(), key.getProbeOutputChannels(), key.getProbeChannels(), key.getProbeHashChannel());
                }
            });

    @Managed
    @Nested
    public CacheStatsMBean getJoinProbeFactoriesStats()
    {
        return new CacheStatsMBean(joinProbeFactories);
    }

    public OperatorFactory compileJoinOperatorFactory(int operatorId,
            PlanNodeId planNodeId,
            LookupSourceFactory lookupSourceFactory,
            List<? extends Type> probeTypes,
            List<Integer> probeJoinChannel,
            Optional<Integer> probeHashChannel,
            List<Integer> probeOutputChannels,
            JoinType joinType)
    {
        try {
            List<Type> probeOutputChannelTypes = probeOutputChannels.stream()
                    .map(probeTypes::get)
                    .collect(toImmutableList());

            HashJoinOperatorFactoryFactory operatorFactoryFactory = joinProbeFactories.get(new JoinOperatorCacheKey(
                    probeTypes,
                    probeOutputChannels,
                    probeJoinChannel,
                    probeHashChannel,
                    joinType));
            return operatorFactoryFactory.createHashJoinOperatorFactory(operatorId, planNodeId, lookupSourceFactory, probeTypes, probeOutputChannelTypes, joinType);
        }
        catch (ExecutionException | UncheckedExecutionException | ExecutionError e) {
            throw Throwables.propagate(e.getCause());
        }
    }

    @VisibleForTesting
    public HashJoinOperatorFactoryFactory internalCompileJoinOperatorFactory(List<Type> types, List<Integer> probeOutputChannels, List<Integer> probeJoinChannel, Optional<Integer> probeHashChannel)
    {
        Class<? extends JoinProbe> joinProbeClass = compileJoinProbe(types, probeOutputChannels, probeJoinChannel, probeHashChannel);

        ClassDefinition classDefinition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName("JoinProbeFactory"),
                type(Object.class),
                type(JoinProbeFactory.class));

        classDefinition.declareDefaultConstructor(a(PUBLIC));

        Parameter lookupSource = arg("lookupSource", LookupSource.class);
        Parameter page = arg("page", Page.class);
        MethodDefinition method = classDefinition.declareMethod(a(PUBLIC), "createJoinProbe", type(JoinProbe.class), lookupSource, page);

        method.getBody()
                .newObject(joinProbeClass)
                .dup()
                .append(lookupSource)
                .append(page)
                .invokeConstructor(joinProbeClass, LookupSource.class, Page.class)
                .retObject();

        DynamicClassLoader classLoader = new DynamicClassLoader(joinProbeClass.getClassLoader());

        JoinProbeFactory joinProbeFactory;
        if (probeJoinChannel.isEmpty()) {
            // see comment in PagesIndex#createLookupSource
            joinProbeFactory = new SimpleJoinProbe.SimpleJoinProbeFactory(types, probeOutputChannels, probeJoinChannel, probeHashChannel);
        }
        else {
            Class<? extends JoinProbeFactory> joinProbeFactoryClass = defineClass(classDefinition, JoinProbeFactory.class, classLoader);
            try {
                joinProbeFactory = joinProbeFactoryClass.newInstance();
            }
            catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }

        Class<? extends OperatorFactory> operatorFactoryClass = IsolatedClass.isolateClass(
                classLoader,
                OperatorFactory.class,
                LookupJoinOperatorFactory.class,
                LookupJoinOperator.class);

        return new HashJoinOperatorFactoryFactory(joinProbeFactory, operatorFactoryClass);
    }

    @VisibleForTesting
    public JoinProbeFactory internalCompileJoinProbe(List<Type> types, List<Integer> probeOutputChannels, List<Integer> probeChannels, Optional<Integer> probeHashChannel)
    {
        return new ReflectionJoinProbeFactory(compileJoinProbe(types, probeOutputChannels, probeChannels, probeHashChannel));
    }

    private Class<? extends JoinProbe> compileJoinProbe(List<Type> types, List<Integer> probeOutputChannels, List<Integer> probeChannels, Optional<Integer> probeHashChannel)
    {
        CallSiteBinder callSiteBinder = new CallSiteBinder();

        ClassDefinition classDefinition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName("JoinProbe"),
                type(Object.class),
                type(JoinProbe.class));

        // declare fields
        FieldDefinition lookupSourceField = classDefinition.declareField(a(PRIVATE, FINAL), "lookupSource", LookupSource.class);
        FieldDefinition positionCountField = classDefinition.declareField(a(PRIVATE, FINAL), "positionCount", int.class);
        List<FieldDefinition> blockFields = new ArrayList<>();
        for (int i = 0; i < types.size(); i++) {
            FieldDefinition channelField = classDefinition.declareField(a(PRIVATE, FINAL), "block_" + i, Block.class);
            blockFields.add(channelField);
        }
        List<FieldDefinition> probeBlockFields = new ArrayList<>();
        for (int i = 0; i < probeChannels.size(); i++) {
            FieldDefinition channelField = classDefinition.declareField(a(PRIVATE, FINAL), "probeBlock_" + i, Block.class);
            probeBlockFields.add(channelField);
        }
        FieldDefinition probeBlocksArrayField = classDefinition.declareField(a(PRIVATE, FINAL), "probeBlocks", Block[].class);
        FieldDefinition probePageField = classDefinition.declareField(a(PRIVATE, FINAL), "probePage", Page.class);
        FieldDefinition pageField = classDefinition.declareField(a(PRIVATE, FINAL), "page", Page.class);
        FieldDefinition positionField = classDefinition.declareField(a(PRIVATE), "position", int.class);
        FieldDefinition probeHashBlockField = classDefinition.declareField(a(PRIVATE, FINAL), "probeHashBlock", Block.class);

        generateConstructor(classDefinition, probeChannels, probeHashChannel, lookupSourceField, blockFields, probeBlockFields, probeBlocksArrayField, probePageField, pageField, probeHashBlockField, positionField, positionCountField);
        generateGetChannelCountMethod(classDefinition, probeOutputChannels.size());
        generateAppendToMethod(classDefinition, callSiteBinder, types, probeOutputChannels, blockFields, positionField);
        generateAdvanceNextPosition(classDefinition, positionField, positionCountField);
        generateGetCurrentJoinPosition(classDefinition, callSiteBinder, lookupSourceField, probePageField, pageField, probeHashChannel, probeHashBlockField, positionField);
        generateCurrentRowContainsNull(classDefinition, probeBlockFields, positionField);
        generateGetPosition(classDefinition, positionField);
        generateGetPage(classDefinition, pageField);

        return defineClass(classDefinition, JoinProbe.class, callSiteBinder.getBindings(), getClass().getClassLoader());
    }

    private static void generateConstructor(ClassDefinition classDefinition,
            List<Integer> probeChannels,
            Optional<Integer> probeHashChannel,
            FieldDefinition lookupSourceField,
            List<FieldDefinition> blockFields,
            List<FieldDefinition> probeChannelFields,
            FieldDefinition probeBlocksArrayField,
            FieldDefinition probePageField,
            FieldDefinition pageField,
            FieldDefinition probeHashBlockField,
            FieldDefinition positionField,
            FieldDefinition positionCountField)
    {
        Parameter lookupSource = arg("lookupSource", LookupSource.class);
        Parameter page = arg("page", Page.class);
        MethodDefinition constructorDefinition = classDefinition.declareConstructor(a(PUBLIC), lookupSource, page);

        Variable thisVariable = constructorDefinition.getThis();

        BytecodeBlock constructor = constructorDefinition
                .getBody()
                .comment("super();")
                .append(thisVariable)
                .invokeConstructor(Object.class);

        constructor.comment("this.lookupSource = lookupSource;")
                .append(thisVariable.setField(lookupSourceField, lookupSource));

        constructor.comment("this.positionCount = page.getPositionCount();")
                .append(thisVariable.setField(positionCountField, page.invoke("getPositionCount", int.class)));

        constructor.comment("Set block fields");
        for (int index = 0; index < blockFields.size(); index++) {
            constructor.append(thisVariable.setField(
                    blockFields.get(index),
                    page.invoke("getBlock", Block.class, constantInt(index))));
        }

        constructor.comment("Set probe channel fields");
        for (int index = 0; index < probeChannelFields.size(); index++) {
            constructor.append(thisVariable.setField(
                    probeChannelFields.get(index),
                    thisVariable.getField(blockFields.get(probeChannels.get(index)))));
        }

        constructor.comment("this.probeBlocks = new Block[<probeChannelCount>];");
        constructor
                .append(thisVariable)
                .push(probeChannelFields.size())
                .newArray(Block.class)
                .putField(probeBlocksArrayField);
        for (int index = 0; index < probeChannelFields.size(); index++) {
            constructor
                    .append(thisVariable)
                    .getField(probeBlocksArrayField)
                    .push(index)
                    .append(thisVariable)
                    .getField(probeChannelFields.get(index))
                    .putObjectArrayElement();
        }

        constructor.comment("this.page = page")
                .append(thisVariable.setField(pageField, page));

        constructor.comment("this.probePage = new Page(probeBlocks)")
                .append(thisVariable.setField(probePageField, newInstance(Page.class, thisVariable.getField(probeBlocksArrayField))));

        if (probeHashChannel.isPresent()) {
            Integer index = probeHashChannel.get();
            constructor.comment("this.probeHashBlock = blocks[hashChannel.get()]")
                    .append(thisVariable.setField(
                            probeHashBlockField,
                            thisVariable.getField(blockFields.get(index))));
        }

        constructor.comment("this.position = -1;")
                .append(thisVariable.setField(positionField, constantInt(-1)));

        constructor.ret();
    }

    private static void generateGetChannelCountMethod(ClassDefinition classDefinition, int channelCount)
    {
        classDefinition.declareMethod(
                a(PUBLIC),
                "getOutputChannelCount",
                type(int.class))
                .getBody()
                .push(channelCount)
                .retInt();
    }

    private static void generateAppendToMethod(
            ClassDefinition classDefinition,
            CallSiteBinder callSiteBinder,
            List<Type> types,
            List<Integer> probeOutputChannels,
            List<FieldDefinition> blockFields,
            FieldDefinition positionField)
    {
        Parameter pageBuilder = arg("pageBuilder", PageBuilder.class);
        MethodDefinition method = classDefinition.declareMethod(
                a(PUBLIC),
                "appendTo",
                type(void.class),
                pageBuilder);

        Variable thisVariable = method.getThis();
        int pageBuilderOutputChannel = 0;
        for (int outputChannel : probeOutputChannels) {
            Type type = types.get(outputChannel);
            method.getBody()
                    .comment("%s.appendTo(block_%s, position, pageBuilder.getBlockBuilder(%s));", type.getClass(), outputChannel, pageBuilderOutputChannel)
                    .append(constantType(callSiteBinder, type).invoke("appendTo", void.class,
                            thisVariable.getField(blockFields.get(outputChannel)),
                            thisVariable.getField(positionField),
                            pageBuilder.invoke("getBlockBuilder", BlockBuilder.class, constantInt(pageBuilderOutputChannel++))));
        }
        method.getBody()
                .ret();
    }

    private static void generateAdvanceNextPosition(ClassDefinition classDefinition, FieldDefinition positionField, FieldDefinition positionCountField)
    {
        MethodDefinition method = classDefinition.declareMethod(
                a(PUBLIC),
                "advanceNextPosition",
                type(boolean.class));

        Variable thisVariable = method.getThis();
        method.getBody()
                .comment("this.position = this.position + 1;")
                .append(thisVariable)
                .append(thisVariable)
                .getField(positionField)
                .push(1)
                .intAdd()
                .putField(positionField);

        LabelNode lessThan = new LabelNode("lessThan");
        LabelNode end = new LabelNode("end");
        method.getBody()
                .comment("return position < positionCount;")
                .append(thisVariable)
                .getField(positionField)
                .append(thisVariable)
                .getField(positionCountField)
                .append(JumpInstruction.jumpIfIntLessThan(lessThan))
                .push(false)
                .gotoLabel(end)
                .visitLabel(lessThan)
                .push(true)
                .visitLabel(end)
                .retBoolean();
    }

    private static void generateGetCurrentJoinPosition(ClassDefinition classDefinition,
            CallSiteBinder callSiteBinder,
            FieldDefinition lookupSourceField,
            FieldDefinition probePageField,
            FieldDefinition pageField,
            Optional<Integer> probeHashChannel,
            FieldDefinition probeHashBlockField,
            FieldDefinition positionField)
    {
        MethodDefinition method = classDefinition.declareMethod(
                a(PUBLIC),
                "getCurrentJoinPosition",
                type(long.class));

        Variable thisVariable = method.getThis();
        BytecodeBlock body = method.getBody()
                .append(new IfStatement()
                        .condition(thisVariable.invoke("currentRowContainsNull", boolean.class))
                        .ifTrue(constantLong(-1).ret()));

        BytecodeExpression position = thisVariable.getField(positionField);
        BytecodeExpression hashChannelsPage = thisVariable.getField(probePageField);
        BytecodeExpression allChannelsPage = thisVariable.getField(pageField);
        BytecodeExpression probeHashBlock = thisVariable.getField(probeHashBlockField);
        if (probeHashChannel.isPresent()) {
            body.append(thisVariable.getField(lookupSourceField).invoke("getJoinPosition", long.class,
                    position,
                    hashChannelsPage,
                    allChannelsPage,
                    constantType(callSiteBinder, BigintType.BIGINT).invoke("getLong",
                            long.class,
                            probeHashBlock,
                            position)))
                    .retLong();
        }
        else {
            body.append(thisVariable.getField(lookupSourceField).invoke("getJoinPosition", long.class, position, hashChannelsPage, allChannelsPage)).retLong();
        }
    }

    private static void generateCurrentRowContainsNull(ClassDefinition classDefinition, List<FieldDefinition> probeBlockFields, FieldDefinition positionField)
    {
        MethodDefinition method = classDefinition.declareMethod(
                a(PRIVATE),
                "currentRowContainsNull",
                type(boolean.class));

        Variable thisVariable = method.getThis();
        for (FieldDefinition probeBlockField : probeBlockFields) {
            LabelNode checkNextField = new LabelNode("checkNextField");
            method.getBody()
                    .append(thisVariable.getField(probeBlockField).invoke("isNull", boolean.class, thisVariable.getField(positionField)))
                    .ifFalseGoto(checkNextField)
                    .push(true)
                    .retBoolean()
                    .visitLabel(checkNextField);
        }

        method.getBody()
                .push(false)
                .retInt();
    }

    private static void generateGetPosition(ClassDefinition classDefinition, FieldDefinition positionField)
    {
        // dummy implementation for now
        // compiled class is used only in usecase case when result of this method is ignored.
        MethodDefinition method = classDefinition.declareMethod(
                a(PUBLIC),
                "getPosition",
                type(int.class));
        Variable thisVariable = method.getThis();
        method.getBody()
                .append(thisVariable.getField(positionField))
                .retInt();
    }

    private static void generateGetPage(ClassDefinition classDefinition, FieldDefinition pageField)
    {
        // dummy implementation for now
        // compiled class is used only in usecase case when result of this method is ignored.
        MethodDefinition method = classDefinition.declareMethod(
                a(PUBLIC),
                "getPage",
                type(Page.class));
        Variable thisVariable = method.getThis();
        method.getBody()
                .append(thisVariable.getField(pageField))
                .ret(Page.class);
    }

    public static class ReflectionJoinProbeFactory
            implements JoinProbeFactory
    {
        private final Constructor<? extends JoinProbe> constructor;

        public ReflectionJoinProbeFactory(Class<? extends JoinProbe> joinProbeClass)
        {
            try {
                constructor = joinProbeClass.getConstructor(LookupSource.class, Page.class);
            }
            catch (NoSuchMethodException e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        public JoinProbe createJoinProbe(LookupSource lookupSource, Page page)
        {
            try {
                return constructor.newInstance(lookupSource, page);
            }
            catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
    }

    private static final class JoinOperatorCacheKey
    {
        private final List<Type> types;
        private final List<Integer> probeOutputChannels;
        private final List<Integer> probeChannels;
        private final JoinType joinType;
        private final Optional<Integer> probeHashChannel;

        private JoinOperatorCacheKey(List<? extends Type> types,
                List<Integer> probeOutputChannels,
                List<Integer> probeChannels,
                Optional<Integer> probeHashChannel,
                JoinType joinType)
        {
            this.probeHashChannel = probeHashChannel;
            this.types = ImmutableList.copyOf(types);
            this.probeOutputChannels = ImmutableList.copyOf(probeOutputChannels);
            this.probeChannels = ImmutableList.copyOf(probeChannels);
            this.joinType = joinType;
        }

        private List<Type> getTypes()
        {
            return types;
        }

        private List<Integer> getProbeOutputChannels()
        {
            return probeOutputChannels;
        }

        private List<Integer> getProbeChannels()
        {
            return probeChannels;
        }

        private Optional<Integer> getProbeHashChannel()
        {
            return probeHashChannel;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(types, probeOutputChannels, probeChannels, joinType);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof JoinOperatorCacheKey)) {
                return false;
            }
            JoinOperatorCacheKey other = (JoinOperatorCacheKey) obj;
            return Objects.equals(this.types, other.types) &&
                    Objects.equals(this.probeOutputChannels, other.probeOutputChannels) &&
                    Objects.equals(this.probeChannels, other.probeChannels) &&
                    Objects.equals(this.probeHashChannel, other.probeHashChannel) &&
                    Objects.equals(this.joinType, other.joinType);
        }
    }

    private static class HashJoinOperatorFactoryFactory
    {
        private final JoinProbeFactory joinProbeFactory;
        private final Constructor<? extends OperatorFactory> constructor;

        private HashJoinOperatorFactoryFactory(JoinProbeFactory joinProbeFactory, Class<? extends OperatorFactory> operatorFactoryClass)
        {
            this.joinProbeFactory = joinProbeFactory;

            try {
                constructor = operatorFactoryClass.getConstructor(int.class, PlanNodeId.class, LookupSourceFactory.class, List.class, List.class, JoinType.class, JoinProbeFactory.class);
            }
            catch (NoSuchMethodException e) {
                throw Throwables.propagate(e);
            }
        }

        public OperatorFactory createHashJoinOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                LookupSourceFactory lookupSourceFactory,
                List<? extends Type> probeTypes,
                List<? extends Type> probeOutputTypes,
                JoinType joinType)
        {
            try {
                return constructor.newInstance(operatorId, planNodeId, lookupSourceFactory, probeTypes, probeOutputTypes, joinType, joinProbeFactory);
            }
            catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
    }

    public static void checkState(boolean left, boolean right)
    {
        if (left != right) {
            throw new IllegalStateException();
        }
    }
}

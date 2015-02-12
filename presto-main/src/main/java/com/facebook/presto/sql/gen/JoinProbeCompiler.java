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
import com.facebook.presto.byteCode.control.IfStatement;
import com.facebook.presto.byteCode.expression.ByteCodeExpression;
import com.facebook.presto.byteCode.instruction.JumpInstruction;
import com.facebook.presto.byteCode.instruction.LabelNode;
import com.facebook.presto.operator.JoinProbe;
import com.facebook.presto.operator.JoinProbeFactory;
import com.facebook.presto.operator.LookupJoinOperator;
import com.facebook.presto.operator.LookupJoinOperatorFactory;
import com.facebook.presto.operator.LookupSource;
import com.facebook.presto.operator.LookupSourceSupplier;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.Type;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ExecutionError;
import com.google.common.util.concurrent.UncheckedExecutionException;

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
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantLong;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.newInstance;
import static com.facebook.presto.sql.gen.Bootstrap.BOOTSTRAP_METHOD;
import static com.facebook.presto.sql.gen.CompilerUtils.defineClass;
import static com.facebook.presto.sql.gen.CompilerUtils.makeClassName;
import static com.facebook.presto.sql.gen.SqlTypeByteCodeExpression.constantType;

public class JoinProbeCompiler
{
    private final LoadingCache<JoinOperatorCacheKey, HashJoinOperatorFactoryFactory> joinProbeFactories = CacheBuilder.newBuilder().maximumSize(1000).build(
            new CacheLoader<JoinOperatorCacheKey, HashJoinOperatorFactoryFactory>()
            {
                @Override
                public HashJoinOperatorFactoryFactory load(JoinOperatorCacheKey key)
                        throws Exception
                {
                    return internalCompileJoinOperatorFactory(key.getTypes(), key.getProbeChannels(), key.getProbeHashChannel());
                }
            });

    public OperatorFactory compileJoinOperatorFactory(int operatorId,
            LookupSourceSupplier lookupSourceSupplier,
            List<? extends Type> probeTypes,
            List<Integer> probeJoinChannel,
            Optional<Integer> probeHashChannel,
            boolean enableOuterJoin)
    {
        try {
            HashJoinOperatorFactoryFactory operatorFactoryFactory = joinProbeFactories.get(new JoinOperatorCacheKey(probeTypes, probeJoinChannel, probeHashChannel, enableOuterJoin));
            return operatorFactoryFactory.createHashJoinOperatorFactory(operatorId, lookupSourceSupplier, probeTypes, probeJoinChannel, enableOuterJoin);
        }
        catch (ExecutionException | UncheckedExecutionException | ExecutionError e) {
            throw Throwables.propagate(e.getCause());
        }
    }

    public HashJoinOperatorFactoryFactory internalCompileJoinOperatorFactory(List<Type> types, List<Integer> probeJoinChannel, Optional<Integer> probeHashChannel)
    {
        Class<? extends JoinProbe> joinProbeClass = compileJoinProbe(types, probeJoinChannel, probeHashChannel);

        ClassDefinition classDefinition = new ClassDefinition(new CompilerContext(BOOTSTRAP_METHOD),
                a(PUBLIC, FINAL),
                makeClassName("JoinProbeFactory"),
                type(Object.class),
                type(JoinProbeFactory.class));

        classDefinition.declareDefaultConstructor(a(PUBLIC));

        classDefinition.declareMethod(new CompilerContext(BOOTSTRAP_METHOD),
                a(PUBLIC),
                "createJoinProbe",
                type(JoinProbe.class),
                arg("lookupSource", LookupSource.class),
                arg("page", Page.class))
                .getBody()
                .newObject(joinProbeClass)
                .dup()
                .getVariable("lookupSource")
                .getVariable("page")
                .invokeConstructor(joinProbeClass, LookupSource.class, Page.class)
                .retObject();

        DynamicClassLoader classLoader = new DynamicClassLoader(joinProbeClass.getClassLoader());
        Class<? extends JoinProbeFactory> joinProbeFactoryClass = defineClass(classDefinition, JoinProbeFactory.class, classLoader);
        JoinProbeFactory joinProbeFactory;
        try {
            joinProbeFactory = joinProbeFactoryClass.newInstance();
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }

        Class<? extends OperatorFactory> operatorFactoryClass = IsolatedClass.isolateClass(
                classLoader,
                OperatorFactory.class,
                LookupJoinOperatorFactory.class,
                LookupJoinOperator.class);

        return new HashJoinOperatorFactoryFactory(joinProbeFactory, operatorFactoryClass);
    }

    @VisibleForTesting
    public JoinProbeFactory internalCompileJoinProbe(List<Type> types, List<Integer> probeChannels, Optional<Integer> probeHashChannel)
    {
        return new ReflectionJoinProbeFactory(compileJoinProbe(types, probeChannels, probeHashChannel));
    }

    private Class<? extends JoinProbe> compileJoinProbe(List<Type> types, List<Integer> probeChannels, Optional<Integer> probeHashChannel)
    {
        CallSiteBinder callSiteBinder = new CallSiteBinder();

        ClassDefinition classDefinition = new ClassDefinition(new CompilerContext(BOOTSTRAP_METHOD),
                a(PUBLIC, FINAL),
                makeClassName("JoinProbe"),
                type(Object.class),
                type(JoinProbe.class));

        // declare fields
        FieldDefinition lookupSourceField = classDefinition.declareField(a(PRIVATE, FINAL), "lookupSource", LookupSource.class);
        FieldDefinition positionCountField = classDefinition.declareField(a(PRIVATE, FINAL), "positionCount", int.class);
        List<FieldDefinition> blockFields = new ArrayList<>();
        for (int i = 0; i < types.size(); i++) {
            FieldDefinition channelField = classDefinition.declareField(a(PRIVATE, FINAL), "block_" + i, com.facebook.presto.spi.block.Block.class);
            blockFields.add(channelField);
        }
        List<FieldDefinition> probeBlockFields = new ArrayList<>();
        for (int i = 0; i < probeChannels.size(); i++) {
            FieldDefinition channelField = classDefinition.declareField(a(PRIVATE, FINAL), "probeBlock_" + i, com.facebook.presto.spi.block.Block.class);
            probeBlockFields.add(channelField);
        }
        FieldDefinition probeBlocksArrayField = classDefinition.declareField(a(PRIVATE, FINAL), "probeBlocks", com.facebook.presto.spi.block.Block[].class);
        FieldDefinition probePageField = classDefinition.declareField(a(PRIVATE, FINAL), "probePage", Page.class);
        FieldDefinition positionField = classDefinition.declareField(a(PRIVATE), "position", int.class);
        FieldDefinition probeHashBlockField = classDefinition.declareField(a(PRIVATE, FINAL), "probeHashBlock", com.facebook.presto.spi.block.Block.class);

        generateConstructor(classDefinition, probeChannels, probeHashChannel, lookupSourceField, blockFields, probeBlockFields, probeBlocksArrayField, probePageField, probeHashBlockField, positionField, positionCountField);
        generateGetChannelCountMethod(classDefinition, blockFields.size());
        generateAppendToMethod(classDefinition, callSiteBinder, types, blockFields, positionField);
        generateAdvanceNextPosition(classDefinition, positionField, positionCountField);
        generateGetCurrentJoinPosition(classDefinition, callSiteBinder, lookupSourceField, probePageField, probeHashChannel, probeHashBlockField, positionField);
        generateCurrentRowContainsNull(classDefinition, probeBlockFields, positionField);

        return defineClass(classDefinition, JoinProbe.class, callSiteBinder.getBindings(), getClass().getClassLoader());
    }

    private void generateConstructor(ClassDefinition classDefinition,
            List<Integer> probeChannels,
            Optional<Integer> probeHashChannel,
            FieldDefinition lookupSourceField,
            List<FieldDefinition> blockFields,
            List<FieldDefinition> probeChannelFields,
            FieldDefinition probeBlocksArrayField,
            FieldDefinition probePageField,
            FieldDefinition probeHashBlockField,
            FieldDefinition positionField,
            FieldDefinition positionCountField)
    {
        CompilerContext context = new CompilerContext(BOOTSTRAP_METHOD);
        Block constructor = classDefinition.declareConstructor(context,
                a(PUBLIC),
                arg("lookupSource", LookupSource.class),
                arg("page", Page.class))
                .getBody()
                .comment("super();")
                .pushThis()
                .invokeConstructor(Object.class);

        constructor.comment("this.lookupSource = lookupSource;")
                .append(context.getVariable("this").setField(lookupSourceField, context.getVariable("lookupSource")));

        constructor.comment("this.positionCount = page.getPositionCount();")
                .append(context.getVariable("this").setField(positionCountField, context.getVariable("page").invoke("getPositionCount", int.class)));

        constructor.comment("Set block fields");
        for (int index = 0; index < blockFields.size(); index++) {
            constructor.append(context.getVariable("this").setField(
                    blockFields.get(index),
                    context.getVariable("page").invoke("getBlock", com.facebook.presto.spi.block.Block.class, constantInt(index))));
        }

        constructor.comment("Set probe channel fields");
        for (int index = 0; index < probeChannelFields.size(); index++) {
            constructor.append(context.getVariable("this").setField(
                    probeChannelFields.get(index),
                    context.getVariable("this").getField(blockFields.get(probeChannels.get(index)))));
        }

        constructor.comment("this.probeBlocks = new Block[<probeChannelCount>];");
        constructor
                .pushThis()
                .push(probeChannelFields.size())
                .newArray(com.facebook.presto.spi.block.Block.class)
                .putField(probeBlocksArrayField);
        for (int index = 0; index < probeChannelFields.size(); index++) {
            constructor
                    .pushThis()
                    .getField(probeBlocksArrayField)
                    .push(index)
                    .pushThis()
                    .getField(probeChannelFields.get(index))
                    .putObjectArrayElement();
        }

        ByteCodeExpression page = newInstance(Page.class, context.getVariable("this").getField(probeBlocksArrayField));
        constructor.comment("this.probePage = new Page(probeBlocks)")
                .append(context.getVariable("this").setField(probePageField, page));

        if (probeHashChannel.isPresent()) {
            Integer index = probeHashChannel.get();
            constructor.comment("this.probeHashBlock = blocks[hashChannel.get()]")
                    .append(context.getVariable("this").setField(
                            probeHashBlockField,
                            context.getVariable("this").getField(blockFields.get(index))));
        }

        constructor.comment("this.position = -1;")
                .append(context.getVariable("this").setField(positionField, constantInt(-1)));

        constructor.ret();
    }

    private void generateGetChannelCountMethod(ClassDefinition classDefinition, int channelCount)
    {
        classDefinition.declareMethod(new CompilerContext(BOOTSTRAP_METHOD),
                a(PUBLIC),
                "getChannelCount",
                type(int.class))
                .getBody()
                .push(channelCount)
                .retInt();
    }

    private void generateAppendToMethod(
            ClassDefinition classDefinition,
            CallSiteBinder callSiteBinder,
            List<Type> types, List<FieldDefinition> blockFields,
            FieldDefinition positionField)
    {
        CompilerContext context = new CompilerContext(BOOTSTRAP_METHOD);
        Block appendToBody = classDefinition.declareMethod(context,
                a(PUBLIC),
                "appendTo",
                type(void.class),
                arg("pageBuilder", PageBuilder.class))
                .getBody();

        for (int index = 0; index < blockFields.size(); index++) {
            Type type = types.get(index);
            appendToBody
                    .comment("%s.appendTo(block_%s, position, pageBuilder.getBlockBuilder(%s));", type.getClass(), index, index)
                    .append(constantType(context, callSiteBinder, type).invoke("appendTo", void.class,
                            context.getVariable("this").getField(blockFields.get(index)),
                            context.getVariable("this").getField(positionField),
                            context.getVariable("pageBuilder").invoke("getBlockBuilder", BlockBuilder.class, constantInt(index))));
        }
        appendToBody.ret();
    }

    private void generateAdvanceNextPosition(ClassDefinition classDefinition, FieldDefinition positionField, FieldDefinition positionCountField)
    {
        CompilerContext context = new CompilerContext(BOOTSTRAP_METHOD);
        Block advanceNextPositionBody = classDefinition.declareMethod(context,
                a(PUBLIC),
                "advanceNextPosition",
                type(boolean.class))
                .getBody();

        advanceNextPositionBody
                .comment("this.position = this.position + 1;")
                .pushThis()
                .pushThis()
                .getField(positionField)
                .push(1)
                .intAdd()
                .putField(positionField);

        LabelNode lessThan = new LabelNode("lessThan");
        LabelNode end = new LabelNode("end");
        advanceNextPositionBody
                .comment("return position < positionCount;")
                .pushThis()
                .getField(positionField)
                .pushThis()
                .getField(positionCountField)
                .append(JumpInstruction.jumpIfIntLessThan(lessThan))
                .push(false)
                .gotoLabel(end)
                .visitLabel(lessThan)
                .push(true)
                .visitLabel(end)
                .retBoolean();
    }

    private void generateGetCurrentJoinPosition(ClassDefinition classDefinition,
            CallSiteBinder callSiteBinder,
            FieldDefinition lookupSourceField,
            FieldDefinition probePageField,
            Optional<Integer> probeHashChannel,
            FieldDefinition probeHashBlockField,
            FieldDefinition positionField)
    {
        CompilerContext context = new CompilerContext(BOOTSTRAP_METHOD);
//        Variable thisVariable = context.getVariable("this");
        Block body = classDefinition.declareMethod(context,
                a(PUBLIC),
                "getCurrentJoinPosition",
                type(long.class))
                .getBody()
                .append(new IfStatement(
                        context,
                        context.getVariable("this").invoke("currentRowContainsNull", boolean.class),
                        constantLong(-1).ret(),
                        null
                ));

        ByteCodeExpression position = context.getVariable("this").getField(positionField);
        ByteCodeExpression page = context.getVariable("this").getField(probePageField);
        ByteCodeExpression probeHashBlock = context.getVariable("this").getField(probeHashBlockField);
        if (probeHashChannel.isPresent()) {
            body.append(context.getVariable("this").getField(lookupSourceField).invoke("getJoinPosition", long.class,
                    position,
                    page,
                    constantType(context, callSiteBinder, BigintType.BIGINT).invoke("getLong",
                            long.class,
                            probeHashBlock,
                            position)
                            .cast(int.class)))
                    .retLong();
        }
        else {
            body.append(context.getVariable("this").getField(lookupSourceField).invoke("getJoinPosition", long.class, position, page)).retLong();
        }
    }

    private void generateCurrentRowContainsNull(ClassDefinition classDefinition, List<FieldDefinition> probeBlockFields, FieldDefinition positionField)
    {
        CompilerContext context = new CompilerContext(BOOTSTRAP_METHOD);
        Block body = classDefinition.declareMethod(context,
                a(PRIVATE),
                "currentRowContainsNull",
                type(boolean.class))
                .getBody();

        for (FieldDefinition probeBlockField : probeBlockFields) {
            LabelNode checkNextField = new LabelNode("checkNextField");
            body
                    .append(context.getVariable("this").getField(probeBlockField).invoke("isNull", boolean.class, context.getVariable("this").getField(positionField)))
                    .ifFalseGoto(checkNextField)
                    .push(true)
                    .retBoolean()
                    .visitLabel(checkNextField);
        }

        body.push(false).retInt();
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
        private final List<Integer> probeChannels;
        private final boolean enableOuterJoin;
        private final Optional<Integer> probeHashChannel;

        private JoinOperatorCacheKey(List<? extends Type> types,
                List<Integer> probeChannels,
                Optional<Integer> probeHashChannel,
                boolean enableOuterJoin)
        {
            this.probeHashChannel = probeHashChannel;
            this.types = ImmutableList.copyOf(types);
            this.probeChannels = ImmutableList.copyOf(probeChannels);
            this.enableOuterJoin = enableOuterJoin;
        }

        private List<Type> getTypes()
        {
            return types;
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
            return Objects.hash(types, probeChannels, enableOuterJoin);
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
                    Objects.equals(this.probeChannels, other.probeChannels) &&
                    Objects.equals(this.probeHashChannel, other.probeHashChannel) &&
                    Objects.equals(this.enableOuterJoin, other.enableOuterJoin);
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
                constructor = operatorFactoryClass.getConstructor(int.class, LookupSourceSupplier.class, List.class, boolean.class, JoinProbeFactory.class);
            }
            catch (NoSuchMethodException e) {
                throw Throwables.propagate(e);
            }
        }

        public OperatorFactory createHashJoinOperatorFactory(
                int operatorId,
                LookupSourceSupplier lookupSourceSupplier,
                List<? extends Type> probeTypes,
                List<Integer> probeJoinChannel,
                boolean enableOuterJoin)
        {
            try {
                return constructor.newInstance(operatorId, lookupSourceSupplier, probeTypes, enableOuterJoin, joinProbeFactory);
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

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
import com.facebook.presto.byteCode.ClassInfoLoader;
import com.facebook.presto.byteCode.CompilerContext;
import com.facebook.presto.byteCode.DumpByteCodeVisitor;
import com.facebook.presto.byteCode.DynamicClassLoader;
import com.facebook.presto.byteCode.FieldDefinition;
import com.facebook.presto.byteCode.LocalVariableDefinition;
import com.facebook.presto.byteCode.ParameterizedType;
import com.facebook.presto.byteCode.SmartClassWriter;
import com.facebook.presto.byteCode.control.IfStatement;
import com.facebook.presto.byteCode.instruction.LabelNode;
import com.facebook.presto.operator.JoinProbe;
import com.facebook.presto.operator.JoinProbeFactory;
import com.facebook.presto.operator.LookupJoinOperator;
import com.facebook.presto.operator.LookupJoinOperatorFactory;
import com.facebook.presto.operator.LookupSource;
import com.facebook.presto.operator.LookupSourceSupplier;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageBuilder;
import com.facebook.presto.operator.aggregation.IsolatedClass;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockCursor;
import com.facebook.presto.spi.type.Type;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.google.common.util.concurrent.ExecutionError;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.log.Logger;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.util.CheckClassAdapter;
import org.objectweb.asm.util.TraceClassVisitor;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.byteCode.Access.FINAL;
import static com.facebook.presto.byteCode.Access.PRIVATE;
import static com.facebook.presto.byteCode.Access.PUBLIC;
import static com.facebook.presto.byteCode.Access.a;
import static com.facebook.presto.byteCode.NamedParameterDefinition.arg;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.byteCode.ParameterizedType.typeFromPathName;

public class JoinProbeCompiler
{
    private static final Logger log = Logger.get(ExpressionCompiler.class);

    private static final AtomicLong CLASS_ID = new AtomicLong();

    private static final boolean DUMP_BYTE_CODE_TREE = false;
    private static final boolean DUMP_BYTE_CODE_RAW = false;
    private static final boolean RUN_ASM_VERIFIER = false; // verifier doesn't work right now
    private static final AtomicReference<String> DUMP_CLASS_FILES_TO = new AtomicReference<>();

    private final Method bootstrapMethod = null;

    private final LoadingCache<JoinOperatorCacheKey, HashJoinOperatorFactoryFactory> joinProbeFactories = CacheBuilder.newBuilder().maximumSize(1000).build(
            new CacheLoader<JoinOperatorCacheKey, HashJoinOperatorFactoryFactory>()
            {
                @Override
                public HashJoinOperatorFactoryFactory load(JoinOperatorCacheKey key)
                        throws Exception
                {
                    return internalCompileJoinOperatorFactory(key.getTypes().size(), key.getProbeChannels());
                }
            });

    public OperatorFactory compileJoinOperatorFactory(int operatorId,
            LookupSourceSupplier lookupSourceSupplier,
            List<? extends Type> probeTypes,
            List<Integer> probeJoinChannel,
            boolean enableOuterJoin)
    {
        try {
            HashJoinOperatorFactoryFactory operatorFactoryFactory = joinProbeFactories.get(new JoinOperatorCacheKey(probeTypes, probeJoinChannel, enableOuterJoin));
            return operatorFactoryFactory.createHashJoinOperatorFactory(operatorId, lookupSourceSupplier, probeTypes, probeJoinChannel, enableOuterJoin);
        }
        catch (ExecutionException | UncheckedExecutionException | ExecutionError e) {
            throw Throwables.propagate(e.getCause());
        }
    }

    public HashJoinOperatorFactoryFactory internalCompileJoinOperatorFactory(int channelCount, List<Integer> probeJoinChannel)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(getClass().getClassLoader());
        Class<? extends JoinProbe> joinProbeClass = compileJoinProbe(channelCount, probeJoinChannel, classLoader);

        ClassDefinition classDefinition = new ClassDefinition(new CompilerContext(bootstrapMethod),
                a(PUBLIC, FINAL),
                typeFromPathName("JoinProbeFactory_" + CLASS_ID.incrementAndGet()),
                type(Object.class),
                type(JoinProbeFactory.class));

        classDefinition.declareConstructor(new CompilerContext(bootstrapMethod),
                a(PUBLIC))
                .getBody()
                .comment("super();")
                .pushThis()
                .invokeConstructor(Object.class)
                .ret();

        classDefinition.declareMethod(new CompilerContext(bootstrapMethod),
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
    public JoinProbeFactory internalCompileJoinProbe(int channelCount, List<Integer> probeChannels)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(getClass().getClassLoader());

        Class<? extends JoinProbe> joinProbeClass = compileJoinProbe(channelCount, probeChannels, classLoader);

        return new ReflectionJoinProbeFactory(joinProbeClass);
    }

    private Class<? extends JoinProbe> compileJoinProbe(int channelCount, List<Integer> probeChannels, DynamicClassLoader classLoader)
    {
        ClassDefinition classDefinition = new ClassDefinition(new CompilerContext(bootstrapMethod),
                a(PUBLIC, FINAL),
                typeFromPathName("JoinProbe_" + CLASS_ID.incrementAndGet()),
                type(Object.class),
                type(JoinProbe.class));

        // declare fields
        FieldDefinition lookupSourceField = classDefinition.declareField(a(PRIVATE, FINAL), "lookupSource", LookupSource.class);
        List<FieldDefinition> cursorFields = new ArrayList<>();
        for (int i = 0; i < channelCount; i++) {
            FieldDefinition channelField = classDefinition.declareField(a(PRIVATE, FINAL), "cursor_" + i, BlockCursor.class);
            cursorFields.add(channelField);
        }
        List<FieldDefinition> probeCursorFields = new ArrayList<>();
        for (int i = 0; i < probeChannels.size(); i++) {
            FieldDefinition channelField = classDefinition.declareField(a(PRIVATE, FINAL), "probeCursor_" + i, BlockCursor.class);
            probeCursorFields.add(channelField);
        }
        FieldDefinition probeCursorsArrayField = classDefinition.declareField(a(PRIVATE, FINAL), "probeCursors", BlockCursor[].class);

        generateConstructor(classDefinition, probeChannels, lookupSourceField, cursorFields, probeCursorFields, probeCursorsArrayField);
        generateGetChannelCountMethod(classDefinition, cursorFields.size());
        generateAppendToMethod(classDefinition, cursorFields);
        generateAdvanceNextPosition(classDefinition, cursorFields);
        generateGetCurrentJoinPosition(classDefinition, lookupSourceField, probeCursorsArrayField);
        generateCurrentRowContainsNull(classDefinition, probeCursorFields);

        Class<? extends JoinProbe> joinProbeClass = defineClass(classDefinition, JoinProbe.class, classLoader);
        return joinProbeClass;
    }

    private void generateConstructor(ClassDefinition classDefinition,
            List<Integer> probeChannels,
            FieldDefinition lookupSourceField,
            List<FieldDefinition> cursorFields,
            List<FieldDefinition> probeChannelFields,
            FieldDefinition probeCursorsArrayField)
    {
        Block constructor = classDefinition.declareConstructor(new CompilerContext(bootstrapMethod),
                a(PUBLIC),
                arg("lookupSource", LookupSource.class),
                arg("page", Page.class))
                .getBody()
                .comment("super();")
                .pushThis()
                .invokeConstructor(Object.class);

        constructor.comment("this.lookupSource = lookupSource;");
        constructor
                .pushThis()
                .getVariable("lookupSource")
                .putField(lookupSourceField);

        constructor.comment("Set cursor fields");
        for (int index = 0; index < cursorFields.size(); index++) {
            constructor
                    .pushThis()
                    .getVariable("page")
                    .push(index)
                    .invokeVirtual(Page.class, "getBlock", com.facebook.presto.spi.block.Block.class, int.class)
                    .invokeInterface(com.facebook.presto.spi.block.Block.class, "cursor", BlockCursor.class)
                    .putField(cursorFields.get(index));
        }

        constructor.comment("Set probe channel fields");
        for (int index = 0; index < probeChannelFields.size(); index++) {
            constructor
                    .pushThis()
                    .pushThis()
                    .getField(cursorFields.get(probeChannels.get(index)))
                    .putField(probeChannelFields.get(index));
        }

        constructor.comment("this.probeCursors = new BlockCursor[<probeChannelCount>];");
        constructor
                .pushThis()
                .push(probeChannelFields.size())
                .newArray(BlockCursor.class)
                .putField(probeCursorsArrayField);
        for (int index = 0; index < probeChannelFields.size(); index++) {
            constructor
                    .pushThis()
                    .getField(probeCursorsArrayField)
                    .push(index)
                    .pushThis()
                    .getField(probeChannelFields.get(index))
                    .putObjectArrayElement();
        }

        constructor.ret();
    }

    private void generateGetChannelCountMethod(ClassDefinition classDefinition, int channelCount)
    {
        classDefinition.declareMethod(new CompilerContext(bootstrapMethod),
                a(PUBLIC),
                "getChannelCount",
                type(int.class))
                .getBody()
                .push(channelCount)
                .retInt();
    }

    private void generateAppendToMethod(ClassDefinition classDefinition, List<FieldDefinition> cursorFields)
    {
        Block appendToBody = classDefinition.declareMethod(new CompilerContext(bootstrapMethod),
                a(PUBLIC),
                "appendTo",
                type(void.class),
                arg("pageBuilder", PageBuilder.class))
                .getBody();

        for (int index = 0; index < cursorFields.size(); index++) {
            appendToBody
                    .pushThis()
                    .getField(cursorFields.get(index))
                    .getVariable("pageBuilder")
                    .push(index)
                    .invokeVirtual(PageBuilder.class, "getBlockBuilder", BlockBuilder.class, int.class)
                    .invokeInterface(BlockCursor.class, "appendTo", void.class, BlockBuilder.class);
        }
        appendToBody.ret();
    }

    private void generateAdvanceNextPosition(ClassDefinition classDefinition, List<FieldDefinition> cursorFields)
    {
        CompilerContext compilerContext = new CompilerContext(bootstrapMethod);
        Block advanceNextPositionBody = classDefinition.declareMethod(compilerContext,
                a(PUBLIC),
                "advanceNextPosition",
                type(boolean.class))
                .getBody();
        LocalVariableDefinition advancedVariable = compilerContext.declareVariable(boolean.class, "advanced");

        advanceNextPositionBody
                .pushThis()
                .getField(cursorFields.get(0))
                .invokeInterface(BlockCursor.class, "advanceNextPosition", boolean.class)
                .putVariable(advancedVariable);

        for (int index = 1; index < cursorFields.size(); index++) {
            advanceNextPositionBody
                    .pushThis()
                    .getField(cursorFields.get(index))
                    .invokeInterface(BlockCursor.class, "advanceNextPosition", boolean.class)
                    .getVariable(advancedVariable)
                    .invokeStatic(JoinProbeCompiler.class, "checkState", void.class, boolean.class, boolean.class);
        }

        advanceNextPositionBody
                .getVariable(advancedVariable)
                .retBoolean();
    }

    private void generateGetCurrentJoinPosition(ClassDefinition classDefinition, FieldDefinition lookupSourceField, FieldDefinition probeCursorsArrayField)
    {
        CompilerContext compilerContext = new CompilerContext(bootstrapMethod);
        classDefinition.declareMethod(compilerContext,
                a(PUBLIC),
                "getCurrentJoinPosition",
                type(long.class))
                .getBody()
                .append(new IfStatement(
                        compilerContext,
                        new Block(compilerContext).pushThis().invokeVirtual(classDefinition.getType(), "currentRowContainsNull", type(boolean.class)),
                        new Block(compilerContext).push(-1L).retLong(),
                        null
                ))
                .pushThis()
                .getField(lookupSourceField)
                .pushThis()
                .getField(probeCursorsArrayField)
                .invokeInterface(LookupSource.class, "getJoinPosition", long.class, BlockCursor[].class)
                .retLong();
    }

    private void generateCurrentRowContainsNull(ClassDefinition classDefinition, List<FieldDefinition> probeCursorFields)
    {
        Block body = classDefinition.declareMethod(new CompilerContext(bootstrapMethod),
                a(PRIVATE),
                "currentRowContainsNull",
                type(boolean.class))
                .getBody();

        for (FieldDefinition probeCursorField : probeCursorFields) {
            LabelNode checkNextField = new LabelNode("checkNextField");
            body
                    .pushThis()
                    .getField(probeCursorField)
                    .invokeInterface(BlockCursor.class, "isNull", boolean.class)
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

    private static <T> Class<? extends T> defineClass(ClassDefinition classDefinition, Class<T> superType, DynamicClassLoader classLoader)
    {
        Class<?> clazz = defineClasses(ImmutableList.of(classDefinition), classLoader).values().iterator().next();
        return clazz.asSubclass(superType);
    }

    private static Map<String, Class<?>> defineClasses(List<ClassDefinition> classDefinitions, DynamicClassLoader classLoader)
    {
        ClassInfoLoader classInfoLoader = ClassInfoLoader.createClassInfoLoader(classDefinitions, classLoader);

        if (DUMP_BYTE_CODE_TREE) {
            DumpByteCodeVisitor dumpByteCode = new DumpByteCodeVisitor(System.out);
            for (ClassDefinition classDefinition : classDefinitions) {
                dumpByteCode.visitClass(classDefinition);
            }
        }

        Map<String, byte[]> byteCodes = new LinkedHashMap<>();
        for (ClassDefinition classDefinition : classDefinitions) {
            ClassWriter cw = new SmartClassWriter(classInfoLoader);
            classDefinition.visit(cw);
            byte[] byteCode = cw.toByteArray();
            if (RUN_ASM_VERIFIER) {
                ClassReader reader = new ClassReader(byteCode);
                CheckClassAdapter.verify(reader, classLoader, true, new PrintWriter(System.out));
            }
            byteCodes.put(classDefinition.getType().getJavaClassName(), byteCode);
        }

        String dumpClassPath = DUMP_CLASS_FILES_TO.get();
        if (dumpClassPath != null) {
            for (Entry<String, byte[]> entry : byteCodes.entrySet()) {
                File file = new File(dumpClassPath, ParameterizedType.typeFromJavaClassName(entry.getKey()).getClassName() + ".class");
                try {
                    log.debug("ClassFile: " + file.getAbsolutePath());
                    Files.createParentDirs(file);
                    Files.write(entry.getValue(), file);
                }
                catch (IOException e) {
                    log.error(e, "Failed to write generated class file to: %s" + file.getAbsolutePath());
                }
            }
        }
        if (DUMP_BYTE_CODE_RAW) {
            for (byte[] byteCode : byteCodes.values()) {
                ClassReader classReader = new ClassReader(byteCode);
                classReader.accept(new TraceClassVisitor(new PrintWriter(System.err)), ClassReader.SKIP_FRAMES);
            }
        }
        return classLoader.defineClasses(byteCodes);
    }

    private static final class JoinOperatorCacheKey
    {
        private final List<Type> types;
        private final List<Integer> probeChannels;
        private final boolean enableOuterJoin;

        private JoinOperatorCacheKey(List<? extends Type> types,
                List<Integer> probeChannels,
                boolean enableOuterJoin)
        {
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

        @Override
        public int hashCode()
        {
            return Objects.hashCode(types, probeChannels, enableOuterJoin);
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
            final JoinOperatorCacheKey other = (JoinOperatorCacheKey) obj;
            return Objects.equal(this.types, other.types) &&
                    Objects.equal(this.probeChannels, other.probeChannels) &&
                    Objects.equal(this.enableOuterJoin, other.enableOuterJoin);
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

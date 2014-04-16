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

import com.facebook.presto.operator.LookupSource;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockCursor;
import com.facebook.presto.spi.block.RandomAccessBlock;
import com.facebook.presto.byteCode.Block;
import com.facebook.presto.byteCode.ClassDefinition;
import com.facebook.presto.byteCode.ClassInfoLoader;
import com.facebook.presto.byteCode.CompilerContext;
import com.facebook.presto.byteCode.DumpByteCodeVisitor;
import com.facebook.presto.byteCode.DynamicClassLoader;
import com.facebook.presto.byteCode.FieldDefinition;
import com.facebook.presto.byteCode.LocalVariableDefinition;
import com.facebook.presto.byteCode.MethodDefinition;
import com.facebook.presto.byteCode.OpCodes;
import com.facebook.presto.byteCode.ParameterizedType;
import com.facebook.presto.byteCode.SmartClassWriter;
import com.facebook.presto.byteCode.instruction.LabelNode;
import com.facebook.presto.operator.InMemoryJoinHash;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.PageBuilder;
import com.facebook.presto.operator.PagesHashStrategy;
import com.facebook.presto.operator.aggregation.IsolatedClass;
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
import it.unimi.dsi.fastutil.longs.LongArrayList;
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
import static com.google.common.base.Preconditions.checkNotNull;

public class JoinCompiler
{
    private static final Logger log = Logger.get(ExpressionCompiler.class);

    private static final AtomicLong CLASS_ID = new AtomicLong();

    private static final boolean DUMP_BYTE_CODE_TREE = false;
    private static final boolean DUMP_BYTE_CODE_RAW = false;
    private static final boolean RUN_ASM_VERIFIER = false; // verifier doesn't work right now
    private static final AtomicReference<String> DUMP_CLASS_FILES_TO = new AtomicReference<>();

    private final Method bootstrapMethod = null;

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
    public LookupSourceFactory internalCompileLookupSourceFactory(List<? extends Type> types, List<Integer> joinChannels)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(getClass().getClassLoader());

        Class<? extends PagesHashStrategy> pagesHashStrategyClass = compilePagesHashStrategy(types.size(), joinChannels, classLoader);

        Class<? extends LookupSource> lookupSourceClass = IsolatedClass.isolateClass(
                classLoader,
                LookupSource.class,
                InMemoryJoinHash.class);

        return new LookupSourceFactory(lookupSourceClass, new PagesHashStrategyFactory(pagesHashStrategyClass));
    }

    @VisibleForTesting
    public PagesHashStrategyFactory compilePagesHashStrategy(int channelCount, List<Integer> joinChannels)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(getClass().getClassLoader());

        Class<? extends PagesHashStrategy> pagesHashStrategyClass = compilePagesHashStrategy(channelCount, joinChannels, classLoader);

        return new PagesHashStrategyFactory(pagesHashStrategyClass);
    }

    private Class<? extends PagesHashStrategy> compilePagesHashStrategy(int channelCount, List<Integer> joinChannels, DynamicClassLoader classLoader)
    {
        ClassDefinition classDefinition = new ClassDefinition(new CompilerContext(bootstrapMethod),
                a(PUBLIC, FINAL),
                typeFromPathName("PagesHashStrategy_" + CLASS_ID.incrementAndGet()),
                type(Object.class),
                type(PagesHashStrategy.class));

        // declare fields
        List<FieldDefinition> channelFields = new ArrayList<>();
        for (int i = 0; i < channelCount; i++) {
            FieldDefinition channelField = classDefinition.declareField(a(PRIVATE, FINAL), "channel_" + i, type(List.class, RandomAccessBlock.class));
            channelFields.add(channelField);
        }
        List<FieldDefinition> joinChannelFields = new ArrayList<>();
        for (int i = 0; i < joinChannels.size(); i++) {
            FieldDefinition channelField = classDefinition.declareField(a(PRIVATE, FINAL), "joinChannel_" + i, type(List.class, RandomAccessBlock.class));
            joinChannelFields.add(channelField);
        }

        generateConstructor(classDefinition, joinChannels, channelFields, joinChannelFields);
        generateGetChannelCountMethod(classDefinition, channelFields);
        generateAppendToMethod(classDefinition, channelFields);
        generateHashPositionMethod(classDefinition, joinChannelFields);
        generatePositionEqualsCursorsMethod(classDefinition, joinChannelFields);
        generatePositionEqualsPositionMethod(classDefinition, joinChannelFields);

        Class<? extends PagesHashStrategy> pagesHashStrategyClass = defineClass(classDefinition, PagesHashStrategy.class, classLoader);
        return pagesHashStrategyClass;
    }

    private void generateConstructor(ClassDefinition classDefinition,
            List<Integer> joinChannels,
            List<FieldDefinition> channelFields,
            List<FieldDefinition> joinChannelFields)
    {
        Block constructor = classDefinition.declareConstructor(new CompilerContext(bootstrapMethod),
                a(PUBLIC),
                arg("channels", type(List.class, type(List.class, RandomAccessBlock.class))))
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
                    .checkCast(type(List.class, RandomAccessBlock.class))
                    .putField(channelFields.get(index));
        }

        constructor.comment("Set join channel fields");
        for (int index = 0; index < joinChannelFields.size(); index++) {
            constructor
                    .pushThis()
                    .getVariable("channels")
                    .push(joinChannels.get(index))
                    .invokeInterface(List.class, "get", Object.class, int.class)
                    .checkCast(type(List.class, RandomAccessBlock.class))
                    .putField(joinChannelFields.get(index));
        }

        constructor.ret();
    }

    private void generateGetChannelCountMethod(ClassDefinition classDefinition, List<FieldDefinition> channelFields)
    {
        classDefinition.declareMethod(new CompilerContext(bootstrapMethod),
                a(PUBLIC),
                "getChannelCount",
                type(int.class))
                .getBody()
                .push(channelFields.size())
                .retInt();
    }

    private void generateAppendToMethod(ClassDefinition classDefinition, List<FieldDefinition> channelFields)
    {
        Block appendToBody = classDefinition.declareMethod(new CompilerContext(bootstrapMethod),
                a(PUBLIC),
                "appendTo",
                type(void.class),
                arg("blockIndex", int.class),
                arg("blockPosition", int.class),
                arg("pageBuilder", PageBuilder.class),
                arg("outputChannelOffset", int.class))
                .getBody();

        for (int index = 0; index < channelFields.size(); index++) {
            appendToBody.pushThis()
                    .getField(channelFields.get(index))
                    .getVariable("blockIndex")
                    .invokeInterface(List.class, "get", Object.class, int.class)
                    .checkCast(RandomAccessBlock.class)
                    .getVariable("blockPosition")
                    .getVariable("pageBuilder")
                    .getVariable("outputChannelOffset")
                    .push(index)
                    .append(OpCodes.IADD)
                    .invokeVirtual(PageBuilder.class, "getBlockBuilder", BlockBuilder.class, int.class)
                    .invokeInterface(RandomAccessBlock.class, "appendTo", void.class, int.class, BlockBuilder.class);
        }
        appendToBody.ret();
    }

    private void generateHashPositionMethod(ClassDefinition classDefinition, List<FieldDefinition> joinChannelFields)
    {
        MethodDefinition hashPositionMethod = classDefinition.declareMethod(new CompilerContext(bootstrapMethod),
                a(PUBLIC),
                "hashPosition",
                type(int.class),
                arg("blockIndex", int.class),
                arg("blockPosition", int.class));

        LocalVariableDefinition resultVariable = hashPositionMethod.getCompilerContext().declareVariable(int.class, "result");
        hashPositionMethod.getBody().push(0).putVariable(resultVariable);

        for (FieldDefinition joinChannelField : joinChannelFields) {
            hashPositionMethod
                    .getBody()
                    .getVariable(resultVariable)
                    .push(31)
                    .append(OpCodes.IMUL)
                    .pushThis()
                    .getField(joinChannelField)
                    .getVariable("blockIndex")
                    .invokeInterface(List.class, "get", Object.class, int.class)
                    .checkCast(RandomAccessBlock.class)
                    .getVariable("blockPosition")
                    .invokeInterface(RandomAccessBlock.class, "hashCode", int.class, int.class)
                    .append(OpCodes.IADD)
                    .putVariable(resultVariable);
        }

        hashPositionMethod
                .getBody()
                .getVariable(resultVariable)
                .retInt();
    }

    private void generatePositionEqualsCursorsMethod(ClassDefinition classDefinition, List<FieldDefinition> joinChannelFields)
    {
        MethodDefinition hashPositionMethod = classDefinition.declareMethod(new CompilerContext(bootstrapMethod),
                a(PUBLIC),
                "positionEqualsCursors",
                type(boolean.class),
                arg("blockIndex", int.class),
                arg("blockPosition", int.class),
                arg("cursors", BlockCursor[].class));

        for (int index = 0; index < joinChannelFields.size(); index++) {
            LabelNode checkNextField = new LabelNode("checkNextField");
            hashPositionMethod
                    .getBody()
                    .pushThis()
                    .getField(joinChannelFields.get(index))
                    .getVariable("blockIndex")
                    .invokeInterface(List.class, "get", Object.class, int.class)
                    .checkCast(RandomAccessBlock.class)
                    .getVariable("blockPosition")
                    .getVariable("cursors")
                    .push(index)
                    .getObjectArrayElement()
                    .invokeInterface(RandomAccessBlock.class, "equals", boolean.class, int.class, BlockCursor.class)
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

    private void generatePositionEqualsPositionMethod(ClassDefinition classDefinition, List<FieldDefinition> joinChannelFields)
    {
        MethodDefinition hashPositionMethod = classDefinition.declareMethod(new CompilerContext(bootstrapMethod),
                a(PUBLIC),
                "positionEqualsPosition",
                type(boolean.class),
                arg("leftBlockIndex", int.class),
                arg("leftBlockPosition", int.class),
                arg("rightBlockIndex", int.class),
                arg("rightBlockPosition", int.class));

        for (FieldDefinition joinChannelField : joinChannelFields) {
            LabelNode checkNextField = new LabelNode("checkNextField");
            hashPositionMethod
                    .getBody()
                    .pushThis()
                    .getField(joinChannelField)
                    .getVariable("leftBlockIndex")
                    .invokeInterface(List.class, "get", Object.class, int.class)
                    .checkCast(RandomAccessBlock.class)
                    .getVariable("leftBlockPosition")
                    .pushThis()
                    .getField(joinChannelField)
                    .getVariable("rightBlockIndex")
                    .invokeInterface(List.class, "get", Object.class, int.class)
                    .checkCast(RandomAccessBlock.class)
                    .getVariable("rightBlockPosition")
                    .invokeInterface(RandomAccessBlock.class, "equals", boolean.class, int.class, RandomAccessBlock.class, int.class)
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
                constructor = lookupSourceClass.getConstructor(LongArrayList.class, PagesHashStrategy.class, OperatorContext.class);
            }
            catch (NoSuchMethodException e) {
                throw Throwables.propagate(e);
            }
        }

        public LookupSource createLookupSource(LongArrayList addresses, List<List<RandomAccessBlock>> channels, OperatorContext operatorContext)
        {
            PagesHashStrategy pagesHashStrategy = pagesHashStrategyFactory.createPagesHashStrategy(channels);
            try {
                return constructor.newInstance(addresses, pagesHashStrategy, operatorContext);
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

        public PagesHashStrategy createPagesHashStrategy(List<List<RandomAccessBlock>> channels)
        {
            try {
                return constructor.newInstance(channels);
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

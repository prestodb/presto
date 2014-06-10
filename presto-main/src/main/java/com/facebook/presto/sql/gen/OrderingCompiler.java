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
import com.facebook.presto.byteCode.LocalVariableDefinition;
import com.facebook.presto.byteCode.MethodDefinition;
import com.facebook.presto.byteCode.ParameterizedType;
import com.facebook.presto.byteCode.SmartClassWriter;
import com.facebook.presto.byteCode.instruction.LabelNode;
import com.facebook.presto.operator.PagesIndex;
import com.facebook.presto.operator.PagesIndexComparator;
import com.facebook.presto.operator.PagesIndexOrdering;
import com.facebook.presto.operator.SimplePagesIndexComparator;
import com.facebook.presto.operator.SyntheticAddress;
import com.facebook.presto.spi.block.SortOrder;
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
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.util.CheckClassAdapter;
import org.objectweb.asm.util.TraceClassVisitor;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.byteCode.Access.FINAL;
import static com.facebook.presto.byteCode.Access.PUBLIC;
import static com.facebook.presto.byteCode.Access.a;
import static com.facebook.presto.byteCode.NamedParameterDefinition.arg;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.byteCode.ParameterizedType.typeFromPathName;
import static com.google.common.base.Preconditions.checkNotNull;

public class OrderingCompiler
{
    private static final Logger log = Logger.get(ExpressionCompiler.class);

    private static final AtomicLong CLASS_ID = new AtomicLong();

    private static final boolean DUMP_BYTE_CODE_TREE = false;
    private static final boolean DUMP_BYTE_CODE_RAW = false;
    private static final boolean RUN_ASM_VERIFIER = false; // verifier doesn't work right now
    private static final AtomicReference<String> DUMP_CLASS_FILES_TO = new AtomicReference<>();

    private final Method bootstrapMethod = null;

    private final LoadingCache<PagesIndexComparatorCacheKey, PagesIndexOrdering> pagesIndexOrderings = CacheBuilder.newBuilder().maximumSize(1000).build(
            new CacheLoader<PagesIndexComparatorCacheKey, PagesIndexOrdering>()
            {
                @Override
                public PagesIndexOrdering load(PagesIndexComparatorCacheKey key)
                        throws Exception
                {
                    return internalCompilePagesIndexOrdering(key.getSortChannels(), key.getSortOrders());
                }
            });

    public PagesIndexOrdering compilePagesIndexOrdering(List<Integer> sortChannels, List<SortOrder> sortOrders)
    {
        checkNotNull(sortChannels, "sortChannels is null");
        checkNotNull(sortOrders, "sortOrders is null");

        try {
            return pagesIndexOrderings.get(new PagesIndexComparatorCacheKey(sortChannels, sortOrders));
        }
        catch (ExecutionException | UncheckedExecutionException | ExecutionError e) {
            throw Throwables.propagate(e.getCause());
        }
    }

    @VisibleForTesting
    public PagesIndexOrdering internalCompilePagesIndexOrdering(List<Integer> sortChannels, List<SortOrder> sortOrders)
            throws Exception
    {
        checkNotNull(sortChannels, "sortChannels is null");
        checkNotNull(sortOrders, "sortOrders is null");

        PagesIndexComparator comparator;
        try {
            DynamicClassLoader classLoader = new DynamicClassLoader(getClass().getClassLoader());

            Class<? extends PagesIndexComparator> pagesHashStrategyClass = compilePagesIndexComparator(sortChannels, sortOrders, classLoader);
            comparator = pagesHashStrategyClass.newInstance();
        }
        catch (Throwable e) {
            log.error(e, "Error compiling comparator for channels %s with order %s", sortChannels, sortChannels);
            comparator = new SimplePagesIndexComparator(sortChannels, sortOrders);
        }

        // we may want to load a separate PagesIndexOrdering for each comparator
        return new PagesIndexOrdering(comparator);
    }

    private Class<? extends PagesIndexComparator> compilePagesIndexComparator(List<Integer> sortChannels, List<SortOrder> sortOrders, DynamicClassLoader classLoader)
    {
        ClassDefinition classDefinition = new ClassDefinition(new CompilerContext(bootstrapMethod),
                a(PUBLIC, FINAL),
                typeFromPathName("PagesIndexComparator" + CLASS_ID.incrementAndGet()),
                type(Object.class),
                type(PagesIndexComparator.class));

        generateConstructor(classDefinition);
        generateCompareTo(classDefinition, sortChannels, sortOrders);

        Class<? extends PagesIndexComparator> joinHashClass = defineClass(classDefinition, PagesIndexComparator.class, classLoader);
        return joinHashClass;
    }

    private void generateConstructor(ClassDefinition classDefinition)
    {
        classDefinition.declareConstructor(new CompilerContext(bootstrapMethod),
                a(PUBLIC))
                .getBody()
                .comment("super();")
                .pushThis()
                .invokeConstructor(Object.class)
                .ret();
    }

    private void generateCompareTo(ClassDefinition classDefinition, List<Integer> sortChannels, List<SortOrder> sortOrders)
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

            block.comment("push leftBlock -- pagesIndex.getChannel(sortChannel).get(leftBlockIndex)")
                    .getVariable("pagesIndex")
                    .push(sortChannel)
                    .invokeVirtual(PagesIndex.class, "getChannel", ObjectArrayList.class, int.class)
                    .getVariable(leftBlockIndex)
                    .invokeVirtual(ObjectArrayList.class, "get", Object.class, int.class)
                    .checkCast(com.facebook.presto.spi.block.Block.class);

            block.comment("push sortOrder")
                    .getStaticField(SortOrder.class, sortOrder.name(), SortOrder.class);

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
                    .invokeInterface(com.facebook.presto.spi.block.Block.class, "compareTo", int.class, SortOrder.class, int.class, com.facebook.presto.spi.block.Block.class, int.class);

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

    private static final class PagesIndexComparatorCacheKey
    {
        private List<Integer> sortChannels;
        private List<SortOrder> sortOrders;

        private PagesIndexComparatorCacheKey(List<Integer> sortChannels, List<SortOrder> sortOrders)
        {
            this.sortChannels = ImmutableList.copyOf(sortChannels);
            this.sortOrders = ImmutableList.copyOf(sortOrders);
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
            return Objects.hashCode(sortChannels, sortOrders);
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
            return Objects.equal(this.sortChannels, other.sortChannels) && Objects.equal(this.sortOrders, other.sortOrders);
        }
    }
}

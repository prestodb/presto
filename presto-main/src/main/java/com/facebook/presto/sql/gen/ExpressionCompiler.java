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

import com.facebook.presto.byteCode.ClassDefinition;
import com.facebook.presto.byteCode.ClassInfoLoader;
import com.facebook.presto.byteCode.CompilerContext;
import com.facebook.presto.byteCode.DumpByteCodeVisitor;
import com.facebook.presto.byteCode.DynamicClassLoader;
import com.facebook.presto.byteCode.ParameterizedType;
import com.facebook.presto.byteCode.SmartClassWriter;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.CursorProcessor;
import com.facebook.presto.operator.PageProcessor;
import com.facebook.presto.sql.relational.RowExpression;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import io.airlift.log.Logger;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.util.CheckClassAdapter;
import org.objectweb.asm.util.TraceClassVisitor;
import org.weakref.jmx.Managed;

import javax.inject.Inject;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.byteCode.Access.FINAL;
import static com.facebook.presto.byteCode.Access.PRIVATE;
import static com.facebook.presto.byteCode.Access.PUBLIC;
import static com.facebook.presto.byteCode.Access.STATIC;
import static com.facebook.presto.byteCode.Access.VOLATILE;
import static com.facebook.presto.byteCode.Access.a;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.byteCode.ParameterizedType.typeFromPathName;
import static com.facebook.presto.sql.gen.Bootstrap.BOOTSTRAP_METHOD;
import static com.facebook.presto.sql.gen.Bootstrap.CALL_SITES_FIELD_NAME;
import static com.facebook.presto.sql.gen.ByteCodeUtils.setCallSitesField;
import static com.google.common.base.Objects.toStringHelper;

public class ExpressionCompiler
{
    private static final Logger log = Logger.get(ExpressionCompiler.class);

    private static final AtomicLong CLASS_ID = new AtomicLong();

    private static final boolean DUMP_BYTE_CODE_TREE = false;
    private static final boolean DUMP_BYTE_CODE_RAW = false;
    private static final boolean RUN_ASM_VERIFIER = false; // verifier doesn't work right now
    private static final AtomicReference<String> DUMP_CLASS_FILES_TO = new AtomicReference<>();

    private final Metadata metadata;

    private final LoadingCache<CacheKey, PageProcessor> pageProcessors = CacheBuilder.newBuilder().maximumSize(1000).build(
            new CacheLoader<CacheKey, PageProcessor>()
            {
                @Override
                public PageProcessor load(CacheKey key)
                        throws Exception
                {
                    return compileAndInstantiate(key.getFilter(), key.getProjections(), new PageProcessorCompiler(metadata), PageProcessor.class);
                }
            });

    private final LoadingCache<CacheKey, CursorProcessor> cursorProcessors = CacheBuilder.newBuilder().maximumSize(1000).build(
            new CacheLoader<CacheKey, CursorProcessor>()
            {
                @Override
                public CursorProcessor load(CacheKey key)
                        throws Exception
                {
                    return compileAndInstantiate(key.getFilter(), key.getProjections(), new CursorProcessorCompiler(metadata), CursorProcessor.class);
                }
            });

    private final AtomicLong generatedClasses = new AtomicLong();

    @Inject
    public ExpressionCompiler(Metadata metadata)
    {
        this.metadata = metadata;
    }

    @Managed
    public long getGeneratedClasses()
    {
        return generatedClasses.get();
    }

    @Managed
    public long getCacheSize()
    {
        return pageProcessors.size();
    }

    public CursorProcessor compileCursorProcessor(RowExpression filter, List<RowExpression> projections, Object uniqueKey)
    {
        return cursorProcessors.getUnchecked(new CacheKey(filter, projections, uniqueKey));
    }

    public PageProcessor compilePageProcessor(RowExpression filter, List<RowExpression> projections)
    {
        return pageProcessors.getUnchecked(new CacheKey(filter, projections, null));
    }

    private <T> T compileAndInstantiate(RowExpression filter, List<RowExpression> projections, BodyCompiler<T> bodyCompiler, Class<? extends T> superType)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(getClass().getClassLoader());

        // create filter and project page iterator class
        Class<? extends T> clazz = compileProcessor(filter, projections, bodyCompiler, superType, classLoader);
        try {
            return clazz.newInstance();
        }
        catch (ReflectiveOperationException e) {
            throw Throwables.propagate(e);
        }
    }

    private <T> Class<? extends T> compileProcessor(
            RowExpression filter,
            List<RowExpression> projections,
            BodyCompiler<T> bodyCompiler,
            Class<? extends T> superType,
            DynamicClassLoader classLoader)
    {
        ClassDefinition classDefinition = new ClassDefinition(new CompilerContext(BOOTSTRAP_METHOD),
                a(PUBLIC, FINAL),
                typeFromPathName(superType.getSimpleName() + "_" + CLASS_ID.incrementAndGet()),
                type(Object.class),
                type(superType));

        classDefinition.declareField(a(PRIVATE, VOLATILE, STATIC), CALL_SITES_FIELD_NAME, Map.class);

        // constructor
        classDefinition.declareConstructor(new CompilerContext(BOOTSTRAP_METHOD), a(PUBLIC))
                .getBody()
                .pushThis()
                .invokeSpecial(Object.class, "<init>", void.class)
                .ret();

        CallSiteBinder callSiteBinder = new CallSiteBinder();
        bodyCompiler.generateMethods(classDefinition, callSiteBinder, filter, projections);

        //
        // toString method
        //
        generateToString(
                classDefinition,
                toStringHelper(classDefinition.getType().getJavaClassName())
                        .add("filter", filter)
                        .add("projections", projections)
                        .toString());

        Class<? extends T> clazz = defineClass(classDefinition, superType, classLoader);
        setCallSitesField(clazz, callSiteBinder.getBindings());
        return clazz;
    }

    private void generateToString(ClassDefinition classDefinition, String string)
    {
        // Constant strings can't be too large or the bytecode becomes invalid
        if (string.length() > 100) {
            string = string.substring(0, 100) + "...";
        }

        classDefinition.declareMethod(new CompilerContext(BOOTSTRAP_METHOD), a(PUBLIC), "toString", type(String.class))
                .getBody()
                .push(string)
                .retObject();
    }

    private <T> Class<? extends T> defineClass(ClassDefinition classDefinition, Class<T> superType, DynamicClassLoader classLoader)
    {
        Class<?> clazz = defineClasses(ImmutableList.of(classDefinition), classLoader).values().iterator().next();
        return clazz.asSubclass(superType);
    }

    private Map<String, Class<?>> defineClasses(List<ClassDefinition> classDefinitions, DynamicClassLoader classLoader)
    {
        ClassInfoLoader classInfoLoader = ClassInfoLoader.createClassInfoLoader(classDefinitions, classLoader);

        if (DUMP_BYTE_CODE_TREE) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DumpByteCodeVisitor dumpByteCode = new DumpByteCodeVisitor(new PrintStream(out));
            for (ClassDefinition classDefinition : classDefinitions) {
                dumpByteCode.visitClass(classDefinition);
            }
            System.out.println(new String(out.toByteArray(), StandardCharsets.UTF_8));
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
        Map<String, Class<?>> classes = classLoader.defineClasses(byteCodes);
        generatedClasses.addAndGet(classes.size());
        return classes;
    }

    private static final class CacheKey
    {
        private final RowExpression filter;
        private final List<RowExpression> projections;
        private final Object uniqueKey;

        private CacheKey(RowExpression filter, List<RowExpression> projections, Object uniqueKey)
        {
            this.filter = filter;
            this.uniqueKey = uniqueKey;
            this.projections = ImmutableList.copyOf(projections);
        }

        private RowExpression getFilter()
        {
            return filter;
        }

        private List<RowExpression> getProjections()
        {
            return projections;
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(filter, projections, uniqueKey);
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
            CacheKey other = (CacheKey) obj;
            return Objects.equal(this.filter, other.filter) &&
                    Objects.equal(this.projections, other.projections) &&
                    Objects.equal(this.uniqueKey, other.uniqueKey);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("filter", filter)
                    .add("projections", projections)
                    .add("uniqueKey", uniqueKey)
                    .toString();
        }
    }
}

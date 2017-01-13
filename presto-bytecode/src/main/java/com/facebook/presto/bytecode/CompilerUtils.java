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
package com.facebook.presto.bytecode;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.google.common.reflect.Reflection;
import io.airlift.log.Logger;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.util.CheckClassAdapter;
import org.objectweb.asm.util.Printer;
import org.objectweb.asm.util.Textifier;
import org.objectweb.asm.util.TraceClassVisitor;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.invoke.MethodHandle;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public final class CompilerUtils
{
    private static final Logger log = Logger.get(CompilerUtils.class);

    private static final boolean ADD_FAKE_LINE_NUMBER = false;
    private static final boolean DUMP_BYTE_CODE_TREE = false;
    private static final boolean DUMP_BYTE_CODE_RAW = false;
    private static final boolean RUN_ASM_VERIFIER = false; // verifier doesn't work right now
    private static final AtomicReference<String> DUMP_CLASS_FILES_TO = new AtomicReference<>();
    private static final AtomicLong CLASS_ID = new AtomicLong();

    private CompilerUtils()
    {
    }

    public static ParameterizedType makeClassName(String baseName)
    {
        String className = "com.facebook.presto.$gen." + baseName + "_" + CLASS_ID.incrementAndGet();
        String javaClassName = toJavaIdentifierString(className);
        return ParameterizedType.typeFromJavaClassName(javaClassName);
    }

    public static String toJavaIdentifierString(String className)
    {
        // replace invalid characters with '_'
        int[] codePoints = className.codePoints().map(c -> Character.isJavaIdentifierPart(c) ? c : '_').toArray();
        return new String(codePoints, 0, codePoints.length);
    }

    public static <T> Class<? extends T> defineClass(ClassDefinition classDefinition, Class<T> superType, DynamicClassLoader classLoader)
    {
        Class<?> clazz = defineClasses(ImmutableList.of(classDefinition), classLoader).values().iterator().next();
        return clazz.asSubclass(superType);
    }

    public static <T> Class<? extends T> defineClass(ClassDefinition classDefinition, Class<T> superType,  Map<Long, MethodHandle> callSiteBindings, ClassLoader parentClassLoader)
    {
        Class<?> clazz = defineClass(classDefinition, superType, new DynamicClassLoader(parentClassLoader, callSiteBindings));
        return clazz.asSubclass(superType);
    }

    private static Map<String, Class<?>> defineClasses(List<ClassDefinition> classDefinitions, DynamicClassLoader classLoader)
    {
        ClassInfoLoader classInfoLoader = ClassInfoLoader.createClassInfoLoader(classDefinitions, classLoader);

        if (DUMP_BYTE_CODE_TREE) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DumpBytecodeVisitor dumpBytecode = new DumpBytecodeVisitor(new PrintStream(out));
            for (ClassDefinition classDefinition : classDefinitions) {
                dumpBytecode.visitClass(classDefinition);
            }
            System.out.println(new String(out.toByteArray(), StandardCharsets.UTF_8));
        }

        Map<String, byte[]> bytecodes = new LinkedHashMap<>();
        for (ClassDefinition classDefinition : classDefinitions) {
            ClassWriter cw = new SmartClassWriter(classInfoLoader);
            try {
                classDefinition.visit(ADD_FAKE_LINE_NUMBER ? new AddFakeLineNumberClassVisitor(cw) : cw);
            }
            catch (IndexOutOfBoundsException | NegativeArraySizeException e) {
                Printer printer = new Textifier();
                StringWriter stringWriter = new StringWriter();
                TraceClassVisitor tcv = new TraceClassVisitor(null, printer, new PrintWriter(stringWriter));
                classDefinition.visit(tcv);
                throw new IllegalArgumentException("An error occurred while processing classDefinition:" + System.lineSeparator() + stringWriter.toString(), e);
            }
            try {
                byte[] bytecode = cw.toByteArray();
                if (RUN_ASM_VERIFIER) {
                    ClassReader reader = new ClassReader(bytecode);
                    CheckClassAdapter.verify(reader, classLoader, true, new PrintWriter(System.out));
                }
                bytecodes.put(classDefinition.getType().getJavaClassName(), bytecode);
            }
            catch (RuntimeException e) {
                throw new CompilationException("Error compiling class " + classDefinition.getName(), e);
            }
        }

        String dumpClassPath = DUMP_CLASS_FILES_TO.get();
        if (dumpClassPath != null) {
            for (Map.Entry<String, byte[]> entry : bytecodes.entrySet()) {
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
            for (byte[] bytecode : bytecodes.values()) {
                ClassReader classReader = new ClassReader(bytecode);
                classReader.accept(new TraceClassVisitor(new PrintWriter(System.err)), ClassReader.EXPAND_FRAMES);
            }
        }
        Map<String, Class<?>> classes = classLoader.defineClasses(bytecodes);
        try {
            for (Class<?> clazz : classes.values()) {
                Reflection.initialize(clazz);
            }
        }
        catch (VerifyError e) {
            throw new RuntimeException(e);
        }
        return classes;
    }
}

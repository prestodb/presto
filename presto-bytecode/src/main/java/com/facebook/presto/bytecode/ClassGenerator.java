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
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.Reflection;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassTooLargeException;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodTooLargeException;
import org.objectweb.asm.util.CheckClassAdapter;
import org.objectweb.asm.util.Textifier;
import org.objectweb.asm.util.TraceClassVisitor;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.lang.invoke.MethodHandle;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.bytecode.ClassInfoLoader.createClassInfoLoader;
import static com.facebook.presto.bytecode.ParameterizedType.typeFromJavaClassName;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.io.CharStreams.nullWriter;
import static java.nio.file.Files.createDirectories;
import static java.util.Objects.requireNonNull;

public class ClassGenerator
{
    private final DynamicClassLoader classLoader;
    private final boolean fakeLineNumbers;
    private final boolean runAsmVerifier;
    private final boolean dumpRawBytecode;
    private final Writer output;
    private final Optional<Path> dumpClassPath;

    public static ClassGenerator classGenerator(ClassLoader parentClassLoader)
    {
        return classGenerator(parentClassLoader, ImmutableMap.of());
    }

    public static ClassGenerator classGenerator(ClassLoader parentClassLoader, Map<Long, MethodHandle> callSiteBindings)
    {
        return classGenerator(new DynamicClassLoader(parentClassLoader, callSiteBindings));
    }

    public static ClassGenerator classGenerator(DynamicClassLoader classLoader)
    {
        return new ClassGenerator(classLoader, false, false, false, nullWriter(), Optional.empty());
    }

    private ClassGenerator(
            DynamicClassLoader classLoader,
            boolean fakeLineNumbers,
            boolean runAsmVerifier,
            boolean dumpRawBytecode,
            Writer output,
            Optional<Path> dumpClassPath)
    {
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
        this.fakeLineNumbers = fakeLineNumbers;
        this.runAsmVerifier = runAsmVerifier;
        this.dumpRawBytecode = dumpRawBytecode;
        this.output = requireNonNull(output, "output is null");
        this.dumpClassPath = requireNonNull(dumpClassPath, "dumpClassPath is null");
    }

    public ClassGenerator fakeLineNumbers(boolean fakeLineNumbers)
    {
        return new ClassGenerator(classLoader, fakeLineNumbers, runAsmVerifier, dumpRawBytecode, output, dumpClassPath);
    }

    public ClassGenerator runAsmVerifier(boolean runAsmVerifier)
    {
        return new ClassGenerator(classLoader, fakeLineNumbers, runAsmVerifier, dumpRawBytecode, output, dumpClassPath);
    }

    public ClassGenerator dumpRawBytecode(boolean dumpRawBytecode)
    {
        return new ClassGenerator(classLoader, fakeLineNumbers, runAsmVerifier, dumpRawBytecode, output, dumpClassPath);
    }

    public ClassGenerator outputTo(Writer output)
    {
        return new ClassGenerator(classLoader, fakeLineNumbers, runAsmVerifier, dumpRawBytecode, output, dumpClassPath);
    }

    public ClassGenerator dumpClassFilesTo(Path dumpClassPath)
    {
        return dumpClassFilesTo(Optional.of(dumpClassPath));
    }

    public ClassGenerator dumpClassFilesTo(Optional<Path> dumpClassPath)
    {
        return new ClassGenerator(classLoader, fakeLineNumbers, runAsmVerifier, dumpRawBytecode, output, dumpClassPath);
    }

    public <T> Class<? extends T> defineClass(ClassDefinition classDefinition, Class<T> superType)
    {
        Map<String, Class<?>> classes = defineClasses(ImmutableList.of(classDefinition));
        return getOnlyElement(classes.values()).asSubclass(superType);
    }

    public Map<String, Class<?>> defineClasses(List<ClassDefinition> classDefinitions)
    {
        ClassInfoLoader classInfoLoader = createClassInfoLoader(classDefinitions, classLoader);
        Map<String, byte[]> bytecodes = new LinkedHashMap<>();

        for (ClassDefinition classDefinition : classDefinitions) {
            // We call the simpler class writer first to get any errors out using simpler setting.
            // This helps when we have large queries that can potentially cause COMPUTE_FRAMES
            // (used by SmartClassWriter for doing more thorough analysis)
            ClassWriter simpleClassWriter = new ClassWriter(ClassWriter.COMPUTE_MAXS);
            classDefinition.visit(simpleClassWriter);
            try {
                simpleClassWriter.toByteArray();
            }
            catch (ClassTooLargeException | MethodTooLargeException largeCodeException) {
                throw new ByteCodeTooLargeException(largeCodeException);
            }
            catch (RuntimeException e) {
                throw new CompilationException("Error compiling class: " + classDefinition.getName(), e);
            }

            ClassWriter writer = new SmartClassWriter(classInfoLoader);

            try {
                classDefinition.visit(fakeLineNumbers ? new AddFakeLineNumberClassVisitor(writer) : writer);
            }
            catch (IndexOutOfBoundsException | NegativeArraySizeException e) {
                StringWriter out = new StringWriter();
                classDefinition.visit(new TraceClassVisitor(null, new Textifier(), new PrintWriter(out)));
                throw new IllegalArgumentException("Error processing class definition:\n" + out, e);
            }

            byte[] bytecode;
            try {
                bytecode = writer.toByteArray();
            }
            catch (ClassTooLargeException | MethodTooLargeException largeCodeException) {
                throw new ByteCodeTooLargeException(largeCodeException);
            }
            catch (RuntimeException e) {
                throw new CompilationException("Error compiling class: " + classDefinition.getName(), e);
            }

            bytecodes.put(classDefinition.getType().getJavaClassName(), bytecode);

            if (runAsmVerifier) {
                ClassReader reader = new ClassReader(bytecode);
                CheckClassAdapter.verify(reader, classLoader, true, new PrintWriter(output));
            }
        }

        dumpClassPath.ifPresent(path -> bytecodes.forEach((className, bytecode) -> {
            String name = typeFromJavaClassName(className).getClassName() + ".class";
            Path file = path.resolve(name).toAbsolutePath();
            try {
                createDirectories(file.getParent());
                Files.write(file, bytecode);
            }
            catch (IOException e) {
                throw new UncheckedIOException("Failed to write generated class file: " + file, e);
            }
        }));

        if (dumpRawBytecode) {
            for (byte[] bytecode : bytecodes.values()) {
                ClassReader classReader = new ClassReader(bytecode);
                classReader.accept(new TraceClassVisitor(new PrintWriter(output)), ClassReader.EXPAND_FRAMES);
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
